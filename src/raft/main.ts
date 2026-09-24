// Combined Raft node entrypoint (Milestone 5, Step 6.6; extended
// Milestone 9, DESIGN.md §7.9 for dashboard support).
//
// Supersedes the Milestone 4 raft-only main.ts: this is the one real
// process per node — owns the Store lifecycle, the RaftNode state
// machine, the apply loop (Step 6.3), the leader-side write path
// (Step 6.4), the Raft-RPC server (unchanged from Milestone 4, own
// port), and the client-facing REST API (Step 6.5, its own port per
// the two-port architecture locked in Step 6.1).
//
// Run with: npx tsx src/raft/main.ts
// (or, spawned by an integration test, directly via node against the
// resolved tsx CLI path, per the §4.4.5 orphan-process fix.)

import http from "node:http";
import path from "node:path";
import { RaftNode } from "./node.js";
import { createRaftApp, runElection, sendHeartbeats } from "./rpc.js";
import { parseClusterConfig, type PeerConfig } from "../cluster/config.js";
import { Store } from "../storage/store.js";
import { ApplyLoop } from "./applyLoop.js";
import { proposeWrite } from "./writePath.js";
import { createApp, type ClusterContext, type StatusSource, type AppFeatures } from "../api/server.js";
import type { LogCommand } from "./log.js";

// Demo-tuned, same family as the existing 100ms peer-timeout / 50ms
// heartbeat / 150-300ms election / 1000ms write-timeout constants — not
// derived, chosen. Governs how quickly a FOLLOWER's local reads reflect
// a newly-committed write (the leader's own writes are visible sooner,
// via proposeWrite's immediate apply-on-commit — see below).
const APPLY_POLL_INTERVAL_MS = 25;

function logState(nodeId: string, term: number, state: string): void {
  // Exact format locked (unchanged since Milestone 4): [{nodeId}] term={n}
  // state={state} — test/raft-election.test.ts polls stdout for exactly
  // this line shape, so it must not change here.
  console.log(`[${nodeId}] term=${term} state=${state}`);
}

/**
 * Builds the leaderRedirectHint() a ClusterContext exposes to the API
 * layer, from this node's real currentLeaderId (Step 6.2) and the real
 * cluster peer list (for the peer's clientPort — Step 6.1).
 *
 * Handles one non-obvious case explicitly: currentLeaderId can be
 * STALE and point at THIS node's own id, if this node was leader and
 * stepped down via a RequestVote (not an AppendEntries) — Step 6.2's
 * currentLeaderId is deliberately only ever updated by a real
 * AppendEntries, since a RequestVote proves candidacy, not victory (see
 * node.ts's header comment). Naively returning "the leader is me" here
 * while isLeader() is simultaneously false would be actively wrong, not
 * just imprecise — treated as unknown (null) instead.
 */
function buildLeaderRedirectHint(
  node: RaftNode,
  peers: PeerConfig[],
): { leaderId: string; url: string } | null {
  const leaderId = node.currentLeaderId;

  if (leaderId === null) {
    return null;
  }

  if (leaderId === node.nodeId) {
    // Stale self-belief from before stepping down via a vote request —
    // see this function's doc comment above.
    return null;
  }

  const peer = peers.find((p) => p.id === leaderId);
  if (peer === undefined) {
    // Shouldn't happen given config invariants (currentLeaderId always
    // comes from a real leaderId on the wire, which must be either this
    // node's own id or a configured peer's id) — defensive fallback
    // rather than throwing, since this only affects a best-effort hint,
    // not correctness of the underlying Raft state.
    return null;
  }

  return { leaderId, url: `http://${peer.host}:${peer.clientPort}` };
}

/**
 * Builds a live-reading StatusSource (DESIGN.md §7.9.1) backed directly
 * by the real node/applyLoop — getters, not a snapshot taken once here,
 * so every GET /status reflects genuinely current state at poll time,
 * not whatever term/commitIndex happened to be true when main() ran.
 * Both fields were verified directly against real node.ts/applyLoop.ts
 * source before this was written (§7.9.7) — node.commitIndex and
 * applyLoop.lastAppliedIndex are both real public getters, not assumed
 * from DESIGN.md prose.
 */
function buildStatusSource(node: RaftNode, applyLoop: ApplyLoop): StatusSource {
  return {
    get nodeId() {
      return node.nodeId;
    },
    get state() {
      return node.state;
    },
    get currentTerm() {
      return node.currentTerm;
    },
    get commitIndex() {
      return node.commitIndex;
    },
    get lastApplied() {
      return applyLoop.lastAppliedIndex;
    },
    get currentLeaderId() {
      return node.currentLeaderId;
    },
  };
}

async function main(): Promise<void> {
  const config = parseClusterConfig(process.env);

  // Per-node data directory, scoped by nodeId so multiple nodes can run
  // on the same machine (as in the integration tests) without colliding
  // on the same WAL/SSTable files. Same "runtime default, not a design
  // decision" reasoning as Milestone 3's index.ts DATA_DIR.
  const dataDir = process.env.DATA_DIR ?? path.join(process.cwd(), "data", config.nodeId);
  const walPath = process.env.WAL_PATH ?? path.join(dataDir, "wal.log");
  const sstableDir = process.env.SSTABLE_DIR ?? path.join(dataDir, "sstables");

  const store = new Store(walPath, sstableDir);
  await store.open();

  const applyLoop = new ApplyLoop(store);

  let lastLoggedState: string | null = null;
  let lastLoggedTerm: number | null = null;

  function logIfChanged(node: RaftNode): void {
    if (node.state !== lastLoggedState || node.currentTerm !== lastLoggedTerm) {
      lastLoggedState = node.state;
      lastLoggedTerm = node.currentTerm;
      logState(node.nodeId, node.currentTerm, node.state);
    }
  }

  const node = new RaftNode({
    nodeId: config.nodeId,
    onElectionTimeout: () => {
      runElection(node, config.peers)
        .then(() => logIfChanged(node))
        .catch((err) => {
          console.error(`[${config.nodeId}] runElection error:`, err);
        });
    },
    onHeartbeatDue: () => {
      sendHeartbeats(node, config.peers)
        .then(() => logIfChanged(node))
        .catch((err) => {
          console.error(`[${config.nodeId}] sendHeartbeats error:`, err);
        });
    },
  });

  // Log the initial state immediately so a watcher polling stdout has
  // something to see even before the first election timeout fires.
  logIfChanged(node);

  // Periodically applies newly-committed entries to THIS node's own
  // local Store — runs uniformly on leader AND follower, no branching,
  // per DESIGN.md Section 3 ("each node's storage engine is fully
  // independent... replication happens at the log entry level"). A
  // leader that just committed a client write via proposeWrite already
  // applied that specific entry immediately (see writePath.ts) — this
  // interval is what makes FOLLOWERS ever apply anything at all, and
  // also catches any commit progress made outside a direct client write
  // (e.g. commitIndex advancing further on a later background heartbeat
  // round). Re-applying an already-applied entry is a no-op (ApplyLoop
  // tracks lastAppliedIndex itself, per Step 6.3), so overlap with
  // proposeWrite's own apply call is harmless, not a double-apply bug.
  //
  // A failed apply is treated as fatal, per Step 6.3's design: it's a
  // serious, unrecoverable state-divergence risk between nodes, not
  // something to log-and-continue past — same "loud failure over silent
  // ambiguity" principle held throughout this project.
  const applyIntervalHandle = setInterval(() => {
    applyLoop.applyCommitted(node).catch((err) => {
      console.error(`[${config.nodeId}] FATAL: applyLoop.applyCommitted failed:`, err);
      process.exit(1);
    });
  }, APPLY_POLL_INTERVAL_MS);

  const clusterContext: ClusterContext = {
    isLeader: () => node.state === "leader",
    leaderRedirectHint: () => buildLeaderRedirectHint(node, config.peers),
    write: (command: LogCommand) => proposeWrite(node, config.peers, applyLoop, command),
  };

  // Milestone 9 (DESIGN.md §7.9.1, §7.9.6): both dashboard-support
  // routes are structurally absent from the app unless explicitly
  // opted into via env var — not mounted-but-gated. Read here, once,
  // at startup; not re-checked per-request, since these are meant to be
  // fixed per-process configuration (matching how every other env var
  // in this file, e.g. DATA_DIR, is read once in main()).
  const features: AppFeatures = {
    status: process.env.ENABLE_STATUS_ENDPOINT === "true" ? buildStatusSource(node, applyLoop) : undefined,
    enableKill: process.env.ENABLE_KILL_ENDPOINT === "true",
  };

  const raftApp = createRaftApp(node);
  const raftServer = http.createServer(raftApp);

  const clientApp = createApp(store, clusterContext, features);
  const clientServer = http.createServer(clientApp);

  // Bind 0.0.0.0, not localhost: these are separate OS processes (in
  // integration tests, and eventually separate machines/containers per
  // Milestone 8), so both listen addresses must be reachable from
  // sibling processes and real clients, not just loopback-restricted.
  raftServer.listen(config.port, "0.0.0.0", () => {
    console.log(
      `[${config.nodeId}] raft RPC listening on 0.0.0.0:${config.port}, peers: ${config.peers
        .map((p) => `${p.id}=${p.host}:${p.port}`)
        .join(", ")}`,
    );
  });

  clientServer.listen(config.clientPort, "0.0.0.0", () => {
    console.log(`[${config.nodeId}] client API listening on 0.0.0.0:${config.clientPort}`);
    if (features.status !== undefined) {
      console.log(`[${config.nodeId}] /status enabled`);
    }
    if (features.enableKill === true) {
      console.log(`[${config.nodeId}] /debug/kill enabled`);
    }
  });

  function shutdown(signal: string): void {
    console.log(`\n[${config.nodeId}] received ${signal}, shutting down...`);
    node.stop();
    clearInterval(applyIntervalHandle);
    raftServer.close();
    clientServer.close();
    store
      .close()
      .catch((err) => {
        console.error(`[${config.nodeId}] error closing store during shutdown:`, err);
      })
      .finally(() => {
        process.exit(0);
      });
  }

  process.on("SIGINT", () => shutdown("SIGINT"));
  process.on("SIGTERM", () => shutdown("SIGTERM"));
}

main().catch((err) => {
  console.error("Failed to start node:", err);
  process.exit(1);
});