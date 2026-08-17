// Raft node entrypoint (Milestone 4, Step 5a).
//
// Parallel to src/index.ts (Milestone 3's REST API entrypoint): this is
// the runnable process that wires together everything built so far —
// cluster config (Step 2), the state machine (Step 1), and RPC wiring
// (Steps 3-4) — into an actual bootable node.
//
// Run with: npx tsx src/raft/main.ts
// (or, once spawned by the Step 5b integration test, directly via node
// against the resolved tsx CLI path, per the §4.4.5 orphan-process fix.)

import http from "node:http";
import { RaftNode } from "./node.js";
import { createRaftApp, runElection, sendHeartbeats } from "./rpc.js";
import { parseClusterConfig } from "../cluster/config.js";

function logState(nodeId: string, term: number, state: string): void {
  // Exact format locked with the person: [{nodeId}] term={n} state={state}
  // — deliberately simple and greppable, since the Step 5b integration
  // test polls stdout for this line rather than any richer structure.
  console.log(`[${nodeId}] term=${term} state=${state}`);
}

function main(): void {
  const config = parseClusterConfig(process.env);

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
      // Fire-and-forget: runElection is async, but the timer callback
      // itself must stay synchronous. Errors are caught and logged
      // rather than crashing the process — an unreachable peer during
      // an election is expected/handled inside runElection itself, but
      // this guards against any unexpected rejection (e.g. a thrown
      // error from fetch in an unusual environment).
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

  const app = createRaftApp(node);
  const server = http.createServer(app);

  // Bind 0.0.0.0, not localhost: these are separate OS processes (in the
  // Step 5b integration test, and eventually separate machines/containers
  // per Milestone 8), so the listen address must be reachable from
  // sibling processes, not just loopback-restricted.
  server.listen(config.port, "0.0.0.0", () => {
    console.log(
      `[${config.nodeId}] listening on 0.0.0.0:${config.port}, peers: ${config.peers
        .map((p) => `${p.id}=${p.host}:${p.port}`)
        .join(", ")}`,
    );
  });

  function shutdown(): void {
    node.stop();
    server.close(() => {
      process.exit(0);
    });
  }

  process.on("SIGINT", shutdown);
  process.on("SIGTERM", shutdown);
}

main();