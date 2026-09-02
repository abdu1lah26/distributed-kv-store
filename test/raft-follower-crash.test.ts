// Integration test: Section 6 item #2 — follower crash + rejoin.
// Top-level /test, not src/test/, same reasoning as raft-election.test.ts
// and raft-write-path.test.ts (spans multiple real processes).
//
// Run via: npx tsx test/raft-follower-crash.test.ts
//
// Scope (DESIGN.md §6.2): "Follower crash + rejoin → catches up via
// AppendEntries, log converges." This test kills ONE follower (never
// the leader), writes more data while it's down, restarts it as a
// fresh process against the same ports/config, and confirms it
// eventually converges with the rest of the cluster.
//
// IMPORTANT — read before trusting this test as evidence of anything
// beyond what it actually proves:
//
// RaftLog (src/raft/log.ts) is entirely in-memory (DESIGN.md §5.3.2,
// a known, tracked, NOT-yet-resolved gap). The restarted node in this
// test comes back with a completely EMPTY log — nothing is replayed
// from disk, because nothing was ever written to disk. This test is
// expected to PASS, but via pure replication: the leader's existing
// §5.3.3 AppendEntries consistency check treats the restarted node
// exactly like an ordinary far-behind follower, walks nextIndex back
// to 1, and streams every entry forward from scratch.
//
// That is CONVERGENCE VIA REPLICATION, not recovery via persistence.
// A passing result here is NOT evidence that crash recovery/persistence
// works — it's evidence that replication-driven catch-up works, which
// happens to produce the same observable end state. Section 6 item #4
// (process crash + restart, WAL recovery) is the test that actually
// exercises persistence, and is expected to expose §5.3.2's gap
// honestly rather than paper over it.
//
// EXPLICITLY OUT OF SCOPE for this test: partition behavior, leader
// crash, concurrent writes, and anything about how long catch-up takes
// beyond "eventually" via polling.

import { spawn, ChildProcess } from "node:child_process";
import { createRequire } from "node:module";
import path from "node:path";
import { fileURLToPath } from "node:url";

const require = createRequire(import.meta.url);
const __dirname = path.dirname(fileURLToPath(import.meta.url));

let passed = 0;
let failed = 0;

function assert(condition: boolean, message: string): void {
  if (condition) {
    passed++;
  } else {
    failed++;
    console.error(`FAIL: ${message}`);
  }
}

// ---- Cross-platform process spawn (§4.4.5 pattern, mirrored from raft-election.test.ts) ----

const tsxCliPath = require.resolve("tsx/cli");
const mainTsPath = path.join(__dirname, "..", "src", "raft", "main.ts");

interface NodeProcess {
  nodeId: string;
  port: number;
  clientPort: number;
  peers: string;
  child: ChildProcess;
  stdoutBuffer: string;
}

function spawnNode(nodeId: string, port: number, clientPort: number, peers: string): NodeProcess {
  const child = spawn(process.execPath, [tsxCliPath, mainTsPath], {
    env: {
      ...process.env,
      NODE_ID: nodeId,
      PORT: String(port),
      CLIENT_PORT: String(clientPort),
      PEERS: peers,
    },
    stdio: ["ignore", "pipe", "pipe"],
    detached: process.platform !== "win32",
  });

  const proc: NodeProcess = { nodeId, port, clientPort, peers, child, stdoutBuffer: "" };

  child.stdout?.on("data", (chunk: Buffer) => {
    proc.stdoutBuffer += chunk.toString();
  });
  child.stderr?.on("data", (chunk: Buffer) => {
    process.stderr.write(`[${nodeId} stderr] ${chunk.toString()}`);
  });

  return proc;
}

/**
 * Restarting a crashed node is NOT a special operation — it's just
 * spawning a fresh process with the same identity/config, same as any
 * other node start. Deliberately not doing anything extra here (no
 * "recovery mode" flag, no pre-seeding) — that absence is the point:
 * whatever this node comes back with is exactly what main.ts + an empty
 * in-memory RaftLog naturally produce on their own.
 */
function restartNode(proc: NodeProcess): NodeProcess {
  return spawnNode(proc.nodeId, proc.port, proc.clientPort, proc.peers);
}

async function killNode(proc: NodeProcess): Promise<void> {
  const pid = proc.child.pid;
  if (pid === undefined) {
    return;
  }

  const exited = new Promise<void>((resolve) => {
    proc.child.once("exit", () => resolve());
  });

  if (process.platform === "win32") {
    spawn("taskkill", ["/pid", String(pid), "/T", "/F"]);
  } else {
    try {
      process.kill(-pid, "SIGKILL");
    } catch {
      // Process may have already exited.
    }
  }

  await Promise.race([exited, new Promise((resolve) => setTimeout(resolve, 3000))]);
}

// ---- Stdout polling for leader election (mirrored from raft-election.test.ts) ----

const LEADER_LINE = /state=leader/;

function nodeIdFromLeaderLine(buffer: string, candidateIds: string[]): string | null {
  const lines = buffer.split("\n");
  for (const line of lines) {
    if (LEADER_LINE.test(line)) {
      const match = candidateIds.find((id) => line.startsWith(`[${id}]`));
      if (match) {
        return match;
      }
    }
  }
  return null;
}

async function waitForLeader(nodes: NodeProcess[], timeoutMs: number): Promise<NodeProcess | null> {
  const candidateIds = nodes.map((n) => n.nodeId);
  const deadline = Date.now() + timeoutMs;

  while (Date.now() < deadline) {
    for (const node of nodes) {
      const leaderId = nodeIdFromLeaderLine(node.stdoutBuffer, candidateIds);
      if (leaderId !== null) {
        return nodes.find((n) => n.nodeId === leaderId) ?? null;
      }
    }
    await new Promise((resolve) => setTimeout(resolve, 50));
  }

  return null;
}

// ---- Generic condition polling (mirrored from raft-write-path.test.ts) ----

async function pollUntil(
  predicate: () => Promise<boolean>,
  timeoutMs: number,
  intervalMs: number = 50,
): Promise<boolean> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    // A predicate built on fetch() (as every use in this file is) can
    // THROW rather than resolve false when the target isn't listening
    // yet — e.g. ECONNREFUSED against a just-restarted node's client
    // port before its server has bound. That's still "not ready yet,"
    // not a real failure, so it's treated the same as a false result:
    // swallowed here and retried, not left to crash the whole test.
    // Only the timeout is a genuine failure signal.
    let result: boolean;
    try {
      result = await predicate();
    } catch {
      result = false;
    }
    if (result) {
      return true;
    }
    await new Promise((resolve) => setTimeout(resolve, intervalMs));
  }
  return false;
}

function toRecordOrNull(value: unknown): Record<string, unknown> | null {
  if (typeof value === "object" && value !== null && !Array.isArray(value)) {
    return value as Record<string, unknown>;
  }
  return null;
}

async function parseJsonBody(res: Response): Promise<Record<string, unknown> | null> {
  let parsed: unknown;
  try {
    parsed = await res.json();
  } catch {
    return null;
  }
  return toRecordOrNull(parsed);
}

interface KvResponse {
  status: number;
  body: Record<string, unknown> | null;
}

async function getKv(clientPort: number, key: string): Promise<KvResponse> {
  const res = await fetch(`http://localhost:${clientPort}/kv/${key}`);
  const body = await parseJsonBody(res);
  return { status: res.status, body };
}

async function putKv(clientPort: number, key: string, value: string): Promise<KvResponse> {
  const res = await fetch(`http://localhost:${clientPort}/kv/${key}`, {
    method: "PUT",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ value }),
  });
  const body = await parseJsonBody(res);
  return { status: res.status, body };
}

// ---- Test ----

async function main(): Promise<void> {
  // Distinct port range from both existing integration test files
  // (5101-5103/5201-5203 and 5301-5303/5401-5403), so all three could
  // in principle run concurrently without colliding.
  const PORT_A = 5501;
  const PORT_B = 5502;
  const PORT_C = 5503;
  const CLIENT_PORT_A = 5601;
  const CLIENT_PORT_B = 5602;
  const CLIENT_PORT_C = 5603;

  const peersFor = (self: "a" | "b" | "c"): string => {
    const all: Record<"a" | "b" | "c", string> = {
      a: `node-a=${PORT_A}:${CLIENT_PORT_A}`,
      b: `node-b=${PORT_B}:${CLIENT_PORT_B}`,
      c: `node-c=${PORT_C}:${CLIENT_PORT_C}`,
    };
    return Object.entries(all)
      .filter(([id]) => id !== self)
      .map(([, entry]) => entry)
      .join(",");
  };

  let nodeA = spawnNode("node-a", PORT_A, CLIENT_PORT_A, peersFor("a"));
  let nodeB = spawnNode("node-b", PORT_B, CLIENT_PORT_B, peersFor("b"));
  let nodeC = spawnNode("node-c", PORT_C, CLIENT_PORT_C, peersFor("c"));
  let allNodes = [nodeA, nodeB, nodeC];

  try {
    // ---- Setup: wait for a real leader ----
    const leader = await waitForLeader(allNodes, 15_000);
    assert(leader !== null, "a leader emerges among the 3 nodes within 15s");
    if (leader === null) {
      return;
    }

    const followers = allNodes.filter((n) => n.nodeId !== leader.nodeId);
    const doomedFollower = followers[0]!;
    const survivingFollower = followers[1]!;

    // Mark stdout offsets now so later "did anyone newly claim
    // leadership" checks are scoped to output produced from this point
    // forward, not stale state=leader lines already in the buffer.
    const postSetupOffsets = new Map(allNodes.map((n) => [n.nodeId, n.stdoutBuffer.length]));

    // ---- Baseline: 3 keys written and converged before anything crashes ----
    const preKillWrites: Array<[string, string]> = [
      ["k1", "v1"],
      ["k2", "v2"],
      ["k3", "v3"],
    ];
    for (const [key, value] of preKillWrites) {
      const res = await putKv(leader.clientPort, key, value);
      assert(res.status === 200, `pre-kill PUT /kv/${key} returns 200`);
    }
    for (const node of allNodes) {
      for (const [key, value] of preKillWrites) {
        const caughtUp = await pollUntil(async () => {
          const res = await getKv(node.clientPort, key);
          return res.status === 200 && res.body?.value === value;
        }, 3000);
        assert(caughtUp, `${node.nodeId} converges on pre-kill key ${key} before the crash`);
      }
    }

    // ---- Kill one follower (never the leader) ----
    await killNode(doomedFollower);

    // ---- While it's down: cluster still makes progress (majority intact) ----
    const duringOutageWrites: Array<[string, string]> = [
      ["k4", "v4"],
      ["k5", "v5"],
    ];
    for (const [key, value] of duringOutageWrites) {
      const res = await putKv(leader.clientPort, key, value);
      assert(res.status === 200, `PUT /kv/${key} while a follower is down still returns 200 (majority intact)`);
    }

    const leaderGetDuringOutage = await getKv(leader.clientPort, "k4");
    assert(leaderGetDuringOutage.body?.value === "v4", "leader reflects its own write while a follower is down");

    const survivingCaughtUpDuringOutage = await pollUntil(async () => {
      const res = await getKv(survivingFollower.clientPort, "k5");
      return res.status === 200 && res.body?.value === "v5";
    }, 3000);
    assert(
      survivingCaughtUpDuringOutage,
      "the surviving follower reflects writes made while the other follower is down",
    );

    // ---- Leadership stability: no election was triggered by a follower dying ----
    const leaderStillClaimsLeader = LEADER_LINE.test(
      leader.stdoutBuffer.trim().split("\n").slice(-1)[0] ?? "",
    );
    assert(leaderStillClaimsLeader, "the original leader still reports state=leader after the follower kill");

    const survivorLoggedNewLeadership = nodeIdFromLeaderLine(
      survivingFollower.stdoutBuffer.slice(postSetupOffsets.get(survivingFollower.nodeId) ?? 0),
      [survivingFollower.nodeId],
    );
    assert(
      survivorLoggedNewLeadership === null,
      "the surviving follower never claims leadership itself (no election was triggered)",
    );

    // ---- Restart the crashed follower as a fresh process, same identity ----
    const restarted = restartNode(doomedFollower);
    allNodes = [leader, restarted, survivingFollower];

    // ---- It eventually catches up on EVERYTHING, including writes it missed ----
    const allWrites = [...preKillWrites, ...duringOutageWrites];
    for (const [key, value] of allWrites) {
      const caughtUp = await pollUntil(async () => {
        const res = await getKv(restarted.clientPort, key);
        return res.status === 200 && res.body?.value === value;
      }, 5000);
      assert(caughtUp, `restarted node ${restarted.nodeId} eventually reflects ${key}=${value} after rejoining`);
    }

    // ---- Final convergence check across all 3 nodes for every key written ----
    for (const node of allNodes) {
      for (const [key, value] of allWrites) {
        const res = await getKv(node.clientPort, key);
        assert(
          res.status === 200 && res.body?.value === value,
          `final state: ${node.nodeId} agrees on ${key}=${value}`,
        );
      }
    }

    // ---- Leadership is still stable after the rejoin too ----
    const finalLeaderCount = allNodes.filter((n) => {
      const lines = n.stdoutBuffer.trim().split("\n");
      const lastLine = lines[lines.length - 1] ?? "";
      return LEADER_LINE.test(lastLine);
    }).length;
    assert(finalLeaderCount === 1, `exactly one node reports state=leader at the end (found ${finalLeaderCount})`);
  } finally {
    await Promise.all(allNodes.map((n) => killNode(n)));
  }
}

main()
  .then(() => {
    console.log(`\n${passed} passed, ${failed} failed`);
    if (failed > 0) {
      process.exit(1);
    }
  })
  .catch((err) => {
    console.error("Integration test crashed:", err);
    process.exit(1);
  });