// Integration test: 3-node Raft cluster leader election (Milestone 4,
// Step 5b). Top-level /test, not src/test/, since this spans multiple
// real processes rather than testing one module in isolation (§9 split).
//
// Run via: npx tsx test/raft-election.test.ts
//
// Spawns 3 real src/raft/main.ts processes with real ports, waits for
// exactly one to report state=leader via stdout polling, kills it, and
// confirms a different node becomes the new leader within a bounded
// time. Process spawn/kill follows the established §4.4.5 pattern:
// resolve tsx's CLI directly via require.resolve (avoiding an orphaned
// child under an npx wrapper), and platform-branch the kill itself
// (Windows has no real process groups/signals).
//
// Per the person's observation while manually testing main.ts: an
// isolated node churns through candidate/term-bump forever (never
// reaching a majority alone), and does so fast (~150-300ms per cycle).
// This means:
//   1. All 3 processes are spawned together, back-to-back, not staggered,
//      so term numbers don't drift wildly apart before election resolves.
//   2. Stdout polling specifically watches for a "state=leader" line and
//      tolerates/ignores heavy candidate-churn noise beforehand, rather
//      than treating any state-change line as the signal.

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

// ---- Cross-platform process spawn (§4.4.5 pattern) ----

const tsxCliPath = require.resolve("tsx/cli");
const mainTsPath = path.join(__dirname, "..", "src", "raft", "main.ts");

interface NodeProcess {
  nodeId: string;
  port: number;
  child: ChildProcess;
  stdoutBuffer: string;
}

function spawnNode(nodeId: string, port: number, peers: string): NodeProcess {
  // Spawn tsx's CLI directly via `node`, not `npx tsx` — avoids the
  // npx-wrapper orphan-process problem documented in §4.4.5.
  const child = spawn(process.execPath, [tsxCliPath, mainTsPath], {
    env: {
      ...process.env,
      NODE_ID: nodeId,
      PORT: String(port),
      PEERS: peers,
    },
    stdio: ["ignore", "pipe", "pipe"],
    // POSIX: detached so we can kill the whole process group later.
    // No effect on Windows, where taskkill /T handles the tree instead.
    detached: process.platform !== "win32",
  });

  const proc: NodeProcess = { nodeId, port, child, stdoutBuffer: "" };

  child.stdout?.on("data", (chunk: Buffer) => {
    proc.stdoutBuffer += chunk.toString();
  });
  child.stderr?.on("data", (chunk: Buffer) => {
    // Surface unexpected stderr for debugging, but don't fail the test
    // on it directly — assertions below are the source of truth.
    process.stderr.write(`[${nodeId} stderr] ${chunk.toString()}`);
  });

  return proc;
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
      process.kill(-pid, "SIGKILL"); // negative pid = whole process group
    } catch {
      // Process may have already exited.
    }
  }

  await Promise.race([exited, new Promise((resolve) => setTimeout(resolve, 3000))]);
}

// ---- Stdout polling helpers ----

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

/**
 * Polls the given nodes' combined stdout until exactly one of the
 * provided nodeIds has logged a state=leader line, or the timeout
 * elapses. Deliberately does NOT stop early on any other state change
 * (candidate churn is expected and ignored, per the person's finding).
 */
async function waitForLeader(
  nodes: NodeProcess[],
  timeoutMs: number,
): Promise<string | null> {
  const candidateIds = nodes.map((n) => n.nodeId);
  const deadline = Date.now() + timeoutMs;

  while (Date.now() < deadline) {
    for (const node of nodes) {
      const leaderId = nodeIdFromLeaderLine(node.stdoutBuffer, candidateIds);
      if (leaderId !== null) {
        return leaderId;
      }
    }
    await new Promise((resolve) => setTimeout(resolve, 50));
  }

  return null;
}

// ---- Test ----

async function main(): Promise<void> {
  const PORT_A = 5101;
  const PORT_B = 5102;
  const PORT_C = 5103;

  const peersFor = (self: "a" | "b" | "c"): string => {
    const all: Record<"a" | "b" | "c", string> = {
      a: `node-a=${PORT_A}`,
      b: `node-b=${PORT_B}`,
      c: `node-c=${PORT_C}`,
    };
    return Object.entries(all)
      .filter(([id]) => id !== self)
      .map(([, entry]) => entry)
      .join(",");
  };

  // Spawn all 3 back-to-back, not staggered, per the person's finding —
  // no awaiting/delay between spawns.
  const nodeA = spawnNode("node-a", PORT_A, peersFor("a"));
  const nodeB = spawnNode("node-b", PORT_B, peersFor("b"));
  const nodeC = spawnNode("node-c", PORT_C, peersFor("c"));
  const allNodes = [nodeA, nodeB, nodeC];

  try {
    // ---- Phase 1: exactly one leader emerges ----
    const firstLeaderId = await waitForLeader(allNodes, 15_000);
    assert(firstLeaderId !== null, "a leader emerges among the 3 nodes within 15s");

    if (firstLeaderId === null) {
      // Can't meaningfully continue without a leader — bail out of the
      // rest of this scenario, but still fall through to cleanup below.
      return;
    }

    // Give a brief settle window, then confirm exactly one node reports
    // state=leader (not zero, not more than one) — checked by scanning
    // each node's most recent state, not just "did a leader line ever
    // appear" (a node could have been leader transiently during a
    // split-vote scenario in theory; we want the *current* state).
    await new Promise((resolve) => setTimeout(resolve, 500));

    const leaderCount = allNodes.filter((n) => {
      const lines = n.stdoutBuffer.trim().split("\n");
      const lastLine = lines[lines.length - 1] ?? "";
      return LEADER_LINE.test(lastLine);
    }).length;

    assert(leaderCount === 1, `exactly one node currently reports state=leader (found ${leaderCount})`);

    const leaderNode = allNodes.find((n) => n.nodeId === firstLeaderId);
    assert(leaderNode !== undefined, "the identified leader corresponds to one of the spawned nodes");
    if (leaderNode === undefined) {
      return;
    }

    // ---- Phase 2: kill the leader, verify re-election ----
    await killNode(leaderNode);

    const survivors = allNodes.filter((n) => n.nodeId !== leaderNode.nodeId);
    // Only look for a leader line logged AFTER the kill, since the old
    // leader's earlier state=leader line is still sitting in a survivor's
    // buffer only if it echoed it — survivors log their own state, not
    // the old leader's, so this is naturally scoped to post-kill lines.
    // Mark the buffer length at kill time per survivor so we only scan
    // new output going forward.
    const bufferOffsets = new Map(survivors.map((n) => [n.nodeId, n.stdoutBuffer.length]));

    const newLeaderId = await pollForNewLeader(survivors, bufferOffsets, 15_000);

    assert(newLeaderId !== null, "a new leader is elected among the 2 survivors within 15s of the old leader's death");
    assert(
      newLeaderId !== leaderNode.nodeId,
      "the new leader is a different node than the one that was killed",
    );
  } finally {
    await Promise.all(allNodes.map((n) => killNode(n)));
  }
}

async function pollForNewLeader(
  survivors: NodeProcess[],
  bufferOffsets: Map<string, number>,
  timeoutMs: number,
): Promise<string | null> {
  const candidateIds = survivors.map((n) => n.nodeId);
  const deadline = Date.now() + timeoutMs;

  while (Date.now() < deadline) {
    for (const node of survivors) {
      const offset = bufferOffsets.get(node.nodeId) ?? 0;
      const newOutput = node.stdoutBuffer.slice(offset);
      const leaderId = nodeIdFromLeaderLine(newOutput, candidateIds);
      if (leaderId !== null) {
        return leaderId;
      }
    }
    await new Promise((resolve) => setTimeout(resolve, 50));
  }

  return null;
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