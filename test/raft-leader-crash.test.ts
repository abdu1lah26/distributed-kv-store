// Integration test: Section 6 item #1 — leader crash mid-write.
// Top-level /test, same tier as the other multi-process integration
// tests.
//
// Run via: npx tsx test/raft-leader-crash.test.ts
//
// SCOPE (DESIGN.md §6.1): "Leader crash mid-write → new leader elected,
// no acknowledged write lost."
//
// THE ACTUAL INVARIANT UNDER TEST, STATED PRECISELY:
//
//   If the client received 200 for a write, that write MUST survive —
//   regardless of exactly when the leader died relative to it.
//
//   Writes that were NOT acknowledged (client saw an error, a dropped
//   connection, or nothing at all) carry NO guarantee either way. This
//   is the correct, standard distributed-systems contract, not a gap —
//   real clients handle this bucket via idempotent retry/dedup, which
//   is explicitly out of scope here. This test must never assert
//   presence OR absence for that bucket.
//
// WHY THIS IS EXPECTED TO HOLD, NOT JUST HOPED: proposeWrite (Step 6.4)
// only returns 200 after confirming real majority commit. A majority of
// 3 always includes at least one follower besides the leader — so if
// the leader dies immediately after acking, that follower already has
// the entry, and Raft's own election-safety rule (§5.3.1, already
// implemented and unit-tested) forbids electing a new leader that
// doesn't have it. This test is a real, end-to-end, concurrent-load,
// real-process-death proof of a guarantee already established in
// isolation by unit tests — valuable because of the conditions it runs
// under, not because the outcome is genuinely in doubt.
//
// LESSON CARRIED FORWARD FROM test/raft-full-crash.test.ts (§6.2):
// "fire without awaiting, then kill" does NOT automatically create a
// real race — that file's first attempt at test #4 fired a single write
// and killed immediately after, and the write won every time (all 3
// runs), because local commit (low single-digit ms) is faster than
// killNode()'s own real wall-clock cost. Test #4 solved that by
// removing the race entirely, which is the right fix THERE because a
// guaranteed outcome was the point. Here the opposite is true — a real
// race is the ENTIRE point (the safety property must hold on both sides
// of it), so the race can't be removed, only exercised as best as a
// black-box test can manage.
//
// FULL TUNING INVESTIGATION (documented in full, since every attempt
// here — including the ones that didn't work — is real evidence about
// this environment, not noise to discard):
//
//   1. BURST_SIZE=50, no delay, default dispatcher: ~1-2/50 acked.
//   2. BURST_SIZE=200, no delay, default dispatcher: 1/200 acked —
//      FEWER, not more, despite 4x the volume. This ruled out
//      "percentage of N completes" as the model; something closer to a
//      small, roughly fixed number of writes complete in the real-time
//      gap before the kill lands, regardless of queue depth.
//   3. BURST_SIZE=100, KILL_DELAY_MS=10 (a small real delay between
//      firing the burst and killing the leader — NOT "wait for it to
//      finish"), default dispatcher: 1/100 acked — essentially
//      unchanged. The delay did not meaningfully widen the window at
//      this magnitude.
//   4. BURST_SIZE=100, KILL_DELAY_MS=10, explicit high-concurrency
//      undici Agent as the fetch dispatcher (testing the hypothesis
//      that most of the burst was queued client-side by Node's default
//      per-host connection-pool cap and never even sent before the
//      kill): 0/100 acked, confirmed clean across 4 repeated runs, not
//      a fluke. This RULED OUT client-side connection pooling as the
//      bottleneck — raising client concurrency did not improve, and if
//      anything performed marginally worse than, the default-dispatcher
//      baseline. Tested and discarded via a separate, explicitly
//      throwaway diagnostic file (test/raft-leader-crash-diagnostic.
//      test.ts, deleted after use, not committed) so this real test
//      file was never carrying the extra dispatcher complexity or its
//      undici-vs-Node-fetch-types cast — see step 4's own investigation
//      for why that cast was needed and why it's not used here.
//
// CONCLUSION: neither burst volume, nor a small added delay, nor
// client-side connection concurrency meaningfully changes the outcome.
// The most likely honest explanation: on this machine, a single local
// write completes end-to-end (log append + 2-peer AppendEntries
// round-trip + majority + commit + apply) fast enough, and killNode()
// itself takes long enough (spawning taskkill on Windows / awaiting a
// real process exit either platform), that only a very small number of
// writes can ever be in a genuinely undecided state at the moment the
// kill actually lands — typically 0-2 out of a 100-write burst. This is
// accepted as an honest, reproducible property of this test
// environment, not a deficiency in the test or something to keep tuning
// around. Settled on: BURST_SIZE=100, no artificial delay, no special
// dispatcher — the simplest version that produces this environment's
// real, achievable sample.
//
// WHY THIS SMALL SAMPLE IS STILL SUFFICIENT: the safety property this
// test exists to check is verified BOTH empirically (whatever writes do
// get acked this run, each one checked for real survival after the
// crash) AND structurally (Raft's election-safety rule, §5.3.1,
// already implemented and unit-tested in Milestone 5, guarantees this
// can never fail by construction — see the invariant explanation
// above). This chaos test is confirmatory evidence under real
// concurrent load and real process death, not the sole source of
// confidence in the guarantee. A small honest sample each run,
// accumulated across repeated runs per this project's standing
// timing-sensitive-test discipline, is a reasonable bar given that.
//
// THE ACK/NO-ACK SPLIT IS A DIAGNOSTIC, NOT AN ASSERTION. What split N
// concurrent writes land at is environment-dependent (machine speed, OS
// scheduling, how fast killNode() actually delivers a kill this run) —
// forcing a specific split into pass/fail logic would create flakiness
// that has nothing to do with actual correctness. The ONLY hard
// assertion is the safety property itself: every acked write survives.
// The split is logged plainly so a human can see it, per the
// investigation above — not something this test's exit code judges.
//
// STANDING CONSTRAINTS, UNCHANGED: pure black-box via the client API
// only — no debug endpoints, no hooks added to writePath.ts or rpc.ts
// to make timing easier or more visible. Pure external timing, same as
// every other test in this project.
//
// EXPLICITLY OUT OF SCOPE: write ordering under concurrency (test #5's
// job), partition behavior (test #3's job).

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

// ---- Cross-platform process spawn (§4.4.5 pattern, mirrored from the other integration tests) ----

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

// ---- Stdout polling for leader election (mirrored from the other integration tests) ----

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

/** Same offset-scoped scan used in raft-election.test.ts / raft-follower-crash.test.ts, so a stale pre-kill leader line can't be mistaken for a new one. */
async function pollForLeaderAmong(
  nodes: NodeProcess[],
  bufferOffsets: Map<string, number>,
  timeoutMs: number,
): Promise<NodeProcess | null> {
  const candidateIds = nodes.map((n) => n.nodeId);
  const deadline = Date.now() + timeoutMs;

  while (Date.now() < deadline) {
    for (const node of nodes) {
      const offset = bufferOffsets.get(node.nodeId) ?? 0;
      const newOutput = node.stdoutBuffer.slice(offset);
      const leaderId = nodeIdFromLeaderLine(newOutput, candidateIds);
      if (leaderId !== null) {
        return nodes.find((n) => n.nodeId === leaderId) ?? null;
      }
    }
    await new Promise((resolve) => setTimeout(resolve, 50));
  }

  return null;
}

// ---- Generic condition polling (same ECONNREFUSED-safe fix as the other integration tests) ----

async function pollUntil(
  predicate: () => Promise<boolean>,
  timeoutMs: number,
  intervalMs: number = 50,
): Promise<boolean> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
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
  try {
    const res = await fetch(`http://localhost:${clientPort}/kv/${key}`);
    const body = await parseJsonBody(res);
    return { status: res.status, body };
  } catch {
    return { status: 0, body: null };
  }
}

async function putKv(clientPort: number, key: string, value: string): Promise<KvResponse> {
  try {
    const res = await fetch(`http://localhost:${clientPort}/kv/${key}`, {
      method: "PUT",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ value }),
    });
    const body = await parseJsonBody(res);
    return { status: res.status, body };
  } catch {
    // Connection dropped mid-request (e.g. the leader died while this
    // request was in flight) — this is a legitimate, expected outcome
    // for the burst below, NOT acked, handled the same as a non-200.
    return { status: 0, body: null };
  }
}

// ---- Test ----

async function main(): Promise<void> {
  const BURST_SIZE = 100;

  const PORT_A = 6101;
  const PORT_B = 6102;
  const PORT_C = 6103;
  const CLIENT_PORT_A = 6201;
  const CLIENT_PORT_B = 6202;
  const CLIENT_PORT_C = 6203;

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

  const nodeA = spawnNode("node-a", PORT_A, CLIENT_PORT_A, peersFor("a"));
  const nodeB = spawnNode("node-b", PORT_B, CLIENT_PORT_B, peersFor("b"));
  const nodeC = spawnNode("node-c", PORT_C, CLIENT_PORT_C, peersFor("c"));
  const allNodes = [nodeA, nodeB, nodeC];

  try {
    // ---- Setup: wait for a real leader ----
    const leader = await waitForLeader(allNodes, 15_000);
    assert(leader !== null, "a leader emerges among the 3 nodes within 15s");
    if (leader === null) {
      return;
    }

    // ---- Baseline: ordinary sequential writes converge before any chaos ----
    const baselineWrites: Array<[string, string]> = [
      ["c1", "vc1"],
      ["c2", "vc2"],
    ];
    for (const [key, value] of baselineWrites) {
      const res = await putKv(leader.clientPort, key, value);
      assert(res.status === 200, `baseline PUT /kv/${key} returns 200`);
    }
    for (const node of allNodes) {
      for (const [key, value] of baselineWrites) {
        const ok = await pollUntil(async () => {
          const r = await getKv(node.clientPort, key);
          return r.status === 200 && r.body?.value === value;
        }, 3000);
        assert(ok, `${node.nodeId} converges on baseline key ${key} before the crash`);
      }
    }

    const survivors = allNodes.filter((n) => n.nodeId !== leader.nodeId);
    const bufferOffsets = new Map(survivors.map((n) => [n.nodeId, n.stdoutBuffer.length]));

    // ---- Fire a concurrent burst of writes, unawaited, then kill the leader immediately ----
    // Each write gets a unique key (burst-000 .. burst-099, for
    // BURST_SIZE=100) so presence can be checked precisely per-write, and a value that records its
    // own index for an unambiguous equality check.
    const burstKeys = Array.from({ length: BURST_SIZE }, (_, i) => `burst-${String(i).padStart(3, "0")}`);
    const burstValue = (i: number): string => `v${i}`;

    const burstPromises = burstKeys.map((key, i) =>
      putKv(leader.clientPort, key, burstValue(i)).then((res) => ({
        key,
        value: burstValue(i),
        acked: res.status === 200,
      })),
    );

    // No artificial delay — kill fires immediately after kicking off
    // the burst. See this file's header comment for the full tuning
    // investigation: neither BURST_SIZE (50 -> 200) nor a small added
    // delay (KILL_DELAY_MS, tried up to 10ms) nor an explicit
    // high-concurrency undici dispatcher (ruling out client-side
    // connection-pool queueing as the bottleneck) meaningfully changed
    // the outcome. A typical run acks 0-2 of 100 — accepted as an
    // honest property of this test environment, not tuned around
    // further.
    await killNode(leader);

    const burstResults = await Promise.all(burstPromises);
    const acked = burstResults.filter((r) => r.acked);
    const notAcked = burstResults.filter((r) => !r.acked);

    // ---- Diagnostic only — NOT an assertion. See header comment. ----
    console.log(
      `\n[diagnostic] burst split: ${acked.length}/${BURST_SIZE} acked, ` +
        `${notAcked.length}/${BURST_SIZE} not acked (error/dropped/non-200). ` +
        `Typical/expected for this environment is 0-2 acked — see header comment.`,
    );
    if (acked.length === BURST_SIZE) {
      console.log(
        "[diagnostic] unusually high ack count for this run (all 100 acked) — worth a second look, " +
          "though the safety assertions below apply regardless.",
      );
    }

    // ---- New leader is elected among the 2 survivors ----
    const newLeader = await pollForLeaderAmong(survivors, bufferOffsets, 15_000);
    assert(newLeader !== null, "a new leader is elected among the 2 survivors within 15s of the crash");

    // ---- THE ACTUAL SAFETY PROPERTY: every acked write survives ----
    for (const { key, value } of acked) {
      const ok = await pollUntil(async () => {
        // Checked against the new leader when known (guaranteed
        // reachable, since it was just elected); falls back to any
        // survivor otherwise.
        const targetPort = newLeader?.clientPort ?? survivors[0]!.clientPort;
        const r = await getKv(targetPort, key);
        return r.status === 200 && r.body?.value === value;
      }, 5000);
      assert(ok, `SAFETY: acknowledged write ${key}=${value} survived the leader crash`);
    }

    // Also confirm full convergence across BOTH survivors for acked
    // writes, not just the new leader — same bar as every other test.
    for (const node of survivors) {
      for (const { key, value } of acked) {
        const ok = await pollUntil(async () => {
          const r = await getKv(node.clientPort, key);
          return r.status === 200 && r.body?.value === value;
        }, 5000);
        assert(ok, `SAFETY: ${node.nodeId} also converges on acknowledged write ${key}=${value}`);
      }
    }

    console.log(
      `[diagnostic] ${notAcked.length} non-acknowledged write(s) were deliberately NOT checked for ` +
        `presence or absence — no guarantee either way is the correct, expected contract.`,
    );

    // ---- Cluster is still fully functional post-recovery ----
    if (newLeader !== null) {
      const postRecoveryRes = await putKv(newLeader.clientPort, "post-recovery", "still-works");
      assert(postRecoveryRes.status === 200, "cluster accepts an ordinary write through the new leader post-recovery");

      for (const node of survivors) {
        const ok = await pollUntil(async () => {
          const r = await getKv(node.clientPort, "post-recovery");
          return r.status === 200 && r.body?.value === "still-works";
        }, 3000);
        assert(ok, `${node.nodeId} converges on the post-recovery write`);
      }
    }
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