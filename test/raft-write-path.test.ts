// Integration test: real 3-node Raft cluster client write path
// (Milestone 5, Step 6.7). Top-level /test, not src/test/, since this
// spans multiple real processes rather than testing one module in
// isolation (§9 split) — same reasoning as test/raft-election.test.ts,
// whose spawn/kill helpers this file mirrors.
//
// Run via: npx tsx test/raft-write-path.test.ts
//
// Scope: proves the fully-wired happy path works end to end against
// real spawned processes and real HTTP — PUT via the leader commits and
// is immediately readable there, followers eventually reflect it (real
// eventual consistency, not assumed-instant), a follower's PUT returns
// 421 with a leaderId/leaderHint that is not just correctly SHAPED but
// genuinely ACTIONABLE (the hint is actually followed and used, not
// just inspected), DELETE propagates as a real tombstone, and multiple
// sequential writes land correctly and in order across the whole
// cluster's final state.
//
// EXPLICITLY OUT OF SCOPE: no failure injection (leader crash mid-write,
// partitions, etc.) — that is Milestone 7's job (DESIGN.md Section 7
// item 7, running the Section 6 failure-model tests). This step's job
// is narrowly "is the happy path fully wired," nothing more.

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

  const proc: NodeProcess = { nodeId, port, clientPort, child, stdoutBuffer: "" };

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

// ---- Generic condition polling, for real eventual-consistency waits ----

/**
 * Polls the given async predicate until it resolves true or timeoutMs
 * elapses. Used throughout below for "eventually the followers reflect
 * this write" style assertions — deliberately NOT assuming replication
 * is instant, since it genuinely isn't (real network calls + the apply
 * loop's real poll interval).
 */
async function pollUntil(
  predicate: () => Promise<boolean>,
  timeoutMs: number,
  intervalMs: number = 50,
): Promise<boolean> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    // A predicate built on fetch() (as every use in this file is) can
    // THROW rather than resolve false when the target isn't listening
    // yet (e.g. ECONNREFUSED). Found as a real bug while building
    // raft-follower-crash.test.ts, which restarts a node mid-test and
    // hit this directly. This file's current predicates only ever
    // target already-up nodes, so it hasn't been hit here yet — but
    // that was incidental, not a guaranteed property of this helper,
    // so the same fix applies here for consistency: swallowed and
    // retried, not left to crash the whole test. Only the timeout is a
    // genuine failure signal.
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

/**
 * Real runtime type guard, not a bare cast: confirms the parsed JSON is
 * actually a plain object (not an array, not null, not a primitive)
 * before narrowing to Record<string, unknown>. res.json() resolves to
 * `unknown` in this project's fetch typings, and neither a length check
 * nor a try/catch on its own lets TypeScript narrow that automatically —
 * same category as config.ts's segments[0] narrowing gap — so this
 * performs (and states) the actual check the narrowing depends on,
 * rather than silencing the compiler without verifying anything.
 */
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

async function deleteKv(clientPort: number, key: string): Promise<KvResponse> {
  const res = await fetch(`http://localhost:${clientPort}/kv/${key}`, { method: "DELETE" });
  const body = await parseJsonBody(res);
  return { status: res.status, body };
}

// ---- Test ----

async function main(): Promise<void> {
  // Distinct port range from raft-election.test.ts's 5101-5103/5201-5203,
  // so both integration test files could in principle run concurrently
  // without colliding.
  const PORT_A = 5301;
  const PORT_B = 5302;
  const PORT_C = 5303;
  const CLIENT_PORT_A = 5401;
  const CLIENT_PORT_B = 5402;
  const CLIENT_PORT_C = 5403;

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
    const followers = allNodes.filter((n) => n.nodeId !== leader.nodeId);

    // ---- PUT via the leader commits and is immediately readable there ----
    const putRes = await putKv(leader.clientPort, "foo", "bar");
    assert(putRes.status === 200, `PUT on the leader returns 200 (got ${putRes.status})`);
    assert(
      JSON.stringify(putRes.body) === JSON.stringify({ key: "foo", value: "bar" }),
      `PUT response body matches {key,value} (got ${JSON.stringify(putRes.body)})`,
    );

    const leaderGet = await getKv(leader.clientPort, "foo");
    assert(leaderGet.status === 200, "GET on the leader immediately after its own committed write returns 200");
    assert(leaderGet.body?.value === "bar", "the leader's own GET reflects its own write immediately");

    // ---- Followers EVENTUALLY reflect the write (real, not assumed-instant) ----
    for (const follower of followers) {
      const caughtUp = await pollUntil(async () => {
        const res = await getKv(follower.clientPort, "foo");
        return res.status === 200 && res.body?.value === "bar";
      }, 3000);
      assert(caughtUp, `follower ${follower.nodeId} eventually reflects the leader's write within 3s`);
    }

    // ---- A follower's PUT returns 421 with a real, correct leaderId/leaderHint ----
    const someFollower = followers[0]!;
    const followerPut = await putKv(someFollower.clientPort, "foo", "should-not-land");
    assert(followerPut.status === 421, `PUT on a follower (${someFollower.nodeId}) returns 421`);
    assert(followerPut.body?.error === "NOT_LEADER", "the 421 body has error: NOT_LEADER");
    assert(
      followerPut.body?.leaderId === leader.nodeId,
      `the 421 body's leaderId (${followerPut.body?.leaderId}) matches the real leader (${leader.nodeId})`,
    );
    const leaderHint: string | undefined =
      typeof followerPut.body?.leaderHint === "string" ? followerPut.body.leaderHint : undefined;
    assert(
      typeof leaderHint === "string" && leaderHint === `http://localhost:${leader.clientPort}`,
      `the 421 body's leaderHint (${leaderHint}) points at the real leader's real client port`,
    );

    // Confirm the rejected follower write never actually landed anywhere.
    const stillBar = await getKv(leader.clientPort, "foo");
    assert(stillBar.body?.value === "bar", "the follower's rejected PUT did not overwrite the real value on the leader");

    // ---- EXPLICITLY REQUIRED: the leaderHint is genuinely ACTIONABLE, ----
    // ---- not just correctly formatted — actually follow it and use it ----
    if (typeof leaderHint === "string") {
      const hintPutRes = await fetch(`${leaderHint}/kv/foo`, {
        method: "PUT",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ value: "via-hint" }),
      });
      const hintPutBody = await parseJsonBody(hintPutRes);
      assert(
        hintPutRes.status === 200,
        `following the leaderHint URL and PUTting against it succeeds with 200 (got ${hintPutRes.status})`,
      );
      assert(
        hintPutBody?.value === "via-hint",
        `the write made by following the leaderHint genuinely lands (got ${JSON.stringify(hintPutBody)})`,
      );

      const leaderGetAfterHint = await getKv(leader.clientPort, "foo");
      assert(
        leaderGetAfterHint.body?.value === "via-hint",
        "the leader's own GET reflects the write that was made by following the leaderHint",
      );

      // And it replicates onward from there too, same as any other write.
      for (const follower of followers) {
        const caughtUp = await pollUntil(async () => {
          const res = await getKv(follower.clientPort, "foo");
          return res.status === 200 && res.body?.value === "via-hint";
        }, 3000);
        assert(caughtUp, `follower ${follower.nodeId} eventually reflects the write made via the leaderHint`);
      }
    }

    // ---- DELETE via the leader propagates as a real tombstone ----
    const delRes = await deleteKv(leader.clientPort, "foo");
    assert(delRes.status === 200, `DELETE on the leader returns 200 (got ${delRes.status})`);
    assert(
      JSON.stringify(delRes.body) === JSON.stringify({ key: "foo", deleted: true }),
      `DELETE response body matches {key, deleted:true} (got ${JSON.stringify(delRes.body)})`,
    );

    const leaderGetAfterDelete = await getKv(leader.clientPort, "foo");
    assert(leaderGetAfterDelete.status === 404, "GET on the leader immediately after its own DELETE returns 404");

    for (const follower of followers) {
      const tombstoneCaughtUp = await pollUntil(async () => {
        const res = await getKv(follower.clientPort, "foo");
        return res.status === 404;
      }, 3000);
      assert(tombstoneCaughtUp, `follower ${follower.nodeId} eventually reflects the tombstone (404) within 3s`);
    }

    // ---- Multiple sequential writes land correctly, in order, across the whole cluster ----
    const seedWrites: Array<[string, string]> = [
      ["alpha", "1"],
      ["beta", "2"],
      ["gamma", "3"],
    ];
    for (const [key, value] of seedWrites) {
      const res = await putKv(leader.clientPort, key, value);
      assert(res.status === 200, `sequential write PUT /kv/${key} returns 200`);
    }
    // Overwrite one of them, to confirm the FINAL state (not just "was
    // written at some point") is what propagates.
    const overwriteRes = await putKv(leader.clientPort, "alpha", "1-updated");
    assert(overwriteRes.status === 200, "overwriting one of the sequential writes returns 200");

    for (const node of allNodes) {
      const alphaCaughtUp = await pollUntil(async () => {
        const res = await getKv(node.clientPort, "alpha");
        return res.status === 200 && res.body?.value === "1-updated";
      }, 3000);
      assert(alphaCaughtUp, `${node.nodeId} eventually reflects the OVERWRITTEN value for alpha, not the original`);

      const betaCaughtUp = await pollUntil(async () => {
        const res = await getKv(node.clientPort, "beta");
        return res.status === 200 && res.body?.value === "2";
      }, 3000);
      assert(betaCaughtUp, `${node.nodeId} eventually reflects beta`);

      const gammaCaughtUp = await pollUntil(async () => {
        const res = await getKv(node.clientPort, "gamma");
        return res.status === 200 && res.body?.value === "3";
      }, 3000);
      assert(gammaCaughtUp, `${node.nodeId} eventually reflects gamma`);
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