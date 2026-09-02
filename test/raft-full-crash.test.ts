// Integration test: Section 6 item #4 — process crash + restart (WAL
// recovery). Top-level /test, same tier as the other multi-process
// integration tests.
//
// Run via: npx tsx test/raft-full-crash.test.ts
//
// WHY THIS IS A DIFFERENT TEST FROM raft-follower-crash.test.ts (#2),
// NOT A VARIANT OF IT:
//
// #2 proved a single crashed follower converges after restart — but
// that convergence came entirely from LIVE REPLICATION off a leader
// that never lost its own copy of the log. With only one node down at
// a time, a majority is always still alive holding the true log in
// memory, so replication-based catch-up is always available as a
// safety net. That's real and correct behavior, but it can never by
// itself distinguish "recovered via persistence" from "recovered via
// replication" — and DESIGN.md §5.3.2 is explicit that RaftLog has NO
// persistence at all. A test that never removes the replication safety
// net can't honestly claim to test §5.3.2.
//
// This file removes that safety net entirely in Part B: ALL 3 nodes
// are killed simultaneously, so there is no surviving in-memory copy
// of the Raft log anywhere. This is the honest test.
//
// TWO PARTS, DELIBERATELY KEPT SEPARATE:
//
//   Part A — partial crash (leader + one follower killed together,
//   both restarted). Genuinely new coverage vs #2: forces real
//   re-election under a down-to-1-node stress condition and a
//   simultaneous multi-node restart. Its PERSISTENCE conclusion is
//   still "recovers via replication" (the one survivor still holds the
//   full log) — this part is a sanity/stress check, not the honest
//   test.
//
//   Part B — full-cluster simultaneous crash. The real test. No
//   survivor exists, so nothing can be replicated from anywhere.
//
// PRECISE CLAIM UNDER TEST IN PART B — deliberately narrower than "all
// data is lost", because it isn't:
//
//   (a) Entries that were APPLIED to a node's local Store before the
//       crash DO survive restart — via that Store's own WAL (§4),
//       which is real, durable, and completely independent of the
//       Raft layer. This must be proven, not assumed, or this test
//       would undersell what already genuinely works.
//
//   (b) Entries that were, at the moment of the crash, either
//       committed-but-not-yet-applied OR appended-but-not-yet-committed
//       are gone, full stop — no node, at the Raft level OR the
//       storage level, has any record of them. This is §5.3.2's actual
//       gap, made concrete.
//
// To make (b) concrete rather than hoping ordinary timing happens to
// land there, this test does NOT rely on winning a race against local
// commit speed — an earlier version tried exactly that (fire the write
// unawaited, then immediately kill all 3 nodes together) and it failed
// consistently: on a healthy localhost cluster, append + replicate +
// commit + apply can complete in low single-digit milliseconds, faster
// than killNode() can even deliver and confirm a kill (see this file's
// Part B comments for the full account, kept in place rather than
// deleted, since a documented dead end is worth as much as a documented
// gap). Instead, this test REMOVES the possibility of commit entirely:
// both followers are killed and confirmed dead FIRST, so the leader
// structurally cannot reach majority for the next entry no matter how
// fast it is — deterministic, not probabilistic. The write is then
// fired against the now-isolated leader, given a short fixed window to
// append to its own in-memory log, and the leader is killed before its
// own 1000ms write-timeout. This is a black-box test (client API only,
// no debug endpoint, per standing project decision) — the guarantee
// comes from removing followers, not from timing the kill precisely.
//
// DATA DIRECTORY ISOLATION: main.ts's DATA_DIR defaults to
// ./data/<nodeId> (cwd-relative, NOT a fresh temp dir per run) — see
// this test's own investigation before writing it. That default is
// unsafe for this test's assertions (a stale directory from a prior
// run, or cross-contamination with raft-election.test.ts /
// raft-write-path.test.ts / raft-follower-crash.test.ts, none of which
// set DATA_DIR explicitly, would make the Store-WAL-survival assertion
// untrustworthy). This file allocates a dedicated mkdtemp'd directory
// per node, per part, held fixed across any restart within that part
// so "restart" genuinely means "same node, same disk", and cleans up
// afterward. NOTE: this same latent gap exists in the three earlier
// integration test files too (they've just never been sensitive to
// it) — tracked as a separate follow-up, not fixed here.
//
// TRACKED CLEANUP ITEM (not fixed here, not blocking this test):
// raft-election.test.ts, raft-write-path.test.ts, and
// raft-follower-crash.test.ts all spawn nodes without ever setting
// DATA_DIR, so they silently rely on main.ts's default
// (./data/<nodeId>, cwd-relative) rather than an isolated temp dir per
// run. This has been harmless so far because none of those three files
// make any assertion that depends on precise on-disk state across
// runs or across each other — but it means repeated/concurrent runs of
// those files can read stale data left behind by a previous run, and
// nothing currently prevents two of them from colliding on the same
// directory if ever run at the same time. Should eventually be
// retrofitted with the same mkdtemp-per-node pattern this file uses,
// the first time any test in this project needs to make a real
// assertion about on-disk state (this file is the first).
//
// EXPLICITLY OUT OF SCOPE: network partitions (test #3's job),
// concurrent-writes ordering (test #5's job).

import { spawn, ChildProcess } from "node:child_process";
import { createRequire } from "node:module";
import path from "node:path";
import os from "node:os";
import fs from "node:fs/promises";
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

// ---- Cross-platform process spawn (§4.4.5 pattern, mirrored from
// raft-election.test.ts / raft-follower-crash.test.ts) ----

const tsxCliPath = require.resolve("tsx/cli");
const mainTsPath = path.join(__dirname, "..", "src", "raft", "main.ts");

interface NodeProcess {
  nodeId: string;
  port: number;
  clientPort: number;
  peers: string;
  dataDir: string;
  child: ChildProcess;
  stdoutBuffer: string;
}

function spawnNode(
  nodeId: string,
  port: number,
  clientPort: number,
  peers: string,
  dataDir: string,
): NodeProcess {
  const child = spawn(process.execPath, [tsxCliPath, mainTsPath], {
    env: {
      ...process.env,
      NODE_ID: nodeId,
      PORT: String(port),
      CLIENT_PORT: String(clientPort),
      PEERS: peers,
      DATA_DIR: dataDir,
    },
    stdio: ["ignore", "pipe", "pipe"],
    detached: process.platform !== "win32",
  });

  const proc: NodeProcess = { nodeId, port, clientPort, peers, dataDir, child, stdoutBuffer: "" };

  child.stdout?.on("data", (chunk: Buffer) => {
    proc.stdoutBuffer += chunk.toString();
  });
  child.stderr?.on("data", (chunk: Buffer) => {
    process.stderr.write(`[${nodeId} stderr] ${chunk.toString()}`);
  });

  return proc;
}

/** Same node identity, same ports, same peers, same data dir — see #2's precedent. */
function restartNode(proc: NodeProcess): NodeProcess {
  return spawnNode(proc.nodeId, proc.port, proc.clientPort, proc.peers, proc.dataDir);
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

/** Confirms NO node among `nodes` logs state=leader within `windowMs` — used in Part B to show the cluster genuinely cannot proceed with only 1 of 3 alive. */
async function assertNoLeaderEmerges(nodes: NodeProcess[], windowMs: number): Promise<boolean> {
  const leader = await waitForLeader(nodes, windowMs);
  return leader === null;
}

// ---- Generic condition polling (mirrored + fixed per the earlier ECONNREFUSED bug) ----

async function pollUntil(
  predicate: () => Promise<boolean>,
  timeoutMs: number,
  intervalMs: number = 50,
): Promise<boolean> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    // Same fix as raft-follower-crash.test.ts / raft-write-path.test.ts:
    // a predicate built on fetch() can THROW (ECONNREFUSED) rather than
    // resolve false while a just-(re)started node isn't listening yet.
    // Treated as "not ready", not a crash.
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
    // Connection refused (node not up / already dead) — treated as
    // "definitely not found", which is the correct read for this
    // test's absence assertions in Part B.
    return { status: 0, body: null };
  }
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

// ---- Temp data directories, one per node, fixed across any restart within a part ----

async function makeDataDirs(prefix: string): Promise<Record<"a" | "b" | "c", string>> {
  const base = await fs.mkdtemp(path.join(os.tmpdir(), `${prefix}-`));
  const dirs: Record<"a" | "b" | "c", string> = {
    a: path.join(base, "node-a"),
    b: path.join(base, "node-b"),
    c: path.join(base, "node-c"),
  };
  // main.ts's Store.open() already creates its own subdirectories
  // (§4.4.8) — no need to pre-create these ourselves.
  return dirs;
}

async function cleanupDataDirs(dirs: Record<"a" | "b" | "c", string>): Promise<void> {
  const base = path.dirname(dirs.a);
  await fs.rm(base, { recursive: true, force: true }).catch(() => {
    // Best-effort cleanup — a leftover temp dir is not a test failure.
  });
}

// ---- Part A: partial crash (leader + one follower), both restarted ----

async function runPartA(): Promise<void> {
  console.log("\n=== Part A: partial crash (leader + one follower) ===");

  const PORT_A = 5701;
  const PORT_B = 5702;
  const PORT_C = 5703;
  const CLIENT_PORT_A = 5801;
  const CLIENT_PORT_B = 5802;
  const CLIENT_PORT_C = 5803;

  const dataDirs = await makeDataDirs("raft-full-crash-a");

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

  let nodeA = spawnNode("node-a", PORT_A, CLIENT_PORT_A, peersFor("a"), dataDirs.a);
  let nodeB = spawnNode("node-b", PORT_B, CLIENT_PORT_B, peersFor("b"), dataDirs.b);
  let nodeC = spawnNode("node-c", PORT_C, CLIENT_PORT_C, peersFor("c"), dataDirs.c);
  let allNodes = [nodeA, nodeB, nodeC];

  try {
    const leader = await waitForLeader(allNodes, 15_000);
    assert(leader !== null, "[A] a leader emerges among the 3 nodes within 15s");
    if (leader === null) {
      return;
    }

    const followers = allNodes.filter((n) => n.nodeId !== leader.nodeId);
    const doomedFollower = followers[0]!;
    const survivor = followers[1]!;

    const baselineWrites: Array<[string, string]> = [
      ["a1", "va1"],
      ["a2", "va2"],
    ];
    for (const [key, value] of baselineWrites) {
      const res = await putKv(leader.clientPort, key, value);
      assert(res.status === 200, `[A] baseline PUT /kv/${key} returns 200`);
    }
    for (const node of allNodes) {
      for (const [key, value] of baselineWrites) {
        const ok = await pollUntil(async () => {
          const r = await getKv(node.clientPort, key);
          return r.status === 200 && r.body?.value === value;
        }, 3000);
        assert(ok, `[A] ${node.nodeId} converges on baseline key ${key} before the crash`);
      }
    }

    // Kill leader + one follower TOGETHER, leaving exactly 1 survivor
    // (a minority of 3) — cluster should become unavailable, same
    // finding as test #3, incidental confirmation not the point here.
    await Promise.all([killNode(leader), killNode(doomedFollower)]);

    const minorityCantElect = await assertNoLeaderEmerges([survivor], 2000);
    assert(minorityCantElect, "[A] the lone survivor (1 of 3) does not elect itself leader");

    // Restart both killed nodes — quorum is restored (3 processes again).
    const restartedLeader = restartNode(leader);
    const restartedFollower = restartNode(doomedFollower);
    allNodes = [restartedLeader, restartedFollower, survivor];

    const newLeader = await waitForLeader(allNodes, 15_000);
    assert(newLeader !== null, "[A] a leader is (re-)elected once quorum is restored");

    for (const node of allNodes) {
      for (const [key, value] of baselineWrites) {
        const ok = await pollUntil(async () => {
          const r = await getKv(node.clientPort, key);
          return r.status === 200 && r.body?.value === value;
        }, 5000);
        assert(ok, `[A] ${node.nodeId} converges on ${key}=${value} after quorum restore`);
      }
    }
  } finally {
    await Promise.all(allNodes.map((n) => killNode(n)));
    await cleanupDataDirs(dataDirs);
  }
}

// ---- Part B: full-cluster simultaneous crash (the honest §5.3.2 test) ----

async function runPartB(): Promise<void> {
  console.log("\n=== Part B: full-cluster simultaneous crash ===");

  const PORT_A = 5901;
  const PORT_B = 5902;
  const PORT_C = 5903;
  const CLIENT_PORT_A = 6001;
  const CLIENT_PORT_B = 6002;
  const CLIENT_PORT_C = 6003;

  const dataDirs = await makeDataDirs("raft-full-crash-b");

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

  let nodeA = spawnNode("node-a", PORT_A, CLIENT_PORT_A, peersFor("a"), dataDirs.a);
  let nodeB = spawnNode("node-b", PORT_B, CLIENT_PORT_B, peersFor("b"), dataDirs.b);
  let nodeC = spawnNode("node-c", PORT_C, CLIENT_PORT_C, peersFor("c"), dataDirs.c);
  let allNodes = [nodeA, nodeB, nodeC];

  try {
    const leader = await waitForLeader(allNodes, 15_000);
    assert(leader !== null, "[B] a leader emerges among the 3 nodes within 15s");
    if (leader === null) {
      return;
    }

    // ---- (a) applied-before-crash data ----
    const appliedWrites: Array<[string, string]> = [
      ["b1", "vb1"],
      ["b2", "vb2"],
    ];
    for (const [key, value] of appliedWrites) {
      const res = await putKv(leader.clientPort, key, value);
      assert(res.status === 200, `[B] applied-write PUT /kv/${key} returns 200`);
    }
    for (const node of allNodes) {
      for (const [key, value] of appliedWrites) {
        const ok = await pollUntil(async () => {
          const r = await getKv(node.clientPort, key);
          return r.status === 200 && r.body?.value === value;
        }, 3000);
        assert(ok, `[B] ${node.nodeId} applies ${key}=${value} before the crash (baseline)`);
      }
    }

    // ---- (b) fire an uncommitted write, then kill ALL 3 ----
    //
    // FIRST ATTEMPT (kept here as a documented dead end, not silently
    // deleted): fire the write unawaited, then immediately kill all 3
    // nodes together. This was tried and FAILED CONSISTENTLY across 3
    // runs — the key survived on every node, every time. Root cause,
    // confirmed by inspection rather than assumed: killNode() has real
    // wall-clock cost (spawning `taskkill` as a separate process on
    // Windows, or awaiting a real `exit` event either platform) that
    // is NOT instant — plausibly tens of milliseconds. Meanwhile, on a
    // healthy 3-node localhost cluster, the entire write path (append
    // to leader's in-memory log, AppendEntries round-trip to 2 peers
    // over loopback, majority reached, commitIndex advanced, applied
    // immediately per Step 6.4) can complete in low single-digit
    // milliseconds. The write was never actually racing the kill — it
    // was finishing first, every time, because everyone involved was
    // still alive and fast. "Fire without awaiting" only creates a
    // real race if something can plausibly still lose it.
    //
    // ACTUAL APPROACH: remove the possibility of commit entirely
    // instead of racing against it. Kill both FOLLOWERS FIRST and
    // confirm they are actually dead (killNode already awaits a real
    // exit). With no live followers, the leader CANNOT reach a
    // majority for the next entry no matter how fast it is — this is
    // deterministic, not a timing bet. THEN fire the write against the
    // now-isolated leader (it appends to its own in-memory log
    // synchronously, attempts replication to the now-dead followers,
    // which fails fast, and then sits polling commitIndex until its
    // own 1000ms write-timeout). THEN kill the leader itself well
    // before that timeout, guaranteeing its only copy of the entry
    // (in-memory, no persistence — §5.3.2) dies with it.
    const uncommittedKey = "b-uncommitted";
    const uncommittedValue = "v-uncommitted";

    const followers = allNodes.filter((n) => n.nodeId !== leader.nodeId);
    await Promise.all(followers.map((n) => killNode(n)));

    // Followers are now confirmed dead (killNode awaited their real
    // exit) — the leader cannot reach majority for anything proposed
    // from this point on.
    const firePromise = putKv(leader.clientPort, uncommittedKey, uncommittedValue).catch(() => null);

    // Give the leader a short, deterministic window to actually append
    // the entry to its own in-memory log — this is a local, synchronous
    // operation inside the same process handling the HTTP request, so
    // 100ms is generous margin, not a race against anything: it cannot
    // commit in this window regardless of how long we wait, since
    // majority is structurally unreachable. This delay exists only to
    // make sure the append has happened before we kill the leader, not
    // to "beat" a commit that could otherwise occur.
    await new Promise((resolve) => setTimeout(resolve, 100));

    await killNode(leader);

    // Let the fired request's promise settle (it will fail/hang since
    // its target just died) — drained so it can't become an unhandled
    // rejection later, not asserted on directly.
    await firePromise;

    // ---- Confirm no leader exists with all 3 down ----
    // (Not a meaningful poll target since nothing is listening — this
    // is really just documenting the state, not a live assertion.)

    // ---- Restart all 3, same identities, same data dirs ----
    const restartedA = restartNode(nodeA);
    const restartedB = restartNode(nodeB);
    const restartedC = restartNode(nodeC);
    allNodes = [restartedA, restartedB, restartedC];

    const newLeader = await waitForLeader(allNodes, 15_000);
    assert(newLeader !== null, "[B] a leader is elected again once all 3 nodes restart");

    // ---- (a) proven: applied-before-crash entries DID survive, via each node's own Store WAL ----
    for (const node of allNodes) {
      for (const [key, value] of appliedWrites) {
        const ok = await pollUntil(async () => {
          const r = await getKv(node.clientPort, key);
          return r.status === 200 && r.body?.value === value;
        }, 5000);
        assert(
          ok,
          `[B] (a) ${node.nodeId} still has ${key}=${value} after full-cluster restart (Store WAL survived independently of Raft)`,
        );
      }
    }

    // ---- (b) proven: the uncommitted entry did NOT survive anywhere ----
    // Give the cluster a real settling window (well past election +
    // several heartbeat/apply cycles) before checking absence, so a
    // "not found yet" isn't confused with "will never be found".
    await new Promise((resolve) => setTimeout(resolve, 2000));

    for (const node of allNodes) {
      const res = await getKv(node.clientPort, uncommittedKey);
      const foundWithExpectedValue = res.status === 200 && res.body?.value === uncommittedValue;
      assert(
        !foundWithExpectedValue,
        `[B] (b) ${node.nodeId} does NOT have the uncommitted-at-crash-time key ${uncommittedKey} after full restart (§5.3.2 gap)`,
      );
    }
  } finally {
    await Promise.all(allNodes.map((n) => killNode(n)));
    await cleanupDataDirs(dataDirs);
  }
}

async function main(): Promise<void> {
  await runPartA();
  await runPartB();
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