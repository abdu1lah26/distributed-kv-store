// Unit tests for src/raft/writePath.ts (Milestone 5, Step 6.4).
// Plain Node script, run via `npx tsx src/test/writePath.test.ts` — no
// test framework, per this project's established convention (§9).

import { RaftNode } from "../raft/node.js";
import { ApplyLoop, type ApplyTarget } from "../raft/applyLoop.js";
import { proposeWrite } from "../raft/writePath.js";
import type { PeerConfig } from "../cluster/config.js";

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

function assertEqual<T>(actual: T, expected: T, message: string): void {
  assert(
    JSON.stringify(actual) === JSON.stringify(expected),
    `${message} (expected ${JSON.stringify(expected)}, got ${JSON.stringify(actual)})`,
  );
}

class FakeStore implements ApplyTarget {
  readonly data = new Map<string, string>();
  async put(key: string, value: string): Promise<void> {
    this.data.set(key, value);
  }
  async delete(key: string): Promise<void> {
    this.data.delete(key);
  }
}

function newNode(nodeId: string): RaftNode {
  return new RaftNode({ nodeId, randomFn: () => 0.5 });
}

// An unreachable peer — real network call, fails fast (connection
// refused), never actually replicates. Used to construct a majority
// that can never be reached without real peers responding, so commit
// never happens on its own — needed for the timeout/stepped-down tests.
const unreachablePeer: PeerConfig = { id: "node-b", host: "localhost", port: 1, clientPort: 2 };

// ---- not-leader: returns immediately, log untouched ----
{
  const node = newNode("node-a"); // follower
  const store = new FakeStore();
  const applyLoop = new ApplyLoop(store);

  const result = await proposeWrite(node, [], applyLoop, { op: "put", key: "k", value: "v" });

  assertEqual(result, { status: "not-leader" }, "a follower's proposeWrite returns not-leader immediately");
  assertEqual(node.lastLogIndex, 0, "the log is untouched — nothing was appended before the leader check");
  node.stop();
}

// ---- committed: a 1-node cluster (zero peers) commits immediately (self alone is a majority of 1) ----
{
  const node = newNode("node-a");
  node.becomeCandidate();
  node.becomeLeader([]); // zero peers — self alone satisfies majority (1 of 1)
  const store = new FakeStore();
  const applyLoop = new ApplyLoop(store);

  const result = await proposeWrite(node, [], applyLoop, { op: "put", key: "k", value: "v" }, 500);

  assertEqual(result, { status: "committed", index: 1 }, "a write on a 1-node cluster commits (self alone is a majority)");
  assertEqual(node.commitIndex, 1, "the node's commitIndex reflects the commit");
  assertEqual(applyLoop.lastAppliedIndex, 1, "proposeWrite applies the entry via the given ApplyLoop before returning committed");
  assertEqual(store.data.get("k"), "v", "the write is genuinely visible in the store after a committed result");
  node.stop();
}

{
  // Multiple sequential writes on the same 1-node leader all commit and apply in order.
  const node = newNode("node-a");
  node.becomeCandidate();
  node.becomeLeader([]);
  const store = new FakeStore();
  const applyLoop = new ApplyLoop(store);

  const r1 = await proposeWrite(node, [], applyLoop, { op: "put", key: "a", value: "1" }, 500);
  const r2 = await proposeWrite(node, [], applyLoop, { op: "put", key: "b", value: "2" }, 500);
  const r3 = await proposeWrite(node, [], applyLoop, { op: "delete", key: "a" }, 500);

  assertEqual(r1, { status: "committed", index: 1 }, "first write commits at index 1");
  assertEqual(r2, { status: "committed", index: 2 }, "second write commits at index 2");
  assertEqual(r3, { status: "committed", index: 3 }, "third write commits at index 3");
  assertEqual(store.data.has("a"), false, "final state reflects the delete");
  assertEqual(store.data.get("b"), "2", "final state reflects the surviving put");
  node.stop();
}

// ---- timeout: an unreachable majority never commits, and the entry is NOT cleaned up ----
{
  const node = newNode("node-a");
  node.becomeCandidate();
  node.becomeLeader(["node-b"]); // 2-node cluster, majority 2 — self alone (1) is not enough
  const store = new FakeStore();
  const applyLoop = new ApplyLoop(store);

  const result = await proposeWrite(node, [unreachablePeer], applyLoop, { op: "put", key: "k", value: "v" }, 100);

  assertEqual(result, { status: "timeout", index: 1 }, "a write that can never reach a majority times out rather than hanging forever");

  // EXPLICITLY REQUIRED: confirm the entry is NOT cleaned up — it must
  // remain a real, legitimate entry in the log.
  assertEqual(node.lastLogIndex, 1, "the entry is still present in the log after a timeout — not truncated or removed");
  assertEqual(
    node.log.getEntry(1),
    { term: 1, index: 1, command: { op: "put", key: "k", value: "v" } },
    "the timed-out entry is still retrievable, byte-for-byte, from the log",
  );
  assertEqual(node.commitIndex, 0, "sanity: it genuinely never committed");
  assertEqual(applyLoop.lastAppliedIndex, 0, "the apply loop never applied it, since it never committed");
  node.stop();
}

{
  // A subsequent write after a timeout still gets the NEXT real index —
  // proposeWrite doesn't reuse or skip the timed-out entry's slot.
  const node = newNode("node-a");
  node.becomeCandidate();
  node.becomeLeader(["node-b"]);
  const store = new FakeStore();
  const applyLoop = new ApplyLoop(store);

  await proposeWrite(node, [unreachablePeer], applyLoop, { op: "put", key: "first", value: "x" }, 50);
  const second = await proposeWrite(node, [unreachablePeer], applyLoop, { op: "put", key: "second", value: "y" }, 50);

  assertEqual(second, { status: "timeout", index: 2 }, "a second timed-out write gets index 2, not reusing index 1");
  assertEqual(node.lastLogIndex, 2, "both timed-out entries remain in the log");
  node.stop();
}

// ---- stepped-down: losing leadership mid-write is detected without waiting out the full timeout ----
{
  const node = newNode("node-a");
  node.becomeCandidate(); // term 1
  node.becomeLeader(["node-b"]); // majority never reachable (unreachablePeer)
  const store = new FakeStore();
  const applyLoop = new ApplyLoop(store);

  const startedAt = Date.now();
  const resultPromise = proposeWrite(node, [unreachablePeer], applyLoop, { op: "put", key: "k", value: "v" }, 5000);

  // Simulate discovering a higher term through some other path (e.g. a
  // concurrent AppendEntries from a real new leader elsewhere) shortly
  // after the write begins polling.
  setTimeout(() => {
    node.becomeFollower(node.currentTerm + 1);
  }, 30);

  const result = await resultPromise;
  const elapsedMs = Date.now() - startedAt;

  assertEqual(result, { status: "stepped-down", index: 1 }, "stepping down mid-write is reported distinctly from a timeout");
  assert(
    elapsedMs < 1000,
    `stepping down is detected quickly (${elapsedMs}ms), not by waiting out the full 5000ms timeout`,
  );
  assertEqual(node.lastLogIndex, 1, "the entry remains in the log after stepping down — not cleaned up here either");
  assertEqual(applyLoop.lastAppliedIndex, 0, "nothing was applied, since it never committed before stepping down");
  node.stop();
}

// ---- poll loop does not block the event loop / starve the heartbeat timer ----
{
  // If proposeWrite's poll loop were a synchronous busy-loop instead of
  // real setTimeout-based waits, RaftNode's own background heartbeat
  // timer (a setInterval) would never get a chance to fire while
  // proposeWrite is polling — Node is single-threaded, so a busy loop
  // starves every other timer. This node's onHeartbeatDue hook counts
  // real firings (deliberately NOT calling the real sendHeartbeats here,
  // to isolate "did the timer get to run at all" from replication
  // correctness, which is already covered by rpc.test.ts).
  let heartbeatFireCount = 0;
  const node = new RaftNode({
    nodeId: "node-a",
    randomFn: () => 0.5,
    onHeartbeatDue: () => {
      heartbeatFireCount++;
    },
  });
  node.becomeCandidate();
  node.becomeLeader(["node-b"]); // majority never reachable -> proposeWrite will poll for the full timeout
  const store = new FakeStore();
  const applyLoop = new ApplyLoop(store);

  const result = await proposeWrite(node, [unreachablePeer], applyLoop, { op: "put", key: "k", value: "v" }, 300);

  assertEqual(result.status, "timeout", "sanity: this write times out (as intended, to force the full poll duration)");
  // Heartbeat interval is a fixed 50ms; over a 300ms poll window with a
  // real (non-blocking) wait, several real firings are expected. A
  // conservative lower bound avoids flakiness on slower machines while
  // still being impossible to satisfy under a busy-loop (which would
  // starve the timer to 0 firings).
  assert(
    heartbeatFireCount >= 2,
    `the background heartbeat timer fired ${heartbeatFireCount} times during a 300ms poll — proposeWrite's poll loop is not blocking the event loop (a busy loop would starve this to 0)`,
  );
  node.stop();
}

console.log(`\n${passed} passed, ${failed} failed`);
if (failed > 0) {
  process.exit(1);
}