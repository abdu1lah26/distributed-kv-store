// Unit tests for src/raft/applyLoop.ts (Milestone 5, Step 6.3).
// Plain Node script, run via `npx tsx src/test/applyLoop.test.ts` — no
// test framework, per this project's established convention (§9).
//
// Tested against a lightweight in-memory FakeStore implementing
// ApplyTarget, not a real Store — ApplyLoop is deliberately decoupled
// from the concrete Store class (see applyLoop.ts's header comment), so
// its own unit tests exercise that same interface boundary directly.
// The real Store (src/storage/store.ts) satisfies ApplyTarget without
// any changes, so wiring a real one in later (e.g. Milestone 5, Step
// 6.6's entrypoint) is a drop-in, not a special case. This also makes
// the error-injection test below (a store that throws on the Nth call)
// trivial to set up with no real filesystem involved.
//
// RaftNode itself IS used for the "real node" tests (via
// receiveAppendEntries, which can append entries and advance
// commitIndex via leaderCommit in one non-networked call) — RaftNode
// satisfies CommitSource without any changes either.

import { ApplyLoop, type ApplyTarget, type CommitSource } from "../raft/applyLoop.js";
import { RaftNode, type AppendEntriesRequest } from "../raft/node.js";
import type { LogCommand } from "../raft/log.js";

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

async function assertRejects(fn: () => Promise<unknown>, expectedSubstring: string, message: string): Promise<void> {
  try {
    await fn();
    failed++;
    console.error(`FAIL: ${message} (expected to reject, but did not)`);
  } catch (err) {
    const text = err instanceof Error ? err.message : String(err);
    assert(
      text.includes(expectedSubstring),
      `${message} (expected rejection to include "${expectedSubstring}", got "${text}")`,
    );
  }
}

/** In-memory ApplyTarget for tests that don't need real disk I/O. */
class FakeStore implements ApplyTarget {
  readonly data = new Map<string, string>();
  readonly calls: Array<{ op: "put" | "delete"; key: string; value?: string }> = [];

  async put(key: string, value: string): Promise<void> {
    this.calls.push({ op: "put", key, value });
    this.data.set(key, value);
  }

  async delete(key: string): Promise<void> {
    this.calls.push({ op: "delete", key });
    this.data.delete(key);
  }
}

/**
 * An ApplyTarget that throws on its Nth call (1-indexed) and behaves
 * like FakeStore otherwise — for the explicitly-requested error-
 * injection test.
 */
class ThrowingOnNthCallStore implements ApplyTarget {
  readonly data = new Map<string, string>();
  private _callCount = 0;

  constructor(private readonly throwOnCallNumber: number) {}

  get callCount(): number {
    return this._callCount;
  }

  async put(key: string, value: string): Promise<void> {
    this._callCount += 1;
    if (this._callCount === this.throwOnCallNumber) {
      throw new Error(`simulated failure on call ${this._callCount}`);
    }
    this.data.set(key, value);
  }

  async delete(key: string): Promise<void> {
    this._callCount += 1;
    if (this._callCount === this.throwOnCallNumber) {
      throw new Error(`simulated failure on call ${this._callCount}`);
    }
    this.data.delete(key);
  }
}

/** A minimal CommitSource stub, for testing the getEntry(i)===undefined defensive case in isolation from real RaftNode invariants. */
function fakeSource(commitIndex: number, entries: Map<number, LogCommand>): CommitSource {
  return {
    commitIndex,
    log: {
      getEntry(index: number) {
        const command = entries.get(index);
        return command === undefined ? undefined : { term: 1, index, command };
      },
    },
  };
}

function newNode(nodeId: string): RaftNode {
  return new RaftNode({ nodeId, randomFn: () => 0.5 });
}

// Builds a real AppendEntries request carrying the given commands
// (starting at index 1) with leaderCommit set to commit all of them —
// lets tests get a real RaftNode into "has N committed entries" state
// in one call, no real networking or multi-round replication needed.
function appendAndCommit(commands: LogCommand[]): AppendEntriesRequest {
  return {
    term: 1,
    leaderId: "node-leader",
    prevLogIndex: 0,
    prevLogTerm: 0,
    entries: commands.map((command, i) => ({ term: 1, index: i + 1, command })),
    leaderCommit: commands.length,
  };
}

// ---- Initial state ----
{
  const store = new FakeStore();
  const loop = new ApplyLoop(store);
  assertEqual(loop.lastAppliedIndex, 0, "a fresh ApplyLoop starts with lastAppliedIndex 0");
}

// ---- No-op when nothing is committed ----
{
  const store = new FakeStore();
  const loop = new ApplyLoop(store);
  const node = newNode("node-a"); // commitIndex 0, empty log

  await loop.applyCommitted(node);

  assertEqual(loop.lastAppliedIndex, 0, "applying against a node with nothing committed is a no-op");
  assertEqual(store.calls.length, 0, "the target's put/delete are never called when there's nothing to apply");
}

// ---- Applies put and delete, in order ----
{
  const store = new FakeStore();
  const loop = new ApplyLoop(store);
  const node = newNode("node-a");

  node.receiveAppendEntries(
    appendAndCommit([
      { op: "put", key: "a", value: "1" },
      { op: "put", key: "b", value: "2" },
      { op: "delete", key: "a" },
    ]),
  );

  await loop.applyCommitted(node);

  assertEqual(loop.lastAppliedIndex, 3, "lastAppliedIndex advances to match commitIndex after applying everything");
  assertEqual(
    store.calls,
    [
      { op: "put", key: "a", value: "1" },
      { op: "put", key: "b", value: "2" },
      { op: "delete", key: "a" },
    ],
    "put and delete are both applied, in the exact order they were committed",
  );
  assertEqual(store.data.get("a"), undefined, "the final state reflects the delete (a was removed)");
  assertEqual(store.data.get("b"), "2", "the final state reflects the surviving put (b)");
}

// ---- Stops exactly at commitIndex, does not over-apply ----
{
  const store = new FakeStore();
  const loop = new ApplyLoop(store);
  const node = newNode("node-a");

  // Log has 3 entries, but only the first 2 are committed.
  node.receiveAppendEntries({
    term: 1,
    leaderId: "node-leader",
    prevLogIndex: 0,
    prevLogTerm: 0,
    entries: [
      { term: 1, index: 1, command: { op: "put", key: "a", value: "1" } },
      { term: 1, index: 2, command: { op: "put", key: "b", value: "2" } },
      { term: 1, index: 3, command: { op: "put", key: "c", value: "3" } },
    ],
    leaderCommit: 2, // only entries 1-2 are committed
  });

  await loop.applyCommitted(node);

  assertEqual(loop.lastAppliedIndex, 2, "lastAppliedIndex stops at commitIndex (2), not lastLogIndex (3)");
  assertEqual(store.calls.length, 2, "only the 2 committed entries are applied");
  assertEqual(store.data.has("c"), false, "the uncommitted 3rd entry (c) is never applied");
}

// ---- Resumes correctly across multiple calls ----
{
  const store = new FakeStore();
  const loop = new ApplyLoop(store);
  const node = newNode("node-a");

  node.receiveAppendEntries({
    term: 1,
    leaderId: "node-leader",
    prevLogIndex: 0,
    prevLogTerm: 0,
    entries: [
      { term: 1, index: 1, command: { op: "put", key: "a", value: "1" } },
      { term: 1, index: 2, command: { op: "put", key: "b", value: "2" } },
    ],
    leaderCommit: 1, // only entry 1 committed so far
  });

  await loop.applyCommitted(node);
  assertEqual(loop.lastAppliedIndex, 1, "sanity: first call applies only what's committed so far (entry 1)");
  assertEqual(store.calls.length, 1, "sanity: only 1 call made so far");

  // A later round: leaderCommit catches up to cover entry 2 as well.
  node.receiveAppendEntries({
    term: 1,
    leaderId: "node-leader",
    prevLogIndex: 2,
    prevLogTerm: 1,
    entries: [],
    leaderCommit: 2,
  });

  await loop.applyCommitted(node);

  assertEqual(loop.lastAppliedIndex, 2, "second call resumes from where it left off and applies the newly-committed entry");
  assertEqual(store.calls.length, 2, "only ONE new call was made on the second round (entry 1 was not re-applied)");
  assertEqual(
    store.calls[1],
    { op: "put", key: "b", value: "2" },
    "the second round's new call is for entry 2, not a re-application of entry 1",
  );

  // A third call with no further progress is a clean no-op.
  await loop.applyCommitted(node);
  assertEqual(loop.lastAppliedIndex, 2, "a third call with no new commit progress leaves lastAppliedIndex unchanged");
  assertEqual(store.calls.length, 2, "a third call with no new commit progress makes no new calls");
}

// ---- EXPLICITLY REQUESTED: error propagation leaves lastAppliedIndex at the correct, resumable point ----
{
  // Fails on the 2nd call specifically, so the 1st entry succeeds before
  // the failure and the 3rd entry is never attempted.
  const store = new ThrowingOnNthCallStore(2);
  const loop = new ApplyLoop(store);
  const node = newNode("node-a");

  node.receiveAppendEntries(
    appendAndCommit([
      { op: "put", key: "a", value: "1" }, // call 1 — succeeds
      { op: "put", key: "b", value: "2" }, // call 2 — throws
      { op: "put", key: "c", value: "3" }, // never attempted
    ]),
  );

  await assertRejects(
    () => loop.applyCommitted(node),
    "simulated failure on call 2",
    "applyCommitted propagates the store's thrown error rather than swallowing it",
  );

  assertEqual(
    loop.lastAppliedIndex,
    1,
    "CRITICAL: lastAppliedIndex reflects exactly the last entry that succeeded (1), not 0 (understating real progress) and not 2 or 3 (overstating it past the failure)",
  );
  assertEqual(store.data.get("a"), "1", "the entry that succeeded before the failure was genuinely applied");
  assertEqual(store.data.has("b"), false, "the entry that failed was not applied");
  assertEqual(store.data.has("c"), false, "the entry after the failure was never even attempted");

  // Confirm the failure genuinely leaves things in a RESUMABLE state:
  // ThrowingOnNthCallStore only throws on its literal 2nd call (a
  // one-time simulated failure, not a permanently-broken target), so
  // calling applyCommitted AGAIN on the very same loop + same store must
  // resume from the failed entry — not skip it, and not redo the
  // already-successful one.
  await loop.applyCommitted(node);

  assertEqual(
    loop.lastAppliedIndex,
    3,
    "calling applyCommitted again on the same loop resumes from the failed entry and completes through commitIndex",
  );
  assertEqual(store.data.get("b"), "2", "entry b (which failed on the first attempt) is genuinely applied on retry");
  assertEqual(store.data.get("c"), "3", "entry c is applied too, continuing past the retried entry");
  assertEqual(
    store.callCount,
    4,
    "4 total calls across both attempts (a-succeeded, b-threw, b-retried-succeeded, c-succeeded) — entry a was never redundantly re-applied on retry",
  );
}

// ---- Defensive case: log/commitIndex invariant violation throws a specific Error ----
{
  // A hand-built CommitSource stub simulating an impossible-in-practice
  // state (commitIndex says index 2 should exist, but the log is
  // missing it) — this can't be constructed through real RaftNode usage
  // (its own invariants prevent it), which is exactly why this is
  // defensive code tested via a direct stub rather than real replication.
  const store = new FakeStore();
  const loop = new ApplyLoop(store);
  const entries = new Map<number, LogCommand>([[1, { op: "put", key: "a", value: "1" }]]); // index 2 deliberately missing
  const source = fakeSource(2, entries);

  await assertRejects(
    () => loop.applyCommitted(source),
    "expected a log entry at index 2",
    "throws a specific, actionable Error when commitIndex implies an entry that the log doesn't actually have",
  );

  assertEqual(loop.lastAppliedIndex, 1, "the entry that DID exist (index 1) was still applied before hitting the missing one");
  assertEqual(store.data.get("a"), "1", "index 1 was genuinely applied before the invariant violation was hit");
}

console.log(`\n${passed} passed, ${failed} failed`);
if (failed > 0) {
  process.exit(1);
}