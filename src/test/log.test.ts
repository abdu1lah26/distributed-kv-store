// Unit tests for src/raft/log.ts (Milestone 5, Step 2).
// Plain Node script, run via `npx tsx src/test/log.test.ts` — no test
// framework, per this project's established convention (§9 / Milestone 2).
//
// Pure module-in-isolation tests, no RaftNode involved — mirrors the
// Section 4.4.2/4.4.3 pattern of testing storage modules standalone
// before they're wired into anything else.

import { RaftLog, type LogCommand } from "../raft/log.js";

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

const putA: LogCommand = { op: "put", key: "a", value: "1" };
const putB: LogCommand = { op: "put", key: "b", value: "2" };
const putC: LogCommand = { op: "put", key: "c", value: "3" };
const deleteA: LogCommand = { op: "delete", key: "a" };

// ---- Empty log ----
{
  const log = new RaftLog();
  assertEqual(log.getLastLogIndex(), 0, "empty log has lastLogIndex 0");
  assertEqual(log.getLastLogTerm(), 0, "empty log has lastLogTerm 0");
  assertEqual(log.size(), 0, "empty log has size 0");
  assertEqual(log.getEntry(1), undefined, "empty log has no entry at index 1");
  assertEqual(log.getEntry(0), undefined, "index 0 is never a valid entry");
  assertEqual(log.getEntriesFrom(1), [], "getEntriesFrom on an empty log returns []");
}

// ---- append ----
{
  const log = new RaftLog();
  const entry1 = log.append(1, putA);
  assertEqual(entry1, { term: 1, index: 1, command: putA }, "first append returns the created entry at index 1");
  assertEqual(log.getLastLogIndex(), 1, "lastLogIndex advances after one append");
  assertEqual(log.getLastLogTerm(), 1, "lastLogTerm reflects the appended entry's term");

  const entry2 = log.append(1, putB);
  assertEqual(entry2.index, 2, "second append gets index 2");

  const entry3 = log.append(2, putC);
  assertEqual(entry3.index, 3, "third append gets index 3");
  assertEqual(log.getLastLogIndex(), 3, "lastLogIndex advances with each append");
  assertEqual(log.getLastLogTerm(), 2, "lastLogTerm reflects only the newest entry's term, not earlier ones");
  assertEqual(log.size(), 3, "size() matches the number of appended entries");
}

// ---- getEntry ----
{
  const log = new RaftLog();
  log.append(1, putA);
  log.append(1, putB);

  assertEqual(log.getEntry(1), { term: 1, index: 1, command: putA }, "getEntry(1) returns the first entry");
  assertEqual(log.getEntry(2), { term: 1, index: 2, command: putB }, "getEntry(2) returns the second entry");
  assertEqual(log.getEntry(3), undefined, "getEntry beyond the end returns undefined");
  assertEqual(log.getEntry(0), undefined, "getEntry(0) returns undefined");
  assertEqual(log.getEntry(-1), undefined, "getEntry with a negative index returns undefined");
}

// ---- getEntriesFrom ----
{
  const log = new RaftLog();
  log.append(1, putA); // index 1
  log.append(1, putB); // index 2
  log.append(2, putC); // index 3
  log.append(2, deleteA); // index 4

  assertEqual(
    log.getEntriesFrom(1).map((e) => e.index),
    [1, 2, 3, 4],
    "getEntriesFrom(1) returns the whole log",
  );
  assertEqual(
    log.getEntriesFrom(3).map((e) => e.index),
    [3, 4],
    "getEntriesFrom(3) returns entries from index 3 onward, inclusive",
  );
  assertEqual(log.getEntriesFrom(5), [], "getEntriesFrom past the end returns []");
  assertEqual(
    log.getEntriesFrom(0).map((e) => e.index),
    [1, 2, 3, 4],
    "getEntriesFrom(0) clamps to index 1 rather than including a nonexistent index 0",
  );

  // Returned array must not be a live view into internal storage.
  const slice = log.getEntriesFrom(1);
  slice.pop();
  assertEqual(log.size(), 4, "mutating a returned slice does not affect the underlying log");
}

// ---- truncateFrom ----
{
  const log = new RaftLog();
  log.append(1, putA); // index 1
  log.append(1, putB); // index 2
  log.append(2, putC); // index 3
  log.append(2, deleteA); // index 4

  log.truncateFrom(3); // discard indices 3, 4
  assertEqual(log.getLastLogIndex(), 2, "truncateFrom(3) leaves indices 1-2 intact");
  assertEqual(log.getLastLogTerm(), 1, "lastLogTerm reflects the new newest entry after truncation");
  assertEqual(log.getEntry(3), undefined, "truncated index is no longer retrievable");
  assertEqual(log.getEntry(2), { term: 1, index: 2, command: putB }, "entries before the truncation point survive");
}

{
  // truncateFrom(1) discards everything.
  const log = new RaftLog();
  log.append(1, putA);
  log.append(1, putB);
  log.truncateFrom(1);
  assertEqual(log.getLastLogIndex(), 0, "truncateFrom(1) empties the log entirely");
  assertEqual(log.getLastLogTerm(), 0, "an emptied log reports lastLogTerm 0 again");
}

{
  // truncateFrom with an index past the end is a no-op.
  const log = new RaftLog();
  log.append(1, putA);
  log.append(1, putB);
  log.truncateFrom(10);
  assertEqual(log.getLastLogIndex(), 2, "truncateFrom past the end does not remove anything");
}

{
  // truncateFrom(0) or negative also empties the log (matches getEntriesFrom's clamping spirit,
  // but here "from index <1" means "discard everything" rather than "clamp to 1").
  const log = new RaftLog();
  log.append(1, putA);
  log.truncateFrom(0);
  assertEqual(log.getLastLogIndex(), 0, "truncateFrom(0) empties the log");
}

// ---- append after truncateFrom continues indexing correctly ----
{
  // Exercises the real Milestone 5, Step 4 scenario in miniature: a
  // follower truncates conflicting entries, then the leader's real
  // entries are appended starting from the truncation point.
  const log = new RaftLog();
  log.append(1, putA); // index 1
  log.append(1, putB); // index 2 (will be a stale/conflicting entry)
  log.truncateFrom(2);

  const replacement = log.append(2, putC); // should become the new index 2
  assertEqual(replacement.index, 2, "append after truncateFrom resumes indexing from the truncation point, not from 0");
  assertEqual(log.getLastLogIndex(), 2, "lastLogIndex reflects the post-truncation append");
  assertEqual(log.getLastLogTerm(), 2, "lastLogTerm reflects the post-truncation append's term");
  assertEqual(log.getEntry(1), { term: 1, index: 1, command: putA }, "the surviving pre-truncation entry is unchanged");
}

console.log(`\n${passed} passed, ${failed} failed`);
if (failed > 0) {
  process.exit(1);
}