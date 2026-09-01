// Unit tests for src/raft/node.ts (Milestone 4, Step 1; updated Milestone
// 5, Steps 1-5, 6.2).
// Plain Node script, run via `npx tsx src/test/node.test.ts` — no test
// framework, per this project's established convention (§9 / Milestone 2).

import { RaftNode, type AppendEntriesRequest } from "../raft/node.js";

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

// Deterministic timer stand-in: never fires on its own during these tests
// since we call transition methods directly rather than waiting on real
// timers. All nodes here get randomFn returning a fixed value so election
// timeout duration is deterministic if it ever mattered for a computation.
function newNode(nodeId: string): RaftNode {
  return new RaftNode({ nodeId, randomFn: () => 0.5 });
}

// Builds a VoteRequest with lastLogIndex/lastLogTerm defaulted to 0/0
// (an "empty log" candidate) unless overridden — keeps existing
// term/candidateId-only test cases readable while still exercising the
// real (now-required) VoteRequest shape. See DESIGN.md §5.3.1.
function voteRequest(
  term: number,
  candidateId: string,
  overrides: { lastLogIndex?: number; lastLogTerm?: number } = {},
) {
  return {
    term,
    candidateId,
    lastLogIndex: overrides.lastLogIndex ?? 0,
    lastLogTerm: overrides.lastLogTerm ?? 0,
  };
}

// ---- Initial state ----
{
  const node = newNode("node-a");
  assertEqual(node.state, "follower", "new node starts as follower");
  assertEqual(node.currentTerm, 0, "new node starts at term 0");
  assertEqual(node.votedFor, null, "new node has voted for nobody");
  node.stop();
}

// ---- becomeCandidate ----
{
  const node = newNode("node-a");
  node.becomeCandidate();
  assertEqual(node.state, "candidate", "becomeCandidate sets state to candidate");
  assertEqual(node.currentTerm, 1, "becomeCandidate increments term");
  assertEqual(node.votedFor, "node-a", "becomeCandidate votes for self");
  node.stop();
}

{
  // Repeated candidacy (e.g. split-vote retry) keeps incrementing term.
  const node = newNode("node-a");
  node.becomeCandidate();
  node.becomeCandidate();
  assertEqual(node.currentTerm, 2, "second becomeCandidate increments term again");
  assertEqual(node.votedFor, "node-a", "still voted for self after retry");
  node.stop();
}

// ---- becomeLeader ----
{
  const node = newNode("node-a");
  node.becomeCandidate();
  node.becomeLeader(["node-b", "node-c"]);
  assertEqual(node.state, "leader", "becomeLeader sets state to leader");
  assertEqual(node.currentTerm, 1, "becomeLeader does not change term");
  node.stop();
}

// ---- becomeLeader: replication state initialization (DESIGN.md §5.3.3, Milestone 5 Step 3) ----
{
  // Empty log: nextIndex starts at 1 (lastLogIndex 0 + 1) for every peer.
  const node = newNode("node-a");
  node.becomeCandidate();
  node.becomeLeader(["node-b", "node-c"]);

  assertEqual(node.getNextIndex("node-b"), 1, "nextIndex starts at lastLogIndex+1 (1) for peer node-b on an empty log");
  assertEqual(node.getNextIndex("node-c"), 1, "nextIndex starts at lastLogIndex+1 (1) for peer node-c on an empty log");
  assertEqual(node.getMatchIndex("node-b"), 0, "matchIndex starts at 0 for peer node-b");
  assertEqual(node.getMatchIndex("node-c"), 0, "matchIndex starts at 0 for peer node-c");
  node.stop();
}

{
  // Non-empty log: nextIndex should start at the real lastLogIndex+1, not
  // unconditionally 1 — becomeLeader() must read the log's actual state.
  const node = newNode("node-a");
  node.log.append(1, { op: "put", key: "a", value: "1" });
  node.log.append(1, { op: "put", key: "b", value: "2" });
  node.becomeCandidate();
  node.becomeLeader(["node-b"]);

  assertEqual(node.getNextIndex("node-b"), 3, "nextIndex starts at the real lastLogIndex+1 (3) when the log already has entries");
  assertEqual(node.getMatchIndex("node-b"), 0, "matchIndex still starts at 0 regardless of existing log length");
  node.stop();
}

{
  // A peer never passed to becomeLeader has no tracked replication state.
  const node = newNode("node-a");
  node.becomeCandidate();
  node.becomeLeader(["node-b"]);

  assertEqual(node.getNextIndex("node-z"), undefined, "getNextIndex is undefined for a peer not passed to becomeLeader");
  assertEqual(node.getMatchIndex("node-z"), undefined, "getMatchIndex is undefined for a peer not passed to becomeLeader");
  node.stop();
}

{
  // A node that never became leader has no tracked replication state for anyone.
  const node = newNode("node-a");
  assertEqual(node.getNextIndex("node-b"), undefined, "getNextIndex is undefined before ever becoming leader");
  assertEqual(node.getMatchIndex("node-b"), undefined, "getMatchIndex is undefined before ever becoming leader");
  node.stop();
}

// ---- advanceReplicationOnSuccess / retreatReplicationOnFailure (DESIGN.md §5.3.3) ----
{
  const node = newNode("node-a");
  node.log.append(1, { op: "put", key: "a", value: "1" });
  node.log.append(1, { op: "put", key: "b", value: "2" });
  node.becomeCandidate();
  node.becomeLeader(["node-b"]); // nextIndex=3, matchIndex=0

  node.advanceReplicationOnSuccess("node-b", 2);
  assertEqual(node.getMatchIndex("node-b"), 2, "advanceReplicationOnSuccess sets matchIndex to the given value");
  assertEqual(node.getNextIndex("node-b"), 3, "advanceReplicationOnSuccess sets nextIndex to matchIndex + 1");
  node.stop();
}

{
  // Guards against an out-of-order/stale response regressing matchIndex backward.
  const node = newNode("node-a");
  node.becomeCandidate();
  node.becomeLeader(["node-b"]);

  node.advanceReplicationOnSuccess("node-b", 5);
  node.advanceReplicationOnSuccess("node-b", 2); // stale/out-of-order response arriving late
  assertEqual(node.getMatchIndex("node-b"), 5, "advanceReplicationOnSuccess never regresses matchIndex backward");
  node.stop();
}

{
  const node = newNode("node-a");
  node.becomeCandidate();
  node.becomeLeader(["node-b"]); // nextIndex starts at 1

  node.retreatReplicationOnFailure("node-b");
  assertEqual(node.getNextIndex("node-b"), 1, "retreatReplicationOnFailure floors nextIndex at 1, never going lower");
  node.stop();
}

{
  const node = newNode("node-a");
  node.log.append(1, { op: "put", key: "a", value: "1" });
  node.log.append(1, { op: "put", key: "b", value: "2" });
  node.log.append(1, { op: "put", key: "c", value: "3" });
  node.becomeCandidate();
  node.becomeLeader(["node-b"]); // nextIndex starts at 4

  node.retreatReplicationOnFailure("node-b");
  assertEqual(node.getNextIndex("node-b"), 3, "retreatReplicationOnFailure decrements nextIndex by one");
  node.stop();
}

{
  // Both methods are no-ops for an untracked peer (e.g. this node is no
  // longer leader by the time a response arrives) — must not throw.
  const node = newNode("node-a");
  node.becomeCandidate();
  node.becomeLeader(["node-b"]);

  node.advanceReplicationOnSuccess("node-z", 5);
  node.retreatReplicationOnFailure("node-z");
  assertEqual(node.getNextIndex("node-z"), undefined, "advance/retreat for an untracked peer id is a silent no-op, not a throw");
  node.stop();
}

// ---- becomeFollower clears replication state (DESIGN.md §5.3.3) ----
{
  const node = newNode("node-a");
  node.becomeCandidate();
  node.becomeLeader(["node-b", "node-c"]);
  assertEqual(node.getNextIndex("node-b"), 1, "sanity: replication state exists while leader");

  node.becomeFollower(node.currentTerm + 1);
  assertEqual(node.getNextIndex("node-b"), undefined, "stepping down from leader clears replication state for node-b");
  assertEqual(node.getNextIndex("node-c"), undefined, "stepping down from leader clears replication state for node-c");
  node.stop();
}

{
  // Fresh leadership after a later election starts with clean state, not
  // whatever was left over from a previous term as leader.
  const node = newNode("node-a");
  node.becomeCandidate();
  node.becomeLeader(["node-b"]);
  node.advanceReplicationOnSuccess("node-b", 7);
  node.becomeFollower(node.currentTerm + 1);

  node.becomeCandidate();
  node.becomeLeader(["node-b"]);
  assertEqual(
    node.getMatchIndex("node-b"),
    0,
    "a fresh becomeLeader() call starts matchIndex at 0 again, not carrying over a prior term's progress",
  );
  node.stop();
}

// ---- becomeFollower ----
{
  const node = newNode("node-a");
  node.becomeCandidate(); // term 1, voted for self
  node.becomeFollower(1); // same term
  assertEqual(node.state, "follower", "becomeFollower(sameTerm) reverts to follower");
  assertEqual(node.currentTerm, 1, "becomeFollower(sameTerm) keeps term unchanged");
  assertEqual(
    node.votedFor,
    "node-a",
    "becomeFollower(sameTerm) preserves votedFor (no re-vote within a term)",
  );
  node.stop();
}

{
  const node = newNode("node-a");
  node.becomeCandidate(); // term 1, voted for self
  node.becomeFollower(2); // higher term
  assertEqual(node.state, "follower", "becomeFollower(higherTerm) reverts to follower");
  assertEqual(node.currentTerm, 2, "becomeFollower(higherTerm) adopts the higher term");
  assertEqual(
    node.votedFor,
    null,
    "becomeFollower(higherTerm) clears votedFor for the new term",
  );
  node.stop();
}

{
  const node = newNode("node-a");
  node.becomeCandidate();
  node.becomeLeader(["node-b", "node-c"]);
  node.becomeFollower(2);
  assertEqual(node.state, "follower", "leader steps down to follower on higher term");
  node.stop();
}

// ---- receiveVoteRequest ----
{
  // Grants vote when unvoted, at or above own term.
  const node = newNode("node-a"); // term 0
  const response = node.receiveVoteRequest(voteRequest(1, "node-b"));
  assertEqual(response.voteGranted, true, "grants vote to first requester at higher term");
  assertEqual(response.term, 1, "response reflects the new (adopted) term");
  assertEqual(node.currentTerm, 1, "receiving a higher-term vote request adopts that term");
  assertEqual(node.votedFor, "node-b", "records the vote");
  node.stop();
}

{
  // Refuses a second vote in the same term for a different candidate.
  const node = newNode("node-a");
  node.receiveVoteRequest(voteRequest(1, "node-b"));
  const response = node.receiveVoteRequest(voteRequest(1, "node-c"));
  assertEqual(response.voteGranted, false, "refuses a second candidate in the same term");
  assertEqual(node.votedFor, "node-b", "votedFor is unchanged after refusal");
  node.stop();
}

{
  // Idempotent: re-requesting from the same candidate in the same term
  // still grants (e.g. a retried/duplicated RPC).
  const node = newNode("node-a");
  node.receiveVoteRequest(voteRequest(1, "node-b"));
  const response = node.receiveVoteRequest(voteRequest(1, "node-b"));
  assertEqual(response.voteGranted, true, "re-granting the same candidate in the same term is idempotent");
  node.stop();
}

{
  // Rejects a request from a stale (lower-term) candidate.
  const node = newNode("node-a");
  node.becomeCandidate(); // term 1
  node.becomeCandidate(); // term 2
  const response = node.receiveVoteRequest(voteRequest(1, "node-b"));
  assertEqual(response.voteGranted, false, "rejects vote request with a stale term");
  assertEqual(response.term, 2, "response reports the node's actual current term");
  node.stop();
}

{
  // A candidate receiving a higher-term vote request steps down and can vote.
  const node = newNode("node-a");
  node.becomeCandidate(); // term 1, candidate, voted for self
  const response = node.receiveVoteRequest(voteRequest(2, "node-b"));
  assertEqual(response.voteGranted, true, "candidate steps down and grants vote at higher term");
  assertEqual(node.state, "follower", "candidate becomes follower upon seeing higher term");
  node.stop();
}

// ---- RaftNode.log / lastLogIndex / lastLogTerm reflect real appends ----
{
  const node = newNode("node-a");
  assertEqual(node.lastLogIndex, 0, "a fresh node has lastLogIndex 0");
  assertEqual(node.lastLogTerm, 0, "a fresh node has lastLogTerm 0");

  const entry1 = node.log.append(1, { op: "put", key: "a", value: "1" });
  assertEqual(entry1.index, 1, "first appended entry gets index 1");
  assertEqual(node.lastLogIndex, 1, "lastLogIndex reflects the real log after one append");
  assertEqual(node.lastLogTerm, 1, "lastLogTerm reflects the real log after one append");

  const entry2 = node.log.append(2, { op: "delete", key: "a" });
  assertEqual(entry2.index, 2, "second appended entry gets index 2");
  assertEqual(node.lastLogIndex, 2, "lastLogIndex reflects the real log after a second append");
  assertEqual(node.lastLogTerm, 2, "lastLogTerm reflects the real log's newest entry's term");

  node.stop();
}

// ---- receiveVoteRequest: log up-to-date check (DESIGN.md §5.3.1, Milestone 5 Steps 1-2) ----
// Grows a node's real log via node.log.append() to reach a target
// lastLogIndex/lastLogTerm, rather than injecting fake state — the
// Milestone 5, Step 1 setLastLogInfo() override was removed once the
// real log (Step 2) existed, so these exercise the real mechanism.
function growLog(node: RaftNode, targetIndex: number, targetTerm: number): void {
  if (targetIndex <= 0) {
    return;
  }
  // Filler entries (all but the last) use term 1, which must be <=
  // targetTerm for every test case below — fine since real Raft terms
  // are non-decreasing across a log's real lifetime anyway.
  for (let i = 1; i < targetIndex; i++) {
    node.log.append(1, { op: "put", key: `filler-${i}`, value: "x" });
  }
  node.log.append(targetTerm, { op: "put", key: `final-${targetIndex}`, value: "x" });
}

{
  // Candidate's lastLogTerm is lower than the voter's -> reject, even
  // though the voter hasn't voted this term and the request term is fine.
  const node = newNode("node-a");
  growLog(node, 10, 5);
  assertEqual(node.lastLogIndex, 10, "sanity: growLog reached the intended lastLogIndex");
  assertEqual(node.lastLogTerm, 5, "sanity: growLog reached the intended lastLogTerm");
  const response = node.receiveVoteRequest(voteRequest(1, "node-b", { lastLogTerm: 4, lastLogIndex: 999 }));
  assertEqual(response.voteGranted, false, "rejects a candidate whose lastLogTerm is behind, regardless of lastLogIndex");
  assertEqual(node.votedFor, null, "votedFor is not set when the candidate's log is behind");
  node.stop();
}

{
  // Equal lastLogTerm, but candidate's lastLogIndex is behind -> reject.
  const node = newNode("node-a");
  growLog(node, 10, 5);
  const response = node.receiveVoteRequest(voteRequest(1, "node-b", { lastLogTerm: 5, lastLogIndex: 9 }));
  assertEqual(response.voteGranted, false, "rejects a candidate with the same lastLogTerm but a lower lastLogIndex");
  node.stop();
}

{
  // Equal lastLogTerm and equal lastLogIndex -> "at least as up-to-date", grant.
  const node = newNode("node-a");
  growLog(node, 10, 5);
  const response = node.receiveVoteRequest(voteRequest(1, "node-b", { lastLogTerm: 5, lastLogIndex: 10 }));
  assertEqual(response.voteGranted, true, "grants when candidate's log is exactly as up-to-date (equal term and index)");
  node.stop();
}

{
  // Candidate's lastLogTerm is higher -> grant, even with a lower lastLogIndex
  // (higher term wins outright per the Raft paper's comparison rule).
  const node = newNode("node-a");
  growLog(node, 10, 5);
  const response = node.receiveVoteRequest(voteRequest(1, "node-b", { lastLogTerm: 6, lastLogIndex: 0 }));
  assertEqual(response.voteGranted, true, "grants when candidate's lastLogTerm is higher, even with a lower lastLogIndex");
  node.stop();
}

{
  // Equal lastLogTerm, candidate's lastLogIndex is ahead -> grant.
  const node = newNode("node-a");
  growLog(node, 10, 5);
  const response = node.receiveVoteRequest(voteRequest(1, "node-b", { lastLogTerm: 5, lastLogIndex: 11 }));
  assertEqual(response.voteGranted, true, "grants when candidate's lastLogIndex is ahead at the same lastLogTerm");
  node.stop();
}

{
  // The up-to-date check applies even when votedFor is null (first-ever
  // request this term) -- being unvoted does not bypass the log check.
  const node = newNode("node-a");
  growLog(node, 5, 3);
  assertEqual(node.votedFor, null, "sanity: node has not voted yet");
  const response = node.receiveVoteRequest(voteRequest(1, "node-b", { lastLogTerm: 2, lastLogIndex: 100 }));
  assertEqual(response.voteGranted, false, "an unvoted node still rejects a behind-on-term candidate");
  node.stop();
}

{
  // An empty log (no entries appended) behaves as lastLogIndex=0,
  // lastLogTerm=0 — any real candidate log is at least as up-to-date.
  const node = newNode("node-a");
  assertEqual(node.lastLogIndex, 0, "sanity: a fresh node's log starts empty (lastLogIndex 0)");
  assertEqual(node.lastLogTerm, 0, "sanity: a fresh node's log starts empty (lastLogTerm 0)");
  const response = node.receiveVoteRequest(voteRequest(1, "node-b", { lastLogTerm: 0, lastLogIndex: 0 }));
  assertEqual(response.voteGranted, true, "grants when both logs are empty (0/0 is trivially up-to-date)");
  node.stop();
}

// Builds an AppendEntries request that is a pure heartbeat (empty
// entries[], prevLogIndex/prevLogTerm/leaderCommit at their trivial
// "start of log" values) — used for tests that only care about the
// term/leadership-recognition side effects, mirroring what the old
// (now-removed) receiveHeartbeat()'s test cases exercised.
function heartbeat(term: number, leaderId: string): AppendEntriesRequest {
  return {
    term,
    leaderId,
    prevLogIndex: 0,
    prevLogTerm: 0,
    entries: [],
    leaderCommit: 0,
  };
}

// ---- receiveAppendEntries: heartbeat-equivalent behavior (empty entries[]) ----
// (Milestone 4's receiveHeartbeat() was removed in Milestone 5, Step 4 —
// an AppendEntries with empty entries[] already was a heartbeat, so these
// cases now exercise the real, unified method instead of a separate one.)
{
  const node = newNode("node-a");
  const response = node.receiveAppendEntries(heartbeat(1, "node-b"));
  assertEqual(response.term, 1, "follower adopts higher term from a heartbeat-equivalent AppendEntries");
  assertEqual(response.success, true, "an empty-entries AppendEntries with a trivially-consistent prevLogIndex succeeds");
  assertEqual(node.currentTerm, 1, "currentTerm updated from the heartbeat");
  assertEqual(node.state, "follower", "remains follower after a heartbeat at a higher term");
  node.stop();
}

{
  // Stale heartbeat (lower term) is ignored; responder reports its real term.
  const node = newNode("node-a");
  node.becomeCandidate(); // term 1
  node.becomeCandidate(); // term 2
  const response = node.receiveAppendEntries(heartbeat(1, "node-b"));
  assertEqual(response.term, 2, "stale heartbeat gets the node's real (higher) term back");
  assertEqual(response.success, false, "a stale (lower-term) AppendEntries is rejected");
  assertEqual(node.state, "candidate", "state unchanged by a stale heartbeat");
  node.stop();
}

{
  // A candidate receiving a same-term heartbeat steps down (someone else won).
  const node = newNode("node-a");
  node.becomeCandidate(); // term 1, candidate
  const response = node.receiveAppendEntries(heartbeat(1, "node-b"));
  assertEqual(response.term, 1, "same-term heartbeat response");
  assertEqual(response.success, true, "a same-term heartbeat with a trivially-consistent log succeeds");
  assertEqual(node.state, "follower", "candidate steps down on same-term heartbeat from a real leader");
  node.stop();
}

{
  // A leader receiving a heartbeat at a higher term steps down.
  const node = newNode("node-a");
  node.becomeCandidate();
  node.becomeLeader(["node-b", "node-c"]);
  const response = node.receiveAppendEntries(heartbeat(5, "node-b"));
  assertEqual(response.term, 5, "leader adopts higher term from heartbeat");
  assertEqual(response.success, true, "the heartbeat itself succeeds once the leader steps down");
  assertEqual(node.state, "follower", "leader steps down on higher-term heartbeat");
  node.stop();
}

// ---- receiveAppendEntries: consistency check + real append (DESIGN.md §5.3.3, Milestone 5 Step 4) ----
{
  // prevLogIndex === 0 is trivially consistent ("start of log") — a
  // follower with an empty log accepts entries starting from index 1.
  const node = newNode("node-a");
  const response = node.receiveAppendEntries({
    term: 1,
    leaderId: "node-b",
    prevLogIndex: 0,
    prevLogTerm: 0,
    entries: [{ term: 1, index: 1, command: { op: "put", key: "k", value: "v" } }],
    leaderCommit: 0,
  });
  assertEqual(response.success, true, "an empty follower accepts entries starting at index 1 (prevLogIndex 0 is trivial)");
  assertEqual(node.log.getEntry(1), { term: 1, index: 1, command: { op: "put", key: "k", value: "v" } }, "the entry is really appended to the follower's log");
  assertEqual(node.lastLogIndex, 1, "lastLogIndex reflects the real append");
  node.stop();
}

{
  // Consistency check fails: follower has no entry at all at prevLogIndex.
  const node = newNode("node-a"); // empty log
  const response = node.receiveAppendEntries({
    term: 1,
    leaderId: "node-b",
    prevLogIndex: 5, // follower has nothing here
    prevLogTerm: 1,
    entries: [],
    leaderCommit: 0,
  });
  assertEqual(response.success, false, "rejects when the follower has no entry at prevLogIndex at all");
  node.stop();
}

{
  // Consistency check fails: follower has an entry at prevLogIndex, but
  // with a different term than the leader expects.
  const node = newNode("node-a");
  node.log.append(1, { op: "put", key: "a", value: "1" }); // index 1, term 1
  const response = node.receiveAppendEntries({
    term: 2,
    leaderId: "node-b",
    prevLogIndex: 1,
    prevLogTerm: 2, // leader thinks index 1 is term 2; follower has term 1
    entries: [],
    leaderCommit: 0,
  });
  assertEqual(response.success, false, "rejects when the follower's entry at prevLogIndex has a conflicting term");
  assertEqual(node.log.getEntry(1)?.term, 1, "a failed consistency check does not touch the follower's existing log");
  node.stop();
}

{
  // Real conflict: follower has a stale/wrong entry at an index the
  // leader is now overwriting -> truncate from there, then append the
  // leader's version.
  const node = newNode("node-a");
  node.log.append(1, { op: "put", key: "a", value: "1" }); // index 1, term 1
  node.log.append(1, { op: "put", key: "stale", value: "wrong" }); // index 2, term 1 (conflicting)
  node.log.append(1, { op: "put", key: "also-stale", value: "wrong" }); // index 3, term 1 (must also be discarded)

  const response = node.receiveAppendEntries({
    term: 2,
    leaderId: "node-b",
    prevLogIndex: 1,
    prevLogTerm: 1, // matches follower's real index 1 -> consistency check passes
    entries: [{ term: 2, index: 2, command: { op: "put", key: "real", value: "correct" } }],
    leaderCommit: 0,
  });

  assertEqual(response.success, true, "a genuine conflict at the append point still succeeds after truncating");
  assertEqual(node.lastLogIndex, 2, "the stale entries (including index 3, past the conflict) are discarded");
  assertEqual(
    node.log.getEntry(2),
    { term: 2, index: 2, command: { op: "put", key: "real", value: "correct" } },
    "the leader's real entry replaces the conflicting one",
  );
  assertEqual(node.log.getEntry(3), undefined, "everything after the conflict point is gone, not just the conflicting entry itself");
  node.stop();
}

{
  // Explicitly requested case: a retried/duplicate AppendEntries carrying
  // entries the follower ALREADY has correctly (same term at that index)
  // must NOT trigger a truncation — only a genuine term conflict should.
  const node = newNode("node-a");
  node.log.append(1, { op: "put", key: "a", value: "1" }); // index 1
  node.log.append(2, { op: "put", key: "b", value: "2" }); // index 2

  const response = node.receiveAppendEntries({
    term: 2,
    leaderId: "node-b",
    prevLogIndex: 0,
    prevLogTerm: 0,
    // Same two entries the follower already has, at the same indices and
    // terms — a retried RPC delivering something already applied.
    entries: [
      { term: 1, index: 1, command: { op: "put", key: "a", value: "1" } },
      { term: 2, index: 2, command: { op: "put", key: "b", value: "2" } },
    ],
    leaderCommit: 0,
  });

  assertEqual(response.success, true, "a duplicate/retried AppendEntries with already-correct entries still succeeds");
  assertEqual(node.lastLogIndex, 2, "no entries are lost or duplicated by re-receiving an already-applied AppendEntries");
  assertEqual(
    node.log.getEntry(1),
    { term: 1, index: 1, command: { op: "put", key: "a", value: "1" } },
    "the original index-1 entry object's data is unchanged (not truncated and re-appended)",
  );
  assertEqual(
    node.log.getEntry(2),
    { term: 2, index: 2, command: { op: "put", key: "b", value: "2" } },
    "the original index-2 entry's data is unchanged (not truncated and re-appended)",
  );
  node.stop();
}

{
  // A mix: the first incoming entry already matches (should be skipped,
  // no truncation), but the second genuinely conflicts (should truncate
  // from THAT point, not from the first entry).
  const node = newNode("node-a");
  node.log.append(1, { op: "put", key: "a", value: "1" }); // index 1 — matches incoming
  node.log.append(1, { op: "put", key: "stale-b", value: "wrong" }); // index 2 — will conflict

  const response = node.receiveAppendEntries({
    term: 2,
    leaderId: "node-b",
    prevLogIndex: 0,
    prevLogTerm: 0,
    entries: [
      { term: 1, index: 1, command: { op: "put", key: "a", value: "1" } }, // matches -> skip, no truncation
      { term: 2, index: 2, command: { op: "put", key: "b", value: "2" } }, // conflicts -> truncate from index 2
    ],
    leaderCommit: 0,
  });

  assertEqual(response.success, true, "mixed matching-then-conflicting entries still succeed overall");
  assertEqual(
    node.log.getEntry(2),
    { term: 2, index: 2, command: { op: "put", key: "b", value: "2" } },
    "only the genuinely conflicting entry (index 2) is replaced",
  );
  assertEqual(node.lastLogIndex, 2, "log length reflects only the real conflict resolution, not a truncation at the matching entry");
  node.stop();
}

{
  // Appending past the end of an existing log: some entries are new
  // (nothing exists there yet), handled the same as an empty log.
  const node = newNode("node-a");
  node.log.append(1, { op: "put", key: "a", value: "1" }); // index 1

  const response = node.receiveAppendEntries({
    term: 1,
    leaderId: "node-b",
    prevLogIndex: 1,
    prevLogTerm: 1,
    entries: [
      { term: 1, index: 2, command: { op: "put", key: "b", value: "2" } },
      { term: 1, index: 3, command: { op: "delete", key: "a" } },
    ],
    leaderCommit: 0,
  });

  assertEqual(response.success, true, "appending genuinely new entries past the end of the log succeeds");
  assertEqual(node.lastLogIndex, 3, "the follower's log grows to include the new entries");
  assertEqual(node.log.getEntry(3), { term: 1, index: 3, command: { op: "delete", key: "a" } }, "the new entries are really present");
  node.stop();
}

// ---- Election timeout hook fires ----
{
  let fired = false;
  const node = new RaftNode({
    nodeId: "node-a",
    randomFn: () => 0, // minimum timeout (150ms) for a faster test
    onElectionTimeout: () => {
      fired = true;
    },
  });

  await new Promise((resolve) => setTimeout(resolve, 250));
  assert(fired, "onElectionTimeout fires after the election timeout elapses with no activity");
  node.stop();
}

{
  // Receiving a heartbeat resets the timer and prevents timeout firing.
  let fired = false;
  const node = new RaftNode({
    nodeId: "node-a",
    randomFn: () => 0,
    onElectionTimeout: () => {
      fired = true;
    },
  });

  const interval = setInterval(() => {
    node.receiveAppendEntries(heartbeat(0, "node-b"));
  }, 50);

  await new Promise((resolve) => setTimeout(resolve, 250));
  clearInterval(interval);
  assert(!fired, "onElectionTimeout does not fire while heartbeats keep resetting the timer");
  node.stop();
}

// ---- Heartbeat timer hook fires only while leader ----
{
  let heartbeatCount = 0;
  const node = new RaftNode({
    nodeId: "node-a",
    randomFn: () => 0.5,
    onHeartbeatDue: () => {
      heartbeatCount++;
    },
  });

  await new Promise((resolve) => setTimeout(resolve, 120));
  assertEqual(heartbeatCount, 0, "onHeartbeatDue does not fire while a follower");

  node.becomeCandidate();
  node.becomeLeader(["node-b", "node-c"]);
  await new Promise((resolve) => setTimeout(resolve, 120));
  assert(heartbeatCount > 0, "onHeartbeatDue fires repeatedly once node becomes leader");

  const countAfterLeader = heartbeatCount;
  node.becomeFollower(node.currentTerm + 1);
  await new Promise((resolve) => setTimeout(resolve, 120));
  assertEqual(
    heartbeatCount,
    countAfterLeader,
    "onHeartbeatDue stops firing after stepping down from leader",
  );

  node.stop();
}

// ---- stop() cleans up so the process can exit ----
{
  const node = newNode("node-a");
  node.becomeCandidate();
  node.becomeLeader(["node-b", "node-c"]);
  node.stop();
  // No explicit assertion beyond "process exits" — if timers weren't
  // cleared, this script would hang instead of reaching the summary below.
  passed++;
}

// ---- commitIndex: initial state ----
{
  const node = newNode("node-a");
  assertEqual(node.commitIndex, 0, "a fresh node starts with commitIndex 0");
  node.stop();
}

// ---- recomputeCommitIndex: no-op when not leader ----
{
  const node = newNode("node-a"); // follower
  node.log.append(1, { op: "put", key: "a", value: "1" });
  node.recomputeCommitIndex();
  assertEqual(node.commitIndex, 0, "recomputeCommitIndex is a no-op for a follower (no replication state to compute a majority over)");
  node.stop();
}

{
  const node = newNode("node-a");
  node.log.append(1, { op: "put", key: "a", value: "1" });
  node.becomeCandidate(); // candidate, not leader
  node.recomputeCommitIndex();
  assertEqual(node.commitIndex, 0, "recomputeCommitIndex is a no-op for a candidate");
  node.stop();
}

// ---- recomputeCommitIndex: basic majority advancement ----
{
  // 3-node cluster (self + 2 peers): majority is 2.
  const node = newNode("node-a");
  node.becomeCandidate(); // term 1
  node.becomeLeader(["node-b", "node-c"]);
  node.log.append(1, { op: "put", key: "a", value: "1" }); // index 1, current term

  assertEqual(node.commitIndex, 0, "sanity: commitIndex has not advanced yet (no peer has replicated)");

  node.advanceReplicationOnSuccess("node-b", 1); // one peer catches up -> self+node-b = 2 = majority
  node.recomputeCommitIndex();

  assertEqual(node.commitIndex, 1, "commitIndex advances once a majority (self + one peer) has replicated a current-term entry");
  node.stop();
}

{
  // Same setup, but only self has the entry (no peer has replicated it yet) -> no majority.
  const node = newNode("node-a");
  node.becomeCandidate();
  node.becomeLeader(["node-b", "node-c"]);
  node.log.append(1, { op: "put", key: "a", value: "1" });

  node.recomputeCommitIndex();

  assertEqual(node.commitIndex, 0, "commitIndex does not advance without a majority (self alone is 1/3, not a majority)");
  node.stop();
}

{
  // Picks the HIGHEST qualifying index, not just any majority-satisfying one.
  const node = newNode("node-a");
  node.becomeCandidate();
  node.becomeLeader(["node-b", "node-c"]);
  node.log.append(1, { op: "put", key: "a", value: "1" }); // index 1
  node.log.append(1, { op: "put", key: "b", value: "2" }); // index 2
  node.log.append(1, { op: "put", key: "c", value: "3" }); // index 3

  node.advanceReplicationOnSuccess("node-b", 3); // node-b fully caught up
  node.recomputeCommitIndex();

  assertEqual(node.commitIndex, 3, "commitIndex advances to the highest index with a majority, not just the lowest qualifying one");
  node.stop();
}

// ---- recomputeCommitIndex: THE critical safety rule (DESIGN.md, term-matching restriction) ----
// Explicitly requested dedicated coverage: a leader must NEVER count an
// entry from an EARLIER term as committed purely from replica counts,
// even with a full majority replicating it. Only a current-term entry
// can trigger a NEW commitIndex advancement (Raft paper §5.4.2). This is
// the single most likely place for a subtle, silent bug (e.g. the
// entry.term !== this._currentTerm check being dropped or inverted), so
// these cases are deliberately isolated rather than folded into the
// general majority tests above.
{
  // An entry from an OLDER term has a full majority replicated, but the
  // leader is now in a LATER term with no current-term entry yet.
  // commitIndex must stay at 0 despite the majority.
  const node = newNode("node-a");
  node.log.append(1, { op: "put", key: "a", value: "1" }); // index 1, term 1 (as if inherited from a prior leader's term)
  node.becomeCandidate(); // term 1
  node.becomeCandidate(); // term 2 -- currentTerm is now 2, but the entry is still term 1
  node.becomeLeader(["node-b"]); // 2-node cluster, majority = 2

  node.advanceReplicationOnSuccess("node-b", 1); // full majority (self + node-b) has index 1

  assertEqual(node.currentTerm, 2, "sanity: leader's current term (2) is ahead of the entry's term (1)");

  node.recomputeCommitIndex();

  assertEqual(
    node.commitIndex,
    0,
    "CRITICAL: a leader must not advance commitIndex on a majority-replicated entry from an EARLIER term than its own — only a current-term entry can trigger a new commit",
  );
  node.stop();
}

{
  // Same setup as above, but now a real current-term entry is appended
  // and ALSO reaches a majority -> commitIndex advances to that higher
  // index, which correctly covers the earlier-term entry too (as an
  // indirect consequence, per the Raft paper), even though the earlier
  // entry could never trigger the advancement by itself.
  const node = newNode("node-a");
  node.log.append(1, { op: "put", key: "a", value: "1" }); // index 1, term 1
  node.becomeCandidate(); // term 1
  node.becomeCandidate(); // term 2
  node.becomeLeader(["node-b"]);
  node.log.append(2, { op: "put", key: "b", value: "2" }); // index 2, CURRENT term (2)

  node.advanceReplicationOnSuccess("node-b", 2); // majority has both index 1 and index 2

  node.recomputeCommitIndex();

  assertEqual(
    node.commitIndex,
    2,
    "a majority-replicated CURRENT-term entry (index 2) advances commitIndex, indirectly covering the earlier-term entry below it",
  );
  node.stop();
}

{
  // A majority replicates an old-term entry (index 1) but NOT a newer
  // current-term entry (index 2, only self has it) -> commitIndex must
  // stay at 0, not jump to 1 just because index 1 has a majority.
  const node = newNode("node-a");
  node.log.append(1, { op: "put", key: "a", value: "1" }); // index 1, term 1
  node.becomeCandidate();
  node.becomeCandidate(); // term 2
  node.becomeLeader(["node-b"]);
  node.log.append(2, { op: "put", key: "b", value: "2" }); // index 2, term 2 -- NOT yet replicated to node-b

  node.advanceReplicationOnSuccess("node-b", 1); // node-b only has index 1 (the old-term entry), not index 2

  node.recomputeCommitIndex();

  assertEqual(
    node.commitIndex,
    0,
    "commitIndex stays at 0: index 1 has a majority but is old-term (can't directly commit), and index 2 (current-term) lacks a majority",
  );
  node.stop();
}

// ---- recomputeCommitIndex: never regresses ----
{
  const node = newNode("node-a");
  node.becomeCandidate();
  node.becomeLeader(["node-b"]);
  node.log.append(1, { op: "put", key: "a", value: "1" });
  node.advanceReplicationOnSuccess("node-b", 1);
  node.recomputeCommitIndex();
  assertEqual(node.commitIndex, 1, "sanity: commitIndex advanced to 1");

  // A subsequent recompute with no further replication progress must not
  // regress or otherwise change an already-advanced commitIndex.
  node.recomputeCommitIndex();
  assertEqual(node.commitIndex, 1, "calling recomputeCommitIndex again with no new progress leaves commitIndex unchanged");
  node.stop();
}

// ---- commitIndex persists across leadership changes ----
{
  const node = newNode("node-a");
  node.becomeCandidate();
  node.becomeLeader(["node-b"]);
  node.log.append(1, { op: "put", key: "a", value: "1" });
  node.advanceReplicationOnSuccess("node-b", 1);
  node.recomputeCommitIndex();
  assertEqual(node.commitIndex, 1, "sanity: commitIndex advanced while leader");

  node.becomeFollower(node.currentTerm + 1);
  assertEqual(node.commitIndex, 1, "commitIndex is NOT reset when stepping down from leader (it reflects a durable fact about the log, not leader-specific state)");
  node.stop();
}

// ---- receiveAppendEntries: follower-side commitIndex adoption from leaderCommit ----
{
  const node = newNode("node-a"); // empty log, follower
  const response = node.receiveAppendEntries({
    term: 1,
    leaderId: "node-b",
    prevLogIndex: 0,
    prevLogTerm: 0,
    entries: [
      { term: 1, index: 1, command: { op: "put", key: "a", value: "1" } },
      { term: 1, index: 2, command: { op: "put", key: "b", value: "2" } },
    ],
    leaderCommit: 1, // leader has only committed up through index 1
  });

  assertEqual(response.success, true, "sanity: the request succeeds");
  assertEqual(node.commitIndex, 1, "follower adopts leaderCommit (1), not the index of the last entry it just appended (2)");
  node.stop();
}

{
  // leaderCommit capped at the index of the last entry in THIS request,
  // even if the leader's own commitIndex is further ahead than that —
  // the follower must never mark something committed that it hasn't
  // actually confirmed matches the leader's log yet.
  const node = newNode("node-a");
  const response = node.receiveAppendEntries({
    term: 1,
    leaderId: "node-b",
    prevLogIndex: 0,
    prevLogTerm: 0,
    entries: [{ term: 1, index: 1, command: { op: "put", key: "a", value: "1" } }],
    leaderCommit: 50, // leader claims much more progress than this batch contains
  });

  assertEqual(response.success, true, "sanity: the request succeeds");
  assertEqual(node.commitIndex, 1, "commitIndex is capped at the index of the last entry actually included in this request (1), not the raw leaderCommit value (50)");
  node.stop();
}

{
  // commitIndex only advances forward, never backward, even if a later
  // (e.g. retried/reordered) request reports a lower leaderCommit.
  const node = newNode("node-a");
  node.receiveAppendEntries({
    term: 1,
    leaderId: "node-b",
    prevLogIndex: 0,
    prevLogTerm: 0,
    entries: [{ term: 1, index: 1, command: { op: "put", key: "a", value: "1" } }],
    leaderCommit: 1,
  });
  assertEqual(node.commitIndex, 1, "sanity: commitIndex advanced to 1");

  node.receiveAppendEntries({
    term: 1,
    leaderId: "node-b",
    prevLogIndex: 1,
    prevLogTerm: 1,
    entries: [],
    leaderCommit: 0, // stale/reordered — must not regress the follower's commitIndex
  });
  assertEqual(node.commitIndex, 1, "a lower leaderCommit in a later request does not regress the follower's commitIndex");
  node.stop();
}

// ---- currentLeaderId (Milestone 5, Step 6.2) ----
{
  const node = newNode("node-a");
  assertEqual(node.currentLeaderId, null, "a fresh node has no known leader");
  node.stop();
}

{
  // A real AppendEntries that passes the term check sets currentLeaderId,
  // even in the trivial case where this node was already a follower at
  // the same term.
  const node = newNode("node-a");
  node.receiveAppendEntries(heartbeat(1, "node-b"));
  assertEqual(node.currentLeaderId, "node-b", "receiveAppendEntries adopts the sender as the known leader");
  node.stop();
}

{
  // Adopted even when the AppendEntries also causes a step-down from
  // candidate/leader (higher-term case).
  const node = newNode("node-a");
  node.becomeCandidate();
  node.becomeLeader(["node-b"]);
  assertEqual(node.currentLeaderId, "node-a", "sanity: a leader considers itself the leader");

  node.receiveAppendEntries(heartbeat(5, "node-c"));
  assertEqual(node.currentLeaderId, "node-c", "currentLeaderId updates to the new leader on a higher-term AppendEntries");
  node.stop();
}

{
  // currentLeaderId is adopted even when the request's CONSISTENCY CHECK
  // fails (success: false) — the sender is still genuinely the leader,
  // it just couldn't append this particular batch.
  const node = newNode("node-a"); // empty log
  const response = node.receiveAppendEntries({
    term: 1,
    leaderId: "node-b",
    prevLogIndex: 5, // this follower has nothing here -> consistency check fails
    prevLogTerm: 1,
    entries: [],
    leaderCommit: 0,
  });
  assertEqual(response.success, false, "sanity: the consistency check genuinely failed");
  assertEqual(node.currentLeaderId, "node-b", "currentLeaderId is still adopted even when this particular AppendEntries is rejected");
  node.stop();
}

{
  // A STALE (lower-term) AppendEntries must NOT update currentLeaderId —
  // it's rejected outright before the term check "passes".
  const node = newNode("node-a");
  node.becomeCandidate(); // term 1
  node.becomeCandidate(); // term 2
  const response = node.receiveAppendEntries(heartbeat(1, "node-stale"));
  assertEqual(response.success, false, "sanity: the stale request is rejected");
  assertEqual(node.currentLeaderId, null, "a stale (lower-term) AppendEntries does not update currentLeaderId");
  node.stop();
}

{
  // becomeCandidate() clears currentLeaderId — no leader is known during
  // an election in progress, even if one was known before it started.
  const node = newNode("node-a");
  node.receiveAppendEntries(heartbeat(1, "node-b"));
  assertEqual(node.currentLeaderId, "node-b", "sanity: a leader was known before the election");

  node.becomeCandidate();
  assertEqual(node.currentLeaderId, null, "becomeCandidate clears currentLeaderId");
  node.stop();
}

{
  // becomeLeader() sets currentLeaderId to self.
  const node = newNode("node-a");
  node.becomeCandidate();
  node.becomeLeader(["node-b"]);
  assertEqual(node.currentLeaderId, "node-a", "becomeLeader sets currentLeaderId to this node's own id");
  node.stop();
}

{
  // EXPLICITLY REQUESTED CASE: a RequestVote that causes this node to
  // step down (via becomeFollower, seeing a higher term) must NOT be
  // treated as evidence of a leader. A candidacy proves someone is
  // running for leadership, not that they won — currentLeaderId stays
  // stale/null in this window, which is correct per Raft's actual
  // guarantees (a node genuinely doesn't know the new leader until that
  // leader's own AppendEntries arrives), not a limitation of this
  // implementation.
  const node = newNode("node-a");
  node.receiveAppendEntries(heartbeat(1, "node-old-leader"));
  assertEqual(node.currentLeaderId, "node-old-leader", "sanity: a leader was known before the higher-term vote request");

  const voteResponse = node.receiveVoteRequest(voteRequest(2, "node-candidate"));
  assertEqual(voteResponse.term, 2, "sanity: the node stepped down to the higher term");
  assertEqual(
    node.currentLeaderId,
    "node-old-leader",
    "a RequestVote causing step-down does NOT update currentLeaderId — it's stale until a real AppendEntries from the actual (possibly different) new leader arrives, not updated to the candidate that merely requested a vote",
  );
  node.stop();
}

{
  // Continuing the above scenario: once the real winner's AppendEntries
  // arrives, currentLeaderId finally updates — demonstrating the window
  // closes correctly, not just that it opens correctly.
  const node = newNode("node-a");
  node.receiveAppendEntries(heartbeat(1, "node-old-leader"));
  node.receiveVoteRequest(voteRequest(2, "node-candidate"));
  assertEqual(node.currentLeaderId, "node-old-leader", "sanity: still stale immediately after the vote request");

  // The candidate that requested the vote goes on to win and sends a
  // real AppendEntries — NOTE this need not even be the same node that
  // requested the vote, illustrating why adopting the candidate directly
  // would have been actively wrong.
  node.receiveAppendEntries(heartbeat(2, "node-actual-winner"));
  assertEqual(
    node.currentLeaderId,
    "node-actual-winner",
    "currentLeaderId updates once a real AppendEntries from the actual winner arrives, closing the stale window",
  );
  node.stop();
}

console.log(`\n${passed} passed, ${failed} failed`);
if (failed > 0) {
  process.exit(1);
}