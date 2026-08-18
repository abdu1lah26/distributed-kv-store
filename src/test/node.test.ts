// Unit tests for src/raft/node.ts (Milestone 4, Step 1).
// Plain Node script, run via `npx tsx src/test/node.test.ts` — no test
// framework, per this project's established convention (§9 / Milestone 2).

import { RaftNode } from "../raft/node.js";

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
    actual === expected,
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
  node.becomeLeader();
  assertEqual(node.state, "leader", "becomeLeader sets state to leader");
  assertEqual(node.currentTerm, 1, "becomeLeader does not change term");
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
  node.becomeLeader();
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

// ---- receiveVoteRequest: log up-to-date check (DESIGN.md §5.3.1, Milestone 5 Step 1) ----
// Injects fake lastLogIndex/lastLogTerm via the constructor option, ahead
// of any real log module (that's Milestone 5, Step 2) — the whole point
// of this step being testable in isolation.
{
  // Candidate's lastLogTerm is lower than the voter's -> reject, even
  // though the voter hasn't voted this term and the request term is fine.
  const node = new RaftNode({ nodeId: "node-a", randomFn: () => 0.5, lastLogTerm: 5, lastLogIndex: 10 });
  const response = node.receiveVoteRequest(voteRequest(1, "node-b", { lastLogTerm: 4, lastLogIndex: 999 }));
  assertEqual(response.voteGranted, false, "rejects a candidate whose lastLogTerm is behind, regardless of lastLogIndex");
  assertEqual(node.votedFor, null, "votedFor is not set when the candidate's log is behind");
  node.stop();
}

{
  // Equal lastLogTerm, but candidate's lastLogIndex is behind -> reject.
  const node = new RaftNode({ nodeId: "node-a", randomFn: () => 0.5, lastLogTerm: 5, lastLogIndex: 10 });
  const response = node.receiveVoteRequest(voteRequest(1, "node-b", { lastLogTerm: 5, lastLogIndex: 9 }));
  assertEqual(response.voteGranted, false, "rejects a candidate with the same lastLogTerm but a lower lastLogIndex");
  node.stop();
}

{
  // Equal lastLogTerm and equal lastLogIndex -> "at least as up-to-date", grant.
  const node = new RaftNode({ nodeId: "node-a", randomFn: () => 0.5, lastLogTerm: 5, lastLogIndex: 10 });
  const response = node.receiveVoteRequest(voteRequest(1, "node-b", { lastLogTerm: 5, lastLogIndex: 10 }));
  assertEqual(response.voteGranted, true, "grants when candidate's log is exactly as up-to-date (equal term and index)");
  node.stop();
}

{
  // Candidate's lastLogTerm is higher -> grant, even with a lower lastLogIndex
  // (higher term wins outright per the Raft paper's comparison rule).
  const node = new RaftNode({ nodeId: "node-a", randomFn: () => 0.5, lastLogTerm: 5, lastLogIndex: 10 });
  const response = node.receiveVoteRequest(voteRequest(1, "node-b", { lastLogTerm: 6, lastLogIndex: 0 }));
  assertEqual(response.voteGranted, true, "grants when candidate's lastLogTerm is higher, even with a lower lastLogIndex");
  node.stop();
}

{
  // Equal lastLogTerm, candidate's lastLogIndex is ahead -> grant.
  const node = new RaftNode({ nodeId: "node-a", randomFn: () => 0.5, lastLogTerm: 5, lastLogIndex: 10 });
  const response = node.receiveVoteRequest(voteRequest(1, "node-b", { lastLogTerm: 5, lastLogIndex: 11 }));
  assertEqual(response.voteGranted, true, "grants when candidate's lastLogIndex is ahead at the same lastLogTerm");
  node.stop();
}

{
  // The up-to-date check applies even when votedFor is null (first-ever
  // request this term) -- being unvoted does not bypass the log check.
  const node = new RaftNode({ nodeId: "node-a", randomFn: () => 0.5, lastLogTerm: 3, lastLogIndex: 5 });
  assertEqual(node.votedFor, null, "sanity: node has not voted yet");
  const response = node.receiveVoteRequest(voteRequest(1, "node-b", { lastLogTerm: 2, lastLogIndex: 100 }));
  assertEqual(response.voteGranted, false, "an unvoted node still rejects a behind-on-term candidate");
  node.stop();
}

{
  // setLastLogInfo() lets a node's log state change after construction,
  // simulating what Milestone 5, Step 2's real log module will drive.
  const node = new RaftNode({ nodeId: "node-a", randomFn: () => 0.5 }); // starts at 0/0
  node.setLastLogInfo(7, 3);
  const behind = node.receiveVoteRequest(voteRequest(1, "node-b", { lastLogTerm: 2, lastLogIndex: 999 }));
  assertEqual(behind.voteGranted, false, "setLastLogInfo() updates the state the up-to-date check compares against");
  node.stop();
}

// ---- receiveHeartbeat ----
{
  const node = newNode("node-a");
  const response = node.receiveHeartbeat({ term: 1, leaderId: "node-b" });
  assertEqual(response.term, 1, "follower adopts higher term from heartbeat");
  assertEqual(node.currentTerm, 1, "currentTerm updated from heartbeat");
  assertEqual(node.state, "follower", "remains follower after heartbeat at higher term");
  node.stop();
}

{
  // Stale heartbeat (lower term) is ignored; responder reports its real term.
  const node = newNode("node-a");
  node.becomeCandidate(); // term 1
  node.becomeCandidate(); // term 2
  const response = node.receiveHeartbeat({ term: 1, leaderId: "node-b" });
  assertEqual(response.term, 2, "stale heartbeat gets the node's real (higher) term back");
  assertEqual(node.state, "candidate", "state unchanged by a stale heartbeat");
  node.stop();
}

{
  // A candidate receiving a same-term heartbeat steps down (someone else won).
  const node = newNode("node-a");
  node.becomeCandidate(); // term 1, candidate
  const response = node.receiveHeartbeat({ term: 1, leaderId: "node-b" });
  assertEqual(response.term, 1, "same-term heartbeat response");
  assertEqual(node.state, "follower", "candidate steps down on same-term heartbeat from a real leader");
  node.stop();
}

{
  // A leader receiving a heartbeat at a higher term steps down.
  const node = newNode("node-a");
  node.becomeCandidate();
  node.becomeLeader();
  const response = node.receiveHeartbeat({ term: 5, leaderId: "node-b" });
  assertEqual(response.term, 5, "leader adopts higher term from heartbeat");
  assertEqual(node.state, "follower", "leader steps down on higher-term heartbeat");
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
    node.receiveHeartbeat({ term: 0, leaderId: "node-b" });
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
  node.becomeLeader();
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
  node.becomeLeader();
  node.stop();
  // No explicit assertion beyond "process exits" — if timers weren't
  // cleared, this script would hang instead of reaching the summary below.
  passed++;
}

console.log(`\n${passed} passed, ${failed} failed`);
if (failed > 0) {
  process.exit(1);
}