// Tests for src/raft/rpc.ts (Milestone 4, Steps 3-4).
// Plain Node script, run via `npx tsx src/test/rpc.test.ts` — no test
// framework, per this project's established convention (§9).
//
// Route-level behavior tested via supertest against createRaftApp
// (same pattern as test/api.test.ts, Milestone 3). requestVoteFromPeer,
// sendHeartbeatToPeer, runElection, and sendHeartbeats are all tested
// against real spun-up servers on real ports, not mocked fetch, per the
// agreed plan.

import http from "node:http";
import request from "supertest";
import { RaftNode } from "../raft/node.js";
import {
  createRaftApp,
  requestVoteFromPeer,
  runElection,
  sendHeartbeatToPeer,
  sendHeartbeats,
} from "../raft/rpc.js";
import type { AppendEntriesRequest } from "../raft/rpc.js";
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

function newNode(nodeId: string): RaftNode {
  return new RaftNode({ nodeId, randomFn: () => 0.5 });
}

// Spins up a real server for a RaftNode on an ephemeral port ("port 0"
// lets the OS choose a free one, avoiding collisions between test runs
// or with anything else running locally).
async function listen(node: RaftNode): Promise<{ server: http.Server; peer: PeerConfig }> {
  const app = createRaftApp(node);
  const server = http.createServer(app);
  await new Promise<void>((resolve) => server.listen(0, "localhost", resolve));
  const address = server.address();
  if (address === null || typeof address === "string") {
    throw new Error("Expected server.address() to return an AddressInfo.");
  }
  return {
    server,
    peer: { id: node.nodeId, host: "localhost", port: address.port },
  };
}

function closeAll(servers: http.Server[]): Promise<void[]> {
  return Promise.all(servers.map((s) => new Promise<void>((resolve) => s.close(() => resolve()))));
}

// ==== Route-level tests via supertest ====

{
  const node = newNode("node-a");
  const app = createRaftApp(node);
  const res = await request(app)
    .post("/raft/request-vote")
    .send({ term: 1, candidateId: "node-b" });

  assertEqual(res.status, 200, "request-vote route returns 200 on a well-formed body");
  assertEqual(res.body, { term: 1, voteGranted: true }, "route returns the node's vote decision");
  node.stop();
}

{
  const node = newNode("node-a");
  const app = createRaftApp(node);
  const res = await request(app).post("/raft/request-vote").send({ term: "not-a-number" });

  assertEqual(res.status, 400, "route returns 400 on a malformed body (wrong types)");
  node.stop();
}

{
  const node = newNode("node-a");
  const app = createRaftApp(node);
  const res = await request(app).post("/raft/request-vote").send({});

  assertEqual(res.status, 400, "route returns 400 on a body missing required fields");
  node.stop();
}

{
  // Second candidate in the same term is refused, exercised over the route.
  const node = newNode("node-a");
  const app = createRaftApp(node);
  await request(app).post("/raft/request-vote").send({ term: 1, candidateId: "node-b" });
  const res = await request(app).post("/raft/request-vote").send({ term: 1, candidateId: "node-c" });

  assertEqual(res.body.voteGranted, false, "route refuses a second candidate in the same term");
  node.stop();
}

// ==== requestVoteFromPeer against a real server ====

{
  const node = newNode("node-a");
  const { server, peer } = await listen(node);

  const response = await requestVoteFromPeer(peer, { term: 1, candidateId: "node-b" });
  assertEqual(response, { term: 1, voteGranted: true }, "requestVoteFromPeer gets a real vote grant over HTTP");

  await closeAll([server]);
  node.stop();
}

{
  // Unreachable peer (nothing listening on this port) returns null, not a throw.
  const response = await requestVoteFromPeer(
    { id: "ghost", host: "localhost", port: 1 }, // port 1 is reserved/unlikely to be listening
    { term: 1, candidateId: "node-b" },
  );
  assertEqual(response, null, "requestVoteFromPeer returns null for an unreachable peer instead of throwing");
}

// ==== runElection: majority granted -> becomes leader ====

{
  const candidate = newNode("node-a");
  const voterB = newNode("node-b");
  const voterC = newNode("node-c");

  const [{ server: serverB, peer: peerB }, { server: serverC, peer: peerC }] = await Promise.all([
    listen(voterB),
    listen(voterC),
  ]);

  await runElection(candidate, [peerB, peerC]);

  assertEqual(candidate.state, "leader", "candidate becomes leader after winning a unanimous vote (3/3)");
  assertEqual(voterB.votedFor, "node-a", "voter B recorded its vote for the candidate");
  assertEqual(voterC.votedFor, "node-a", "voter C recorded its vote for the candidate");

  await closeAll([serverB, serverC]);
  candidate.stop();
  voterB.stop();
  voterC.stop();
}

// ==== runElection: majority still reached with one unreachable peer ====

{
  const candidate = newNode("node-a");
  const voterB = newNode("node-b");

  const { server: serverB, peer: peerB } = await listen(voterB);
  const unreachablePeer: PeerConfig = { id: "node-c", host: "localhost", port: 1 };

  await runElection(candidate, [peerB, unreachablePeer]);

  assertEqual(
    candidate.state,
    "leader",
    "candidate becomes leader with 2/3 votes (self + one real voter) even though the third peer is unreachable",
  );

  await closeAll([serverB]);
  candidate.stop();
  voterB.stop();
}

// ==== runElection: no majority -> stays candidate ====

{
  const candidate = newNode("node-a");
  // Both peers unreachable: only the self-vote counts, 1/3 is not a majority.
  const unreachableB: PeerConfig = { id: "node-b", host: "localhost", port: 1 };
  const unreachableC: PeerConfig = { id: "node-c", host: "localhost", port: 2 };

  await runElection(candidate, [unreachableB, unreachableC]);

  assertEqual(
    candidate.state,
    "candidate",
    "candidate remains candidate (not leader) when it fails to reach a majority",
  );

  candidate.stop();
}

// ==== runElection: a voter already committed to another candidate in the same term refuses ====

{
  const candidate = newNode("node-a");
  const voterB = newNode("node-b");

  const { server: serverB, peer: peerB } = await listen(voterB);

  // voterB already voted for a different candidate in term 1 before the election runs.
  voterB.receiveVoteRequest({ term: 1, candidateId: "node-z" });

  await runElection(candidate, [peerB]);

  // candidate's becomeCandidate() bumps it to term 1 as well (starting from term 0),
  // so this exercises the real same-term conflict.
  assertEqual(
    candidate.state,
    "candidate",
    "candidate fails to win when its only peer already voted for someone else in the same term",
  );

  await closeAll([serverB]);
  candidate.stop();
  voterB.stop();
}

// ==== runElection: stale in-flight election does not become leader (race guard) ====

{
  // Exercises runElection's stillValidCandidate guard for real: a real
  // peer server whose /raft/request-vote handler deliberately delays
  // before responding (long enough to be slower than the concurrent
  // heartbeat below, but still under PEER_REQUEST_TIMEOUT_MS so it
  // resolves with a genuine grant rather than a null/timeout). While
  // runElection is awaiting that slow response, a concurrent heartbeat
  // at a higher term lands on the candidate directly — modeling a real
  // leader's message arriving mid-election — stepping it down before
  // the vote response (a majority-granting one) comes back.
  const candidate = newNode("node-a");
  const voterB = newNode("node-b");

  // Typecast the app so TypeScript accepts it in the raw http.createServer
  const app = createRaftApp(voterB) as unknown as http.RequestListener;
  
  const server = http.createServer((req, res) => {
    if (req.url === "/raft/request-vote") {
      // Delay the request before passing it to the Express app
      setTimeout(() => app(req, res), 40);
    } else {
      app(req, res);
    }
  });

  await new Promise<void>((resolve) => server.listen(0, "localhost", resolve));
  const address = server.address();
  if (address === null || typeof address === "string") {
    throw new Error("Expected server.address() to return an AddressInfo.");
  }
  const peerB: PeerConfig = { id: "node-b", host: "localhost", port: address.port };

  const electionPromise = runElection(candidate, [peerB]);

  // Land the concurrent heartbeat shortly after runElection has started
  // (and therefore after becomeCandidate() already ran) but before the
  // slow peer response resolves.
  await new Promise((resolve) => setTimeout(resolve, 10));
  candidate.receiveHeartbeat({ term: 99, leaderId: "node-z" });

  await electionPromise;

  assertEqual(
    candidate.state,
    "follower",
    "candidate that was stepped down mid-election by a concurrent higher-term heartbeat does NOT become leader, even though its vote response (once it arrives) would have been a majority",
  );
  assertEqual(candidate.currentTerm, 99, "candidate's term reflects the concurrent heartbeat, not the stale election term");

  await new Promise<void>((resolve) => server.close(() => resolve()));
  candidate.stop();
  voterB.stop();
}

// ==== AppendEntries route-level tests via supertest ====

function heartbeatBody(overrides: Partial<AppendEntriesRequest> = {}): AppendEntriesRequest {
  return {
    term: 1,
    leaderId: "node-b",
    prevLogIndex: 0,
    prevLogTerm: 0,
    entries: [],
    leaderCommit: 0,
    ...overrides,
  };
}

{
  const node = newNode("node-a");
  const app = createRaftApp(node);
  const res = await request(app).post("/raft/append-entries").send(heartbeatBody());

  assertEqual(res.status, 200, "append-entries route returns 200 on a well-formed heartbeat body");
  assertEqual(res.body, { term: 1 }, "route returns the node's post-heartbeat term");
  assertEqual(node.state, "follower", "node remains/becomes follower after a valid heartbeat");
  node.stop();
}

{
  const node = newNode("node-a");
  const app = createRaftApp(node);
  const res = await request(app)
    .post("/raft/append-entries")
    .send({ term: "not-a-number", leaderId: "node-b", entries: [] });

  assertEqual(res.status, 400, "append-entries route returns 400 when term has the wrong type");
  node.stop();
}

{
  const node = newNode("node-a");
  const app = createRaftApp(node);
  const res = await request(app)
    .post("/raft/append-entries")
    .send({ term: 1, leaderId: "node-b" }); // entries missing entirely

  assertEqual(res.status, 400, "append-entries route returns 400 when entries[] is missing");
  node.stop();
}

{
  const node = newNode("node-a");
  const app = createRaftApp(node);
  const res = await request(app)
    .post("/raft/append-entries")
    .send(heartbeatBody({ entries: "not-an-array" as unknown as unknown[] }));

  assertEqual(res.status, 400, "append-entries route returns 400 when entries is present but not an array");
  node.stop();
}

{
  // prevLogIndex/prevLogTerm/leaderCommit are accepted but ignored per §5.3.1 —
  // a request omitting them (beyond the required term/leaderId/entries) should
  // still succeed, since M4 doesn't read them.
  const node = newNode("node-a");
  const app = createRaftApp(node);
  const res = await request(app)
    .post("/raft/append-entries")
    .send({ term: 1, leaderId: "node-b", entries: [] });

  assertEqual(res.status, 200, "append-entries succeeds even without prevLogIndex/prevLogTerm/leaderCommit, since M4 ignores them");
  node.stop();
}

// ==== sendHeartbeatToPeer against a real server ====

{
  const node = newNode("node-a");
  const { server, peer } = await listen(node);

  const response = await sendHeartbeatToPeer(peer, heartbeatBody({ term: 1, leaderId: "node-b" }));
  assertEqual(response, { term: 1 }, "sendHeartbeatToPeer gets a real response over HTTP");

  await closeAll([server]);
  node.stop();
}

{
  const response = await sendHeartbeatToPeer(
    { id: "ghost", host: "localhost", port: 1 },
    heartbeatBody(),
  );
  assertEqual(response, null, "sendHeartbeatToPeer returns null for an unreachable peer instead of throwing");
}

// ==== sendHeartbeats: leader steps down when a peer reports a higher term ====

{
  // Mirror case of Step 3's race guard: the leader must yield the moment
  // it learns of a higher term, from the *sending* side this time rather
  // than the receiving side. peerB here is already ahead (e.g. it won an
  // election the leader doesn't know about yet) and its real
  // receiveHeartbeat() response will naturally report its own higher term.
  const leader = newNode("node-a");
  leader.becomeCandidate(); // term 1
  leader.becomeLeader();

  const peerBNode = newNode("node-b");
  peerBNode.becomeCandidate(); // term 1
  peerBNode.becomeCandidate(); // term 2 — ahead of the leader
  const { server, peer } = await listen(peerBNode);

  assertEqual(leader.state, "leader", "sanity: leader starts as leader before sending heartbeats");
  assertEqual(leader.currentTerm, 1, "sanity: leader starts at term 1");

  await sendHeartbeats(leader, [peer]);

  assertEqual(
    leader.state,
    "follower",
    "leader steps down to follower after a peer's real response reports a higher term",
  );
  assertEqual(leader.currentTerm, 2, "leader adopts the higher term reported by the peer");

  await closeAll([server]);
  leader.stop();
  peerBNode.stop();
}

{
  // Sanity: when no peer reports a higher term, the leader remains leader.
  const leader = newNode("node-a");
  leader.becomeCandidate();
  leader.becomeLeader(); // term 1

  const followerNode = newNode("node-b"); // stays at term 0 until it hears from the leader
  const { server, peer } = await listen(followerNode);

  await sendHeartbeats(leader, [peer]);

  assertEqual(leader.state, "leader", "leader remains leader when no peer reports a higher term");
  assertEqual(followerNode.currentTerm, 1, "follower adopted the leader's term via the real heartbeat");

  await closeAll([server]);
  leader.stop();
  followerNode.stop();
}

{
  // Leader remains leader when peers are unreachable (null responses don't
  // count as "higher term" — they simply don't participate in the max()).
  const leader = newNode("node-a");
  leader.becomeCandidate();
  leader.becomeLeader();

  await sendHeartbeats(leader, [{ id: "ghost", host: "localhost", port: 1 }]);

  assertEqual(leader.state, "leader", "leader remains leader when heartbeats to peers fail/time out");
  leader.stop();
}

console.log(`\n${passed} passed, ${failed} failed`);
if (failed > 0) {
  process.exit(1);
}