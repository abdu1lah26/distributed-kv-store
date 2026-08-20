// RequestVote + AppendEntries RPC wiring (Milestone 4, Step 3-4; updated
// Milestone 5, Step 3).
//
// Sits between the pure state machine (src/raft/node.ts) and cluster
// config (src/cluster/config.ts): turns RaftNode's onElectionTimeout/
// onHeartbeatDue hooks into real HTTP calls, and exposes the server-side
// routes peers call into.
//
// Peer-request timeout (100ms) is tuned for localhost/demo conditions —
// well under the 150-300ms election timeout so one unreachable peer can't
// itself stall an election, not meant to hold up over a real network
// (same spirit as the Milestone 2 flush thresholds being demo-tuned).

import express, { type Express } from "express";
import type { PeerConfig } from "../cluster/config.js";
import type {
  HeartbeatRequest,
  HeartbeatResponse,
  LogEntry,
  RaftNode,
  VoteRequest,
  VoteResponse,
} from "./node.js";

const PEER_REQUEST_TIMEOUT_MS = 100;

// AppendEntries request shape per §5.3. As of Milestone 5, Step 3, this
// carries REAL log entries built from the leader's per-peer nextIndex
// (see buildAppendEntriesRequest() below) — no longer heartbeat-only.
// leaderCommit remains stubbed at 0 (commitIndex advancement is
// Milestone 5, Step 5's scope, not this step's).
export interface AppendEntriesRequest {
  term: number;
  leaderId: string;
  prevLogIndex: number;
  prevLogTerm: number;
  entries: LogEntry[];
  leaderCommit: number;
}

export interface AppendEntriesResponse {
  term: number;
  /**
   * Whether the follower accepted and applied the entries. As of
   * Milestone 5, Step 3 this is unconditionally true on the follower
   * side below — the real consistency check that could ever produce
   * false doesn't exist until Step 4. See DESIGN.md §5.3.3: leader-side
   * nextIndex/matchIndex advancement driven by this field is therefore
   * NOT yet safe against a follower with a genuinely conflicting log.
   */
  success: boolean;
}

// ---- Server side ----

/**
 * Express app factory exposing POST /raft/request-vote, wired to the
 * given node's pure receiveVoteRequest(). Mirrors createApp(store) from
 * Milestone 3 — no port binding here, so it's testable in isolation.
 */
export function createRaftApp(node: RaftNode): Express {
  const app = express();
  app.use(express.json());

  app.post("/raft/request-vote", (req, res) => {
    const body = req.body as Partial<VoteRequest>;

    if (
      typeof body.term !== "number" ||
      typeof body.candidateId !== "string" ||
      typeof body.lastLogIndex !== "number" ||
      typeof body.lastLogTerm !== "number"
    ) {
      res.status(400).json({
        error:
          "Malformed request-vote body: expected {term, candidateId, lastLogIndex, lastLogTerm}.",
      });
      return;
    }

    const response = node.receiveVoteRequest({
      term: body.term,
      candidateId: body.candidateId,
      lastLogIndex: body.lastLogIndex,
      lastLogTerm: body.lastLogTerm,
    });
    res.status(200).json(response);
  });

  app.post("/raft/append-entries", (req, res) => {
    const body = req.body as Partial<AppendEntriesRequest>;

    if (
      typeof body.term !== "number" ||
      typeof body.leaderId !== "string" ||
      !Array.isArray(body.entries)
    ) {
      res.status(400).json({
        error:
          "Malformed append-entries body: expected {term, leaderId, ..., entries: []}.",
      });
      return;
    }

    // Milestone 5, Step 3: term/leaderId still drive the node's state
    // transition via receiveHeartbeat() (§5.3.1's log up-to-date check
    // is a RequestVote-only concern, unrelated to this route). Real
    // entries now arrive in the body, but this route does not yet apply
    // them to the follower's log or run any consistency check against
    // prevLogIndex/prevLogTerm — that's Milestone 5, Step 4. success is
    // unconditionally true here in the meantime; see DESIGN.md §5.3.3.
    const heartbeatRequest: HeartbeatRequest = {
      term: body.term,
      leaderId: body.leaderId,
    };

    const response = node.receiveHeartbeat(heartbeatRequest);
    const appendEntriesResponse: AppendEntriesResponse = {
      term: response.term,
      success: true,
    };
    res.status(200).json(appendEntriesResponse);
  });

  return app;
}

// ---- Client side ----

/**
 * Sends a RequestVote RPC to a single peer. Treats timeout, network
 * error, or a non-2xx response as "peer did not grant a vote" rather
 * than throwing — an unreachable peer must not crash or block an
 * election, it simply doesn't count toward the majority.
 */
export async function requestVoteFromPeer(
  peer: PeerConfig,
  request: VoteRequest,
): Promise<VoteResponse | null> {
  const controller = new AbortController();
  const timeoutHandle = setTimeout(() => controller.abort(), PEER_REQUEST_TIMEOUT_MS);

  try {
    const res = await fetch(`http://${peer.host}:${peer.port}/raft/request-vote`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(request),
      signal: controller.signal,
    });

    if (!res.ok) {
      return null;
    }

    const data = (await res.json()) as VoteResponse;
    return data;
  } catch {
    // Network error, timeout/abort, or malformed response body — all
    // treated uniformly as "no vote from this peer."
    return null;
  } finally {
    clearTimeout(timeoutHandle);
  }
}

/**
 * Sends a single AppendEntries RPC (real entries as of Milestone 5, Step
 * 3 — an empty entries[] is simply the case where the peer is already
 * caught up, which still functions as a heartbeat) to one peer. Same
 * treat-failure-as-null policy as requestVoteFromPeer: an unreachable
 * peer must not crash or block the leader's replication loop.
 */
export async function sendHeartbeatToPeer(
  peer: PeerConfig,
  request: AppendEntriesRequest,
): Promise<AppendEntriesResponse | null> {
  const controller = new AbortController();
  const timeoutHandle = setTimeout(() => controller.abort(), PEER_REQUEST_TIMEOUT_MS);

  try {
    const res = await fetch(`http://${peer.host}:${peer.port}/raft/append-entries`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(request),
      signal: controller.signal,
    });

    if (!res.ok) {
      return null;
    }

    const data = (await res.json()) as AppendEntriesResponse;
    return data;
  } catch {
    return null;
  } finally {
    clearTimeout(timeoutHandle);
  }
}

/**
 * Builds the AppendEntries request for one peer from the leader's real
 * log and that peer's tracked nextIndex (Milestone 5, Step 3). Falls
 * back to lastLogIndex + 1 (i.e. "peer is caught up, send nothing") if
 * this peer has no tracked replication state yet — defensive only; in
 * normal operation becomeLeader() initializes every configured peer's
 * state up front, so this should not be reachable in practice.
 */
function buildAppendEntriesRequest(node: RaftNode, peer: PeerConfig): AppendEntriesRequest {
  const nextIndex = node.getNextIndex(peer.id) ?? node.lastLogIndex + 1;
  const prevLogIndex = nextIndex - 1;
  const prevLogTerm = prevLogIndex > 0 ? (node.log.getEntry(prevLogIndex)?.term ?? 0) : 0;
  const entries = node.log.getEntriesFrom(nextIndex);

  return {
    term: node.currentTerm,
    leaderId: node.nodeId,
    prevLogIndex,
    prevLogTerm,
    entries,
    leaderCommit: 0, // stubbed — commitIndex advancement is Milestone 5, Step 5's scope
  };
}

/**
 * Leader-side replication broadcast: sends a real AppendEntries (per
 * buildAppendEntriesRequest above) to every peer in parallel, then:
 *
 * 1. If any peer reports a term higher than this node's own, the leader
 *    steps down immediately (node.becomeFollower) — the mirror case of
 *    runElection's stillValidCandidate guard: whoever learns of a higher
 *    term must yield right away, regardless of what state they were in.
 *    Stepping down short-circuits step 2 below: a leader that just
 *    discovered it isn't one anymore must not also apply stale
 *    replication-progress updates for a term it no longer holds.
 * 2. Otherwise, for each peer response: on success, advance that peer's
 *    nextIndex/matchIndex to reflect the entries just sent; on failure,
 *    retreat nextIndex by one for a retry on the next round (Raft
 *    paper's back-off strategy). See DESIGN.md §5.3.3 — success is
 *    unconditionally true from every real follower as of this step, so
 *    the failure branch exists but isn't yet exercised by real behavior.
 *
 * Called from RaftNode's onHeartbeatDue hook for a live node (hook
 * wiring itself happens at the node's construction/startup site).
 */
export async function sendHeartbeats(node: RaftNode, peers: PeerConfig[]): Promise<void> {
  const heartbeatTerm = node.currentTerm;

  const results = await Promise.all(
    peers.map(async (peer) => {
      const request = buildAppendEntriesRequest(node, peer);
      const response = await sendHeartbeatToPeer(peer, request);
      return { peer, request, response };
    }),
  );

  const highestPeerTerm = results.reduce(
    (max, { response }) => (response !== null && response.term > max ? response.term : max),
    heartbeatTerm,
  );

  if (highestPeerTerm > node.currentTerm) {
    node.becomeFollower(highestPeerTerm);
    return;
  }

  for (const { peer, request, response } of results) {
    if (response === null) {
      // Unreachable peer this round — leave its nextIndex/matchIndex
      // untouched; the next heartbeat interval will retry from the same
      // point.
      continue;
    }
    if (response.success) {
      const matchIndex = request.prevLogIndex + request.entries.length;
      node.advanceReplicationOnSuccess(peer.id, matchIndex);
    } else {
      node.retreatReplicationOnFailure(peer.id);
    }
  }
}

// ---- Election-triggering / vote-counting logic ----

/**
 * Runs one election round: becomes candidate, requests votes from all
 * peers in parallel, and becomes leader if a majority of the full
 * cluster (peers + self) granted a vote — but only if this node's state
 * and term are still exactly what they were when the election started.
 * A concurrent higher-term RPC (e.g. another peer's heartbeat or vote
 * request arriving mid-flight) may have already stepped this node down;
 * that must win over a stale majority computed from outdated responses.
 *
 * Called from RaftNode's onElectionTimeout hook (wiring for that hook is
 * the caller's responsibility, e.g. in the node's construction site).
 */
export async function runElection(node: RaftNode, peers: PeerConfig[]): Promise<void> {
  node.becomeCandidate();

  const electionTerm = node.currentTerm;
  const request: VoteRequest = {
    term: electionTerm,
    candidateId: node.nodeId,
    lastLogIndex: node.lastLogIndex,
    lastLogTerm: node.lastLogTerm,
  };

  const responses = await Promise.all(
    peers.map((peer) => requestVoteFromPeer(peer, request)),
  );

  const votesGranted =
    1 + // self-vote from becomeCandidate()
    responses.filter((response) => response !== null && response.voteGranted).length;

  const clusterSize = peers.length + 1;
  const majority = Math.floor(clusterSize / 2) + 1;

  const stillValidCandidate = node.state === "candidate" && node.currentTerm === electionTerm;

  if (votesGranted >= majority && stillValidCandidate) {
    node.becomeLeader(peers.map((peer) => peer.id));
  }
}