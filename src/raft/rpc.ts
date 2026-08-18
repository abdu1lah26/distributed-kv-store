// RequestVote RPC wiring (Milestone 4, Step 3).
//
// Sits between the pure state machine (src/raft/node.ts) and cluster
// config (src/cluster/config.ts): turns RaftNode's onElectionTimeout hook
// into a real election over HTTP, and exposes the server-side route peers
// call into.
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
  RaftNode,
  VoteRequest,
  VoteResponse,
} from "./node.js";

const PEER_REQUEST_TIMEOUT_MS = 100;

// AppendEntries request shape per §5.3. Still heartbeat-only as of
// Milestone 5, Step 1 (no real log module yet — that's Step 2):
// prevLogIndex/prevLogTerm/leaderCommit are accepted here in the wire
// shape for forward compatibility, but are NOT read or acted on below.
// entries[] is expected to be empty until Step 2+ add real log
// replication. Note this is a separate RPC from RequestVote — the
// lastLogIndex/lastLogTerm up-to-date check landed in node.ts's
// receiveVoteRequest() as of this step, but has no bearing on
// AppendEntries, which doesn't carry those fields at all.
export interface AppendEntriesRequest {
  term: number;
  leaderId: string;
  prevLogIndex: number;
  prevLogTerm: number;
  entries: unknown[];
  leaderCommit: number;
}

export interface AppendEntriesResponse {
  term: number;
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

    // M4 scope: only term/leaderId drive behavior (see §5.3.1 note above
    // this interface). prevLogIndex/prevLogTerm/leaderCommit are ignored;
    // entries[] is not applied to any log, since there isn't one yet.
    const heartbeatRequest: HeartbeatRequest = {
      term: body.term,
      leaderId: body.leaderId,
    };

    const response = node.receiveHeartbeat(heartbeatRequest);
    res.status(200).json(response);
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
 * Sends an AppendEntries heartbeat (empty entries[], per M4 scope) to a
 * single peer. Same treat-failure-as-null policy as requestVoteFromPeer:
 * an unreachable peer must not crash or block the leader's heartbeat
 * loop.
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
 * Leader-side heartbeat broadcast: sends an empty-entries AppendEntries
 * to every peer in parallel. If any peer reports a term higher than this
 * node's own, the leader steps down immediately (node.becomeFollower) —
 * the mirror case of runElection's stillValidCandidate guard: whoever
 * learns of a higher term must yield right away, regardless of what
 * state they were in. Without this, a stale/partitioned-then-rejoined
 * leader could keep sending heartbeats after the cluster has already
 * moved on to a new term and leader.
 *
 * Called from RaftNode's onHeartbeatDue hook for a live node (hook
 * wiring itself happens at the node's construction/startup site).
 */
export async function sendHeartbeats(node: RaftNode, peers: PeerConfig[]): Promise<void> {
  const heartbeatTerm = node.currentTerm;
  const request: AppendEntriesRequest = {
    term: heartbeatTerm,
    leaderId: node.nodeId,
    prevLogIndex: 0, // stubbed — no log yet, see §5.3.1
    prevLogTerm: 0, // stubbed — no log yet, see §5.3.1
    entries: [], // heartbeat-only in M4
    leaderCommit: 0, // stubbed — no log yet, see §5.3.1
  };

  const responses = await Promise.all(
    peers.map((peer) => sendHeartbeatToPeer(peer, request)),
  );

  const highestPeerTerm = responses.reduce(
    (max, response) => (response !== null && response.term > max ? response.term : max),
    heartbeatTerm,
  );

  if (highestPeerTerm > node.currentTerm) {
    node.becomeFollower(highestPeerTerm);
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
    node.becomeLeader();
  }
}