// RequestVote + AppendEntries RPC wiring (Milestone 4, Step 3-4; updated
// Milestone 5, Steps 3-5).
//
// Sits between the pure state machine (src/raft/node.ts) and cluster
// config (src/cluster/config.ts): turns RaftNode's onElectionTimeout/
// onHeartbeatDue hooks into real HTTP calls, and exposes the server-side
// routes peers call into.
//
// Milestone 5, Step 4: AppendEntriesRequest/AppendEntriesResponse are no
// longer defined here — they now live in node.ts as canonical pure
// types (mirroring VoteRequest/VoteResponse), since the real consistency-
// check/append logic (receiveAppendEntries) needs them there too. This
// file imports them rather than re-declaring a wire-only duplicate.
//
// Milestone 5, Step 5: buildAppendEntriesRequest() sends the leader's
// real commitIndex instead of a stubbed 0, and sendHeartbeats() calls
// node.recomputeCommitIndex() once per round after processing all
// replication responses (not per-peer — see node.ts's doc comment on
// why commit computation is intentionally separate from per-peer
// matchIndex bookkeeping).
//
// Peer-request timeout (100ms) is tuned for localhost/demo conditions —
// well under the 150-300ms election timeout so one unreachable peer can't
// itself stall an election, not meant to hold up over a real network
// (same spirit as the Milestone 2 flush thresholds being demo-tuned).

import express, { type Express } from "express";
import type { PeerConfig } from "../cluster/config.js";
import type {
  AppendEntriesRequest,
  AppendEntriesResponse,
  RaftNode,
  VoteRequest,
  VoteResponse,
} from "./node.js";

const PEER_REQUEST_TIMEOUT_MS = 100;

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
      typeof body.prevLogIndex !== "number" ||
      typeof body.prevLogTerm !== "number" ||
      typeof body.leaderCommit !== "number" ||
      !Array.isArray(body.entries)
    ) {
      res.status(400).json({
        error:
          "Malformed append-entries body: expected {term, leaderId, prevLogIndex, prevLogTerm, entries: [], leaderCommit}.",
      });
      return;
    }

    // Milestone 5, Step 4: the real consistency check and log append now
    // happen here via receiveAppendEntries() — this route no longer
    // stubs success:true unconditionally (DESIGN.md §5.3.3, resolved).
    const response = node.receiveAppendEntries({
      term: body.term,
      leaderId: body.leaderId,
      prevLogIndex: body.prevLogIndex,
      prevLogTerm: body.prevLogTerm,
      entries: body.entries,
      leaderCommit: body.leaderCommit,
    });
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
    leaderCommit: node.commitIndex, // real as of Milestone 5, Step 5
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
 *    paper's back-off strategy). As of Milestone 5, Step 4, both
 *    branches are genuinely reachable — the follower-side consistency
 *    check (DESIGN.md §5.3.3) is now real.
 * 3. Once every response is processed, recompute commitIndex (Milestone
 *    5, Step 5) — done once per round after all matchIndex updates are
 *    in, not per-peer, since the computation needs every peer's current
 *    matchIndex together to determine a majority.
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

  node.recomputeCommitIndex();
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