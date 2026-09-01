// Raft node state machine core (Milestone 4, Step 1; updated Milestone 5,
// Steps 1-4).
//
// Scope: pure state machine + timers. No HTTP/RPC calls to peers here —
// that's rpc.ts.
//
// DESIGN.md §5.3.1 (Milestone 5, Step 1): the real Raft "log up-to-date"
// check is implemented in receiveVoteRequest() below.
//
// Milestone 5, Step 2: lastLogIndex/lastLogTerm are now real delegations
// to an owned RaftLog instance (src/raft/log.ts) rather than manually-set
// stand-in fields — the Step 1 setLastLogInfo() escape hatch has been
// removed entirely. Tests exercise real behavior via node.log.append().
//
// Milestone 5, Step 3: becomeLeader() now takes the cluster's peer ids
// and initializes per-peer replication state (nextIndex/matchIndex) on
// this node, per the Raft paper.
//
// Milestone 5, Step 4 (closes DESIGN.md §5.3.3): receiveAppendEntries()
// below implements the real Raft consistency check and log append,
// replacing receiveHeartbeat() entirely — an AppendEntries with empty
// entries[] already was a heartbeat, so keeping a separate pure method
// for that case would have been exactly the kind of overlapping,
// drift-prone logic the Step 2 setLastLogInfo() removal and the Step 3
// becomeLeader() peer-list requirement were both trying to avoid.
// AppendEntriesRequest/AppendEntriesResponse are now canonical here
// (previously wire-only types living in rpc.ts), mirroring how
// VoteRequest/VoteResponse are organized.
//
// Milestone 5, Step 5: commitIndex tracking. Deliberately NOT
// lastApplied/state-machine application (that's Milestone 5, Step 6's
// job, per the person's explicit instruction) — Raft separates "is this
// entry safe" (commitIndex) from "has this entry been executed"
// (lastApplied) on purpose, and conflating them is a classic source of
// real bugs (premature application, inconsistent ordering across
// nodes). recomputeCommitIndex() implements the Raft paper's leader-side
// safety rule that an entry can only be counted toward a NEW commitIndex
// advancement if it was created in the leader's current term — a leader
// must never conclude an older-term entry is committed purely by
// replica count (Raft paper §5.4.2).
//
// Milestone 5, Step 6.2: currentLeaderId tracking, for the eventual
// 421 NOT_LEADER redirect hint (Section 3 / §5.4). Set ONLY from a real
// AppendEntries that passes the term check (receiveAppendEntries) — a
// RequestVote is deliberately NOT treated as evidence of a leader, since
// it proves someone is running for leadership, not that they won;
// adopting it would risk pointing a future redirect at a candidate who
// goes on to lose the election. This means there is a real, expected
// window — after stepping down (e.g. via receiveVoteRequest seeing a
// higher term) but before hearing a real AppendEntries from the winner —
// where currentLeaderId is stale or null. That is Raft's actual
// guarantee, not a gap in this implementation: a node genuinely does not
// know who the new leader is until that leader actually contacts it. Any
// 421 redirect hint built from this field is therefore best-effort and
// can legitimately be stale or absent in that window.

import { RaftLog, type LogCommand, type LogEntry } from "./log.js";

export { type LogCommand, type LogEntry };

export type RaftState = "follower" | "candidate" | "leader";

const ELECTION_TIMEOUT_MIN_MS = 150;
const ELECTION_TIMEOUT_MAX_MS = 300;
const HEARTBEAT_INTERVAL_MS = 50;

export interface VoteRequest {
  term: number;
  candidateId: string;
  /** Index of the candidate's last log entry. See DESIGN.md §5.3.1. */
  lastLogIndex: number;
  /** Term of the candidate's last log entry. See DESIGN.md §5.3.1. */
  lastLogTerm: number;
}

export interface VoteResponse {
  term: number;
  voteGranted: boolean;
}

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
   * Whether the follower's consistency check passed and entries[] (if
   * any) were appended. Real as of Milestone 5, Step 4 — previously
   * stubbed unconditionally true (DESIGN.md §5.3.3, now resolved).
   */
  success: boolean;
}

/**
 * Per-peer replication bookkeeping the leader maintains, per the Raft
 * paper: nextIndex is the index of the next log entry to send to that
 * peer; matchIndex is the highest index known to be replicated there.
 * Initialized in becomeLeader() and updated via
 * advanceReplicationOnSuccess()/retreatReplicationOnFailure() as
 * AppendEntries responses come back (rpc.ts, Step 3+).
 */
export interface PeerReplicationState {
  nextIndex: number;
  matchIndex: number;
}

export interface RaftNodeOptions {
  nodeId: string;
  /** Called when the election timer fires with no heartbeat received. */
  onElectionTimeout?: () => void;
  /** Called on the heartbeat interval while this node is leader. */
  onHeartbeatDue?: () => void;
  /** Overridable for deterministic tests instead of real randomness. */
  randomFn?: () => number;
}

export class RaftNode {
  readonly nodeId: string;

  private _currentTerm: number;
  private _votedFor: string | null;
  private _state: RaftState;
  private readonly _log: RaftLog;
  private _replicationState: Map<string, PeerReplicationState> = new Map();
  private _commitIndex: number = 0;
  private _currentLeaderId: string | null = null;

  private electionTimer: NodeJS.Timeout | null = null;
  private heartbeatTimer: NodeJS.Timeout | null = null;

  private readonly onElectionTimeout: () => void;
  private readonly onHeartbeatDue: () => void;
  private readonly randomFn: () => number;

  constructor(options: RaftNodeOptions) {
    this.nodeId = options.nodeId;
    this._currentTerm = 0;
    this._votedFor = null;
    this._state = "follower";
    this._log = new RaftLog();

    this.onElectionTimeout = options.onElectionTimeout ?? (() => {});
    this.onHeartbeatDue = options.onHeartbeatDue ?? (() => {});
    this.randomFn = options.randomFn ?? Math.random;

    this.resetElectionTimer();
  }

  get currentTerm(): number {
    return this._currentTerm;
  }

  get votedFor(): string | null {
    return this._votedFor;
  }

  get state(): RaftState {
    return this._state;
  }

  get lastLogIndex(): number {
    return this._log.getLastLogIndex();
  }

  get lastLogTerm(): number {
    return this._log.getLastLogTerm();
  }

  /**
   * The highest log index known to be safely replicated to a majority
   * of the cluster (Raft paper §5.3, §5.4.1). Starts at 0. Deliberately
   * NOT the same concept as "applied to the state machine" (lastApplied)
   * — see the Step 5 header comment above. Advanced by the leader via
   * recomputeCommitIndex(), and by a follower directly from leaderCommit
   * inside receiveAppendEntries(). Persists across leadership changes
   * (never reset in becomeFollower()/becomeLeader()) since it reflects a
   * durable fact about the log, not leader-specific bookkeeping.
   */
  get commitIndex(): number {
    return this._commitIndex;
  }

  /**
   * This node's best current belief about who the cluster's leader is,
   * for building a 421 NOT_LEADER redirect hint (Section 3 / §5.4). Only
   * ever set from a real AppendEntries that passed the term check
   * (receiveAppendEntries) — never from a RequestVote, and never merely
   * from stepping down. See the Step 6.2 header comment above for why:
   * this is a best-effort hint that can legitimately be stale or null,
   * by Raft's actual guarantees, not a bug in this tracking.
   */
  get currentLeaderId(): string | null {
    return this._currentLeaderId;
  }

  /**
   * The node's log, exposed directly so callers (tests, and later steps'
   * AppendEntries leader/follower wiring) can append/read/truncate real
   * entries rather than going through any node-level indirection. There
   * is deliberately no setLastLogInfo()-style override anymore — the
   * Milestone 5, Step 1 escape hatch was removed once this real log
   * existed, per the same reasoning that ended the Milestone 4, Step 3
   * fake race-test incident: a manual override sitting next to the real
   * mechanism is exactly what lets a future test quietly exercise the
   * fake path instead of the real one.
   */
  get log(): RaftLog {
    return this._log;
  }

  /** Stops all timers. Call on node shutdown to avoid leaking handles (e.g. in tests). */
  stop(): void {
    if (this.electionTimer !== null) {
      clearTimeout(this.electionTimer);
      this.electionTimer = null;
    }
    if (this.heartbeatTimer !== null) {
      clearInterval(this.heartbeatTimer);
      this.heartbeatTimer = null;
    }
  }

  // ---- State transitions ----

  /**
   * Step down to follower for the given term. Used both when discovering
   * a higher term (from a vote request, heartbeat, or vote response) and
   * when a candidate/leader must yield.
   */
  becomeFollower(term: number): void {
    const steppingDownFromLeader = this._state === "leader";

    this._state = "follower";
    if (term > this._currentTerm) {
      this._currentTerm = term;
      this._votedFor = null;
    }

    if (steppingDownFromLeader) {
      this.stopHeartbeatTimer();
      // Replication bookkeeping is only meaningful while leader — clear
      // it so a later becomeLeader() call starts fresh rather than
      // carrying over stale nextIndex/matchIndex from a prior term.
      this._replicationState.clear();
    }
    this.resetElectionTimer();
  }

  /**
   * Transition to candidate: increment term, vote for self, restart the
   * election timer (Raft paper: a new election timeout is chosen for
   * each election round, including retries after a split vote).
   */
  becomeCandidate(): void {
    this._state = "candidate";
    this._currentTerm += 1;
    this._votedFor = this.nodeId;
    // No leader is known during an election in progress — a stale
    // belief from the previous term would be actively wrong to keep
    // (that leader, if it still exists, might not even win re-election).
    this._currentLeaderId = null;
    this.resetElectionTimer();
  }

  /**
   * Transition to leader. Caller (rpc.ts's runElection) is responsible
   * for only calling this after winning a majority of votes for the
   * current term.
   *
   * peerIds: the full cluster's other node ids, used to initialize this
   * leader's per-peer replication state (Raft paper: nextIndex starts at
   * lastLogIndex + 1 for every peer, matchIndex starts at 0). Required —
   * not defaulted to [] — so that any call site that doesn't yet know
   * its cluster's peer ids fails to compile rather than silently
   * leaving replication state empty.
   */
  becomeLeader(peerIds: string[]): void {
    this._state = "leader";
    this._currentLeaderId = this.nodeId;
    if (this.electionTimer !== null) {
      clearTimeout(this.electionTimer);
      this.electionTimer = null;
    }

    this._replicationState = new Map();
    const initialNextIndex = this.lastLogIndex + 1;
    for (const peerId of peerIds) {
      this._replicationState.set(peerId, { nextIndex: initialNextIndex, matchIndex: 0 });
    }

    this.startHeartbeatTimer();
  }

  /** This peer's nextIndex, or undefined if not currently tracked (not leader, or unknown peer id). */
  getNextIndex(peerId: string): number | undefined {
    return this._replicationState.get(peerId)?.nextIndex;
  }

  /** This peer's matchIndex, or undefined if not currently tracked (not leader, or unknown peer id). */
  getMatchIndex(peerId: string): number | undefined {
    return this._replicationState.get(peerId)?.matchIndex;
  }

  /**
   * Records a successful AppendEntries response from a peer: advances
   * matchIndex to the given value (only forward, guarding against an
   * out-of-order/stale response regressing it) and sets nextIndex to
   * matchIndex + 1. A no-op if peerId isn't currently tracked (e.g. this
   * node is no longer leader by the time the response arrives).
   */
  advanceReplicationOnSuccess(peerId: string, matchIndex: number): void {
    const peerState = this._replicationState.get(peerId);
    if (peerState === undefined) {
      return;
    }
    if (matchIndex > peerState.matchIndex) {
      peerState.matchIndex = matchIndex;
    }
    peerState.nextIndex = matchIndex + 1;
  }

  /**
   * Records a rejected AppendEntries response from a peer: decrements
   * nextIndex by one (per the Raft paper's back-off-and-retry strategy),
   * floored at 1. A no-op if peerId isn't currently tracked.
   *
   * As of Milestone 5, Step 4, this path is genuinely reachable: the
   * follower-side consistency check in receiveAppendEntries() can now
   * really return success:false (DESIGN.md §5.3.3, resolved this step).
   */
  retreatReplicationOnFailure(peerId: string): void {
    const peerState = this._replicationState.get(peerId);
    if (peerState === undefined) {
      return;
    }
    peerState.nextIndex = Math.max(1, peerState.nextIndex - 1);
  }

  /**
   * Leader-side commitIndex advancement (Raft paper §5.3, §5.4.1). A
   * separate method from advanceReplicationOnSuccess() deliberately:
   * that method is scoped to one peer's progress, while this one looks
   * across every peer's matchIndex plus this node's own log to decide
   * whether the cluster-wide commit point can move — a genuinely
   * different scope that shouldn't be folded into per-peer bookkeeping.
   *
   * No-op if this node isn't currently leader (an empty replicationState
   * means there's no peer set to compute a majority over).
   *
   * Algorithm: scan candidate indices N from lastLogIndex down to
   * commitIndex + 1. For each N:
   * - If the entry at N is not from this node's CURRENT term, skip it
   *   as a direct commit candidate — Raft's critical safety rule (§5.4.2):
   *   a leader must never conclude an entry is committed purely by
   *   counting replicas if that entry is from an earlier term, since a
   *   later leader could still legitimately overwrite it. (An older-term
   *   entry still gets committed, but only indirectly, as a side effect
   *   of a later current-term entry at a higher index being committed —
   *   which is exactly what setting commitIndex = N does for every index
   *   below N too.)
   * - Otherwise, count this node (always trivially caught up to its own
   *   log) plus every peer whose matchIndex >= N. If that count is a
   *   majority of the full cluster, set commitIndex = N and stop — N is
   *   scanned from highest to lowest, so the first qualifying N is the
   *   highest one.
   */
  recomputeCommitIndex(): void {
    if (this._state !== "leader") {
      return;
    }

    const peerIds = Array.from(this._replicationState.keys());
    const clusterSize = peerIds.length + 1;
    const majorityNeeded = Math.floor(clusterSize / 2) + 1;

    for (let n = this.lastLogIndex; n > this._commitIndex; n--) {
      const entry = this._log.getEntry(n);
      if (entry === undefined || entry.term !== this._currentTerm) {
        // Either shouldn't happen (n <= lastLogIndex always has an
        // entry) or a genuine older-term entry — not a valid direct
        // commit candidate per the safety rule above. Keep scanning
        // downward for a qualifying current-term index.
        continue;
      }

      const replicatedCount =
        1 + // this node itself
        peerIds.filter((peerId) => (this._replicationState.get(peerId)?.matchIndex ?? 0) >= n)
          .length;

      if (replicatedCount >= majorityNeeded) {
        this._commitIndex = n;
        break;
      }
    }
  }

  // ---- RPC-handling logic (pure — no network here) ----

  /**
   * Pure vote-granting decision, per Raft paper rules including the log
   * up-to-date check (DESIGN.md §5.3.1 — implemented as of Milestone 5,
   * Step 1; previously stubbed out during Milestone 4 since no log
   * existed to compare).
   *
   * Rules implemented here, in order:
   * - Reject if request.term < currentTerm.
   * - If request.term > currentTerm, step down to follower for that term
   *   first (this may clear votedFor for the new term).
   * - Reject if the candidate's log is less up-to-date than this node's:
   *   candidate's lastLogTerm < ours, OR (equal lastLogTerm AND
   *   candidate's lastLogIndex < ours). Per the Raft paper, this check
   *   applies regardless of votedFor — an out-of-date candidate must
   *   never receive a vote, even from a node that hasn't voted yet.
   * - Otherwise grant only if votedFor is null or already this
   *   candidateId for the current term (one vote per term).
   */
  receiveVoteRequest(request: VoteRequest): VoteResponse {
    if (request.term > this._currentTerm) {
      this.becomeFollower(request.term);
    }

    if (request.term < this._currentTerm) {
      return { term: this._currentTerm, voteGranted: false };
    }

    const candidateLogIsUpToDate =
      request.lastLogTerm > this.lastLogTerm ||
      (request.lastLogTerm === this.lastLogTerm &&
        request.lastLogIndex >= this.lastLogIndex);

    if (!candidateLogIsUpToDate) {
      return { term: this._currentTerm, voteGranted: false };
    }

    const canVote =
      this._votedFor === null || this._votedFor === request.candidateId;

    if (canVote) {
      this._votedFor = request.candidateId;
      this.resetElectionTimer();
      return { term: this._currentTerm, voteGranted: true };
    }

    return { term: this._currentTerm, voteGranted: false };
  }

  /**
   * Real Raft AppendEntries receiver logic (Raft paper, Figure 2), which
   * subsumes what receiveHeartbeat() used to handle as a special case —
   * an AppendEntries with an empty entries[] is simply a heartbeat, so
   * there is deliberately no separate pure method for that case anymore.
   *
   * Rules implemented here, in order:
   * 1. Reply false if request.term < currentTerm (stale leader).
   * 2. If request.term > currentTerm, step down to follower for that
   *    term first (same as receiveVoteRequest/the old receiveHeartbeat).
   * 3. Same-term AppendEntries from a real leader: if this node was
   *    candidate/leader, step down to follower (a legitimate leader for
   *    this term exists). Otherwise (already follower) just reset the
   *    election timer — this node continues recognizing the sender.
   *    Also (Milestone 5, Step 6.2): once the term check above has
   *    passed, this request is known to be from a real leader for a
   *    term this node accepts, so currentLeaderId is adopted — even if
   *    the consistency check below goes on to reject this particular
   *    batch of entries, the sender is still genuinely the leader.
   * 4. Consistency check: if prevLogIndex > 0, this node's log must
   *    contain an entry at that index whose term matches prevLogTerm.
   *    prevLogIndex === 0 means "start of log," which is trivially
   *    consistent (nothing to check against). A missing entry or a term
   *    mismatch both fail the check -> reply false without touching the
   *    log at all.
   * 5. Append entries[], one at a time: if an existing entry at that
   *    index has a conflicting term, truncate from that index onward
   *    (deleting it and everything after — Raft paper §5.3) before
   *    appending the leader's version. If an existing entry at that
   *    index already matches (same term), it is a duplicate/retried RPC
   *    delivering something this node already has correctly — skip it
   *    without truncating, since truncating a correct suffix based on a
   *    stale/replayed request would needlessly discard already-agreed
   *    log entries for no reason. Only entries genuinely new (past the
   *    end of the current log) are appended directly.
   * 6. Reply true.
   */
  receiveAppendEntries(request: AppendEntriesRequest): AppendEntriesResponse {
    if (request.term < this._currentTerm) {
      // Stale leader — ignore, tell it the real term so it can step down.
      return { term: this._currentTerm, success: false };
    }

    if (request.term > this._currentTerm) {
      this.becomeFollower(request.term);
    } else if (this._state !== "follower") {
      // Same term, but we were candidate/leader — a legitimate leader
      // for this term exists, so step down.
      this.becomeFollower(request.term);
    } else {
      this.resetElectionTimer();
    }

    // Milestone 5, Step 6.2: this request passed the term check above
    // (we didn't return early for a stale term), so it's a real
    // AppendEntries from a real leader for a term this node accepts —
    // adopt it as the known leader. Deliberately set here, not inside
    // becomeFollower(), since becomeFollower() is also called from
    // receiveVoteRequest() on a mere candidacy, which must NOT be
    // treated as evidence of a leader (see header comment).
    this._currentLeaderId = request.leaderId;

    if (request.prevLogIndex > 0) {
      const prevEntry = this._log.getEntry(request.prevLogIndex);
      if (prevEntry === undefined || prevEntry.term !== request.prevLogTerm) {
        return { term: this._currentTerm, success: false };
      }
    }

    let nextIndexToWrite = request.prevLogIndex + 1;
    for (const entry of request.entries) {
      const existing = this._log.getEntry(nextIndexToWrite);
      if (existing === undefined) {
        // Past the end of our log — genuinely new, append directly.
        this._log.append(entry.term, entry.command);
      } else if (existing.term !== entry.term) {
        // Real conflict: discard this entry and everything after it,
        // then write the leader's version.
        this._log.truncateFrom(nextIndexToWrite);
        this._log.append(entry.term, entry.command);
      }
      // else: existing.term === entry.term — same entry already present
      // (a duplicate/retried RPC). Deliberately not truncated or
      // re-appended; see rule 5 above.
      nextIndexToWrite += 1;
    }

    // Milestone 5, Step 5: adopt the leader's commit progress, capped at
    // the index of the last entry included in THIS request (Raft paper
    // §5.3) — never past what this node has actually just confirmed
    // matches the leader's log, even if the leader's own commitIndex is
    // further ahead than what it sent this round.
    const indexOfLastNewEntry = request.prevLogIndex + request.entries.length;
    if (request.leaderCommit > this._commitIndex) {
      this._commitIndex = Math.min(request.leaderCommit, indexOfLastNewEntry);
    }

    return { term: this._currentTerm, success: true };
  }

  // ---- Timers ----

  private resetElectionTimer(): void {
    if (this.electionTimer !== null) {
      clearTimeout(this.electionTimer);
    }
    const timeout =
      ELECTION_TIMEOUT_MIN_MS +
      this.randomFn() * (ELECTION_TIMEOUT_MAX_MS - ELECTION_TIMEOUT_MIN_MS);

    this.electionTimer = setTimeout(() => {
      this.onElectionTimeout();
    }, timeout);
  }

  private startHeartbeatTimer(): void {
    if (this.heartbeatTimer !== null) {
      clearInterval(this.heartbeatTimer);
    }
    this.heartbeatTimer = setInterval(() => {
      this.onHeartbeatDue();
    }, HEARTBEAT_INTERVAL_MS);
  }

  private stopHeartbeatTimer(): void {
    if (this.heartbeatTimer !== null) {
      clearInterval(this.heartbeatTimer);
      this.heartbeatTimer = null;
    }
  }
}