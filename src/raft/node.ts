// Raft node state machine core (Milestone 4, Step 1; updated Milestone 5,
// Steps 1-2).
//
// Scope: pure state machine + timers. No HTTP/RPC calls to peers here —
// that's Step 3 (RequestVote) and Step 4 (AppendEntries heartbeat) from
// Milestone 4.
//
// DESIGN.md §5.3.1 (Milestone 5, Step 1): the real Raft "log up-to-date"
// check is implemented in receiveVoteRequest() below.
//
// Milestone 5, Step 2: lastLogIndex/lastLogTerm are now real delegations
// to an owned RaftLog instance (src/raft/log.ts) rather than manually-set
// stand-in fields — the Step 1 setLastLogInfo() escape hatch has been
// removed entirely. Tests exercise real behavior via node.log.append().

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

export interface HeartbeatRequest {
  term: number;
  leaderId: string;
}

export interface HeartbeatResponse {
  term: number;
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
    this.resetElectionTimer();
  }

  /**
   * Transition to leader. Caller (Step 3 logic) is responsible for only
   * calling this after winning a majority of votes for the current term.
   */
  becomeLeader(): void {
    this._state = "leader";
    if (this.electionTimer !== null) {
      clearTimeout(this.electionTimer);
      this.electionTimer = null;
    }
    this.startHeartbeatTimer();
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
   * Pure heartbeat-handling decision. A heartbeat is an AppendEntries
   * with an empty entries[] (§4.1 of this file's header comment / §5.3).
   * Any node receiving a heartbeat at term >= currentTerm recognizes the
   * sender as leader and reverts to (or remains) follower.
   */
  receiveHeartbeat(request: HeartbeatRequest): HeartbeatResponse {
    if (request.term < this._currentTerm) {
      // Stale leader — ignore, tell it the real term so it can step down.
      return { term: this._currentTerm };
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

    return { term: this._currentTerm };
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