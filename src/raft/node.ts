// Raft node state machine core (Milestone 4, Step 1).
//
// Scope: pure state machine + timers. No HTTP/RPC calls to peers here —
// that's Step 3 (RequestVote) and Step 4 (AppendEntries heartbeat).
// This module exposes timer-expiry hooks (onElectionTimeout, onHeartbeatDue)
// that later steps wire up to actual peer RPC calls.
//
// See DESIGN.md §5.3.1: lastLogIndex/lastLogTerm are NOT part of this
// module's vote-granting logic yet, since there is no log in Milestone 4.
// receiveVoteRequest() below intentionally does not accept or compare them.
// This MUST be revisited in Milestone 5 per that section.

export type RaftState = "follower" | "candidate" | "leader";

const ELECTION_TIMEOUT_MIN_MS = 150;
const ELECTION_TIMEOUT_MAX_MS = 300;
const HEARTBEAT_INTERVAL_MS = 50;

export interface VoteRequest {
  term: number;
  candidateId: string;
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
   * Pure vote-granting decision, per Raft paper rules minus the log
   * up-to-date check (see DESIGN.md §5.3.1 — deferred to Milestone 5).
   *
   * Rules implemented here:
   * - Reject if request.term < currentTerm.
   * - If request.term > currentTerm, step down to follower for that term
   *   first (this may clear votedFor for the new term).
   * - Grant only if votedFor is null or already this candidateId for the
   *   current term (one vote per term).
   */
  receiveVoteRequest(request: VoteRequest): VoteResponse {
    if (request.term > this._currentTerm) {
      this.becomeFollower(request.term);
    }

    if (request.term < this._currentTerm) {
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