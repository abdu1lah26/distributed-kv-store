// In-memory Raft log storage (Milestone 5, Step 2).
//
// Scope: pure in-memory log entry storage — append, read, truncate. No
// persistence in this step (see DESIGN.md — flagged as an explicit
// deferred gap, same tracking treatment as the §5.3.1 stub was before
// Step 1 resolved it). A node's log is lost entirely on crash/restart
// until a future step adds WAL-backed (or similar) durability for it.
//
// No knowledge of terms/candidates/elections here — that's node.ts. This
// module only stores and retrieves entries.

export type LogCommand =
  | { op: "put"; key: string; value: string }
  | { op: "delete"; key: string };

export interface LogEntry {
  /** Raft term this entry was created in. */
  term: number;
  /** 1-based Raft log index. Index 0 conceptually means "no entry" / empty log. */
  index: number;
  command: LogCommand;
}

/**
 * In-memory Raft log. Entries are stored in a plain array where array
 * position 0 holds Raft index 1 (i.e. Raft index = array position + 1).
 * This offset is intentional and documented here rather than left
 * implicit, since off-by-one errors on log indexing are a classic Raft
 * implementation bug.
 */
export class RaftLog {
  private entries: LogEntry[] = [];

  /**
   * Appends a new entry at the next index (current length + 1). Returns
   * the created entry so callers (e.g. the leader-side AppendEntries
   * wiring in a later step) can read back its assigned index/term
   * without a second lookup.
   */
  append(term: number, command: LogCommand): LogEntry {
    const index = this.entries.length + 1;
    const entry: LogEntry = { term, index, command };
    this.entries.push(entry);
    return entry;
  }

  /**
   * Returns the entry at the given 1-based Raft index, or undefined if
   * out of range (including index 0 or negative indices, which are
   * never valid entries).
   */
  getEntry(index: number): LogEntry | undefined {
    if (index < 1) {
      return undefined;
    }
    return this.entries[index - 1];
  }

  /** 0 if the log is empty, per the Raft paper's "index 0 = no entry" convention. */
  getLastLogIndex(): number {
    return this.entries.length;
  }

  /** 0 if the log is empty. */
  getLastLogTerm(): number {
    const last = this.entries[this.entries.length - 1];
    return last === undefined ? 0 : last.term;
  }

  /**
   * Returns all entries from the given 1-based index through the end of
   * the log, inclusive. Returns an empty array if fromIndex is past the
   * end. Intended for building an AppendEntries RPC's entries[] payload
   * in a later step — no mutation of the underlying log.
   */
  getEntriesFrom(fromIndex: number): LogEntry[] {
    if (fromIndex < 1) {
      // Raft indices are 1-based; clamp rather than silently include
      // a nonexistent "index 0" entry.
      fromIndex = 1;
    }
    return this.entries.slice(fromIndex - 1);
  }

  /**
   * Discards all entries from the given 1-based index onward (inclusive),
   * keeping entries 1..fromIndex-1. Intended for a later step's follower-
   * side conflict resolution (Raft paper: a follower must delete its own
   * conflicting entries and everything after them before appending the
   * leader's version). A no-op if fromIndex is past the current end.
   */
  truncateFrom(fromIndex: number): void {
    if (fromIndex < 1) {
      this.entries = [];
      return;
    }
    this.entries.length = Math.min(this.entries.length, fromIndex - 1);
  }

  /** Total number of entries currently stored. Equivalent to getLastLogIndex(). */
  size(): number {
    return this.entries.length;
  }
}