// Apply loop: applies committed Raft log entries to the local storage
// engine (Milestone 5, Step 6.3).
//
// Deliberately a SEPARATE module from both RaftNode (src/raft/node.ts)
// and Store (src/storage/store.ts) — per the person's explicit
// instruction, RaftNode stays pure and Store-free, and Raft's own
// "is this entry safe" (commitIndex, Step 5) vs "has this entry been
// executed" (lastApplied, here) separation is preserved rather than
// conflated. Conflating the two is a classic source of real bugs
// (premature application, inconsistent ordering across nodes) — this
// module exists specifically so that distinction has a real home.
//
// Depends on two small STRUCTURAL interfaces below, not the concrete
// RaftNode/Store classes directly:
//   - CommitSource: the minimal read surface this module needs from a
//     node (commitIndex + log.getEntry). RaftNode satisfies this without
//     any changes.
//   - ApplyTarget: the minimal write surface this module needs from a
//     store (put/delete). Store satisfies this without any changes.
// This keeps the module honestly scoped to what it actually touches,
// and makes it independently unit-testable against lightweight fakes
// (see src/test/applyLoop.test.ts) without needing a real Store/RaftNode
// wired together for every case — in particular, the error-injection
// test (a store that throws on the Nth call) needs no real filesystem
// or real replication to set up.

import type { LogCommand, LogEntry } from "./log.js";

export interface CommitSource {
  readonly commitIndex: number;
  readonly log: {
    getEntry(index: number): LogEntry | undefined;
  };
}

export interface ApplyTarget {
  put(key: string, value: string): Promise<void>;
  delete(key: string): Promise<void>;
}

/**
 * Applies committed Raft log entries to a local ApplyTarget (in
 * practice, a Store), in strict order, exactly once each. Runs on every
 * node — leader AND followers — since each node applies to its OWN
 * local storage engine; replication only copies log entries, per
 * DESIGN.md Section 3 ("Each node's storage engine is fully
 * independent — replication happens at the log entry level").
 */
export class ApplyLoop {
  private _lastAppliedIndex: number = 0;

  constructor(private readonly target: ApplyTarget) {}

  /** The highest log index successfully applied to the target so far. Starts at 0. */
  get lastAppliedIndex(): number {
    return this._lastAppliedIndex;
  }

  /**
   * Applies every committed-but-not-yet-applied entry (lastAppliedIndex+1
   * through source.commitIndex, inclusive) to the target, one at a time,
   * in order. A no-op if there's nothing new to apply.
   *
   * Error handling is deliberately strict, not best-effort:
   * - If the target throws while applying an entry, that error
   *   propagates out of this method as-is — NOT caught/logged/swallowed.
   *   lastAppliedIndex is only advanced AFTER a successful apply, so a
   *   thrown error leaves it at exactly the last index that genuinely
   *   succeeded — the next call resumes from there, not from scratch and
   *   not skipping the failed entry. Silently continuing past a failed
   *   apply (or silently getting stuck) would risk real state divergence
   *   between nodes, which is worse than failing loudly.
   * - If source.log.getEntry(i) returns undefined for an i that
   *   commitIndex says should exist, that is a serious invariant
   *   violation between the log and commitIndex (should not happen given
   *   the guarantees elsewhere in this codebase) and throws a specific
   *   Error rather than silently stopping — this is the boundary where
   *   log state becomes real durable side effects, so defensive
   *   strictness matters here specifically.
   */
  async applyCommitted(source: CommitSource): Promise<void> {
    while (this._lastAppliedIndex < source.commitIndex) {
      const nextIndex = this._lastAppliedIndex + 1;
      const entry = source.log.getEntry(nextIndex);

      if (entry === undefined) {
        throw new Error(
          `ApplyLoop: expected a log entry at index ${nextIndex} ` +
            `(commitIndex=${source.commitIndex}, lastAppliedIndex=${this._lastAppliedIndex}), ` +
            `but none was found. This indicates a serious invariant violation ` +
            `between the log and commitIndex, not a normal/expected condition.`,
        );
      }

      await this.applyCommand(entry.command);

      // Only advance after a successful apply — see error-handling note
      // above. If applyCommand throws, execution never reaches this line
      // for this entry, so lastAppliedIndex correctly stays one behind.
      this._lastAppliedIndex = nextIndex;
    }
  }

  private async applyCommand(command: LogCommand): Promise<void> {
    if (command.op === "put") {
      await this.target.put(command.key, command.value);
    } else {
      await this.target.delete(command.key);
    }
  }
}