// Leader-side client write path (Milestone 5, Step 6.4).
//
// Ties together Step 3's replication (sendHeartbeats), Step 5's commit
// tracking (commitIndex), and Step 6.3's apply loop into the one thing a
// client-facing PUT/DELETE handler (Step 6.5) actually needs: propose a
// command, wait until it's safely committed, apply it locally, and
// report what happened. This module owns none of those mechanisms
// itself — it only orchestrates calls into RaftNode/rpc.ts/ApplyLoop,
// the same "orchestration lives outside the pure pieces" pattern rpc.ts
// itself already established relative to node.ts.
//
// WHAT HAPPENS TO A LOG ENTRY IF ITS WRITE TIMES OUT WITHOUT COMMITTING
// (explicitly required to be addressed, not assumed):
//
// Nothing. The entry is NOT truncated, removed, or otherwise cleaned up
// by this module — it remains sitting in the leader's log as a
// legitimate (if not-yet-confirmed) tail entry. This is not merely
// "acceptable" but the ONLY correct behavior: any ad-hoc self-truncation
// here would be a second, informal mechanism operating outside Raft's
// actual rules, racing against genuine replication that might still be
// in flight via the background heartbeat timer. Concretely, one of three
// things subsequently happens to a timed-out entry, none of which this
// module controls or needs to:
//   1. Replication catches up later (the background heartbeat timer,
//      already running via RaftNode's onHeartbeatDue hook, keeps
//      retrying regardless of this function returning) and the entry
//      commits anyway — invisibly to the client that already received a
//      timeout response.
//   2. This node loses leadership and a divergent log wins out — the
//      entry gets discarded via the EXISTING Milestone 5, Step 4
//      mechanism (a follower's receiveAppendEntries truncating on a real
//      conflict), not by any new logic in this module.
//   3. It simply sits uncommitted (or eventually committed) harmlessly —
//      the log is unbounded for v1 anyway (Section 2 non-goal: no
//      compaction).
// This matches DESIGN.md Section 6's "an acknowledged write is never
// lost" / unacknowledged-write wording precisely: what can be lost is
// the CLIENT's confirmation, not the server's attempt.

import type { RaftNode } from "./node.js";
import type { LogCommand } from "./log.js";
import type { PeerConfig } from "../cluster/config.js";
import type { ApplyLoop } from "./applyLoop.js";
import { sendHeartbeats } from "./rpc.js";

/** Demo-tuned, same family as the existing 100ms peer-timeout / 50ms heartbeat / 150-300ms election constants — not derived, chosen. */
export const DEFAULT_WRITE_TIMEOUT_MS = 1000;

/** Poll interval while waiting for commit. Real setTimeout-based waits, not a busy loop — see the loop below. */
export const WRITE_POLL_INTERVAL_MS = 10;

export type WriteResult =
  | { status: "committed"; index: number }
  | { status: "not-leader" }
  | { status: "timeout"; index: number }
  | { status: "stepped-down"; index: number };

/**
 * Proposes a write (PUT/DELETE command) on the given node, which MUST
 * currently believe itself to be leader (checked first; returns
 * "not-leader" immediately, without touching the log at all, if not).
 *
 * Appends the command to the leader's own log, triggers one immediate
 * out-of-band replication round (rather than waiting up to 50ms for the
 * next background heartbeat tick), then polls commitIndex until the new
 * entry is safely committed, this node steps down, or timeoutMs elapses
 * — whichever comes first. On commit, applies it via the given
 * ApplyLoop before returning "committed", so a client that receives a
 * committed response can immediately read its own write locally.
 *
 * The poll loop uses a real setTimeout-based wait each iteration (see
 * sleep() below), not a synchronous busy loop — this matters because
 * the background heartbeat timer that actually drives replication
 * forward runs on Node's own event loop via RaftNode's internal
 * setInterval, and a busy loop here would starve it of any chance to
 * fire, which would make this function unable to ever observe progress
 * (a live-lock, not just a slow path).
 */
export async function proposeWrite(
  node: RaftNode,
  peers: PeerConfig[],
  applyLoop: ApplyLoop,
  command: LogCommand,
  timeoutMs: number = DEFAULT_WRITE_TIMEOUT_MS,
): Promise<WriteResult> {
  if (node.state !== "leader") {
    return { status: "not-leader" };
  }

  const entry = node.log.append(node.currentTerm, command);

  // Immediate out-of-band replication attempt so this write doesn't have
  // to wait for the next background heartbeat tick. This is purely a
  // latency optimization, not this entry's only chance at replication —
  // the background heartbeat timer keeps retrying regardless of what
  // happens here.
  try {
    await sendHeartbeats(node, peers);
  } catch {
    // sendHeartbeats already treats individual unreachable peers as
    // non-fatal (null responses, per rpc.ts); a synchronous throw here
    // would be unexpected. Deliberately swallowed rather than aborting
    // the whole write attempt — the poll loop below and the background
    // heartbeat timer are the real mechanisms driving this entry toward
    // commit, not this one optimistic extra round.
  }

  const deadline = Date.now() + timeoutMs;

  while (true) {
    if (node.state !== "leader") {
      // A leader can lose leadership mid-write (partition, another
      // node's election). Stop polling immediately rather than waiting
      // out the rest of a timeout that can no longer resolve favorably
      // from this node's perspective.
      return { status: "stepped-down", index: entry.index };
    }

    if (node.commitIndex >= entry.index) {
      // RaftNode satisfies ApplyLoop's CommitSource structurally (see
      // applyLoop.ts) — no cast needed.
      await applyLoop.applyCommitted(node);
      return { status: "committed", index: entry.index };
    }

    if (Date.now() >= deadline) {
      // See the module header comment: the entry is deliberately left
      // exactly as-is in the log. Nothing to clean up here.
      return { status: "timeout", index: entry.index };
    }

    await sleep(WRITE_POLL_INTERVAL_MS);
  }
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}