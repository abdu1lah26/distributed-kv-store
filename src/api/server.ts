import express, { type Express, type Request, type Response, type NextFunction } from "express";
import type { Store } from "../storage/store.js";
import type { LogCommand } from "../raft/log.js";
import type { WriteResult } from "../raft/writePath.js";

/**
 * Small structural interface describing exactly what createApp needs
 * from the Raft layer — decoupled from RaftNode/PeerConfig/ApplyLoop
 * directly, same pattern as ApplyLoop's CommitSource/ApplyTarget
 * (Milestone 5, Step 6.3) and the PeerConfig-shaped literals throughout
 * rpc.ts. Milestone 5, Step 6.6's real entrypoint constructs a
 * ClusterContext backed by a real RaftNode + writePath.proposeWrite();
 * tests (and Milestone 3's standalone src/index.ts, which predates Raft
 * entirely) can satisfy this with a trivial always-leader fake instead,
 * without needing a real cluster.
 */
export interface ClusterContext {
  /** Whether this node currently believes itself to be leader. */
  isLeader(): boolean;
  /**
   * This node's best current belief about who the leader is, for a 421
   * redirect hint. null if unknown. Per DESIGN.md (Milestone 5, Step
   * 6.2's currentLeaderId note), this is inherently best-effort and can
   * be stale — that is Raft's actual guarantee, not a gap here.
   */
  leaderRedirectHint(): { leaderId: string; url: string } | null;
  /**
   * Proposes a write and resolves once it's been decided — committed,
   * timed out, or this node turned out not to be leader after all (a
   * real race: isLeader() can return true and then writePath's own
   * internal check can still find this node stepped down microseconds
   * later). See writePath.ts's WriteResult for the full outcome shape.
   */
  write(command: LogCommand): Promise<WriteResult>;
}

/**
 * Structural interface describing exactly what the /status route needs
 * (Milestone 9 / DESIGN.md §7.9.1) — same "small structural interface,
 * not the concrete class" pattern as ClusterContext above and
 * CommitSource/ApplyTarget (applyLoop.ts). Verified directly against
 * real node.ts/applyLoop.ts source before this was written (§7.9.7) —
 * commitIndex is a real RaftNode getter, lastApplied maps to
 * ApplyLoop.lastAppliedIndex.
 *
 * Deliberately all getters/readonly, mirroring how main.ts's real
 * implementation is expected to be built: live-reading accessors backed
 * by a real RaftNode + ApplyLoop, not a one-time snapshot taken at
 * construction. A fake for tests can just use plain properties instead —
 * readonly here only documents the contract, TypeScript's structural
 * typing doesn't require the fake to use get syntax too.
 */
export interface StatusSource {
  readonly nodeId: string;
  readonly state: string;
  readonly currentTerm: number;
  readonly commitIndex: number;
  readonly lastApplied: number;
  readonly currentLeaderId: string | null;
}

/**
 * Optional features bundle for createApp, grouped into one object
 * rather than more positional params — same reasoning as ClusterContext
 * itself: a named, structural bag is easier to extend later (e.g. a
 * future configurable CORS origin) without yet another breaking
 * positional-signature change, per this project's established
 * "evolve in place" precedent (Step 4's AppendEntriesRequest move,
 * Step 6.1's config format change, Step 6.5's ClusterContext param).
 */
export interface AppFeatures {
  /**
   * If provided, mounts GET /status. If omitted, the route is not
   * mounted at all — not present-but-empty, not gated inside the
   * handler. DESIGN.md §7.9.1: index.ts's solo mode has no real Raft
   * state to source this from and must keep working without it.
   */
  status?: StatusSource | undefined;
  /**
   * If true, mounts POST /debug/kill (process.exit(1)). If false/
   * omitted, the route is not mounted at all — same "structurally
   * absent, not gated" principle as status above and as becomeLeader's
   * required peerIds param. DESIGN.md §7.9.6.
   */
  enableKill?: boolean | undefined;
}

/**
 * Builds an Express app wired to the given Store instance and Raft
 * cluster context.
 *
 * DESIGN.md Section 5.4 defines the client-facing RPC shape
 * (PUT/GET/DELETE /kv/:key). Milestone 3 implemented this route shape
 * for a single node with no Raft awareness at all; Milestone 5, Step 6.5
 * makes PUT/DELETE Raft-aware — leader-only, real 421 NOT_LEADER
 * redirects, and real commit-timeout handling — while GET stays exactly
 * as it was: always served locally, regardless of leadership, per
 * Section 3's already-documented eventual-consistency tradeoff for
 * reads. This is a breaking, in-place signature change (createApp now
 * requires a ClusterContext, no Raft-optional mode) — same "evolve in
 * place, don't maintain a parallel version" precedent as Milestone 5,
 * Step 4's AppendEntriesRequest move and Step 6.1's config format
 * change.
 *
 * Milestone 9 (DESIGN.md §7.9) adds an optional third parameter,
 * `features` — additive, not breaking: every existing 2-arg call site
 * (index.ts, existing tests) is unaffected. See AppFeatures above.
 *
 * Request-body validation on PUT happens BEFORE the leader check,
 * deliberately: a malformed body is a client error regardless of which
 * node receives it — redirecting the client to another node first would
 * just reproduce the identical 400 there, so surfacing the real problem
 * immediately is more useful than bouncing them around the cluster.
 *
 * This function does NOT call store.open() and does NOT start
 * listening on a port — callers (the real entrypoint, or a test) own
 * the Store lifecycle and the port binding. This keeps the app
 * constructible in tests without a real socket (see supertest usage
 * in test/api.test.ts).
 */
export function createApp(store: Store, cluster: ClusterContext, features?: AppFeatures): Express {
  const app = express();
  app.use(express.json());

  function notLeaderResponse(res: Response): void {
    const hint = cluster.leaderRedirectHint();
    res.status(421).json({
      error: "NOT_LEADER",
      leaderId: hint?.leaderId ?? null,
      leaderHint: hint?.url ?? null,
    });
  }

  /**
   * Maps a WriteResult onto the HTTP response, given the response body
   * to send on a successful commit. Shared between PUT and DELETE,
   * which only differ in what a "committed" result's body looks like.
   *
   * The switch has no default case on purpose — the `never` assignment
   * in its place gives a compile-time exhaustiveness check: if
   * WriteResult ever grows a new status variant, this function fails to
   * compile until it's handled here too, rather than silently falling
   * through at runtime.
   */
  function respondToWrite(res: Response, result: WriteResult, onCommitted: () => unknown): void {
    switch (result.status) {
      case "committed":
        res.status(200).json(onCommitted());
        return;
      case "not-leader":
      case "stepped-down":
        // Same client-facing situation as the up-front isLeader() check
        // failing — "wrong node, here's where to go instead" — just
        // discovered slightly later, inside the write attempt itself.
        notLeaderResponse(res);
        return;
      case "timeout":
        // 503, not 421 or 500: this is neither a leadership problem nor
        // a server bug — it's a genuine "don't know if this committed"
        // per DESIGN.md Section 6. 503 specifically signals "can't
        // fulfill this right now, not your fault, retry may help."
        res.status(503).json({ error: "WRITE_TIMEOUT", index: result.index });
        return;
      default: {
        const exhaustiveCheck: never = result;
        throw new Error(`Unexpected WriteResult status: ${JSON.stringify(exhaustiveCheck)}`);
      }
    }
  }

  app.get("/kv/:key", async (req: Request, res: Response, next: NextFunction) => {
    try {
      const key = req.params.key as string;
      const value = await store.get(key);

      if (value === undefined) {
        res.status(404).json({ error: "not found" });
        return;
      }

      res.status(200).json({ key, value });
    } catch (err) {
      next(err);
    }
  });

  app.put("/kv/:key", async (req: Request, res: Response, next: NextFunction) => {
    try {
      const key = req.params.key as string;
      const body: unknown = req.body;

      if (
        typeof body !== "object" ||
        body === null ||
        !("value" in body) ||
        typeof (body as { value: unknown }).value !== "string"
      ) {
        res.status(400).json({ error: "request body must be JSON with a string 'value' field" });
        return;
      }

      const value = (body as { value: string }).value;

      if (!cluster.isLeader()) {
        notLeaderResponse(res);
        return;
      }

      const result = await cluster.write({ op: "put", key, value });
      respondToWrite(res, result, () => ({ key, value }));
    } catch (err) {
      next(err);
    }
  });

  app.delete("/kv/:key", async (req: Request, res: Response, next: NextFunction) => {
    try {
      const key = req.params.key as string;

      if (!cluster.isLeader()) {
        notLeaderResponse(res);
        return;
      }

      // Unconditional delete: Store's tombstone semantics (DESIGN.md
      // Section 4.4.2) mean delete() is always a valid write regardless
      // of whether the key currently exists. No pre-check GET here —
      // that would be a wasted extra read just to choose a status code.
      const result = await cluster.write({ op: "delete", key });
      respondToWrite(res, result, () => ({ key, deleted: true }));
    } catch (err) {
      next(err);
    }
  });

  // ---- Milestone 9 (DESIGN.md §7.9): dashboard support routes ----
  // Both are structurally absent unless explicitly opted into via
  // `features` — not mounted-but-gated-inside-the-handler. An
  // unconfigured environment (including every existing test/call site
  // that doesn't pass `features`) has no way to even discover these
  // routes exist, consistent with this project's "make the unwanted
  // state impossible, not just discouraged" instinct (becomeLeader's
  // required peerIds param is the precedent cited in DESIGN.md §7.9.1).

  if (features?.status !== undefined) {
    const status = features.status;

    app.get("/status", (_req: Request, res: Response) => {
      // Scoped CORS: only this route, not a general opening of the API
      // (DESIGN.md §7.9.2). The dashboard is a plain static file
      // (§7.9.5), likely opened from a different origin (file:// or a
      // separate static server) than the 3 node ports it polls.
      res.set("Access-Control-Allow-Origin", "*");
      res.status(200).json({
        nodeId: status.nodeId,
        state: status.state,
        currentTerm: status.currentTerm,
        commitIndex: status.commitIndex,
        lastApplied: status.lastApplied,
        currentLeaderId: status.currentLeaderId,
      });
    });
  }

    if (features?.enableKill === true) {
    app.post("/debug/kill", (_req: Request, res: Response) => {
      // Wait for the real 'finish' event — confirms the response was
      // actually flushed to the client — rather than a guessed delay.
      // This project has already learned, repeatedly (§4.4.5's kill
      // detection, §6.3's whole burst-tuning saga), that an arbitrary
      // timing constant is not a substitute for a real signal when one
      // exists. Nonzero exit code to distinguish a deliberate demo-kill
      // from clean shutdown in any container/process logs (§7.9.6).
      res.on("finish", () => {
        process.exit(1);
      });
      res.status(200).json({ killed: true });
    });
  }

  // Unknown routes -> JSON 404, not Express's default HTML page.
  app.use((_req: Request, res: Response) => {
    res.status(404).json({ error: "not found" });
  });

  // Central error handler -> JSON 500, not Express's default HTML page.
  // Must have 4 params for Express to recognize it as an error handler.
  app.use((err: unknown, _req: Request, res: Response, _next: NextFunction) => {
    // express.json()'s body-parser throws a SyntaxError with
    // status/statusCode = 400 on malformed JSON. Respect that when
    // present instead of always collapsing to 500 — otherwise a client
    // sending bad JSON gets told "internal server error" for what is
    // very much a client error, which is both misleading and contrary
    // to the approved design ("malformed JSON body -> Express's own
    // 400"). Anything without a recognizable client-error status still
    // falls through to 500 as a genuine unexpected failure.
    const status =
      typeof err === "object" &&
      err !== null &&
      "status" in err &&
      typeof (err as { status: unknown }).status === "number"
        ? (err as { status: number }).status
        : typeof err === "object" &&
            err !== null &&
            "statusCode" in err &&
            typeof (err as { statusCode: unknown }).statusCode === "number"
          ? (err as { statusCode: number }).statusCode
          : 500;

    const message = err instanceof Error ? err.message : "internal server error";
    res.status(status).json({ error: message });
  });

  return app;
}