import express, { type Express, type Request, type Response, type NextFunction } from "express";
import type { Store } from "../storage/store.js";

/**
 * Builds an Express app wired to the given Store instance.
 *
 * DESIGN.md Section 5.4 defines the client-facing RPC shape for the
 * eventual Raft-backed cluster (PUT/GET/DELETE /kv/:key). This module
 * implements that same route shape for Milestone 3 (single node, no
 * Raft) per Section 7 item 3 — leader-redirect behavior (421 NOT_LEADER)
 * is explicitly deferred until Raft exists (Milestone 4+).
 *
 * This function does NOT call store.open() and does NOT start
 * listening on a port — callers (the real entrypoint, or a test) own
 * the Store lifecycle and the port binding. This keeps the app
 * constructible in tests without a real socket (see supertest usage
 * in test/api.test.ts).
 */
export function createApp(store: Store): Express {
  const app = express();
  app.use(express.json());

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
      await store.put(key, value);

      res.status(200).json({ key, value });
    } catch (err) {
      next(err);
    }
  });

  app.delete("/kv/:key", async (req: Request, res: Response, next: NextFunction) => {
    try {
      const key = req.params.key as string;
      // Unconditional delete: Store's tombstone semantics (DESIGN.md
      // Section 4.4.2) mean delete() is always a valid write regardless
      // of whether the key currently exists. No pre-check GET here —
      // that would be a wasted extra read just to choose a status code.
      await store.delete(key);

      res.status(200).json({ key, deleted: true });
    } catch (err) {
      next(err);
    }
  });

  // Unknown routes -> JSON 404, not Express's default HTML page.
  app.use((_req: Request, res: Response) => {
    res.status(404).json({ error: "not found" });
  });

  // Central error handler -> JSON 500, not Express's default HTML page.
  // Must have 4 params for Express to recognize it as an error handler.
  app.use((err: unknown, _req: Request, res: Response, _next: NextFunction) => {
    const message = err instanceof Error ? err.message : "internal server error";
    res.status(500).json({ error: message });
  });

  return app;
}