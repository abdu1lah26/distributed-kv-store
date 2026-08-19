# Distributed KV Store — Design Doc

**Author:** Abdullah
**Status:** Locked (v1) — scope changes require updating this file first
**Stack:** TypeScript (ESM, `module: nodenext`), Node.js, Docker Compose
**Script runner: use `tsx`, not `ts-node`.** `ts-node` (including `--loader ts-node/esm`) is broken with this project's `nodenext` + `"type": "module"` config (`TypeError: Cannot read properties of undefined (reading 'fileExists')`). Always run scripts with `npx tsx <file>`.

---

## 1. Goal

Build a distributed key-value store from scratch to demonstrate understanding of:
- Durable single-node storage engines (LSM-tree family)
- Distributed consensus (Raft: leader election + log replication)
- Failure handling in a real running system (not just theory)

This is a learning/portfolio project, not a production system. Every layer is built manually — no Redis, no `raft` npm packages, no ORMs for storage.

---

## 2. Non-Goals (explicitly out of scope for v1)

Write code freely for anything in scope; if you're about to build one of these, stop and come back here first.

- ❌ ML-based cache eviction (no XGBoost/ONNX layer)
- ❌ Natural language / LLM query interface
- ❌ Multi-datacenter / cross-region replication
- ❌ Log compaction / snapshotting (log grows unbounded for v1 — fine for demo scale)
- ❌ Dynamic cluster membership changes (cluster is fixed at 3 nodes, configured at boot)
- ❌ gRPC (HTTP/JSON only — see ADR-1 below)
- ❌ Authentication / TLS (local demo only)

If a stretch goal is wanted later, add it as a new numbered milestone at the bottom — don't silently expand scope mid-build.

---

## 3. Architecture Overview

```text
        ┌────────────┐
        │   Client    │  (curl / simple CLI / Postman)
        └──────┬─────┘
               │ HTTP (GET/PUT/DELETE)
               ▼
     ┌───────────────────────────────┐
     │      3-node Raft Cluster       │
     │                                 │
     │   Node A ──┐                   │
     │   Node B ──┼── Raft RPCs (HTTP)│
     │   Node C ──┘                   │
     └───────────────────────────────┘
               │
        Each node owns its own
        local storage engine
        (WAL + MemTable + SSTables)

```

* Client writes MUST go to the current leader. If a follower receives a write, it responds `421 NOT_LEADER` with a redirect hint to the known leader.
* Client reads can go to any node for v1 (eventually consistent reads from followers — document this tradeoff explicitly in the README, don't hide it).
* Each node's storage engine is fully independent — replication happens at the **log entry** level (Raft replicates commands, not files).

---

## 4. Component 1: Storage Engine (per node, single-node scope)

### 4.1 Write path

1. Write arrives → append to **WAL** (Write-Ahead Log) on disk, fsync.
2. Only after WAL write succeeds → apply to in-memory **MemTable** (sorted map).
3. If MemTable exceeds a size threshold (e.g. 1000 keys or 1MB, whichever hits first) → flush to a new immutable **SSTable** file on disk, clear MemTable.

### 4.2 Read path

1. Check MemTable first (most recent data).
2. Check **Bloom filter** for each SSTable, newest to oldest — skip files the filter says definitely don't contain the key.
3. For SSTables the filter didn't rule out, binary search the sorted file (in practice: binary search an in-memory key→byteOffset index, then seek directly to the value on disk — see Section 4.4.4).

### 4.3 Crash recovery

On startup: replay the WAL to rebuild the MemTable to the state it was in before the crash. This is the one behavior you MUST test explicitly (kill -9 the process, restart, verify no acknowledged write is lost).

### 4.4 File formats (lock these before coding storage)

* **WAL entry:** `[4-byte length][JSON: {op, key, value, timestamp}]\n`
* **SSTable:** sorted JSON lines, one key-value pair per line, filename = `sstable_<timestamp>.dat`
* **Bloom filter:** built in-memory when an SSTable is loaded/created; not persisted separately in v1 (rebuilt on startup from the SSTable — simpler, acceptable tradeoff to document).

---

### 4.4.1 WAL Implementation Decisions (locked after Milestone 1 build)

* **WAL is append-only, never truncated or compacted** — true for Milestone 1 (Section 2 non-goal) and remains true through Milestone 2 even after SSTable flushes exist. Correct, but means crash-recovery time grows unbounded with total WAL size over the system's lifetime. Acceptable for a learning-scale project; would need addressing (checkpoint + truncate, or WAL segmentation) in a production system. Revisit only if a compaction milestone is explicitly added later.
* **fsync on every single append**, not batched — chosen for correctness-first over performance, appropriate for a learning project at this stage. Batching fsyncs is a valid future optimization and a good interview talking point, but is NOT implemented — don't assume it exists.
* **Torn writes on replay:** if the WAL file ends mid-record (partial length prefix or partial JSON payload — the crash-mid-write scenario from Section 4.3), `replay()` stops cleanly at that point rather than throwing. This is the mechanism that satisfies the Section 4.3 crash-recovery requirement.

### 4.4.2 MemTable Implementation Decisions

**Milestone 1 (superseded — kept for history):**

* MemTable was backed by a plain `Map<string, string>`, not sorted.
* Deletes were plain removals from the Map — no tombstones, since there was no older SSTable layer for a delete to shadow yet.

**Milestone 2, Step 1 (current — implemented and tested):**

* Public API: `put(key, value)`, `delete(key)`, `get(key): GetResult`, `has(key): boolean`, `size(): number`, `entries(): Array<[string, string | Tombstone]>`, `clear(): void`.
⚠️ Method is `put`, not `set`. `has()` and `clear()` both exist. This was the source of a real mismatch mid-build (a stale/superseded version of this module briefly caused test failures) — if any future conversation proposes a different method name or omits one of these, check the actual file before trusting it, not memory of an earlier message.
* Deletes are represented as **tombstones**, not removals. Backing structure is `Map<string, string | Tombstone>`, where `Tombstone` is a unique `Symbol` sentinel (`TOMBSTONE`). The key stays in the Map; only its value changes to the sentinel. Required because a flushed SSTable can hold a stale value for a deleted key — the tombstone is what tells a read "stop here, key is deleted" instead of "keep checking older SSTables."
* `delete()` on a key never `put` in this MemTable instance **still writes a tombstone** — the MemTable can't know whether an older SSTable holds a value for that key, so it must shadow it regardless of local history.
* `get(key)` returns a discriminated result instead of `string | undefined`:
* `{ found: false }` — no entry in MemTable at all, caller should check SSTables next.
* `{ found: true, tombstone: true }` — definitively deleted, stop here.
* `{ found: true, tombstone: false, value: string }` — live value.
* `has()` / `size()` both count tombstoned entries as present — `size()` drives the flush-threshold check (Section 4.1), and a tombstone still occupies space that needs to be flushed out, not silently ignored.
* `entries()` returns a **sorted array of raw tuples** `[key, value | TOMBSTONE]` (not a discriminated-object shape, not a lazy generator/iterator), sorted at call time rather than continuously maintained. Array chosen deliberately to avoid a half-consumed-iterator bug if flush logic needs to pass over entries more than once. Consumed directly by `writeSSTable()` (Section 4.4.3) — that module imports `TOMBSTONE`/`Tombstone`/`GetResult` from this one and depends on nothing else.
* `clear()` empties the Map entirely (live values and tombstones) — called after a successful SSTable flush to reset for the next batch.
* `MemTable.get()` is **synchronous**. Note this in contrast to `SSTableHandle.get()` (Section 4.4.3), which is async — Store (Step 4) must handle the two differently in its unified read path, not loop over both the same way.
* **Known consequence (resolved in Milestone 2, Step 4):** `Store.ts`'s `get()` call sites were built against the old `string | undefined` contract and did not compile against this change until Step 4 updated them. This was expected and intentionally deferred, not a bug.

### 4.4.3 SSTable Implementation Decisions (Milestone 2, Step 2 — done, tested)

File: `src/storage/sstable.ts`

**Write path — `writeSSTable(dir, entries): Promise<string>**`

* Takes the already-sorted `entries()` output from MemTable directly — no re-sort performed here.
* Filename: `sstable_<timestamp>.dat` (Section 4.4 convention).
* Each entry serialized as one JSON line: `{"key",...,"value"}` for live entries, `{"key",...,"tombstone":true}` for deleted ones.
* **Write pattern: temp file + fsync + atomic rename.** Writes to `<filename>.dat.tmp` first, fsyncs it, then renames to the final `<filename>.dat`. A crash mid-write leaves only an orphaned `.tmp` file, which `loadSSTable()` never looks at — so a torn/partial SSTable file can't occur under the real filename. This is why SSTable, unlike WAL, does NOT need runtime torn-write tolerance in its normal read path (see test note below).
* Returns the final file path (Store, Step 4, tracks known SSTables by this).

**Read path — `loadSSTable(filepath): Promise<SSTableHandle>**`

* On load: scans the file once, recording each key's **byte offset and length** (not its value) into an in-memory index. Sorted "for free" since the underlying file is already sorted — no extra sort step.
* Also builds the SSTable's Bloom filter during this same load pass (Section 4.4.4) — a second, purely in-memory pass over the now-known index, no additional disk I/O.
* Returns a handle exposing `get(key): Promise<GetResult>`.

**`SSTableHandle.get(key)` — async, unlike `MemTable.get()`:**

1. Bloom filter pre-check (`mightContain(key)`) — if false, return `{ found: false }` immediately, no disk I/O at all.
2. Binary search the in-memory byte-offset index.
3. Not found in index → `{ found: false }`.
4. Found → seek to the recorded byte offset, read just that one line, parse it, return `{ found: true, tombstone: true }` or `{ found: true, tombstone: false, value }` — same `GetResult` shape as `MemTable.get()`, so Store's read logic (Step 4) can treat both uniformly aside from the sync/async difference.

**Explicitly out of scope for this module:**

* No knowledge of "newest to oldest" or multiple files — that ordering lives in Store (Step 4), which holds a list of `SSTableHandle`s.
* No compaction/merging (Section 2 non-goal).

**Test coverage (8/8 passing):** round-trip live+tombstone entries, never-written key returns `found:false`, filename convention, on-disk sort order, temp file cleanup after rename, index/seek correctness over many entries (exercises binary search, not just linear scan), empty-entries SSTable is valid and loadable, `loadSSTable()` throws loudly on a deliberately corrupted/truncated file rather than silently misreading it.
That last case is a **direct parse-error test against `loadSSTable()**`, not a test of the temp+rename mechanism — temp+rename's job is to prevent this situation from occurring naturally, not to recover from it once it has. A torn SSTable is not treated as an expected/recoverable case the way a torn WAL record is.

### 4.4.4 Bloom Filter Implementation Decisions (Milestone 2, Step 3 — done, tested)

File: `src/storage/bloomfilter.ts`

* **True bit-packed `Uint32Array**`, not one-byte-per-bit — chosen deliberately over the simpler byte-array option to match how real Bloom filters are implemented (memory efficiency is the actual point of the data structure). Bits are set/tested via bitwise shift + mask against 32-bit words (`i >>> 5` for word index, `1 << (i & 31)` for bit mask).
* **Sizing:** given an expected key count `n` and target false-positive rate `p` (default 1%), computed via the standard formulas:
`m (bits) = ceil(-(n·ln p) / (ln 2)²)`, `k (hash fns) = round((m/n)·ln 2)`.
* **Hashing: Kirsch-Mitzenmacher double hashing** — rather than implementing `k` independent hash functions, two real hashes (FNV-1a and djb2) are combined as `h_i(x) = (h1(x) + i·h2(x)) mod m` to derive all `k` hash values from two real hash computations per key. Standard technique used in real Bloom filter implementations — worth remembering the name for interviews.
* **Never persisted to disk** — rebuilt in-memory every time an SSTable is loaded (Section 4.4), as a second in-memory-only pass during `loadSSTable()`, right after the byte-offset index is built.
* **Wired into `SSTableHandle.get()**` as the very first check, before the binary search — `mightContain(key) === false` short-circuits to `{found: false}` with zero disk I/O. No false negatives are possible; false positives just fall through to the real index/seek lookup.
* Degenerate `n = 0` case (e.g. an empty SSTable) guarded explicitly — produces a harmless 1-bit/1-hash filter where `mightContain()` correctly returns false for everything, matching an empty SSTable containing nothing.

**Test coverage (5/5 passing):** no false negatives across 1000 added keys, an unseen-key sample isn't universally reported present, empirical false-positive rate stays within a sane bound of the 1% target across 20,000 trials, an intentionally overloaded small filter shows a visibly worse false-positive rate than a correctly-sized large filter, `n=0` doesn't throw. Plus: SSTable's original 8/8 tests re-run unchanged as a regression check after wiring the filter in — confirmed the pre-check didn't alter observable `get()` behavior, only skipped I/O.

### 4.4.5 Test Infrastructure: Cross-Platform Process Kill (found during M2 Step 4)

`durability.test.ts`'s SIGKILL test spawns a child process and force-kills it mid-write to simulate a crash. Two real, unrelated bugs surfaced here, both fixed:

1. **Orphaned process via `npx`.** Spawning via `npx tsx ...` forks a wrapper process; killing that wrapper doesn't reach the real fixture process running underneath it, which survived as an orphan. Harmless in Milestone 1 (nothing for the orphan to break), but in Milestone 2 an orphan could live long enough to cross the flush threshold and crash against an already-deleted temp directory. Fixed by spawning tsx's CLI directly via `node`, removing the extra `npx` layer.
2. **POSIX vs. Windows process termination.** `process.kill(-pid, 'SIGKILL')` (kill a whole process group via negative PID) is POSIX-only and does not work on Windows, which has no real process groups or signals. On Windows, `taskkill /pid <pid> /T /F` is used instead (kills the full process tree). The exit-detection assertion is also platform-branched: POSIX reports `signal: 'SIGKILL'`; Windows reports `code !== 0` with `signal: null`, since Windows has no signal to report — checking for a specific signal name is meaningless there.

Also replaced the original test's fixed 150ms wait-before-kill with polling for actual evidence of resolved writes (via fixture stdout), since cold-start time for spawning `tsx` varies significantly across machines/OSes — a constant tuned for one environment isn't reliable on another.

### 4.4.6 Type-Safety Fix: `exactOptionalPropertyTypes` and Node's `ChildProcess` (found during M2 Step 5)

`tsc --noEmit` (run manually after a sandbox session couldn't reach the npm registry to verify types itself) caught 3 real type errors invisible to `tsx`, which only strips types and never checks them:

* `child.stdout` accessed without a null-check — Node types it as `Readable | null`. Fixed with an explicit guard that throws loudly if ever actually null, rather than a `!` non-null assertion.
* A hand-written `killProcessGroup(child: { pid?: number })` helper (in both `durability.test.ts` and `durability-flush.test.ts`) didn't structurally match Node's real `ChildProcess.pid` type under this project's `exactOptionalPropertyTypes: true` setting — `pid?: number` (property may be absent) and `pid: number | undefined` (property always present, value may be undefined) are NOT the same type under that flag, even though they look equivalent. Fix: import and use Node's actual `ChildProcess` type directly in the helper's signature, rather than hand-rolling a structural type intended to match it — guarantees a 1:1 match with zero risk of drift, and is the generally better pattern whenever TypeScript disagrees with a hand-written type meant to mirror a library's real one.

**Lesson for this project going forward:** any step verified only via `tsx` (running code) without also running `npx tsc --noEmit` (checking types) has NOT been fully verified — this exact gap let a type error ride uncaught from Step 4 all the way into Step 5 before being caught. Always run the typecheck first, before running any tests.

### 4.4.7 Store: Configurable Flush Thresholds (Milestone 2, Step 5)

Store's constructor gained an optional third parameter: `constructor(walPath, sstableDir, thresholds?: { maxKeys?, maxBytes? })`.
Defaults to the real production values (1000 keys / 1MB) when omitted — every pre-existing 2-arg call site is unaffected. Exists purely so tests can force a flush quickly and deterministically (e.g. `maxKeys: 10`) without waiting on the real threshold. This isn't a test-only hack: the original Section 4.1 wording ("e.g. 1000 keys or 1MB") already implied these were tunable defaults, not hardcoded constants.

### 4.4.8 Bug fix: `Store.open()` didn't create the WAL's parent directory (found during Milestone 3)

`Store.open()` already created `sstableDir` via `mkdir(..., { recursive: true })`, but only *after* calling `wal.open()` — and never created the WAL path's own parent directory at all. `WriteAheadLog.open()` opens its file in `"a+"` mode, which creates the file if missing but not its containing directory, so constructing a `Store` against a not-yet-existing data directory (e.g. a fresh clone, first run, or a fresh Docker volume) threw `ENOENT` on startup. Surfaced by Milestone 3's REST API entrypoint (`src/index.ts`) hitting it on a real `curl` against a clean `./data` directory — the durability test fixtures never hit this because they always create their temp directory (and therefore both paths' parent) via `mkdtemp()` before constructing a `Store`.

Fixed by having `Store` retain `walPath` as a field and run both `mkdir(path.dirname(walPath), { recursive: true })` and `mkdir(sstableDir, { recursive: true })` up front, before any file I/O, rather than only ensuring `sstableDir` after WAL replay. Fixed at the `Store` level rather than in the new entrypoint, since this was an inconsistency in `Store`'s own directory-creation responsibility (it already handled this for `sstableDir`), not a missing caller responsibility — any future `Store` construction against a fresh directory, including Raft nodes in later milestones, would otherwise hit the same bug.

Full Milestone 2 regression suite (`src/test/store.test.ts`, `test/durability.test.ts`, `test/durability-flush.test.ts`) re-run and passing after this change, since it touches committed, tested code.

### 4.4.9 API Bug Fix: Error Middleware Ignored express.json()'s Own Status Code (found during M3 Step 5)

**Bug:** `src/api/server.ts`'s central error-handling middleware always
returned `500`, even when the thrown error already carried a correct
client-error status. Specifically, `express.json()`'s body-parser throws
a `SyntaxError` with `err.status = 400` when it can't parse a malformed
JSON request body — but the middleware ignored that and collapsed every
error to `500`, contradicting the approved API design note ("malformed
JSON body → Express's own 400"). Found by `test/api.test.ts`'s
`"PUT with a malformed JSON body returns 400"` test, which failed for
real (got 500, expected 400) rather than being caught by inspection.

**Fix:** the error middleware now checks `err.status`/`err.statusCode`
first (covering both property names some error types use) and only
falls back to `500` when neither is a valid client-error status.
Confirmed this didn't accidentally swallow real server errors: the
"unopened `Store` throws a plain `Error`" test case still correctly
returns `500`, since a plain `Error` has neither property.

## **Test coverage:** `test/api.test.ts`, 10/10 passing, including both the
corrected 400 case and the still-correct 500 case side by side.

## 5. Component 2: Raft Consensus

### 5.1 Scope for v1

* Leader election (randomized timeouts, terms, votes) ✅
* Log replication (AppendEntries, commit index advancement) ✅
* Log compaction / snapshots ❌ (non-goal above)

### 5.2 Node states

`Follower → Candidate → Leader`, exactly per the Raft paper. Do not invent shortcuts here — this is the part interviewers will actually probe.

### 5.3 RPCs (HTTP/JSON, per ADR-1)

* `POST /raft/request-vote` — `{term, candidateId, lastLogIndex, lastLogTerm}`
* `POST /raft/append-entries` — `{term, leaderId, prevLogIndex, prevLogTerm, entries[], leaderCommit}` (empty `entries[]` = heartbeat)

### 5.3.1 Log Up-To-Date Check — Resolved in Milestone 5, Step 1

Milestone 4 (leader election only, no log replication yet) implemented RequestVote vote-granting using only term and candidateId comparison, with lastLogIndex/lastLogTerm stubbed as constants (0/0) — safe only because no real log existed yet to compare. This was flagged as a must-fix election-safety gap before any real log entries could be added.

**Resolved in Milestone 5, Step 1** (before any log storage was built): `RaftNode` now tracks real `lastLogIndex`/`lastLogTerm` (constructor-injectable and settable via `setLastLogInfo()`, defaulting to `0`/`0` until Step 2 wires in a real log module). `receiveVoteRequest()` in `node.ts` implements the actual Raft rule: reject if the candidate's `lastLogTerm` is lower, or equal term with a lower `lastLogIndex` — checked *before* the `votedFor` check, since an out-of-date candidate must never receive a vote even from a node that hasn't voted yet. The wire format (`VoteRequest`, the `/raft/request-vote` route, and `runElection`'s request-building) was updated end to end to carry real values instead of the stub. Verified via 7 new unit tests covering both grant and reject cases, plus a route-level integration test over real HTTP — full regression against all Milestone 4 election tests confirmed clean.

### 5.3.2 In-Memory-Only Log Storage — Deferred Gap (Milestone 5, Step 2)

`RaftLog` (`src/raft/log.ts`) is entirely in-memory — no persistence. A node's whole log is lost on crash/restart, unlike the storage engine's WAL-backed durability (§4.3). This is a real gap, not a non-issue: it means a restarted node currently cannot recover its log at all, which will need addressing (WAL-backed log persistence, or similar) before this project's failure-model tests in §6 (particularly #2 and #4) can be considered fully passing. Deferred for now; must be resolved before Milestone 5 is considered done.

### 5.4 Client-facing RPC

* `PUT /kv/:key` `{value}` — only leader accepts; must be committed (majority ack) before returning 200 to client.
* `GET /kv/:key` — served locally, any node (documented eventual-consistency caveat).
* `DELETE /kv/:key` — same commit path as PUT.

### 5.5 Timeouts to lock now (avoid tuning-by-guessing mid-demo)

* Election timeout: randomized 150–300ms per node
* Heartbeat interval: 50ms

---

## 6. Failure Model — what you will explicitly test and demo

Write a test (script or manual) for each of these. This list IS the rubric for "is this a real systems project or just a demo that ran once."

1. **Leader crash mid-write** → new leader elected, no acknowledged write lost.
2. **Follower crash + rejoin** → catches up via AppendEntries, log converges.
3. **Network partition (minority side)** → minority side cannot elect a leader or accept writes (no split-brain).
4. **Process crash + restart (WAL recovery)** → node rebuilds correct state from WAL alone.
5. **Concurrent writes to leader** → correctly ordered in the log, applied in order on all nodes.

---

## 7. Milestones (each = a runnable, demoable state — commit at each one)

1. Single-node KV store: WAL + MemTable — **✅ DONE.** WAL, MemTable, Store coordinator all implemented and tested (manual sanity check + 3 durability test variants: clean-exit replay, SIGKILL mid-write, torn-record tolerance). Runner: tsx (ts-node incompatible with nodenext, see Tooling Lock).
2. SSTable flush + Bloom filter, per Section 7 item 2 — **✅ DONE.**

* ✅ **Step 1: MemTable tombstones** — `put`/`delete`/`get`/`has`/`size`/`entries`/`clear`, tombstone sentinel, discriminated `GetResult`. Done, tested, committed. See Section 4.4.2.
* ✅ **Step 2: SSTable module** — write (temp+rename) + load (byte-offset index) + async `get()` with index+seek reads. 8/8 tests passing. Done, tested, committed. See Section 4.4.3.
* ✅ **Step 3: Bloom filter** — bit-packed `Uint32Array`, FNV-1a + djb2 double hashing, wired into `SSTable.get()` as a pre-check. 5/5 BloomFilter tests + 8/8 regression SSTable tests passing. Done, tested, committed. See Section 4.4.4.
* ✅ **Step 4: Store coordinator wiring** — flush-threshold trigger, newest-first SSTable discovery on open(), unified MemTable(sync)/SSTable(async) read path, tombstone shadowing across the boundary. Breaking API change: Store constructor now takes `(walPath, sstableDir)`, `get()` is now async. 7/7 new integration tests + all 3 M1 durability tests re-verified against the new signature. Done, tested, committed.
* ✅ **Step 5: Durability test with flush involved** — two complementary tests: Test A (real SIGKILL after a genuine flush, verifying recovery combines replayed WAL state with existing SSTable state) and Test B (deterministic delete/overwrite across the flush boundary, verifying tombstones correctly shadow stale flushed values — explicitly NOT a `kill -9` test, documented as testing a different, complementary scenario). Required adding Store's configurable flush thresholds (Section 4.4.7) and fixing 3 type errors caught by `tsc --noEmit` (Section 4.4.6). Done, tested, committed.

**Milestone 2 complete** — full single-node LSM-tree-style storage engine (WAL, MemTable with tombstones, SSTable, Bloom filter, Store coordination with flush triggering) with crash recovery proven under both real SIGKILL and deterministic boundary conditions.
3. Wrap with REST API (`GET/PUT/DELETE /kv/:key`) — single node, no Raft yet — **✅ DONE.**
`src/api/server.ts` (`createApp(store)`, testable without a real socket)

* `src/index.ts` (real entrypoint, owns Store lifecycle + port binding).
10/10 integration tests passing (`test/api.test.ts`), full Milestone 2
regression suite re-verified clean. Two real bugs found and fixed along
the way: `Store.open()` not creating the WAL's parent directory
(Section 4.4.8) and the error middleware collapsing every error to 500
instead of respecting `express.json()`'s own 400 on malformed JSON
(Section 4.4.9). `package.json` cleaned up to match (stale
`main`/`start`/`test` boilerplate replaced with real scripts).

**Milestone 3 complete** — real, running, curl-able single-node REST
API in front of the full Milestone 2 storage engine.

4. 3-node cluster, Raft leader election only (no log replication) — **✅ DONE.**

* ✅ **Step 1: State machine core** — `src/raft/node.ts`: term/vote/state
  fields, election timer (150-300ms) + heartbeat timer (50ms) with
  injectable `randomFn` for deterministic tests, pure `becomeFollower`/
  `becomeCandidate`/`becomeLeader`/`receiveVoteRequest`/`receiveHeartbeat`.
  43/43 unit tests passing, no networking involved. Done, tested, committed.
* ✅ **Step 2: Cluster config** — `src/cluster/config.ts`: `NODE_ID`/`PORT`/
  `PEERS` env var parsing into a validated `ClusterConfig`, pure function
  over an env-like object (not `process.env` directly) for testability.
  23/23 unit tests passing, including every malformed-input case. Done,
  tested, committed.
* ✅ **Step 3: RequestVote RPC** — `src/raft/rpc.ts`: `createRaftApp(node)`
  server route + `requestVoteFromPeer`/`runElection` client-side, 100ms
  peer-timeout (demo-tuned), unreachable peers treated as non-votes not
  crashes. Includes the `stillValidCandidate` race guard — a concurrent
  higher-term RPC arriving mid-election must win over a stale majority
  computed from outdated responses. 15/15 tests passing against real
  spun-up servers (not mocked fetch), including a genuine timing-based
  race test verified stable across repeated runs. Done, tested, committed.
* ✅ **Step 4: AppendEntries RPC (heartbeat-only)** — extended `rpc.ts`:
  `sendHeartbeatToPeer`/`sendHeartbeats`, leader steps down immediately
  on discovering a higher term from any peer response (mirror case of
  Step 3's guard, from the sending side). `prevLogIndex`/`prevLogTerm`/
  `entries`/`leaderCommit` accepted in the wire shape but not acted on
  yet, per Section 5.3.1. 31/31 tests passing (appended to `rpc.test.ts`).
  Done, tested, committed.
* ✅ **Step 5: Integration test — real 3-node election** —
  `src/raft/main.ts` (runnable entrypoint wiring config + state machine +
  RPCs together, binds `0.0.0.0` since these are separate OS processes)
  and `test/raft-election.test.ts`: spawns 3 real node processes, polls
  stdout for exactly one `state=leader`, force-kills the leader
  (cross-platform, same pattern as Section 4.4.5), confirms a different
  survivor becomes the new leader within a bounded time. Verified
  stable across 3 repeated full runs, zero orphaned processes after
  each. Done, tested, committed.

**Milestone 4 complete** — real 3-node Raft cluster with correct leader
election, verified end-to-end with actual OS processes, real network
calls, and a real kill/re-election cycle. Known gap carried forward:
Section 5.3.1's `lastLogIndex`/`lastLogTerm` stub — currently safe only
because there's no log to compare yet, and MUST be replaced with a real
log up-to-date check when Milestone 5 adds real log entries, or election
safety silently breaks.
5. Add log replication (AppendEntries, commit index) — this is the hardest milestone, budget the most time here — **IN PROGRESS.**

* ✅ **Step 1: Real log up-to-date check** — resolved Section 5.3.1's known
  gap from Milestone 4: `RaftNode` tracks real `lastLogIndex`/`lastLogTerm`,
  `receiveVoteRequest()` implements the actual Raft comparison (checked
  before the `votedFor` check), wire format updated end to end. Full
  Milestone 4 regression re-verified clean. Done, tested, committed.
* ✅ **Step 2: In-memory log module** — new `src/raft/log.ts` (`RaftLog`
  class, array-backed, 1-indexed, documented explicitly to avoid the
  classic off-by-one footgun), `LogCommand` shape mirroring the WAL entry
  format (§4.4). `RaftNode` now owns a real `RaftLog` internally;
  `lastLogIndex`/`lastLogTerm` are pure delegating getters. Step 1's
  `setLastLogInfo()` escape hatch removed entirely — tests now exercise
  real `log.append()` behavior, not injected fake state. 36/36 new
  `log.test.ts` tests + 64/64 `node.test.ts` + 33/33 `rpc.test.ts` +
  3 repeated clean runs of the full 3-node election integration test,
  zero orphaned processes. Caught and fixed a latent bug along the way:
  a stale `RaftNode` constructor call in `rpc.test.ts` had been masked
  by an `as any` cast that predated this step. In-memory-only storage
  is a real, tracked gap — see Section 5.3.2. Done, tested, committed.
6. Wire client writes through leader-only path with `421 NOT_LEADER` redirect.
7. Run failure-model tests from Section 6, document results (including any that fail initially — document the fix, like your cousin did with the benchmark post. That's the most convincing part of a writeup).
8. Docker Compose for the 3-node cluster, README with `docker compose up`.
9. (Stretch, optional) Simple dashboard or CLI showing cluster state.

---

## 8. Architecture Decision Records (ADRs)

**ADR-1: HTTP/JSON over gRPC for all RPCs**
Chosen for debuggability (curl-able, human-readable) while learning Raft for the first time. The hard part of this project is consensus logic, not transport. Revisit only as a stretch goal after Milestone 7 is solid.

**ADR-2: Fixed 3-node cluster, no dynamic membership**
Dynamic membership changes are a known-hard Raft extension (Raft paper Section 6) and explicitly out of scope to keep the project finishable.

---

## 9. Repo structure (proposed)

```text
/src
  /storage → WAL, MemTable, SSTable, BloomFilter
  /raft → node state machine, RPC handlers, election/heartbeat timers
  /api → REST layer (client-facing + raft RPC routes)
  /cluster → node config, peer discovery (static list for v1)
  /test → unit tests for individual storage modules
(bloomfilter.test.ts, memtable.test.ts, sstable.test.ts)
/test → integration / failure-model tests (Section 6),
e.g. durability.test.ts and its fixtures
docker-compose.yml
README.md → setup + demo instructions
DESIGN.md → this file
```

**Two test locations, intentional split — not drift:**

* `src/test/` — unit tests for one module in isolation (no cross-module wiring, e.g. `memtable.test.ts` tests only `MemTable`).
* `/test` (top-level) — integration and failure-model tests that exercise multiple modules together or spawn real processes (e.g. `durability.test.ts` + its `fixtures/` subfolder).

Going forward: a new unit test for a single module → `src/test/`. A new test that wires modules together or maps to a Section 6 failure scenario → top-level `/test`.

---

## 9.1 Tooling Lock

* `package.json`: `"type": "module"` (ESM, not CommonJS)
* `tsconfig.json`: `--module nodenext --target es2020 --strict --esModuleInterop`
* Do not switch back to CommonJS or change these flags without updating this section first — ESM import syntax (`import`/`export`, `.js` extensions in relative imports even for `.ts` files) applies to all code from here on.
* Script runner: `tsx`, not `ts-node` (see header note).

---

## 10. How this doc gets used

* Paste this whole file at the start of any new Claude conversation about this project, or keep it in the repo root for Claude Code to read directly.
* Update it BEFORE changing scope, not after — if you're tempted to add something not listed here, add it to Section 2/7 first, then build it.
* **When in doubt about what a module's actual current interface is, check the real file on disk before trusting recollection from earlier in a conversation** — this file has already caught one real drift incident (Section 4.4.2) where a described interface and the actual implemented one disagreed after a long back-and-forth.
```