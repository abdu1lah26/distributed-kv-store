import path from 'node:path';
import { Store } from '../../src/storage/store.js';

/**
 * Test fixture for Milestone 2, Step 5 — Test B (deterministic
 * delete/overwrite-across-a-flush-boundary correctness case).
 *
 * Not a kill -9 test — see durability-flush.test.ts for why this is
 * deliberately a clean-but-uncommitted exit (no close()) instead of a
 * real SIGKILL: the point of this fixture is an exact, repeatable
 * sequence, and racing a real async kill against "did the overwrite
 * happen yet" would make the scenario flaky to set up reliably.
 *
 * Sequence (all against a small configurable flush threshold, passed as
 * the second CLI arg, so this resolves in well under a second):
 *   1. Write `maxKeys - 2` filler keys, plus 'target' and 'doomed' —
 *      totals exactly `maxKeys` distinct keys, so the LAST of these
 *      puts crosses the threshold and triggers exactly one flush.
 *      'target' and 'doomed' are written early, so both are included
 *      in that flush.
 *   2. After the flush (MemTable now empty): overwrite 'target' with a
 *      new value, delete 'doomed', and write a brand-new 'fresh' key —
 *      none of these three trigger a second flush (MemTable is nowhere
 *      near the threshold again yet).
 *   3. Exit without close() — simulated crash right after the last
 *      acknowledged write.
 */
async function main(): Promise<void> {
  const [, , walPath, maxKeysArg] = process.argv;
  if (!walPath || !maxKeysArg) {
    console.error('Usage: flush-then-mutate.ts <walPath> <maxKeys>');
    process.exit(1);
  }

  const maxKeys = Number(maxKeysArg);
  if (!Number.isInteger(maxKeys) || maxKeys < 3) {
    console.error(`Invalid maxKeys (must be >= 3 to leave room for target/doomed/fillers): ${maxKeysArg}`);
    process.exit(1);
  }

  const sstableDir = path.join(path.dirname(walPath), 'sstables');
  const store = new Store(walPath, sstableDir, { maxKeys });
  await store.open();

  // Step 1: get exactly `maxKeys` distinct keys written, including
  // 'target' and 'doomed', to force exactly one flush containing both.
  await store.put('target', 'original_target_value');
  await store.put('doomed', 'original_doomed_value');
  for (let i = 0; i < maxKeys - 2; i++) {
    await store.put(`filler_${String(i).padStart(4, '0')}`, `filler_value_${i}`);
  }

  // Step 2: mutate across the flush boundary, in the fresh MemTable.
  await store.put('target', 'updated_target_value');
  await store.delete('doomed');
  await store.put('fresh', 'fresh_value');

  // Step 3: simulated crash — no close().
  process.exit(0);
}

main();