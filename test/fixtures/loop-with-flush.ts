import path from 'node:path';
import { Store } from '../../src/storage/store.js';

/**
 * Test fixture for Milestone 2, Step 5 — Test A (literal Section 7
 * requirement: kill -9 after a flush has happened).
 *
 * Writes an unbounded sequence of unique keys forever, logging each one
 * to stdout right after its WAL append (fsync) resolves — same pattern
 * as write-and-die.ts's "loop" mode. The difference is a configurable,
 * small flush threshold (passed as the third CLI arg), so the parent
 * test can reliably force a real flush to happen quickly instead of
 * waiting on the real 1000-key/1MB threshold.
 *
 * This is a separate file from write-and-die.ts (rather than a third
 * mode added to it) specifically so write-and-die.ts — already verified
 * stable across Steps 2-4 — doesn't need to change for an unrelated
 * scenario.
 */
async function main(): Promise<void> {
  const [, , walPath, maxKeysArg] = process.argv;
  if (!walPath || !maxKeysArg) {
    console.error('Usage: loop-with-flush.ts <walPath> <maxKeys>');
    process.exit(1);
  }

  const maxKeys = Number(maxKeysArg);
  if (!Number.isInteger(maxKeys) || maxKeys <= 0) {
    console.error(`Invalid maxKeys: ${maxKeysArg}`);
    process.exit(1);
  }

  const sstableDir = path.join(path.dirname(walPath), 'sstables');
  const store = new Store(walPath, sstableDir, { maxKeys });
  await store.open();

  let i = 0;
  for (;;) {
    const key = `key${i}`;
    const value = `value${i}`;
    await store.put(key, value);
    console.log(`WROTE ${key}=${value}`);
    i += 1;
    await new Promise((resolve) => setTimeout(resolve, 15));
  }
}

main();