import path from 'node:path';
import { Store } from '../../src/storage/store.js';

/**
 * Test fixture, run as a child process by test/durability.test.ts.
 *
 * Two modes:
 * - "fixed": performs a small, known sequence of writes, then exits
 *   normally WITHOUT calling store.close() — simulates a crash
 *   immediately after the last acknowledged write.
 * - "loop": writes an unbounded sequence of keys, logging each one to
 *   stdout right after its WAL append (and therefore fsync) resolves.
 *   The parent test SIGKILLs this process partway through and uses the
 *   stdout log to know exactly which writes had resolved before the kill.
 */
async function main(): Promise<void> {
  const [, , walPath, mode] = process.argv;
  if (!walPath || !mode) {
    console.error('Usage: write-and-die.ts <walPath> <fixed|loop>');
    process.exit(1);
  }

  // sstableDir is derived the same way durability.test.ts constructs it
  // (a "sstables" sibling of wal.log, inside the shared temp dir) rather
  // than passed as a separate CLI arg, so the two files can't drift out
  // of sync with each other about where SSTables live for a given run.
  const sstableDir = path.join(path.dirname(walPath), 'sstables');

  const store = new Store(walPath, sstableDir);
  await store.open();

  if (mode === 'fixed') {
    await store.put('a', '1');
    await store.put('b', '2');
    await store.delete('a');
    await store.put('c', '3');
    // Deliberately no close() — this is the simulated crash point.
    process.exit(0);
  } else if (mode === 'loop') {
    let i = 0;
    for (;;) {
      const key = `key${i}`;
      const value = `value${i}`;
      await store.put(key, value);
      // Only printed once the write is fully durable (fsync resolved).
      console.log(`WROTE ${key}=${value}`);
      i += 1;
      await new Promise((resolve) => setTimeout(resolve, 15));
    }
  } else {
    console.error(`Unknown mode: ${mode}`);
    process.exit(1);
  }
}

main();