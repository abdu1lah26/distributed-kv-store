/**
 * Store integration tests (Milestone 2, Step 4) — run with:
 *   npx tsx src/storage/store.test.ts
 */
import { strict as assert } from 'node:assert';
import { promises as fs } from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { Store } from '../storage/store.js';

function test(name: string, fn: () => Promise<void>): Promise<void> {
  return fn()
    .then(() => console.log(`  ✓ ${name}`))
    .catch((err) => {
      console.error(`  ✗ ${name}`);
      throw err;
    });
}

async function withTmpDir<T>(fn: (dir: string) => Promise<T>): Promise<T> {
  const dir = await fs.mkdtemp(path.join(os.tmpdir(), 'store-test-'));
  try {
    return await fn(dir);
  } finally {
    await fs.rm(dir, { recursive: true, force: true });
  }
}

async function listSSTableFiles(dir: string): Promise<string[]> {
  const files = await fs.readdir(dir);
  return files.filter((f) => f.startsWith('sstable_') && f.endsWith('.dat'));
}

async function main(): Promise<void> {
  console.log('Store integration tests');

  await test('put then get returns the value, no flush yet for a small write', async () => {
    await withTmpDir(async (dir) => {
      const store = new Store(path.join(dir, 'wal.log'), path.join(dir, 'sstables'));
      await store.open();
      await store.put('a', '1');
      assert.equal(await store.get('a'), '1');
      assert.deepEqual(await listSSTableFiles(path.join(dir, 'sstables')), []);
      await store.close();
    });
  });

  await test('delete then get returns undefined', async () => {
    await withTmpDir(async (dir) => {
      const store = new Store(path.join(dir, 'wal.log'), path.join(dir, 'sstables'));
      await store.open();
      await store.put('a', '1');
      await store.delete('a');
      assert.equal(await store.get('a'), undefined);
      await store.close();
    });
  });

  await test('crossing the 1000-key threshold triggers a flush to a new SSTable', async () => {
    await withTmpDir(async (dir) => {
      const sstableDir = path.join(dir, 'sstables');
      const store = new Store(path.join(dir, 'wal.log'), sstableDir);
      await store.open();

      for (let i = 0; i < 1000; i++) {
        await store.put(`key_${String(i).padStart(4, '0')}`, `value_${i}`);
      }

      const files = await listSSTableFiles(sstableDir);
      assert.equal(files.length, 1, 'expected exactly one SSTable after crossing the key threshold');

      // A read for a flushed key should now be served from the SSTable,
      // since the MemTable was cleared after flush.
      assert.equal(await store.get('key_0000'), 'value_0');
      assert.equal(await store.get('key_0999'), 'value_999');

      await store.close();
    });
  });

  await test('a read for a key falls through MemTable -> SSTable correctly after a flush', async () => {
    await withTmpDir(async (dir) => {
      const sstableDir = path.join(dir, 'sstables');
      const store = new Store(path.join(dir, 'wal.log'), sstableDir);
      await store.open();

      for (let i = 0; i < 1000; i++) {
        await store.put(`k${String(i).padStart(4, '0')}`, `flushed_${i}`);
      }
      // This key lands in the fresh (post-flush) MemTable, not the SSTable.
      await store.put('fresh_key', 'fresh_value');

      assert.equal(await store.get('k0500'), 'flushed_500'); // from SSTable
      assert.equal(await store.get('fresh_key'), 'fresh_value'); // from MemTable
      assert.equal(await store.get('does_not_exist'), undefined); // absent everywhere

      await store.close();
    });
  });

  await test('a delete after a flush correctly shadows the older SSTable value', async () => {
    await withTmpDir(async (dir) => {
      const sstableDir = path.join(dir, 'sstables');
      const store = new Store(path.join(dir, 'wal.log'), sstableDir);
      await store.open();

      // Flush 'target' into an SSTable along with filler to cross the threshold.
      await store.put('target', 'old_value');
      for (let i = 0; i < 999; i++) {
        await store.put(`filler_${String(i).padStart(4, '0')}`, 'x');
      }
      assert.equal((await listSSTableFiles(sstableDir)).length, 1);
      assert.equal(await store.get('target'), 'old_value');

      // Now delete it — tombstone lands fresh in the (cleared) MemTable,
      // and must shadow the stale 'old_value' still sitting in the SSTable.
      await store.delete('target');
      assert.equal(await store.get('target'), undefined);

      await store.close();
    });
  });

  await test('newest SSTable wins when the same key was flushed twice with different values', async () => {
    await withTmpDir(async (dir) => {
      const sstableDir = path.join(dir, 'sstables');
      const store = new Store(path.join(dir, 'wal.log'), sstableDir);
      await store.open();

      // First flush: 'k' = 'first'
      await store.put('k', 'first');
      for (let i = 0; i < 999; i++) {
        await store.put(`filler_a_${String(i).padStart(4, '0')}`, 'x');
      }
      assert.equal((await listSSTableFiles(sstableDir)).length, 1);

      // Second flush: 'k' = 'second' — must land in a newer SSTable file.
      await store.put('k', 'second');
      for (let i = 0; i < 999; i++) {
        await store.put(`filler_b_${String(i).padStart(4, '0')}`, 'x');
      }
      assert.equal((await listSSTableFiles(sstableDir)).length, 2);

      assert.equal(await store.get('k'), 'second', 'newest SSTable value should win');

      await store.close();
    });
  });

  await test('open() discovers and correctly orders existing SSTables from a prior run', async () => {
    await withTmpDir(async (dir) => {
      const walPath = path.join(dir, 'wal.log');
      const sstableDir = path.join(dir, 'sstables');

      // Run 1: flush twice, same key both times, then close (simulating
      // a clean-ish shutdown; WAL is never truncated regardless).
      {
        const store = new Store(walPath, sstableDir);
        await store.open();
        await store.put('k', 'first');
        for (let i = 0; i < 999; i++) {
          await store.put(`fa_${String(i).padStart(4, '0')}`, 'x');
        }
        await store.put('k', 'second');
        for (let i = 0; i < 999; i++) {
          await store.put(`fb_${String(i).padStart(4, '0')}`, 'x');
        }
        await store.close();
      }
      assert.equal((await listSSTableFiles(sstableDir)).length, 2);

      // Run 2: fresh Store instance pointed at the same directories —
      // must discover both SSTables, newest first, AND correctly replay
      // the WAL (which still has every entry, per the no-truncation
      // decision) into a fresh MemTable.
      {
        const store2 = new Store(walPath, sstableDir);
        await store2.open();
        assert.equal(await store2.get('k'), 'second', 'newest-first SSTable ordering must survive a restart');
        await store2.close();
      }
    });
  });

  console.log('\nAll Store integration tests passed.');
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});