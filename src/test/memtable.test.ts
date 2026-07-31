import { strict as assert } from 'node:assert';
import { MemTable, TOMBSTONE } from '../storage/memtable.js';

function testGetOnUntouchedKeyReturnsFoundFalse(): void {
  const memtable = new MemTable();
  const result = memtable.get('missing');
  assert.deepEqual(result, { found: false });
  console.log('PASS: testGetOnUntouchedKeyReturnsFoundFalse');
}

function testPutThenGetReturnsLiveValue(): void {
  const memtable = new MemTable();
  memtable.put('a', '1');
  const result = memtable.get('a');
  assert.deepEqual(result, { found: true, tombstone: false, value: '1' });
  console.log('PASS: testPutThenGetReturnsLiveValue');
}

function testDeleteThenGetReturnsTombstoneNotFoundFalse(): void {
  const memtable = new MemTable();
  memtable.put('a', '1');
  memtable.delete('a');
  const result = memtable.get('a');
  assert.deepEqual(result, { found: true, tombstone: true });
  console.log('PASS: testDeleteThenGetReturnsTombstoneNotFoundFalse');
}

function testDeleteOnNeverPresentKeyStillProducesTombstone(): void {
  // Deleting a key that was never put should still leave a tombstone,
  // not a "found: false" — a delete of a nonexistent key still needs to
  // shadow any older SSTable value once SSTables exist.
  const memtable = new MemTable();
  memtable.delete('never-put');
  const result = memtable.get('never-put');
  assert.deepEqual(result, { found: true, tombstone: true });
  console.log('PASS: testDeleteOnNeverPresentKeyStillProducesTombstone');
}

function testPutDeletePutReturnsLiveValue(): void {
  const memtable = new MemTable();
  memtable.put('a', '1');
  memtable.delete('a');
  memtable.put('a', '2');
  const result = memtable.get('a');
  assert.deepEqual(result, { found: true, tombstone: false, value: '2' });
  console.log('PASS: testPutDeletePutReturnsLiveValue');
}

function testHasReflectsPresenceIncludingTombstones(): void {
  const memtable = new MemTable();
  assert.equal(memtable.has('a'), false, 'untouched key: has() should be false');

  memtable.put('a', '1');
  assert.equal(memtable.has('a'), true, 'live key: has() should be true');

  memtable.delete('a');
  assert.equal(
    memtable.has('a'),
    true,
    'tombstoned key: has() should still be true — the entry persists as a tombstone',
  );

  console.log('PASS: testHasReflectsPresenceIncludingTombstones');
}

function testSizeCountsLiveAndTombstonedEntries(): void {
  const memtable = new MemTable();
  assert.equal(memtable.size(), 0);

  memtable.put('a', '1');
  memtable.put('b', '2');
  assert.equal(memtable.size(), 2);

  memtable.delete('a');
  assert.equal(
    memtable.size(),
    2,
    'deleting should not shrink size — the tombstone still occupies the entry',
  );

  memtable.put('a', '1'); // re-putting an existing key should not double-count
  assert.equal(memtable.size(), 2);

  memtable.put('c', '3');
  assert.equal(memtable.size(), 3);

  console.log('PASS: testSizeCountsLiveAndTombstonedEntries');
}

function testEntriesReturnsSortedKeysWithTombstonesSurfaced(): void {
  const memtable = new MemTable();
  memtable.put('banana', '2');
  memtable.put('apple', '1');
  memtable.put('cherry', '3');
  memtable.delete('banana');

  const entries = memtable.entries();

  assert.deepEqual(
    entries.map(([key]) => key),
    ['apple', 'banana', 'cherry'],
    'entries() should be sorted by key',
  );

  assert.equal(entries.length, 3, 'entries() should return exactly 3 items');

  const appleEntry = entries[0]!;
  const bananaEntry = entries[1]!;
  const cherryEntry = entries[2]!;

  assert.equal(appleEntry[0], 'apple');
  assert.equal(appleEntry[1], '1');

  assert.equal(bananaEntry[0], 'banana');
  assert.equal(
    bananaEntry[1],
    TOMBSTONE,
    'deleted key should surface as the TOMBSTONE sentinel in entries()',
  );

  assert.equal(cherryEntry[0], 'cherry');
  assert.equal(cherryEntry[1], '3');

  console.log('PASS: testEntriesReturnsSortedKeysWithTombstonesSurfaced');
}

function main(): void {
  testGetOnUntouchedKeyReturnsFoundFalse();
  testPutThenGetReturnsLiveValue();
  testDeleteThenGetReturnsTombstoneNotFoundFalse();
  testDeleteOnNeverPresentKeyStillProducesTombstone();
  testPutDeletePutReturnsLiveValue();
  testHasReflectsPresenceIncludingTombstones();
  testSizeCountsLiveAndTombstonedEntries();
  testEntriesReturnsSortedKeysWithTombstonesSurfaced();
  console.log('\nAll MemTable unit tests passed.');
}

main();