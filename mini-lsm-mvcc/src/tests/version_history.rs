// Copyright (c) 2022-2026 Alex Chi Z
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::ops::Bound;

use bytes::Bytes;
use tempfile::tempdir;

use crate::{
    compact::CompactionOptions,
    iterators::StorageIterator,
    lsm_storage::{LsmStorageOptions, MiniLsm, WriteBatchRecord},
    mvcc::version_history::{KeyVersionHistory, VersionEntry},
};

fn entry(commit_ts: u64, value: &[u8], is_delete: bool) -> VersionEntry {
    VersionEntry {
        commit_ts,
        value: Bytes::copy_from_slice(value),
        is_delete,
    }
}

#[test]
fn test_single_key_history_visibility_and_order() {
    let dir = tempdir().unwrap();
    let options = LsmStorageOptions::default_for_week2_test(CompactionOptions::NoCompaction);
    let storage = MiniLsm::open(&dir, options).unwrap();
    // ts 1 and ts 2 land in an SST after the flush, ts 3 stays in the memtable.
    storage
        .write_batch(&[WriteBatchRecord::Put("a", "v1")])
        .unwrap();
    storage
        .write_batch(&[WriteBatchRecord::Put("a", "v2")])
        .unwrap();
    storage.force_flush().unwrap();

    // This transaction takes its snapshot before the ts-3 write is committed.
    let txn = storage.new_txn().unwrap();
    storage
        .write_batch(&[WriteBatchRecord::Put("a", "v3")])
        .unwrap();

    // Only versions committed at or before the read ts show up, newest first.
    let history = txn.get_version_history(b"a").unwrap();
    assert_eq!(
        history,
        KeyVersionHistory {
            entries: vec![entry(2, b"v2", false), entry(1, b"v1", false)],
            total: 2,
        }
    );
    // The normal point read agrees with the newest history entry.
    assert_eq!(txn.get(b"a").unwrap(), Some(Bytes::from("v2")));
    drop(txn);

    // A fresh transaction sees all committed versions, across both the SST and the memtable.
    let txn = storage.new_txn().unwrap();
    let history = txn.get_version_history(b"a").unwrap();
    assert_eq!(
        history,
        KeyVersionHistory {
            entries: vec![
                entry(3, b"v3", false),
                entry(2, b"v2", false),
                entry(1, b"v1", false)
            ],
            total: 3,
        }
    );
    assert_eq!(txn.get(b"a").unwrap(), Some(Bytes::from("v3")));
}

#[test]
fn test_local_write_overlays_chain_top() {
    let dir = tempdir().unwrap();
    let options = LsmStorageOptions::default_for_week2_test(CompactionOptions::NoCompaction);
    let storage = MiniLsm::open(&dir, options).unwrap();
    storage
        .write_batch(&[WriteBatchRecord::Put("a", "v1")])
        .unwrap();
    storage
        .write_batch(&[WriteBatchRecord::Put("a", "v2")])
        .unwrap();

    let txn = storage.new_txn().unwrap();
    // A pending local put becomes a synthetic entry on top of the chain; the covered chain-top
    // version (ts 2) is still listed exactly once below it.
    txn.put(b"a", b"local");
    let history = txn.get_version_history(b"a").unwrap();
    assert_eq!(
        history,
        KeyVersionHistory {
            entries: vec![
                entry(u64::MAX, b"local", false),
                entry(2, b"v2", false),
                entry(1, b"v1", false),
            ],
            total: 3,
        }
    );
    assert_eq!(txn.get(b"a").unwrap(), Some(Bytes::from("local")));

    // A pending local delete also occupies one entry, flagged as a tombstone.
    txn.delete(b"a");
    let history = txn.get_version_history(b"a").unwrap();
    assert_eq!(
        history,
        KeyVersionHistory {
            entries: vec![
                entry(u64::MAX, b"", true),
                entry(2, b"v2", false),
                entry(1, b"v1", false),
            ],
            total: 3,
        }
    );
    assert_eq!(txn.get(b"a").unwrap(), None);

    // Another transaction on the same key sees only the committed versions.
    let other = storage.new_txn().unwrap();
    let history = other.get_version_history(b"a").unwrap();
    assert_eq!(
        history,
        KeyVersionHistory {
            entries: vec![entry(2, b"v2", false), entry(1, b"v1", false)],
            total: 2,
        }
    );
    assert_eq!(other.get(b"a").unwrap(), Some(Bytes::from("v2")));
}

#[test]
fn test_delete_tombstones_and_empty_history() {
    let dir = tempdir().unwrap();
    let options = LsmStorageOptions::default_for_week2_test(CompactionOptions::NoCompaction);
    let storage = MiniLsm::open(&dir, options).unwrap();
    // The key is created, deleted, then recreated; all three versions stay in the history.
    storage
        .write_batch(&[WriteBatchRecord::Put("a", "v1")])
        .unwrap();
    storage
        .write_batch(&[WriteBatchRecord::Del("a")])
        .unwrap();
    storage.force_flush().unwrap();
    storage
        .write_batch(&[WriteBatchRecord::Put("a", "v2")])
        .unwrap();

    let txn = storage.new_txn().unwrap();
    let history = txn.get_version_history(b"a").unwrap();
    assert_eq!(
        history,
        KeyVersionHistory {
            entries: vec![
                entry(3, b"v2", false),
                entry(2, b"", true),
                entry(1, b"v1", false),
            ],
            total: 3,
        }
    );

    // A key that was never written has an empty history.
    let history = txn.get_version_history(b"ghost").unwrap();
    assert_eq!(
        history,
        KeyVersionHistory {
            entries: Vec::new(),
            total: 0,
        }
    );

    // Locally deleting a key without any committed version yields exactly one tombstone entry.
    txn.delete(b"ghost");
    let history = txn.get_version_history(b"ghost").unwrap();
    assert_eq!(
        history,
        KeyVersionHistory {
            entries: vec![entry(u64::MAX, b"", true)],
            total: 1,
        }
    );
}

/// Build a storage where key "a" has versions at ts 1/2/3, "b" at ts 1/2, and "c" has a put at
/// ts 1 plus a tombstone at ts 3. Returns the storage and its temp dir guard.
fn build_multi_key_storage() -> (tempfile::TempDir, std::sync::Arc<MiniLsm>) {
    let dir = tempdir().unwrap();
    let options = LsmStorageOptions::default_for_week2_test(CompactionOptions::NoCompaction);
    let storage = MiniLsm::open(&dir, options).unwrap();
    storage
        .write_batch(&[
            WriteBatchRecord::Put("a", "a1"),
            WriteBatchRecord::Put("b", "b1"),
            WriteBatchRecord::Put("c", "c1"),
        ])
        .unwrap();
    storage.force_flush().unwrap();
    storage
        .write_batch(&[WriteBatchRecord::Put("a", "a2"), WriteBatchRecord::Put("b", "b2")])
        .unwrap();
    storage
        .write_batch(&[WriteBatchRecord::Put("a", "a3"), WriteBatchRecord::Del("c")])
        .unwrap();
    (dir, storage)
}

#[test]
fn test_scan_version_history_groups_and_limits() {
    let (_dir, storage) = build_multi_key_storage();
    let txn = storage.new_txn().unwrap();

    // Without a limit, every visible entry is returned, grouped per key in key order.
    let result = txn
        .scan_version_history(Bound::Unbounded, Bound::Unbounded, None)
        .unwrap();
    assert_eq!(
        result,
        vec![
            (
                Bytes::from("a"),
                KeyVersionHistory {
                    entries: vec![
                        entry(3, b"a3", false),
                        entry(2, b"a2", false),
                        entry(1, b"a1", false),
                    ],
                    total: 3,
                }
            ),
            (
                Bytes::from("b"),
                KeyVersionHistory {
                    entries: vec![entry(2, b"b2", false), entry(1, b"b1", false)],
                    total: 2,
                }
            ),
            (
                Bytes::from("c"),
                KeyVersionHistory {
                    entries: vec![entry(3, b"", true), entry(1, b"c1", false)],
                    total: 2,
                }
            ),
        ]
    );

    // The limit truncates the oldest entries only; the newest entry is always kept and `total`
    // still reports the untruncated count.
    let result = txn
        .scan_version_history(Bound::Unbounded, Bound::Unbounded, Some(1))
        .unwrap();
    assert_eq!(
        result,
        vec![
            (
                Bytes::from("a"),
                KeyVersionHistory {
                    entries: vec![entry(3, b"a3", false)],
                    total: 3,
                }
            ),
            (
                Bytes::from("b"),
                KeyVersionHistory {
                    entries: vec![entry(2, b"b2", false)],
                    total: 2,
                }
            ),
            (
                Bytes::from("c"),
                KeyVersionHistory {
                    entries: vec![entry(3, b"", true)],
                    total: 2,
                }
            ),
        ]
    );

    // A zero limit is an empty result, not "no limit".
    let result = txn
        .scan_version_history(Bound::Unbounded, Bound::Unbounded, Some(0))
        .unwrap();
    assert!(result.is_empty());
}

#[test]
fn test_scan_version_history_bounds_and_local_overlay() {
    let (_dir, storage) = build_multi_key_storage();
    let txn = storage.new_txn().unwrap();
    // Pending local writes: "b" gets a new version on top of its chain, "d" is a brand-new key
    // that only exists locally.
    txn.put(b"b", b"b-local");
    txn.put(b"d", b"d-local");

    // Included/excluded bounds follow the same semantics as a normal scan.
    let result = txn
        .scan_version_history(Bound::Included(b"b"), Bound::Excluded(b"d"), None)
        .unwrap();
    assert_eq!(
        result,
        vec![
            (
                Bytes::from("b"),
                KeyVersionHistory {
                    entries: vec![
                        entry(u64::MAX, b"b-local", false),
                        entry(2, b"b2", false),
                        entry(1, b"b1", false),
                    ],
                    total: 3,
                }
            ),
            (
                Bytes::from("c"),
                KeyVersionHistory {
                    entries: vec![entry(3, b"", true), entry(1, b"c1", false)],
                    total: 2,
                }
            ),
        ]
    );

    // Excluded lower bound skips "a"; unbounded upper bound picks up the local-only key "d".
    let result = txn
        .scan_version_history(Bound::Excluded(b"a"), Bound::Unbounded, Some(1))
        .unwrap();
    assert_eq!(
        result,
        vec![
            (
                Bytes::from("b"),
                KeyVersionHistory {
                    entries: vec![entry(u64::MAX, b"b-local", false)],
                    total: 3,
                }
            ),
            (
                Bytes::from("c"),
                KeyVersionHistory {
                    entries: vec![entry(3, b"", true)],
                    total: 2,
                }
            ),
            (
                Bytes::from("d"),
                KeyVersionHistory {
                    entries: vec![entry(u64::MAX, b"d-local", false)],
                    total: 1,
                }
            ),
        ]
    );

    // A normal scan over the same range shares the key bounds but hides the deleted key "c",
    // while the history above keeps its tombstone entries.
    let mut scan = txn.scan(Bound::Excluded(b"a"), Bound::Unbounded).unwrap();
    let mut keys = Vec::new();
    while scan.is_valid() {
        keys.push(Bytes::copy_from_slice(scan.key()));
        scan.next().unwrap();
    }
    assert_eq!(keys, vec![Bytes::from("b"), Bytes::from("d")]);
}

#[test]
fn test_history_read_does_not_join_read_set() {
    let dir = tempdir().unwrap();
    let mut options = LsmStorageOptions::default_for_week2_test(CompactionOptions::NoCompaction);
    options.serializable = true;
    let storage = MiniLsm::open(&dir, options).unwrap();
    storage
        .write_batch(&[WriteBatchRecord::Put("a", "v1")])
        .unwrap();

    // txn1 reads the history of "a" but only writes "b".
    let txn1 = storage.new_txn().unwrap();
    let history = txn1.get_version_history(b"a").unwrap();
    assert_eq!(history.total, 1);
    txn1.put(b"b", b"from-txn1");

    // A concurrent transaction commits a newer version of "a" after txn1's snapshot.
    let txn2 = storage.new_txn().unwrap();
    txn2.put(b"a", b"v2");
    txn2.commit().unwrap();

    // History reads are not registered in the read set, so txn1 still commits cleanly. (A normal
    // `get` on "a" would have made this commit fail the serializable check.)
    txn1.commit().unwrap();

    let txn3 = storage.new_txn().unwrap();
    assert_eq!(txn3.get(b"a").unwrap(), Some(Bytes::from("v2")));
    assert_eq!(txn3.get(b"b").unwrap(), Some(Bytes::from("from-txn1")));
    let history = txn3.get_version_history(b"a").unwrap();
    assert_eq!(
        history,
        KeyVersionHistory {
            entries: vec![entry(2, b"v2", false), entry(1, b"v1", false)],
            total: 2,
        }
    );
}
