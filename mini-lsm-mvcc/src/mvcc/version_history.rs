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

use std::collections::BTreeMap;
use std::ops::Bound;

use anyhow::Result;
use bytes::Bytes;

use crate::iterators::StorageIterator;
use crate::mem_table::map_bound;

use super::txn::Transaction;

/// A single version of a key in its version history.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VersionEntry {
    /// The commit timestamp of the version. Versions written by the current transaction but not
    /// yet committed carry `u64::MAX` as a placeholder, as they are newer than any committed
    /// version visible to the transaction.
    pub commit_ts: u64,
    /// The value of this version. Empty when `is_delete` is true.
    pub value: Bytes,
    /// Whether this version is a deletion tombstone.
    pub is_delete: bool,
}

/// The version history of a single key, as seen by a transaction.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct KeyVersionHistory {
    /// Visible versions, ordered from the newest to the oldest. A pending local write of the
    /// transaction, if any, sits on top as a synthetic entry.
    pub entries: Vec<VersionEntry>,
    /// The total number of visible entries before applying the per-key entry limit.
    pub total: usize,
}

/// Check whether `key` still falls within the upper bound of a scan. The raw storage iterators do
/// not stop at the upper bound by themselves, so callers of `scan_all_versions` must enforce it.
fn within_upper_bound(key: &[u8], upper: Bound<&[u8]>) -> bool {
    match upper {
        Bound::Included(bound) => key <= bound,
        Bound::Excluded(bound) => key < bound,
        Bound::Unbounded => true,
    }
}

impl Transaction {
    /// Collect all committed versions in `[lower, upper]` that are visible to this transaction
    /// (commit ts <= read ts), grouped by key. Within each group, versions are ordered from the
    /// newest to the oldest. Deletion tombstones are kept as entries with empty values.
    fn collect_committed_versions(
        &self,
        lower: Bound<&[u8]>,
        upper: Bound<&[u8]>,
    ) -> Result<BTreeMap<Bytes, Vec<VersionEntry>>> {
        let mut iter = self.inner.scan_all_versions(lower, upper)?;
        let mut groups: BTreeMap<Bytes, Vec<VersionEntry>> = BTreeMap::new();
        while iter.is_valid() {
            let key = iter.key();
            if !within_upper_bound(key.key_ref(), upper) {
                break;
            }
            if key.ts() <= self.read_ts {
                let value = iter.value();
                groups
                    .entry(Bytes::copy_from_slice(key.key_ref()))
                    .or_default()
                    .push(VersionEntry {
                        commit_ts: key.ts(),
                        value: Bytes::copy_from_slice(value),
                        is_delete: value.is_empty(),
                    });
            }
            iter.next()?;
        }
        Ok(groups)
    }

    /// Prepend the pending local write of `key`, if any, as a synthetic top entry. The covered
    /// committed versions remain listed below it exactly once.
    fn overlay_local_write(&self, key: &[u8], entries: &mut Vec<VersionEntry>) {
        if let Some(local) = self.local_storage.get(key) {
            let value = local.value().clone();
            entries.insert(
                0,
                VersionEntry {
                    commit_ts: u64::MAX,
                    is_delete: value.is_empty(),
                    value,
                },
            );
        }
    }

    /// Fetch the version history of a single key from the transaction's point of view.
    pub fn get_version_history(&self, key: &[u8]) -> Result<KeyVersionHistory> {
        if self.committed.load(std::sync::atomic::Ordering::SeqCst) {
            panic!("cannot operate on committed txn!");
        }
        let mut groups =
            self.collect_committed_versions(Bound::Included(key), Bound::Included(key))?;
        let mut entries = groups.remove(key).unwrap_or_default();
        self.overlay_local_write(key, &mut entries);
        let total = entries.len();
        Ok(KeyVersionHistory { entries, total })
    }

    /// Fetch the version histories of all keys in `[lower, upper)` (bounds follow the same
    /// semantics as [`Transaction::scan`]), with at most `limit` newest entries kept per key.
    pub fn scan_version_history(
        &self,
        lower: Bound<&[u8]>,
        upper: Bound<&[u8]>,
        limit: Option<usize>,
    ) -> Result<Vec<(Bytes, KeyVersionHistory)>> {
        if self.committed.load(std::sync::atomic::Ordering::SeqCst) {
            panic!("cannot operate on committed txn!");
        }
        // A zero limit means an empty result, not "no limit".
        if limit == Some(0) {
            return Ok(Vec::new());
        }
        let mut groups = self.collect_committed_versions(lower, upper)?;
        // Overlay pending local writes within the range as synthetic top entries.
        for entry in self
            .local_storage
            .range((map_bound(lower), map_bound(upper)))
        {
            let value = entry.value().clone();
            groups
                .entry(entry.key().clone())
                .or_default()
                .insert(
                    0,
                    VersionEntry {
                        commit_ts: u64::MAX,
                        is_delete: value.is_empty(),
                        value,
                    },
                );
        }
        Ok(groups
            .into_iter()
            .map(|(key, mut entries)| {
                let total = entries.len();
                if let Some(limit) = limit {
                    // Truncation only drops the oldest entries; the newest ones are always kept.
                    entries.truncate(limit);
                }
                (key, KeyVersionHistory { entries, total })
            })
            .collect())
    }
}
