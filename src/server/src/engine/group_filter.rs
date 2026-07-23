// Copyright 2026-present The Sekas Authors.
// Copyright 2023 The Engula Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::ffi::{CStr, CString};
use std::sync::OnceLock;

use rocksdb::compaction_filter::{CompactionFilter, Decision};
use rocksdb::compaction_filter_factory::{CompactionFilterContext, CompactionFilterFactory};
use sekas_schema::system::txn::TXN_INTENT_VERSION;

use crate::engine::group::keys;
use crate::engine::mvcc_gc::min_allowed_version_from_retention;

pub(crate) struct GroupCompactionFilter {
    min_allowed_version: u64,
    current_key: Option<(u64, Vec<u8>)>,
    floor_version_kept: bool,
}

impl GroupCompactionFilter {
    pub(crate) fn new(min_allowed_version: u64) -> Self {
        GroupCompactionFilter { min_allowed_version, current_key: None, floor_version_kept: false }
    }

    fn filter_mvcc_key(&mut self, key: &[u8]) -> Decision {
        if !keys::is_mvcc_key(key) {
            return Decision::Keep;
        }
        let Some(parsed) = keys::parse_mvcc_key(key) else {
            return Decision::Keep;
        };

        let current_key = (parsed.table_id, parsed.encoded_user_key.to_vec());
        if self.current_key.as_ref() != Some(&current_key) {
            self.current_key = Some(current_key);
            self.floor_version_kept = false;
        }

        if self.min_allowed_version == 0
            || parsed.version == TXN_INTENT_VERSION
            || parsed.version >= self.min_allowed_version
        {
            return Decision::Keep;
        }

        if !self.floor_version_kept {
            self.floor_version_kept = true;
            return Decision::Keep;
        }

        Decision::Remove
    }
}

pub(crate) struct GroupCompactionFactory {
    retention_ms: u64,
}

impl GroupCompactionFactory {
    pub(crate) fn new(retention_ms: u64) -> Self {
        GroupCompactionFactory { retention_ms }
    }
}

impl CompactionFilter for GroupCompactionFilter {
    fn filter(&mut self, _level: u32, key: &[u8], _value: &[u8]) -> Decision {
        self.filter_mvcc_key(key)
    }

    fn name(&self) -> &CStr {
        filter_name()
    }
}

impl CompactionFilterFactory for GroupCompactionFactory {
    type Filter = GroupCompactionFilter;

    fn create(&mut self, _context: CompactionFilterContext) -> Self::Filter {
        GroupCompactionFilter::new(min_allowed_version_from_retention(self.retention_ms))
    }

    fn name(&self) -> &CStr {
        filter_name()
    }
}

fn filter_name() -> &'static CStr {
    static NAME: OnceLock<CString> = OnceLock::new();
    NAME.get_or_init(|| CString::new("group compaction filter").unwrap()).as_c_str()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn keep_non_mvcc_keys() {
        let mut filter = GroupCompactionFilter::new(60);
        assert!(matches!(filter.filter_mvcc_key(b"APPLY_STATE"), Decision::Keep));
    }

    #[test]
    fn keep_floor_version_and_remove_older_versions() {
        let mut filter = GroupCompactionFilter::new(60);
        let versions = [100, 80, 50, 20];
        let decisions = versions
            .into_iter()
            .map(|version| filter.filter_mvcc_key(&keys::mvcc_key(1, b"k", version)))
            .collect::<Vec<_>>();

        assert!(matches!(decisions[0], Decision::Keep));
        assert!(matches!(decisions[1], Decision::Keep));
        assert!(matches!(decisions[2], Decision::Keep));
        assert!(matches!(decisions[3], Decision::Remove));
    }

    #[test]
    fn keep_single_low_version_for_partial_compaction() {
        let mut filter = GroupCompactionFilter::new(60);
        let decision = filter.filter_mvcc_key(&keys::mvcc_key(1, b"k", 20));
        assert!(matches!(decision, Decision::Keep));
    }

    #[test]
    fn reset_floor_state_when_user_key_changes() {
        let mut filter = GroupCompactionFilter::new(60);
        assert!(matches!(filter.filter_mvcc_key(&keys::mvcc_key(1, b"a", 50)), Decision::Keep));
        assert!(matches!(filter.filter_mvcc_key(&keys::mvcc_key(1, b"a", 20)), Decision::Remove));
        assert!(matches!(filter.filter_mvcc_key(&keys::mvcc_key(1, b"b", 20)), Decision::Keep));
    }

    #[test]
    fn reset_floor_state_when_table_changes() {
        let mut filter = GroupCompactionFilter::new(60);
        assert!(matches!(filter.filter_mvcc_key(&keys::mvcc_key(1, b"k", 50)), Decision::Keep));
        assert!(matches!(filter.filter_mvcc_key(&keys::mvcc_key(1, b"k", 20)), Decision::Remove));
        assert!(matches!(filter.filter_mvcc_key(&keys::mvcc_key(2, b"k", 20)), Decision::Keep));
    }

    #[test]
    fn keep_txn_intent_version() {
        let mut filter = GroupCompactionFilter::new(60);
        let decision = filter.filter_mvcc_key(&keys::mvcc_key(1, b"k", TXN_INTENT_VERSION));
        assert!(matches!(decision, Decision::Keep));
    }
}
