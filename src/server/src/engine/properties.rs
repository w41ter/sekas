// Copyright 2024 The Engula Authors.
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

use std::collections::BTreeMap;
use std::ffi::CStr;

use prost::Message;
use rocksdb::table_properties::*;
use serde::{Deserialize, Serialize};

const PROPERTY_SPLIT_KEYS: &[u8] = b"sekas-split-keys";
const ESTIMATE_KEYS_INTERVALS: usize = 1024;
const ESTIMATE_SIZE_INTERVALS: usize = 16 << 20; // 16MB

/// The table properties collector factory for shard split keys.
#[derive(Debug)]
pub(crate) struct SplitKeyCollectorFactory;

impl TablePropertiesCollectorFactory for SplitKeyCollectorFactory {
    type Collector = SplitKeyCollector;

    fn create(&mut self, _: TablePropertiesCollectorFactoryContext) -> Self::Collector {
        SplitKeyCollector::default()
    }

    fn name(&self) -> &CStr {
        CStr::from_bytes_with_nul(b"sekas-split-key-collector-factory\0").expect("nul is provided")
    }
}

/// The table properties collector for shard split keys.
#[derive(Debug, Default)]
pub(crate) struct SplitKeyCollector {
    keys: Vec<Vec<u8>>,
    num_keys: usize,
    total_size: usize,
    last_estimate_keys: usize,
    last_estimate_size: usize,
}

impl TablePropertiesCollector for SplitKeyCollector {
    fn name(&self) -> &CStr {
        CStr::from_bytes_with_nul(b"sekas-split-key-collector\0").expect("nul is provide")
    }

    fn add_user_key(
        &mut self,
        key: &[u8],
        value: &[u8],
        entry_type: EntryType,
        _seq: u64,
        _file_size: u64,
    ) {
        if !matches!(
            entry_type,
            EntryType::Put
                | EntryType::TimedPut
                | EntryType::Delete
                | EntryType::SingleDelete
                | EntryType::DeleteWithTimestamp
        ) {
            return;
        }

        self.num_keys += 1;
        self.total_size += key.len() + value.len();
        if self.last_estimate_keys + ESTIMATE_KEYS_INTERVALS >= self.num_keys
            || self.last_estimate_size + ESTIMATE_SIZE_INTERVALS >= self.total_size
        {
            self.last_estimate_keys = self.num_keys;
            self.last_estimate_size = self.total_size;
            self.keys.push(key.to_owned());
        }
    }

    fn finish_properties(&mut self) -> BTreeMap<Box<[u8]>, Box<[u8]>> {
        let split_keys = EstimatedSplitKeys { keys: std::mem::take(&mut self.keys) };
        let encoded_split_keys = split_keys.encode_to_vec();
        let mut map = BTreeMap::default();
        map.insert(PROPERTY_SPLIT_KEYS.into(), encoded_split_keys.into_boxed_slice());
        map
    }
}

/// The estimated split keys, collected during table building.
#[derive(Serialize, Deserialize, prost::Message)]
pub(crate) struct EstimatedSplitKeys {
    #[prost(bytes = "vec", repeated, tag = "1")]
    keys: Vec<Vec<u8>>,
}
