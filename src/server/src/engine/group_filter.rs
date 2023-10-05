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

use rocksdb::{
    compaction_filter::{CompactionFilter, Decision},
    compaction_filter_factory::CompactionFilterFactory,
};

struct GroupCompactionFilter {
    min_allowed_version: u64,
}

impl CompactionFilter for GroupCompactionFilter {
    fn filter(&mut self, level: u32, key: &[u8], value: &[u8]) -> Decision {
        todo!()
    }

    /// Returns a name that identifies this compaction filter.
    /// The name will be printed to LOG file on start up for diagnosis.
    fn name(&self) -> &CStr {
        todo!()
    }
}

struct GroupCompactionFactory {}

impl CompactionFilterFactory for GroupCompactionFactory {
    type Filter: CompactionFilter;

    /// Returns a CompactionFilter for the compaction process
    fn create(&mut self, context: CompactionFilterContext) -> Self::Filter;

    /// Returns a name that identifies this compaction filter factory.
    fn name(&self) -> &CStr {
        &Cstr::new("group compaction filter")
    }
}
