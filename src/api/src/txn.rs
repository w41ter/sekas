// Copyright 2023-present The Engula Authors.
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

//! A mod to hold the helper functions of txn related structures.

use crate::server::v1::TxnIntent;

impl TxnIntent {
    pub fn tombstone(start_version: u64) -> Self {
        TxnIntent { start_version, is_delete: true, value: None }
    }

    pub fn with_put(start_version: u64, value: Option<Vec<u8>>) -> Self {
        TxnIntent { start_version, is_delete: false, value }
    }
}
