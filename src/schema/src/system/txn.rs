// Copyright 2023-present The Sekas Authors.
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
use std::hash::Hasher;

use fnv::FnvHasher;

/// The version of write intent.
pub const TXN_INTENT_VERSION: u64 = u64::MAX;
/// The max version a txn could be.
pub const TXN_MAX_VERSION: u64 = u64::MAX - 1;

/// Compute the hash tag for a transaction.
#[inline]
pub fn hash_tag(txn_id: u64) -> u8 {
    let mut hasher = FnvHasher::default();
    hasher.write(&txn_id.to_le_bytes());
    hasher.finish() as u8
}
