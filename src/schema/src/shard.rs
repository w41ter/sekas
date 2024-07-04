// Copyright 2023-present The Sekas Authors.
// Copyright 2022 The Engula Authors.
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

use sekas_api::server::v1::*;

lazy_static::lazy_static! {
    pub static ref SHARD_MIN: Vec<u8> = vec![];
    pub static ref SHARD_MAX: Vec<u8> = vec![];
}

pub fn in_range(start: &[u8], end: &[u8], use_key: &[u8]) -> bool {
    start <= use_key && (use_key < end || end.is_empty())
}

/// Return whether a user key belongs to the corresponding shard.
pub fn belong_to(shard: &ShardDesc, user_key: &[u8]) -> bool {
    shard
        .range
        .as_ref()
        .map(|range| in_range(&range.start, &range.end, user_key))
        .unwrap_or_default()
}

/// Return the start key of the corresponding shard.
#[inline]
pub fn start_key(shard: &ShardDesc) -> Vec<u8> {
    shard.range.as_ref().map(|range| range.start.clone()).unwrap_or_default()
}

/// Return the end key of the corresponding shard.
///
/// For now, it only support range shard.
#[inline]
pub fn end_key(shard: &ShardDesc) -> Vec<u8> {
    shard.range.as_ref().map(|range| range.end.clone()).unwrap_or_default()
}
