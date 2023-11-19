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

// In order to update values without reading, the txn record are split into
// multi keys.
pub const TXN_PREFIX: &[u8] = b"txn_";
pub const TXN_SUFFIX_STATE: &[u8] = b"state";
pub const TXN_SUFFIX_HEARTBEAT: &[u8] = b"hb";
pub const TXN_SUFFIX_COMMIT: &[u8] = b"commit";

/// The boundary of a txn tag.
#[inline]
pub fn txn_lower_key(hash_tag: u8) -> Vec<u8> {
    let mut buf = Vec::with_capacity(32);
    buf.extend_from_slice(TXN_PREFIX);
    buf.push(hash_tag);
    buf
}

/// The prefix of a txn key.
#[inline]
pub fn txn_prefix(hash_tag: u8, txn_id: u64) -> Vec<u8> {
    let mut buf = txn_lower_key(hash_tag);
    buf.extend_from_slice(&txn_id.to_be_bytes());
    buf
}

/// The txn state key.
#[inline]
pub fn txn_state_key(hash_tag: u8, txn_id: u64) -> Vec<u8> {
    let mut buf = txn_prefix(hash_tag, txn_id);
    buf.extend_from_slice(TXN_SUFFIX_STATE);
    buf
}

/// The txn heartbeat key.
#[inline]
pub fn txn_heartbeat_key(hash_tag: u8, txn_id: u64) -> Vec<u8> {
    let mut buf = txn_prefix(hash_tag, txn_id);
    buf.extend_from_slice(TXN_SUFFIX_HEARTBEAT);
    buf
}

/// The txn commit key.
#[inline]
pub fn txn_commit_key(hash_tag: u8, txn_id: u64) -> Vec<u8> {
    let mut buf = txn_prefix(hash_tag, txn_id);
    buf.extend_from_slice(TXN_SUFFIX_COMMIT);
    buf
}
