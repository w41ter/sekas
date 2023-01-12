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
use engula_api::server::v1::*;

use crate::{
    engine::{GroupEngine, SnapshotMode},
    Result,
};

/// Scan the specified range.
pub(crate) async fn scan(
    engine: &GroupEngine,
    req: &ShardScanRequest,
) -> Result<ShardScanResponse> {
    if let Some(prefix) = &req.prefix {
        return scan_prefix(engine, req.shard_id, prefix).await;
    }

    scan_range(engine, req).await
}

/// Scan key-value pairs with the specified prefix.
async fn scan_prefix(
    engine: &GroupEngine,
    shard_id: u64,
    prefix: &[u8],
) -> Result<ShardScanResponse> {
    // TODO(walter) shall I support migrating?
    let snapshot_mode = SnapshotMode::Prefix { key: prefix };
    let mut snapshot = engine.snapshot(shard_id, snapshot_mode)?;
    let mut data = Vec::new();
    for mvcc_iter in snapshot.iter() {
        let mut mvcc_iter = mvcc_iter?;
        if let Some(entry) = mvcc_iter.next() {
            let entry = entry?;
            if let Some(value) = entry.value().map(ToOwned::to_owned) {
                data.push(ShardData {
                    key: entry.user_key().to_owned(),
                    value,
                    version: entry.version(),
                });
            }
        }
    }
    Ok(ShardScanResponse { data })
}

/// Scan key-value pairs with the specified range.
async fn scan_range(engine: &GroupEngine, req: &ShardScanRequest) -> Result<ShardScanResponse> {
    let snapshot_mode = SnapshotMode::Start {
        start_key: req.start_key.as_ref().map(|v| v.as_ref()),
    };
    let mut snapshot = engine.snapshot(req.shard_id, snapshot_mode)?;
    let mut data = Vec::new();
    let mut total_bytes = 0;
    for mvcc_iter in snapshot.iter() {
        let mut mvcc_iter = mvcc_iter?;
        if let Some(entry) = mvcc_iter.next() {
            let entry = entry?;

            if req.exclude_start_key && is_equals(&req.start_key, entry.user_key()) {
                continue;
            }

            if req.exclude_end_key && is_equals(&req.end_key, entry.user_key()) {
                continue;
            }

            if is_exceeds(&req.end_key, entry.user_key()) {
                break;
            }

            if let Some(value) = entry.value().map(ToOwned::to_owned) {
                let key = entry.user_key().to_owned();
                let version = entry.version();
                total_bytes += value.len() + key.len();
                data.push(ShardData {
                    key,
                    value,
                    version,
                });
            }

            if (req.limit != 0 && req.limit as usize == data.len())
                || (req.limit_bytes != 0 && req.limit_bytes as usize <= total_bytes)
            {
                break;
            }
        }
    }
    Ok(ShardScanResponse { data })
}

#[inline]
fn is_equals(target: &Option<Vec<u8>>, user_key: &[u8]) -> bool {
    target
        .as_ref()
        .map(|target_key| target_key == user_key)
        .unwrap_or_default()
}

#[inline]
fn is_exceeds(target: &Option<Vec<u8>>, user_key: &[u8]) -> bool {
    target
        .as_ref()
        .map(|target_key| target_key.as_slice() < user_key)
        .unwrap_or_default()
}
