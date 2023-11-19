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
use prost::Message;
use sekas_api::server::v1::*;
use sekas_schema::system::txn::TXN_INTENT_VERSION;

use super::LatchManager;
use crate::engine::{GroupEngine, Snapshot, SnapshotMode};
use crate::Result;

/// Scan the specified range.
pub(crate) async fn scan<T>(
    engine: &GroupEngine,
    latch_mgr: &T,
    req: &ShardScanRequest,
) -> Result<ShardScanResponse>
where
    T: LatchManager,
{
    let mut req = req.clone();
    let snapshot_mode = match &req.prefix {
        Some(prefix) => {
            req.exclude_end_key = false;
            req.exclude_start_key = false;
            SnapshotMode::Prefix { key: prefix }
        }
        None => SnapshotMode::Start { start_key: req.start_key.as_ref().map(|v| v.as_ref()) },
    };
    let snapshot = engine.snapshot(req.shard_id, snapshot_mode)?;
    scan_inner(latch_mgr, snapshot, &req).await
}

async fn scan_inner<T>(
    latch_mgr: &T,
    mut snapshot: Snapshot<'_>,
    req: &ShardScanRequest,
) -> Result<ShardScanResponse>
where
    T: LatchManager,
{
    let mut data = Vec::new();
    let mut total_bytes = 0;
    'OUTER: while let Some(mvcc_iter) = snapshot.next() {
        let mut value_set = ValueSet::default();
        for entry in mvcc_iter? {
            let entry = entry?;

            // skip exclude keys.
            if req.exclude_start_key && is_equals(&req.start_key, entry.user_key()) {
                continue 'OUTER;
            }

            if req.exclude_end_key && is_equals(&req.end_key, entry.user_key()) {
                continue 'OUTER;
            }

            if is_exceeds(&req.end_key, entry.user_key()) {
                break 'OUTER;
            }

            if entry.version() == TXN_INTENT_VERSION {
                let encoded_intent = entry.value().ok_or_else(|| {
                    crate::Error::InvalidData(format!(
                        "the value of intent key {:?} is not exists",
                        entry.user_key()
                    ))
                })?;
                let intent = TxnIntent::decode(encoded_intent)?;
                if intent.start_version < req.start_version {
                    if let Some(value) = latch_mgr
                        .resolve_txn(
                            req.shard_id,
                            entry.user_key(),
                            req.start_version,
                            intent.start_version,
                        )
                        .await?
                    {
                        if value.version < req.start_version {
                            todo!("save result and refresh snapshot")
                        }
                    }
                    // TODO(walter) what happen if a intent is resolved before
                    // ingest?
                }
            }

            // skip invisible versions.
            if req.start_version < entry.version() {
                continue;
            }

            if let Some(value) = entry.value().map(ToOwned::to_owned) {
                total_bytes += value.len();
                value_set.values.push(Value { content: Some(value), version: entry.version() });
            } else if req.include_raw_data {
                value_set.values.push(Value { content: None, version: entry.version() });
            }

            if !value_set.values.is_empty() && value_set.user_key.is_empty() {
                value_set.user_key = entry.user_key().to_owned();
                total_bytes += value_set.user_key.len();
            }

            if !req.include_raw_data {
                // only returns the first non-tombstone version.
                break;
            }
        }

        if !value_set.values.is_empty() {
            data.push(value_set);

            // ATTN: the iterator needs to ensure that all values of a key are returned.
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
    target.as_ref().map(|target_key| target_key == user_key).unwrap_or_default()
}

#[inline]
fn is_exceeds(target: &Option<Vec<u8>>, user_key: &[u8]) -> bool {
    target.as_ref().map(|target_key| target_key.as_slice() < user_key).unwrap_or_default()
}

// TODO(walter) add scan test
