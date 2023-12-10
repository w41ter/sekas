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
use crate::engine::{GroupEngine, MvccIterator, Snapshot, SnapshotMode};
use crate::node::move_shard::ForwardCtx;
use crate::replica::ExecCtx;
use crate::{Error, Result};

/// Scan the specified range.
pub(crate) async fn scan<T>(
    exec_ctx: &ExecCtx,
    engine: &GroupEngine,
    latch_mgr: &T,
    req: &ShardScanRequest,
) -> Result<ShardScanResponse>
where
    T: LatchManager,
{
    let mut req = req.clone();
    let mut forward_target_group_id = None;
    if let Some(move_shard_desc) = exec_ctx.move_shard_desc.as_ref() {
        if move_shard_desc.get_shard_id() == req.shard_id
            && move_shard_desc.src_group_epoch == exec_ctx.group_id
        {
            // Scan value set and forward it to target group.
            req.include_raw_data = true;
            req.ignore_txn_intent = true;
            req.start_version = TXN_INTENT_VERSION;
            forward_target_group_id = Some(move_shard_desc.dest_group_epoch);
        }
    }

    let snapshot_mode = match &req.prefix {
        Some(prefix) => {
            req.exclude_end_key = false;
            req.exclude_start_key = false;
            SnapshotMode::Prefix { key: prefix }
        }
        None => SnapshotMode::Start { start_key: req.start_key.as_ref().map(|v| v.as_ref()) },
    };
    let snapshot = engine.snapshot(req.shard_id, snapshot_mode)?;
    let resp = scan_inner(latch_mgr, snapshot, &req).await?;
    if let Some(dest_group_id) = forward_target_group_id {
        Err(Error::Forward(ForwardCtx {
            shard_id: req.shard_id,
            dest_group_id,
            payloads: resp.data,
        }))
    } else {
        Ok(resp)
    }
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
    let mut has_more = false;
    while let Some(mvcc_iter) = snapshot.next() {
        let mvcc_iter = mvcc_iter?;
        if is_exceeds(&req.end_key, mvcc_iter.user_key()) {
            break;
        }

        let value_set_opt = scan_value_set(mvcc_iter, latch_mgr, req).await?;
        let Some((value_set, value_bytes)) = value_set_opt else { continue };

        data.push(value_set);
        total_bytes += value_bytes;

        // ATTN: the iterator needs to ensure that all values of a key are returned.
        if (req.limit != 0 && req.limit as usize == data.len())
            || (req.limit_bytes != 0 && req.limit_bytes as usize <= total_bytes)
        {
            has_more = true;
            break;
        }
    }
    Ok(ShardScanResponse { data, has_more })
}

async fn scan_value_set<T: LatchManager>(
    mut mvcc_iter: MvccIterator<'_, '_>,
    latch_mgr: &T,
    req: &ShardScanRequest,
) -> Result<Option<(ValueSet, usize)>> {
    let mut values = Vec::default();
    let mut total_bytes = 0;
    for entry in &mut mvcc_iter {
        let entry = entry?;
        let (user_key, mut version) = (entry.user_key(), entry.version());
        if is_exclude_boundary(req, user_key) {
            // skip exclude keys.
            return Ok(None);
        }

        let value;
        if version == TXN_INTENT_VERSION && !req.ignore_txn_intent {
            let intent_value = entry.value().ok_or_else(|| {
                Error::InvalidData(format!("the value of intent key {user_key:?} is not exists",))
            })?;
            match resolve_txn(latch_mgr, req.shard_id, req.start_version, user_key, intent_value)
                .await?
            {
                Some(v) => (value, version) = v,
                None => continue,
            }
        } else if req.start_version < version {
            // skip invisible versions.
            continue;
        } else {
            value = entry.value().map(ToOwned::to_owned);
        }

        if let Some(value) = value {
            total_bytes += value.len();
            values.push(Value { content: Some(value), version });
        } else if req.include_raw_data {
            values.push(Value { content: None, version });
        }

        if !req.include_raw_data {
            // only returns the first non-tombstone version.
            break;
        }
    }
    if values.is_empty() {
        return Ok(None);
    }

    let user_key = mvcc_iter.user_key();
    total_bytes += user_key.len();
    let value_set = ValueSet { user_key: user_key.to_owned(), values };
    Ok(Some((value_set, total_bytes)))
}

#[inline]
fn is_equals(target: &Option<Vec<u8>>, user_key: &[u8]) -> bool {
    target.as_ref().map(|target_key| target_key == user_key).unwrap_or_default()
}

#[inline]
fn is_exceeds(target: &Option<Vec<u8>>, user_key: &[u8]) -> bool {
    target.as_ref().map(|target_key| target_key.as_slice() < user_key).unwrap_or_default()
}

#[inline]
fn is_exclude_boundary(req: &ShardScanRequest, user_key: &[u8]) -> bool {
    if req.exclude_start_key && is_equals(&req.start_key, user_key) {
        return true;
    }

    if req.exclude_end_key && is_equals(&req.end_key, user_key) {
        return true;
    }

    false
}

async fn resolve_txn<T: LatchManager>(
    latch_mgr: &T,
    shard_id: u64,
    start_version: u64,
    user_key: &[u8],
    encoded_intent_value: &[u8],
) -> Result<Option<(Option<Vec<u8>>, u64)>> {
    let intent = TxnIntent::decode(encoded_intent_value)?;
    if intent.start_version > start_version {
        // skip invisible versions.
        return Ok(None);
    }

    let intent_value_opt =
        latch_mgr.resolve_txn(shard_id, user_key, start_version, intent.start_version).await?;

    // skip aborted txn value.
    let Some(intent_value) = intent_value_opt else { return Ok(None) };
    if intent_value.version > start_version {
        // skip invisible versions.
        return Ok(None);
    }

    Ok(Some((intent_value.content, intent_value.version)))
}

#[cfg(test)]
mod tests {
    use sekas_api::server::v1::Value;
    use sekas_rock::fn_name;
    use tempdir::TempDir;

    use super::*;
    use crate::engine::{create_group_engine, WriteBatch, WriteStates};
    use crate::replica::eval::latch::local::LocalLatchManager;

    const SHARD_ID: u64 = 1;

    fn commit_values(engine: &GroupEngine, key: &[u8], values: &[Value]) {
        let mut wb = WriteBatch::default();
        for Value { version, content } in values {
            if let Some(value) = content {
                engine.put(&mut wb, SHARD_ID, key, value, *version).unwrap();
            } else {
                engine.tombstone(&mut wb, SHARD_ID, key, *version).unwrap();
            }
        }
        engine.commit(wb, WriteStates::default(), false).unwrap();
    }

    #[sekas_macro::test]
    async fn scan_with_txn_intent() {
        // 1. write intent with version 90
        // 2. take snapshot at version 100
        // 3. commit intent with version 95
        // 4. write intent with version 99
        // 5. commit intent with version 101
        // 6. scan try resolve intent 90, and it should returns version 95.
    }

    #[sekas_macro::test]
    async fn scan_with_limit_should_returns_all_versions() {
        let dir = TempDir::new(fn_name!()).unwrap();
        let engine = create_group_engine(dir.path(), 1, 1, 1).await;
        let latch_mgr = LocalLatchManager::default();

        let key = vec![0u8];
        for i in 1..101u8 {
            let value = vec![i];
            let value = Value::with_value(value, i as u64);
            commit_values(&engine, &key, &[value]);
        }

        let scan_req = ShardScanRequest {
            shard_id: SHARD_ID,
            start_version: 1000,
            limit: 1,
            include_raw_data: true,
            ..Default::default()
        };
        let resp = scan(&ExecCtx::default(), &engine, &latch_mgr, &scan_req).await.unwrap();
        assert_eq!(resp.data.len(), 1);
        assert_eq!(resp.data[0].values.len(), 100);
    }

    #[sekas_macro::test]
    async fn scan_with_limit_should_returns_more() {
        let dir = TempDir::new(fn_name!()).unwrap();
        let engine = create_group_engine(dir.path(), 1, 1, 1).await;
        let latch_mgr = LocalLatchManager::default();

        for i in 1..100u8 {
            let (key, value) = (vec![i], vec![i]);
            let value = Value::with_value(value, 100);
            commit_values(&engine, &key, &[value]);
        }

        // case 1: scan single key returns has more.
        let scan_req = ShardScanRequest {
            shard_id: SHARD_ID,
            start_version: 1000,
            limit: 1,
            ..Default::default()
        };
        let resp = scan(&ExecCtx::default(), &engine, &latch_mgr, &scan_req).await.unwrap();
        assert!(resp.has_more);

        // case 2: scan all keys returns no more.
        let scan_req = ShardScanRequest {
            shard_id: SHARD_ID,
            start_version: 1000,
            limit: 1000,
            ..Default::default()
        };

        let resp = scan(&ExecCtx::default(), &engine, &latch_mgr, &scan_req).await.unwrap();
        assert!(!resp.has_more);
    }

    #[sekas_macro::test]
    async fn scan_with_request_range() {
        let dir = TempDir::new(fn_name!()).unwrap();
        let engine = create_group_engine(dir.path(), 1, 1, 1).await;
        let latch_mgr = LocalLatchManager::default();

        for i in 1..100u8 {
            let (key, value) = (vec![i], vec![i]);
            let value = Value::with_value(value, 100);
            commit_values(&engine, &key, &[value]);
        }

        // case 1: scan exclude start key.
        let scan_req = ShardScanRequest {
            shard_id: SHARD_ID,
            start_version: 1000,
            start_key: Some(vec![1u8]),
            exclude_start_key: true,
            limit: 1,
            ..Default::default()
        };
        let resp = scan(&ExecCtx::default(), &engine, &latch_mgr, &scan_req).await.unwrap();
        assert_eq!(resp.data.len(), 1);
        assert_eq!(resp.data[0].user_key, vec![2u8]);

        // case 2: scan exclude end key.
        let scan_req = ShardScanRequest {
            shard_id: SHARD_ID,
            start_version: 1000,
            start_key: Some(vec![1u8]),
            end_key: Some(vec![2u8]),
            exclude_end_key: true,
            limit: 2,
            ..Default::default()
        };
        let resp = scan(&ExecCtx::default(), &engine, &latch_mgr, &scan_req).await.unwrap();
        assert!(!resp.has_more);
        assert_eq!(resp.data.len(), 1);
        assert_eq!(resp.data[0].user_key, vec![1u8]);

        // case 3: scan in range.
        let scan_req = ShardScanRequest {
            shard_id: SHARD_ID,
            start_version: 1000,
            start_key: Some(vec![3u8]),
            end_key: Some(vec![4u8]),
            ..Default::default()
        };
        let resp = scan(&ExecCtx::default(), &engine, &latch_mgr, &scan_req).await.unwrap();
        assert_eq!(resp.data.len(), 2);
        assert_eq!(resp.data[0].user_key, vec![3u8]);
        assert_eq!(resp.data[1].user_key, vec![4u8]);
    }

    #[sekas_macro::test]
    async fn scan_with_prefix() {
        let dir = TempDir::new(fn_name!()).unwrap();
        let engine = create_group_engine(dir.path(), 1, 1, 1).await;
        let latch_mgr = LocalLatchManager::default();

        // prepare keys
        // a1, b1, b2, c1
        let i: u8 = 1;
        let (key, value) = (vec![b'a', i], vec![i]);
        let value = Value::with_value(value, 100);
        commit_values(&engine, &key, &[value]);

        let (key, value) = (vec![b'b', i], vec![i]);
        let value = Value::with_value(value, 100);
        commit_values(&engine, &key, &[value]);

        let i = 2;
        let (key, value) = (vec![b'b', i], vec![i]);
        let value = Value::with_value(value, 100);
        commit_values(&engine, &key, &[value]);

        let i = 1;
        let (key, value) = (vec![b'c', i], vec![i]);
        let value = Value::with_value(value, 100);
        commit_values(&engine, &key, &[value]);

        // case 1. scan b'a'
        let scan_req = ShardScanRequest {
            shard_id: SHARD_ID,
            start_version: 1000,
            prefix: Some(vec![b'a']),
            ..Default::default()
        };
        let resp = scan(&ExecCtx::default(), &engine, &latch_mgr, &scan_req).await.unwrap();
        assert_eq!(resp.data.len(), 1);
        assert_eq!(resp.data[0].user_key, vec![b'a', 1]);

        // case 2. scan b'b'
        let scan_req = ShardScanRequest {
            shard_id: SHARD_ID,
            start_version: 1000,
            prefix: Some(vec![b'b']),
            ..Default::default()
        };
        let resp = scan(&ExecCtx::default(), &engine, &latch_mgr, &scan_req).await.unwrap();
        assert_eq!(resp.data.len(), 2);
        assert_eq!(resp.data[0].user_key, vec![b'b', 1]);
        assert_eq!(resp.data[1].user_key, vec![b'b', 2]);

        // case 3. scan b'c'
        let scan_req = ShardScanRequest {
            shard_id: SHARD_ID,
            start_version: 1000,
            prefix: Some(vec![b'c']),
            ..Default::default()
        };
        let resp = scan(&ExecCtx::default(), &engine, &latch_mgr, &scan_req).await.unwrap();
        assert_eq!(resp.data.len(), 1);
        assert_eq!(resp.data[0].user_key, vec![b'c', 1]);

        // case 4. scan b'd'
        let scan_req = ShardScanRequest {
            shard_id: SHARD_ID,
            start_version: 1000,
            prefix: Some(vec![b'd']),
            ..Default::default()
        };
        let resp = scan(&ExecCtx::default(), &engine, &latch_mgr, &scan_req).await.unwrap();
        assert!(resp.data.is_empty());
    }

    #[sekas_macro::test]
    async fn scan_value_set_ignore_tombstones() {
        let dir = TempDir::new(fn_name!()).unwrap();
        let engine = create_group_engine(dir.path(), 1, 1, 1).await;
        let latch_mgr = LocalLatchManager::default();

        // prepare keys
        // a1 [value] 100,
        // b1 [tombstone] 100, [value] 90
        let i: u8 = 1;
        let (key, value) = (vec![b'a', i], vec![i]);
        let value = Value::with_value(value, 100);
        commit_values(&engine, &key, &[value]);

        let key = vec![b'b', i];
        let value = Value::tombstone(100);
        commit_values(&engine, &key, &[value]);

        let (key, value) = (vec![b'b', i], vec![i]);
        let value = Value::with_value(value, 90);
        commit_values(&engine, &key, &[value]);

        // case 1. the tombstone will be ignored.
        let scan_req =
            ShardScanRequest { shard_id: SHARD_ID, start_version: 1000, ..Default::default() };
        let resp = scan(&ExecCtx::default(), &engine, &latch_mgr, &scan_req).await.unwrap();
        assert_eq!(resp.data.len(), 1);
        assert_eq!(resp.data[0].user_key, vec![b'a', 1]);

        // case 2. the value is visible if tombstone is not visible.
        let scan_req =
            ShardScanRequest { shard_id: SHARD_ID, start_version: 99, ..Default::default() };
        let resp = scan(&ExecCtx::default(), &engine, &latch_mgr, &scan_req).await.unwrap();
        assert_eq!(resp.data.len(), 1);
        assert_eq!(resp.data[0].user_key, vec![b'b', 1]);
        assert_eq!(resp.data[0].values[0].version, 90);
    }

    #[sekas_macro::test]
    async fn scan_value_set_with_include_raws() {
        let dir = TempDir::new(fn_name!()).unwrap();
        let engine = create_group_engine(dir.path(), 1, 1, 1).await;
        let latch_mgr = LocalLatchManager::default();

        // prepare keys
        // a1 [value] 100,
        // b1 [tombstone] 100, [value] 90
        let i: u8 = 1;
        let (key, value) = (vec![b'a', i], vec![i]);
        let value = Value::with_value(value, 100);
        commit_values(&engine, &key, &[value]);

        let key = vec![b'b', i];
        let value = Value::tombstone(100);
        commit_values(&engine, &key, &[value]);

        let (key, value) = (vec![b'b', i], vec![i]);
        let value = Value::with_value(value, 90);
        commit_values(&engine, &key, &[value]);

        let scan_req = ShardScanRequest {
            shard_id: SHARD_ID,
            start_version: 1000,
            include_raw_data: true,
            ..Default::default()
        };
        let resp = scan(&ExecCtx::default(), &engine, &latch_mgr, &scan_req).await.unwrap();
        assert_eq!(resp.data.len(), 2);
        assert_eq!(resp.data[0].user_key, vec![b'a', 1]);
        assert_eq!(resp.data[1].user_key, vec![b'b', 1]);
        assert_eq!(resp.data[1].values.len(), 2);
        assert_eq!(resp.data[1].values[0].version, 100);
        assert_eq!(resp.data[1].values[1].version, 90);
    }

    #[sekas_macro::test]
    async fn scan_value_set_ignore_txn_intent() {
        let dir = TempDir::new(fn_name!()).unwrap();
        let engine = create_group_engine(dir.path(), 1, 1, 1).await;
        let latch_mgr = LocalLatchManager::default();

        // prepare keys
        // a1 [value] TXN INTENT, [value] 100
        let i: u8 = 1;
        let (key, value) = (vec![b'a', i], vec![i]);
        let value = Value::with_value(value, TXN_INTENT_VERSION);
        commit_values(&engine, &key, &[value]);
        let value = Value::with_value(vec![i], 100);
        commit_values(&engine, &key, &[value]);

        // case 1: ignore txn intent
        let scan_req = ShardScanRequest {
            shard_id: SHARD_ID,
            start_version: 1000,
            include_raw_data: true,
            ignore_txn_intent: true,
            ..Default::default()
        };
        let resp = scan(&ExecCtx::default(), &engine, &latch_mgr, &scan_req).await.unwrap();
        assert_eq!(resp.data.len(), 1);
        assert_eq!(resp.data[0].user_key, vec![b'a', 1]);
        assert_eq!(resp.data[0].values.len(), 1);
        assert_eq!(resp.data[0].values[0].version, 100);

        // case 2: ignore txn intent and with TXN_INTENT_VERSION
        let scan_req = ShardScanRequest {
            shard_id: SHARD_ID,
            start_version: TXN_INTENT_VERSION,
            include_raw_data: true,
            ignore_txn_intent: true,
            ..Default::default()
        };
        let resp = scan(&ExecCtx::default(), &engine, &latch_mgr, &scan_req).await.unwrap();
        assert_eq!(resp.data.len(), 1);
        assert_eq!(resp.data[0].user_key, vec![b'a', 1]);
        assert_eq!(resp.data[0].values.len(), 2);
        assert_eq!(resp.data[0].values[0].version, TXN_INTENT_VERSION);
        assert_eq!(resp.data[0].values[1].version, 100);
    }

    #[sekas_macro::test]
    async fn scan_with_shard_moving_should_ignore_txn_intent() {
        let dir = TempDir::new(fn_name!()).unwrap();
        let engine = create_group_engine(dir.path(), 1, 1, 1).await;
        let latch_mgr = LocalLatchManager::default();

        // prepare keys
        // a1 [value] TXN INTENT, [value] 100
        // b1 [tombstone] 90
        let i: u8 = 1;
        let key = vec![b'a', i];
        let value = TxnIntent::with_put(110, Some(vec![1u8, 2, 3])).encode_to_vec();
        let value = Value::with_value(value, TXN_INTENT_VERSION);
        commit_values(&engine, &key, &[value]);
        let value = Value::with_value(vec![i], 100);
        commit_values(&engine, &key, &[value]);

        let key = vec![b'b', i];
        let value = Value::tombstone(90);
        commit_values(&engine, &key, &[value]);

        let scan_req =
            ShardScanRequest { shard_id: SHARD_ID, start_version: 1000, ..Default::default() };
        let exec_ctx = ExecCtx {
            group_id: 123,
            move_shard_desc: Some(MoveShardDesc {
                shard_desc: Some(ShardDesc { id: SHARD_ID, ..Default::default() }),
                src_group_id: 123,
                src_group_epoch: 123,
                dest_group_epoch: 123,
                dest_group_id: 1234,
            }),
            ..Default::default()
        };
        let values = match scan(&exec_ctx, &engine, &latch_mgr, &scan_req).await {
            Err(Error::Forward(ctx)) => ctx.payloads,
            _ => panic!("this request should be forward to target group"),
        };
        assert_eq!(values.len(), 2);
        assert_eq!(values[0].user_key, vec![b'a', 1]);
        assert_eq!(values[0].values.len(), 2);
        assert_eq!(values[0].values[0].version, TXN_INTENT_VERSION);
        assert_eq!(values[0].values[1].version, 100);
        assert_eq!(values[1].user_key, vec![b'b', 1]);
        assert_eq!(values[1].values[0].version, 90);
    }
}
