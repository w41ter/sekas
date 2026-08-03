// Copyright 2026-present The Sekas Authors.
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

use std::sync::Arc;
use std::time::Duration;

use log::{debug, warn};
use prost::Message;
use sekas_api::server::v1::group_request_union::Request;
use sekas_api::server::v1::{
    ClearIntentRequest, CommitIntentRequest, ShardDesc, ShardKey, TxnIntent, TxnState,
};
use sekas_client::TxnStateTable;
use sekas_runtime::JoinHandle;
use sekas_schema::system::txn::TXN_INTENT_VERSION;
use sekas_schema::system::{keys as system_keys, table};

use crate::Result;
use crate::engine::{GroupEngine, SnapshotMode};
use crate::node::NodeConfig;
use crate::replica::{ExecCtx, Replica};

const ORPHAN_ASYNC_INTENT_GRACE_MS: u64 = 30_000;

pub(crate) fn setup(cfg: NodeConfig, replica: Arc<Replica>) -> Option<JoinHandle<()>> {
    if cfg.mvcc_gc_interval_ms == 0 {
        return None;
    }

    Some(sekas_runtime::spawn(async move {
        let interval = Duration::from_millis(cfg.mvcc_gc_interval_ms);
        loop {
            sekas_runtime::time::sleep(interval).await;
            if let Err(err) = resolve_replica(&replica).await {
                warn!(
                    "group {} replica {} async txn resolver: {err}",
                    replica.replica_info().group_id,
                    replica.replica_info().replica_id
                );
            }
        }
    }))
}

async fn resolve_replica(replica: &Arc<Replica>) -> Result<()> {
    if replica.move_shard_state().is_some() {
        debug!(
            "group {} replica {} skip async txn resolver because shard is moving",
            replica.replica_info().group_id,
            replica.replica_info().replica_id
        );
        return Ok(());
    }

    let group_engine = replica.group_engine();
    for shard in replica.descriptor().shards {
        if shard.table_id == table::txn_table_id() {
            resolve_txn_table_shard(replica, &group_engine, &shard).await?;
        } else {
            resolve_data_shard(replica, &group_engine, shard.id).await?;
        }
    }
    Ok(())
}

async fn resolve_txn_table_shard(
    replica: &Arc<Replica>,
    group_engine: &GroupEngine,
    shard: &ShardDesc,
) -> Result<()> {
    let txn_table = TxnStateTable::new(replica.client(), Some(Duration::from_secs(5)));
    let mut snapshot = group_engine.snapshot(shard.id, SnapshotMode::Start { start_key: None })?;
    while let Some(mvcc_iter) = snapshot.next() {
        let mut mvcc_iter = mvcc_iter?;
        let Some(start_version) = parse_txn_state_key(mvcc_iter.user_key()) else {
            continue;
        };
        let Some(entry) = mvcc_iter.next() else {
            continue;
        };
        let entry = entry?;
        let Some(content) = entry.value() else {
            continue;
        };
        let Some(state) = std::str::from_utf8(content).ok().and_then(TxnState::from_str_name)
        else {
            continue;
        };
        if state == TxnState::AsyncCommitting {
            let _ = txn_table.try_commit_async_txn(start_version).await?;
        }
        sekas_runtime::yield_now().await;
    }
    Ok(())
}

fn parse_txn_state_key(user_key: &[u8]) -> Option<u64> {
    let prefix_len = system_keys::TXN_PREFIX.len();
    let tag_len = 1;
    let txn_id_len = std::mem::size_of::<u64>();
    let header_len = prefix_len + tag_len + txn_id_len;
    if user_key.len() != header_len + system_keys::TXN_SUFFIX_STATE.len() {
        return None;
    }
    if &user_key[..prefix_len] != system_keys::TXN_PREFIX {
        return None;
    }
    if &user_key[header_len..] != system_keys::TXN_SUFFIX_STATE {
        return None;
    }
    let txn_id_bytes: [u8; 8] = user_key[prefix_len + tag_len..header_len].try_into().ok()?;
    Some(u64::from_be_bytes(txn_id_bytes))
}

async fn resolve_data_shard(
    replica: &Arc<Replica>,
    group_engine: &GroupEngine,
    shard_id: u64,
) -> Result<()> {
    let mut snapshot = group_engine.snapshot(shard_id, SnapshotMode::Start { start_key: None })?;
    while let Some(mvcc_iter) = snapshot.next() {
        let mut mvcc_iter = mvcc_iter?;
        let user_key = mvcc_iter.user_key().to_vec();
        let Some(entry) = mvcc_iter.next() else {
            continue;
        };
        let entry = entry?;
        if entry.version() != TXN_INTENT_VERSION {
            continue;
        }
        let Some(content) = entry.value() else {
            continue;
        };
        let intent = TxnIntent::decode(content)?;
        if intent.async_commit {
            resolve_async_intent(replica, shard_id, user_key, intent).await?;
        }
        sekas_runtime::yield_now().await;
    }
    Ok(())
}

async fn resolve_async_intent(
    replica: &Arc<Replica>,
    shard_id: u64,
    user_key: Vec<u8>,
    intent: TxnIntent,
) -> Result<()> {
    let txn_table = TxnStateTable::new(replica.client(), Some(Duration::from_secs(5)));
    let start_version = intent.start_version;
    let state = match txn_table.get_txn_record(start_version).await? {
        Some(record) if record.state == TxnState::AsyncCommitting => {
            txn_table.try_commit_async_txn(start_version).await?.state
        }
        Some(record) => record.state,
        None if orphan_intent_expired(intent.deadline_ms) => {
            txn_table.abort_txn_if_absent(start_version).await?
        }
        None => return Ok(()),
    };

    let shard_key = ShardKey { shard_id, user_key };
    match state {
        TxnState::Committed => {
            let Some(record) = txn_table.get_txn_record(start_version).await? else {
                return Ok(());
            };
            let Some(commit_version) = record.commit_version else {
                return Ok(());
            };
            let req = Request::CommitIntent(CommitIntentRequest {
                start_version,
                commit_version,
                shard_keys: vec![shard_key],
            });
            let _ = replica.execute(&mut ExecCtx::with_epoch(replica.epoch()), &req).await?;
        }
        TxnState::Aborted => {
            let req = Request::ClearIntent(ClearIntentRequest {
                start_version,
                shard_keys: vec![shard_key],
            });
            let _ = replica.execute(&mut ExecCtx::with_epoch(replica.epoch()), &req).await?;
        }
        TxnState::Running | TxnState::AsyncCommitting => {}
    }
    Ok(())
}

fn orphan_intent_expired(deadline_ms: u64) -> bool {
    deadline_ms.saturating_add(ORPHAN_ASYNC_INTENT_GRACE_MS) <= sekas_rock::time::timestamp_millis()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn orphan_intent_expiration_respects_grace_period() {
        let now = sekas_rock::time::timestamp_millis();
        assert!(!orphan_intent_expired(now));
        assert!(!orphan_intent_expired(now.saturating_sub(ORPHAN_ASYNC_INTENT_GRACE_MS / 2)));
        assert!(orphan_intent_expired(now.saturating_sub(ORPHAN_ASYNC_INTENT_GRACE_MS + 1)));
    }
}
