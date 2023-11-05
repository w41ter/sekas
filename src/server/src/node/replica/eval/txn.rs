// Copyright 2023 The Sekas Authors.
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

#![allow(unused)]

use std::sync::Mutex;
use std::time::{Duration, Instant, SystemTime};

use dashmap::mapref::entry::Entry;
use dashmap::DashMap;
use futures::channel::oneshot;
use prost::Message;
use sekas_api::server::v1::{
    ClearIntentRequest, CommitIntentRequest, TxnState, WriteIntentRequest,
};

use super::cas::eval_conditions;
use super::{LatchGuard, ShardKey};
use crate::engine::{GroupEngine, WriteBatch};
use crate::node::migrate::ForwardCtx;
use crate::node::replica::ExecCtx;
use crate::serverpb::v1::{EvalResult, WriteBatchRep};
use crate::{Error, Result};

pub(crate) async fn write_intent(
    exec_ctx: &ExecCtx,
    group_engine: &GroupEngine,
    req: &WriteIntentRequest,
) -> Result<EvalResult> {
    // TODO(walter) support migration?
    let mut wb = WriteBatch::default();
    for put in &req.puts {
        let value = group_engine.get(req.shard_id, &put.key).await?;
        if let Some((encoded_intent, super::INTENT_KEY_VERSION)) = &value {
            let intent = WriteIntent::decode(encoded_intent.as_slice())?;
            if intent.start_version == req.start_version {
                // To support idempotent.
                continue;
            }

            // TODO: resolve intent.
        }
        if !put.conditions.is_empty() {
            let value = value.as_ref().map(|(v, _)| v.as_slice());
            eval_conditions(value, &put.conditions)?;
            eval_put_op(put, value);
        }
        let intent = WriteIntent {
            start_version: req.start_version,
            value: put.value.clone(),
            is_delete: false,
            deadline: req.deadline,
        };
        let value = intent.encode_to_vec();
        group_engine.put(&mut wb, req.shard_id, &put.key, &value, super::INTENT_KEY_VERSION)?;
    }
    for del in &req.deletes {
        let value = group_engine.get(req.shard_id, &del.key).await?;
        if let Some((encoded_intent, super::INTENT_KEY_VERSION)) = &value {
            let intent = WriteIntent::decode(encoded_intent.as_slice())?;
            if intent.start_version == req.start_version {
                // To support idempotent.
                continue;
            }

            // TODO: resolve intent.
        }
        if !del.conditions.is_empty() {
            let value = value.as_ref().map(|(v, _)| v.as_slice());
            eval_conditions(value, &del.conditions)?;
        }

        let intent = WriteIntent {
            start_version: req.start_version,
            value: vec![],
            is_delete: true,
            deadline: req.deadline,
        };
        let value = intent.encode_to_vec();
        group_engine.put(&mut wb, req.shard_id, &del.key, &value, super::INTENT_KEY_VERSION)?;
    }

    Ok(EvalResult {
        batch: Some(WriteBatchRep { data: wb.data().to_owned() }),
        ..Default::default()
    })
}

pub(crate) async fn commit_intent(
    exec_ctx: &ExecCtx,
    group_engine: &GroupEngine,
    req: &CommitIntentRequest,
) -> Result<EvalResult> {
    // FIXME(walter) support migration.
    let mut wb = WriteBatch::default();
    for key in &req.keys {
        // Skip not exists intent
        let Some((value, super::INTENT_KEY_VERSION)) = group_engine.get(req.shard_id, key).await?
        else {
            continue;
        };

        // Avoid to commit other txn intents.
        let intent = WriteIntent::decode(value.as_slice())?;
        if intent.start_version != req.start_version {
            continue;
        }
        group_engine.delete(&mut wb, req.shard_id, key, super::INTENT_KEY_VERSION);
        if intent.is_delete {
            group_engine.tombstone(&mut wb, req.shard_id, key, req.commit_version);
        } else {
            group_engine.put(&mut wb, req.shard_id, key, &intent.value, req.commit_version);
        }
    }

    Ok(EvalResult {
        batch: Some(WriteBatchRep { data: wb.data().to_owned() }),
        ..Default::default()
    })
}

pub(crate) async fn clear_intent(
    exec_ctx: &ExecCtx,
    group_engine: &GroupEngine,
    req: &ClearIntentRequest,
) -> Result<EvalResult> {
    // FIXME(walter) support migration.
    let mut wb = WriteBatch::default();
    for key in &req.keys {
        // Skip not exists intent
        let Some((value, super::INTENT_KEY_VERSION)) = group_engine.get(req.shard_id, key).await?
        else {
            continue;
        };

        // Avoid to commit other txn intents.
        let intent = WriteIntent::decode(value.as_slice())?;
        if intent.start_version != req.start_version {
            continue;
        }
        group_engine.delete(&mut wb, req.shard_id, key, super::INTENT_KEY_VERSION);
    }

    Ok(EvalResult {
        batch: Some(WriteBatchRep { data: wb.data().to_owned() }),
        ..Default::default()
    })
}

#[derive(Default)]
struct IntentRecord {
    waiters: Vec<oneshot::Sender<()>>,
}

struct TxnResolveManager {
    txn_client: TxnClient,
    intent_records: DashMap<(u64, ShardKey), IntentRecord>,
}

impl TxnResolveManager {
    /// Wait the txn intent to finish, return `None` if deadline is reached.
    async fn wait_txn_intent(
        &self,
        start_version: u64,
        deadline: u64,
        shard_id: u64,
        key: &[u8],
        latch_guard: LatchGuard,
    ) -> Result<LatchGuard, LatchGuard> {
        let receiver = self.insert_waiter(start_version, shard_id, key);
        let latch_mgr = &latch_guard.latch_manager;
        assert_eq!(shard_id, latch_guard.shard_key.shard_id);
        assert_eq!(key, latch_guard.shard_key.user_key);
        latch_mgr.release(shard_id, key);

        let now = sekas_rock::time::timestamp_millis();
        let expired = if now < deadline {
            // Need to sleep
            let duration = Duration::from_millis(deadline - now);
            crate::runtime::time::timeout(duration, receiver).await.is_err()
        } else {
            receiver.await;
            false
        };

        let latch_guard = latch_mgr.acquire(shard_id, key, None).await.unwrap();
        if expired {
            Ok(latch_guard)
        } else {
            Err(latch_guard)
        }
    }

    fn insert_waiter(
        &self,
        start_version: u64,
        shard_id: u64,
        key: &[u8],
    ) -> oneshot::Receiver<()> {
        let shard_key = ShardKey { shard_id, user_key: key.to_owned() };
        let intent_key = (start_version, shard_key);
        let (sender, receiver) = oneshot::channel();
        let mut entry = self.intent_records.entry(intent_key).or_default();
        let value = entry.value_mut();
        value.waiters.push(sender);
        receiver
    }

    /// Resolve the state of txn, abort or commit.
    async fn resolve_txn(&self, start_version: u64, latch_guard: LatchGuard) -> Result<TxnState> {
        let mut txn_client = self.txn_client.clone();
        match txn_client.abort_txn(start_version).await {
            Ok(_) => Ok(TxnState::Aborted),
            Err(sekas_client::Error::CasFailed(_)) => Ok(TxnState::Aborted),
            Err(err) => Err(err.into()),
        }
    }

    /// Notify the state of txn intent.
    async fn notify_intent(&self, start_version: u64, shard_id: u64, key: &[u8]) {
        let shard_key = ShardKey { shard_id, user_key: key.to_owned() };
        let intent_key = (start_version, shard_key);
        if let Some((_, intent_record)) = self.intent_records.remove(&intent_key) {
            for waiter in intent_record.waiters {
                let _ = waiter.send(());
            }
        }
    }
}

#[cfg(test)]
mod tests {
    // TODO: add test
    // 1. commit intent and clear intent is idempotent.
    // 2. only commit or clear intent with the same start_version.
    // 3. insert intent is idempotent.
}
