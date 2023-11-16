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

#![allow(unused)]

use std::sync::Mutex;
use std::time::{Duration, Instant, SystemTime};

use dashmap::mapref::entry::Entry;
use dashmap::DashMap;
use futures::channel::oneshot;
use log::debug;
use prost::Message;
use sekas_api::server::v1::{
    ClearIntentRequest, CommitIntentRequest, PutType, ShardWriteResponse, TxnIntent, TxnState,
    Value, WriteIntentRequest, WriteIntentResponse, WriteResponse,
};
use sekas_rock::num::decode_i64;
use sekas_schema::system::txn::{TXN_INTENT_VERSION, TXN_MAX_VERSION};

use super::cas::eval_conditions;
use super::{LatchGuard, ShardKey};
use crate::engine::{GroupEngine, SnapshotMode, WriteBatch};
use crate::node::migrate::ForwardCtx;
use crate::node::replica::ExecCtx;
use crate::serverpb::v1::{EvalResult, WriteBatchRep};
use crate::{Error, Result};

pub(crate) async fn write_intent(
    exec_ctx: &ExecCtx,
    group_engine: &GroupEngine,
    req: &WriteIntentRequest,
) -> Result<(Option<EvalResult>, WriteIntentResponse)> {
    // TODO(walter) support migration?
    let write = req
        .write
        .as_ref()
        .ok_or_else(|| Error::InvalidArgument("`write` is required".to_string()))?;

    let mut wb = WriteBatch::default();
    let mut resp = ShardWriteResponse::default();
    for del in &write.deletes {
        let (txn_intent, prev_value) =
            read_intent_and_next_key(group_engine, req.start_version, write.shard_id, &del.key)?;
        let mut skip_write = false;
        if let Some(txn_intent) = txn_intent {
            if txn_intent.start_version != req.start_version {
                todo!("wait until the former intent to commit or clear");
            }

            // Support idempotent.
            debug!(
                "the intent of key {:?} already exists, shard {}, start version {}",
                del.key, write.shard_id, req.start_version
            );
            skip_write = true;
        }
        if !skip_write {
            eval_conditions(prev_value.as_ref(), &del.conditions)?;
            let txn_intent = TxnIntent::tombstone(req.start_version).encode_to_vec();
            group_engine.put(&mut wb, write.shard_id, &del.key, &txn_intent, TXN_INTENT_VERSION)?;
        }
        if del.take_prev_value {
            resp.deletes.push(WriteResponse { prev_value });
        }
    }
    for put in &write.puts {
        let (txn_intent, prev_value) =
            read_intent_and_next_key(group_engine, req.start_version, write.shard_id, &put.key)?;
        let mut skip_write = false;
        if let Some(txn_intent) = txn_intent {
            if txn_intent.start_version != req.start_version {
                todo!("wait until the former intent to commit or clear");
            }

            // Support idempotent.
            debug!(
                "the intent of key {:?} already exists, shard {}, start version {}",
                put.key, write.shard_id, req.start_version
            );
            skip_write = true;
        }
        if !skip_write {
            eval_conditions(prev_value.as_ref(), &put.conditions)?;
            let apply_value = apply_put_op(put.put_type(), prev_value.as_ref(), put.value.clone())?;
            let txn_intent = TxnIntent::with_put(req.start_version, apply_value).encode_to_vec();
            group_engine.put(&mut wb, write.shard_id, &put.key, &txn_intent, TXN_INTENT_VERSION)?;
        }
        if put.take_prev_value {
            resp.puts.push(WriteResponse { prev_value });
        }
    }

    let eval_result =
        if !wb.is_empty() { Some(EvalResult::with_batch(wb.data().to_owned())) } else { None };
    Ok((eval_result, WriteIntentResponse { write: Some(resp) }))
}

pub(crate) async fn commit_intent(
    exec_ctx: &ExecCtx,
    group_engine: &GroupEngine,
    req: &CommitIntentRequest,
) -> Result<Option<EvalResult>> {
    // FIXME(walter) support migration.
    let mut wb = WriteBatch::default();
    for key in &req.keys {
        let Some(intent) =
            read_target_intent(group_engine, req.start_version, req.shard_id, key).await?
        else {
            continue;
        };
        group_engine.delete(&mut wb, req.shard_id, key, TXN_INTENT_VERSION);
        if intent.is_delete {
            group_engine.tombstone(&mut wb, req.shard_id, key, req.commit_version);
        } else if let Some(value) = intent.value {
            group_engine.put(&mut wb, req.shard_id, key, &value, req.commit_version);
        }
    }

    Ok(if wb.is_empty() { None } else { Some(EvalResult::with_batch(wb.data().to_owned())) })
}

pub(crate) async fn clear_intent(
    exec_ctx: &ExecCtx,
    group_engine: &GroupEngine,
    req: &ClearIntentRequest,
) -> Result<Option<EvalResult>> {
    // FIXME(walter) support migration.
    let mut wb = WriteBatch::default();
    for key in &req.keys {
        let Some(intent) =
            read_target_intent(group_engine, req.start_version, req.shard_id, key).await?
        else {
            continue;
        };
        group_engine.delete(&mut wb, req.shard_id, key, TXN_INTENT_VERSION);
    }

    Ok(if wb.is_empty() { None } else { Some(EvalResult::with_batch(wb.data().to_owned())) })
}

fn apply_put_op(
    r#type: PutType,
    prev_value: Option<&Value>,
    value: Vec<u8>,
) -> Result<Option<Vec<u8>>> {
    match r#type {
        PutType::AddI64 => {
            let delta = decode_i64(&value)
                .ok_or_else(|| Error::InvalidArgument("input value is not a valid i64".into()))?;

            let former_value = match prev_value.and_then(|v| v.content.as_ref()) {
                Some(content) => decode_i64(&content).ok_or_else(|| {
                    Error::InvalidArgument("the exists value is not a valid i64".into())
                })?,
                None => 0,
            };
            Ok(Some(former_value.wrapping_add(delta).to_le_bytes().to_vec()))
        }
        PutType::None => Ok(Some(value)),
        PutType::Nop => Ok(None),
    }
}

fn read_intent_and_next_key(
    engine: &GroupEngine,
    start_version: u64,
    shard_id: u64,
    key: &[u8],
) -> Result<(Option<TxnIntent>, Option<Value>)> {
    let snapshot = engine.snapshot(shard_id, SnapshotMode::Key { key })?;
    if let Some(mvcc_iter) = snapshot.mvcc_iter() {
        let mut mvcc_iter = mvcc_iter?;
        if let Some(entry) = mvcc_iter.next() {
            let mut entry = entry?;
            if entry.version() == TXN_INTENT_VERSION {
                let content = entry.value().ok_or_else(|| {
                    Error::InvalidData(format!(
                        "intent value must exist, shard={}, key={:?}, txn={}",
                        shard_id, key, start_version,
                    ))
                })?;
                let txn_intent = TxnIntent::decode(content)?;
                let prev_value = mvcc_iter.next().transpose()?.map(|v| Into::<Value>::into(v));
                return Ok((Some(txn_intent), prev_value));
            } else {
                return Ok((None, Some(entry.into())));
            }
        }
    }
    Ok((None, None))
}

async fn read_target_intent(
    engine: &GroupEngine,
    start_version: u64,
    shard_id: u64,
    key: &[u8],
) -> Result<Option<TxnIntent>> {
    let value = engine.get(shard_id, key).await?;
    let Some(value) = value else { return Ok(None) };
    if value.version != TXN_INTENT_VERSION {
        return Ok(None);
    }

    let content = value.content.ok_or_else(|| {
        Error::InvalidData(format!("txn intent without value, shard {shard_id} key {key:?}"))
    })?;

    let intent = TxnIntent::decode(content.as_slice())?;

    // To support idempotent.
    if intent.start_version != start_version {
        return Ok(None);
    }
    Ok(Some(intent))
}

// #[derive(Default)]
// struct IntentRecord {
//     waiters: Vec<oneshot::Sender<()>>,
// }

// struct TxnResolveManager {
//     txn_client: TxnClient,
//     intent_records: DashMap<(u64, ShardKey), IntentRecord>,
// }

// impl TxnResolveManager {
//     /// Wait the txn intent to finish, return `None` if deadline is reached.
//     async fn wait_txn_intent(
//         &self,
//         start_version: u64,
//         deadline: u64,
//         shard_id: u64,
//         key: &[u8],
//         latch_guard: LatchGuard,
//     ) -> Result<LatchGuard, LatchGuard> {
//         let receiver = self.insert_waiter(start_version, shard_id, key);
//         let latch_mgr = &latch_guard.latch_manager;
//         assert_eq!(shard_id, latch_guard.shard_key.shard_id);
//         assert_eq!(key, latch_guard.shard_key.user_key);
//         latch_mgr.release(shard_id, key);

//         let now = sekas_rock::time::timestamp_millis();
//         let expired = if now < deadline {
//             // Need to sleep
//             let duration = Duration::from_millis(deadline - now);
//             sekas_runtime::time::timeout(duration, receiver).await.is_err()
//         } else {
//             receiver.await;
//             false
//         };

//         let latch_guard = latch_mgr.acquire(shard_id, key,
// None).await.unwrap();         if expired {
//             Ok(latch_guard)
//         } else {
//             Err(latch_guard)
//         }
//     }

//     fn insert_waiter(
//         &self,
//         start_version: u64,
//         shard_id: u64,
//         key: &[u8],
//     ) -> oneshot::Receiver<()> {
//         let shard_key = ShardKey { shard_id, user_key: key.to_owned() };
//         let intent_key = (start_version, shard_key);
//         let (sender, receiver) = oneshot::channel();
//         let mut entry = self.intent_records.entry(intent_key).or_default();
//         let value = entry.value_mut();
//         value.waiters.push(sender);
//         receiver
//     }

//     /// Resolve the state of txn, abort or commit.
//     async fn resolve_txn(&self, start_version: u64, latch_guard: LatchGuard)
// -> Result<TxnState> {         let mut txn_client = self.txn_client.clone();
//         match txn_client.abort_txn(start_version).await {
//             Ok(_) => Ok(TxnState::Aborted),
//             Err(sekas_client::Error::CasFailed(_)) => Ok(TxnState::Aborted),
//             Err(err) => Err(err.into()),
//         }
//     }

//     /// Notify the state of txn intent.
//     async fn notify_intent(&self, start_version: u64, shard_id: u64, key:
// &[u8]) {         let shard_key = ShardKey { shard_id, user_key:
// key.to_owned() };         let intent_key = (start_version, shard_key);
//         if let Some((_, intent_record)) =
// self.intent_records.remove(&intent_key) {             for waiter in
// intent_record.waiters {                 let _ = waiter.send(());
//             }
//         }
//     }
// }

#[cfg(test)]
mod tests {
    use sekas_rock::fn_name;
    use tempdir::TempDir;

    use super::*;
    use crate::engine::{create_group_engine, WriteStates};

    // TODO: add test
    // 1. commit intent and clear intent is idempotent.
    // 2. only commit or clear intent with the same start_version.
    // 3. insert intent is idempotent.
    #[test]
    fn apply_nop() {
        assert!(apply_put_op(PutType::Nop, None, vec![]).unwrap().is_none());
        assert!(apply_put_op(PutType::Nop, Some(&Value::tombstone(123)), vec![])
            .unwrap()
            .is_none());
        assert!(apply_put_op(PutType::Nop, Some(&Value::with_value(vec![], 123)), vec![])
            .unwrap()
            .is_none());
    }

    fn commit_values(engine: &GroupEngine, key: &[u8], values: &[Value]) {
        let mut wb = WriteBatch::default();
        for Value { version, content } in values {
            if let Some(value) = content {
                engine.put(&mut wb, 1, key, &value, *version).unwrap();
            } else {
                engine.tombstone(&mut wb, 1, key, *version).unwrap();
            }
        }
        engine.commit(wb, WriteStates::default(), false).unwrap();
    }

    #[sekas_macro::test]
    fn load_recent_keys() {
        struct TestCase {
            expect_intent: Option<TxnIntent>,
            expect_prev_value: Option<Value>,
        }

        let cases = vec![
            // No values
            TestCase { expect_intent: None, expect_prev_value: None },
            // No intent, but prev value exists
            TestCase { expect_intent: None, expect_prev_value: Some(Value::with_value(vec![], 1)) },
            // No intent, but prev tombstone exists
            TestCase { expect_intent: None, expect_prev_value: Some(Value::tombstone(1)) },
            // Has intent, and prev tombstone exists
            TestCase {
                expect_intent: Some(TxnIntent::with_put(123, Some(vec![]))),
                expect_prev_value: Some(Value::tombstone(1)),
            },
            // Has intent, and prev value exists
            TestCase {
                expect_intent: Some(TxnIntent::with_put(123, Some(vec![]))),
                expect_prev_value: Some(Value::with_value(vec![], 1)),
            },
            // Has intent, no prev value exists
            TestCase {
                expect_intent: Some(TxnIntent::with_put(123, Some(vec![]))),
                expect_prev_value: None,
            },
        ];

        let dir = TempDir::new(fn_name!()).unwrap();
        let engine = create_group_engine(dir.path(), 1, 1, 1).await;
        let mut idx: u8 = 0;
        for TestCase { expect_intent, expect_prev_value } in cases {
            let mut values = vec![];
            if let Some(intent) = expect_intent.as_ref() {
                values.push(Value::with_value(intent.encode_to_vec(), TXN_INTENT_VERSION));
            }
            if let Some(value) = expect_prev_value.as_ref() {
                values.push(value.clone());
            }
            commit_values(&engine, &[idx], &values);
            let (intent, prev_value) = read_intent_and_next_key(&engine, 123, 1, &[idx]).unwrap();

            assert_eq!(intent, expect_intent, "idx={idx}");
            assert_eq!(prev_value, expect_prev_value, "idx={idx}");
            idx += 1;
        }
    }
}
