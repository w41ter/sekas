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

use log::{debug, trace};
use prost::Message;
use sekas_api::server::v1::group_request_union::Request;
use sekas_api::server::v1::*;
use sekas_rock::num::decode_i64;
use sekas_schema::system::txn::TXN_INTENT_VERSION;

use super::LatchGuard;
use super::cas::eval_conditions;
use super::latch::DeferSignalLatchGuard;
use crate::engine::{GroupEngine, SnapshotMode, WriteBatch};
use crate::node::move_shard::ForwardCtx;
use crate::replica::{ExecCtx, ForwardPart};
use crate::serverpb::v1::EvalResult;
use crate::{Error, Result};

pub(crate) async fn write_intent<T: LatchGuard>(
    exec_ctx: &ExecCtx,
    group_engine: &GroupEngine,
    latch_guard: &mut DeferSignalLatchGuard<T>,
    req: &WriteIntentRequest,
) -> Result<(Option<EvalResult>, WriteIntentResponse, Vec<ForwardPart>)> {
    let mut wb = WriteBatch::default();
    let mut responses = vec![retry_write_result("intent entry is not executed"); req.writes.len()];
    let mut forwards = Vec::new();
    let mut local_indexes = Vec::new();
    for (index, write) in req.writes.iter().enumerate() {
        if let Some(forward_part) =
            write_intent_forward_part(exec_ctx, group_engine, req, index, write).await?
        {
            forwards.push(forward_part);
            continue;
        }

        let resp = write_intent_inner(
            exec_ctx,
            group_engine,
            latch_guard,
            req.start_version,
            req.check_write_conflict,
            write,
            &mut wb,
        )
        .await
        .map_err(|err| match err {
            Error::CasFailed(_, cond_index, prev_value) => {
                Error::CasFailed(index as u64, cond_index, prev_value)
            }
            err => err,
        });
        match resp {
            Ok(resp) => {
                responses[index] = WriteIntentResult::ok(resp);
                local_indexes.push(index);
            }
            Err(err @ (Error::CasFailed(..) | Error::TxnConflict)) => {
                for local_index in local_indexes {
                    responses[local_index] = retry_write_result("intent entry is not executed");
                }
                responses[index] = WriteIntentResult::err(err.into());
                return Ok((None, WriteIntentResponse { writes: responses }, Vec::new()));
            }
            Err(err) => return Err(err),
        }
    }
    let eval_result =
        if !wb.is_empty() { Some(EvalResult::with_batch(wb.data().to_owned())) } else { None };
    Ok((eval_result, WriteIntentResponse { writes: responses }, forwards))
}

async fn write_intent_forward_part(
    exec_ctx: &ExecCtx,
    group_engine: &GroupEngine,
    req: &WriteIntentRequest,
    index: usize,
    write: &ShardWriteRequest,
) -> Result<Option<ForwardPart>> {
    let Some(desc) = exec_ctx.move_shard_desc.as_ref() else {
        return Ok(None);
    };
    let shard_id = desc.shard_desc.as_ref().unwrap().id;
    if shard_id != write.shard_id {
        return Ok(None);
    }

    let write_req = single_write_request(write)?;
    let payload = group_engine.get_all_versions(write.shard_id, write_req.user_key()).await?;
    let request = Request::WriteIntent(WriteIntentRequest {
        start_version: req.start_version,
        writes: vec![write.clone()],
        check_write_conflict: req.check_write_conflict,
    });
    Ok(Some(ForwardPart {
        indexes: vec![index],
        request,
        forward_ctx: ForwardCtx {
            shard_id,
            dest_group_id: desc.dest_group_id,
            payloads: vec![payload],
        },
    }))
}

async fn write_intent_inner<T: LatchGuard>(
    _exec_ctx: &ExecCtx,
    group_engine: &GroupEngine,
    latch_guard: &mut DeferSignalLatchGuard<T>,
    start_version: u64,
    check_write_conflict: bool,
    req: &ShardWriteRequest,
    wb: &mut WriteBatch,
) -> Result<WriteResponse> {
    // TODO(walter) txn for internal shards is not supported.
    let write = single_write_request(req)?;

    let user_key = write.user_key();
    let (skip_write, prev_value) =
        read_first_non_intent_key(latch_guard, group_engine, start_version, req.shard_id, user_key)
            .await?;

    if let Some(value) = prev_value.as_ref() {
        if check_write_conflict && value.version > start_version && !is_atomic_operation(&write) {
            trace!("txn {} are conflict with committed value {}", start_version, value.version);
            return Err(Error::TxnConflict);
        }
    }

    let prev_value = match write {
        WriteRequest::Delete(del) => {
            if !skip_write {
                if let Some(cond_idx) = eval_conditions(prev_value.as_ref(), &del.conditions)? {
                    return Err(Error::CasFailed(0, cond_idx as u64, prev_value));
                }
                let txn_intent = TxnIntent::tombstone(start_version).encode_to_vec();
                group_engine.put(
                    &mut *wb,
                    req.shard_id,
                    &del.key,
                    &txn_intent,
                    TXN_INTENT_VERSION,
                )?;
            }
            if del.take_prev_value { prev_value } else { None }
        }
        WriteRequest::Put(put) => {
            if !skip_write {
                log::debug!("eval conditions {:?}, prev value {:?}", put.conditions, prev_value);
                if let Some(cond_idx) = eval_conditions(prev_value.as_ref(), &put.conditions)? {
                    return Err(Error::CasFailed(0, cond_idx as u64, prev_value));
                }
                let apply_value =
                    apply_put_op(put.put_type(), prev_value.as_ref(), put.value.clone())?;
                let txn_intent = TxnIntent::with_put(start_version, apply_value).encode_to_vec();
                group_engine.put(
                    &mut *wb,
                    req.shard_id,
                    &put.key,
                    &txn_intent,
                    TXN_INTENT_VERSION,
                )?;
            }
            if put.take_prev_value { prev_value } else { None }
        }
    };

    Ok(WriteResponse { prev_value })
}

fn single_write_request(req: &ShardWriteRequest) -> Result<WriteRequest> {
    match (req.deletes.as_slice(), req.puts.as_slice()) {
        ([delete], []) => Ok(WriteRequest::Delete(delete.clone())),
        ([], [put]) => Ok(WriteRequest::Put(put.clone())),
        _ => Err(Error::InvalidArgument(
            "WriteIntent currently requires exactly one put or delete per ShardWriteRequest".into(),
        )),
    }
}

pub(crate) async fn commit_intent<T: LatchGuard>(
    exec_ctx: &ExecCtx,
    group_engine: &GroupEngine,
    latch_guard: &mut DeferSignalLatchGuard<T>,
    req: &CommitIntentRequest,
) -> Result<(Option<EvalResult>, CommitIntentResponse, Vec<ForwardPart>)> {
    let mut wb = WriteBatch::default();
    let mut responses =
        vec![retry_intent_result("intent entry is not executed"); req.shard_keys.len()];
    let mut forwards = Vec::new();
    for (index, shard_key) in req.shard_keys.iter().enumerate() {
        if let Some(forward_part) = intent_key_forward_part(
            exec_ctx,
            group_engine,
            req.start_version,
            RequestKind::Commit(req.commit_version),
            index,
            shard_key,
        )
        .await?
        {
            forwards.push(forward_part);
            continue;
        }
        commit_intent_inner(
            exec_ctx,
            group_engine,
            latch_guard,
            req.start_version,
            req.commit_version,
            shard_key,
            &mut wb,
        )
        .await?;
        responses[index] = IntentResult::ok();
    }
    let eval_result =
        if wb.is_empty() { None } else { Some(EvalResult::with_batch(wb.data().to_owned())) };
    Ok((eval_result, CommitIntentResponse { shard_keys: responses }, forwards))
}

async fn commit_intent_inner<T: LatchGuard>(
    exec_ctx: &ExecCtx,
    group_engine: &GroupEngine,
    latch_guard: &mut DeferSignalLatchGuard<T>,
    start_version: u64,
    commit_version: u64,
    shard_key: &ShardKey,
    wb: &mut WriteBatch,
) -> Result<()> {
    trace!(
        "group {} commit txn {} intent with version {}",
        exec_ctx.group_id, start_version, commit_version
    );

    let Some(intent) =
        read_target_intent(group_engine, start_version, shard_key.shard_id, &shard_key.user_key)
            .await?
    else {
        trace!("txn {} intent not exists exists", start_version);
        return Ok(());
    };

    group_engine.delete(&mut *wb, shard_key.shard_id, &shard_key.user_key, TXN_INTENT_VERSION)?;
    if intent.is_delete {
        trace!(
            "group {} commit txn {} intents, shard id {}, version {}, delete kv {}",
            exec_ctx.group_id,
            start_version,
            shard_key.shard_id,
            commit_version,
            sekas_rock::ascii::escape_bytes(&shard_key.user_key),
        );
        group_engine.tombstone(
            &mut *wb,
            shard_key.shard_id,
            &shard_key.user_key,
            commit_version,
        )?;
    } else if let Some(value) = intent.value {
        trace!(
            "group {} commit txn {} intents, shard id {}, version {}, put kv {} => {}",
            exec_ctx.group_id,
            start_version,
            shard_key.shard_id,
            commit_version,
            sekas_rock::ascii::escape_bytes(&shard_key.user_key),
            sekas_rock::ascii::escape_bytes(&value),
        );
        group_engine.put(
            &mut *wb,
            shard_key.shard_id,
            &shard_key.user_key,
            &value,
            commit_version,
        )?;
    }

    trace!(
        "group {} commit txn {} intent with version {}, try signal all",
        exec_ctx.group_id, start_version, commit_version
    );

    latch_guard.signal_all(TxnState::Committed, Some(commit_version));

    trace!(
        "group {} commit txn {} intent with version {}, after signal all",
        exec_ctx.group_id, start_version, commit_version
    );

    Ok(())
}

pub(crate) async fn clear_intent<T: LatchGuard>(
    exec_ctx: &ExecCtx,
    group_engine: &GroupEngine,
    latch_guard: &mut DeferSignalLatchGuard<T>,
    req: &ClearIntentRequest,
) -> Result<(Option<EvalResult>, ClearIntentResponse, Vec<ForwardPart>)> {
    let mut wb = WriteBatch::default();
    let mut responses =
        vec![retry_intent_result("intent entry is not executed"); req.shard_keys.len()];
    let mut forwards = Vec::new();
    for (index, shard_key) in req.shard_keys.iter().enumerate() {
        if let Some(forward_part) = intent_key_forward_part(
            exec_ctx,
            group_engine,
            req.start_version,
            RequestKind::Clear,
            index,
            shard_key,
        )
        .await?
        {
            forwards.push(forward_part);
            continue;
        }
        clear_intent_inner(
            exec_ctx,
            group_engine,
            latch_guard,
            req.start_version,
            shard_key,
            &mut wb,
        )
        .await?;
        responses[index] = IntentResult::ok();
    }
    let eval_result =
        if wb.is_empty() { None } else { Some(EvalResult::with_batch(wb.data().to_owned())) };
    Ok((eval_result, ClearIntentResponse { shard_keys: responses }, forwards))
}

async fn clear_intent_inner<T: LatchGuard>(
    _exec_ctx: &ExecCtx,
    group_engine: &GroupEngine,
    latch_guard: &mut DeferSignalLatchGuard<T>,
    start_version: u64,
    shard_key: &ShardKey,
    wb: &mut WriteBatch,
) -> Result<()> {
    if read_target_intent(group_engine, start_version, shard_key.shard_id, &shard_key.user_key)
        .await?
        .is_none()
    {
        return Ok(());
    }

    group_engine.delete(&mut *wb, shard_key.shard_id, &shard_key.user_key, TXN_INTENT_VERSION)?;

    latch_guard.signal_all(TxnState::Aborted, None);

    Ok(())
}

#[derive(Clone, Copy)]
enum RequestKind {
    Commit(u64),
    Clear,
}

async fn intent_key_forward_part(
    exec_ctx: &ExecCtx,
    group_engine: &GroupEngine,
    start_version: u64,
    kind: RequestKind,
    index: usize,
    shard_key: &ShardKey,
) -> Result<Option<ForwardPart>> {
    let Some(desc) = exec_ctx.move_shard_desc.as_ref() else {
        return Ok(None);
    };
    let shard_id = desc.shard_desc.as_ref().unwrap().id;
    if shard_id != shard_key.shard_id {
        return Ok(None);
    }
    let payload = group_engine.get_all_versions(shard_key.shard_id, &shard_key.user_key).await?;
    let request = match kind {
        RequestKind::Commit(commit_version) => Request::CommitIntent(CommitIntentRequest {
            start_version,
            commit_version,
            shard_keys: vec![shard_key.clone()],
        }),
        RequestKind::Clear => Request::ClearIntent(ClearIntentRequest {
            start_version,
            shard_keys: vec![shard_key.clone()],
        }),
    };
    Ok(Some(ForwardPart {
        indexes: vec![index],
        request,
        forward_ctx: ForwardCtx {
            shard_id,
            dest_group_id: desc.dest_group_id,
            payloads: vec![payload],
        },
    }))
}

fn retry_write_result(message: &'static str) -> WriteIntentResult {
    WriteIntentResult::err(sekas_api::server::v1::Error::status(
        tonic::Code::NotFound.into(),
        message,
    ))
}

fn retry_intent_result(message: &'static str) -> IntentResult {
    IntentResult::err(sekas_api::server::v1::Error::status(tonic::Code::NotFound.into(), message))
}

pub(super) fn apply_put_op(
    r#type: PutType,
    prev_value: Option<&Value>,
    value: Vec<u8>,
) -> Result<Option<Vec<u8>>> {
    match r#type {
        PutType::AddI64 => {
            let delta = decode_i64(&value)
                .ok_or_else(|| Error::InvalidArgument("input value is not a valid i64".into()))?;

            let former_value = match prev_value.and_then(|v| v.content.as_ref()) {
                Some(content) => decode_i64(content).ok_or_else(|| {
                    Error::InvalidArgument("the exists value is not a valid i64".into())
                })?,
                None => 0,
            };
            trace!("add i64 former value {} delta value {}", former_value, delta);
            Ok(Some(former_value.wrapping_add(delta).to_be_bytes().to_vec()))
        }
        PutType::None => Ok(Some(value)),
        PutType::Nop => Ok(None),
    }
}

pub(super) async fn read_first_non_intent_key<T: LatchGuard>(
    latch_guard: &mut DeferSignalLatchGuard<T>,
    engine: &GroupEngine,
    start_version: u64,
    shard_id: u64,
    key: &[u8],
) -> Result<(bool, Option<Value>)> {
    loop {
        let (txn_intent, prev_value) =
            read_intent_and_next_key(engine, start_version, shard_id, key)?;
        let Some(txn_intent) = txn_intent else { return Ok((false, prev_value)) };
        if txn_intent.start_version == start_version {
            // Support idempotent.
            debug!(
                "the intent of key {key:?} already exists, shard {shard_id}, start version {start_version}"
            );
            return Ok((true, prev_value));
        }

        trace!("another txn {} intent exists", txn_intent.start_version);
        latch_guard.resolve_txn(shard_id, key, txn_intent).await?;
    }
}

fn read_intent_and_next_key(
    engine: &GroupEngine,
    start_version: u64,
    shard_id: u64,
    key: &[u8],
) -> Result<(Option<TxnIntent>, Option<Value>)> {
    let mut snapshot = engine.snapshot(shard_id, SnapshotMode::Key { key })?;
    if let Some(mvcc_iter) = snapshot.next() {
        let mut mvcc_iter = mvcc_iter?;
        if let Some(entry) = mvcc_iter.next() {
            let entry = entry?;
            if entry.version() == TXN_INTENT_VERSION {
                let content = entry.value().ok_or_else(|| {
                    Error::InvalidData(format!(
                        "intent value must exist, shard={}, key={:?}, txn={}",
                        shard_id, key, start_version,
                    ))
                })?;
                let txn_intent = TxnIntent::decode(content)?;
                let prev_value = mvcc_iter.next().transpose()?.map(Into::<Value>::into);
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

// An atomic operation will not conflict with previous values.
pub(super) fn is_atomic_operation(write: &WriteRequest) -> bool {
    matches!(
        write,
        WriteRequest::Put(put)
            if put.conditions.is_empty() && put.put_type == PutType::AddI64 as i32
    )
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicU64;
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    use futures::channel::oneshot;
    use log::info;
    use sekas_client::WriteBuilder;
    use sekas_rock::fn_name;
    use tempdir::TempDir;

    use super::*;
    use crate::engine::{WriteStates, create_group_engine};
    use crate::replica::eval::LatchManager;
    use crate::replica::eval::latch::local::LocalLatchManager;

    #[derive(Default)]
    struct NotifyLatchGuard {
        #[allow(clippy::type_complexity)]
        waiters: Arc<Mutex<Vec<oneshot::Sender<(TxnState, Option<u64>)>>>>,
    }

    impl LatchGuard for NotifyLatchGuard {
        async fn resolve_txn(&mut self, _txn_intent: TxnIntent) -> Result<Option<Value>> {
            let (sender, receiver) = oneshot::channel();
            {
                let mut waiters = self.waiters.lock().unwrap();
                waiters.push(sender);
            }
            let (txn_state, version) = receiver.await.unwrap();
            match txn_state {
                TxnState::Aborted => Ok(None),
                TxnState::Committed => Ok(Some(Value::with_value(vec![], version.unwrap()))),
                _ => unreachable!(),
            }
        }

        fn signal_all(&self, txn_state: TxnState, commit_version: Option<u64>) {
            let mut waiters = self.waiters.lock().unwrap();
            while let Some(sender) = waiters.pop() {
                let _ = sender.send((txn_state, commit_version));
            }
        }
    }

    #[test]
    fn apply_nop() {
        assert!(apply_put_op(PutType::Nop, None, vec![]).unwrap().is_none());
        assert!(
            apply_put_op(PutType::Nop, Some(&Value::tombstone(123)), vec![]).unwrap().is_none()
        );
        assert!(
            apply_put_op(PutType::Nop, Some(&Value::with_value(vec![], 123)), vec![])
                .unwrap()
                .is_none()
        );
    }

    fn commit_values(engine: &GroupEngine, key: &[u8], values: &[Value]) {
        let mut wb = WriteBatch::default();
        for Value { version, content } in values {
            if let Some(value) = content {
                engine.put(&mut wb, 1, key, value, *version).unwrap();
            } else {
                engine.tombstone(&mut wb, 1, key, *version).unwrap();
            }
        }
        engine.commit(wb, WriteStates::default(), false).unwrap();
    }

    fn commit_eval_result(engine: &GroupEngine, eval_result: Option<EvalResult>) {
        if let Some(eval_result) = eval_result
            && let Some(batch) = eval_result.batch
        {
            let wb = WriteBatch::new(&batch.data);
            engine.commit(wb, WriteStates::default(), false).unwrap();
        }
    }

    fn unwrap_single_write_error(resp: WriteIntentResponse) -> Error {
        let err = resp.writes.into_iter().next().unwrap().into_result().unwrap_err();
        err.into()
    }

    #[sekas_macro::test]
    async fn load_recent_keys() {
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
        for (idx, TestCase { expect_intent, expect_prev_value }) in (0_u8..).zip(cases.into_iter())
        {
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
        }
    }

    fn write_intent_request(start_version: u64, key: Vec<u8>) -> WriteIntentRequest {
        write_intent_request_with_value(start_version, key, vec![])
    }

    fn write_intent_request_with_value(
        start_version: u64,
        key: Vec<u8>,
        value: Vec<u8>,
    ) -> WriteIntentRequest {
        WriteIntentRequest {
            start_version,
            writes: vec![ShardWriteRequest {
                shard_id: 1,
                puts: vec![PutRequest {
                    put_type: PutType::None.into(),
                    key,
                    value,
                    take_prev_value: true,
                    ..Default::default()
                }],
                deletes: Vec::new(),
            }],
            check_write_conflict: false,
        }
    }

    #[sekas_macro::test]
    async fn write_and_commit_intent() {
        let dir = TempDir::new(fn_name!()).unwrap();
        let engine = create_group_engine(dir.path(), 1, 1, 1).await;
        let mut latch_guard = DeferSignalLatchGuard::<NotifyLatchGuard>::empty();

        let key = b"123321".to_vec();
        let start_version = 9394;
        let req = write_intent_request(start_version, key.clone());
        let (eval_result, _resp, forwards) =
            write_intent(&ExecCtx::default(), &engine, &mut latch_guard, &req).await.unwrap();
        assert!(forwards.is_empty());
        assert!(eval_result.is_some());
        let wb = WriteBatch::new(&eval_result.unwrap().batch.unwrap().data);
        engine.commit(wb, WriteStates::default(), false).unwrap();

        let req = CommitIntentRequest {
            start_version,
            commit_version: start_version + 1,
            shard_keys: vec![ShardKey { shard_id: 1, user_key: key.clone() }],
        };
        let (eval_result, _resp, forwards) =
            commit_intent(&ExecCtx::default(), &engine, &mut latch_guard, &req).await.unwrap();
        assert!(forwards.is_empty());
        assert!(eval_result.is_some());
        let wb = WriteBatch::new(&eval_result.unwrap().batch.unwrap().data);
        engine.commit(wb, WriteStates::default(), false).unwrap();

        // commit intent is idempotent
        let req = CommitIntentRequest {
            start_version,
            commit_version: start_version + 1,
            shard_keys: vec![ShardKey { shard_id: 1, user_key: key.clone() }],
        };
        let (eval_result, _resp, forwards) =
            commit_intent(&ExecCtx::default(), &engine, &mut latch_guard, &req).await.unwrap();
        assert!(forwards.is_empty());
        assert!(eval_result.is_none());
    }

    #[sekas_macro::test]
    async fn write_and_clear_intent() {
        let dir = TempDir::new(fn_name!()).unwrap();
        let engine = create_group_engine(dir.path(), 1, 1, 1).await;
        let mut latch_guard = DeferSignalLatchGuard::<NotifyLatchGuard>::empty();

        let key = b"123321".to_vec();
        let start_version = 9394;
        let req = write_intent_request(start_version, key.clone());
        let (eval_result, _resp, forwards) =
            write_intent(&ExecCtx::default(), &engine, &mut latch_guard, &req).await.unwrap();
        assert!(forwards.is_empty());
        assert!(eval_result.is_some());
        let wb = WriteBatch::new(&eval_result.unwrap().batch.unwrap().data);
        engine.commit(wb, WriteStates::default(), false).unwrap();

        let req = ClearIntentRequest {
            start_version,
            shard_keys: vec![ShardKey { shard_id: 1, user_key: key.clone() }],
        };
        let (eval_result, _resp, forwards) =
            clear_intent(&ExecCtx::default(), &engine, &mut latch_guard, &req).await.unwrap();
        assert!(forwards.is_empty());
        assert!(eval_result.is_some());
        let wb = WriteBatch::new(&eval_result.unwrap().batch.unwrap().data);
        engine.commit(wb, WriteStates::default(), false).unwrap();

        // clear intent is idempotent
        let req = ClearIntentRequest {
            start_version,
            shard_keys: vec![ShardKey { shard_id: 1, user_key: key.clone() }],
        };
        let (eval_result, _resp, forwards) =
            clear_intent(&ExecCtx::default(), &engine, &mut latch_guard, &req).await.unwrap();
        assert!(forwards.is_empty());
        assert!(eval_result.is_none());
    }

    #[sekas_macro::test]
    async fn write_intent_idempotent() {
        let dir = TempDir::new(fn_name!()).unwrap();
        let engine = create_group_engine(dir.path(), 1, 1, 1).await;
        let mut latch_guard = DeferSignalLatchGuard::<NotifyLatchGuard>::empty();

        let key = b"123321".to_vec();
        let start_version = 9394;
        let req = write_intent_request(start_version, key.clone());
        let (eval_result, _resp, forwards) =
            write_intent(&ExecCtx::default(), &engine, &mut latch_guard, &req).await.unwrap();
        assert!(forwards.is_empty());
        assert!(eval_result.is_some());
        let wb = WriteBatch::new(&eval_result.unwrap().batch.unwrap().data);
        engine.commit(wb, WriteStates::default(), false).unwrap();

        let req = write_intent_request(start_version, key);
        let (eval_result, resp, forwards) =
            write_intent(&ExecCtx::default(), &engine, &mut latch_guard, &req).await.unwrap();
        assert!(forwards.is_empty());
        assert!(eval_result.is_none());

        // Take the prev value.
        let write = resp.writes.into_iter().next().unwrap().into_result().unwrap();
        assert!(write.prev_value.is_none());
    }

    #[sekas_macro::test]
    async fn write_intent_with_condition() {
        let dir = TempDir::new(fn_name!()).unwrap();
        let engine = create_group_engine(dir.path(), 1, 1, 1).await;
        let mut latch_guard = DeferSignalLatchGuard::<NotifyLatchGuard>::empty();

        let key = b"123321".to_vec();
        let start_version = 9394;

        // 1. put exists failed.
        let req = WriteIntentRequest {
            start_version,
            writes: vec![ShardWriteRequest {
                shard_id: 1,
                puts: vec![
                    WriteBuilder::new(key.clone()).expect_exists().ensure_put(b"value".to_vec()),
                ],
                deletes: Vec::new(),
            }],
            check_write_conflict: false,
        };
        let (eval_result, resp, forwards) =
            write_intent(&ExecCtx::default(), &engine, &mut latch_guard, &req).await.unwrap();
        assert!(eval_result.is_none());
        assert!(forwards.is_empty());
        assert!(matches!(unwrap_single_write_error(resp), Error::CasFailed(0, 0, _)));

        // 2. delete exists failed.
        let req = WriteIntentRequest {
            start_version,
            writes: vec![ShardWriteRequest {
                shard_id: 1,
                deletes: vec![WriteBuilder::new(key.clone()).expect_exists().ensure_delete()],
                puts: Vec::new(),
            }],
            check_write_conflict: false,
        };
        let (eval_result, resp, forwards) =
            write_intent(&ExecCtx::default(), &engine, &mut latch_guard, &req).await.unwrap();
        assert!(eval_result.is_none());
        assert!(forwards.is_empty());
        assert!(matches!(unwrap_single_write_error(resp), Error::CasFailed(0, 0, _)));

        commit_values(&engine, &key, &[Value::with_value(b"value".to_vec(), start_version - 100)]);

        // 3. put exists success
        let req = WriteIntentRequest {
            start_version,
            writes: vec![ShardWriteRequest {
                shard_id: 1,
                puts: vec![
                    WriteBuilder::new(key.clone())
                        .expect_exists()
                        .take_prev_value()
                        .ensure_put(b"value".to_vec()),
                ],
                deletes: Vec::new(),
            }],
            check_write_conflict: false,
        };
        let (eval_result, resp, forwards) =
            write_intent(&ExecCtx::default(), &engine, &mut latch_guard, &req).await.unwrap();
        assert!(eval_result.is_some());
        assert!(forwards.is_empty());
        assert!(resp.writes.into_iter().next().unwrap().into_result().is_ok());
    }

    #[sekas_macro::test]
    async fn write_intent_checks_conflict_only_when_requested() {
        let dir = TempDir::new(fn_name!()).unwrap();
        let engine = create_group_engine(dir.path(), 1, 1, 1).await;
        let mut latch_guard = DeferSignalLatchGuard::<NotifyLatchGuard>::empty();

        let key = b"conflict-check-key".to_vec();
        let start_version = 10;
        commit_values(&engine, &key, &[Value::with_value(b"newer".to_vec(), start_version + 1)]);

        let mut req =
            write_intent_request_with_value(start_version, key.clone(), b"overwrite".to_vec());
        req.check_write_conflict = true;
        let (eval_result, resp, forwards) =
            write_intent(&ExecCtx::default(), &engine, &mut latch_guard, &req).await.unwrap();
        assert!(eval_result.is_none());
        assert!(forwards.is_empty());
        assert!(matches!(unwrap_single_write_error(resp), Error::TxnConflict));

        req.check_write_conflict = false;
        let (eval_result, resp, forwards) =
            write_intent(&ExecCtx::default(), &engine, &mut latch_guard, &req).await.unwrap();
        assert!(eval_result.is_some());
        assert!(forwards.is_empty());
        assert!(resp.writes.into_iter().next().unwrap().into_result().is_ok());
    }

    #[test]
    fn apply_put_op_add_i64() {
        struct TestCase {
            prev_value: Option<i64>,
            delta: i64,
            expect: i64,
        }

        let cases = vec![
            // prev value not exists
            TestCase { prev_value: None, delta: 0, expect: 0 },
            TestCase { prev_value: None, delta: 1, expect: 1 },
            TestCase { prev_value: None, delta: i64::MAX, expect: i64::MAX },
            TestCase { prev_value: None, delta: i64::MIN, expect: i64::MIN },
            // normal case
            TestCase { prev_value: Some(0), delta: i64::MAX, expect: i64::MAX },
            TestCase { prev_value: Some(0), delta: i64::MIN, expect: i64::MIN },
            TestCase { prev_value: Some(1), delta: 1, expect: 2 },
            TestCase { prev_value: Some(-1), delta: i64::MAX, expect: i64::MAX - 1 },
            // wrapping
            TestCase { prev_value: Some(1), delta: i64::MAX, expect: i64::MAX.wrapping_add(1) },
            TestCase { prev_value: Some(i64::MAX), delta: 1, expect: i64::MAX.wrapping_add(1) },
            TestCase { prev_value: Some(i64::MIN), delta: -1, expect: i64::MIN.wrapping_sub(1) },
            TestCase { prev_value: Some(-1), delta: i64::MIN, expect: i64::MIN.wrapping_sub(1) },
        ];
        for TestCase { prev_value, delta, expect } in cases {
            let value = prev_value.map(|v| Value::with_value(v.to_be_bytes().to_vec(), 1));
            let r = apply_put_op(PutType::AddI64, value.as_ref(), delta.to_be_bytes().to_vec())
                .unwrap()
                .unwrap();
            assert!(matches!(decode_i64(&r), Some(v) if v == expect), "{r:?}");
        }
    }

    #[test]
    fn apply_put_op_add_invalid() {
        assert!(matches!(
            apply_put_op(PutType::AddI64, None, vec![1u8]),
            Err(Error::InvalidArgument(_))
        ));
        let value = Value::with_value(vec![2u8], 1);
        assert!(matches!(
            apply_put_op(PutType::AddI64, Some(&value), 1i64.to_be_bytes().to_vec()),
            Err(Error::InvalidArgument(_))
        ));
    }

    #[test]
    fn apply_put_op_nop() {
        let r = apply_put_op(PutType::Nop, None, vec![]).unwrap();
        assert!(r.is_none());
        let value = Value::with_value(vec![1u8], 1);
        let r = apply_put_op(PutType::Nop, Some(&value), vec![1u8]).unwrap();
        assert!(r.is_none());
    }

    #[test]
    fn apply_put_op_none() {
        let r = apply_put_op(PutType::None, None, vec![1u8]).unwrap();
        assert!(matches!(r, Some(v) if v == vec![1u8]));

        let value = Value::with_value(vec![2u8], 1);
        let r = apply_put_op(PutType::None, Some(&value), vec![1u8]).unwrap();
        assert!(matches!(r, Some(v) if v == vec![1u8]));
    }

    #[sekas_macro::test]
    async fn write_intent_resolve_orphan_txn_read_latest_write() {
        // A case:
        // 1. txn 1 write intent
        // 2. txn 2 write intent and wait txn 1
        // 3. txn 1 commit intent
        // 4. txn 3 write intent
        // 5. txn 3 commit intent
        // 6. txn 2 wakeup and commit intent

        let dir = TempDir::new(fn_name!()).unwrap();

        let shard_id = 1;
        let key = b"123321".to_vec();
        let start_version = 9394;

        let mut handles = Vec::default();
        let version_allocator = Arc::new(AtomicU64::new(start_version));
        let latch_mgr = LocalLatchManager::default();
        let engine = create_group_engine(dir.path(), 1, 1, 1).await;
        for i in 0..100 {
            let key_clone = key.clone();
            let engine_clone = engine.clone();
            let latch_mgr_clone = latch_mgr.clone();
            let version_allocator_clone = version_allocator.clone();
            let handle = sekas_runtime::spawn(async move {
                let start_version =
                    version_allocator_clone.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                let req = WriteIntentRequest {
                    start_version,
                    writes: vec![ShardWriteRequest {
                        shard_id: 1,
                        puts: vec![WriteBuilder::new(key_clone.clone()).ensure_add(1)],
                        deletes: Vec::new(),
                    }],
                    check_write_conflict: false,
                };
                let mut latch_guard = DeferSignalLatchGuard::with_single(
                    &ShardKey { shard_id, user_key: key_clone.to_vec() },
                    latch_mgr_clone.acquire(shard_id, &key_clone).await.unwrap(),
                );
                let (eval_result, _, forwards) =
                    write_intent(&ExecCtx::default(), &engine_clone, &mut latch_guard, &req)
                        .await
                        .unwrap();
                assert!(forwards.is_empty());
                commit_eval_result(&engine_clone, eval_result);
                drop(latch_guard);

                info!("txn {i} write intent with start version {start_version}");

                sekas_runtime::time::sleep(Duration::from_millis(i % 10)).await;

                let mut latch_guard = DeferSignalLatchGuard::with_single(
                    &ShardKey { shard_id, user_key: key_clone.to_vec() },
                    latch_mgr_clone.acquire(shard_id, &key_clone).await.unwrap(),
                );
                let commit_version =
                    version_allocator_clone.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                let req = CommitIntentRequest {
                    start_version,
                    commit_version,
                    shard_keys: vec![ShardKey { shard_id, user_key: key_clone }],
                };
                let (eval_result, _, forwards) =
                    commit_intent(&ExecCtx::default(), &engine_clone, &mut latch_guard, &req)
                        .await
                        .unwrap();
                assert!(forwards.is_empty());
                commit_eval_result(&engine_clone, eval_result);

                info!(
                    "txn {i} write intent with start version {start_version}, commit version {commit_version}"
                );
            });
            handles.push(handle);
        }

        for handle in handles {
            handle.await.unwrap();
        }

        let value = engine.get(shard_id, &key).await.unwrap().unwrap();
        let value = decode_i64(&value.content.unwrap()).unwrap();
        assert_eq!(value, 100);
    }
}
