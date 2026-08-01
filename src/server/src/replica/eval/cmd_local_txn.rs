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

use sekas_api::server::v1::{
    LocalTxnWriteRequest, LocalTxnWriteResponse, PutType, ShardWriteRequest, WriteRequest,
    WriteResponse,
};
use sekas_schema::system::txn::TXN_INTENT_VERSION;

use super::LatchGuard;
use super::cas::eval_conditions;
use super::cmd_txn::{apply_put_op, read_first_non_intent_key};
use super::latch::DeferSignalLatchGuard;
use crate::engine::{GroupEngine, WriteBatch};
use crate::replica::ExecCtx;
use crate::replica::local_txn::{LocalTxnManager, PendingLocalTxnGuard};
use crate::replica::pending::{CommitFence, PendingWrite};
use crate::replica::write_view::PendingWriteView;
use crate::serverpb::v1::EvalResult;
use crate::{Error, Result};

pub(crate) enum LocalTxnEval {
    Write {
        pending: PendingLocalTxnGuard,
        eval_result: Option<EvalResult>,
        pending_writes: Vec<PendingWrite>,
        fence: CommitFence,
        response: LocalTxnWriteResponse,
    },
    WaitPending(CommitFence),
}

#[cfg(test)]
pub(crate) async fn prepare_local_txn_write<T: LatchGuard>(
    exec_ctx: &ExecCtx,
    group_engine: &GroupEngine,
    latch_guard: &mut DeferSignalLatchGuard<T>,
    local_txn_mgr: &LocalTxnManager,
    req: &LocalTxnWriteRequest,
) -> Result<(PendingLocalTxnGuard, Option<EvalResult>, LocalTxnWriteResponse)> {
    if local_txn_hits_moving_shard(exec_ctx, req) {
        return Err(Error::LocalTxnNotAllowed);
    }
    validate_local_shards(group_engine, req)?;

    let pending = local_txn_mgr.begin_commit(req.commit_version).await;
    let commit_version = pending.commit_version();
    let (eval_result, resp) =
        match eval_local_txn_write(group_engine, latch_guard, req, commit_version).await {
            Ok(result) => result,
            Err(err) => {
                pending.abort().await;
                return Err(err);
            }
        };
    Ok((pending, eval_result, resp))
}

pub(crate) async fn prepare_local_txn_write_with_view<T: LatchGuard>(
    exec_ctx: &ExecCtx,
    group_engine: &GroupEngine,
    write_view: &PendingWriteView,
    latch_guard: &mut DeferSignalLatchGuard<T>,
    local_txn_mgr: &LocalTxnManager,
    req: &LocalTxnWriteRequest,
) -> Result<LocalTxnEval> {
    if local_txn_hits_moving_shard(exec_ctx, req) {
        return Err(Error::LocalTxnNotAllowed);
    }
    validate_local_shards(group_engine, req)?;

    let pending = local_txn_mgr.begin_commit(req.commit_version).await;
    let commit_version = pending.commit_version();
    let result =
        eval_local_txn_write_with_view(group_engine, write_view, latch_guard, req, commit_version)
            .await;
    let (eval_result, pending_writes, fence, resp) = match result {
        Ok(result) => result,
        Err(LocalTxnEvalError::WaitPending(fence)) => {
            pending.abort().await;
            return Ok(LocalTxnEval::WaitPending(fence));
        }
        Err(LocalTxnEvalError::Error(err)) => {
            pending.abort().await;
            return Err(err);
        }
    };
    Ok(LocalTxnEval::Write { pending, eval_result, pending_writes, fence, response: resp })
}

fn local_txn_hits_moving_shard(exec_ctx: &ExecCtx, req: &LocalTxnWriteRequest) -> bool {
    let Some(desc) = exec_ctx.move_shard_desc.as_ref() else {
        return false;
    };
    let shard_id = desc.shard_desc.as_ref().unwrap().id;
    req.writes.iter().any(|write| write.shard_id == shard_id)
}

#[cfg(test)]
async fn eval_local_txn_write<T: LatchGuard>(
    group_engine: &GroupEngine,
    latch_guard: &mut DeferSignalLatchGuard<T>,
    req: &LocalTxnWriteRequest,
    commit_version: u64,
) -> Result<(Option<EvalResult>, LocalTxnWriteResponse)> {
    let mut wb = WriteBatch::default();
    let mut responses = Vec::with_capacity(req.writes.len());

    for write in &req.writes {
        let write_index = responses.len();
        let mut write_responses =
            eval_local_txn_write_request(group_engine, latch_guard, commit_version, write, &mut wb)
                .await
                .map_err(|err| match err {
                    Error::CasFailed(_, cond_index, prev_value) => {
                        Error::CasFailed(write_index as u64, cond_index, prev_value)
                    }
                    err => err,
                })?;
        responses.append(&mut write_responses);
    }

    let eval_result =
        if wb.is_empty() { None } else { Some(EvalResult::with_batch(wb.data().to_owned())) };
    Ok((eval_result, LocalTxnWriteResponse { commit_version, writes: responses }))
}

async fn eval_local_txn_write_with_view<T: LatchGuard>(
    group_engine: &GroupEngine,
    write_view: &PendingWriteView,
    latch_guard: &mut DeferSignalLatchGuard<T>,
    req: &LocalTxnWriteRequest,
    commit_version: u64,
) -> std::result::Result<
    (Option<EvalResult>, Vec<PendingWrite>, CommitFence, LocalTxnWriteResponse),
    LocalTxnEvalError,
> {
    let mut wb = WriteBatch::default();
    let mut responses = Vec::with_capacity(req.writes.len());
    let mut pending_writes = Vec::new();
    let mut fence = CommitFence::none();

    for write in &req.writes {
        let write_index = responses.len();
        let mut write_responses = eval_local_txn_write_request_with_view(
            group_engine,
            write_view,
            latch_guard,
            commit_version,
            write,
            &mut wb,
            &mut pending_writes,
            &mut fence,
        )
        .await
        .map_err(|err| match err {
            LocalTxnEvalError::Error(Error::CasFailed(_, cond_index, prev_value)) => {
                LocalTxnEvalError::Error(Error::CasFailed(
                    write_index as u64,
                    cond_index,
                    prev_value,
                ))
            }
            err => err,
        })?;
        responses.append(&mut write_responses);
    }

    let eval_result =
        if wb.is_empty() { None } else { Some(EvalResult::with_batch(wb.data().to_owned())) };
    Ok((
        eval_result,
        pending_writes,
        fence,
        LocalTxnWriteResponse { commit_version, writes: responses },
    ))
}

#[cfg(test)]
async fn eval_local_txn_write_request<T: LatchGuard>(
    group_engine: &GroupEngine,
    latch_guard: &mut DeferSignalLatchGuard<T>,
    commit_version: u64,
    req: &ShardWriteRequest,
    wb: &mut WriteBatch,
) -> Result<Vec<WriteResponse>> {
    let mut responses = Vec::with_capacity(req.deletes.len() + req.puts.len());
    for delete in &req.deletes {
        responses.push(
            eval_local_txn_write_entry(
                group_engine,
                latch_guard,
                req.shard_id,
                commit_version,
                &WriteRequest::Delete(delete.clone()),
                wb,
            )
            .await
            .map_err(|err| map_cas_index(err, responses.len()))?,
        );
    }
    for put in &req.puts {
        responses.push(
            eval_local_txn_write_entry(
                group_engine,
                latch_guard,
                req.shard_id,
                commit_version,
                &WriteRequest::Put(put.clone()),
                wb,
            )
            .await
            .map_err(|err| map_cas_index(err, responses.len()))?,
        );
    }
    Ok(responses)
}

async fn eval_local_txn_write_request_with_view<T: LatchGuard>(
    group_engine: &GroupEngine,
    write_view: &PendingWriteView,
    latch_guard: &mut DeferSignalLatchGuard<T>,
    commit_version: u64,
    req: &ShardWriteRequest,
    wb: &mut WriteBatch,
    pending_writes: &mut Vec<PendingWrite>,
    fence: &mut CommitFence,
) -> std::result::Result<Vec<WriteResponse>, LocalTxnEvalError> {
    let mut responses = Vec::with_capacity(req.deletes.len() + req.puts.len());
    for delete in &req.deletes {
        responses.push(
            eval_local_txn_write_entry_with_view(
                group_engine,
                write_view,
                latch_guard,
                req.shard_id,
                commit_version,
                &WriteRequest::Delete(delete.clone()),
                wb,
                pending_writes,
                fence,
            )
            .await
            .map_err(|err| map_local_eval_cas_index(err, responses.len()))?,
        );
    }
    for put in &req.puts {
        responses.push(
            eval_local_txn_write_entry_with_view(
                group_engine,
                write_view,
                latch_guard,
                req.shard_id,
                commit_version,
                &WriteRequest::Put(put.clone()),
                wb,
                pending_writes,
                fence,
            )
            .await
            .map_err(|err| map_local_eval_cas_index(err, responses.len()))?,
        );
    }
    Ok(responses)
}

#[cfg(test)]
async fn eval_local_txn_write_entry<T: LatchGuard>(
    group_engine: &GroupEngine,
    latch_guard: &mut DeferSignalLatchGuard<T>,
    shard_id: u64,
    commit_version: u64,
    write: &WriteRequest,
    wb: &mut WriteBatch,
) -> Result<WriteResponse> {
    let user_key = local_txn_write_user_key(write);
    let (_, prev_value) =
        read_first_non_intent_key(latch_guard, group_engine, commit_version, shard_id, user_key)
            .await?;

    match write {
        WriteRequest::Delete(del) => {
            if let Some(cond_idx) = eval_conditions(prev_value.as_ref(), &del.conditions)? {
                return Err(Error::CasFailed(0, cond_idx as u64, prev_value));
            }
            group_engine.tombstone(wb, shard_id, &del.key, commit_version)?;
            Ok(WriteResponse { prev_value: if del.take_prev_value { prev_value } else { None } })
        }
        WriteRequest::Put(put) => {
            if let Some(cond_idx) = eval_conditions(prev_value.as_ref(), &put.conditions)? {
                return Err(Error::CasFailed(0, cond_idx as u64, prev_value));
            }
            if let Some(value) =
                apply_put_op(put.put_type(), prev_value.as_ref(), put.value.clone())?
            {
                group_engine.put(wb, shard_id, &put.key, &value, commit_version)?;
            }
            Ok(WriteResponse { prev_value: if put.take_prev_value { prev_value } else { None } })
        }
    }
}

async fn eval_local_txn_write_entry_with_view<T: LatchGuard>(
    group_engine: &GroupEngine,
    write_view: &PendingWriteView,
    latch_guard: &mut DeferSignalLatchGuard<T>,
    shard_id: u64,
    commit_version: u64,
    write: &WriteRequest,
    wb: &mut WriteBatch,
    pending_writes: &mut Vec<PendingWrite>,
    fence: &mut CommitFence,
) -> std::result::Result<WriteResponse, LocalTxnEvalError> {
    let user_key = local_txn_write_user_key(write);
    let (_, committed_value) =
        read_first_non_intent_key(latch_guard, group_engine, commit_version, shard_id, user_key)
            .await
            .map_err(LocalTxnEvalError::Error)?;
    let view = write_view.read_latest(shard_id, user_key, committed_value);
    if view.value().map(|v| v.version == TXN_INTENT_VERSION).unwrap_or_default() {
        let pending_fence = view.fence();
        if !pending_fence.is_empty() {
            return Err(LocalTxnEvalError::WaitPending(pending_fence));
        }
    }
    fence.join(view.fence());
    let prev_value = view.value().cloned();

    match write {
        WriteRequest::Delete(del) => {
            if let Some(cond_idx) = eval_conditions(prev_value.as_ref(), &del.conditions)
                .map_err(LocalTxnEvalError::Error)?
            {
                return Err(LocalTxnEvalError::Error(Error::CasFailed(
                    0,
                    cond_idx as u64,
                    prev_value,
                )));
            }
            group_engine
                .tombstone(wb, shard_id, &del.key, commit_version)
                .map_err(LocalTxnEvalError::Error)?;
            pending_writes.push(PendingWrite::new(
                shard_id,
                del.key.clone(),
                sekas_api::server::v1::Value::tombstone(commit_version),
            ));
            Ok(WriteResponse { prev_value: if del.take_prev_value { prev_value } else { None } })
        }
        WriteRequest::Put(put) => {
            if let Some(cond_idx) = eval_conditions(prev_value.as_ref(), &put.conditions)
                .map_err(LocalTxnEvalError::Error)?
            {
                return Err(LocalTxnEvalError::Error(Error::CasFailed(
                    0,
                    cond_idx as u64,
                    prev_value,
                )));
            }
            if let Some(value) =
                apply_put_op(put.put_type(), prev_value.as_ref(), put.value.clone())
                    .map_err(LocalTxnEvalError::Error)?
            {
                group_engine
                    .put(wb, shard_id, &put.key, &value, commit_version)
                    .map_err(LocalTxnEvalError::Error)?;
                pending_writes.push(PendingWrite::new(
                    shard_id,
                    put.key.clone(),
                    sekas_api::server::v1::Value::with_value(value, commit_version),
                ));
            } else if put.put_type == PutType::Nop as i32 {
                // Nop produces no raft write and therefore no pending overlay
                // entry.
            }
            Ok(WriteResponse { prev_value: if put.take_prev_value { prev_value } else { None } })
        }
    }
}

enum LocalTxnEvalError {
    WaitPending(CommitFence),
    Error(Error),
}

fn validate_local_shards(group_engine: &GroupEngine, req: &LocalTxnWriteRequest) -> Result<()> {
    for write in &req.writes {
        let desc = group_engine.shard_desc(write.shard_id)?;
        for user_key in write
            .deletes
            .iter()
            .map(|delete| &delete.key)
            .chain(write.puts.iter().map(|put| &put.key))
        {
            if !sekas_schema::shard::belong_to(&desc, user_key) {
                return Err(Error::ShardNotFound(write.shard_id));
            }
        }
    }
    Ok(())
}

fn map_cas_index(err: Error, index: usize) -> Error {
    match err {
        Error::CasFailed(_, cond_index, prev_value) => {
            Error::CasFailed(index as u64, cond_index, prev_value)
        }
        err => err,
    }
}

fn map_local_eval_cas_index(err: LocalTxnEvalError, index: usize) -> LocalTxnEvalError {
    match err {
        LocalTxnEvalError::Error(err) => LocalTxnEvalError::Error(map_cas_index(err, index)),
        err => err,
    }
}

pub(super) fn local_txn_write_user_key(write: &WriteRequest) -> &[u8] {
    match write {
        WriteRequest::Put(put) => &put.key,
        WriteRequest::Delete(delete) => &delete.key,
    }
}

#[cfg(test)]
mod tests {
    use sekas_api::server::v1::{PutRequest, PutType, Value};
    use sekas_client::WriteBuilder;
    use sekas_rock::fn_name;
    use tempdir::TempDir;

    use super::*;
    use crate::engine::{WriteStates, create_group_engine};
    use crate::replica::eval::latch::DeferSignalLatchGuard;
    use crate::replica::pending::{CommitFence, PendingWriteOverlay};
    use crate::replica::write_view::PendingWriteView;

    const SHARD_ID: u64 = 1;

    #[derive(Default)]
    struct TestLatchGuard;

    impl LatchGuard for TestLatchGuard {
        async fn resolve_txn(
            &mut self,
            txn_intent: sekas_api::server::v1::TxnIntent,
        ) -> Result<Option<Value>> {
            Ok(txn_intent.value.map(|value| Value::with_value(value, txn_intent.start_version)))
        }

        fn signal_all(
            &self,
            _txn_state: sekas_api::server::v1::TxnState,
            _commit_version: Option<u64>,
        ) {
        }
    }

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

    fn commit_eval_result(engine: &GroupEngine, eval_result: Option<EvalResult>) {
        if let Some(eval_result) = eval_result
            && let Some(batch) = eval_result.batch
        {
            let wb = WriteBatch::new(&batch.data);
            engine.commit(wb, WriteStates::default(), false).unwrap();
        }
    }

    fn put_write(key: &[u8], value: &[u8]) -> ShardWriteRequest {
        ShardWriteRequest {
            shard_id: SHARD_ID,
            puts: vec![PutRequest {
                put_type: PutType::None.into(),
                key: key.to_vec(),
                value: value.to_vec(),
                ..Default::default()
            }],
            deletes: Vec::new(),
        }
    }

    fn new_req(commit_version: u64, writes: Vec<ShardWriteRequest>) -> LocalTxnWriteRequest {
        LocalTxnWriteRequest { commit_version, writes }
    }

    async fn new_engine(test_name: &str) -> GroupEngine {
        let dir = TempDir::new(test_name).unwrap();
        let engine = create_group_engine(dir.path(), 1, 1, 1).await;
        std::mem::forget(dir);
        engine
    }

    #[sekas_macro::test]
    async fn local_txn_writes_multiple_keys_with_one_commit_version() {
        let engine = new_engine(fn_name!()).await;
        let manager = LocalTxnManager::default();
        let mut latch_guard = DeferSignalLatchGuard::<TestLatchGuard>::empty();
        let req = new_req(20, vec![put_write(b"a", b"va"), put_write(b"b", b"vb")]);

        let (pending, eval_result, resp) =
            prepare_local_txn_write(&ExecCtx::default(), &engine, &mut latch_guard, &manager, &req)
                .await
                .unwrap();
        assert_eq!(pending.commit_version(), 20);
        assert_eq!(resp.commit_version, 20);
        assert_eq!(resp.writes.len(), 2);
        commit_eval_result(&engine, eval_result);
        pending.finish().await;

        assert_eq!(engine.get(SHARD_ID, b"a").await.unwrap().unwrap().version, 20);
        assert_eq!(engine.get(SHARD_ID, b"b").await.unwrap().unwrap().version, 20);
    }

    #[sekas_macro::test]
    async fn local_txn_raises_commit_version_from_served_read() {
        let engine = new_engine(fn_name!()).await;
        let manager = LocalTxnManager::default();
        manager.before_read(100).await;
        let mut latch_guard = DeferSignalLatchGuard::<TestLatchGuard>::empty();
        let req = new_req(20, vec![put_write(b"a", b"va")]);

        let (pending, _eval_result, resp) =
            prepare_local_txn_write(&ExecCtx::default(), &engine, &mut latch_guard, &manager, &req)
                .await
                .unwrap();
        assert_eq!(pending.commit_version(), 101);
        assert_eq!(resp.commit_version, 101);
        pending.abort().await;
    }

    #[sekas_macro::test]
    async fn local_txn_overwrites_newer_committed_value() {
        let engine = new_engine(fn_name!()).await;
        commit_values(&engine, b"a", &[Value::with_value(b"old".to_vec(), 30)]);
        let manager = LocalTxnManager::default();
        let mut latch_guard = DeferSignalLatchGuard::<TestLatchGuard>::empty();
        let req = new_req(40, vec![put_write(b"a", b"new")]);

        let (pending, eval_result, resp) =
            prepare_local_txn_write(&ExecCtx::default(), &engine, &mut latch_guard, &manager, &req)
                .await
                .unwrap();
        assert_eq!(resp.commit_version, 40);
        commit_eval_result(&engine, eval_result);
        pending.finish().await;
        let value = engine.get(SHARD_ID, b"a").await.unwrap().unwrap();
        assert_eq!(value.version, 40);
        assert_eq!(value.content.as_deref(), Some(&b"new"[..]));
    }

    #[sekas_macro::test]
    async fn local_txn_maps_cas_failure_to_write_index() {
        let engine = new_engine(fn_name!()).await;
        let manager = LocalTxnManager::default();
        let mut latch_guard = DeferSignalLatchGuard::<TestLatchGuard>::empty();
        let failed_put = ShardWriteRequest {
            shard_id: SHARD_ID,
            puts: vec![WriteBuilder::new(b"b".to_vec()).expect_exists().ensure_put(b"vb".to_vec())],
            deletes: Vec::new(),
        };
        let req = new_req(40, vec![put_write(b"a", b"va"), failed_put]);

        let result =
            prepare_local_txn_write(&ExecCtx::default(), &engine, &mut latch_guard, &manager, &req)
                .await;
        assert!(matches!(result, Err(Error::CasFailed(1, 0, _))));
        assert_eq!(manager.pending_count().await, 0);
    }

    #[sekas_macro::test]
    async fn local_txn_allows_atomic_add_i64_after_start_version() {
        let engine = new_engine(fn_name!()).await;
        commit_values(&engine, b"a", &[Value::with_value(1_i64.to_be_bytes().to_vec(), 30)]);
        let manager = LocalTxnManager::default();
        let mut latch_guard = DeferSignalLatchGuard::<TestLatchGuard>::empty();
        let req = new_req(
            40,
            vec![ShardWriteRequest {
                shard_id: SHARD_ID,
                puts: vec![PutRequest {
                    put_type: PutType::AddI64.into(),
                    key: b"a".to_vec(),
                    value: 2_i64.to_be_bytes().to_vec(),
                    ..Default::default()
                }],
                deletes: Vec::new(),
            }],
        );

        let (pending, eval_result, resp) =
            prepare_local_txn_write(&ExecCtx::default(), &engine, &mut latch_guard, &manager, &req)
                .await
                .unwrap();
        assert_eq!(resp.commit_version, 40);
        commit_eval_result(&engine, eval_result);
        pending.finish().await;
        assert_eq!(
            engine.get(SHARD_ID, b"a").await.unwrap().unwrap().content.unwrap(),
            3_i64.to_be_bytes().to_vec()
        );
    }

    #[sekas_macro::test]
    async fn local_txn_uses_pending_overlay_as_latest_value() {
        let engine = new_engine(fn_name!()).await;
        commit_values(&engine, b"a", &[Value::with_value(1_i64.to_be_bytes().to_vec(), 30)]);

        let overlay = PendingWriteOverlay::default();
        overlay.insert_batch(
            &[PendingWrite::new(
                SHARD_ID,
                b"a".to_vec(),
                Value::with_value(5_i64.to_be_bytes().to_vec(), 40),
            )],
            CommitFence::none(),
        );
        let write_view = PendingWriteView::new(engine.clone(), overlay);
        let manager = LocalTxnManager::default();
        let mut latch_guard = DeferSignalLatchGuard::<TestLatchGuard>::empty();
        let req = new_req(
            50,
            vec![ShardWriteRequest {
                shard_id: SHARD_ID,
                puts: vec![PutRequest {
                    put_type: PutType::AddI64.into(),
                    key: b"a".to_vec(),
                    value: 2_i64.to_be_bytes().to_vec(),
                    take_prev_value: true,
                    ..Default::default()
                }],
                deletes: Vec::new(),
            }],
        );

        let eval = prepare_local_txn_write_with_view(
            &ExecCtx::default(),
            &engine,
            &write_view,
            &mut latch_guard,
            &manager,
            &req,
        )
        .await
        .unwrap();
        let LocalTxnEval::Write { pending, eval_result, pending_writes, response, .. } = eval
        else {
            panic!("unexpected pending wait");
        };
        assert_eq!(response.writes[0].prev_value.as_ref().unwrap().version, 40);
        assert_eq!(
            response.writes[0].prev_value.as_ref().unwrap().content.as_deref(),
            Some(&5_i64.to_be_bytes()[..])
        );
        assert_eq!(pending_writes.len(), 1);
        assert_eq!(pending_writes[0].value.version, 50);
        assert_eq!(pending_writes[0].value.content.as_deref(), Some(&7_i64.to_be_bytes()[..]));
        commit_eval_result(&engine, eval_result);
        pending.finish().await;
    }
}
