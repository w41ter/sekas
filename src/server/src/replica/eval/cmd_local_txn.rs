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

use sekas_api::server::v1::{LocalTxnWriteRequest, LocalTxnWriteResponse, PutType, WriteResponse};

use super::cas::eval_conditions;
use super::cmd_txn::{apply_put_op, read_first_non_intent_key};
use super::latch::DeferSignalLatchGuard;
use super::{LatchGuard, WriteEvalResult, write_not_executed, write_ok};
use crate::engine::GroupEngine;
use crate::replica::ExecCtx;
use crate::replica::write_view::WriteEvalContext;
use crate::{Error, Result};

pub(crate) async fn local_txn_write<T: LatchGuard>(
    exec_ctx: &ExecCtx,
    write_ctx: &mut WriteEvalContext<'_>,
    latch_guard: &mut DeferSignalLatchGuard<T>,
    req: &LocalTxnWriteRequest,
    commit_version: u64,
) -> Result<(Option<WriteEvalResult>, LocalTxnWriteResponse)> {
    if is_local_txn_hits_moving_shard(exec_ctx, req) {
        return Err(Error::LocalTxnNotAllowed);
    }
    validate_local_shards(write_ctx.engine(), req)?;

    let mut eval_result = WriteEvalResult::default();
    let mut txn_resp = LocalTxnWriteResponse { commit_version, ..Default::default() };
    let mut write_index_base = 0;
    for shard_req in &req.writes {
        let shard_id = shard_req.shard_id;
        let num_deletes = shard_req.deletes.len();
        for (idx, del) in shard_req.deletes.iter().enumerate() {
            let (_, prev_value) = read_first_non_intent_key(
                latch_guard,
                write_ctx,
                commit_version,
                shard_id,
                &del.key,
            )
            .await?;
            if let Some(cond_idx) = eval_conditions(prev_value.as_ref(), &del.conditions)? {
                let write_index = write_index_base + idx;
                return Ok((
                    None,
                    cas_failed_response(
                        req,
                        commit_version,
                        write_index,
                        Error::CasFailed(write_index as u64, cond_idx as u64, prev_value),
                    ),
                ));
            }
            eval_result.tombstone(shard_id, del.key.clone(), commit_version);
            txn_resp.writes.push(write_ok(WriteResponse {
                prev_value: if del.take_prev_value { prev_value } else { None },
                candidate_version: 0,
            }))
        }
        for (idx, put) in shard_req.puts.iter().enumerate() {
            let (_, prev_value) = read_first_non_intent_key(
                latch_guard,
                write_ctx,
                commit_version,
                shard_id,
                &put.key,
            )
            .await?;
            if let Some(cond_idx) = eval_conditions(prev_value.as_ref(), &put.conditions)? {
                let write_index = write_index_base + num_deletes + idx;
                return Ok((
                    None,
                    cas_failed_response(
                        req,
                        commit_version,
                        write_index,
                        Error::CasFailed(write_index as u64, cond_idx as u64, prev_value),
                    ),
                ));
            }
            if let Some(value) =
                apply_put_op(put.put_type(), prev_value.as_ref(), put.value.clone())?
            {
                eval_result.put(shard_id, put.key.clone(), value, commit_version);
            } else if put.put_type == PutType::Nop as i32 {
                // Nop produces no raft write and therefore no pending overlay
                // entry.
            }
            txn_resp.writes.push(write_ok(WriteResponse {
                prev_value: if put.take_prev_value { prev_value } else { None },
                candidate_version: 0,
            }));
        }
        write_index_base += num_deletes + shard_req.puts.len();
    }
    Ok((Some(eval_result), txn_resp))
}

fn is_local_txn_hits_moving_shard(exec_ctx: &ExecCtx, req: &LocalTxnWriteRequest) -> bool {
    let Some(desc) = exec_ctx.move_shard_desc.as_ref() else {
        return false;
    };
    let shard_id = desc.shard_desc.as_ref().unwrap().id;
    req.writes.iter().any(|write| write.shard_id == shard_id)
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

fn cas_failed_response(
    req: &LocalTxnWriteRequest,
    commit_version: u64,
    failed_index: usize,
    err: Error,
) -> LocalTxnWriteResponse {
    let err: sekas_api::server::v1::Error = err.into();
    let num_writes = req.writes.iter().map(|write| write.deletes.len() + write.puts.len()).sum();
    let mut resp = LocalTxnWriteResponse { commit_version, ..Default::default() };
    for idx in 0..num_writes {
        resp.writes.push(if idx == failed_index {
            sekas_api::server::v1::WriteResult::err(err.clone())
        } else {
            write_not_executed()
        });
    }
    resp
}

#[cfg(test)]
mod tests {
    use sekas_api::server::v1::{PutRequest, PutType, ShardWriteRequest, Value};
    use sekas_client::WriteBuilder;
    use sekas_rock::fn_name;
    use tempdir::TempDir;

    use super::*;
    use crate::engine::{WriteBatch, WriteStates, create_group_engine};
    use crate::replica::eval::latch::DeferSignalLatchGuard;
    use crate::replica::pending::{PendingMutationKind, PendingMutationOverlay};
    use crate::replica::write_view::WriteEvalContext;

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
            _intent_version: u64,
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

    fn commit_eval_result(engine: &GroupEngine, eval_result: Option<WriteEvalResult>) {
        if let Some(eval_result) = eval_result {
            let wb = eval_result.serialize(engine).unwrap();
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

    fn write_ctx(engine: &GroupEngine) -> WriteEvalContext<'_> {
        WriteEvalContext::new(engine, PendingMutationOverlay::default())
    }

    #[sekas_macro::test]
    async fn local_txn_writes_multiple_keys_with_one_commit_version() {
        let engine = new_engine(fn_name!()).await;
        let mut latch_guard = DeferSignalLatchGuard::<TestLatchGuard>::empty();
        let req = new_req(20, vec![put_write(b"a", b"va"), put_write(b"b", b"vb")]);

        let (eval_result, resp) = local_txn_write(
            &ExecCtx::default(),
            &mut write_ctx(&engine),
            &mut latch_guard,
            &req,
            20,
        )
        .await
        .unwrap();
        assert_eq!(resp.writes.len(), 2);
        commit_eval_result(&engine, eval_result);

        assert_eq!(engine.get(SHARD_ID, b"a").await.unwrap().unwrap().version, 20);
        assert_eq!(engine.get(SHARD_ID, b"b").await.unwrap().unwrap().version, 20);
    }

    #[sekas_macro::test]
    async fn local_txn_overwrites_newer_committed_value() {
        let engine = new_engine(fn_name!()).await;
        commit_values(&engine, b"a", &[Value::with_value(b"old".to_vec(), 30)]);
        let mut latch_guard = DeferSignalLatchGuard::<TestLatchGuard>::empty();
        let req = new_req(40, vec![put_write(b"a", b"new")]);

        let (eval_result, resp) = local_txn_write(
            &ExecCtx::default(),
            &mut write_ctx(&engine),
            &mut latch_guard,
            &req,
            40,
        )
        .await
        .unwrap();
        assert_eq!(resp.writes.len(), 1);
        commit_eval_result(&engine, eval_result);
        let value = engine.get(SHARD_ID, b"a").await.unwrap().unwrap();
        assert_eq!(value.version, 40);
        assert_eq!(value.content.as_deref(), Some(&b"new"[..]));
    }

    #[sekas_macro::test]
    async fn local_txn_maps_cas_failure_to_flattened_write_index() {
        let engine = new_engine(fn_name!()).await;
        let mut latch_guard = DeferSignalLatchGuard::<TestLatchGuard>::empty();
        let failed_put = ShardWriteRequest {
            shard_id: SHARD_ID,
            puts: vec![WriteBuilder::new(b"b".to_vec()).expect_exists().ensure_put(b"vb".to_vec())],
            deletes: Vec::new(),
        };
        let req = new_req(40, vec![put_write(b"a", b"va"), failed_put]);

        let (eval_result, resp) = local_txn_write(
            &ExecCtx::default(),
            &mut write_ctx(&engine),
            &mut latch_guard,
            &req,
            40,
        )
        .await
        .unwrap();
        assert!(eval_result.is_none());
        assert!(matches!(
            resp.writes[1].clone().into_result().unwrap_err().into(),
            Error::CasFailed(1, 0, _)
        ));
    }

    #[sekas_macro::test]
    async fn local_txn_allows_atomic_add_i64_after_start_version() {
        let engine = new_engine(fn_name!()).await;
        commit_values(&engine, b"a", &[Value::with_value(1_i64.to_be_bytes().to_vec(), 30)]);
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

        let (eval_result, resp) = local_txn_write(
            &ExecCtx::default(),
            &mut write_ctx(&engine),
            &mut latch_guard,
            &req,
            40,
        )
        .await
        .unwrap();
        assert_eq!(resp.writes.len(), 1);
        commit_eval_result(&engine, eval_result);
        assert_eq!(
            engine.get(SHARD_ID, b"a").await.unwrap().unwrap().content.unwrap(),
            3_i64.to_be_bytes().to_vec()
        );
    }

    #[sekas_macro::test]
    async fn local_txn_records_pending_mutations_without_serializing() {
        let engine = new_engine(fn_name!()).await;
        let mut latch_guard = DeferSignalLatchGuard::<TestLatchGuard>::empty();
        let req = new_req(
            50,
            vec![ShardWriteRequest {
                shard_id: SHARD_ID,
                puts: vec![PutRequest {
                    key: b"a".to_vec(),
                    value: b"va".to_vec(),
                    ..Default::default()
                }],
                deletes: Vec::new(),
            }],
        );

        let (eval_result, _resp) = local_txn_write(
            &ExecCtx::default(),
            &mut write_ctx(&engine),
            &mut latch_guard,
            &req,
            50,
        )
        .await
        .unwrap();
        let eval_result = eval_result.unwrap();
        let pending_mutations = eval_result.pending_mutations();
        assert_eq!(pending_mutations.len(), 1);
        assert_eq!(pending_mutations[0].version, 50);
        assert_eq!(pending_mutations[0].kind, PendingMutationKind::Put(b"va".to_vec()));
        commit_eval_result(&engine, Some(eval_result));
        assert_eq!(engine.get(SHARD_ID, b"a").await.unwrap().unwrap().version, 50);
    }
}
