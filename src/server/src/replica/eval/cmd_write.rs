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

use log::trace;
use sekas_api::server::v1::{PutType, ShardWriteRequest, ShardWriteResponse, WriteResponse};
use sekas_rock::time::timestamp_nanos;

use super::cas::eval_conditions;
use super::{WriteEvalResult, write_not_executed, write_ok};
use crate::node::move_shard::ForwardCtx;
use crate::replica::ExecCtx;
use crate::replica::write_view::WriteEvalContext;
use crate::{Error, Result};

pub(crate) async fn batch_write(
    exec_ctx: &ExecCtx,
    write_ctx: &mut WriteEvalContext<'_>,
    req: &ShardWriteRequest,
) -> Result<(Option<WriteEvalResult>, ShardWriteResponse)> {
    // TODO(walter) only internal shards would write in batch.
    if req.deletes.is_empty() && req.puts.is_empty() {
        return Ok((None, ShardWriteResponse::default()));
    }

    if let Some(desc) = exec_ctx.move_shard_desc.as_ref() {
        let shard_id = desc.shard_desc.as_ref().unwrap().id;
        if shard_id == req.shard_id {
            let mut payloads = Vec::with_capacity(req.puts.len() + req.deletes.len());
            for del in &req.deletes {
                payloads.push(write_ctx.engine().get_all_versions(req.shard_id, &del.key).await?);
            }
            for put in &req.puts {
                payloads.push(write_ctx.engine().get_all_versions(req.shard_id, &put.key).await?);
            }
            let forward_ctx = ForwardCtx { shard_id, dest_group_id: desc.dest_group_id, payloads };
            return Err(Error::Forward(forward_ctx));
        }
    }

    let mut eval_result = WriteEvalResult::default();
    let mut resp = ShardWriteResponse::default();
    let num_deletes = req.deletes.len();
    for (idx, del) in req.deletes.iter().enumerate() {
        let prev_value = write_ctx.latest_value(req.shard_id, &del.key).await?;
        if let Some(cond_idx) = eval_conditions(prev_value.as_ref(), &del.conditions)? {
            return Ok((
                None,
                cas_failed_response(
                    req,
                    idx,
                    Error::CasFailed(idx as u64, cond_idx as u64, prev_value),
                ),
            ));
        }
        let prev_version = prev_value.as_ref().map(|v| v.version).unwrap_or_default();
        resp.deletes.push(write_ok(WriteResponse {
            prev_value: if del.take_prev_value { prev_value } else { None },
            candidate_version: 0,
        }));
        let version = std::cmp::max(prev_version + 1, next_version());
        eval_result.tombstone(req.shard_id, del.key.clone(), version);
    }
    for (idx, put) in req.puts.iter().enumerate() {
        if put.put_type != PutType::None as i32 {
            panic!("BatchWrite does not support put operation");
        }

        let prev_value = write_ctx.latest_value(req.shard_id, &put.key).await?;
        if let Some(cond_idx) = eval_conditions(prev_value.as_ref(), &put.conditions)? {
            let idx = num_deletes + idx;
            return Ok((
                None,
                cas_failed_response(
                    req,
                    idx,
                    Error::CasFailed(idx as u64, cond_idx as u64, prev_value),
                ),
            ));
        }
        let prev_version = prev_value.as_ref().map(|v| v.version).unwrap_or_default();
        resp.puts.push(write_ok(WriteResponse {
            prev_value: if put.take_prev_value { prev_value } else { None },
            candidate_version: 0,
        }));
        let version = std::cmp::max(prev_version + 1, next_version());
        trace!(
            "batch write, shard id {}, version {}, kv {} => {}",
            req.shard_id,
            version,
            sekas_rock::ascii::escape_bytes(&put.key),
            sekas_rock::ascii::escape_bytes(&put.value),
        );
        eval_result.put(req.shard_id, put.key.clone(), put.value.clone(), version);
    }
    Ok((Some(eval_result), resp))
}

#[inline]
fn next_version() -> u64 {
    timestamp_nanos()
}

fn cas_failed_response(
    req: &ShardWriteRequest,
    failed_index: usize,
    err: Error,
) -> ShardWriteResponse {
    let err: sekas_api::server::v1::Error = err.into();
    let mut resp = ShardWriteResponse::default();
    for idx in 0..req.deletes.len() {
        resp.deletes.push(if idx == failed_index {
            sekas_api::server::v1::WriteResult::err(err.clone())
        } else {
            write_not_executed()
        });
    }
    for idx in 0..req.puts.len() {
        let flattened = req.deletes.len() + idx;
        resp.puts.push(if flattened == failed_index {
            sekas_api::server::v1::WriteResult::err(err.clone())
        } else {
            write_not_executed()
        });
    }
    resp
}

#[cfg(test)]
mod tests {
    use sekas_api::server::v1::Value;
    use sekas_client::WriteBuilder;
    use sekas_rock::fn_name;
    use tempdir::TempDir;

    use super::*;
    use crate::engine::{GroupEngine, WriteBatch, WriteStates, create_group_engine};
    use crate::replica::pending::PendingMutationOverlay;
    use crate::replica::write_view::WriteEvalContext;

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

    fn write_ctx(engine: &GroupEngine) -> WriteEvalContext<'_> {
        WriteEvalContext::new(engine, PendingMutationOverlay::default())
    }

    #[sekas_macro::test]
    async fn batch_write_when_exists() {
        let dir = TempDir::new(fn_name!()).unwrap();
        let engine = create_group_engine(dir.path(), 1, 1, 1).await;

        // 1. put exists failed
        let exec_ctx = ExecCtx::default();
        let req = ShardWriteRequest {
            shard_id: SHARD_ID,
            puts: vec![
                WriteBuilder::new(b"key".to_vec()).expect_exists().ensure_put(b"value".to_vec()),
            ],
            ..Default::default()
        };
        let (eval_result, resp) =
            batch_write(&exec_ctx, &mut write_ctx(&engine), &req).await.unwrap();
        assert!(eval_result.is_none());
        assert!(matches!(
            resp.puts.into_iter().next().unwrap().into_result().unwrap_err().into(),
            Error::CasFailed(0, 0, _)
        ));

        // 2. delete exists failed
        let exec_ctx = ExecCtx::default();
        let req = ShardWriteRequest {
            shard_id: SHARD_ID,
            deletes: vec![WriteBuilder::new(b"key".to_vec()).expect_exists().ensure_delete()],
            ..Default::default()
        };
        let (eval_result, resp) =
            batch_write(&exec_ctx, &mut write_ctx(&engine), &req).await.unwrap();
        assert!(eval_result.is_none());
        assert!(matches!(
            resp.deletes.into_iter().next().unwrap().into_result().unwrap_err().into(),
            Error::CasFailed(0, 0, _)
        ));

        commit_values(&engine, b"key", &[Value::with_value(b"value".to_vec(), 123)]);

        // 3. put exists success
        let req = ShardWriteRequest {
            shard_id: SHARD_ID,
            puts: vec![
                WriteBuilder::new(b"key".to_vec()).expect_exists().ensure_put(b"value".to_vec()),
            ],
            ..Default::default()
        };
        let r = batch_write(&exec_ctx, &mut write_ctx(&engine), &req).await;
        assert!(r.is_ok());

        // 4. delete exists success
        let req = ShardWriteRequest {
            shard_id: SHARD_ID,
            deletes: vec![WriteBuilder::new(b"key".to_vec()).expect_exists().ensure_delete()],
            ..Default::default()
        };
        let r = batch_write(&exec_ctx, &mut write_ctx(&engine), &req).await;
        assert!(r.is_ok());
    }
}
