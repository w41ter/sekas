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

use sekas_api::server::v1::{PutType, ShardWriteRequest, ShardWriteResponse, WriteResponse};

use super::cas::eval_conditions;
use crate::engine::{GroupEngine, WriteBatch};
use crate::node::replica::ExecCtx;
use crate::serverpb::v1::{EvalResult, WriteBatchRep};
use crate::Result;

pub(crate) async fn batch_write(
    exec_ctx: &ExecCtx,
    group_engine: &GroupEngine,
    req: &ShardWriteRequest,
) -> Result<(Option<EvalResult>, ShardWriteResponse)> {
    if req.deletes.is_empty() && req.puts.is_empty() {
        return Ok((None, ShardWriteResponse::default()));
    }

    let mut wb = WriteBatch::default();
    let mut resp = ShardWriteResponse::default();
    for del in &req.deletes {
        if !del.conditions.is_empty() || del.take_prev_value {
            // TODO(walter) support get value in parallel.
            let prev_value = group_engine.get(req.shard_id, &del.key).await?;
            eval_conditions(prev_value.as_ref(), &del.conditions)?;
            resp.deletes.push(WriteResponse { prev_value });
        }
        if exec_ctx.is_migrating_shard(req.shard_id) {
            panic!("BatchWrite does not support migrating shard");
        }
        group_engine.delete(&mut wb, req.shard_id, &del.key, super::FLAT_KEY_VERSION)?;
    }
    for put in &req.puts {
        if !put.conditions.is_empty() || put.take_prev_value {
            // TODO(walter) support get value in parallel.
            let prev_value = group_engine.get(req.shard_id, &put.key).await?;
            eval_conditions(prev_value.as_ref(), &put.conditions)?;
            resp.puts.push(WriteResponse { prev_value });
        }
        if put.put_type != PutType::None as i32 {
            panic!("BatchWrite does not support put operation");
        }
        if exec_ctx.is_migrating_shard(req.shard_id) {
            panic!("BatchWrite does not support migrating shard");
        }
        // FIXME(walter) change flat version, to support move internal shards.
        group_engine.put(&mut wb, req.shard_id, &put.key, &put.value, super::FLAT_KEY_VERSION)?;
    }
    let eval_result = EvalResult {
        batch: Some(WriteBatchRep { data: wb.data().to_owned() }),
        ..Default::default()
    };
    Ok((Some(eval_result), resp))
}
