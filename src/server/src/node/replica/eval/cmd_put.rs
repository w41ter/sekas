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

use sekas_api::server::v1::ShardPutRequest;
use sekas_api::v1::{PutOperation, PutRequest};

use super::cas::eval_conditions;
use crate::engine::{GroupEngine, WriteBatch};
use crate::node::migrate::ForwardCtx;
use crate::node::replica::ExecCtx;
use crate::serverpb::v1::{EvalResult, WriteBatchRep};
use crate::{Error, Result};

enum Op {
    Add,
    Sub,
}

pub(crate) async fn put(
    exec_ctx: &ExecCtx,
    group_engine: &GroupEngine,
    req: &ShardPutRequest,
) -> Result<EvalResult> {
    let put = req
        .put
        .as_ref()
        .ok_or_else(|| Error::InvalidArgument("ShardPutRequest::put is None".into()))?;

    if let Some(desc) = exec_ctx.migration_desc.as_ref() {
        let shard_id = desc.shard_desc.as_ref().unwrap().id;
        if shard_id == req.shard_id {
            let forward_ctx =
                ForwardCtx { shard_id, dest_group_id: desc.dest_group_id, payloads: vec![] };
            return Err(Error::Forward(forward_ctx));
        }
    }

    let value_result = if !put.conditions.is_empty() || put.op != PutOperation::None as i32 {
        group_engine.get(req.shard_id, &put.key).await?.map(|(val, _)| val)
    } else {
        None
    };
    if !put.conditions.is_empty() {
        eval_conditions(value_result.as_deref(), &put.conditions)?;
    }

    let value = eval_put_op(put, value_result.as_deref())?;
    let mut wb = WriteBatch::default();
    group_engine.put(&mut wb, req.shard_id, &put.key, &value, super::FLAT_KEY_VERSION)?;
    Ok(EvalResult {
        batch: Some(WriteBatchRep { data: wb.data().to_owned() }),
        ..Default::default()
    })
}

fn eval_put_op(put: &PutRequest, value_result: Option<&[u8]>) -> Result<Vec<u8>> {
    match PutOperation::from_i32(put.op) {
        Some(PutOperation::None) => Ok(put.value.clone()),
        Some(PutOperation::Add) => do_op(Op::Add, value_result, &put.value),
        Some(PutOperation::Sub) => do_op(Op::Sub, value_result, &put.value),
        None => Err(Error::InvalidArgument(format!("Invalid PutOperation {}", put.op))),
    }
}

fn do_op(op: Op, value: Option<&[u8]>, input: &[u8]) -> Result<Vec<u8>> {
    let default = [0u8; 8];
    let input = i64_from_le_bytes(input).ok_or_else(|| {
        Error::InvalidArgument(format!(
            "PutOperation require value be a number (8 bytes), but got {} bytes",
            input.len()
        ))
    })?;
    let value = i64_from_le_bytes(value.unwrap_or(&default)).ok_or_else(|| {
        Error::InvalidArgument("PutOperation require exists value be a number(8 bytes)".to_owned())
    })?;

    let value = match op {
        Op::Add => value.wrapping_add(input),
        Op::Sub => value.wrapping_sub(input),
    };
    Ok(value.to_le_bytes().to_vec())
}

fn i64_from_le_bytes(bytes: &[u8]) -> Option<i64> {
    let mut buf = [0u8; 8];
    if buf.len() == bytes.len() {
        buf.copy_from_slice(bytes);
        Some(i64::from_le_bytes(buf))
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn op_add_not_exists() {
        let value = (-1i64).to_le_bytes().to_vec();
        let result = do_op(Op::Add, None, &value).unwrap();
        assert_eq!(value, result);
    }

    #[test]
    fn op_wrapping_add() {
        let value = (i64::MAX).to_le_bytes().to_vec();
        let input = (1i64).to_le_bytes().to_vec();
        let result = do_op(Op::Add, Some(value).as_deref(), &input).unwrap();
        let expect = (i64::MIN).to_le_bytes().to_vec();
        assert_eq!(result, expect);
    }

    #[test]
    fn op_invalid_input() {
        let value = (i64::MAX).to_le_bytes().to_vec();
        let result = do_op(Op::Add, Some(&[0u8]), &value);
        assert!(matches!(result, Err(Error::InvalidArgument(_))));

        let result = do_op(Op::Add, None, &[0u8, 0u8, 0u8]);
        assert!(matches!(result, Err(Error::InvalidArgument(_))));
    }
}
