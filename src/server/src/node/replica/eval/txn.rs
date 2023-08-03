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

use libc::group;
use prost::Message;
use sekas_api::server::v1::{
    ClearIntentRequest, CommitIntentRequest, ShardDeleteRequest, ShardPutRequest, WriteIntent,
    WriteIntentRequest,
};
use sekas_api::v1::{DeleteRequest, PutOperation, PutRequest};

use super::cas::eval_conditions;
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
    let mut values = Vec::with_capacity(req.puts.len() + req.deletes.len());
    for put in &req.puts {
        values.push(group_engine.get(req.shard_id, &put.key).await?);
    }
    for del in &req.deletes {
        values.push(group_engine.get(req.shard_id, &del.key).await?);
    }

    let mut wb = WriteBatch::default();
    for put in &req.puts {
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
        let Some(value) = group_engine.get(req.shard_id, key).await? else {
            continue
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
        let Some(value) = group_engine.get(req.shard_id, key).await? else {
            continue
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

#[cfg(test)]
mod tests {
    // TODO: add test
    // 1. commit intent and clear intent is idempotent.
    // 2. only commit or clear intent with the same start_version.
}
