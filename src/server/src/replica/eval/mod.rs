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

mod cas;
mod cmd_get;
mod cmd_ingest;
mod cmd_local_txn;
mod cmd_move_replicas;
mod cmd_scan;
mod cmd_shard;
mod cmd_txn;
mod cmd_write;
mod latch;

use sekas_api::server::v1::{ShardWriteRequest, WriteResponse, WriteResult};

pub(crate) use self::cmd_get::get;
pub(crate) use self::cmd_ingest::ingest_value_set;
pub(crate) use self::cmd_local_txn::local_txn_write;
pub(crate) use self::cmd_move_replicas::move_replicas;
pub(crate) use self::cmd_scan::{merge_scan_response, scan};
pub(crate) use self::cmd_shard::{
    accept_shard, add_shard, delete_shard, get_split_key, merge_shard, split_shard,
};
pub(crate) use self::cmd_txn::{clear_intent, commit_intent, query_intent, write_intent};
pub(crate) use self::cmd_write::batch_write;
pub(crate) use self::latch::{
    DeferSignalLatchGuard, LatchGuard, LatchManager, acquire_row_latches, remote,
};
use crate::engine::{GroupEngine, WriteBatch};
use crate::replica::pending::PendingMutation;
use crate::serverpb::v1::{EvalResult, SyncOp, WriteBatchRep};
use crate::{Error, Result};

fn write_ok(response: WriteResponse) -> WriteResult {
    WriteResult::ok(response)
}

fn write_not_executed() -> WriteResult {
    WriteResult::err(sekas_api::server::v1::Error::status(
        vaelo::StatusCode::NotFound.as_i32(),
        "write entry is not executed",
    ))
}

fn validate_value_sizes<'a>(writes: impl IntoIterator<Item = &'a ShardWriteRequest>) -> Result<()> {
    for put in writes.into_iter().flat_map(|write| &write.puts) {
        if put.value.len() > sekas_api::MAX_VALUE_SIZE {
            return Err(Error::InvalidArgument(format!(
                "value size {} exceeds limit {}",
                put.value.len(),
                sekas_api::MAX_VALUE_SIZE
            )));
        }
    }
    Ok(())
}

#[cfg(test)]
mod limit_tests {
    use sekas_api::server::v1::PutRequest;

    use super::*;

    #[test]
    fn server_rejects_oversized_values() {
        let valid = ShardWriteRequest {
            puts: vec![PutRequest {
                value: vec![0; sekas_api::MAX_VALUE_SIZE],
                ..Default::default()
            }],
            ..Default::default()
        };
        assert!(validate_value_sizes(std::iter::once(&valid)).is_ok());

        let oversized = ShardWriteRequest {
            puts: vec![PutRequest {
                value: vec![0; sekas_api::MAX_VALUE_SIZE + 1],
                ..Default::default()
            }],
            ..Default::default()
        };
        assert!(matches!(
            validate_value_sizes(std::iter::once(&oversized)),
            Err(Error::InvalidArgument(_))
        ));
    }
}

#[derive(Debug, Default)]
pub(crate) struct WriteEvalResult {
    writes: Vec<WriteEvalOp>,
    op: Option<Box<SyncOp>>,
}

#[derive(Debug)]
enum WriteEvalOp {
    Put { shard_id: u64, user_key: Vec<u8>, value: Vec<u8>, version: u64 },
    Tombstone { shard_id: u64, user_key: Vec<u8>, version: u64 },
    Delete { shard_id: u64, user_key: Vec<u8>, version: u64 },
}

impl WriteEvalResult {
    pub fn is_empty(&self) -> bool {
        self.writes.is_empty() && self.op.is_none()
    }

    pub fn put(&mut self, shard_id: u64, user_key: Vec<u8>, value: Vec<u8>, version: u64) {
        self.writes.push(WriteEvalOp::Put { shard_id, user_key, value, version });
    }

    pub fn tombstone(&mut self, shard_id: u64, user_key: Vec<u8>, version: u64) {
        self.writes.push(WriteEvalOp::Tombstone { shard_id, user_key, version });
    }

    pub fn delete(&mut self, shard_id: u64, user_key: Vec<u8>, version: u64) {
        self.writes.push(WriteEvalOp::Delete { shard_id, user_key, version });
    }

    pub fn pending_mutations(&self) -> Vec<PendingMutation> {
        self.writes
            .iter()
            .map(|op| match op {
                WriteEvalOp::Put { shard_id, user_key, value, version } => {
                    PendingMutation::put(*shard_id, user_key.clone(), value.clone(), *version)
                }
                WriteEvalOp::Tombstone { shard_id, user_key, version } => {
                    PendingMutation::tombstone(*shard_id, user_key.clone(), *version)
                }
                WriteEvalOp::Delete { shard_id, user_key, version } => {
                    PendingMutation::delete(*shard_id, user_key.clone(), *version)
                }
            })
            .collect()
    }

    pub fn with_op(op: Box<SyncOp>) -> Self {
        WriteEvalResult { op: Some(op), ..Default::default() }
    }

    pub fn serialize(&self, group_engine: &GroupEngine) -> Result<WriteBatch> {
        let mut batch = WriteBatch::default();
        for op in &self.writes {
            match op {
                WriteEvalOp::Put { shard_id, user_key, value, version } => {
                    group_engine.put(&mut batch, *shard_id, user_key, value, *version)?;
                }
                WriteEvalOp::Tombstone { shard_id, user_key, version } => {
                    group_engine.tombstone(&mut batch, *shard_id, user_key, *version)?;
                }
                WriteEvalOp::Delete { shard_id, user_key, version } => {
                    group_engine.delete(&mut batch, *shard_id, user_key, *version)?;
                }
            }
        }
        Ok(batch)
    }

    pub fn into_eval_result(self, group_engine: &GroupEngine) -> Result<EvalResult> {
        let batch = if self.writes.is_empty() {
            None
        } else {
            Some(WriteBatchRep { data: self.serialize(group_engine)?.data().to_owned() })
        };
        Ok(EvalResult { batch, op: self.op })
    }

    pub fn into_eval_result_without_writes(self) -> EvalResult {
        debug_assert!(self.writes.is_empty());
        EvalResult { batch: None, op: self.op }
    }
}
