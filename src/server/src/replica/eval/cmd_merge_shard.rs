// Copyright 2024-present The Sekas Authors.
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

use log::debug;
use sekas_api::server::v1::*;

use crate::replica::{EvalResult, GroupEngine, MergeShard, SyncOp};
use crate::{Error, Result};

/// Eval merge shard request.
pub(crate) fn merge_shard(engine: &GroupEngine, req: &MergeShardRequest) -> Result<EvalResult> {
    let left_shard_id = req.left_shard_id;
    let right_shard_id = req.right_shard_id;

    debug!("execute merge shard {right_shard_id} into {left_shard_id}",);

    let left_shard = engine.shard_desc(left_shard_id)?;
    let right_shard = engine.shard_desc(right_shard_id)?;
    let Some(RangePartition { start: _, end: left_end }) = &left_shard.range else {
        return Err(Error::InvalidData(format!(
            "apply merge shard but left shard {left_shard_id} range is missing",
        )));
    };
    let Some(RangePartition { start: right_start, end: _ }) = &right_shard.range else {
        return Err(Error::InvalidData(format!(
            "apply merge shard but right shard {right_shard_id} range is missing",
        )));
    };
    if left_end != right_start {
        return Err(Error::InvalidData(format!(
            "the left shard {left_shard_id} is not mergeable with right shard {right_shard_id}",
        )));
    }

    let merge_shard = MergeShard { left_shard_id, right_shard_id };
    let sync_op = Box::new(SyncOp { merge_shard: Some(merge_shard), ..Default::default() });
    Ok(EvalResult { batch: None, op: Some(sync_op) })
}
