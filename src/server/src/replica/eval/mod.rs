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
mod cmd_accept_shard;
mod cmd_get;
mod cmd_ingest;
mod cmd_move_replicas;
mod cmd_scan;
mod cmd_txn;
mod cmd_write;
mod latch;

use sekas_api::server::v1::ShardDesc;

pub(crate) use self::cmd_accept_shard::accept_shard;
pub(crate) use self::cmd_get::get;
pub(crate) use self::cmd_ingest::ingest_value_set;
pub(crate) use self::cmd_move_replicas::move_replicas;
pub(crate) use self::cmd_scan::{merge_scan_response, scan};
pub(crate) use self::cmd_txn::{clear_intent, commit_intent, write_intent};
pub(crate) use self::cmd_write::batch_write;
pub(crate) use self::latch::{acquire_row_latches, remote, LatchGuard, LatchManager};
use crate::serverpb::v1::EvalResult;

pub fn add_shard(shard: ShardDesc) -> EvalResult {
    use crate::serverpb::v1::SyncOp;

    EvalResult { op: Some(SyncOp::add_shard(shard)), ..Default::default() }
}
