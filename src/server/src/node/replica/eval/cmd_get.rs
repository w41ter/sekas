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

use prost::Message;
use sekas_api::server::v1::*;

use crate::engine::{GroupEngine, SnapshotMode};
use crate::node::migrate::ForwardCtx;
use crate::node::replica::ExecCtx;
use crate::{Error, Result};

/// Get the value of the specified key.
pub(crate) async fn get(
    exec_ctx: &ExecCtx,
    engine: &GroupEngine,
    req: &ShardGetRequest,
) -> Result<Option<Vec<u8>>> {
    if let Some(desc) = exec_ctx.migration_desc.as_ref() {
        let shard_id = desc.shard_desc.as_ref().unwrap().id;
        if shard_id == req.shard_id {
            let payloads = read_shard_key_versions(engine, req.shard_id, &req.key).await?;
            let forward_ctx = ForwardCtx { shard_id, dest_group_id: desc.dest_group_id, payloads };
            return Err(Error::Forward(forward_ctx));
        }
    }

    read_key(engine, req.shard_id, &req.key, req.start_version).await
}

async fn read_key(
    engine: &GroupEngine,
    shard_id: u64,
    key: &[u8],
    start_version: u64,
) -> Result<Option<Vec<u8>>> {
    let snapshot_mode = SnapshotMode::Key { key };
    let mut snapshot = engine.snapshot(shard_id, snapshot_mode)?;
    if let Some(iter) = snapshot.mvcc_iter() {
        for entry in iter? {
            let entry = entry?;
            if entry.version() == super::INTENT_KEY_VERSION {
                // maybe we need to wait intent.
                let Some(value) = entry.value() else {
                    return Err(Error::InvalidData(
                        format!("the intent value of key: {key:?} not exists?")
                    ));
                };
                let intent = TxnIntent::decode(value)?;
                if intent.start_version <= start_version {
                    // TODO(walter) We need to wait intent lock to release!
                }
            } else if entry.version() < start_version {
                // This entry is safe for reading.
                return Ok(entry.value().map(ToOwned::to_owned));
            }
        }
    }
    Ok(None)
}

async fn read_shard_key_versions(
    engine: &GroupEngine,
    shard_id: u64,
    key: &[u8],
) -> Result<ValueSet> {
    let snapshot_mode = SnapshotMode::Key { key };
    let mut snapshot = engine.snapshot(shard_id, snapshot_mode)?;
    let mut value_set = ValueSet { user_key: key.to_owned(), values: vec![] };
    if let Some(iter) = snapshot.mvcc_iter() {
        for entry in iter? {
            let entry = entry?;
            match entry.value() {
                Some(value) => {
                    value_set
                        .values
                        .push(Value { content: Some(value.to_owned()), version: value.version() });
                }
                None => {
                    // Send tombstone to target to support MVCC.
                    value_set.values.push(Value { content: None, version: entry.version() });
                }
            }
        }
    }
    Ok(value_set)
}
