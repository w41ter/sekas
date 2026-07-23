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

use std::sync::Arc;
use std::time::Duration;

use log::{debug, warn};
use sekas_runtime::JoinHandle;

use crate::node::NodeConfig;
use crate::node::move_shard::gc::remove_shard;
use crate::replica::Replica;
use crate::{Error, Result};

pub(crate) fn setup(cfg: NodeConfig, replica: Arc<Replica>) -> JoinHandle<()> {
    sekas_runtime::spawn(async move {
        let interval = Duration::from_millis(100);
        loop {
            sekas_runtime::time::sleep(interval).await;
            if let Err(err) = purge_replica(&cfg, &replica).await {
                warn!(
                    "group {} replica {} shard purge: {err}",
                    replica.replica_info().group_id,
                    replica.replica_info().replica_id
                );
            }
        }
    })
}

async fn purge_replica(cfg: &NodeConfig, replica: &Arc<Replica>) -> Result<()> {
    match replica.on_leader("shard purge", true).await? {
        Some(_) => {}
        None => return Ok(()),
    }

    let group_engine = replica.group_engine();
    for shard in group_engine.deleted_shards() {
        debug!(
            "group {} replica {} purge deleted shard {}",
            replica.replica_info().group_id,
            replica.replica_info().replica_id,
            shard.id
        );
        match remove_shard(cfg, replica.as_ref(), group_engine.clone(), shard.id).await {
            Ok(()) | Err(Error::ShardNotFound(_)) => {
                group_engine.clear_deleted_shard(shard.id)?;
            }
            Err(err) => return Err(err),
        }
    }

    Ok(())
}
