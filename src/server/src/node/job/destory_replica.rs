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

use std::sync::Arc;

use log::error;
use sekas_runtime::JoinHandle;

use crate::engine::{Engines, GroupEngine, RawDb, StateEngine};
use crate::node::metrics::*;
use crate::raftgroup::destory_storage;
use crate::serverpb::v1::ReplicaLocalState;
use crate::{record_latency, Error, Result};

/// Clean a group engine and save the replica state to
/// [`ReplicaLocalState::Tombstone`].
pub(crate) fn setup(group_id: u64, replica_id: u64, engines: Engines) -> JoinHandle<()> {
    sekas_runtime::spawn(async move {
        if let Err(err) =
            destory_replica(group_id, replica_id, engines.state(), engines.db(), engines.log())
                .await
        {
            error!("destory group engine: {}, group {}", err, group_id);
        }
    })
}

async fn destory_replica(
    group_id: u64,
    replica_id: u64,
    state_engine: StateEngine,
    raw_db: Arc<RawDb>,
    raft_engine: Arc<raft_engine::Engine>,
) -> Result<()> {
    record_latency!(take_destory_replica_metrics());
    match GroupEngine::destory(group_id, replica_id, raw_db).await {
        Ok(()) => {}
        Err(Error::RocksDb(err)) if err.to_string().contains("Invalid column family") => {}
        e => {
            return e;
        }
    }
    destory_storage(&raft_engine, replica_id).await?;
    state_engine.save_replica_state(group_id, replica_id, ReplicaLocalState::Tombstone).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use sekas_rock::fs::create_dir_all_if_not_exists;
    use tempdir::TempDir;

    use super::*;
    use crate::bootstrap::open_engine_with_default_config;

    #[sekas_macro::test]
    async fn destory_replica_ignore_not_existed_column_families() {
        let tmp_dir = TempDir::new("destory_replica_ignore_not_existed_column_families").unwrap();
        let db_path = tmp_dir.path().join("db");
        let log_path = tmp_dir.path().join("log");
        let raw_db = Arc::new(open_engine_with_default_config(db_path).unwrap());
        let group_id = 1;
        let replica_id = 1;

        use raft_engine::{Config, Engine};
        let engine_dir = log_path.join("engine");
        let snap_dir = log_path.join("snap");
        create_dir_all_if_not_exists(&engine_dir).unwrap();
        create_dir_all_if_not_exists(&snap_dir).unwrap();
        let engine_cfg =
            Config { dir: engine_dir.to_str().unwrap().to_owned(), ..Default::default() };
        let engine = Arc::new(Engine::open(engine_cfg).unwrap());
        let state_engine = StateEngine::new(engine.clone());
        destory_replica(group_id, replica_id, state_engine, raw_db, engine).await.unwrap();
    }
}
