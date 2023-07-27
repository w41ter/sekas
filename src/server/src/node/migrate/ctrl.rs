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

use std::sync::Arc;

use futures::channel::mpsc;
use futures::StreamExt;
use log::{debug, error, info, warn};
use sekas_api::server::v1::group_request_union::Request;
use sekas_api::server::v1::group_response_union::Response;
use sekas_api::server::v1::*;
use sekas_client::{MigrateClient, Router};

use crate::node::metrics::*;
use crate::node::Replica;
use crate::runtime::sync::WaitGroup;
use crate::serverpb::v1::*;
use crate::transport::TransportManager;
use crate::{record_latency, NodeConfig, Result};

#[derive(Debug)]
pub struct ForwardCtx {
    pub shard_id: u64,
    pub dest_group_id: u64,
    pub payloads: Vec<ShardData>,
}

struct MigrationCoordinator {
    cfg: NodeConfig,

    replica_id: u64,
    group_id: u64,

    replica: Arc<Replica>,

    client: MigrateClient,
    desc: MigrationDesc,
}

#[derive(Clone)]
pub struct MigrateController {
    shared: Arc<MigrateControllerShared>,
}

struct MigrateControllerShared {
    cfg: NodeConfig,
    transport_manager: TransportManager,
}

impl MigrateController {
    pub(crate) fn new(cfg: NodeConfig, transport_manager: TransportManager) -> Self {
        MigrateController { shared: Arc::new(MigrateControllerShared { cfg, transport_manager }) }
    }

    pub fn router(&self) -> Router {
        self.shared.transport_manager.router().clone()
    }

    /// Watch migration state and do the corresponding step.
    pub async fn watch_state_changes(
        &self,
        replica: Arc<Replica>,
        mut receiver: mpsc::UnboundedReceiver<MigrationState>,
        wait_group: WaitGroup,
    ) {
        use crate::runtime::{current, TaskPriority};

        let info = replica.replica_info();
        let replica_id = info.replica_id;
        let group_id = info.group_id;

        let ctrl = self.clone();
        current().spawn(Some(group_id), TaskPriority::High, async move {
            let mut coord: Option<MigrationCoordinator> = None;
            while let Some(state) = receiver.next().await {
                debug!(
                    "on migration step: {:?}. group={group_id}, replica={replica_id}",
                    MigrationStep::from_i32(state.step)
                );
                let desc = state.get_migration_desc();
                if coord.is_none() || coord.as_ref().unwrap().desc != *desc {
                    let target_group_id = if desc.src_group_id == group_id {
                        desc.dest_group_id
                    } else {
                        desc.src_group_id
                    };
                    let client =
                        ctrl.shared.transport_manager.build_migrate_client(target_group_id);
                    coord = Some(MigrationCoordinator {
                        cfg: ctrl.shared.cfg.clone(),
                        replica_id,
                        group_id,
                        replica: replica.clone(),
                        client,
                        desc: desc.clone(),
                    });
                }
                coord.as_mut().unwrap().next_step(state).await;
            }
            debug!(
                "migration state watcher is stopped. replica_id={replica_id}, group_id={group_id}"
            );
            drop(wait_group);
        });
    }

    pub async fn forward(&self, forward_ctx: ForwardCtx, request: &Request) -> Result<Response> {
        let group_id = forward_ctx.dest_group_id;
        let mut client = self.shared.transport_manager.build_migrate_client(group_id);
        let req = ForwardRequest {
            shard_id: forward_ctx.shard_id,
            group_id,
            forward_data: forward_ctx.payloads.clone(),
            request: Some(GroupRequestUnion { request: Some(request.clone()) }),
        };
        let resp = client.forward(&req).await?;
        let resp = resp.response.and_then(|resp| resp.response);
        Ok(resp.unwrap())
    }
}

impl MigrationCoordinator {
    async fn next_step(&mut self, state: MigrationState) {
        let step = MigrationStep::from_i32(state.step).unwrap();
        if self.is_dest_group() {
            match step {
                MigrationStep::Prepare => {
                    self.setup_source_group().await;
                }
                MigrationStep::Migrating => {
                    self.pull(state.last_migrated_key).await;
                }
                MigrationStep::Migrated => {
                    // Send finish migration request to source group.
                    self.commit_source_group().await;
                }
                MigrationStep::Finished | MigrationStep::Aborted => unreachable!(),
            }
        } else {
            match step {
                MigrationStep::Migrated => {
                    self.clean_orphan_shard().await;
                }
                MigrationStep::Prepare | MigrationStep::Migrating => {}
                MigrationStep::Finished | MigrationStep::Aborted => unreachable!(),
            }
        }
    }

    async fn setup_source_group(&mut self) {
        debug!(
            "setup source group migration. replica={}, group={}, desc={}",
            self.replica_id, self.group_id, self.desc
        );

        match self.client.setup_migration(&self.desc).await {
            Ok(_) => {
                info!(
                    "setup source group migration success. replica={}, group={}, desc={}",
                    self.replica_id, self.group_id, self.desc
                );
                self.enter_pulling_step().await;
            }
            Err(sekas_client::Error::EpochNotMatch(group_desc)) => {
                // Since the epoch is not matched, this migration should be rollback.
                warn!(
                    "abort migration since epoch not match, new epoch is {}. replica={}, group={}, desc={}",
                        group_desc.epoch, self.replica_id, self.group_id, self.desc);
                self.abort_migration().await;
            }
            Err(err) => {
                error!(
                    "setup source group migration: {err:?}. replica={}, group={}, desc={}",
                    self.replica_id, self.group_id, self.desc
                );
            }
        }
    }

    async fn commit_source_group(&mut self) {
        if let Err(e) = self.client.commit_migration(&self.desc).await {
            error!(
                "commit source group migration: {e:?}. replica={}, group={}, desc={}",
                self.replica_id, self.group_id, self.desc
            );
            return;
        }

        info!(
            "source group migration is committed. replica={}, group={}, desc={}",
            self.replica_id, self.group_id, self.desc
        );

        self.clean_migration_state().await;
    }

    async fn commit_dest_group(&self) {
        if let Err(e) = self.replica.commit_migration(&self.desc).await {
            error!(
                "commit dest migration: {e:?}. replica={}, group={}, desc={}",
                self.replica_id, self.group_id, self.desc,
            );
            return;
        }

        info!(
            "dest group migration is committed. replica={}, group={}, desc={}",
            self.replica_id, self.group_id, self.desc,
        );
    }

    async fn clean_migration_state(&self) {
        if let Err(e) = self.replica.finish_migration(&self.desc).await {
            error!(
                "clean migration state: {e:?}. replica={}, group={}, desc={}",
                self.replica_id, self.group_id, self.desc,
            );
            return;
        }

        info!(
            "migration state is cleaned. replica={}, group={}, desc={}",
            self.replica_id, self.group_id, self.desc
        );
    }

    async fn abort_migration(&self) {
        if let Err(e) = self.replica.abort_migration(&self.desc).await {
            error!(
                "abort migration: {e:?}. replica={}, group={}, desc={}",
                self.replica_id, self.group_id, self.desc
            );
            return;
        }

        info!(
            "migration is aborted. replica={}, group={}, desc={}",
            self.replica_id, self.group_id, self.desc
        );
    }

    async fn enter_pulling_step(&self) {
        if let Err(e) = self.replica.enter_pulling_step(&self.desc).await {
            error!(
                "enter pulling step: {e:?}. replica={}, group={}, desc={}",
                self.replica_id, self.group_id, self.desc
            );
        }
    }

    async fn clean_orphan_shard(&self) {
        use super::gc::remove_shard;

        let group_engine = self.replica.group_engine();
        if let Err(e) =
            remove_shard(&self.cfg, self.replica.as_ref(), group_engine, self.desc.get_shard_id())
                .await
        {
            error!(
                "remove migrated shard from source group: {e:?}. replica={}, group={}, desc={}",
                self.replica_id, self.group_id, self.desc
            );
            return;
        }

        self.clean_migration_state().await;
    }

    async fn pull(&mut self, last_migrated_key: Option<Vec<u8>>) {
        if let Err(e) =
            pull_shard(&self.client, self.replica.as_ref(), &self.desc, last_migrated_key).await
        {
            error!(
                "pull shard from source group: {e:?}. replica={}, group={}, desc={}",
                self.replica_id, self.group_id, self.desc
            );
            return;
        }

        self.commit_dest_group().await;
    }

    #[inline]
    fn is_dest_group(&self) -> bool {
        self.group_id == self.desc.dest_group_id
    }
}

pub async fn pull_shard(
    client: &MigrateClient,
    replica: &Replica,
    desc: &MigrationDesc,
    last_migrated_key: Option<Vec<u8>>,
) -> Result<()> {
    record_latency!(take_pull_shard_metrics());
    let shard_id = desc.get_shard_id();
    let mut finished = false;
    let mut last_key = last_migrated_key;
    while !finished {
        info!("pull shard chunk with last key {last_key:?}");
        let shard_chunk = client.pull_shard_chunk(shard_id, last_key.clone()).await?;
        info!("pull shard chunk with {} data", shard_chunk.len());
        if shard_chunk.is_empty() {
            finished = true;
        } else {
            last_key = Some(shard_chunk.last().unwrap().key.clone());
        }
        NODE_INGEST_CHUNK_TOTAL.inc();
        replica.ingest(shard_id, shard_chunk, false).await?;
    }
    Ok(())
}
