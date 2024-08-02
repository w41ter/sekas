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
use log::{debug, error, info, trace, warn};
use sekas_api::server::v1::group_request_union::Request;
use sekas_api::server::v1::group_response_union::Response;
use sekas_api::server::v1::*;
use sekas_api::Epoch;
use sekas_client::MoveShardClient;
use sekas_runtime::JoinHandle;

use crate::node::metrics::*;
use crate::node::Replica;
use crate::serverpb::v1::*;
use crate::transport::TransportManager;
use crate::{record_latency, NodeConfig, Result};

#[derive(Debug)]
pub struct ForwardCtx {
    pub shard_id: u64,
    pub dest_group_id: u64,
    pub payloads: Vec<ValueSet>,
}

struct MoveShardCoordinator {
    cfg: NodeConfig,

    replica_id: u64,
    group_id: u64,

    replica: Arc<Replica>,

    client: MoveShardClient,
    desc: MoveShardDesc,
}

#[derive(Clone)]
pub struct MoveShardController {
    shared: Arc<MoveShardControllerShared>,
}

struct MoveShardControllerShared {
    cfg: NodeConfig,
    transport_manager: TransportManager,
}

impl MoveShardController {
    pub(crate) fn new(cfg: NodeConfig, transport_manager: TransportManager) -> Self {
        MoveShardController {
            shared: Arc::new(MoveShardControllerShared { cfg, transport_manager }),
        }
    }

    /// Watch moving shard state and do the corresponding step.
    pub fn watch_state_changes(
        &self,
        replica: Arc<Replica>,
        mut receiver: mpsc::UnboundedReceiver<MoveShardState>,
    ) -> JoinHandle<()> {
        let info = replica.replica_info();
        let replica_id = info.replica_id;
        let group_id = info.group_id;

        let ctrl = self.clone();
        sekas_runtime::spawn(async move {
            let mut coord: Option<MoveShardCoordinator> = None;
            while let Some(state) = receiver.next().await {
                debug!(
                    "on move shard step: {:?}. group={group_id}, replica={replica_id}",
                    MoveShardStep::from_i32(state.step)
                );
                let desc = state.get_move_shard_desc();
                if coord.is_none() || coord.as_ref().unwrap().desc != *desc {
                    let target_group_id = if desc.src_group_id == group_id {
                        desc.dest_group_id
                    } else {
                        desc.src_group_id
                    };
                    let client =
                        ctrl.shared.transport_manager.build_move_shard_client(target_group_id);
                    coord = Some(MoveShardCoordinator {
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
                "move shard state watcher is stopped. replica_id={replica_id}, group_id={group_id}"
            );
        })
    }

    pub async fn forward(
        &self,
        mut forward_ctx: ForwardCtx,
        request: &Request,
    ) -> Result<Response> {
        let group_id = forward_ctx.dest_group_id;
        let mut client = self.shared.transport_manager.build_move_shard_client(group_id);
        forward_ctx.payloads.retain(|f| !f.values.is_empty());
        let req = ForwardRequest {
            shard_id: forward_ctx.shard_id,
            group_id,
            forward_data: forward_ctx.payloads,
            request: Some(GroupRequestUnion { request: Some(request.clone()) }),
        };
        let resp = client.forward(&req).await?;
        let resp = resp.response.and_then(|resp| resp.response);
        Ok(resp.unwrap())
    }
}

impl MoveShardCoordinator {
    async fn next_step(&mut self, state: MoveShardState) {
        let step = MoveShardStep::from_i32(state.step).unwrap();
        if self.is_dest_group() {
            match step {
                MoveShardStep::Prepare => {
                    self.setup_source_group().await;
                }
                MoveShardStep::Moving => {
                    self.pull(state.last_moved_key).await;
                }
                MoveShardStep::Moved => {
                    // Send finish moving request to source group.
                    self.commit_source_group().await;
                }
                MoveShardStep::Finished | MoveShardStep::Aborted => unreachable!(),
            }
        } else {
            match step {
                MoveShardStep::Moved => {
                    self.clean_orphan_shard().await;
                }
                MoveShardStep::Prepare | MoveShardStep::Moving => {}
                MoveShardStep::Finished | MoveShardStep::Aborted => unreachable!(),
            }
        }
    }

    async fn setup_source_group(&mut self) {
        debug!(
            "setup source group moving shard. replica={}, group={}, desc={}",
            self.replica_id, self.group_id, self.desc
        );

        match self.client.acquire_shard(&self.desc).await {
            Ok(_) => {
                info!(
                    "setup source group moving shard success. replica={}, group={}, desc={}",
                    self.replica_id, self.group_id, self.desc
                );
                self.enter_pulling_step().await;
            }
            Err(sekas_client::Error::EpochNotMatch(group_desc)) => {
                // Since the epoch is not matched, this moving shard should be rollback.
                warn!(
                    "abort moving shard since epoch not match, new epoch is {}. replica={}, group={}, desc={}",
                        Epoch(group_desc.epoch), self.replica_id, self.group_id, self.desc);
                self.abort_moving_shard().await;
            }
            Err(err) => {
                error!(
                    "setup source group moving shard: {err:?}. replica={}, group={}, desc={}",
                    self.replica_id, self.group_id, self.desc
                );
            }
        }
    }

    async fn commit_source_group(&mut self) {
        if let Err(e) = self.client.move_out(&self.desc).await {
            error!(
                "commit source group moving shard: {e:?}. replica={}, group={}, desc={}",
                self.replica_id, self.group_id, self.desc
            );
            return;
        }

        info!(
            "source group moving shard is committed. replica={}, group={}, desc={}",
            self.replica_id, self.group_id, self.desc
        );

        self.clean_move_shard_state().await;
    }

    async fn commit_dest_group(&self) {
        if let Err(e) = self.replica.commit_shard_moving(&self.desc).await {
            error!(
                "commit dest moving shard: {e:?}. replica={}, group={}, desc={}",
                self.replica_id, self.group_id, self.desc,
            );
            return;
        }

        info!(
            "dest group moving shard is committed. replica={}, group={}, desc={}",
            self.replica_id, self.group_id, self.desc,
        );
    }

    async fn clean_move_shard_state(&self) {
        if let Err(e) = self.replica.finish_shard_moving(&self.desc).await {
            error!(
                "clean moving shard state: {e:?}. replica={}, group={}, desc={}",
                self.replica_id, self.group_id, self.desc,
            );
            return;
        }

        info!(
            "move shard state is cleaned. replica={}, group={}, desc={}",
            self.replica_id, self.group_id, self.desc
        );
    }

    async fn abort_moving_shard(&self) {
        if let Err(e) = self.replica.abort_shard_moving(&self.desc).await {
            error!(
                "abort moving shard: {e:?}. replica={}, group={}, desc={}",
                self.replica_id, self.group_id, self.desc
            );
            return;
        }

        info!(
            "moving shard is aborted. replica={}, group={}, desc={}",
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
                "remove moved out shard from source group: {e:?}. replica={}, group={}, desc={}",
                self.replica_id, self.group_id, self.desc
            );
            return;
        }

        self.clean_move_shard_state().await;
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
    client: &MoveShardClient,
    replica: &Replica,
    desc: &MoveShardDesc,
    last_migrated_key: Option<Vec<u8>>,
) -> Result<()> {
    record_latency!(take_pull_shard_metrics());
    let shard_id = desc.get_shard_id();
    let mut finished = false;
    let mut last_key = last_migrated_key;
    while !finished {
        trace!("pull shard {shard_id} chunk, last key {last_key:?}");
        let shard_chunk = client.pull_shard_chunk(shard_id, last_key.clone()).await?;
        trace!("pull shard {shard_id} chunk, receive {} value sets", shard_chunk.len());
        if let Some(value_set) = shard_chunk.last() {
            last_key = Some(value_set.user_key.clone());
        } else {
            finished = true;
        }
        for value_set in &shard_chunk {
            replica.ingest_value_set(shard_id, value_set).await?;
        }
        if let Some(value_set) = shard_chunk.last() {
            replica.save_ingest_progress(shard_id, &value_set.user_key).await?
        }
        NODE_INGEST_CHUNK_TOTAL.inc();
    }
    Ok(())
}
