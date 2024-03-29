// Copyright 2023 The Sekas Authors.
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
mod applier;
mod fsm;
mod group;
mod io;
mod metrics;
mod monitor;
mod node;
pub mod snap;
mod storage;
mod worker;

use std::sync::Arc;

use raft::prelude::{
    ConfChangeSingle, ConfChangeTransition, ConfChangeType, ConfChangeV2, ConfState,
};
use sekas_api::server::v1::*;
use sekas_runtime::{JoinHandle, TaskGroup};

pub use self::fsm::{ApplyEntry, SnapshotBuilder, StateMachine};
pub use self::group::RaftGroup;
use self::io::LogWriter;
pub use self::io::{retrive_snapshot, AddressResolver, ChannelManager};
pub use self::monitor::*;
pub use self::snap::SnapManager;
pub use self::storage::{destory as destory_storage, write_initial_state};
use self::worker::RaftWorker;
pub use self::worker::{RaftGroupState, StateObserver};
use crate::raftgroup::io::start_purging_expired_files;
use crate::{RaftConfig, Result};

/// `ReadPolicy` is used to control `RaftNodeFacade::read` behavior.
#[derive(Debug, Clone, Copy)]
pub enum ReadPolicy {
    /// Do nothing
    Relaxed,
    /// Wait until all former committed entries be applied.
    LeaseRead,
    /// Like `ReadPolicy::LeaseRead`, but require exchange heartbeat with
    /// majority members before waiting.
    ReadIndex,
}

pub struct RaftManager {
    pub cfg: RaftConfig,
    engine: Arc<raft_engine::Engine>,
    log_writer: LogWriter,
    transport_mgr: Arc<ChannelManager>,
    snap_mgr: SnapManager,
    _task_handle: Option<JoinHandle<()>>,
}

impl RaftManager {
    pub(crate) async fn open(
        cfg: RaftConfig,
        engine: Arc<raft_engine::Engine>,
        snap_mgr: SnapManager,
        transport_mgr: Arc<ChannelManager>,
    ) -> Result<Self> {
        let task_handle = start_purging_expired_files(engine.clone());
        let log_writer = LogWriter::new(cfg.max_io_batch_size, engine.clone());
        Ok(RaftManager {
            cfg,
            engine,
            transport_mgr,
            snap_mgr,
            log_writer,
            _task_handle: Some(task_handle),
        })
    }

    #[inline]
    pub fn engine(&self) -> Arc<raft_engine::Engine> {
        self.engine.clone()
    }

    #[inline]
    pub fn snapshot_manager(&self) -> &SnapManager {
        &self.snap_mgr
    }

    #[inline]
    pub async fn list_groups(&self) -> Vec<u64> {
        self.engine.raft_groups()
    }

    pub async fn start_raft_group<M: 'static + StateMachine>(
        &self,
        group_id: u64,
        replica_id: u64,
        node_id: u64,
        state_machine: M,
        observer: Box<dyn StateObserver>,
        task_group: &TaskGroup,
    ) -> Result<RaftGroup> {
        let worker =
            RaftWorker::open(group_id, replica_id, node_id, state_machine, self, observer).await?;
        let raft_group = RaftGroup::open(worker.request_sender());
        let log_writer = self.log_writer.clone();
        let task_handle = sekas_runtime::spawn(async move {
            if let Err(err) = worker.run(log_writer).await {
                // TODO(walter) handle result.
                panic!("run raft group worker: {err:?}");
            }
        });
        task_group.add_task(task_handle);
        Ok(raft_group)
    }
}

fn encode_to_conf_change(change_replicas: ChangeReplicas) -> ConfChangeV2 {
    use prost::Message;

    let mut conf_changes = vec![];
    for c in &change_replicas.changes {
        let change_type = match ChangeReplicaType::from_i32(c.change_type) {
            Some(ChangeReplicaType::Add) => ConfChangeType::AddNode,
            Some(ChangeReplicaType::Remove) => ConfChangeType::RemoveNode,
            Some(ChangeReplicaType::AddLearner) => ConfChangeType::AddLearnerNode,
            None => panic!("such change replica operation isn't supported"),
        };
        conf_changes
            .push(ConfChangeSingle { change_type: change_type.into(), node_id: c.replica_id });
    }

    ConfChangeV2 {
        transition: ConfChangeTransition::Auto.into(),
        context: change_replicas.encode_to_vec(),
        changes: conf_changes,
    }
}

fn decode_from_conf_change(conf_change: &ConfChangeV2) -> ChangeReplicas {
    use prost::Message;

    ChangeReplicas::decode(&*conf_change.context)
        .expect("ChangeReplicas is saved in ConfChangeV2::context")
}

pub fn conf_state_from_group_descriptor(desc: &GroupDesc) -> ConfState {
    let mut cs = ConfState::default();
    let mut in_joint = false;
    for replica in desc.replicas.iter() {
        match ReplicaRole::from_i32(replica.role).unwrap_or(ReplicaRole::Voter) {
            ReplicaRole::Voter => {
                cs.voters.push(replica.id);
            }
            ReplicaRole::Learner => {
                cs.learners.push(replica.id);
            }
            ReplicaRole::IncomingVoter => {
                in_joint = true;
                cs.voters.push(replica.id);
            }
            ReplicaRole::DemotingVoter => {
                in_joint = true;
                cs.voters_outgoing.push(replica.id);
                cs.learners_next.push(replica.id);
            }
        }
    }
    if !in_joint {
        cs.voters_outgoing.clear();
    }
    cs
}
