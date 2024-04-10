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

mod checkpoint;

use std::collections::HashSet;
use std::path::Path;
use std::sync::Arc;

use log::{info, trace, warn};
use sekas_api::server::v1::{
    ChangeReplica, ChangeReplicaType, ChangeReplicas, GroupDesc, MoveShardDesc, ReplicaDesc,
    ReplicaRole,
};

use super::ReplicaInfo;
use crate::engine::{GroupEngine, WriteBatch, WriteStates};
use crate::raftgroup::{ApplyEntry, SnapshotBuilder, StateMachine};
use crate::serverpb::v1::*;
use crate::{ReplicaConfig, Result};

const SHARD_UPDATE_DELTA: u64 = 1 << 32;
const CONFIG_CHANGE_DELTA: u64 = 1;

#[derive(Debug)]
enum ChangeReplicaKind {
    Simple,
    EnterJoint,
    LeaveJoint,
}

/// An abstracted structure used to subscribe to state machine changes.
pub trait StateMachineObserver: Send + Sync {
    /// This function will be called every time the `GroupDesc` changes.
    fn on_descriptor_updated(&mut self, descriptor: GroupDesc);

    /// This function will be called once the encountered term changes.
    fn on_term_updated(&mut self, term: u64);

    /// This function will be called once the move shard state changes.
    fn on_move_shard_state_updated(&mut self, state: Option<MoveShardState>);
}

pub struct GroupStateMachine
where
    Self: Send,
{
    cfg: ReplicaConfig,
    info: Arc<ReplicaInfo>,

    group_engine: GroupEngine,
    observer: Box<dyn StateMachineObserver>,

    plugged_write_batches: Vec<WriteBatch>,
    plugged_write_states: WriteStates,

    /// Whether `GroupDesc` changes during apply.
    desc_updated: bool,
    move_shard_state_updated: bool,
    last_applied_term: u64,
}

impl GroupStateMachine {
    pub(crate) fn new(
        cfg: ReplicaConfig,
        info: Arc<ReplicaInfo>,
        group_engine: GroupEngine,
        observer: Box<dyn StateMachineObserver>,
    ) -> Self {
        let apply_state = group_engine.flushed_apply_state().expect("access flushed index");
        GroupStateMachine {
            cfg,
            info,
            group_engine,
            observer,
            plugged_write_batches: Vec::default(),
            plugged_write_states: WriteStates::default(),
            desc_updated: false,
            move_shard_state_updated: false,
            last_applied_term: apply_state.term,
        }
    }
}

impl GroupStateMachine {
    fn apply_change_replicas(&mut self, change_replicas: ChangeReplicas) -> Result<()> {
        let local_id = self.info.replica_id;
        let mut desc = self.descriptor();
        match ChangeReplicaKind::new(&change_replicas) {
            ChangeReplicaKind::LeaveJoint => apply_leave_joint(local_id, &mut desc),
            ChangeReplicaKind::EnterJoint => {
                apply_enter_joint(local_id, &mut desc, &change_replicas.changes)
            }
            ChangeReplicaKind::Simple => {
                apply_simple_change(local_id, &mut desc, &change_replicas.changes[0])
            }
        }
        desc.epoch += CONFIG_CHANGE_DELTA;
        self.desc_updated = true;
        self.plugged_write_states.descriptor = Some(desc);

        Ok(())
    }

    fn apply_proposal(&mut self, eval_result: EvalResult) -> Result<()> {
        if let Some(wb) = eval_result.batch {
            self.plugged_write_batches.push(WriteBatch::new(&wb.data));
        }

        if let Some(op) = eval_result.op {
            let mut desc = self.descriptor();
            if let Some(AddShard { shard: Some(shard) }) = op.add_shard {
                for existed_shard in &desc.shards {
                    if existed_shard.id == shard.id {
                        todo!("shard {} already existed in group", shard.id);
                    }
                }
                info!(
                    "group {} add shard {} at epoch {}",
                    self.info.group_id, shard.id, desc.epoch
                );
                self.desc_updated = true;
                desc.epoch += SHARD_UPDATE_DELTA;
                desc.shards.push(shard);
            }
            if let Some(m) = op.move_shard {
                self.apply_move_shard_event(m, &mut desc);
            }

            // Any sync_op will update group desc.
            self.plugged_write_states.descriptor = Some(desc);
        }

        Ok(())
    }

    fn apply_move_shard_event(&mut self, move_shard: MoveShard, group_desc: &mut GroupDesc) {
        let event = MoveShardEvent::from_i32(move_shard.event).expect("unknown moving shard event");
        if let Some(desc) = move_shard.desc.as_ref() {
            info!(
                "apply moving shard event. replica={}, group={}, desc={}, event={:?}",
                self.info.replica_id, self.info.group_id, desc, event
            );
        }

        match event {
            MoveShardEvent::Setup => {
                if move_shard.desc.is_none() {
                    warn!(
                        "MovingShard::desc is None. replica={}, group={}",
                        self.info.replica_id, self.info.group_id
                    );
                    return;
                }

                let state = MoveShardState {
                    move_shard: move_shard.desc,
                    last_moved_key: None,
                    step: MoveShardStep::Prepare as i32,
                };
                debug_assert!(state.move_shard.is_some());
                self.plugged_write_states.move_shard_state = Some(state);
                self.move_shard_state_updated = true;
            }
            MoveShardEvent::Ingest => {
                let mut state = self.must_move_shard_state();

                // If only the ingested key changes, there is no need to notify the move shard
                // controller to perform corresponding operations.
                if state.step == MoveShardStep::Prepare as i32 {
                    state.step = MoveShardStep::Moving as i32;
                    self.move_shard_state_updated = true;
                }

                debug_assert!(state.step == MoveShardStep::Moving as i32);
                state.last_moved_key = Some(move_shard.last_ingested_key);

                self.plugged_write_states.move_shard_state = Some(state);
            }
            MoveShardEvent::Commit => {
                let mut state = self.must_move_shard_state();
                debug_assert!(
                    state.step == MoveShardStep::Moving as i32
                        || state.step == MoveShardStep::Prepare as i32
                );
                state.step = MoveShardStep::Moved as i32;
                self.plugged_write_states.move_shard_state = Some(state);
                self.move_shard_state_updated = true;
            }
            MoveShardEvent::Apply => {
                let mut state = self.must_move_shard_state();
                debug_assert!(state.step == MoveShardStep::Moved as i32);

                let desc = state.get_move_shard_desc();
                self.apply_moving_shard(group_desc, desc);

                state.step = MoveShardStep::Finished as i32;
                self.plugged_write_states.move_shard_state = Some(state);
                self.move_shard_state_updated = true;
            }
            MoveShardEvent::Abort => {
                let mut state = self.must_move_shard_state();
                debug_assert!(state.step == MoveShardStep::Prepare as i32);

                state.step = MoveShardStep::Aborted as i32;
                self.plugged_write_states.move_shard_state = Some(state);
                self.move_shard_state_updated = true;
            }
        }
    }

    fn apply_moving_shard(&mut self, group_desc: &mut GroupDesc, desc: &MoveShardDesc) {
        let shard_desc = desc.get_shard_desc();

        let inherited_epoch = std::cmp::max(desc.src_group_epoch, desc.dest_group_epoch);
        let inherited_epoch = std::cmp::max(group_desc.epoch, inherited_epoch);
        group_desc.epoch = inherited_epoch + SHARD_UPDATE_DELTA;
        let msg = if desc.src_group_id == group_desc.id {
            group_desc.shards.retain(|r| r.id != shard_desc.id);
            "shard migrated out"
        } else {
            debug_assert_eq!(desc.dest_group_id, group_desc.id);
            group_desc.shards.push(shard_desc.clone());
            "shard migrated in"
        };
        info!(
            "apply moving shard: {msg}. replica={}, group={}, epoch={}, shard={}",
            self.info.replica_id, self.info.group_id, group_desc.epoch, shard_desc.id
        );
        self.desc_updated = true;
    }

    fn flush_updated_events(&mut self, term: u64) {
        if self.desc_updated {
            self.desc_updated = false;
            self.observer.on_descriptor_updated(self.group_engine.descriptor());
        }

        if term > self.last_applied_term {
            self.last_applied_term = term;
            self.observer.on_term_updated(term);
        }

        if self.move_shard_state_updated {
            self.move_shard_state_updated = false;
            self.observer.on_move_shard_state_updated(self.group_engine.move_shard_state());
        }
    }

    #[inline]
    fn flushed_apply_state(&self) -> ApplyState {
        self.group_engine.flushed_apply_state().expect("access flushed index")
    }

    #[inline]
    fn must_move_shard_state(&self) -> MoveShardState {
        self.plugged_write_states.move_shard_state.clone().unwrap_or_else(|| {
            self.group_engine.move_shard_state().expect("The MoveShardState should exist")
        })
    }
}

impl StateMachine for GroupStateMachine {
    #[inline]
    fn start_plug(&mut self) -> Result<()> {
        assert!(self.plugged_write_batches.is_empty());
        assert!(self.plugged_write_states.apply_state.is_none());
        Ok(())
    }

    fn apply(&mut self, index: u64, term: u64, entry: ApplyEntry) -> Result<()> {
        let group_id = self.info.group_id;
        trace!("group {group_id} apply entry index {index} term {term}",);
        match entry {
            ApplyEntry::Empty => {}
            ApplyEntry::ConfigChange { change_replicas } => {
                self.apply_change_replicas(change_replicas)?;
            }
            ApplyEntry::Proposal { eval_result } => {
                self.apply_proposal(eval_result)?;
            }
        }
        self.plugged_write_states.apply_state = Some(ApplyState { index, term });

        Ok(())
    }

    fn finish_plug(&mut self) -> Result<()> {
        let Some(ApplyState { term, .. }) = self.plugged_write_states.apply_state else {
            panic!("invoke GroupStateMachine::finish_plug but WriteStates::apply_states is None");
        };
        self.group_engine.group_commit(
            self.plugged_write_batches.as_slice(),
            std::mem::take(&mut self.plugged_write_states),
            false,
        )?;

        struct TriggerIterator {}
        impl rocksdb::WriteBatchIterator for TriggerIterator {
            fn put(&mut self, key: Box<[u8]>, value: Box<[u8]>) {}

            fn delete(&mut self, key: Box<[u8]>) {}
        }
        let mut trigger_iter = TriggerIterator {};
        for batch in &self.plugged_write_batches {
            batch.iterate(&mut trigger_iter);
        }

        self.plugged_write_batches.clear();
        self.flush_updated_events(term);

        Ok(())
    }

    fn apply_snapshot(&mut self, snap_dir: &Path) -> Result<()> {
        checkpoint::apply_snapshot(&self.group_engine, self.info.replica_id, snap_dir)?;
        self.observer.on_descriptor_updated(self.group_engine.descriptor());
        let apply_state = self.flushed_apply_state();
        self.observer.on_term_updated(apply_state.term);
        Ok(())
    }

    fn snapshot_builder(&self) -> Box<dyn SnapshotBuilder> {
        Box::new(checkpoint::GroupSnapshotBuilder::new(self.cfg.clone(), self.group_engine.clone()))
    }

    #[inline]
    fn flushed_index(&self) -> u64 {
        // FIXME(walter) avoid disk IO.
        self.group_engine.flushed_apply_state().expect("access flushed index").index
    }

    #[inline]
    fn descriptor(&self) -> GroupDesc {
        self.plugged_write_states
            .descriptor
            .clone()
            .unwrap_or_else(|| self.group_engine.descriptor())
    }
}

impl ChangeReplicaKind {
    fn new(cc: &ChangeReplicas) -> Self {
        match cc.changes.len() {
            0 => ChangeReplicaKind::LeaveJoint,
            1 => ChangeReplicaKind::Simple,
            _ => ChangeReplicaKind::EnterJoint,
        }
    }
}

fn apply_simple_change(local_id: u64, desc: &mut GroupDesc, change: &ChangeReplica) {
    let group_id = desc.id;
    let replica_id = change.replica_id;
    let node_id = change.node_id;
    let exist = find_replica_mut(desc, replica_id);
    check_not_in_joint_state(&exist);
    match ChangeReplicaType::from_i32(change.change_type) {
        Some(ChangeReplicaType::Add) => {
            info!("group {group_id} replica {local_id} add voter {replica_id}");
            if let Some(replica) = exist {
                replica.role = ReplicaRole::Voter.into();
            } else {
                desc.replicas.push(ReplicaDesc {
                    id: replica_id,
                    node_id,
                    role: ReplicaRole::Voter.into(),
                });
            }
        }
        Some(ChangeReplicaType::AddLearner) => {
            info!("group {group_id} replica {local_id} add learner {replica_id}");
            if let Some(replica) = exist {
                replica.role = ReplicaRole::Learner.into();
            } else {
                desc.replicas.push(ReplicaDesc {
                    id: replica_id,
                    node_id,
                    role: ReplicaRole::Learner.into(),
                });
            }
        }
        Some(ChangeReplicaType::Remove) => {
            info!("group {group_id} replica {local_id} remove voter {replica_id}");
            desc.replicas.retain(|rep| rep.id != replica_id);
        }
        None => {
            panic!("such change replica operation isn't supported")
        }
    }
}

fn apply_enter_joint(local_id: u64, desc: &mut GroupDesc, changes: &[ChangeReplica]) {
    let group_id = desc.id;
    let roles = group_role_digest(desc);
    let mut outgoing_learners = HashSet::new();
    for change in changes {
        let replica_id = change.replica_id;
        let node_id = change.node_id;
        let exist = find_replica_mut(desc, replica_id);
        check_not_in_joint_state(&exist);
        let exist_role = exist.as_ref().and_then(|r| ReplicaRole::from_i32(r.role));
        let change = ChangeReplicaType::from_i32(change.change_type)
            .expect("such change replica operation isn't supported");

        match (exist_role, change) {
            (Some(ReplicaRole::Learner), ChangeReplicaType::Add) => {
                exist.unwrap().role = ReplicaRole::IncomingVoter as i32;
            }
            (Some(ReplicaRole::Voter), ChangeReplicaType::AddLearner) => {
                exist.unwrap().role = ReplicaRole::DemotingVoter as i32;
            }
            (Some(ReplicaRole::Voter), ChangeReplicaType::Remove) => {
                exist.unwrap().role = ReplicaRole::DemotingVoter as i32;
            }
            (None, ChangeReplicaType::Add) => {
                desc.replicas.push(ReplicaDesc {
                    id: replica_id,
                    node_id,
                    role: ReplicaRole::IncomingVoter as i32,
                });
            }
            (None, ChangeReplicaType::AddLearner) => {
                desc.replicas.push(ReplicaDesc {
                    id: replica_id,
                    node_id,
                    role: ReplicaRole::Learner as i32,
                });
            }
            (Some(ReplicaRole::Learner), ChangeReplicaType::Remove) => {
                outgoing_learners.insert(replica_id);
            }
            (Some(ReplicaRole::Voter), ChangeReplicaType::Add)
            | (Some(ReplicaRole::Learner), ChangeReplicaType::AddLearner)
            | (None, ChangeReplicaType::Remove) => {}
            _ => unreachable!(),
        }
    }

    desc.replicas.retain(|r| !outgoing_learners.contains(&r.id));

    let changes = change_replicas_digest(changes);
    info!("group {group_id} replica {local_id} enter join and {changes}, former {roles}");
}

fn apply_leave_joint(local_id: u64, desc: &mut GroupDesc) {
    let group_id = desc.id;
    for replica in &mut desc.replicas {
        let role = match ReplicaRole::from_i32(replica.role) {
            Some(ReplicaRole::IncomingVoter) => ReplicaRole::Voter,
            Some(ReplicaRole::DemotingVoter) => ReplicaRole::Learner,
            _ => continue,
        };
        replica.role = role as i32;
    }

    info!("group {group_id} replica {local_id} leave joint with {}", group_role_digest(desc));
}

fn group_role_digest(desc: &GroupDesc) -> String {
    let mut voters = vec![];
    let mut learners = vec![];
    for r in &desc.replicas {
        match ReplicaRole::from_i32(r.role) {
            Some(ReplicaRole::Voter | ReplicaRole::IncomingVoter | ReplicaRole::DemotingVoter) => {
                voters.push(r.id)
            }
            Some(ReplicaRole::Learner) => learners.push(r.id),
            _ => continue,
        }
    }
    format!("voters {voters:?} learners {learners:?}")
}

fn change_replicas_digest(changes: &[ChangeReplica]) -> String {
    let mut add_voters = vec![];
    let mut remove_replicas = vec![];
    let mut add_learners = vec![];
    for cc in changes {
        match ChangeReplicaType::from_i32(cc.change_type) {
            Some(ChangeReplicaType::Add) => add_voters.push(cc.replica_id),
            Some(ChangeReplicaType::AddLearner) => add_learners.push(cc.replica_id),
            Some(ChangeReplicaType::Remove) => remove_replicas.push(cc.replica_id),
            _ => continue,
        }
    }
    format!("add voters {add_voters:?} learners {add_learners:?} remove {remove_replicas:?}")
}

fn find_replica_mut(desc: &mut GroupDesc, replica_id: u64) -> Option<&mut ReplicaDesc> {
    desc.replicas.iter_mut().find(|rep| rep.id == replica_id)
}

fn check_not_in_joint_state(exist: &Option<&mut ReplicaDesc>) {
    if matches!(
        exist
            .as_ref()
            .and_then(|rep| ReplicaRole::from_i32(rep.role))
            .unwrap_or(ReplicaRole::Voter),
        ReplicaRole::IncomingVoter | ReplicaRole::DemotingVoter
    ) {
        panic!("execute conf change but still in joint state");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn group_replicas(desc: &GroupDesc) -> Vec<(u64, ReplicaRole)> {
        let mut result: Vec<(u64, ReplicaRole)> =
            desc.replicas.iter().map(|r| (r.id, ReplicaRole::from_i32(r.role).unwrap())).collect();

        result.sort_unstable();
        result
    }

    #[test]
    fn simple_config_change() {
        struct Test {
            tips: &'static str,
            change_type: ChangeReplicaType,
            replica_id: u64,
            expects: Vec<(u64, ReplicaRole)>,
        }
        let tests = vec![
            Test {
                tips: "1. add not exists voter",
                change_type: ChangeReplicaType::Add,
                replica_id: 3,
                expects: vec![
                    (1, ReplicaRole::Learner),
                    (2, ReplicaRole::Voter),
                    (3, ReplicaRole::Voter),
                ],
            },
            Test {
                tips: "2. add exists voter",
                change_type: ChangeReplicaType::Add,
                replica_id: 2,
                expects: vec![(1, ReplicaRole::Learner), (2, ReplicaRole::Voter)],
            },
            Test {
                tips: "3. promote learner",
                change_type: ChangeReplicaType::Add,
                replica_id: 1,
                expects: vec![(1, ReplicaRole::Voter), (2, ReplicaRole::Voter)],
            },
            Test {
                tips: "4. add not exists learner",
                change_type: ChangeReplicaType::AddLearner,
                replica_id: 3,
                expects: vec![
                    (1, ReplicaRole::Learner),
                    (2, ReplicaRole::Voter),
                    (3, ReplicaRole::Learner),
                ],
            },
            Test {
                tips: "5. add exists learner",
                change_type: ChangeReplicaType::AddLearner,
                replica_id: 1,
                expects: vec![(1, ReplicaRole::Learner), (2, ReplicaRole::Voter)],
            },
            Test {
                tips: "6. demote voter",
                change_type: ChangeReplicaType::AddLearner,
                replica_id: 2,
                expects: vec![(1, ReplicaRole::Learner), (2, ReplicaRole::Learner)],
            },
            Test {
                tips: "6. remove not exists",
                change_type: ChangeReplicaType::Remove,
                replica_id: 3,
                expects: vec![(1, ReplicaRole::Learner), (2, ReplicaRole::Voter)],
            },
            Test {
                tips: "7. remove learner",
                change_type: ChangeReplicaType::Remove,
                replica_id: 1,
                expects: vec![(2, ReplicaRole::Voter)],
            },
            Test {
                tips: "8. remove voter",
                change_type: ChangeReplicaType::Remove,
                replica_id: 2,
                expects: vec![(1, ReplicaRole::Learner)],
            },
        ];

        let base_group_desc = GroupDesc {
            id: 1,
            epoch: 1,
            shards: vec![],
            replicas: vec![
                ReplicaDesc { id: 1, node_id: 1, role: ReplicaRole::Learner as i32 },
                ReplicaDesc { id: 2, node_id: 2, role: ReplicaRole::Voter as i32 },
            ],
        };

        for Test { tips, change_type, replica_id, expects } in tests {
            let mut descriptor = base_group_desc.clone();
            let change =
                ChangeReplica { change_type: change_type as i32, replica_id, node_id: 123 };
            apply_simple_change(0, &mut descriptor, &change);
            let replicas = group_replicas(&descriptor);
            assert_eq!(replicas, expects, "{tips}");
        }
    }

    #[test]
    fn joint_config_change() {
        struct Test {
            tips: &'static str,
            change_type: ChangeReplicaType,
            replica_id: u64,
            expects: Vec<(u64, ReplicaRole)>,
        }

        let base_group_desc = GroupDesc {
            id: 1,
            epoch: 1,
            shards: vec![],
            replicas: vec![
                ReplicaDesc { id: 1, node_id: 1, role: ReplicaRole::Learner as i32 },
                ReplicaDesc { id: 2, node_id: 2, role: ReplicaRole::Voter as i32 },
            ],
        };

        let tests = vec![
            Test {
                tips: "1. add new voter",
                change_type: ChangeReplicaType::Add,
                replica_id: 3,
                expects: vec![
                    (1, ReplicaRole::Learner),
                    (2, ReplicaRole::Voter),
                    (3, ReplicaRole::Voter),
                ],
            },
            Test {
                tips: "2. promote learner",
                change_type: ChangeReplicaType::Add,
                replica_id: 1,
                expects: vec![(1, ReplicaRole::Voter), (2, ReplicaRole::Voter)],
            },
            Test {
                tips: "3. add exists voter",
                change_type: ChangeReplicaType::Add,
                replica_id: 2,
                expects: vec![(1, ReplicaRole::Learner), (2, ReplicaRole::Voter)],
            },
            Test {
                tips: "4. add new learner",
                change_type: ChangeReplicaType::AddLearner,
                replica_id: 3,
                expects: vec![
                    (1, ReplicaRole::Learner),
                    (2, ReplicaRole::Voter),
                    (3, ReplicaRole::Learner),
                ],
            },
            Test {
                tips: "5. add exists learner",
                change_type: ChangeReplicaType::AddLearner,
                replica_id: 1,
                expects: vec![(1, ReplicaRole::Learner), (2, ReplicaRole::Voter)],
            },
            Test {
                tips: "6. demote voter",
                change_type: ChangeReplicaType::AddLearner,
                replica_id: 2,
                expects: vec![(1, ReplicaRole::Learner), (2, ReplicaRole::Learner)],
            },
            Test {
                tips: "7. remove voter",
                change_type: ChangeReplicaType::Remove,
                replica_id: 2,
                expects: vec![(1, ReplicaRole::Learner), (2, ReplicaRole::Learner)],
            },
            Test {
                tips: "8. remove learner",
                change_type: ChangeReplicaType::Remove,
                replica_id: 1,
                expects: vec![(2, ReplicaRole::Voter)],
            },
            Test {
                tips: "8. remove not exists voter",
                change_type: ChangeReplicaType::Remove,
                replica_id: 3,
                expects: vec![(1, ReplicaRole::Learner), (2, ReplicaRole::Voter)],
            },
        ];

        for Test { tips, change_type, replica_id, expects } in tests {
            let mut descriptor = base_group_desc.clone();
            let change =
                ChangeReplica { change_type: change_type as i32, replica_id, node_id: 123 };
            apply_enter_joint(0, &mut descriptor, &[change]);
            apply_leave_joint(0, &mut descriptor);
            let replicas = group_replicas(&descriptor);
            assert_eq!(replicas, expects, "{tips}");
        }
    }
}
