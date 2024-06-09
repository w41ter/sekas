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

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::task::Waker;

use futures::channel::mpsc;
use log::info;
use sekas_api::server::v1::*;
use sekas_api::Epoch;

use super::fsm::StateMachineObserver;
use super::ReplicaInfo;
use crate::node::job::StateChannel;
use crate::raftgroup::StateObserver;
use crate::schedule::ScheduleStateObserver;
use crate::serverpb::v1::MoveShardState;

pub struct LeaseState {
    pub leader_id: u64,
    /// the largest term which state machine already known.
    pub applied_term: u64,
    pub replica_state: ReplicaState,
    pub descriptor: GroupDesc,
    pub move_shard_state: Option<MoveShardState>,
    pub move_shard_state_subscriber: mpsc::UnboundedSender<MoveShardState>,
    pub schedule_state: ScheduleState,
    pub leader_subscribers: HashMap<&'static str, Waker>,
}

/// A struct that observes changes to `GroupDesc` and `ReplicaState` , and
/// broadcasts those changes while saving them to `LeaseState`.
#[derive(Clone)]
pub struct LeaseStateObserver {
    info: Arc<ReplicaInfo>,
    lease_state: Arc<Mutex<LeaseState>>,
    state_channel: Arc<StateChannel>,
}

impl LeaseState {
    pub fn new(
        descriptor: GroupDesc,
        move_shard_state: Option<MoveShardState>,
        move_shard_state_subscriber: mpsc::UnboundedSender<MoveShardState>,
    ) -> Self {
        LeaseState {
            descriptor,
            move_shard_state,
            move_shard_state_subscriber,
            leader_id: 0,
            applied_term: 0,
            schedule_state: ScheduleState::default(),
            replica_state: ReplicaState::default(),
            leader_subscribers: HashMap::default(),
        }
    }

    #[inline]
    pub fn is_raft_leader(&self) -> bool {
        self.replica_state.role == RaftRole::Leader as i32
    }

    /// At least one log for the current term has been applied?
    #[inline]
    pub fn is_log_term_matched(&self) -> bool {
        self.applied_term == self.replica_state.term
    }

    #[inline]
    pub fn is_ready_for_serving(&self) -> bool {
        self.is_raft_leader() && self.is_log_term_matched()
    }

    #[inline]
    pub fn has_shard_moving(&self) -> bool {
        self.move_shard_state.is_some()
    }

    #[inline]
    pub fn is_shard_in_moving(&self, shard_id: u64) -> bool {
        self.move_shard_state.as_ref().map(|s| s.get_shard_id() == shard_id).unwrap_or_default()
    }

    #[inline]
    pub fn is_same_shard_moving(&self, desc: &MoveShardDesc) -> bool {
        self.move_shard_state.as_ref().unwrap().get_move_shard_desc() == desc
    }

    #[inline]
    pub fn wake_all_waiters(&mut self) {
        for (_, waker) in std::mem::take(&mut self.leader_subscribers) {
            waker.wake();
        }
    }

    #[inline]
    pub fn leader_descriptor(&self) -> Option<ReplicaDesc> {
        self.descriptor.replicas.iter().find(|r| r.id == self.leader_id).cloned()
    }

    #[inline]
    pub fn terminate(&mut self) {
        self.wake_all_waiters();
        self.move_shard_state_subscriber.close_channel();
    }
}

impl LeaseStateObserver {
    pub fn new(
        info: Arc<ReplicaInfo>,
        lease_state: Arc<Mutex<LeaseState>>,
        state_channel: Arc<StateChannel>,
    ) -> Self {
        LeaseStateObserver { info, lease_state, state_channel }
    }

    fn update_replica_state(
        &self,
        leader_id: u64,
        voted_for: u64,
        term: u64,
        role: RaftRole,
    ) -> (ReplicaState, Option<GroupDesc>) {
        let replica_state = ReplicaState {
            replica_id: self.info.replica_id,
            group_id: self.info.group_id,
            term,
            voted_for,
            role: role.into(),
            node_id: self.info.node_id,
        };
        let mut lease_state = self.lease_state.lock().unwrap();
        let prev_role = lease_state.replica_state.role;
        let epoch = lease_state.descriptor.epoch;
        lease_state.leader_id = leader_id;
        lease_state.replica_state = replica_state.clone();
        let desc = if role == RaftRole::Leader {
            info!(
                "replica {} node {} become leader of group {} at term {term} epoch {}",
                self.info.replica_id,
                self.info.node_id,
                self.info.group_id,
                Epoch(epoch)
            );
            Some(lease_state.descriptor.clone())
        } else {
            if prev_role == RaftRole::Leader as i32 {
                info!(
                    "replica {} node {} resign as leader of group {} at term {term} epoch {}",
                    self.info.replica_id,
                    self.info.node_id,
                    self.info.group_id,
                    Epoch(epoch)
                );
            }
            None
        };
        (replica_state, desc)
    }

    fn update_descriptor(&self, descriptor: GroupDesc) -> bool {
        let mut lease_state = self.lease_state.lock().unwrap();
        lease_state.descriptor = descriptor;
        lease_state.replica_state.role == RaftRole::Leader as i32
    }
}

impl StateObserver for LeaseStateObserver {
    fn on_state_updated(&mut self, leader_id: u64, voted_for: u64, term: u64, role: RaftRole) {
        let (state, desc) = self.update_replica_state(leader_id, voted_for, term, role);
        self.state_channel.broadcast_replica_state(self.info.group_id, state);
        if let Some(desc) = desc {
            self.state_channel.broadcast_group_descriptor(self.info.group_id, desc);
        }
    }
}

impl StateMachineObserver for LeaseStateObserver {
    fn on_descriptor_updated(&mut self, descriptor: GroupDesc) {
        if self.update_descriptor(descriptor.clone()) {
            self.state_channel.broadcast_group_descriptor(self.info.group_id, descriptor);
        }
    }

    fn on_term_updated(&mut self, term: u64) {
        let mut lease_state = self.lease_state.lock().unwrap();
        lease_state.applied_term = term;
        if lease_state.is_ready_for_serving() {
            info!(
                "replica {} node {} is ready for serving requests of group {} at term {term}",
                self.info.replica_id, self.info.node_id, self.info.group_id
            );
            lease_state.wake_all_waiters();
            if let Some(move_shard_state) = lease_state.move_shard_state.as_ref() {
                lease_state
                    .move_shard_state_subscriber
                    .unbounded_send(move_shard_state.to_owned())
                    .unwrap_or_default();
            }
        }
    }

    fn on_move_shard_state_updated(&mut self, move_shard_state: Option<MoveShardState>) {
        let mut lease_state = self.lease_state.lock().unwrap();
        lease_state.move_shard_state = move_shard_state;
        if let Some(move_shard_state) = lease_state.move_shard_state.as_ref() {
            if lease_state.is_ready_for_serving() {
                lease_state
                    .move_shard_state_subscriber
                    .unbounded_send(move_shard_state.to_owned())
                    .unwrap_or_default();
            }
        }
    }
}

impl ScheduleStateObserver for LeaseStateObserver {
    fn on_schedule_state_updated(&self, schedule_state: ScheduleState) {
        let cloned_schedule_state = schedule_state.clone();
        let mut lease_state = self.lease_state.lock().unwrap();
        lease_state.schedule_state = schedule_state;
        self.state_channel.broadcast_schedule_state(self.info.group_id, cloned_schedule_state);
    }
}
