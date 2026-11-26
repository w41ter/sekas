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

use log::{debug, info};
use sekas_api::server::v1::*;

use super::eval::{LatchManager, ingest_value_set};
use super::{LeaseState, Replica, ReplicaInfo};
use crate::engine::WriteBatch;
use crate::error::BusyReason;
use crate::serverpb::v1::*;
use crate::{Error, Result};

impl Replica {
    /// Import source history before executing a forwarded request.
    pub(crate) async fn ingest_forwarded_value_set(
        &self,
        shard_id: u64,
        dest_group_epoch: u64,
        value_set: &ValueSet,
    ) -> Result<()> {
        let _acl_guard = self.take_read_acl_guard().await;
        let (term, movement) = self.shard_import_state(shard_id)?;
        let Some(movement) = movement else { return Ok(()) };
        if !movement.matches_forward(self.info.group_id, dest_group_epoch) {
            return Err(Error::EpochNotMatch(self.descriptor()));
        }
        if movement.step() == MoveShardStep::Moved {
            return Ok(());
        }

        let _latch = self.latch_mgr.acquire(shard_id, &value_set.user_key).await?;
        let mut wb = WriteBatch::default();
        let desc = movement.get_move_shard_desc();
        if ingest_value_set(&self.group_engine, desc, value_set, &mut wb)? {
            wb.mark_migrated_key(desc.dest_group_epoch, &value_set.user_key);
            self.propose_shard_import(term, wb, None).await?;
        }
        Ok(())
    }

    /// Import a contiguous chunk from the background pull and advance its
    /// checkpoint in the same proposal.
    pub(crate) async fn ingest_shard_chunk(
        &self,
        desc: &MoveShardDesc,
        value_sets: &[ValueSet],
    ) -> Result<()> {
        let Some(last) = value_sets.last() else { return Ok(()) };
        let shard_id = desc.get_shard_id();
        let _acl_guard = self.take_read_acl_guard().await;
        let (term, movement) = self.shard_import_state(shard_id)?;
        if !movement.as_ref().is_some_and(|state| {
            desc.dest_group_id == self.info.group_id
                && state.get_move_shard_desc() == desc
                && state.step() == MoveShardStep::Moving
        }) {
            return Err(Error::ShardNotFound(shard_id));
        }

        let mut wb = WriteBatch::default();
        let mut latches = Vec::with_capacity(value_sets.len());
        for value_set in value_sets {
            latches.push(self.latch_mgr.acquire(shard_id, &value_set.user_key).await?);
            ingest_value_set(&self.group_engine, desc, value_set, &mut wb)?;
        }

        self.propose_shard_import(term, wb, Some(SyncOp::ingest(last.user_key.clone()))).await
    }

    fn shard_import_state(&self, shard_id: u64) -> Result<(u64, Option<MoveShardState>)> {
        let state = self.lease_state.lock().unwrap();
        if !state.is_ready_for_serving() {
            return Err(Error::NotLeader(
                self.info.group_id,
                state.applied_term,
                state.leader_descriptor(),
            ));
        }
        let movement =
            state.move_shard_state.clone().filter(|state| state.get_shard_id() == shard_id);
        Ok((state.replica_state.term, movement))
    }

    async fn propose_shard_import(
        &self,
        term: u64,
        wb: WriteBatch,
        op: Option<Box<SyncOp>>,
    ) -> Result<()> {
        let batch = (!wb.is_empty()).then(|| WriteBatchRep { data: wb.data().to_owned() });
        self.raft_group.propose(EvalResult { batch, op }, Some(term))?.wait_result().await
    }

    pub async fn delete_chunks(&self, shard_id: u64, keys: &[(Vec<u8>, u64)]) -> Result<()> {
        if keys.is_empty() {
            return Ok(());
        }

        let _acl_guard = self.take_read_acl_guard().await;
        self.check_moving_shard_request_early(shard_id)?;
        self.delete_chunks_inner(shard_id, keys).await
    }

    async fn delete_chunks_inner(&self, shard_id: u64, keys: &[(Vec<u8>, u64)]) -> Result<()> {
        let mut wb = WriteBatch::default();
        for (key, version) in keys {
            self.group_engine.delete(&mut wb, shard_id, key, *version)?;
        }

        let eval_result =
            EvalResult { batch: Some(WriteBatchRep { data: wb.data().to_owned() }), op: None };
        self.raft_group.propose(eval_result, None)?.wait_result().await?;

        Ok(())
    }

    pub async fn setup_shard_moving(&self, desc: &MoveShardDesc) -> Result<()> {
        self.update_move_shard_state(desc, MoveShardEvent::Setup).await
    }

    pub async fn enter_pulling_step(&self, desc: &MoveShardDesc) -> Result<()> {
        self.update_move_shard_state(desc, MoveShardEvent::Ingest).await?;
        if let Some(barrier) = &self.cfg.testing_knobs.after_moving_shard_setup {
            barrier.on_reached().await;
        }
        Ok(())
    }

    pub async fn commit_shard_moving(&self, desc: &MoveShardDesc) -> Result<()> {
        self.update_move_shard_state(desc, MoveShardEvent::Commit).await
    }

    pub async fn abort_shard_moving(&self, desc: &MoveShardDesc) -> Result<()> {
        self.update_move_shard_state(desc, MoveShardEvent::Abort).await
    }

    pub async fn finish_shard_moving(&self, desc: &MoveShardDesc) -> Result<()> {
        self.update_move_shard_state(desc, MoveShardEvent::Apply).await
    }

    async fn update_move_shard_state(
        &self,
        desc: &MoveShardDesc,
        event: MoveShardEvent,
    ) -> Result<()> {
        debug!(
            "update moving shard state. replica={}, group={}, desc={}, event={:?}",
            self.info.replica_id, self.info.group_id, desc, event
        );

        // FIXME(walter) The write acl guard is need here, but if there exists txn
        // conflicts, the blocking time might be very long?
        let _guard = self.take_write_acl_guard().await;
        let term = self
            .on_leader("move shard state", true)
            .await?
            .ok_or_else(|| Error::GroupNotReady(self.info.group_id))?;
        if !self.check_move_shard_state_update_early(desc, event)? {
            return Ok(());
        }

        let eval_result =
            EvalResult { op: Some(SyncOp::move_shard(event, desc.clone())), ..Default::default() };
        self.raft_group.propose(eval_result, Some(term))?.wait_result().await?;

        Ok(())
    }

    pub fn check_moving_shard_request_early(&self, shard_id: u64) -> Result<()> {
        let lease_state = self.lease_state.lock().unwrap();
        if !lease_state.is_ready_for_serving() {
            Err(Error::NotLeader(
                self.info.group_id,
                lease_state.applied_term,
                lease_state.leader_descriptor(),
            ))
        } else if !lease_state.is_shard_in_moving(shard_id) {
            Err(Error::ShardNotFound(shard_id))
        } else {
            Ok(())
        }
    }

    fn check_move_shard_state_update_early(
        &self,
        desc: &MoveShardDesc,
        event: MoveShardEvent,
    ) -> Result<bool> {
        let group_id = self.info.group_id;

        let lease_state = self.lease_state.lock().unwrap();
        if !lease_state.is_ready_for_serving() {
            Err(Error::NotLeader(
                group_id,
                lease_state.applied_term,
                lease_state.leader_descriptor(),
            ))
        } else if matches!(event, MoveShardEvent::Setup) {
            Self::check_moving_shard_setup(self.info.as_ref(), &lease_state, desc)
        } else if matches!(event, MoveShardEvent::Commit) {
            Self::check_moving_shard_commit(self.info.as_ref(), &lease_state, desc)
        } else if lease_state.move_shard_state.is_none() {
            Err(Error::InvalidArgument("no such moving shard exists".to_owned()))
        } else if !lease_state.is_same_shard_moving(desc) {
            Err(Error::InvalidArgument("exists another moving shard task".to_owned()))
        } else {
            Ok(true)
        }
    }

    fn check_moving_shard_setup(
        info: &ReplicaInfo,
        lease_state: &LeaseState,
        desc: &MoveShardDesc,
    ) -> Result<bool> {
        let epoch = desc.src_group_epoch;
        if lease_state.has_joint_config() {
            Err(Error::ServiceIsBusy(BusyReason::PendingConfigChange))
        } else if epoch != lease_state.descriptor.epoch {
            // This moving needs to be rollback.
            Err(Error::EpochNotMatch(lease_state.descriptor.clone()))
        } else if lease_state.move_shard_state.is_none() {
            if lease_state.descriptor.shard(desc.get_shard_id()) == desc.shard_desc.as_ref() {
                Ok(true)
            } else {
                Err(Error::EpochNotMatch(lease_state.descriptor.clone()))
            }
        } else if !lease_state.is_same_shard_moving(desc) {
            // This moving needs to be rollback too, because the epoch will be bumped
            // once the former moving finished.
            Err(Error::EpochNotMatch(lease_state.descriptor.clone()))
        } else {
            info!(
                "the same moving shard task already exists. replica={}, group={}, desc={}",
                info.replica_id, info.group_id, desc
            );
            Ok(false)
        }
    }

    fn check_moving_shard_commit(
        info: &ReplicaInfo,
        lease_state: &LeaseState,
        desc: &MoveShardDesc,
    ) -> Result<bool> {
        if is_moving_shard_finished(info, desc, &lease_state.descriptor) {
            info!(
                "this moving shard has been committed, skip commit request. replica={}, group={}, desc={}",
                info.replica_id, info.group_id, desc
            );
            Ok(false)
        } else if lease_state.move_shard_state.is_none() || !lease_state.is_same_shard_moving(desc)
        {
            info!(
                "move shard state is {:?}, descriptor {:?}",
                lease_state.move_shard_state, lease_state.descriptor
            );
            Err(Error::InvalidArgument("no such moving shard task exists".to_owned()))
        } else if lease_state.move_shard_state.as_ref().unwrap().step == MoveShardStep::Moved as i32
        {
            info!(
                "this moving shard has been committed, skip commit request. replica={}, group={}, desc={}",
                info.replica_id, info.group_id, desc
            );
            Ok(false)
        } else {
            Ok(true)
        }
    }
}

fn is_moving_shard_finished(
    info: &ReplicaInfo,
    desc: &MoveShardDesc,
    descriptor: &GroupDesc,
) -> bool {
    let shard_desc = desc.shard_desc.as_ref().unwrap();
    if desc.src_group_id == info.group_id
        && desc.src_group_epoch < descriptor.epoch
        && is_shard_moved_out(shard_desc, descriptor)
    {
        return true;
    }

    if desc.dest_group_id == info.group_id
        && desc.dest_group_epoch < descriptor.epoch
        && is_shard_moved_in(shard_desc, descriptor)
    {
        return true;
    }

    false
}

fn is_shard_moved_out(shard_desc: &ShardDesc, group_desc: &GroupDesc) -> bool {
    // For source dest, if a shard is moved, the shard desc should not exists.
    for shard in &group_desc.shards {
        if shard.id == shard_desc.id {
            return false;
        }
    }
    true
}

fn is_shard_moved_in(shard_desc: &ShardDesc, group_desc: &GroupDesc) -> bool {
    // For dest dest, if a shard is moved, the shard desc should exists.
    for shard in &group_desc.shards {
        if shard.id == shard_desc.id {
            return true;
        }
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn source_acquire_waits_for_joint_configuration_to_finish() {
        let replica = ReplicaDesc { id: 1, node_id: 1, role: ReplicaRole::IncomingVoter as i32 };
        let info = ReplicaInfo::new(&replica, 1, ReplicaLocalState::Normal);
        let (sender, _) = futures::channel::mpsc::unbounded();
        let mut state = LeaseState::new(
            GroupDesc {
                id: 1,
                epoch: 8,
                replicas: vec![replica],
                shards: vec![ShardDesc::whole(1, 1024)],
                ..Default::default()
            },
            None,
            sender,
        );
        let desc = MoveShardDesc {
            src_group_id: 1,
            src_group_epoch: 8,
            dest_group_id: 2,
            shard_desc: Some(ShardDesc::whole(1, 1024)),
            ..Default::default()
        };
        assert!(matches!(
            Replica::check_moving_shard_setup(&info, &state, &desc),
            Err(Error::ServiceIsBusy(BusyReason::PendingConfigChange))
        ));
        state.descriptor.replicas[0].role = ReplicaRole::Voter as i32;
        assert!(Replica::check_moving_shard_setup(&info, &state, &desc).unwrap());
        state.descriptor.epoch += 1;
        assert!(matches!(
            Replica::check_moving_shard_setup(&info, &state, &desc),
            Err(Error::EpochNotMatch(_))
        ));
    }
}
