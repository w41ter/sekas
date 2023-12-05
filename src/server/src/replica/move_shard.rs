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

use super::eval::{ingest_value_set, LatchManager};
use super::{LeaseState, Replica, ReplicaInfo};
use crate::engine::WriteBatch;
use crate::serverpb::v1::*;
use crate::{Error, Result};

impl Replica {
    /// Ingest value set of a key if it not exists before.
    pub async fn ingest_value_set(&self, shard_id: u64, value_set: &ValueSet) -> Result<()> {
        let _acl_guard = self.take_read_acl_guard().await;
        self.check_moving_shard_request_early(shard_id)?;

        let _latch_guard = self.latch_mgr.acquire(shard_id, &value_set.user_key).await?;
        let eval_result = match ingest_value_set(&self.group_engine, shard_id, value_set).await? {
            Some(eval_result) => eval_result,
            None => return Ok(()),
        };
        self.raft_group.propose(eval_result).await?;

        Ok(())
    }

    /// Save the ingestion progress to support fast recovery.
    pub async fn save_ingest_progress(&self, shard_id: u64, user_key: &[u8]) -> Result<()> {
        let _acl_guard = self.take_read_acl_guard().await;
        self.check_moving_shard_request_early(shard_id)?;
        let eval_result =
            EvalResult { op: Some(SyncOp::ingest(user_key.to_vec())), ..Default::default() };
        self.raft_group.propose(eval_result).await?;
        Ok(())
    }

    pub async fn delete_chunks(&self, shard_id: u64, keys: &[(Vec<u8>, u64)]) -> Result<()> {
        if keys.is_empty() {
            return Ok(());
        }

        let _acl_guard = self.take_read_acl_guard().await;
        self.check_moving_shard_request_early(shard_id)?;

        let mut wb = WriteBatch::default();
        for (key, version) in keys {
            self.group_engine.delete(&mut wb, shard_id, key, *version)?;
        }

        let eval_result =
            EvalResult { batch: Some(WriteBatchRep { data: wb.data().to_owned() }), op: None };
        self.raft_group.propose(eval_result).await?;

        Ok(())
    }

    pub async fn setup_shard_moving(&self, desc: &MoveShardDesc) -> Result<()> {
        self.update_move_shard_state(desc, MoveShardEvent::Setup).await
    }

    pub async fn enter_pulling_step(&self, desc: &MoveShardDesc) -> Result<()> {
        self.update_move_shard_state(desc, MoveShardEvent::Ingest).await
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

        let _guard = self.take_write_acl_guard().await;
        if !self.check_move_shard_state_update_early(desc, event)? {
            return Ok(());
        }

        let eval_result =
            EvalResult { op: Some(SyncOp::move_shard(event, desc.clone())), ..Default::default() };
        self.raft_group.propose(eval_result).await?;

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
        if epoch < lease_state.descriptor.epoch {
            // This moving needs to be rollback.
            Err(Error::EpochNotMatch(lease_state.descriptor.clone()))
        } else if lease_state.move_shard_state.is_none() {
            debug_assert_eq!(epoch, lease_state.descriptor.epoch);
            Ok(true)
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
                    info.replica_id, info.group_id, desc);
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
                info.replica_id, info.group_id, desc);
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
