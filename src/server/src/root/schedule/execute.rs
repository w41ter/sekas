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

use prometheus::HistogramTimer;
use sekas_api::server::v1::group_request_union::Request;
use sekas_api::server::v1::group_response_union::Response;

use super::*;
use crate::schedule::{ReplacementStep, replacement_step};

impl Action {
    /// A shard move reserves both groups until ownership has settled.
    pub(super) fn groups(&self) -> [Option<u64>; 2] {
        match self {
            Self::MoveShard(action) => {
                [Some(action.desc.src_group_id), Some(action.desc.dest_group_id)]
            }
            Self::TransferLeader(action) => [Some(action.group), None],
            Self::MoveReplica(action) => [Some(action.group), None],
            Self::SplitShard(action) => [Some(action.group), None],
            Self::MergeShards(action) => [Some(action.group), None],
        }
    }

    pub(super) fn target(&self) -> (&'static str, String) {
        match self {
            Self::MoveShard(a) => (
                "migrate_shard",
                format!(
                    "shard {}: g{} -> g{}",
                    a.desc.get_shard_id(),
                    a.desc.src_group_id,
                    a.desc.dest_group_id
                ),
            ),
            Self::TransferLeader(a) => {
                ("transfer_leader", format!("group {}: r{} -> r{}", a.group, a.source, a.target))
            }
            Self::MoveReplica(a) => (
                "reallocate_replica",
                format!("group {}: r{} -> n{}", a.group, a.outgoing, a.target_node),
            ),
            Self::SplitShard(a) => {
                ("split_shard", format!("group {}, shard {}", a.group, a.shard.id))
            }
            Self::MergeShards(a) => {
                ("merge_shard", format!("group {}, shards {} + {}", a.group, a.left.id, a.right.id))
            }
        }
    }

    pub(super) fn record_attempt(&self) -> HistogramTimer {
        let count = &metrics::RECONCILE_HANDLE_TASK_TOTAL;
        let duration = &metrics::RECONCILE_HANDLE_TASK_DURATION_SECONDS;
        let (counter, histogram) = match self {
            Self::MoveShard(_) => (&count.migrate_shard, &duration.migrate_shard),
            Self::TransferLeader(_) => (&count.transfer_leader, &duration.transfer_leader),
            Self::MoveReplica(_) => (&count.reallocate_replica, &duration.reallocate_replica),
            Self::SplitShard(_) => (&count.split_shard, &duration.split_shard),
            Self::MergeShards(_) => (&count.merge_shard, &duration.merge_shard),
        };
        counter.inc();
        histogram.start_timer()
    }

    pub(super) fn record_retry(&self) {
        let count = &metrics::RECONCILE_RETRY_TASK_TOTAL;
        match self {
            Self::MoveShard(_) => &count.migrate_shard,
            Self::TransferLeader(_) => &count.transfer_leader,
            Self::MoveReplica(_) => &count.reallocate_replica,
            Self::SplitShard(_) => &count.split_shard,
            Self::MergeShards(_) => &count.merge_shard,
        }
        .inc();
    }
}

impl ScheduleContext {
    pub(super) async fn step<'a>(
        &self,
        job: &'a BackgroundJob,
        schema: &Schema,
        root_term: u64,
    ) -> (&'a BackgroundJob, Result<Step>) {
        let task = placement(job);
        let action = task.action.as_ref().expect("placement action");
        let _timer = action.record_attempt();
        let work = async {
            self.check_term(schema, root_term).await?;
            match action {
                Action::MoveShard(a) => self.move_shard(a, schema, root_term).await,
                Action::TransferLeader(a) => self.transfer(a, schema, root_term).await,
                Action::MoveReplica(a) => self.move_replica(a, schema, root_term).await,
                Action::SplitShard(a) => self.split(a, schema, root_term).await,
                Action::MergeShards(a) => self.merge(a, schema, root_term).await,
            }
        };
        let result = sekas_runtime::time::timeout(
            Duration::from_secs(self.cfg.heartbeat_timeout_sec.max(1)),
            work,
        )
        .await
        .unwrap_or_else(|_| Err(Error::DeadlineExceeded("placement step timed out".into())));
        (job, result)
    }

    async fn inspect(&self, group: u64) -> Result<InspectGroupResponse> {
        Ok(self.shared.transport_manager.lazy_group_client(group).inspect_group().await?)
    }

    async fn request_sample_ready(&self, inspected: &InspectGroupResponse) -> Result<bool> {
        if !self.cfg.enable_node_request_balance {
            return Ok(true);
        }
        let group = descriptor(inspected)?;
        let snapshot = self.cluster_stats.show_snapshot(
            std::slice::from_ref(group),
            Duration::from_secs(self.cfg.liveness_threshold_sec),
        );
        let units = snapshot.group_request_units(group);
        if snapshot.leader(group.id).is_some_and(|leader| {
            leader.replica_id == inspected.leader_id && leader.term == inspected.term
        }) && BalanceDimension::REQUESTS
            .into_iter()
            .all(|kind| !kind.enabled(&self.cfg) || units.get(kind).is_some())
        {
            return Ok(true);
        }
        if let Some(leader) = group.replicas.iter().find(|r| r.id == inspected.leader_id) {
            self.heartbeat_queue
                .try_schedule(vec![HeartbeatTask { node_id: leader.node_id }], Instant::now())
                .await;
        }
        Ok(false)
    }

    async fn submit(
        &self,
        schema: &Schema,
        root_term: u64,
        request: GroupRequest,
        expected_term: u64,
        inspected: &InspectGroupResponse,
    ) -> Result<Response> {
        self.check_term(schema, root_term).await?;
        let report_replica =
            match request.request.as_ref().and_then(|request| request.request.as_ref()) {
                Some(Request::GetSplitKey(_)) => None,
                Some(Request::Transfer(transfer)) => Some(transfer.transferee),
                _ => Some(inspected.leader_id),
            };
        let report_node = report_replica.and_then(|id| {
            inspected
                .group
                .as_ref()?
                .replicas
                .iter()
                .find(|replica| replica.id == id)
                .map(|replica| replica.node_id)
        });
        let result = self
            .shared
            .transport_manager
            .lazy_group_client(request.group_id)
            .execute_scheduled(request, expected_term)
            .await?;
        if let Some(node_id) = report_node {
            self.heartbeat_queue
                .try_schedule(vec![HeartbeatTask { node_id }], Instant::now())
                .await;
        }
        Ok(result)
    }

    async fn move_shard(&self, action: &MoveShard, schema: &Schema, term: u64) -> Result<Step> {
        let desc = &action.desc;
        let shard = desc.get_shard_desc();
        let version = GroupVersion { epoch: desc.dest_group_epoch, term: action.target_term };
        if self
            .cluster_stats
            .reported_migration(
                desc.dest_group_id,
                Duration::from_secs(self.cfg.liveness_threshold_sec),
            )
            .as_ref()
            == Some(desc)
        {
            return Ok(Step::Pending("Group is moving the shard"));
        }
        let target = self.inspect(desc.dest_group_id).await?;
        let group = descriptor(&target)?;
        if target.shard_migration.is_some() {
            return Ok(Step::Pending("Target group has an active shard migration"));
        }
        if group.shard(shard.id) == Some(shard) {
            let source = self.inspect(desc.src_group_id).await?;
            return Ok(
                if descriptor(&source)?.shard(shard.id).is_none()
                    && source.shard_migration.as_ref().and_then(|migration| migration.desc.as_ref())
                        != Some(desc)
                {
                    if self.request_sample_ready(&source).await?
                        && self.request_sample_ready(&target).await?
                    {
                        done("Shard ownership has changed")
                    } else {
                        Step::Pending("Waiting for request samples after shard migration")
                    }
                } else {
                    Step::Pending("Waiting for source to finish migration")
                },
            );
        }
        if !version.matches(&target) {
            return Ok(cancelled("Target admission version changed"));
        }
        // AcquireShard validates source ownership in the Group's migration
        // protocol. An expired source is handled there even after Root changes.
        let req = GroupRequest::accept_shard(
            desc.dest_group_id,
            version.epoch,
            desc.src_group_id,
            desc.src_group_epoch,
            shard,
        );
        self.submit(schema, term, req, version.term, &target).await?;
        Ok(Step::Pending("Waiting for both groups to report ownership"))
    }

    async fn transfer(&self, action: &TransferLeader, schema: &Schema, term: u64) -> Result<Step> {
        let state = self.inspect(action.group).await?;
        if state.leader_id == action.target {
            return Ok(
                if action.draining_node.is_some() || self.request_sample_ready(&state).await? {
                    done("Target is the current leader")
                } else {
                    Step::Pending("Waiting for request samples from the new leader")
                },
            );
        }
        let version = action.version;
        if state.leader_id != action.source || !version.matches(&state) {
            return Ok(cancelled("Group leadership or membership changed"));
        }
        let group = descriptor(&state)?;
        let Some(target) = group
            .replicas
            .iter()
            .find(|replica| replica.id == action.target && replica.role() == ReplicaRole::Voter)
        else {
            return Ok(cancelled("Target is no longer a voter"));
        };
        let target_active = schema
            .get_node(target.node_id)
            .await?
            .is_some_and(|node| node.status() == NodeStatus::Active);
        let drain_active = match action.draining_node {
            Some(node) => schema.get_node(node).await?.is_some_and(|node| {
                matches!(node.status(), NodeStatus::Draining | NodeStatus::Drained)
            }),
            None => true,
        };
        if !target_active || !drain_active {
            return Ok(Step::Pending("Waiting for original transfer admission version to expire"));
        }
        self.submit(
            schema,
            term,
            GroupRequest::transfer_leader(action.group, version.epoch, action.target),
            version.term,
            &state,
        )
        .await?;
        Ok(Step::Pending("Waiting for the Raft election"))
    }

    async fn split(&self, action: &SplitShard, schema: &Schema, term: u64) -> Result<Step> {
        let state = self.inspect(action.group).await?;
        let group = descriptor(&state)?;
        let shard = &action.shard;
        if split_applied(group, action) {
            if action.max_movable_split_bytes != 0 && !self.request_sample_ready(&state).await? {
                return Ok(Step::Pending("Waiting for request samples after a hot shard split"));
            }
            return Ok(done("Split ranges applied"));
        }
        if group.epoch != action.epoch
            || group.shard(shard.id) != Some(shard)
            || action.prepared.as_ref().is_some_and(|prepared| prepared.term != state.term)
        {
            return Ok(cancelled("Split admission version changed"));
        }
        if state.shard_migration.is_some() {
            return Ok(Step::Pending("Group is moving a shard"));
        }
        let Some(prepared) = &action.prepared else {
            let start = action.target_bytes.map(|_| sekas_schema::shard::start_key(shard).to_vec());
            let mut req = GroupRequest::get_split_key(
                group.id,
                action.epoch,
                shard.id,
                start,
                action.target_bytes,
            );
            if let Some(Request::GetSplitKey(request)) =
                req.request.as_mut().and_then(|r| r.request.as_mut())
            {
                request.min_split_side_bytes = self.cfg.min_split_side_bytes();
                request.max_movable_split_bytes = action.max_movable_split_bytes;
            }
            let response = self.submit(schema, term, req, state.term, &state).await?;
            let Response::GetSplitKey(response) = response else {
                return Err(Error::InvalidData("missing split key response".into()));
            };
            let Some(key) = response.split_key else {
                self.cluster_stats.defer_shard_split(
                    shard.id,
                    Duration::from_secs(self.cfg.liveness_threshold_sec.max(1)),
                );
                return Ok(Step::Finished(OutcomeKind::Failed, "Shard has no split point"));
            };
            let mut action = action.clone();
            action.prepared = Some(PreparedSplit {
                new_shard: schema.next_shard_id().await?,
                key,
                term: state.term,
            });
            return Ok(Step::Prepared(Action::SplitShard(action)));
        };
        let version = GroupVersion { epoch: action.epoch, term: prepared.term };
        let req = GroupRequest::split_shard(
            group.id,
            version.epoch,
            shard.id,
            prepared.new_shard,
            Some(prepared.key.clone()),
        );
        self.submit(schema, term, req, version.term, &state).await?;
        Ok(Step::Pending("Waiting for the split descriptor"))
    }

    async fn merge(&self, action: &MergeShards, schema: &Schema, term: u64) -> Result<Step> {
        let left = &action.left;
        let right = &action.right;
        let state = self.inspect(action.group).await?;
        let group = descriptor(&state)?;
        if merge_applied(group, left, right) {
            return Ok(done("Merged range applied"));
        }
        let version = action.version;
        if !version.matches(&state)
            || group.shard(left.id) != Some(left)
            || group.shard(right.id) != Some(right)
        {
            return Ok(cancelled("Merge admission version changed"));
        }
        if state.shard_migration.is_some() {
            return Ok(Step::Pending("Group is moving a shard"));
        }
        self.submit(
            schema,
            term,
            GroupRequest::merge_shard(group.id, version.epoch, left.id, right.id),
            version.term,
            &state,
        )
        .await?;
        Ok(Step::Pending("Waiting for the merged descriptor"))
    }

    async fn move_replica(&self, action: &MoveReplica, schema: &Schema, term: u64) -> Result<Step> {
        let state = self.inspect(action.group).await?;
        let group = descriptor(&state)?;
        let target_active = schema
            .get_node(action.target_node)
            .await?
            .is_some_and(|node| node.status() == NodeStatus::Active);
        let Some(learner) = action.learner else {
            if !target_active {
                return Ok(cancelled("Replica replacement target is no longer active"));
            }
            let action = MoveReplica {
                learner: Some(LearnerAdmission {
                    id: schema.next_replica_id().await?,
                    epoch: group.epoch,
                }),
                ..*action
            };
            return Ok(Step::Prepared(Action::MoveReplica(action)));
        };
        let intent = ReplicaReplacement {
            incoming_id: learner.id,
            outgoing_id: action.outgoing,
            target_node: action.target_node,
            add_epoch: learner.epoch,
        };
        match replacement_step(group, intent, target_active)? {
            ReplacementStep::Complete => return Ok(done("Replica membership has changed")),
            ReplacementStep::Cancel => {
                return Ok(cancelled("Replica replacement is no longer eligible"));
            }
            ReplacementStep::Wait => {
                return Ok(Step::Pending("Waiting for Group membership to change"));
            }
            _ => {}
        }
        let request = GroupRequest {
            group_id: group.id,
            epoch: group.epoch,
            expected_term: None,
            request: Some(GroupRequestUnion {
                request: Some(Request::MoveReplicas(MoveReplicasRequest {
                    replacement: Some(intent),
                    replacement_target_active: target_active,
                    ..Default::default()
                })),
            }),
        };
        self.submit(schema, term, request, state.term, &state).await?;
        Ok(Step::Pending("Waiting for Group membership to change"))
    }
}

impl GroupVersion {
    fn matches(self, state: &InspectGroupResponse) -> bool {
        self.term == state.term
            && state.group.as_ref().is_some_and(|group| self.epoch == group.epoch)
    }
}

fn descriptor(state: &InspectGroupResponse) -> Result<&GroupDesc> {
    state
        .group
        .as_ref()
        .ok_or_else(|| Error::InvalidData("missing group inspection descriptor".into()))
}

fn done(detail: &'static str) -> Step {
    Step::Finished(OutcomeKind::Succeeded, detail)
}

fn cancelled(detail: &'static str) -> Step {
    Step::Finished(OutcomeKind::Cancelled, detail)
}

fn split_applied(group: &GroupDesc, action: &SplitShard) -> bool {
    let shard = &action.shard;
    let (Some(old), Some(prepared)) = (&shard.range, &action.prepared) else {
        return false;
    };
    let key = &prepared.key;
    let old_part = group.shard(shard.id);
    let new_part = group.shard(prepared.new_shard);
    old_part.zip(new_part).is_some_and(|(left, right)| {
        left.table_id == shard.table_id
            && right.table_id == shard.table_id
            && left.range.as_ref().zip(right.range.as_ref()).is_some_and(|(left, right)| {
                (left.start == old.start
                    && &left.end == key
                    && &right.start == key
                    && right.end == old.end)
                    || (right.start == old.start
                        && &right.end == key
                        && &left.start == key
                        && left.end == old.end)
            })
    })
}

fn merge_applied(group: &GroupDesc, left: &ShardDesc, right: &ShardDesc) -> bool {
    group.shard(right.id).is_none()
        && group.shard(left.id).is_some_and(|merged| {
            merged.table_id == left.table_id
                && left
                    .range
                    .as_ref()
                    .zip(right.range.as_ref())
                    .zip(merged.range.as_ref())
                    .is_some_and(|((l, r), m)| m.start == l.start && m.end == r.end)
        })
}
