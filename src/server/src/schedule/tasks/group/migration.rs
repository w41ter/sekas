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
use std::time::{Duration, Instant};

use log::debug;
use sekas_api::server::v1::group_request_union::Request;
use sekas_api::server::v1::*;

use super::{ActionTaskWithLocks, GroupLocks};
use crate::error::BusyReason;
use crate::replica::{ExecCtx, Replica};
use crate::schedule::actions::*;
use crate::schedule::event_source::EventSource;
use crate::schedule::provider::GroupProviders;
use crate::schedule::replacement::{ReplacementStep, replacement_step};
use crate::schedule::scheduler::ScheduleContext;
use crate::schedule::task::{Task, TaskState};
use crate::schedule::tasks::{ActionTask, REPLICA_MIGRATION_TASK_ID};
use crate::{Error, Result};

// Leave autonomous repair available if Root stops driving a replacement.
const REPLACEMENT_IDLE_TIMEOUT: Duration = Duration::from_secs(15);
const CREATE_REPLICA_TIMEOUT: Duration = Duration::from_secs(2);

pub struct ReplicaMigration {
    providers: Arc<GroupProviders>,
    replacement: Option<ActiveReplacement>,
}

struct ActiveReplacement {
    intent: ReplicaReplacement,
    locks: GroupLocks,
    idle_at: Instant,
}

impl ReplicaMigration {
    pub fn new(providers: Arc<GroupProviders>) -> Self {
        Self { providers, replacement: None }
    }

    fn release_replacement(&mut self, ctx: &mut ScheduleContext<'_>) {
        if let Some(active) = self.replacement.take() {
            ctx.group_lock_table.release(active.locks);
        }
    }

    fn schedule_move(
        &self,
        ctx: &mut ScheduleContext<'_>,
        group: &GroupDesc,
        incoming: Vec<ReplicaDesc>,
        outgoing: Vec<ReplicaDesc>,
    ) -> Result<()> {
        let task_id = ctx.next_task_id();
        let peers =
            group.replicas.iter().chain(&incoming).map(|replica| replica.id).collect::<Vec<_>>();
        let locks = ctx
            .group_lock_table
            .config_change(task_id, group.epoch, &peers, &incoming, &outgoing)
            .ok_or_else(|| Error::AlreadyExists("config change".to_owned()))?;

        let enters_joint = incoming.len() + outgoing.len() > 1;
        let mut actions: Vec<Box<dyn Action>> = Vec::new();
        if !incoming.is_empty() {
            actions.push(Box::new(CreateReplicas::new(incoming.clone())));
            actions.push(Box::new(AddLearners {
                providers: self.providers.clone(),
                learners: incoming.clone(),
            }));
        }
        actions.push(Box::new(ReplaceVoters {
            providers: self.providers.clone(),
            incoming_voters: incoming,
            demoting_voters: outgoing.clone(),
        }));
        if enters_joint && !outgoing.is_empty() {
            actions.push(Box::new(RemoveLearners {
                providers: self.providers.clone(),
                learners: outgoing,
            }));
        }
        let task = ActionTask::new(task_id, actions);
        ctx.delegate(Box::new(ActionTaskWithLocks::new(locks, task)));
        Ok(())
    }

    async fn replace_replica(
        &mut self,
        ctx: &mut ScheduleContext<'_>,
        group: &GroupDesc,
        intent: ReplicaReplacement,
        target_active: bool,
    ) -> Result<()> {
        use ReplacementStep::*;
        if self.replacement.as_ref().is_some_and(|active| active.intent != intent) {
            return Ok(());
        }
        let step = replacement_step(group, intent, target_active)?;
        if matches!(step, Wait | Complete | Cancel) {
            self.release_replacement(ctx);
            return Ok(());
        }
        if matches!(step, AddLearner | CatchUpAndReplace) {
            let lost = self.providers.raft_state.lost_peers();
            if group
                .replicas
                .iter()
                .any(|replica| replica.role() == ReplicaRole::Voter && lost.contains(&replica.id))
            {
                self.release_replacement(ctx);
                return Ok(());
            }
        }

        if !self.reserve_replacement(ctx, group, intent) {
            return Ok(());
        }
        let replica = ctx.replica.as_ref();
        let epoch = group.epoch;
        let term = ctx.current_term;
        let changes = match step {
            LeaveJoint => vec![],
            RemoveLearner(id) => vec![change(ChangeReplicaType::Remove, id, 0)],
            AddLearner => {
                // Membership reserves the ID before the target Node creates it.
                vec![change(ChangeReplicaType::AddLearner, intent.incoming_id, intent.target_node)]
            }
            CatchUpAndReplace => return self.catch_up_and_replace(ctx, group, intent).await,
            Wait | Complete | Cancel => unreachable!("no membership change"),
        };
        change_replicas(replica, epoch, term, changes).await
    }

    fn reserve_replacement(
        &mut self,
        ctx: &mut ScheduleContext<'_>,
        group: &GroupDesc,
        intent: ReplicaReplacement,
    ) -> bool {
        let idle_at = Instant::now() + REPLACEMENT_IDLE_TIMEOUT;
        if let Some(active) = &mut self.replacement {
            active.idle_at = idle_at;
            return true;
        }
        let task_id = ctx.next_task_id();
        let incoming = ReplicaDesc {
            id: intent.incoming_id,
            node_id: intent.target_node,
            role: ReplicaRole::Learner.into(),
        };
        let outgoing =
            group.replicas.iter().find(|replica| replica.id == intent.outgoing_id).copied();
        let peers = group
            .replicas
            .iter()
            .map(|replica| replica.id)
            .chain([incoming.id])
            .collect::<Vec<_>>();
        let Some(locks) = ctx.group_lock_table.config_change(
            task_id,
            group.epoch,
            &peers,
            &[incoming],
            outgoing.as_slice(),
        ) else {
            return false;
        };
        self.replacement = Some(ActiveReplacement { intent, locks, idle_at });
        true
    }

    async fn catch_up_and_replace(
        &self,
        ctx: &mut ScheduleContext<'_>,
        group: &GroupDesc,
        intent: ReplicaReplacement,
    ) -> Result<()> {
        let replica = ctx.replica.as_ref();
        let term = ctx.current_term;
        let raft = replica
            .raft_node()
            .raft_group_state()
            .await
            .filter(|raft| raft.hs.term == term)
            .ok_or(Error::GroupNotReady(ctx.group_id))?;
        let caught_up = raft
            .peers
            .get(&intent.incoming_id)
            .is_some_and(|peer| !peer.might_lost && peer.matched >= raft.committed);
        if !caught_up {
            if replica.on_leader("creating replacement learner", true).await? != Some(term) {
                return Err(Error::GroupNotReady(ctx.group_id));
            }
            let node = ctx.transport_manager.find_node_client(intent.target_node)?;
            sekas_runtime::time::timeout(
                CREATE_REPLICA_TIMEOUT,
                node.create_replica(
                    intent.incoming_id,
                    GroupDesc { id: group.id, ..Default::default() },
                ),
            )
            .await
            .map_err(|_| Error::DeadlineExceeded("creating replacement learner".into()))?
            .map_err(sekas_client::Error::Rpc)?;
        } else if ctx.replica_id == intent.outgoing_id {
            if let Some(target) = group
                .replicas
                .iter()
                .filter(|replica| {
                    replica.id != intent.outgoing_id && replica.role() == ReplicaRole::Voter
                })
                .min_by_key(|replica| replica.id)
            {
                execute_membership_step(
                    replica,
                    group.epoch,
                    term,
                    Request::Transfer(TransferRequest { transferee: target.id }),
                )
                .await?;
            }
        } else {
            change_replicas(
                replica,
                group.epoch,
                term,
                vec![
                    change(ChangeReplicaType::Add, intent.incoming_id, intent.target_node),
                    change(ChangeReplicaType::Remove, intent.outgoing_id, 0),
                ],
            )
            .await?;
        }
        Ok(())
    }
}

#[crate::async_trait]
impl Task for ReplicaMigration {
    fn id(&self) -> u64 {
        REPLICA_MIGRATION_TASK_ID
    }

    async fn poll(&mut self, ctx: &mut ScheduleContext<'_>) -> TaskState {
        let command = self.providers.move_replicas.take_and_watch(self.id());
        if self.replacement.is_none() && command.is_none() {
            return TaskState::Pending(None);
        }
        self.providers.descriptor.watch(self.id());
        let group = ctx.replica.descriptor();
        if self.replacement.as_ref().is_some_and(|active| {
            active.idle_at <= Instant::now()
                || matches!(
                    replacement_step(&group, active.intent, true),
                    Ok(ReplacementStep::Wait | ReplacementStep::Complete | ReplacementStep::Cancel)
                )
        }) {
            self.release_replacement(ctx);
        }
        if let Some(command) = command {
            let result = if command.term != ctx.current_term {
                Err(Error::GroupNotReady(ctx.group_id))
            } else if command.epoch != group.epoch {
                Err(Error::EpochNotMatch(group))
            } else if ctx.replica.move_shard_state().is_some() {
                Err(Error::ServiceIsBusy(BusyReason::Moving))
            } else if let Some(replacement) = command.request.replacement {
                self.replace_replica(
                    ctx,
                    &group,
                    replacement,
                    command.request.replacement_target_active,
                )
                .await
            } else {
                self.schedule_move(
                    ctx,
                    &group,
                    command.request.incoming_voters,
                    command.request.outgoing_voters,
                )
            };
            if let Err(err) = result {
                debug!("group {} cannot advance replica move: {err}", ctx.group_id);
            }
        }

        TaskState::Pending(
            self.replacement
                .as_ref()
                .map(|active| active.idle_at.saturating_duration_since(Instant::now())),
        )
    }
}

async fn execute_membership_step(
    replica: &Replica,
    epoch: u64,
    term: u64,
    request: Request,
) -> Result<()> {
    let mut exec = ExecCtx::with_epoch(epoch);
    exec.expected_term = Some(term);
    // The submitting RPC may still hold a metadata read lock while returning.
    replica.execute(&mut exec, &request).await?;
    Ok(())
}

async fn change_replicas(
    replica: &Replica,
    epoch: u64,
    term: u64,
    changes: Vec<ChangeReplica>,
) -> Result<()> {
    let request = Request::ChangeReplicas(ChangeReplicasRequest {
        change_replicas: Some(ChangeReplicas { changes }),
    });
    execute_membership_step(replica, epoch, term, request).await
}

fn change(kind: ChangeReplicaType, replica_id: u64, node_id: u64) -> ChangeReplica {
    ChangeReplica { change_type: kind as i32, replica_id, node_id }
}
