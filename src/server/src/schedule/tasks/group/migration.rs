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

use std::collections::HashSet;
use std::sync::Arc;

use log::debug;
use sekas_api::server::v1::ReplicaDesc;

use super::ActionTaskWithLocks;
use crate::Error;
use crate::schedule::actions::*;
use crate::schedule::event_source::EventSource;
use crate::schedule::provider::GroupProviders;
use crate::schedule::scheduler::ScheduleContext;
use crate::schedule::task::{Task, TaskState};
use crate::schedule::tasks::{ActionTask, REPLICA_MIGRATION_TASK_ID};

pub struct ReplicaMigration {
    providers: Arc<GroupProviders>,
}

impl ReplicaMigration {
    pub fn new(providers: Arc<GroupProviders>) -> Self {
        ReplicaMigration { providers }
    }
}

#[crate::async_trait]
impl Task for ReplicaMigration {
    fn id(&self) -> u64 {
        REPLICA_MIGRATION_TASK_ID
    }

    async fn poll(&mut self, ctx: &mut ScheduleContext<'_>) -> TaskState {
        if let Some(move_replicas) = self.providers.move_replicas.take() {
            let replicas = self.providers.descriptor.replicas();
            if let Err(e) = validate_move_replicas(
                &replicas,
                &move_replicas.incoming_replicas,
                &move_replicas.outgoing_replicas,
            ) {
                move_replicas.sender.send(Err(e)).unwrap_or_default();
                self.providers.move_replicas.watch(self.id());
                return TaskState::Pending(None);
            }

            let mut peers = replicas.iter().map(|v| v.id).collect::<Vec<_>>();
            peers.extend(move_replicas.incoming_replicas.iter().map(|v| v.id));
            let task_id = ctx.next_task_id();
            if let Some(locks) = ctx.group_lock_table.config_change(
                task_id,
                move_replicas.epoch,
                &peers,
                &move_replicas.incoming_replicas,
                &move_replicas.outgoing_replicas,
            ) {
                let incoming_replicas = move_replicas.incoming_replicas.clone();
                let outgoing_replicas = move_replicas.outgoing_replicas.clone();
                let enters_joint = incoming_replicas.len() + outgoing_replicas.len() > 1;
                let mut actions: Vec<Box<dyn Action>> = Vec::new();
                if !incoming_replicas.is_empty() {
                    actions.push(Box::new(CreateReplicas::new(incoming_replicas.clone())));
                    actions.push(Box::new(AddLearners {
                        providers: self.providers.clone(),
                        learners: incoming_replicas.clone(),
                    }));
                }
                actions.push(Box::new(ReplaceVoters {
                    providers: self.providers.clone(),
                    incoming_voters: incoming_replicas,
                    demoting_voters: outgoing_replicas.clone(),
                }));
                if enters_joint && !outgoing_replicas.is_empty() {
                    actions.push(Box::new(RemoveLearners {
                        providers: self.providers.clone(),
                        learners: outgoing_replicas,
                    }));
                }
                let action_task = ActionTask::new(task_id, actions);
                ctx.delegate(Box::new(ActionTaskWithLocks::new(locks, action_task)));
                move_replicas.sender.send(Ok(())).unwrap_or_default();
            } else {
                debug!(
                    "group {} replica {} reject moving replicas requests because config change already exists",
                    ctx.group_id, ctx.replica_id
                );
                move_replicas
                    .sender
                    .send(Err(Error::AlreadyExists("config change".to_owned())))
                    .unwrap_or_default();
            }
        }

        self.providers.move_replicas.watch(self.id());
        TaskState::Pending(None)
    }
}

fn validate_move_replicas(
    current_replicas: &[ReplicaDesc],
    incoming_replicas: &[ReplicaDesc],
    outgoing_replicas: &[ReplicaDesc],
) -> Result<(), Error> {
    if incoming_replicas.is_empty() && outgoing_replicas.is_empty() {
        return Err(Error::InvalidArgument("empty MoveReplicas request".to_owned()));
    }

    let current_ids = current_replicas.iter().map(|r| r.id).collect::<HashSet<_>>();
    let current_nodes = current_replicas.iter().map(|r| r.node_id).collect::<HashSet<_>>();
    let outgoing_ids = outgoing_replicas.iter().map(|r| r.id).collect::<HashSet<_>>();
    let mut incoming_ids = HashSet::new();
    let mut incoming_nodes = HashSet::new();
    let mut seen_outgoing_ids = HashSet::new();

    for replica in incoming_replicas {
        if !incoming_ids.insert(replica.id) {
            return Err(Error::InvalidArgument(format!(
                "duplicated incoming replica {}",
                replica.id
            )));
        }
        if !incoming_nodes.insert(replica.node_id) {
            return Err(Error::InvalidArgument(format!(
                "duplicated incoming node {}",
                replica.node_id
            )));
        }
        if current_ids.contains(&replica.id) || outgoing_ids.contains(&replica.id) {
            return Err(Error::AlreadyExists(format!("replica {}", replica.id)));
        }
        if current_nodes.contains(&replica.node_id) {
            return Err(Error::AlreadyExists(format!("replica on node {}", replica.node_id)));
        }
    }

    for replica in outgoing_replicas {
        if !seen_outgoing_ids.insert(replica.id) {
            return Err(Error::InvalidArgument(format!(
                "duplicated outgoing replica {}",
                replica.id
            )));
        }
        if !current_ids.contains(&replica.id) {
            return Err(Error::InvalidArgument(format!("replica {} not found", replica.id)));
        }
    }

    Ok(())
}
