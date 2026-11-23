// Copyright 2024-present The Sekas Authors.
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

use std::collections::BTreeMap;

use super::ReconcileTask;
use super::reconcile_task::Task;
use crate::root::allocator::BalanceDecision;
use crate::{Error, RootConfig};

#[derive(Clone, Copy)]
pub enum Policy {
    GroupCount,
    ReplicaCount,
    LeaderCount,
    ShardCount,
}

impl Policy {
    const ALL: [Self; 4] =
        [Self::GroupCount, Self::ReplicaCount, Self::LeaderCount, Self::ShardCount];

    pub fn name(self) -> &'static str {
        match self {
            Self::GroupCount => "group_count",
            Self::ReplicaCount => "replica_count",
            Self::LeaderCount => "leader_count",
            Self::ShardCount => "shard_count",
        }
    }

    fn enabled(self, cfg: &RootConfig) -> bool {
        match self {
            Self::GroupCount => cfg.enable_group_balance,
            Self::ReplicaCount => cfg.enable_replica_balance,
            Self::LeaderCount => cfg.enable_leader_balance,
            Self::ShardCount => cfg.enable_shard_balance,
        }
    }
}

#[derive(Clone)]
pub struct BalanceSnapshot {
    reports: [BalanceDecision; Policy::ALL.len()],
}

impl BalanceSnapshot {
    pub(super) fn new(cfg: &RootConfig) -> Self {
        let reports = Policy::ALL.map(|policy| {
            if policy.enabled(cfg) { BalanceDecision::Unknown } else { BalanceDecision::Disabled }
        });
        Self { reports }
    }

    pub fn iter(&self) -> impl Iterator<Item = (Policy, &BalanceDecision)> {
        Policy::ALL.into_iter().zip(&self.reports)
    }

    pub(super) fn record(&mut self, policy: Policy, decision: BalanceDecision) {
        self.reports[policy as usize] = decision;
    }

    pub(super) fn record_refresh_error(&mut self, err: &Error) {
        let decision = BalanceDecision::Error(format!("refresh cluster metadata: {err}"));
        for policy in Policy::ALL {
            if !matches!(self.reports[policy as usize], BalanceDecision::Disabled) {
                self.record(policy, decision.clone());
            }
        }
    }

    pub(super) fn defer(&mut self, reason: &'static str) {
        for report in &mut self.reports {
            if matches!(report, BalanceDecision::Unknown) {
                *report = BalanceDecision::Deferred(reason);
            }
        }
    }
}

#[derive(Default)]
pub(super) struct QueueStatus {
    next_id: u64,
    pub(super) tasks: BTreeMap<u64, TaskStatus>,
}

#[derive(Default)]
pub struct SchedulerSummary {
    pub ready: usize,
    pub running: usize,
    pub waiting: usize,
}

#[derive(Clone)]
pub struct TaskStatus {
    pub kind: &'static str,
    pub target: String,
    pub created_at: u64,
    pub fire_at: u64,
    pub retry_count: u32,
    running: bool,
}

#[derive(Clone, Copy)]
pub enum TaskState {
    Ready,
    Running,
    Waiting,
}

impl TaskState {
    pub fn name(self) -> &'static str {
        match self {
            Self::Ready => "ready",
            Self::Running => "running",
            Self::Waiting => "waiting",
        }
    }
}

impl TaskStatus {
    pub fn state(&self, now: u64) -> TaskState {
        if self.running {
            TaskState::Running
        } else if now < self.fire_at {
            TaskState::Waiting
        } else {
            TaskState::Ready
        }
    }
}

impl QueueStatus {
    pub(super) fn insert(&mut self, task: &ReconcileTask) -> u64 {
        let (kind, target) = match task.task.as_ref().unwrap() {
            Task::ReallocateReplica(t) => {
                let dest = t
                    .dest_node
                    .as_ref()
                    .map_or_else(|| "?".to_owned(), |node| format!("n{}", node.id));
                (
                    "reallocate_replica",
                    format!(
                        "group {}, replica {}: n{} -> {dest}",
                        t.group, t.src_replica, t.src_node
                    ),
                )
            }
            Task::MigrateShard(t) => (
                "migrate_shard",
                format!("shard {}: g{} -> g{}", t.shard, t.src_group, t.dest_group),
            ),
            Task::TransferGroupLeader(t) => (
                "transfer_leader",
                format!("group {}: n{} -> n{}", t.group, t.src_node, t.dest_node),
            ),
            Task::ShedLeader(t) => ("shed_group_leaders", format!("node {}", t.node_id)),
            Task::ShedRoot(t) => ("shed_root_leader", format!("node {}", t.node_id)),
            Task::SplitShard(t) => {
                ("split_shard", format!("group {}, shard {}", t.group_id, t.shard_id))
            }
            Task::MergeShard(t) => (
                "merge_shard",
                format!("group {}, shards {} + {}", t.group_id, t.left_shard_id, t.right_shard_id),
            ),
        };
        let id = self.next_id;
        self.next_id += 1;
        self.tasks.insert(
            id,
            TaskStatus {
                kind,
                target,
                created_at: task.created_at,
                fire_at: task.fire_at,
                retry_count: task.retry_count,
                running: false,
            },
        );
        id
    }

    pub(super) fn start_attempt(&mut self, id: u64) {
        self.tasks.get_mut(&id).unwrap().running = true;
    }

    pub(super) fn finish_attempt(&mut self, id: u64, task: &ReconcileTask) {
        let status = self.tasks.get_mut(&id).unwrap();
        status.running = false;
        status.fire_at = task.fire_at;
        status.retry_count = task.retry_count;
    }

    pub(super) fn summary(&self, now: u64) -> SchedulerSummary {
        let mut summary = SchedulerSummary::default();
        for task in self.tasks.values() {
            match task.state(now) {
                TaskState::Ready => summary.ready += 1,
                TaskState::Running => summary.running += 1,
                TaskState::Waiting => summary.waiting += 1,
            }
        }
        summary
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;
    use crate::root::schedule::ShedLeaderTask;

    #[test]
    fn queue_status_tracks_running_and_delayed_attempts() {
        let mut status = QueueStatus::default();
        let mut task = ReconcileTask {
            created_at: 1000,
            task: Some(Task::ShedLeader(ShedLeaderTask { node_id: 42 })),
            ..Default::default()
        };
        let id = status.insert(&task);
        assert_eq!(status.summary(1000).ready, 1);
        status.start_attempt(id);
        assert_eq!(status.summary(1000).running, 1);

        task.defer_retry(1100, None);
        status.finish_attempt(id, &task);
        assert_eq!(status.tasks[&id].retry_count, 1);
        assert_eq!(status.summary(2099).waiting, 1);
        assert_eq!(status.summary(2100).ready, 1);

        status.start_attempt(id);
        task.defer_retry(2100, Some(Duration::from_secs(1)));
        status.finish_attempt(id, &task);
        assert_eq!(status.tasks[&id].retry_count, 0);
        assert_eq!(status.summary(2100).waiting, 1);
        status.tasks.remove(&id);
        assert_eq!(status.summary(2100).waiting, 0);
    }
}
