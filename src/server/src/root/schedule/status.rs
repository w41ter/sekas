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

use std::sync::Arc;

use super::ExecutionStatus;
use super::action::{Action, PlacementJob};
use crate::root::allocator::{BalanceDecision, BalanceThresholds};
use crate::{BalanceDimension, Error, RootConfig};

#[derive(Clone, Copy)]
pub enum Policy {
    GroupCount,
    Placement(BalanceDimension),
}

impl Policy {
    const ALL: [Self; BalanceDimension::ALL.len() + 1] = [
        Self::GroupCount,
        Self::Placement(BalanceDimension::ReplicaCount),
        Self::Placement(BalanceDimension::LeaderCount),
        Self::Placement(BalanceDimension::ShardCount),
        Self::Placement(BalanceDimension::ShardSize),
        Self::Placement(BalanceDimension::ReadUnits),
        Self::Placement(BalanceDimension::WriteUnits),
        Self::Placement(BalanceDimension::Cpu),
        Self::Placement(BalanceDimension::Memory),
        Self::Placement(BalanceDimension::Disk),
    ];

    pub fn name(self) -> &'static str {
        match self {
            Self::GroupCount => "group_count",
            Self::Placement(dimension) => dimension.name(),
        }
    }

    fn enabled(self, cfg: &RootConfig) -> bool {
        match self {
            Self::GroupCount => cfg.enable_group_balance,
            Self::Placement(dimension) => dimension.enabled(cfg),
        }
    }

    fn index(self) -> usize {
        match self {
            Self::GroupCount => 0,
            Self::Placement(dimension) => dimension as usize + 1,
        }
    }
}

#[derive(Clone)]
pub struct BalanceSnapshot {
    reports: [BalanceDecision; Policy::ALL.len()],
    thresholds: [Option<BalanceThresholds>; Policy::ALL.len()],
}

impl BalanceSnapshot {
    pub(super) fn new(cfg: &RootConfig) -> Self {
        let mut reports = std::array::from_fn(|_| BalanceDecision::Disabled);
        for policy in Policy::ALL {
            if policy.enabled(cfg) {
                reports[policy.index()] = BalanceDecision::Unknown;
            }
        }
        Self { reports, thresholds: [None; Policy::ALL.len()] }
    }

    pub fn iter(&self) -> impl Iterator<Item = (Policy, &BalanceDecision)> {
        Policy::ALL.into_iter().map(|policy| (policy, &self.reports[policy.index()]))
    }

    pub(super) fn record(&mut self, policy: Policy, decision: BalanceDecision) {
        self.reports[policy.index()] = decision;
    }

    pub(super) fn defer_ready_placements(&mut self) {
        for report in &mut self.reports {
            if matches!(report, BalanceDecision::ActionNeeded(_)) {
                *report = BalanceDecision::Deferred("placement concurrency limit reached".into());
            }
        }
    }

    pub fn thresholds(&self, policy: Policy) -> Option<BalanceThresholds> {
        self.thresholds[policy.index()]
    }

    pub(super) fn retain_thresholds(&mut self, previous: &Self) {
        for policy in Policy::ALL {
            let index = policy.index();
            if !matches!(self.reports[index], BalanceDecision::Disabled)
                && self.thresholds[index].is_none()
            {
                self.thresholds[index] = previous.thresholds[index];
            }
        }
    }

    pub(super) fn record_with_thresholds(
        &mut self,
        policy: Policy,
        decision: BalanceDecision,
        thresholds: Option<BalanceThresholds>,
    ) {
        self.record(policy, decision);
        self.thresholds[policy.index()] = thresholds;
    }

    pub(super) fn record_refresh_error(&mut self, err: &Error) {
        let decision = BalanceDecision::Error(format!("refresh cluster metadata: {err}"));
        for policy in Policy::ALL {
            if !matches!(self.reports[policy.index()], BalanceDecision::Disabled) {
                self.record(policy, decision.clone());
            }
        }
    }
}

#[derive(Default)]
pub struct SchedulerSummary {
    pub ready: usize,
    pub running: usize,
    pub waiting: usize,
}

#[derive(Clone)]
pub struct TaskReason {
    pub policy: Option<Policy>,
    pub detail: Arc<str>,
}

impl TaskReason {
    pub(super) fn new(detail: impl Into<Arc<str>>) -> Self {
        Self { policy: None, detail: detail.into() }
    }

    pub(super) fn for_balance(dimension: BalanceDimension, reason: String) -> Self {
        Self { policy: Some(Policy::Placement(dimension)), detail: reason.into() }
    }
}

#[derive(Clone)]
pub struct TaskStatus {
    pub kind: &'static str,
    pub target: String,
    pub reason: TaskReason,
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn incomplete_balance_round_keeps_last_thresholds_and_reports_current_decision() {
        let mut config = RootConfig { enable_node_request_balance: true, ..RootConfig::default() };
        let policy = Policy::Placement(BalanceDimension::ReadUnits);
        let mut previous = BalanceSnapshot::new(&config);
        let known =
            BalanceThresholds { avg: 100., low: 90., high: 110., max: 105., tolerance: 10. };
        previous.record_with_thresholds(
            policy,
            BalanceDecision::NoAction(String::new()),
            Some(known),
        );

        let mut current = BalanceSnapshot::new(&config);
        current.record_with_thresholds(
            policy,
            BalanceDecision::Deferred("waiting for samples".into()),
            None,
        );
        assert!(current.thresholds(policy).is_none());
        current.retain_thresholds(&previous);
        assert_eq!(current.thresholds(policy).unwrap().avg, 100.);
        assert!(matches!(current.reports[policy.index()], BalanceDecision::Deferred(_)));

        let updated = BalanceThresholds { avg: 120., ..known };
        current.record_with_thresholds(
            policy,
            BalanceDecision::NoAction(String::new()),
            Some(updated),
        );
        current.retain_thresholds(&previous);
        assert_eq!(current.thresholds(policy).unwrap().avg, 120.);

        config.enable_read_balance = false;
        let mut disabled = BalanceSnapshot::new(&config);
        disabled.retain_thresholds(&previous);
        assert!(disabled.thresholds(policy).is_none());
    }
}

impl SchedulerSummary {
    pub(super) fn from_tasks(tasks: &[TaskStatus], now: u64) -> Self {
        let mut summary = Self::default();
        for task in tasks {
            match task.state(now) {
                TaskState::Ready => summary.ready += 1,
                TaskState::Running => summary.running += 1,
                TaskState::Waiting => summary.waiting += 1,
            }
        }
        summary
    }
}

impl TaskStatus {
    pub(super) fn new(
        id: u64,
        task: &PlacementJob,
        action: &Action,
        status: Option<&ExecutionStatus>,
    ) -> Self {
        let (kind, target) = action.target();
        let policy = task.policy.as_deref().and_then(|policy| {
            if policy == Policy::GroupCount.name() {
                Some(Policy::GroupCount)
            } else {
                BalanceDimension::parse(policy).map(Policy::Placement)
            }
        });
        let detail = match status.and_then(|status| status.waiting.as_deref()) {
            Some(waiting) => format!("{}. {waiting}", task.reason).into(),
            None => task.reason.as_str().into(),
        };
        Self {
            kind,
            target: format!("job {id} {target}"),
            reason: TaskReason { policy, detail },
            created_at: task.created_at,
            fire_at: status.map_or(0, |status| status.fire_at),
            retry_count: status.map_or(0, |status| status.retry_count),
            running: status.is_some_and(|status| status.running),
        }
    }
}
