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

pub(super) mod action;
mod execute;
mod status;
mod task;

use std::sync::Mutex as SyncMutex;

use futures::stream::{FuturesUnordered, StreamExt};
use log::warn;
use sekas_api::server::v1::*;
use sekas_rock::time::timestamp_millis;
use sekas_runtime::sync::{AsyncMutex, Notify};

pub(super) use self::action::{Action, PlacementJob};
use self::action::{
    GroupVersion, LearnerAdmission, MergeShards, MoveReplica, MoveShard, Outcome, OutcomeKind,
    PreparedSplit, SplitShard, TransferLeader,
};
use self::background_job::Job;
use self::status::{BalanceSnapshot, SchedulerSummary, TaskStatus};
pub(super) use self::status::{Policy, TaskReason};
pub use self::task::*;
use super::allocator::*;
use super::stats::ShowSnapshot;
use super::*;
use crate::{BalanceDimension, Error};

pub struct ReconcileScheduler {
    ctx: ScheduleContext,
    poll: AsyncMutex<bool>,
    balance: SyncMutex<BalanceSnapshot>,
    attempts: SyncMutex<HashMap<u64, ExecutionStatus>>,
    leader_cooldowns: SyncMutex<HashMap<(u64, u64), Instant>>,
    wake: Notify,
}

#[derive(Default)]
struct ExecutionStatus {
    fire_at: u64,
    retry_count: u32,
    running: bool,
    waiting: Option<String>,
}

enum Plan {
    Placement(PlacementJob),
    CreateGroup,
    Drained(NodeDesc),
}

/// A step reads committed business state, prepares a request, or submits one
/// mutation.
enum Step {
    Pending(&'static str),
    Prepared(Action),
    Finished(OutcomeKind, &'static str),
}

pub struct ScheduleContext {
    shared: Arc<RootShared>,
    alloc: Arc<Allocator>,
    heartbeat_queue: Arc<HeartbeatQueue>,
    cluster_stats: Arc<ClusterStats>,
    bg_jobs: Arc<Jobs>,
    cfg: RootConfig,
}

/// Read-only inputs for a scheduling round. Active jobs supply resource
/// ownership.
pub(super) struct ClusterView {
    pub(super) groups: Vec<GroupDesc>,
    pub(super) nodes: Vec<NodeDesc>,
    pub(super) available: HashSet<u64>,
    pub(super) occupied: HashSet<(u64, u64)>,
    pub(super) split_cooldowns: HashSet<u64>,
    changing_membership: HashSet<u64>,
    pub(super) samples: ShowSnapshot,
    pub(super) busy: HashSet<u64>,
    pub(super) creating: usize,
    pub(super) unpublished: HashSet<u64>,
    pub(super) copying_nodes: HashSet<u64>,
    pub(super) placements: Vec<Action>,
    pub(super) leader_cooldowns: HashSet<(u64, u64)>,
}

impl ClusterView {
    /// Resolve serving leaders once. A newer term from any current member
    /// invalidates an older stats report even before a leader is observed.
    pub(super) fn new(
        nodes: Vec<NodeDesc>,
        mut groups: Vec<GroupDesc>,
        states: Vec<ReplicaState>,
        available: HashSet<u64>,
        stats: &ClusterStats,
        max_age: Duration,
    ) -> Self {
        groups.sort_unstable_by_key(|group| group.id);
        let mut samples = stats.show_snapshot(&groups, max_age);
        let (changing_membership, split_cooldowns) = stats.placement_blockers();
        let mut occupied = HashSet::with_capacity(states.len());
        let mut latest = HashMap::<u64, ReplicaState>::new();
        for state in states {
            occupied.insert((state.group_id, state.node_id));
            let Ok(index) = groups.binary_search_by_key(&state.group_id, |group| group.id) else {
                continue;
            };
            if !groups[index]
                .replicas
                .iter()
                .any(|r| r.id == state.replica_id && r.node_id == state.node_id)
            {
                continue;
            }
            let current = latest.entry(state.group_id).or_insert(state);
            if (state.term, state.role() == RaftRole::Leader)
                > (current.term, current.role() == RaftRole::Leader)
            {
                *current = state;
            }
        }
        samples.leaders.retain(|group, report| {
            latest.get(group).is_none_or(|state| {
                report.term > state.term
                    || (report.term == state.term
                        && (state.role() != RaftRole::Leader
                            || report.replica_id == state.replica_id))
            })
        });
        Self {
            nodes,
            groups,
            available,
            occupied,
            samples,
            changing_membership,
            split_cooldowns,
            busy: HashSet::new(),
            creating: 0,
            unpublished: HashSet::new(),
            copying_nodes: HashSet::new(),
            placements: Vec::new(),
            leader_cooldowns: HashSet::new(),
        }
    }

    pub(super) fn group(&self, id: u64) -> Option<&GroupDesc> {
        self.groups
            .binary_search_by_key(&id, |group| group.id)
            .ok()
            .map(|index| &self.groups[index])
    }

    pub(super) fn group_available(&self, id: u64) -> bool {
        !self.busy.contains(&id)
            && !self.unpublished.contains(&id)
            && !self.changing_membership.contains(&id)
    }

    pub(super) fn published_groups(&self) -> impl Iterator<Item = &GroupDesc> {
        self.groups.iter().filter(|group| !self.unpublished.contains(&group.id))
    }

    pub(super) fn migration_idle(&self, group: u64) -> bool {
        self.samples.leader(group).is_some() && self.samples.migration_idle(group)
    }

    fn version(&self, group: u64) -> Option<GroupVersion> {
        Some(GroupVersion {
            epoch: self.group(group)?.epoch,
            term: self.samples.leader(group)?.term,
        })
    }

    pub(super) fn track_placement(&mut self, action: Action) {
        self.busy.extend(action.groups().into_iter().flatten());
        match &action {
            Action::MoveShard(action) => {
                if let Ok(index) =
                    self.groups.binary_search_by_key(&action.desc.dest_group_id, |group| group.id)
                {
                    self.copying_nodes
                        .extend(self.groups[index].replicas.iter().map(|replica| replica.node_id));
                }
            }
            Action::MoveReplica(action) => {
                self.copying_nodes.insert(action.target_node);
            }
            _ => {}
        }
        self.placements.push(action);
    }
}

impl Plan {
    fn placement(action: Action, reason: TaskReason) -> Self {
        Self::Placement(PlacementJob {
            action: Some(action),
            reason: reason.detail.to_string(),
            policy: reason.policy.map(|policy| policy.name().to_owned()),
            ..Default::default()
        })
    }
}

impl ReconcileScheduler {
    pub fn new(ctx: ScheduleContext) -> Self {
        let balance = BalanceSnapshot::new(&ctx.cfg);
        Self {
            ctx,
            balance: balance.into(),
            poll: Default::default(),
            attempts: Default::default(),
            leader_cooldowns: Default::default(),
            wake: Notify::new(),
        }
    }

    pub(super) fn reset_status(&self) {
        *self.balance.lock().unwrap() = BalanceSnapshot::new(&self.ctx.cfg);
        self.attempts.lock().unwrap().clear();
        self.leader_cooldowns.lock().unwrap().clear();
    }

    pub(super) fn balance_status(&self) -> BalanceSnapshot {
        self.balance.lock().unwrap().clone()
    }

    pub(super) fn scheduler_status(&self, now: u64) -> SchedulerSummary {
        SchedulerSummary::from_tasks(&self.task_statuses(), now)
    }

    pub(super) fn task_statuses(&self) -> Vec<TaskStatus> {
        let mut jobs = self.ctx.bg_jobs.placement_jobs();
        jobs.sort_unstable_by_key(|job| job.id);
        let attempts = self.attempts.lock().unwrap();
        jobs.iter()
            .filter_map(|job| {
                let task = placement(job);
                Some(TaskStatus::new(job.id, task, task.action.as_ref()?, attempts.get(&job.id)))
            })
            .collect()
    }

    pub fn pending_tasks(&self) -> usize {
        self.ctx.bg_jobs.placement_jobs().len()
    }

    pub(super) fn need_reconcile(&self) -> bool {
        self.balance.lock().unwrap().iter().any(|(_, decision)| {
            matches!(
                decision,
                BalanceDecision::ActionNeeded(..)
                    | BalanceDecision::Deferred(_)
                    | BalanceDecision::Blocked(_)
                    | BalanceDecision::Unknown
            )
        })
    }

    pub async fn poll_and_schedule(&self) -> Duration {
        let mut recover = self.poll.lock().await;
        if *recover {
            if let Err(error) = self.ctx.bg_jobs.recover_placement().await {
                warn!("recover placement tasks: {error:?}");
                return Duration::from_secs(self.ctx.cfg.schedule_interval_sec.max(1));
            }
            self.attempts.lock().unwrap().clear();
            *recover = false;
        }
        match self.tick().await {
            Ok(true) => return Duration::from_millis(20),
            Ok(false) => {}
            Err(error) => {
                *recover = true;
                let mut balance = self.balance.lock().unwrap();
                balance.record_refresh_error(&error);
                self.record_request_loads(&balance, [false; 2]);
                warn!("advance Root scheduling: {error:?}");
                return Duration::from_secs(self.ctx.cfg.schedule_interval_sec.max(1));
            }
        }
        let interval = Duration::from_secs(self.ctx.cfg.schedule_interval_sec.max(1));
        let now = timestamp_millis();
        self.attempts
            .lock()
            .unwrap()
            .values()
            .map(|status| Duration::from_millis(status.fire_at.saturating_sub(now).max(20)))
            .fold(interval, Duration::min)
    }

    pub(super) fn wake(&self) {
        self.wake.notify_one();
    }

    pub(super) async fn wait(&self, interval: Duration) {
        let _ = sekas_runtime::time::timeout(interval, self.wake.notified()).await;
    }

    async fn tick(&self) -> Result<bool> {
        let schema = self.ctx.shared.schema()?;
        let term = schema.term().await?;
        let mut options = schema.balance_config(self.ctx.cfg.balance.clone()).await?;
        let _timer = metrics::RECONCILE_STEP_DURATION_SECONDS.start_timer();
        let jobs = self.ctx.bg_jobs.placement_jobs();
        metrics::RECONCILE_SCHEDULER_TASK_QUEUE_SIZE.set(jobs.len() as i64);
        let now = timestamp_millis();
        let due: Vec<_> = {
            let mut attempts = self.attempts.lock().unwrap();
            let mut due = Vec::new();
            for job in &jobs {
                if attempts.entry(job.id).or_default().fire_at <= now {
                    due.push(job);
                }
            }
            due
        };
        let had_due = !due.is_empty();
        let mut due = due.into_iter().map(|job| {
            self.attempts.lock().unwrap().entry(job.id).or_default().running = true;
            self.ctx.step(job, &schema, term)
        });
        let mut running = FuturesUnordered::new();
        for step in due.by_ref().take(options.max_concurrent_tasks.get()) {
            running.push(step);
        }

        let mut write_error = None;
        let mut progress = false;
        while let Some((job, result)) = running.next().await {
            if let Some(step) = due.next() {
                running.push(step);
            }
            let result = match result {
                Ok(Step::Pending(reason)) => {
                    self.defer(job.id, reason.to_owned(), false);
                    continue;
                }
                Err(error) => {
                    placement(job).action.as_ref().unwrap().record_retry();
                    self.defer(job.id, error.to_string(), true);
                    continue;
                }
                Ok(Step::Prepared(action)) => {
                    PlacementJob { action: Some(action), ..placement(job).clone() }
                }
                Ok(Step::Finished(kind, detail)) => {
                    let mut change = placement(job).clone();
                    change.outcome = Some(Outcome {
                        kind: kind as i32,
                        detail: detail.into(),
                        finished_at: timestamp_millis(),
                    });
                    change
                }
            };
            let transferred = match result.action.as_ref() {
                Some(Action::TransferLeader(transfer))
                    if transfer.draining_node.is_none()
                        && result
                            .outcome
                            .as_ref()
                            .is_some_and(|outcome| outcome.kind() == OutcomeKind::Succeeded) =>
                {
                    Some(*transfer)
                }
                _ => None,
            };
            let new = BackgroundJob { id: job.id, job: Some(Job::Placement(result)) };
            match self.ctx.bg_jobs.replace_schedule_job(job, new).await {
                Ok(()) => {
                    self.attempts.lock().unwrap().remove(&job.id);
                    if self.ctx.cfg.enable_node_request_balance
                        && let Some(transfer) = transferred
                    {
                        self.leader_cooldowns.lock().unwrap().insert(
                            (transfer.group, transfer.source),
                            Instant::now() + Duration::from_secs(30),
                        );
                    }
                    progress = true;
                }
                Err(error) => {
                    self.defer(job.id, error.to_string(), true);
                    write_error = Some(error);
                }
            }
        }
        if let Some(error) = write_error {
            return Err(error);
        }

        self.ctx.check_term(&schema, term).await?;
        let _timer = metrics::RECONCILE_CHECK_DURATION_SECONDS.start_timer();
        if had_due {
            options = schema.balance_config(self.ctx.cfg.balance.clone()).await?;
        }
        let mut view = self.ctx.cluster_view(&schema).await?;
        {
            let mut cooldowns = self.leader_cooldowns.lock().unwrap();
            let now = Instant::now();
            cooldowns.retain(|_, deadline| *deadline > now);
            view.leader_cooldowns.extend(cooldowns.keys().copied());
        }
        let mut balance = BalanceSnapshot::new(&self.ctx.cfg);
        let mut plan = self.ctx.plan(&view, &options, &mut balance);
        if view.placements.len().saturating_add(view.creating) >= options.max_concurrent_tasks.get()
            && matches!(plan, Some(Plan::Placement(_) | Plan::CreateGroup))
        {
            plan = None;
            balance.defer_ready_placements();
        }
        {
            let fresh = BalanceDimension::REQUESTS
                .map(|kind| balance.thresholds(Policy::Placement(kind)).is_some());
            let mut previous = self.balance.lock().unwrap();
            balance.retain_thresholds(&previous);
            self.record_request_loads(&balance, fresh);
            *previous = balance;
        }
        match plan {
            Some(Plan::Placement(mut job)) => {
                job.created_at = timestamp_millis();
                self.ctx
                    .bg_jobs
                    .submit(BackgroundJob { id: 0, job: Some(Job::Placement(job)) }, false)
                    .await?;
                progress = true;
            }
            Some(Plan::CreateGroup) => {
                self.ctx.bg_jobs.submit_create_group_job().await?;
                progress = true;
            }
            Some(Plan::Drained(node)) => {
                let mut drained = node.clone();
                drained.status = NodeStatus::Drained as i32;
                schema.replace_node(&node, &drained).await?;
            }
            None => {}
        }
        Ok(progress)
    }

    fn defer(&self, id: u64, reason: String, error: bool) {
        let mut attempts = self.attempts.lock().unwrap();
        let status = attempts.entry(id).or_default();
        status.running = false;
        status.waiting = Some(reason);
        if error {
            let seconds = (1_u64 << status.retry_count.min(5)).min(30);
            status.fire_at = timestamp_millis().saturating_add(seconds * 1000);
            status.retry_count = status.retry_count.saturating_add(1);
        } else {
            status.fire_at = timestamp_millis().saturating_add(1000);
            status.retry_count = 0;
        }
    }

    fn record_request_loads(&self, balance: &BalanceSnapshot, fresh: [bool; 2]) {
        if self.ctx.cfg.enable_node_request_balance {
            for (kind, fresh) in BalanceDimension::REQUESTS.into_iter().zip(fresh) {
                metrics::record_request_load(
                    kind,
                    balance.thresholds(Policy::Placement(kind)),
                    fresh,
                );
            }
        }
    }
}

impl ScheduleContext {
    pub(crate) fn new(
        shared: Arc<RootShared>,
        alloc: Arc<Allocator>,
        heartbeat_queue: Arc<HeartbeatQueue>,
        cluster_stats: Arc<ClusterStats>,
        bg_jobs: Arc<Jobs>,
        cfg: RootConfig,
    ) -> Self {
        Self { shared, alloc, heartbeat_queue, cluster_stats, bg_jobs, cfg }
    }

    async fn check_term(&self, schema: &Schema, term: u64) -> Result<()> {
        if schema.term().await? != term {
            return Err(Error::NotRootLeader(RootDesc::default(), 0, None));
        }
        Ok(())
    }

    async fn cluster_view(&self, schema: &Schema) -> Result<ClusterView> {
        let nodes = schema.list_node().await?;
        let groups = schema.list_group().await?;
        let states = schema.list_replica_state().await?;
        let available =
            nodes.iter().filter(|node| self.alloc.schedulable(node)).map(|node| node.id).collect();
        let mut view = ClusterView::new(
            nodes,
            groups,
            states,
            available,
            &self.cluster_stats,
            Duration::from_secs(self.cfg.liveness_threshold_sec),
        );
        (view.creating, view.unpublished) = self.bg_jobs.creating_groups();
        let mut jobs = self.bg_jobs.placement_jobs();
        jobs.sort_unstable_by_key(|job| job.id);
        for job in jobs {
            let Some(Job::Placement(task)) = job.job else { unreachable!() };
            view.track_placement(task.action.unwrap());
        }
        Ok(view)
    }

    fn plan(
        &self,
        view: &ClusterView,
        options: &crate::BalanceConfig,
        balance: &mut BalanceSnapshot,
    ) -> Option<Plan> {
        if let Some(drain) = drain_action(view) {
            return Some(drain);
        }
        let (create_group, decision) = self.group_count_plan(view);
        balance.record(Policy::GroupCount, decision);
        let (capacity, placement) = plan_round(view, &self.cfg, options, &self.cluster_stats);
        for (dimension, decision, thresholds) in placement.decisions {
            balance.record_with_thresholds(Policy::Placement(dimension), decision, thresholds);
        }
        if let Some(capacity) = capacity
            && !(capacity.priority == PlacementPriority::PlanningWatermark
                && placement.hot_split.is_some())
            && placement.choice.as_ref().is_none_or(|choice| {
                capacity.priority > choice.priority
                    && !(self.cfg.enable_node_request_balance
                        && capacity.priority == PlacementPriority::PlanningWatermark
                        && (choice.node_request
                            || matches!(
                                choice.dimension,
                                crate::BalanceDimension::Cpu
                                    | crate::BalanceDimension::Memory
                                    | crate::BalanceDimension::Disk
                            )))
            })
        {
            let reason = "Reduce group capacity pressure";
            let policy = match &capacity.action {
                CapacityAction::CreateGroup => Policy::GroupCount,
                CapacityAction::Place(..) => Policy::Placement(crate::BalanceDimension::ShardSize),
            };
            balance.record(policy, BalanceDecision::ActionNeeded(reason.into()));
            return match capacity.action {
                CapacityAction::CreateGroup => Some(Plan::CreateGroup),
                CapacityAction::Place(action) => Some(Plan::placement(
                    planned_action(view, PlacementAction::Shard(action))?,
                    TaskReason::new(reason),
                )),
            };
        }
        if let Some(split) = placement.hot_split {
            let group = view.group(split.group)?;
            let shard = group.shard(split.shard)?.clone();
            return Some(Plan::placement(
                Action::SplitShard(SplitShard {
                    group: group.id,
                    epoch: group.epoch,
                    shard,
                    max_movable_split_bytes: split.move_budget,
                    ..Default::default()
                }),
                TaskReason::for_balance(
                    split.dimension,
                    "Split a hot shard and remeasure its pieces".into(),
                ),
            ));
        }
        if let Some(choice) = placement.choice {
            return Some(Plan::placement(
                planned_action(view, choice.action)?,
                TaskReason::for_balance(choice.dimension, choice.reason),
            ));
        }
        if create_group {
            return Some(Plan::CreateGroup);
        }
        self.shard_maintenance(view)
    }

    fn group_count_plan(&self, view: &ClusterView) -> (bool, BalanceDecision) {
        if !self.cfg.enable_group_balance {
            return (false, BalanceDecision::Disabled);
        }
        let mut nodes = 0_usize;
        let mut cpus = 0.;
        for node in &view.nodes {
            if node.status() != NodeStatus::Decommissioned {
                nodes += 1;
                cpus += node.capacity.as_ref().map_or(0., |capacity| capacity.cpu_nums);
            }
        }
        let min_nodes = self.cfg.replicas_per_group;
        let groups = view.published_groups().count();
        if nodes < min_nodes {
            return (
                false,
                BalanceDecision::NoAction(format!(
                    "too few nodes. nodes={nodes}, min_nodes={min_nodes}, groups={groups}"
                )),
            );
        }
        let desired_groups = (cpus as usize).max(nodes);
        let inputs = format!(
            "nodes={nodes}, min_nodes={min_nodes}, cpus={cpus}, groups={groups}, desired_groups={desired_groups}"
        );
        let add = desired_groups.saturating_sub(groups + view.creating);
        if add == 0 {
            let decision = if groups < desired_groups {
                BalanceDecision::Deferred("waiting for groups being created".into())
            } else {
                BalanceDecision::NoAction(inputs)
            };
            return (false, decision);
        }
        if view.available.len() < min_nodes {
            return (
                false,
                BalanceDecision::Deferred("not enough scheduable nodes to create a group".into()),
            );
        }
        (true, BalanceDecision::ActionNeeded(format!("create {add} groups\n{inputs}")))
    }

    fn shard_maintenance(&self, view: &ClusterView) -> Option<Plan> {
        let split = self.cfg.enable_auto_shard_split && self.cfg.enable_proactive_shard_split;
        let limit = self.cfg.move_shard_size_limit;
        for group in view.groups.iter().filter(|group| {
            group.id != ROOT_GROUP_ID
                && view.group_available(group.id)
                && view.migration_idle(group.id)
        }) {
            if split {
                for shard in &group.shards {
                    if shard.table_id >= sekas_schema::FIRST_USER_TABLE_ID
                        && view.samples.shard(shard.id).bytes.is_some_and(|bytes| bytes > limit)
                        && !view.split_cooldowns.contains(&shard.id)
                    {
                        let action = Action::SplitShard(SplitShard {
                            group: group.id,
                            epoch: group.epoch,
                            shard: shard.clone(),
                            ..Default::default()
                        });
                        return Some(Plan::placement(
                            action,
                            TaskReason::new(format!(
                                "Shard exceeds the automatic split threshold of {limit} B"
                            )),
                        ));
                    }
                }
            }
            if self.cfg.enable_auto_shard_merge {
                let quiet = Duration::from_secs(self.cfg.shard_merge_quiet_period_sec);
                let mut shards: Vec<_> = group
                    .shards
                    .iter()
                    .filter(|shard| shard.table_id >= sekas_schema::FIRST_USER_TABLE_ID)
                    .collect();
                shards.sort_unstable_by_key(|shard| {
                    (shard.table_id, sekas_schema::shard::start_key(shard))
                });
                for adjacent in shards.windows(2) {
                    let [left, right] = adjacent else { unreachable!() };
                    if left.table_id == right.table_id
                        && view.samples.shard_is_quiet(group.id, left.id, quiet)
                        && view.samples.shard_is_quiet(group.id, right.id, quiet)
                        && !view.split_cooldowns.contains(&left.id)
                        && !view.split_cooldowns.contains(&right.id)
                        && left
                            .range
                            .as_ref()
                            .zip(right.range.as_ref())
                            .is_some_and(|(l, r)| l.end == r.start)
                        && view
                            .samples
                            .shard(left.id)
                            .bytes
                            .zip(view.samples.shard(right.id).bytes)
                            .is_some_and(|(l, r)| l.saturating_add(r) <= limit / 2)
                    {
                        let action = Action::MergeShards(MergeShards {
                            group: group.id,
                            left: (*left).clone(),
                            right: (*right).clone(),
                            version: view.version(group.id)?,
                        });
                        return Some(Plan::placement(
                            action,
                            TaskReason::new("Adjacent undersized shards have been quiet"),
                        ));
                    }
                }
            }
        }
        None
    }
}

fn drain_action(view: &ClusterView) -> Option<Plan> {
    for node in view
        .nodes
        .iter()
        .filter(|node| matches!(node.status(), NodeStatus::Draining | NodeStatus::Drained))
    {
        let mut has_leader = false;
        let mut observed_all = true;
        for group in view
            .groups
            .iter()
            .filter(|group| group.replicas.iter().any(|replica| replica.node_id == node.id))
        {
            let Some(leader) = view.samples.leader(group.id) else {
                observed_all = false;
                continue;
            };
            if leader.node_id != node.id {
                continue;
            }
            has_leader = true;
            if !view.group_available(group.id) {
                continue;
            }
            if let Some(target) = group
                .replicas
                .iter()
                .filter(|replica| {
                    replica.role() == ReplicaRole::Voter
                        && replica.node_id != node.id
                        && view.available.contains(&replica.node_id)
                })
                .min_by_key(|replica| {
                    (
                        view.samples
                            .leaders
                            .values()
                            .filter(|state| state.node_id == replica.node_id)
                            .count(),
                        replica.id,
                    )
                })
            {
                return Some(Plan::placement(
                    Action::TransferLeader(TransferLeader {
                        group: group.id,
                        source: leader.replica_id,
                        target: target.id,
                        draining_node: Some(node.id),
                        version: GroupVersion { epoch: group.epoch, term: leader.term },
                    }),
                    TaskReason::new("Node drain requested"),
                ));
            }
        }
        if node.status() == NodeStatus::Draining && !has_leader && observed_all {
            return Some(Plan::Drained(node.clone()));
        }
    }
    None
}

fn planned_action(view: &ClusterView, action: PlacementAction) -> Option<Action> {
    Some(match action {
        PlacementAction::Leader(action) => Action::TransferLeader(TransferLeader {
            group: action.group,
            source: action.src_replica,
            target: action.target_replica,
            draining_node: None,
            version: view.version(action.group)?,
        }),
        PlacementAction::Replica(action) => Action::MoveReplica(MoveReplica {
            group: action.group,
            outgoing: action.source_replica,
            target_node: action.target_node,
            ..Default::default()
        }),
        PlacementAction::Shard(action) => {
            let source = view.group(action.source_group)?;
            let target = view.group(action.target_group)?;
            let shard = source.shard(action.shard)?.clone();
            if let Some(target_bytes) = action.split_bytes {
                Action::SplitShard(SplitShard {
                    group: source.id,
                    epoch: source.epoch,
                    shard,
                    target_bytes: Some(target_bytes),
                    ..Default::default()
                })
            } else {
                Action::MoveShard(MoveShard {
                    desc: MoveShardDesc {
                        shard_desc: Some(shard),
                        src_group_id: source.id,
                        src_group_epoch: source.epoch,
                        dest_group_id: target.id,
                        dest_group_epoch: target.epoch,
                    },
                    target_term: view.samples.leader(target.id)?.term,
                })
            }
        }
    })
}

fn placement(job: &BackgroundJob) -> &PlacementJob {
    match &job.job {
        Some(Job::Placement(task)) => task,
        _ => unreachable!("expected a placement job"),
    }
}
