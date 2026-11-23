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

mod status;
mod task;

use std::sync::Mutex as SyncMutex;

use log::{debug, error, info, warn};
use prometheus::HistogramTimer;
use sekas_api::server::v1::*;
use sekas_rock::time::timestamp_millis;
use tokio::sync::Mutex;

use self::status::{BalanceSnapshot, Policy, QueueStatus};
use self::task::reconcile_task::Task;
pub use self::task::*;
use super::allocator::*;
use super::*;
use crate::Error;

pub struct ReconcileScheduler {
    ctx: ScheduleContext,
    tasks: Mutex<LinkedList<(u64, ReconcileTask)>>,
    balance: SyncMutex<BalanceSnapshot>,
    // Keep status readable while the execution queue is held across a task RPC.
    queue_status: SyncMutex<QueueStatus>,
}

enum BalanceActions {
    CreateGroups(usize),
    Move {
        replicas: Vec<ReplicaAction>,
        leaders: Vec<LeaderAction>,
        shards: Vec<ShardAction>,
        shard_guard: ShardMoveGuard,
    },
}

pub struct ScheduleContext {
    shared: Arc<RootShared>,
    alloc: Arc<Allocator<SysAllocSource>>,
    heartbeat_queue: Arc<HeartbeatQueue>,
    cluster_stats: Arc<ClusterStats>,
    bg_jobs: Arc<Jobs>,
    cfg: RootConfig,
}

impl ReconcileScheduler {
    pub fn new(ctx: ScheduleContext) -> Self {
        let balance = BalanceSnapshot::new(&ctx.cfg);
        Self {
            ctx,
            tasks: Default::default(),
            balance: balance.into(),
            queue_status: Default::default(),
        }
    }

    pub(super) fn reset_status(&self) {
        *self.balance.lock().unwrap() = BalanceSnapshot::new(&self.ctx.cfg);
    }

    pub(super) fn balance_status(&self) -> BalanceSnapshot {
        self.balance.lock().unwrap().clone()
    }

    pub(super) fn scheduler_status(&self, now: u64) -> status::SchedulerSummary {
        self.queue_status.lock().unwrap().summary(now)
    }

    pub(super) fn task_statuses(&self) -> Vec<status::TaskStatus> {
        self.queue_status.lock().unwrap().tasks.values().cloned().collect()
    }

    pub async fn poll_and_schedule(&self) -> Duration {
        let cr = self.generate_schedule_task().await; // TODO: take care self.tasks then can give more > 1 value here.
        if matches!(cr, Ok(true)) || !self.is_empty().await {
            let _step_timer = metrics::RECONCILE_STEP_DURATION_SECONDS.start_timer();
            while self.advance_tasks().await {}
        }
        Duration::from_secs(self.ctx.cfg.schedule_interval_sec)
    }

    pub async fn wait_one_heartbeat_tick(&self) {
        self.ctx.heartbeat_queue.wait_one_heartbeat_tick().await
    }

    pub async fn setup_task(&self, task: ReconcileTask) {
        let mut tasks = self.tasks.lock().await;
        if let Some(Task::MigrateShard(migration)) = &task.task
            && migration.guard() != ShardMoveGuard::Unrestricted
            && (self.ctx.cluster_stats.has_pending_shard_moves()
                || tasks
                    .iter()
                    .any(|(_, pending)| matches!(pending.task, Some(Task::MigrateShard(_)))))
        {
            return;
        }
        let shards = task.shards();
        if shards.iter().flatten().any(|shard| self.ctx.cluster_stats.is_shard_scheduled(*shard))
            || tasks.iter().any(|(_, pending)| {
                pending.shards().iter().flatten().any(|shard| shards.contains(&Some(*shard)))
            })
        {
            debug!("skip conflicting reconcile task: {task:?}");
            return;
        }
        match task.task.as_ref().unwrap() {
            Task::MigrateShard(t) => {
                self.ctx.cluster_stats.handle_move_shard(t.shard, t.dest_group)
            }
            Task::SplitShard(t) => self.ctx.cluster_stats.handle_split_shard(t.shard_id),
            Task::MergeShard(t) => {
                self.ctx.cluster_stats.handle_merge_shard(t.left_shard_id, t.right_shard_id)
            }
            _ => {}
        }
        let id = self.queue_status.lock().unwrap().insert(&task);
        info!("setup new reconcile task. len={}, task={:?}", tasks.len() + 1, task);
        tasks.push_back((id, task));
    }

    async fn is_empty(&self) -> bool {
        self.tasks.lock().await.is_empty()
    }

    pub async fn pending_tasks(&self) -> usize {
        self.tasks.lock().await.len()
    }

    /// Schedule moving root leader task.
    pub async fn sched_root_leader(&self, node_id: u64) {
        self.setup_task(ReconcileTask {
            task: Some(reconcile_task::Task::ShedRoot(ShedRootLeaderTask { node_id })),
            created_at: timestamp_millis(),
            fire_at: 0,
            retry_count: 0,
        })
        .await;
    }

    /// Schedule moving leader task.
    pub async fn sched_leader(&self, node_id: u64) {
        self.setup_task(ReconcileTask {
            task: Some(reconcile_task::Task::ShedLeader(ShedLeaderTask { node_id })),
            created_at: timestamp_millis(),
            fire_at: 0,
            retry_count: 0,
        })
        .await;
    }

    /// Schedule transfering leader task.
    pub async fn sched_transfer_leader_task(&self, transfer_leader: TransferLeader) {
        self.setup_task(ReconcileTask {
            task: Some(reconcile_task::Task::TransferGroupLeader(TransferGroupLeaderTask {
                group: transfer_leader.group,
                target_replica: transfer_leader.target_replica,
                src_node: transfer_leader.src_node,
                dest_node: transfer_leader.target_node,
            })),
            created_at: timestamp_millis(),
            fire_at: 0,
            retry_count: 0,
        })
        .await;
    }

    /// Schedule migrate replica task.
    pub async fn sched_migrate_replica_task(&self, action: ReallocateReplica) {
        self.setup_task(ReconcileTask {
            task: Some(reconcile_task::Task::ReallocateReplica(ReallocateReplicaTask {
                group: action.group,
                src_node: action.source_node,
                src_replica: action.source_replica,
                dest_node: Some(action.target_node),
                dest_replica: None,
            })),
            created_at: timestamp_millis(),
            fire_at: 0,
            retry_count: 0,
        })
        .await;
    }

    /// Schedule migrate shard task.
    pub async fn sched_migrate_shard_task(&self, action: ReallocateShard, guard: ShardMoveGuard) {
        self.setup_task(ReconcileTask {
            task: Some(reconcile_task::Task::MigrateShard(MigrateShardTask {
                shard: action.shard,
                src_group: action.source_group,
                dest_group: action.target_group,
                split_task: None,
                guard: guard as i32,
            })),
            created_at: timestamp_millis(),
            fire_at: 0,
            retry_count: 0,
        })
        .await;
    }

    /// Schedule split shard task.
    pub async fn sched_split_shard_task(&self, group_id: u64, shard_id: u64) {
        debug!("sched split shard task, group_id {group_id}, shard_id {shard_id}");
        self.setup_task(ReconcileTask {
            task: Some(reconcile_task::Task::SplitShard(SplitShardTask {
                group_id,
                shard_id,
                ..Default::default()
            })),
            created_at: timestamp_millis(),
            fire_at: 0,
            retry_count: 0,
        })
        .await;
    }

    /// Schedule merge shard task.
    pub async fn sched_merge_shard_task(
        &self,
        group_id: u64,
        left_shard_id: u64,
        right_shard_id: u64,
    ) {
        debug!(
            "sched merge shard task, group_id {group_id}, left {left_shard_id}, right {right_shard_id}"
        );
        self.setup_task(ReconcileTask {
            task: Some(reconcile_task::Task::MergeShard(MergeShardTask {
                group_id,
                left_shard_id,
                right_shard_id,
            })),
            created_at: timestamp_millis(),
            fire_at: 0,
            retry_count: 0,
        })
        .await;
    }
}

impl ReconcileScheduler {
    pub async fn need_reconcile(&self) -> Result<bool> {
        let mut balance = BalanceSnapshot::new(&self.ctx.cfg);
        let needed = match self.evaluate_balance(&mut balance).await? {
            BalanceActions::CreateGroups(_) => true,
            BalanceActions::Move { replicas, leaders, shards, .. } => {
                !replicas.is_empty() || !leaders.is_empty() || !shards.is_empty()
            }
        };
        let waiting_for_size = balance.iter().any(|(policy, decision)| {
            matches!((policy, decision), (Policy::ShardSize, BalanceDecision::Deferred(_)))
        });
        Ok(needed || waiting_for_size || self.shard_balance_move_pending())
    }

    async fn generate_schedule_task(&self) -> Result<bool> {
        let _timer = super::metrics::RECONCILE_CHECK_DURATION_SECONDS.start_timer();
        let mut balance = BalanceSnapshot::new(&self.ctx.cfg);
        let actions = self.evaluate_balance(&mut balance).await;
        *self.balance.lock().unwrap() = balance;

        let (replica_actions, leader_actions, shard_actions, shard_guard) = match actions? {
            BalanceActions::CreateGroups(count) => {
                for _ in 0..count {
                    self.ctx.bg_jobs.submit_create_group_job().await?;
                }
                return Ok(true);
            }
            BalanceActions::Move { replicas, leaders, shards, shard_guard } => {
                (replicas, leaders, shards, shard_guard)
            }
        };

        for ReplicaAction::Migrate(action) in replica_actions {
            self.sched_migrate_replica_task(action).await;
        }
        for action in leader_actions {
            if let LeaderAction::Shed(action) = action {
                self.sched_transfer_leader_task(action).await;
            }
        }
        for ShardAction::Migrate(action) in shard_actions {
            self.sched_migrate_shard_task(action, shard_guard).await;
        }

        if self.shard_balance_move_pending() {
            return Ok(!self.is_empty().await);
        }

        if self.ctx.cfg.enable_auto_shard_split && self.ctx.cfg.enable_proactive_shard_split {
            for (group_id, shard_id) in
                self.ctx.cluster_stats.get_large_shards(5, self.ctx.cfg.move_shard_size_limit)
            {
                self.sched_split_shard_task(group_id, shard_id).await;
            }
        }

        if self.ctx.cfg.enable_auto_shard_merge {
            for (group_id, left_shard_id, right_shard_id) in
                self.ctx.merge_shard_candidates().await?
            {
                self.sched_merge_shard_task(group_id, left_shard_id, right_shard_id).await;
            }
        }

        Ok(!self.is_empty().await)
    }

    async fn evaluate_balance(&self, balance: &mut BalanceSnapshot) -> Result<BalanceActions> {
        if let Err(err) = self.ctx.alloc.refresh_for_balance().await {
            balance.record_refresh_error(&err);
            return Err(err);
        }
        let (group_action, decision) = self.ctx.alloc.compute_group_action();
        balance.record(Policy::GroupCount, decision);
        if let GroupAction::Add(count) = group_action {
            metrics::RECONCILE_ALREADY_BALANCED_INFO.cluster_groups.set(0);
            balance.defer("group creation takes priority");
            return Ok(BalanceActions::CreateGroups(count));
        }
        metrics::RECONCILE_ALREADY_BALANCED_INFO.cluster_groups.set(1);

        let (replicas, decision) = self.compute_replica_actions();
        balance.record(Policy::ReplicaCount, decision);
        let (leaders, decision) = self.compute_leader_actions();
        balance.record(Policy::LeaderCount, decision);
        let shards = self.ctx.alloc.compute_shard_action();
        let shard_guard = if matches!(shards.size, BalanceDecision::ActionNeeded(_)) {
            ShardMoveGuard::ImproveSize
        } else if self.ctx.cfg.enable_shard_size_balance {
            ShardMoveGuard::PreserveSize
        } else {
            ShardMoveGuard::Unrestricted
        };
        balance.record(Policy::ShardSize, shards.size);
        balance.record(Policy::ShardCount, shards.count);
        Ok(BalanceActions::Move { replicas, leaders, shards: shards.actions, shard_guard })
    }

    fn shard_balance_move_pending(&self) -> bool {
        self.ctx.cfg.enable_shard_size_balance && self.ctx.cluster_stats.has_pending_shard_moves()
    }

    fn compute_replica_actions(&self) -> (Vec<ReplicaAction>, BalanceDecision) {
        let (actions, decision) = self.ctx.alloc.compute_replica_action();
        metrics::RECONCILE_ALREADY_BALANCED_INFO
            .node_replica_count
            .set(i64::from(actions.is_empty()));
        (actions, decision)
    }

    fn compute_leader_actions(&self) -> (Vec<LeaderAction>, BalanceDecision) {
        let (actions, decision) = self.ctx.alloc.compute_leader_action();
        metrics::RECONCILE_ALREADY_BALANCED_INFO
            .node_leader_count
            .set(i64::from(actions.is_empty()));
        (actions, decision)
    }
}

impl ReconcileScheduler {
    async fn advance_tasks(&self) -> bool {
        let mut tasks = self.tasks.lock().await;
        metrics::RECONCILE_SCHEDULER_TASK_QUEUE_SIZE.set(tasks.len() as i64);
        let mut immediately_next = false;
        let mut cursor = tasks.cursor_front_mut();
        while let Some((id, task)) = cursor.current() {
            if timestamp_millis() < task.fire_at {
                cursor.move_next();
                continue;
            }

            let id = *id;
            self.queue_status.lock().unwrap().start_attempt(id);
            let _timer = Self::record_exec(task);
            let sched_result = self.ctx.handle_task(task).await;
            let retry_at = timestamp_millis();
            match sched_result {
                Ok(result) if result.ack => {
                    immediately_next |= result.immediately_next;
                    self.queue_status.lock().unwrap().tasks.remove(&id);
                    cursor.remove_current();
                }
                Ok(result) => {
                    Self::record_retry(task);
                    task.defer_retry(retry_at, result.delay);
                    self.queue_status.lock().unwrap().finish_attempt(id, task);
                    cursor.move_next();
                }
                Err(_) => {
                    Self::record_retry(task);
                    task.defer_retry(retry_at, None);
                    self.queue_status.lock().unwrap().finish_attempt(id, task);
                    cursor.move_next();
                }
            }
        }
        // A queue containing only deferred tasks must return to the timed poll.
        immediately_next && tasks.iter().any(|(_, task)| task.fire_at <= timestamp_millis())
    }

    fn record_exec(task: &mut ReconcileTask) -> HistogramTimer {
        match task.task.as_ref().unwrap() {
            Task::ReallocateReplica(_) => {
                metrics::RECONCILE_HANDLE_TASK_TOTAL.reallocate_replica.inc();
                metrics::RECONCILE_HANDLE_TASK_DURATION_SECONDS.reallocate_replica.start_timer()
            }
            Task::MigrateShard(_) => {
                metrics::RECONCILE_HANDLE_TASK_TOTAL.migrate_shard.inc();
                metrics::RECONCILE_HANDLE_TASK_DURATION_SECONDS.migrate_shard.start_timer()
            }
            Task::TransferGroupLeader(_) => {
                metrics::RECONCILE_HANDLE_TASK_TOTAL.transfer_leader.inc();
                metrics::RECONCILE_HANDLE_TASK_DURATION_SECONDS.transfer_leader.start_timer()
            }
            Task::ShedLeader(_) => {
                metrics::RECONCILE_HANDLE_TASK_TOTAL.shed_group_leaders.inc();
                metrics::RECONCILE_HANDLE_TASK_DURATION_SECONDS.shed_group_leaders.start_timer()
            }
            Task::ShedRoot(_) => {
                metrics::RECONCILE_HANDLE_TASK_TOTAL.shed_root_leader.inc();
                metrics::RECONCILE_HANDLE_TASK_DURATION_SECONDS.shed_root_leader.start_timer()
            }
            Task::SplitShard(_) => {
                metrics::RECONCILE_HANDLE_TASK_TOTAL.split_shard.inc();
                metrics::RECONCILE_HANDLE_TASK_DURATION_SECONDS.split_shard.start_timer()
            }
            Task::MergeShard(_) => {
                metrics::RECONCILE_HANDLE_TASK_TOTAL.merge_shard.inc();
                metrics::RECONCILE_HANDLE_TASK_DURATION_SECONDS.merge_shard.start_timer()
            }
        }
    }

    fn record_retry(task: &mut ReconcileTask) {
        match task.task.as_ref().unwrap() {
            Task::ReallocateReplica(_) => {
                metrics::RECONCILE_RETRY_TASK_TOTAL.reallocate_replica.inc()
            }
            Task::MigrateShard(_) => metrics::RECONCILE_RETRY_TASK_TOTAL.migrate_shard.inc(),
            Task::TransferGroupLeader(_) => {
                metrics::RECONCILE_RETRY_TASK_TOTAL.transfer_leader.inc()
            }
            Task::ShedLeader(_) => metrics::RECONCILE_RETRY_TASK_TOTAL.shed_group_leaders.inc(),
            Task::ShedRoot(_) => metrics::RECONCILE_RETRY_TASK_TOTAL.shed_root_leader.inc(),
            Task::SplitShard(_) => metrics::RECONCILE_RETRY_TASK_TOTAL.split_shard.inc(),
            Task::MergeShard(_) => metrics::RECONCILE_RETRY_TASK_TOTAL.merge_shard.inc(),
        }
    }
}

#[derive(Debug, Default)]
struct SchedResult {
    /// Ack current task.
    ack: bool,
    /// immediately step next tick.
    immediately_next: bool,
    /// Delay task by duration.
    delay: Option<Duration>,
}

impl SchedResult {
    /// Ack but not step next immediately
    fn ack() -> Self {
        SchedResult { ack: true, ..Default::default() }
    }

    /// Ack and immediately step next
    fn next() -> Self {
        SchedResult { ack: true, immediately_next: true, ..Default::default() }
    }

    /// Keep current task and retry after a delay.
    fn retry_after(duration: Duration) -> Self {
        SchedResult { ack: false, immediately_next: false, delay: Some(duration) }
    }
}

/// Returns true when this split has already completed, false when it can run.
fn precheck_split_shard(task: &SplitShardTask, group: Option<&GroupDesc>) -> Result<bool> {
    let group = group.ok_or(Error::AbortScheduleTask("split group no longer exists"))?;
    if task.new_shard_id.is_some_and(|id| group.shard(id).is_some()) {
        return Ok(true);
    }
    if group.shard(task.shard_id).is_none() {
        return Err(Error::AbortScheduleTask("split shard no longer belongs to the group"));
    }
    if task.group_epoch.is_some_and(|epoch| epoch != group.epoch) {
        return Err(Error::AbortScheduleTask("split group metadata has changed"));
    }
    Ok(false)
}

impl ReconcileTask {
    fn shards(&self) -> [Option<u64>; 2] {
        match self.task.as_ref().unwrap() {
            Task::MigrateShard(t) => [Some(t.shard), None],
            Task::SplitShard(t) => [Some(t.shard_id), None],
            Task::MergeShard(t) => [Some(t.left_shard_id), Some(t.right_shard_id)],
            _ => [None, None],
        }
    }

    fn defer_retry(&mut self, now: u64, delay: Option<Duration>) {
        let delay = match delay {
            Some(delay) => {
                self.retry_count = 0;
                delay
            }
            None => {
                let secs = (1u64 << self.retry_count.min(5)).min(30);
                self.retry_count = self.retry_count.saturating_add(1);
                Duration::from_secs(secs)
            }
        };
        self.fire_at = now.saturating_add(delay.as_millis().min(u64::MAX as u128) as u64);
    }
}

fn is_merge_candidate(
    left: &ShardDesc,
    right: &ShardDesc,
    left_stats: &ShardStats,
    right_stats: &ShardStats,
    move_shard_size_limit: u64,
    is_scheduled: bool,
) -> bool {
    if is_scheduled {
        return false;
    }
    if check_merge_shards(left, right).is_err() {
        return false;
    }
    left_stats.shard_size.saturating_add(right_stats.shard_size) <= move_shard_size_limit
}

fn check_merge_shards(left: &ShardDesc, right: &ShardDesc) -> Result<(), &'static str> {
    if left.table_id != right.table_id {
        return Err("different table");
    }
    if left.table_id < sekas_schema::FIRST_USER_TABLE_ID {
        return Err("system table");
    }
    let (Some(left_range), Some(right_range)) = (&left.range, &right.range) else {
        return Err("missing range");
    };
    if left_range.end != right_range.start {
        return Err("not mergeable");
    }
    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MigrateShardPrecheck {
    Move,
    Split(Option<u64>),
    WaitStats,
}

fn precheck_migrate_shard_stats(cfg: &RootConfig, shard_size: Option<u64>) -> MigrateShardPrecheck {
    if !cfg.enable_auto_shard_split {
        return MigrateShardPrecheck::Move;
    }
    let Some(shard_size) = shard_size else {
        return MigrateShardPrecheck::WaitStats;
    };
    if shard_size > cfg.move_shard_size_limit {
        MigrateShardPrecheck::Split(None)
    } else {
        MigrateShardPrecheck::Move
    }
}

impl ScheduleContext {
    pub(crate) fn new(
        shared: Arc<RootShared>,
        alloc: Arc<Allocator<SysAllocSource>>,
        heartbeat_queue: Arc<HeartbeatQueue>,
        cluster_stats: Arc<ClusterStats>,
        bg_jobs: Arc<Jobs>,
        cfg: RootConfig,
    ) -> Self {
        ScheduleContext { shared, alloc, heartbeat_queue, cluster_stats, bg_jobs, cfg }
    }

    async fn merge_shard_candidates(&self) -> Result<Vec<(u64, u64, u64)>> {
        const LIMIT: usize = 5;

        let schema = self.shared.schema()?;
        let mut candidates = Vec::with_capacity(LIMIT);
        for group in schema.list_group().await? {
            if group.id == ROOT_GROUP_ID {
                continue;
            }

            let mut shards = group.shards.clone();
            shards.sort_by(|a, b| {
                a.table_id.cmp(&b.table_id).then_with(|| {
                    sekas_schema::shard::start_key(a).cmp(&sekas_schema::shard::start_key(b))
                })
            });

            for pair in shards.windows(2) {
                let [left, right] = pair else { continue };
                if !self.is_merge_candidate(left, right) {
                    continue;
                }
                candidates.push((group.id, left.id, right.id));
                if candidates.len() >= LIMIT {
                    return Ok(candidates);
                }
            }
        }
        Ok(candidates)
    }

    fn is_merge_candidate(&self, left: &ShardDesc, right: &ShardDesc) -> bool {
        let (Some(left_stats), Some(right_stats)) = (
            self.cluster_stats.get_shard_stats(left.id),
            self.cluster_stats.get_shard_stats(right.id),
        ) else {
            return false;
        };
        is_merge_candidate(
            left,
            right,
            &left_stats,
            &right_stats,
            self.cfg.move_shard_size_limit,
            self.cluster_stats.is_shard_scheduled(left.id)
                || self.cluster_stats.is_shard_scheduled(right.id)
                || self.cluster_stats.is_merging_shard(left.id, right.id),
        )
    }

    async fn handle_task(&self, task: &mut ReconcileTask) -> Result<SchedResult> {
        info!("handle reconcile task. task={task:?}");
        match task.task.as_mut().unwrap() {
            Task::ReallocateReplica(reallocate_replica) => {
                self.handle_reallocate_replica(reallocate_replica).await
            }
            Task::MigrateShard(migrate_shard) => self.handle_migrate_shard(migrate_shard).await,
            Task::TransferGroupLeader(transfer_leader) => {
                self.handle_transfer_leader(transfer_leader).await
            }
            Task::ShedLeader(shed_leader) => self.handle_shed_leader(shed_leader).await,
            Task::ShedRoot(shed_root) => self.handle_shed_root(shed_root).await,
            Task::SplitShard(split_shard) => self.handle_split_shard(split_shard).await,
            Task::MergeShard(merge_shard) => self.handle_merge_shard(merge_shard).await,
        }
    }

    async fn handle_reallocate_replica(
        &self,
        task: &mut ReallocateReplicaTask,
    ) -> Result<SchedResult> {
        let schema = self.shared.schema()?;

        let group = task.group;
        let replica = task.src_replica;
        let r = self.try_shed_leader_before_remove(group, replica).await;
        match r {
            Ok(_) => {}
            Err(crate::Error::AbortScheduleTask(_)) => return Ok(SchedResult::next()),
            Err(crate::Error::EpochNotMatch(new_group)) => {
                warn!(
                    "shed leader meet epoch not match, abort task and retry allocator. group={group}, replica={replica}, new_group={new_group:?}"
                );
                return Ok(SchedResult::ack());
            }
            Err(err) => {
                warn!(
                    "shed leader in source replica fail, retry in next tick: {err:?}. group={group}, replica={replica}",
                );
                metrics::RECONCILE_RETRY_TASK_TOTAL.reallocate_replica.inc();
                return Err(err);
            }
        };

        let group_desc = schema.get_group(group).await?;
        if group_desc.is_none() {
            warn!("group not found abort reallocate replica task. group={group}");
            return Ok(SchedResult::ack());
        }

        let src_replica =
            group_desc.as_ref().unwrap().replicas.iter().find(|r| r.id == task.src_replica);

        if src_replica.is_none() {
            warn!(
                "source replica not found abort reallocate replica task. group={group}, replica={}",
                task.src_replica
            );
            return Ok(SchedResult::ack());
        }

        info!(
            "start move replica. group={group}, src_node={}, dest_node={}",
            task.src_node,
            task.dest_node.as_ref().unwrap().id
        );
        let next_replica = schema.next_replica_id().await?;
        match self
            .try_move_replica(
                group,
                ReplicaDesc {
                    id: next_replica,
                    node_id: task.dest_node.as_ref().unwrap().id,
                    role: ReplicaRole::Voter as i32,
                },
                src_replica.unwrap().to_owned(),
            )
            .await
        {
            Ok(schedule_state) => {
                self.cluster_stats.handle_schedule_update(&[schedule_state], None);
                Ok(SchedResult::ack())
            }
            Err(crate::Error::AlreadyExists(_)) | Err(crate::Error::EpochNotMatch(_)) => {
                warn!(
                    "move replica task aborted due to replica already changed. group={group}, src_node={}, dest_node={}",
                    task.src_node,
                    task.dest_node.as_ref().unwrap().id
                );
                Ok(SchedResult::ack())
            }
            Err(err) => {
                warn!(
                    "move replica meet error and retry later: {err:?}. group={group}, src_node={}, dest_node={}",
                    task.src_node,
                    task.dest_node.as_ref().unwrap().id
                );
                metrics::RECONCILE_RETRY_TASK_TOTAL.reallocate_replica.inc();
                Err(err)
            }
        }
    }

    async fn handle_migrate_shard(&self, task: &mut MigrateShardTask) -> Result<SchedResult> {
        if let Some(split_task) = &mut task.split_task {
            let result = self.handle_split_shard(split_task).await?;
            if result.ack {
                self.cluster_stats.finish_move_shard(task.shard);
            }
            return Ok(result);
        }
        info!(
            "start migrate shard. shard={}, src={}, dest={}",
            task.shard, task.src_group, task.dest_group
        );
        let guard = task.guard();
        let (planned_groups, precheck) = if guard == ShardMoveGuard::Unrestricted {
            (
                Vec::new(),
                precheck_migrate_shard_stats(
                    &self.cfg,
                    self.cluster_stats.get_shard_stats(task.shard).map(|stats| stats.shard_size),
                ),
            )
        } else {
            let mut planned_groups = self.shared.schema()?.list_group().await?;
            planned_groups.retain(|group| group.id != ROOT_GROUP_ID);
            let action = ReallocateShard {
                shard: task.shard,
                source_group: task.src_group,
                target_group: task.dest_group,
            };
            let Some(placement) = self.alloc.validate_shard_balance_move(
                &planned_groups,
                &action,
                guard == ShardMoveGuard::ImproveSize,
            ) else {
                self.cluster_stats.finish_move_shard(task.shard);
                return Ok(SchedResult::ack());
            };
            let precheck = match placement {
                ShardSizePlacement::Move => MigrateShardPrecheck::Move,
                ShardSizePlacement::Split(bytes) => MigrateShardPrecheck::Split(Some(bytes)),
            };
            (planned_groups, precheck)
        };

        match precheck {
            MigrateShardPrecheck::Move => {}
            MigrateShardPrecheck::Split(target_size) => {
                info!(
                    "split shard before migrating because it exceeds move size limit. shard={}, src={}, dest={}, limit={}",
                    task.shard, task.src_group, task.dest_group, self.cfg.move_shard_size_limit
                );
                let split_task = task.split_task.insert(SplitShardTask {
                    group_id: task.src_group,
                    shard_id: task.shard,
                    target_size,
                    ..Default::default()
                });
                let result = self.handle_split_shard(split_task).await?;
                if result.ack {
                    self.cluster_stats.finish_move_shard(task.shard);
                }
                return Ok(result);
            }
            MigrateShardPrecheck::WaitStats => {
                info!(
                    "delay migrating shard until shard stats are collected. shard={}, src={}, dest={}",
                    task.shard, task.src_group, task.dest_group
                );
                return Ok(SchedResult::retry_after(Duration::from_secs(1)));
            }
        }
        let r = self.try_migrate_shard(task, &planned_groups).await;
        match r {
            Ok(_) => Ok(SchedResult::ack()),
            Err(crate::Error::EpochNotMatch(_)) if guard != ShardMoveGuard::Unrestricted => {
                self.cluster_stats.finish_move_shard(task.shard);
                Ok(SchedResult::ack())
            }
            Err(crate::Error::AbortScheduleTask(reason)) => {
                warn!(
                    "abort migrate shard. shard={}, src={}, dest={}, reason={reason}",
                    task.shard, task.src_group, task.dest_group
                );
                self.cluster_stats.finish_move_shard(task.shard);
                Ok(SchedResult::ack())
            }
            Err(err) => {
                warn!(
                    "migrate shard fail, retry later: {err:?}. shard={}, src={}, dest={}",
                    task.shard, task.src_group, task.dest_group
                );
                Err(err)
            }
        }
    }

    async fn handle_transfer_leader(
        &self,
        task: &mut TransferGroupLeaderTask,
    ) -> Result<SchedResult> {
        match self.try_transfer_leader(task.group, task.target_replica).await {
            Ok(_) => {}
            Err(crate::Error::EpochNotMatch(new_group)) => {
                warn!(
                    "transfer target meet epoch not match, abort transfer task. group={}, dest={}, new_group={:?}",
                    task.group, task.target_replica, new_group
                );
                return Ok(SchedResult::ack());
            }
            Err(err) => {
                error!(
                    "transfer group leader: {err:?}. group={}, dest={}",
                    task.group, task.target_replica
                );
                return Err(err);
            }
        }
        self.heartbeat_queue
            .try_schedule(
                vec![
                    HeartbeatTask { node_id: task.dest_node },
                    HeartbeatTask { node_id: task.src_node },
                ],
                Instant::now(),
            )
            .await;
        Ok(SchedResult::next())
    }

    async fn handle_shed_leader(&self, shed: &mut ShedLeaderTask) -> Result<SchedResult> {
        let node = shed.node_id;
        loop {
            let schema = self.shared.schema()?;

            if let Some(desc) = schema.get_node(node).await? {
                if desc.status != NodeStatus::Draining as i32 {
                    warn!("shed leader task cancelled. node={node}");
                    break;
                }
            }

            let leader_replicas = schema
                .list_replica_state()
                .await?
                .into_iter()
                .filter(|r| r.node_id == node && r.role == RaftRole::Leader as i32)
                .collect::<Vec<_>>();

            // exit when all leader move-out
            // also change node status to Drained
            if leader_replicas.is_empty() {
                if let Some(mut desc) = schema.get_node(node).await? {
                    if desc.status == NodeStatus::Draining as i32 {
                        desc.status = NodeStatus::Drained as i32;
                        schema.update_node(desc).await?; // TODO: cas
                    }
                }
                break;
            }

            for replica in &leader_replicas {
                let group_id = replica.group_id;
                if let Some(group) = schema.get_group(group_id).await? {
                    let mut target_replica = None;
                    for r in &group.replicas {
                        if r.id == replica.replica_id {
                            continue;
                        }
                        let target_node = schema.get_node(r.node_id).await?;
                        if target_node.is_none() {
                            continue;
                        }
                        if target_node.as_ref().unwrap().status != NodeStatus::Active as i32 {
                            continue;
                        }
                        target_replica = Some(r.to_owned())
                    }
                    if let Some(target_replica) = target_replica {
                        self.try_transfer_leader(group_id, target_replica.id).await?;
                    } else {
                        warn!(
                            "shed leader from node fail due to no suitable target replica. node={node}, group={group_id}, src_replica={}",
                            replica.replica_id
                        );
                        metrics::RECONCILE_RETRY_TASK_TOTAL.shed_group_leaders.inc();
                    }
                }
            }
        }

        Ok(SchedResult::next())
    }

    async fn handle_shed_root(&self, task: &mut ShedRootLeaderTask) -> Result<SchedResult> {
        let node = task.node_id;
        let schema = self.shared.schema()?;
        let root_group = schema.get_group(ROOT_GROUP_ID).await?.unwrap();
        let mut target = None;
        for r in &root_group.replicas {
            if r.node_id == node {
                continue;
            }
            let target_node = schema.get_node(r.node_id).await?;
            if target_node.is_none() {
                continue;
            }
            if target_node.as_ref().unwrap().status != NodeStatus::Active as i32 {
                continue;
            }
            target = Some(r.to_owned())
        }
        if let Some(r) = target {
            self.try_transfer_leader(root_group.id, r.id).await?
        }
        Ok(SchedResult::ack())
    }

    /// Handle the spliting shard shards and update the sched stats.
    async fn handle_split_shard(&self, task: &mut SplitShardTask) -> Result<SchedResult> {
        self.cluster_stats.handle_split_shard(task.shard_id);
        let result = self.handle_split_shard_inner(task).await?;
        if result.ack {
            self.cluster_stats.finish_split_shard(task.shard_id);
        }
        Ok(result)
    }

    /// Validate and execute a split against one group metadata version.
    async fn handle_split_shard_inner(&self, task: &mut SplitShardTask) -> Result<SchedResult> {
        let schema = self.shared.schema()?;
        let group = schema.get_group(task.group_id).await?;
        match precheck_split_shard(task, group.as_ref()) {
            Ok(true) => return Ok(SchedResult::next()),
            Ok(false) => {}
            Err(Error::AbortScheduleTask(reason)) => {
                warn!(
                    "abort split shard task. group={}, shard={}, reason={reason}",
                    task.group_id, task.shard_id
                );
                return Ok(SchedResult::next());
            }
            Err(err) => return Err(err),
        }
        let group = group.unwrap();
        task.group_epoch = Some(group.epoch);
        let old_shard_id = task.shard_id;
        let new_shard_id = match task.new_shard_id {
            Some(id) => id,
            None => {
                let id = schema.next_shard_id().await?;
                task.new_shard_id = Some(id);
                id
            }
        };
        let split_policy = self.split_policy_for_moving(&group, old_shard_id, task.target_size);

        match self.try_split_shard(&group, old_shard_id, new_shard_id, split_policy).await {
            Ok(true) => Ok(SchedResult::next()),
            Ok(false) => {
                self.defer_unsplittable_shard(old_shard_id);
                info!(
                    "skip split shard without a split key. group={}, shard={}",
                    task.group_id, old_shard_id
                );
                Ok(SchedResult::ack())
            }
            Err(crate::Error::EpochNotMatch(desc)) => {
                if matches!(precheck_split_shard(task, Some(&desc)), Ok(true)) {
                    return Ok(SchedResult::next());
                }
                warn!(
                    "split shard meet epoch not match, abort split shard task. group={}, shard={}, new_shard={}",
                    task.group_id, old_shard_id, new_shard_id
                );
                Ok(SchedResult::next())
            }
            Err(crate::Error::InvalidArgument(msg))
                if msg.contains("shard estimated split keys is empty") =>
            {
                self.defer_unsplittable_shard(old_shard_id);
                info!(
                    "skip split shard without estimated keys. group={}, shard={}",
                    task.group_id, old_shard_id
                );
                Ok(SchedResult::ack())
            }
            Err(err) => {
                error!(
                    "split shard: {err:?}. group={}, shard={}, new_shard={}",
                    task.group_id, old_shard_id, new_shard_id
                );
                Err(err)
            }
        }
    }

    fn defer_unsplittable_shard(&self, shard: u64) {
        if self.cfg.enable_shard_size_balance || !self.cfg.enable_proactive_shard_split {
            self.cluster_stats.defer_shard_split(
                shard,
                Duration::from_secs(self.cfg.liveness_threshold_sec.max(1)),
            );
        }
    }

    fn split_policy_for_moving(
        &self,
        group: &GroupDesc,
        shard_id: u64,
        target_size: Option<u64>,
    ) -> Option<(Vec<u8>, u64)> {
        let target_size = target_size.or_else(|| {
            let stats = self.cluster_stats.get_shard_stats(shard_id)?;
            (stats.shard_size > self.cfg.move_shard_size_limit)
                .then_some(self.cfg.move_shard_size_limit)
        })?;
        let shard = group.shard(shard_id)?;
        let range = shard.range.as_ref()?;
        Some((range.start.clone(), target_size))
    }

    async fn handle_merge_shard(&self, task: &mut MergeShardTask) -> Result<SchedResult> {
        let result = self.handle_merge_shard_inner(task).await?;
        if result.ack {
            self.cluster_stats.finish_merge_shard(task.left_shard_id, task.right_shard_id);
        }
        Ok(result)
    }

    async fn handle_merge_shard_inner(&self, task: &mut MergeShardTask) -> Result<SchedResult> {
        let schema = self.shared.schema()?;
        let Some(group) = schema.get_group(task.group_id).await? else {
            warn!("merge shard but group {} is not exists", task.group_id);
            return Ok(SchedResult::next());
        };
        let Some(left) = group.shard(task.left_shard_id) else {
            warn!(
                "abort merge shard task because left shard is missing. group={}, left={}, right={}",
                task.group_id, task.left_shard_id, task.right_shard_id
            );
            return Ok(SchedResult::next());
        };
        let Some(right) = group.shard(task.right_shard_id) else {
            warn!(
                "abort merge shard task because right shard is missing. group={}, left={}, right={}",
                task.group_id, task.left_shard_id, task.right_shard_id
            );
            return Ok(SchedResult::next());
        };
        if let Err(reason) = check_merge_shards(left, right) {
            warn!(
                "abort merge shard task because shards are not mergeable. group={}, left={}, right={}, reason={reason}",
                task.group_id, task.left_shard_id, task.right_shard_id
            );
            return Ok(SchedResult::next());
        }

        match self.try_merge_shard(task.group_id, task.left_shard_id, task.right_shard_id).await {
            Ok(()) => Ok(SchedResult::next()),
            Err(crate::Error::EpochNotMatch(_)) => {
                warn!(
                    "merge shard meet epoch not match, abort merge shard task. group={}, left={}, right={}",
                    task.group_id, task.left_shard_id, task.right_shard_id
                );
                Ok(SchedResult::next())
            }
            Err(crate::Error::InvalidData(msg)) if msg.contains("not mergeable") => {
                warn!(
                    "abort merge shard task because shards are not mergeable. group={}, left={}, right={}, reason={msg}",
                    task.group_id, task.left_shard_id, task.right_shard_id
                );
                Ok(SchedResult::next())
            }
            Err(err) => {
                warn!(
                    "merge shard fail, retry later: {err:?}. group={}, left={}, right={}",
                    task.group_id, task.left_shard_id, task.right_shard_id
                );
                Err(err)
            }
        }
    }
}

impl ScheduleContext {
    async fn get_group_leader(&self, group_id: u64) -> Result<Option<GroupDesc>> {
        let schema = self.shared.schema()?;
        let group = schema.get_group(group_id).await?;
        Ok(group)
    }

    async fn try_shed_leader_before_remove(
        &self,
        group_id: u64,
        remove_replica: u64,
    ) -> Result<()> {
        let schema = self.shared.schema()?;

        let replica_state = schema
            .get_replica_state(group_id, remove_replica)
            .await?
            .ok_or(crate::Error::AbortScheduleTask("shed leader replica has be destroyed"))?;

        if replica_state.role != RaftRole::Leader as i32 {
            return Ok(());
        }

        let group = self
            .get_group_leader(group_id)
            .await?
            .ok_or(crate::Error::AbortScheduleTask("shed leader group has be destroyed"))?;
        if let Some(target_replica) = group.replicas.iter().find(|e| e.id != remove_replica) {
            // TODO: find least-leader node.
            info!(
                "attempt remove leader replica, so transfer leader to {} in node {}. group={}, replica={}",
                target_replica.id, target_replica.node_id, group.id, remove_replica
            );
            self.try_transfer_leader(group_id, target_replica.id).await?;
        }
        Ok(())
    }

    async fn try_move_replica(
        &self,
        group: u64,
        incoming_replica: ReplicaDesc,
        outgoing_replica: ReplicaDesc,
    ) -> Result<ScheduleState> {
        let mut group_client = self.shared.transport_manager.lazy_group_client(group);
        let current_state =
            group_client.move_replicas(vec![incoming_replica], vec![outgoing_replica]).await?;
        Ok(current_state)
    }

    async fn try_transfer_leader(&self, group: u64, target_replica: u64) -> Result<()> {
        let mut group_client = self.shared.transport_manager.lazy_group_client(group);
        group_client.transfer_leader(target_replica).await?;
        Ok(())
    }

    async fn try_migrate_shard(
        &self,
        task: &MigrateShardTask,
        planned_groups: &[GroupDesc],
    ) -> Result<()> {
        let MigrateShardTask { src_group, dest_group: target_group, shard, .. } = *task;
        let guarded = task.guard() != ShardMoveGuard::Unrestricted;
        let (source, mut group_client) = if guarded {
            let target = planned_groups
                .iter()
                .find(|group| group.id == target_group)
                .ok_or(Error::AbortScheduleTask("migrate target group no longer exists"))?;
            (
                planned_groups.iter().find(|group| group.id == src_group).cloned(),
                self.group_client_at(target),
            )
        } else {
            (
                self.get_group_leader(src_group).await?,
                self.shared.transport_manager.lazy_group_client(target_group),
            )
        };
        let src_group =
            source.ok_or(Error::AbortScheduleTask("migrate source group no longer exists"))?;
        let shard_desc = src_group
            .shards
            .iter()
            .find(|s| s.id == shard)
            .ok_or(crate::Error::AbortScheduleTask("migrate shard has be moved out"))?;

        group_client.accept_shard(src_group.id, src_group.epoch, shard_desc).await?;
        if guarded {
            self.cluster_stats.mark_shard_move_submitted(shard);
        }

        let mut hearbeat_nodes = Vec::new();
        let src_node = self.find_leader_node(target_group).ok().flatten();
        if let Some(node_id) = src_node {
            hearbeat_nodes.push(HeartbeatTask { node_id })
        }
        let target_node = self.find_leader_node(src_group.id).ok().flatten();
        if let Some(node_id) = target_node {
            hearbeat_nodes.push(HeartbeatTask { node_id })
        }
        self.heartbeat_queue.try_schedule(hearbeat_nodes, Instant::now()).await;

        info!(
            "migrate shard submitted, shard: {shard}, from: {}, to: {target_group}, {:?}->{:?}",
            src_group.id, src_node, target_node,
        );
        // TODO: handle src_group epoch not match?
        Ok(())
    }

    /// Split shard request.
    async fn try_split_shard(
        &self,
        group: &GroupDesc,
        old_shard_id: u64,
        new_shard_id: u64,
        split_policy: Option<(Vec<u8>, u64)>,
    ) -> Result<bool> {
        let mut group_client = self.group_client_at(group);
        let split_key = if let Some((split_start_key, split_target_size)) = split_policy {
            let Some(split_key) = group_client
                .get_split_key(old_shard_id, Some(split_start_key), Some(split_target_size))
                .await?
            else {
                return Ok(false);
            };
            Some(split_key)
        } else {
            None
        };
        group_client.split_shard(old_shard_id, new_shard_id, split_key).await?;
        Ok(true)
    }

    fn group_client_at(&self, group: &GroupDesc) -> sekas_client::GroupClient {
        let group_state = sekas_client::RouterGroupState {
            id: group.id,
            epoch: group.epoch,
            replicas: group.replicas.iter().map(|replica| (replica.id, replica.clone())).collect(),
            leader_state: None,
        };
        let client =
            self.shared.transport_manager.build_client(sekas_client::ClientOptions::default());
        sekas_client::GroupClient::new(group_state, client)
    }

    async fn try_merge_shard(
        &self,
        group_id: u64,
        left_shard_id: u64,
        right_shard_id: u64,
    ) -> Result<()> {
        let mut group_client = self.shared.transport_manager.lazy_group_client(group_id);
        group_client.merge_shard(left_shard_id, right_shard_id).await?;
        Ok(())
    }

    fn find_leader_node(&self, group: u64) -> Result<Option<u64>> {
        let group_router = self.shared.transport_manager.find_group(group)?;
        if group_router.leader_state.is_none() {
            return Ok(None);
        }
        let (leader_repl, _) = group_router.leader_state.unwrap();
        Ok(group_router.replicas.iter().find(|(_, r)| r.id == leader_repl).map(|(_, r)| r.node_id))
    }
}

#[cfg(test)]
mod tests {
    use sekas_api::server::v1::{RangePartition, ShardDesc, ShardStats};

    use super::{
        MigrateShardPrecheck, check_merge_shards, is_merge_candidate, precheck_migrate_shard_stats,
    };
    use crate::RootConfig;

    #[test]
    fn split_precheck_rejects_obsolete_tasks_and_recognizes_completion() {
        let mut task = super::SplitShardTask { group_id: 3, shard_id: 1028, ..Default::default() };
        let mut group = super::GroupDesc {
            id: 3,
            epoch: 8,
            shards: vec![ShardDesc::whole(1028, 1024)],
            ..Default::default()
        };
        assert!(!super::precheck_split_shard(&task, Some(&group)).unwrap());
        assert!(matches!(
            super::precheck_split_shard(&task, None),
            Err(crate::Error::AbortScheduleTask(_))
        ));
        group.shards.clear();
        assert!(matches!(
            super::precheck_split_shard(&task, Some(&group)),
            Err(crate::Error::AbortScheduleTask(_))
        ));
        group.shards.push(ShardDesc::whole(1028, 1024));
        task.group_epoch = Some(group.epoch);
        task.new_shard_id = Some(1056);
        group.epoch += 1;
        assert!(matches!(
            super::precheck_split_shard(&task, Some(&group)),
            Err(crate::Error::AbortScheduleTask(_))
        ));
        // A lost response must be treated as success, even if the parent has
        // since moved out and the epoch has advanced.
        group.shards = vec![ShardDesc::whole(1056, 1024)];
        assert!(super::precheck_split_shard(&task, Some(&group)).unwrap());
    }

    #[test]
    fn task_backoff_is_capped_and_explicit_delays_are_preserved() {
        let mut task = super::ReconcileTask::default();
        for seconds in [1, 2, 4, 8, 16, 30, 30] {
            task.defer_retry(1000, None);
            assert_eq!(task.fire_at, 1000 + seconds * 1000);
        }
        task.defer_retry(1000, Some(super::Duration::from_secs(30)));
        assert_eq!(task.fire_at, 31000);
        task.defer_retry(1000, None);
        assert_eq!(task.fire_at, 2000);
        task.retry_count = u32::MAX;
        task.defer_retry(u64::MAX - 100, None);
        assert_eq!(task.fire_at, u64::MAX);
        assert_eq!(task.retry_count, u32::MAX);
    }

    #[sekas_macro::test]
    async fn scheduler_defers_errors_and_does_not_spin_on_delayed_tasks() {
        let dir = crate::testutil::TempDir::new("scheduler-backoff").unwrap();
        let cfg = crate::Config { root_dir: dir.path().to_owned(), ..Default::default() };
        let engines = crate::engine::Engines::open(&cfg.root_dir, &cfg.db).unwrap();
        let transport = crate::transport::TransportManager::new(vec![], engines.state()).await;
        let root = super::Root::new(transport, &crate::serverpb::v1::NodeIdent::default(), cfg);
        let scheduler = &root.scheduler;
        scheduler.sched_split_shard_task(3, 1028).await;
        assert_eq!(scheduler.scheduler_status(super::timestamp_millis()).ready, 1);
        // This root has no leadership/schema, so the real handler fails.
        let before_retry = super::timestamp_millis();
        assert!(!scheduler.advance_tasks().await);
        {
            let mut tasks = scheduler.tasks.lock().await;
            let (id, task) = tasks.front_mut().unwrap();
            assert_eq!(task.retry_count, 1);
            assert!(task.fire_at >= before_retry + 1000);
            task.fire_at = super::timestamp_millis() + 60000;
            scheduler.queue_status.lock().unwrap().finish_attempt(*id, task);
        }
        assert_eq!(scheduler.scheduler_status(super::timestamp_millis()).waiting, 1);
        tokio::time::timeout(super::Duration::from_millis(100), async {
            while scheduler.advance_tasks().await {}
        })
        .await
        .unwrap();
        {
            let mut tasks = scheduler.tasks.lock().await;
            let (id, task) = tasks.front_mut().unwrap();
            assert_eq!(task.retry_count, 1);
            task.fire_at = 0;
            scheduler.queue_status.lock().unwrap().finish_attempt(*id, task);
        }
        let before_retry = super::timestamp_millis();
        assert!(!scheduler.advance_tasks().await);
        let tasks = scheduler.tasks.lock().await;
        let (_, task) = tasks.front().unwrap();
        assert_eq!(task.retry_count, 2);
        assert!(task.fire_at >= before_retry + 2000);
        let status = &scheduler.task_statuses()[0];
        assert_eq!(status.kind, "split_shard");
        assert_eq!(status.target, "group 3, shard 1028");
        assert_eq!(status.retry_count, task.retry_count);
        assert_eq!(status.fire_at, task.fire_at);
    }

    #[test]
    fn migrate_shard_precheck_uses_stats_and_size_limit() {
        let cfg = RootConfig { move_shard_size_limit: 1024, ..Default::default() };

        assert_eq!(precheck_migrate_shard_stats(&cfg, None), MigrateShardPrecheck::WaitStats);
        assert_eq!(
            precheck_migrate_shard_stats(&cfg, Some(1025)),
            MigrateShardPrecheck::Split(None)
        );
        assert_eq!(precheck_migrate_shard_stats(&cfg, Some(1024)), MigrateShardPrecheck::Move);

        let cfg = RootConfig { enable_auto_shard_split: false, ..cfg };
        assert_eq!(precheck_migrate_shard_stats(&cfg, None), MigrateShardPrecheck::Move);
    }

    #[sekas_macro::test]
    async fn scheduler_excludes_overlapping_shard_tasks() {
        let dir = crate::testutil::TempDir::new("scheduler-shard-exclusion").unwrap();
        let cfg = crate::Config { root_dir: dir.path().to_owned(), ..Default::default() };
        let engines = crate::engine::Engines::open(&cfg.root_dir, &cfg.db).unwrap();
        let transport = crate::transport::TransportManager::new(vec![], engines.state()).await;
        let root = super::Root::new(transport, &crate::serverpb::v1::NodeIdent::default(), cfg);
        let scheduler = &root.scheduler;
        let moving = super::ReallocateShard { shard: 1028, source_group: 3, target_group: 4 };
        futures::join!(
            scheduler.sched_migrate_shard_task(moving.clone(), super::ShardMoveGuard::Unrestricted),
            scheduler.sched_split_shard_task(3, 1028),
            scheduler.sched_merge_shard_task(3, 1028, 1029),
        );
        assert_eq!(scheduler.pending_tasks().await, 1);
        assert!(scheduler.ctx.cluster_stats.is_shard_scheduled(1028));
        // A rejected merge must not leave its other shard locked.
        assert!(!scheduler.ctx.cluster_stats.is_shard_scheduled(1029));
        scheduler.sched_merge_shard_task(3, 1029, 1030).await;
        scheduler.sched_split_shard_task(3, 1030).await;
        assert_eq!(scheduler.pending_tasks().await, 2);
        // Queue ownership still excludes duplicates if a heartbeat has
        // already cleared the move marker.
        scheduler.ctx.cluster_stats.finish_move_shard(1028);
        scheduler.sched_migrate_shard_task(moving, super::ShardMoveGuard::Unrestricted).await;
        assert_eq!(scheduler.pending_tasks().await, 2);
    }

    #[test]
    fn merge_candidate_requires_adjacency_size_and_no_schedule_lock() {
        let table_id = sekas_schema::FIRST_USER_TABLE_ID;
        let left = ShardDesc::with_range(1, table_id, b"a".to_vec(), b"m".to_vec());
        let right = ShardDesc::with_range(2, table_id, b"m".to_vec(), b"z".to_vec());
        let left_stats = ShardStats { shard_id: 1, table_id, shard_size: 400, traffic: None };
        let right_stats = ShardStats { shard_id: 2, table_id, shard_size: 600, traffic: None };

        assert!(is_merge_candidate(&left, &right, &left_stats, &right_stats, 1000, false));
        assert!(!is_merge_candidate(&left, &right, &left_stats, &right_stats, 999, false));
        assert!(!is_merge_candidate(&left, &right, &left_stats, &right_stats, 1000, true));

        let mut non_adjacent = right.clone();
        non_adjacent.range = Some(RangePartition { start: b"n".to_vec(), end: b"z".to_vec() });
        assert!(!is_merge_candidate(&left, &non_adjacent, &left_stats, &right_stats, 1000, false));

        let other_table = ShardDesc::with_range(3, table_id + 1, b"m".to_vec(), b"z".to_vec());
        assert!(!is_merge_candidate(&left, &other_table, &left_stats, &right_stats, 1000, false));

        let system_left = ShardDesc::with_range(4, 2, b"a".to_vec(), b"m".to_vec());
        let system_right = ShardDesc::with_range(5, 2, b"m".to_vec(), b"z".to_vec());
        let system_left_stats =
            ShardStats { shard_id: 4, table_id: 2, shard_size: 400, traffic: None };
        let system_right_stats =
            ShardStats { shard_id: 5, table_id: 2, shard_size: 600, traffic: None };
        assert!(!is_merge_candidate(
            &system_left,
            &system_right,
            &system_left_stats,
            &system_right_stats,
            1000,
            false
        ));
    }

    #[test]
    fn merge_precheck_rejects_cross_table_and_non_user_shards() {
        let table_id = sekas_schema::FIRST_USER_TABLE_ID;
        let left = ShardDesc::with_range(1, table_id, b"a".to_vec(), b"m".to_vec());
        let right = ShardDesc::with_range(2, table_id, b"m".to_vec(), b"z".to_vec());
        assert!(check_merge_shards(&left, &right).is_ok());

        let other_table = ShardDesc::with_range(3, table_id + 1, b"m".to_vec(), b"z".to_vec());
        assert_eq!(check_merge_shards(&left, &other_table), Err("different table"));

        let system_left = ShardDesc::with_range(4, 2, b"a".to_vec(), b"m".to_vec());
        let system_right = ShardDesc::with_range(5, 2, b"m".to_vec(), b"z".to_vec());
        assert_eq!(check_merge_shards(&system_left, &system_right), Err("system table"));

        let non_adjacent = ShardDesc::with_range(6, table_id, b"n".to_vec(), b"z".to_vec());
        assert_eq!(check_merge_shards(&left, &non_adjacent), Err("not mergeable"));
    }
}
