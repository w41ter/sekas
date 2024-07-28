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

mod task;

use log::{debug, error, info, warn};
use prometheus::HistogramTimer;
use sekas_api::server::v1::*;
use sekas_rock::time::timestamp_millis;
use tokio::sync::Mutex;

use self::task::reconcile_task::Task;
pub use self::task::*;
use super::allocator::*;
use super::*;

pub struct ReconcileScheduler {
    ctx: ScheduleContext,
    tasks: Mutex<LinkedList<ReconcileTask>>,
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
        Self { ctx, tasks: Default::default() }
    }

    pub async fn poll_and_schedule(&self) -> Duration {
        let cr = self.generate_schedule_task().await; // TODO: take care self.tasks then can give more > 1 value here.
        if matches!(cr, Ok(true)) {
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
        tasks.push_back(task.to_owned());
        info!("setup new reconcile task. len={}, task={:?}", tasks.len(), task);
    }

    async fn is_empty(&self) -> bool {
        self.tasks.lock().await.is_empty()
    }

    /// Schedule moving root leader task.
    pub async fn sched_root_leader(&self, node_id: u64) {
        self.setup_task(ReconcileTask {
            task: Some(reconcile_task::Task::ShedRoot(ShedRootLeaderTask { node_id })),
            created_at: timestamp_millis(),
            fire_at: 0,
        })
        .await;
    }

    /// Schedule moving leader task.
    pub async fn sched_leader(&self, node_id: u64) {
        self.setup_task(ReconcileTask {
            task: Some(reconcile_task::Task::ShedLeader(ShedLeaderTask { node_id })),
            created_at: timestamp_millis(),
            fire_at: 0,
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
        })
        .await;
    }

    /// Schedule migrate shard task.
    pub async fn sched_migrate_shard_task(&self, action: ReallocateShard) {
        self.setup_task(ReconcileTask {
            task: Some(reconcile_task::Task::MigrateShard(MigrateShardTask {
                shard: action.shard,
                src_group: action.source_group,
                dest_group: action.target_group,
            })),
            created_at: timestamp_millis(),
            fire_at: 0,
        })
        .await;
    }

    /// Schedule split shard task.
    pub async fn sched_split_shard_task(&self, group_id: u64, shard_id: u64) {
        debug!("sched split shard task, group_id {group_id}, shard_id {shard_id}");
        self.setup_task(ReconcileTask {
            task: Some(reconcile_task::Task::SplitShard(SplitShardTask { group_id, shard_id })),
            created_at: timestamp_millis(),
            fire_at: 0,
        })
        .await;
        self.ctx.cluster_stats.handle_split_shard(shard_id);
    }
}

impl ReconcileScheduler {
    pub async fn need_reconcile(&self) -> Result<bool> {
        let group_action = self.ctx.alloc.compute_group_action().await?;
        if matches!(group_action, GroupAction::Add(_)) {
            return Ok(true);
        }

        let actions = self.comput_replica_role_action().await?;
        if !actions.is_empty() {
            return Ok(true);
        }

        let shard_actions = self.ctx.alloc.compute_shard_action().await?;
        if !shard_actions.is_empty() {
            return Ok(true);
        }
        Ok(false)
    }

    pub async fn generate_schedule_task(&self) -> Result<bool> {
        let _timer = super::metrics::RECONCILE_CHECK_DURATION_SECONDS.start_timer();
        let group_action = self.ctx.alloc.compute_group_action().await?;
        if let GroupAction::Add(cnt) = group_action {
            metrics::RECONCILE_ALREADY_BALANCED_INFO.cluster_groups.set(0);
            for _ in 0..cnt {
                self.ctx.bg_jobs.submit_create_group_job().await?;
            }
            return Ok(true);
        }
        metrics::RECONCILE_ALREADY_BALANCED_INFO.cluster_groups.set(1);

        let ractions = self.comput_replica_role_action().await?;
        let sactions = self.ctx.alloc.compute_shard_action().await?;
        for action in ractions {
            match action {
                ReplicaRoleAction::Replica(ReplicaAction::Migrate(action)) => {
                    self.sched_migrate_replica_task(action).await
                }
                ReplicaRoleAction::Leader(LeaderAction::Shed(action)) => {
                    self.sched_transfer_leader_task(action).await
                }
                _ => {}
            }
        }

        for action in sactions {
            match action {
                ShardAction::Migrate(action) => self.sched_migrate_shard_task(action).await,
            }
        }

        for (group_id, shard_id) in self.ctx.cluster_stats.get_large_shards(5) {
            self.sched_split_shard_task(group_id, shard_id).await;
        }

        Ok(!self.is_empty().await)
    }

    pub async fn comput_replica_role_action(&self) -> Result<Vec<ReplicaRoleAction>> {
        let mut actions = Vec::new();
        let replica_actions = self.ctx.alloc.compute_replica_action().await?;
        if replica_actions.is_empty() {
            metrics::RECONCILE_ALREADY_BALANCED_INFO.node_replica_count.set(1);
        } else {
            metrics::RECONCILE_ALREADY_BALANCED_INFO.node_replica_count.set(0);
        }
        actions.extend_from_slice(
            &replica_actions.iter().cloned().map(ReplicaRoleAction::Replica).collect::<Vec<_>>(),
        );
        let leader_actions = self.ctx.alloc.compute_leader_action().await?;
        if leader_actions.is_empty() {
            metrics::RECONCILE_ALREADY_BALANCED_INFO.node_leader_count.set(1);
        } else {
            metrics::RECONCILE_ALREADY_BALANCED_INFO.node_leader_count.set(0);
        }
        actions.extend_from_slice(
            &leader_actions.iter().cloned().map(ReplicaRoleAction::Leader).collect::<Vec<_>>(),
        );
        Ok(actions)
    }
}

impl ReconcileScheduler {
    async fn advance_tasks(&self) -> bool {
        let mut task = self.tasks.lock().await;
        let mut nowait_next = !task.is_empty();
        metrics::RECONCILE_SCHEDULER_TASK_QUEUE_SIZE.set(task.len() as i64);

        let start_at = timestamp_millis();
        let mut cursor = task.cursor_front_mut();
        while let Some(task) = cursor.current() {
            if start_at < task.fire_at {
                cursor.move_next();
                continue;
            }

            let _timer = Self::record_exec(task);
            let Ok(sched_result) = self.ctx.handle_task(task).await else {
                Self::record_retry(task);
                // ack == false or meet error, skip current task and retry later.
                cursor.move_next();
                continue;
            };

            if sched_result.ack {
                cursor.remove_current();
                if !sched_result.immediately_next {
                    nowait_next = false
                }
            } else if let Some(delay) = sched_result.delay {
                Self::record_retry(task);
                task.fire_at = start_at + delay.as_millis() as u64;
                cursor.move_next();
            } else {
                Self::record_retry(task);
                // ack == false or meet error, skip current task and retry later.
                cursor.move_next();
            }
        }
        nowait_next
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

    /// Immediately step next and save the delay intervals.
    fn delay(duration: Duration) -> Self {
        SchedResult { ack: true, immediately_next: true, delay: Some(duration) }
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
  "shed leader meet epoch not match, abort task and retry allocator. group={group}, replica={replica}, new_group={new_group:?}");
                return Ok(SchedResult::ack());
            }
            Err(err) => {
                warn!("shed leader in source replica fail, retry in next tick: {err:?}. group={group}, replica={replica}",
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
        info!(
            "start migrate shard. shard={}, src={}, dest={}",
            task.shard, task.src_group, task.dest_group
        );
        let r = self.try_migrate_shard(task.src_group, task.dest_group, task.shard).await;
        match r {
            Ok(_) => Ok(SchedResult::ack()),
            Err(crate::Error::AbortScheduleTask(reason)) => {
                warn!(
                    "abort migrate shard. shard={}, src={}, dest={}, reason={reason}",
                    task.shard, task.src_group, task.dest_group
                );
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
                        task.group, task.target_replica, new_group);
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

    /// Handle the spliting shard stask and update the sched stats.
    async fn handle_split_shard(&self, task: &mut SplitShardTask) -> Result<SchedResult> {
        let result = self.handle_split_shard_inner(task).await?;
        if result.ack {
            self.cluster_stats.finish_split_shard(task.shard_id);
        }
        Ok(result)
    }

    /// Handle the spliting shard stask.
    async fn handle_split_shard_inner(&self, task: &mut SplitShardTask) -> Result<SchedResult> {
        let schema = self.shared.schema()?;
        if schema.get_group(task.group_id).await?.is_none() {
            warn!("split shard {} but group {} is not exists", task.shard_id, task.group_id);
            return Ok(SchedResult::next());
        };

        let old_shard_id = task.shard_id;
        let new_shard_id = schema.next_shard_id().await?;
        match self.try_split_shard(task.group_id, old_shard_id, new_shard_id).await {
            Ok(_) => Ok(SchedResult::next()),
            Err(crate::Error::EpochNotMatch(_)) => {
                warn!(
                    "split shard meet epoch not match, abort split shard task. group={}, shard={}, new_shard={}",
                        task.group_id, old_shard_id, new_shard_id);
                Ok(SchedResult::next())
            }
            Err(crate::Error::InvalidArgument(msg))
                if msg.contains("shard estimated split keys is empty") =>
            {
                Ok(SchedResult::delay(Duration::from_secs(30)))
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
                target_replica.id,
                target_replica.node_id, group.id, remove_replica
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

    async fn try_migrate_shard(&self, src_group: u64, target_group: u64, shard: u64) -> Result<()> {
        let src_group = self
            .get_group_leader(src_group)
            .await?
            .ok_or(crate::Error::AbortScheduleTask("migrate source group has be destroyed"))?;
        let shard_desc = src_group
            .shards
            .iter()
            .find(|s| s.id == shard)
            .ok_or(crate::Error::AbortScheduleTask("migrate shard has be moved out"))?;

        let mut group_client = self.shared.transport_manager.lazy_group_client(target_group);
        group_client.accept_shard(src_group.id, src_group.epoch, shard_desc).await?;

        let mut hearbeat_nodes = Vec::new();
        let src_node = self.find_leader_node(target_group)?;
        if let Some(node_id) = src_node {
            hearbeat_nodes.push(HeartbeatTask { node_id })
        }
        let target_node = self.find_leader_node(src_group.id)?;
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
        group_id: u64,
        old_shard_id: u64,
        new_shard_id: u64,
    ) -> Result<()> {
        let mut group_client = self.shared.transport_manager.lazy_group_client(group_id);
        group_client.split_shard(old_shard_id, new_shard_id, None).await?;
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
