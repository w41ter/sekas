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

use std::collections::HashSet;
use std::sync::{atomic, Arc, Mutex};
use std::task::{Poll, Waker};
use std::time::Duration;

use futures::future::poll_fn;
use log::{error, info, warn};
use prometheus::HistogramTimer;
use sekas_api::server::v1::*;
use sekas_client::RetryState;
use tokio::time::Instant;

use super::allocator::*;
use super::schedule::background_job::Job;
use super::schedule::*;
use super::{HeartbeatQueue, HeartbeatTask, RootShared, Schema};
use crate::constants::INITIAL_EPOCH;
use crate::root::metrics;
use crate::Result;

pub struct Jobs {
    core: JobCore,
}

impl Jobs {
    pub fn new(
        root_shared: Arc<RootShared>,
        alloc: Arc<Allocator<SysAllocSource>>,
        heartbeat_queue: Arc<HeartbeatQueue>,
    ) -> Self {
        Self {
            core: JobCore {
                root_shared,
                alloc,
                heartbeat_queue,
                mem_jobs: Default::default(),
                res_locks: Default::default(),
                enable: Default::default(),
            },
        }
    }

    pub async fn submit(&self, job: BackgroundJob, wait_result: bool) -> Result<()> {
        self.core.check_root_leader()?;
        let job = self.core.append(job).await?;
        if wait_result {
            self.core.wait_and_check_result(&job.id).await?;
        }
        Ok(())
    }

    pub async fn wait_more_jobs(&self) {
        self.core.wait_more_jobs().await;
    }

    pub async fn advance_jobs(&self) -> Result<()> {
        let jobs = self.core.need_handle_jobs();
        for job in &jobs {
            self.handle_job(job).await?;
        }
        Ok(())
    }

    pub async fn on_step_leader(&self) -> Result<()> {
        self.core.recovery().await?;
        self.core.enable.store(true, atomic::Ordering::Relaxed);
        Ok(())
    }

    pub fn on_drop_leader(&self) {
        self.core.enable.store(false, atomic::Ordering::Relaxed);
        self.core.on_drop_leader()
    }

    async fn handle_job(&self, job: &BackgroundJob) -> Result<()> {
        info!("start background job: {job:?}");
        let r = match job.job.as_ref().unwrap() {
            background_job::Job::CreateTable(create_table) => {
                self.handle_create_table(job, create_table).await
            }
            background_job::Job::CreateOneGroup(create_group) => {
                self.handle_create_one_group(job, create_group).await
            }
            background_job::Job::PurgeTable(purge_table) => {
                self.handle_purge_table(job, purge_table).await
            }
            background_job::Job::PurgeDatabase(purge_database) => {
                self.handle_purge_database(job, purge_database).await
            }
        };
        info!("background job: {job:?}, handle result: {r:?}");
        r
    }

    /// Submit purge database job.
    pub async fn submit_purge_database_job(
        &self,
        database_id: u64,
        database_name: String,
    ) -> Result<()> {
        self.submit(
            BackgroundJob {
                job: Some(Job::PurgeDatabase(PurgeDatabaseJob {
                    database_id,
                    database_name,
                    created_time: format!("{:?}", Instant::now()),
                })),
                ..Default::default()
            },
            false,
        )
        .await
    }

    /// Submit create table job.
    pub async fn submit_create_table_job(
        &self,
        table: TableDesc,
        wait_create: Vec<ShardDesc>,
    ) -> Result<()> {
        self.submit(
            BackgroundJob {
                job: Some(Job::CreateTable(CreateTableJob {
                    database: table.db,
                    table_name: table.name.to_owned(),
                    wait_create,
                    status: CreateTableJobStatus::Creating as i32,
                    desc: Some(table.to_owned()),
                    ..Default::default()
                })),
                ..Default::default()
            },
            true,
        )
        .await
    }

    /// Submit pruge table job.
    pub async fn submit_purge_table_job(&self, db: &DatabaseDesc, table: &TableDesc) -> Result<()> {
        let table_id = table.id;
        let database_name = db.name.to_owned();
        let table_name = table.name.to_owned();
        self.submit(
            BackgroundJob {
                job: Some(Job::PurgeTable(PurgeTableJob {
                    database_id: db.id,
                    table_id,
                    database_name,
                    table_name,
                    created_time: format!("{:?}", Instant::now()),
                })),
                ..Default::default()
            },
            false,
        )
        .await
    }

    /// Submit create group job.
    pub async fn submit_create_group_job(&self) -> Result<()> {
        let status = CreateOneGroupStatus::Init as i32;
        let request_replica_cnt = self.core.alloc.replicas_per_group() as u64;
        self.submit(
            BackgroundJob {
                job: Some(Job::CreateOneGroup(CreateOneGroupJob {
                    request_replica_cnt,
                    status,
                    ..Default::default()
                })),
                ..Default::default()
            },
            true,
        )
        .await
    }
}

impl Jobs {
    // handle create_table.
    async fn handle_create_table(
        &self,
        job: &BackgroundJob,
        create_table: &CreateTableJob,
    ) -> Result<()> {
        let mut create_table = create_table.to_owned();
        loop {
            let status = CreateTableJobStatus::from_i32(create_table.status).unwrap();
            let _timer = Self::record_create_table_step(&status);
            match status {
                CreateTableJobStatus::Creating => {
                    self.handle_wait_create_shard(job.id, &mut create_table).await?;
                }
                CreateTableJobStatus::Rollbacking => {
                    self.handle_wait_cleanup_shard(job.id, &mut create_table).await?;
                }
                CreateTableJobStatus::WriteDesc => {
                    self.handle_write_desc(job.id, &mut create_table).await?;
                }
                CreateTableJobStatus::Finish | CreateTableJobStatus::Abort => {
                    self.handle_finish_create_table(job, create_table).await?;
                    break;
                }
            }
        }
        Ok(())
    }

    async fn handle_wait_create_shard(
        &self,
        job_id: u64,
        create_table: &mut CreateTableJob,
    ) -> Result<()> {
        while let Some(shard) = create_table.wait_create.pop() {
            let groups = self.core.alloc.place_group_for_shard(1).await?;
            if groups.is_empty() {
                return Err(crate::Error::ResourceExhausted("no enough groups".into()));
            }
            let group = groups.first().unwrap();
            info!("try create shard at group {}, shards: {}", group.id, group.shards.len());
            if let Err(err) = self.try_create_shard(group.id, &shard).await {
                error!(
                    "create table shard error and try to rollback: {err:?}. group={}, shard={}",
                    group.id, shard.id
                );
                create_table.remark = format!("{err:?}");
                create_table.wait_cleanup.push(shard);
                create_table.status = CreateTableJobStatus::Rollbacking as i32;
                self.save_create_table(job_id, create_table).await?;
                return Ok(());
            }
            create_table.wait_cleanup.push(shard);
            self.save_create_table(job_id, create_table).await?;
        }
        create_table.status = CreateTableJobStatus::WriteDesc as i32;
        self.save_create_table(job_id, create_table).await?;
        Ok(())
    }

    async fn handle_write_desc(
        &self,
        job_id: u64,
        create_table: &mut CreateTableJob,
    ) -> Result<()> {
        // FIXME(walter) what happen if create_table success but save_create_table
        // failed?
        let schema = self.core.root_shared.schema()?;
        schema.create_table(create_table.desc.as_ref().unwrap().to_owned()).await?;
        create_table.status = CreateTableJobStatus::Finish as i32;
        self.save_create_table(job_id, create_table).await?;
        Ok(())
    }

    async fn handle_wait_cleanup_shard(
        &self,
        job_id: u64,
        create_table: &mut CreateTableJob,
    ) -> Result<()> {
        loop {
            let shard = create_table.wait_cleanup.pop();
            if shard.is_none() {
                break;
            }
            let _ = shard; // TODO: delete shard.
            self.save_create_table(job_id, create_table).await?;
        }
        create_table.status = CreateTableJobStatus::Abort as i32;
        self.save_create_table(job_id, create_table).await?;
        Ok(())
    }

    async fn handle_finish_create_table(
        &self,
        job: &BackgroundJob,
        create_table: CreateTableJob,
    ) -> Result<()> {
        let mut job = job.to_owned();
        job.job = Some(background_job::Job::CreateTable(create_table));
        self.core.finish(job).await?;
        Ok(())
    }

    async fn save_create_table(&self, job_id: u64, create_table: &CreateTableJob) -> Result<()> {
        self.core
            .update(BackgroundJob {
                id: job_id,
                job: Some(background_job::Job::CreateTable(create_table.to_owned())),
            })
            .await?;
        Ok(())
    }

    fn record_create_table_step(step: &CreateTableJobStatus) -> Option<HistogramTimer> {
        match step {
            CreateTableJobStatus::Creating => {
                Some(metrics::RECONCILE_CREATE_TABLE_STEP_DURATION_SECONDS.create.start_timer())
            }
            CreateTableJobStatus::Rollbacking => {
                Some(metrics::RECONCILE_CREATE_TABLE_STEP_DURATION_SECONDS.rollback.start_timer())
            }
            CreateTableJobStatus::WriteDesc => {
                Some(metrics::RECONCILE_CREATE_TABLE_STEP_DURATION_SECONDS.write_desc.start_timer())
            }
            CreateTableJobStatus::Finish | CreateTableJobStatus::Abort => {
                Some(metrics::RECONCILE_CREATE_TABLE_STEP_DURATION_SECONDS.finish.start_timer())
            }
        }
    }
}

impl Jobs {
    // handle create_one_group
    async fn handle_create_one_group(
        &self,
        job: &BackgroundJob,
        create_group: &CreateOneGroupJob,
    ) -> Result<()> {
        let mut create_group = create_group.to_owned();
        loop {
            let status = CreateOneGroupStatus::from_i32(create_group.status).unwrap();
            let _timer = Self::record_create_group_step(&status);
            match status {
                CreateOneGroupStatus::Init => {
                    self.handle_init_create_group_replicas(job.id, &mut create_group).await?
                }
                CreateOneGroupStatus::Creating => {
                    self.handle_wait_create_group_replicas(job.id, &mut create_group).await?
                }
                CreateOneGroupStatus::Rollbacking => {
                    self.handle_rollback_group_replicas(job.id, &mut create_group).await?
                }

                CreateOneGroupStatus::Finish | CreateOneGroupStatus::Abort => {
                    return self.handle_finish_create_group(job, create_group).await
                }
            }
        }
    }

    async fn check_is_already_meet_requirement(
        &self,
        job_id: u64,
        create_group: &mut CreateOneGroupJob,
        schema: Arc<Schema>,
    ) -> Result<bool> {
        if create_group.group_desc.is_none() {
            return Ok(false);
        }
        let replicas =
            schema.group_replica_states(create_group.group_desc.as_ref().unwrap().id).await?;
        if replicas.len() < create_group.request_replica_cnt as usize {
            return Ok(false);
        }
        warn!(
            "cluster group count already meet requirement, so abort group creation. group={}",
            create_group.group_desc.as_ref().unwrap().id,
        );
        create_group.status = CreateOneGroupStatus::Rollbacking as i32;
        self.save_create_group(job_id, create_group).await?;
        Ok(true)
    }

    async fn handle_init_create_group_replicas(
        &self,
        job_id: u64,
        create_group: &mut CreateOneGroupJob,
    ) -> Result<()> {
        let schema = self.core.root_shared.schema()?;
        if self.check_is_already_meet_requirement(job_id, create_group, schema.to_owned()).await? {
            return Ok(());
        }
        let nodes = self
            .core
            .alloc
            .allocate_group_replica(vec![], create_group.request_replica_cnt as usize)
            .await?;
        let group_id = schema.next_group_id().await?;
        let mut replicas = Vec::new();
        for n in &nodes {
            let replica_id = schema.next_replica_id().await?;
            replicas.push(ReplicaDesc {
                id: replica_id,
                node_id: n.id,
                role: ReplicaRole::Voter.into(),
            });
        }
        let group_desc = GroupDesc { id: group_id, epoch: INITIAL_EPOCH, shards: vec![], replicas };
        create_group.group_desc = Some(group_desc);
        create_group.wait_create = nodes;
        create_group.status = CreateOneGroupStatus::Creating as i32;
        self.save_create_group(job_id, create_group).await
    }

    async fn handle_wait_create_group_replicas(
        &self,
        job_id: u64,
        create_group: &mut CreateOneGroupJob,
    ) -> Result<()> {
        let schema = self.core.root_shared.schema()?;
        if self.check_is_already_meet_requirement(job_id, create_group, schema).await? {
            return Ok(());
        }
        let mut wait_create = create_group.wait_create.to_owned();
        let group_desc = create_group.group_desc.as_ref().unwrap().to_owned();
        let mut undo = Vec::new();
        loop {
            let n = wait_create.pop();
            if n.is_none() {
                break;
            }
            let n = n.unwrap();
            let replica = group_desc.replicas.iter().find(|r| r.node_id == n.id).unwrap();
            if let Err(err) =
                self.try_create_replica(&n.addr, &replica.id, group_desc.to_owned()).await
            {
                let retried = create_group.create_retry;
                if retried < 20 {
                    warn!(
                        "create replica for new group error, retry in next: {err:?}. node={}, replica={}, group={}, retried={}",
                        n.id, replica.id, group_desc.id, retried
                    );
                    metrics::RECONCILE_RETRY_TASK_TOTAL.create_group.inc();
                    create_group.create_retry += 1;
                } else {
                    warn!(
                        "create replica for new group error, start rollback: {err:?}. node={}, replica={}, group={}", 
                        n.id, replica.id, group_desc.id);
                    create_group.status = CreateOneGroupStatus::Rollbacking as i32;
                };
                self.save_create_group(job_id, create_group).await?;
                continue;
            }
            undo.push(replica.to_owned());
            create_group.wait_create.clone_from(&wait_create);
            create_group.wait_cleanup.clone_from(&undo);
            self.save_create_group(job_id, create_group).await?;
        }
        create_group.status = CreateOneGroupStatus::Finish as i32;
        self.save_create_group(job_id, create_group).await?;
        Ok(())
    }

    async fn handle_rollback_group_replicas(
        &self,
        job_id: u64,
        create_group: &mut CreateOneGroupJob,
    ) -> Result<()> {
        let mut wait_clean = create_group.wait_cleanup.to_owned();
        loop {
            let r = wait_clean.pop();
            if r.is_none() {
                break;
            }
            let group = create_group.group_desc.as_ref().unwrap().id;
            let r = r.unwrap();
            if let Err(err) = self.try_remove_replica(group, r.id).await {
                error!(
                    "rollback temp replica of new group fail and retry later: {err:?}. replica={}",
                    r.id
                );
                create_group.wait_cleanup.clone_from(&wait_clean);
                self.save_create_group(job_id, create_group).await?;
                return Err(err);
            }
        }
        create_group.status = CreateOneGroupStatus::Abort as i32;
        self.save_create_group(job_id, create_group).await
    }

    async fn save_create_group(&self, job_id: u64, create_group: &CreateOneGroupJob) -> Result<()> {
        self.core
            .update(BackgroundJob {
                id: job_id,
                job: Some(background_job::Job::CreateOneGroup(create_group.to_owned())),
            })
            .await?;
        Ok(())
    }

    async fn handle_finish_create_group(
        &self,
        job: &BackgroundJob,
        create_group: CreateOneGroupJob,
    ) -> Result<()> {
        if matches!(
            CreateOneGroupStatus::from_i32(create_group.status).unwrap(),
            CreateOneGroupStatus::Finish
        ) {
            self.core
                .heartbeat_queue
                .try_schedule(
                    create_group
                        .invoked_nodes
                        .iter()
                        .cloned()
                        .map(|node_id| HeartbeatTask { node_id })
                        .collect(),
                    Instant::now(),
                )
                .await;
        }
        let mut job = job.to_owned();
        job.job = Some(background_job::Job::CreateOneGroup(create_group));
        self.core.finish(job).await?;
        Ok(())
    }

    fn record_create_group_step(step: &CreateOneGroupStatus) -> Option<HistogramTimer> {
        match step {
            CreateOneGroupStatus::Init => {
                Some(metrics::RECONCILE_CREATE_GROUP_STEP_DURATION_SECONDS.init.start_timer())
            }
            CreateOneGroupStatus::Creating => {
                Some(metrics::RECONCILE_CREATE_GROUP_STEP_DURATION_SECONDS.create.start_timer())
            }
            CreateOneGroupStatus::Rollbacking => {
                Some(metrics::RECONCILE_CREATE_GROUP_STEP_DURATION_SECONDS.rollback.start_timer())
            }
            CreateOneGroupStatus::Finish | CreateOneGroupStatus::Abort => {
                Some(metrics::RECONCILE_CREATE_GROUP_STEP_DURATION_SECONDS.finish.start_timer())
            }
        }
    }
}

impl Jobs {
    async fn handle_purge_table(
        &self,
        job: &BackgroundJob,
        purge_table: &PurgeTableJob,
    ) -> Result<()> {
        let schema = self.core.root_shared.schema()?;
        let mut group_shards = schema.get_table_shards(purge_table.table_id).await?;
        loop {
            if let Some((group, shard)) = group_shards.pop() {
                self.try_remove_shard(group, shard.id).await?;
                continue;
            }
            break;
        }
        self.core.finish(job.to_owned()).await?;
        Ok(())
    }

    async fn handle_purge_database(
        &self,
        job: &BackgroundJob,
        purge_database: &PurgeDatabaseJob,
    ) -> Result<()> {
        let schema = self.core.root_shared.schema()?;
        let mut tables = schema.list_database_tables(purge_database.database_id).await?;
        loop {
            if let Some(co) = tables.pop() {
                let job = BackgroundJob {
                    job: Some(Job::PurgeTable(PurgeTableJob {
                        database_id: co.db,
                        table_id: co.id,
                        database_name: "".to_owned(),
                        table_name: co.name.to_owned(),
                        created_time: format!("{:?}", Instant::now()),
                    })),
                    ..Default::default()
                };
                match self.submit(job, false).await {
                    Ok(_) => {}
                    Err(crate::Error::AlreadyExists(_)) => {}
                    Err(err) => return Err(err),
                };
                schema.delete_table(co).await?;
                continue;
            }
            break;
        }
        self.core.finish(job.to_owned()).await?;
        Ok(())
    }
}

impl Jobs {
    async fn try_create_shard(&self, group_id: u64, desc: &ShardDesc) -> Result<()> {
        let mut group_client = self.core.root_shared.transport_manager.lazy_group_client(group_id);
        let mut retry_state = RetryState::new(Duration::from_secs(10));
        loop {
            match group_client.create_shard(desc).await {
                Ok(()) => return Ok(()),
                Err(err) => {
                    retry_state.retry(err).await?;
                }
            }
        }
    }

    async fn try_create_replica(
        &self,
        addr: &str,
        replica_id: &u64,
        group: GroupDesc,
    ) -> Result<()> {
        let client = self.core.root_shared.transport_manager.get_node_client(addr.to_owned())?;
        client.create_replica(replica_id.to_owned(), group).await?;
        Ok(())
    }

    async fn try_remove_replica(&self, group: u64, replica: u64) -> Result<()> {
        let schema = self.core.root_shared.schema()?;
        let rs = schema
            .get_replica_state(group, replica)
            .await?
            .ok_or(crate::Error::AbortScheduleTask("source replica already has be destroyed"))?;

        let target_node = schema
            .get_node(rs.node_id.to_owned())
            .await?
            .ok_or(crate::Error::AbortScheduleTask("source node not exist"))?;
        let client =
            self.core.root_shared.transport_manager.get_node_client(target_node.addr.to_owned())?;
        client
            .remove_replica(replica.to_owned(), GroupDesc { id: group, ..Default::default() })
            .await?;
        schema.remove_replica_state(group, replica).await?;
        Ok(())
    }

    async fn try_remove_shard(&self, _group: u64, _shard: u64) -> Result<()> {
        // TODO: impl remove shard.
        Ok(())
    }
}

struct JobCore {
    root_shared: Arc<RootShared>,
    mem_jobs: Arc<Mutex<MemJobs>>,
    res_locks: Arc<Mutex<HashSet<Vec<u8>>>>,
    alloc: Arc<Allocator<SysAllocSource>>,
    heartbeat_queue: Arc<HeartbeatQueue>,
    enable: atomic::AtomicBool,
}

#[derive(Default)]
struct MemJobs {
    jobs: Vec<BackgroundJob>,
    removed_wakers: Vec<Waker>,
    added_wakers: Vec<Waker>,
}

impl JobCore {
    fn check_root_leader(&self) -> Result<()> {
        if !self.enable.load(atomic::Ordering::Relaxed) {
            return Err(crate::Error::NotRootLeader(RootDesc::default(), 0, None));
        }
        Ok(())
    }

    pub async fn recovery(&self) -> Result<()> {
        let schema = self.root_shared.schema()?;
        let jobs = schema.list_job().await?;
        {
            let mut mem_jobs = self.mem_jobs.lock().unwrap();
            mem_jobs.jobs.clear();
            let wakers = std::mem::take(&mut mem_jobs.removed_wakers);
            for waker in wakers {
                waker.wake();
            }
            mem_jobs.jobs.extend_from_slice(&jobs);
            let wakers = std::mem::take(&mut mem_jobs.added_wakers);
            for waker in wakers {
                waker.wake();
            }
        }
        {
            let mut res_locks = self.res_locks.lock().unwrap();
            res_locks.clear();
            for job in &jobs {
                if let Some(key) = res_key(job) {
                    res_locks.insert(key);
                }
            }
        }
        Ok(())
    }

    pub fn on_drop_leader(&self) {
        {
            let mut mem_jobs = self.mem_jobs.lock().unwrap();
            let wakers = std::mem::take(&mut mem_jobs.removed_wakers);
            for waker in wakers {
                waker.wake();
            }
            let wakers = std::mem::take(&mut mem_jobs.added_wakers);
            for waker in wakers {
                waker.wake();
            }
        }
        {
            let mut res_locks = self.res_locks.lock().unwrap();
            res_locks.clear();
        }
    }

    pub async fn append(&self, job: BackgroundJob) -> Result<BackgroundJob> {
        let schema = self.root_shared.schema()?;
        if let Some(res_key) = res_key(&job) {
            if !self.try_lock_res(res_key) {
                return Err(crate::Error::AlreadyExists(
                    "job for target resource already exist".into(),
                ));
            }
        }
        let job = schema.append_job(job).await?;
        {
            let mut mem_jobs = self.mem_jobs.lock().unwrap();
            mem_jobs.jobs.push(job.to_owned());
            let wakers = std::mem::take(&mut mem_jobs.added_wakers);
            for waker in wakers {
                waker.wake();
            }
        }
        Ok(job)
    }

    pub async fn finish(&self, job: BackgroundJob) -> Result<()> {
        let schema = self.root_shared.schema()?;
        schema.remove_job(&job).await?;
        {
            let mut mem_jobs = self.mem_jobs.lock().unwrap();
            mem_jobs.jobs.retain(|j| j.id != job.id);
            let wakers = std::mem::take(&mut mem_jobs.removed_wakers);
            for waker in wakers {
                waker.wake();
            }
        }
        if let Some(res_key) = res_key(&job) {
            self.unlock_res(&res_key);
        }
        Ok(())
    }

    pub async fn update(&self, job: BackgroundJob) -> Result<()> {
        let schema = self.root_shared.schema()?;
        let updated = schema.update_job(job.to_owned()).await?;
        if updated {
            let mut mem_jobs = self.mem_jobs.lock().unwrap();
            if let Some(idx) = mem_jobs.jobs.iter().position(|j| j.id == job.id) {
                let _ = std::mem::replace(&mut mem_jobs.jobs[idx], job);
            }
        }
        Ok(())
    }

    pub async fn wait_more_jobs(&self) {
        poll_fn(|ctx| {
            let mut mem_jobs = self.mem_jobs.lock().unwrap();
            if mem_jobs.jobs.is_empty() {
                mem_jobs.added_wakers.push(ctx.waker().clone());
                Poll::Pending
            } else {
                Poll::Ready(())
            }
        })
        .await;
    }

    pub async fn wait_and_check_result(&self, id: &u64) -> Result<()> {
        poll_fn(|ctx| {
            let mut mem_jobs = self.mem_jobs.lock().unwrap();
            if mem_jobs.jobs.iter().any(|j| j.id == *id) {
                mem_jobs.removed_wakers.push(ctx.waker().clone());
                Poll::Pending
            } else {
                Poll::Ready(())
            }
        })
        .await;

        let job = self.get_history(id).await?;
        if job.is_none() {
            self.check_root_leader()?;
            unreachable!()
        }
        match job.unwrap().job.as_ref().unwrap() {
            background_job::Job::CreateTable(job) => {
                match CreateTableJobStatus::from_i32(job.status).unwrap() {
                    CreateTableJobStatus::Finish => Ok(()),
                    CreateTableJobStatus::Abort => Err(crate::Error::InvalidArgument(format!(
                        "create table fail {}",
                        job.remark
                    ))),
                    _ => unreachable!(),
                }
            }
            background_job::Job::CreateOneGroup(job) => {
                match CreateOneGroupStatus::from_i32(job.status).unwrap() {
                    CreateOneGroupStatus::Finish => Ok(()),
                    CreateOneGroupStatus::Abort => {
                        Err(crate::Error::InvalidArgument("create group fail".into()))
                    }
                    _ => unreachable!(),
                }
            }
            _ => unreachable!(),
        }
    }

    pub async fn get_history(&self, id: &u64) -> Result<Option<BackgroundJob>> {
        let schema = self.root_shared.schema()?;
        schema.get_job_history(id).await
    }

    pub fn need_handle_jobs(&self) -> Vec<BackgroundJob> {
        let jobs = self.mem_jobs.lock().unwrap();
        jobs.jobs.to_owned()
    }

    fn try_lock_res(&self, res_key: Vec<u8>) -> bool {
        let mut res_locks = self.res_locks.lock().unwrap();
        res_locks.insert(res_key)
    }

    fn unlock_res(&self, res_key: &[u8]) {
        let mut res_locks = self.res_locks.lock().unwrap();
        res_locks.remove(res_key);
    }
}

fn res_key(job: &BackgroundJob) -> Option<Vec<u8>> {
    match job.job.as_ref().unwrap() {
        background_job::Job::CreateTable(job) => {
            let mut key = job.database.to_le_bytes().to_vec();
            key.extend_from_slice(job.table_name.as_bytes());
            Some(key)
        }
        background_job::Job::PurgeTable(job) => {
            let mut key = job.database_id.to_le_bytes().to_vec();
            key.extend_from_slice(job.table_name.as_bytes());
            Some(key)
        }
        background_job::Job::CreateOneGroup(_) | background_job::Job::PurgeDatabase(_) => None,
    }
}
