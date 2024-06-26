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

mod allocator;
mod bg_job;
mod collector;
mod heartbeat;
mod liveness;
mod metrics;
mod schedule;
mod schema;
mod stats;
mod stmt_executor;
mod store;
mod watch;

use std::collections::*;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::*;
use std::task::Poll;
use std::time::Duration;

use log::{error, info, trace, warn};
use schedule::BackgroundJob;
use sekas_api::server::v1::report_request::GroupUpdates;
use sekas_api::server::v1::watch_response::*;
use sekas_api::server::v1::*;
use sekas_rock::time::timestamp_nanos;
use sekas_runtime::TaskGroup;
use sekas_schema::shard::{SHARD_MAX, SHARD_MIN};
use tokio::time::Instant;
use tokio_util::time::delay_queue;

use self::allocator::SysAllocSource;
use self::bg_job::Jobs;
pub use self::collector::RootCollector;
use self::diagnosis::Metadata;
use self::schedule::ReconcileScheduler;
pub(crate) use self::schema::*;
use self::stats::ClusterStats;
use self::store::RootStore;
pub use self::watch::{WatchHub, Watcher};
use crate::constants::ROOT_GROUP_ID;
use crate::node::{Node, Replica, ReplicaRouteTable};
use crate::serverpb::v1::*;
use crate::transport::TransportManager;
use crate::{Config, Error, Result, RootConfig};

#[derive(Clone)]
pub struct Root {
    cfg: RootConfig,
    shared: Arc<RootShared>,
    alloc: Arc<allocator::Allocator<SysAllocSource>>,
    liveness: Arc<liveness::Liveness>,
    scheduler: Arc<ReconcileScheduler>,
    heartbeat_queue: Arc<HeartbeatQueue>,
    cluster_stats: Arc<ClusterStats>,
    jobs: Arc<Jobs>,
    task_group: TaskGroup,
}

pub struct RootShared {
    transport_manager: TransportManager,
    node_ident: NodeIdent,
    local_addr: String,
    cfg_cpu_nums: u32,
    core: Mutex<Option<RootCore>>,
    watcher_hub: Arc<WatchHub>,
}

impl RootShared {
    pub fn schema(&self) -> Result<Arc<Schema>> {
        let core = self.core.lock().unwrap();
        core.as_ref()
            .map(|c| c.schema.clone())
            .ok_or_else(|| Error::NotRootLeader(RootDesc::default(), 0, None))
    }

    fn root_core(&self) -> Result<RootCore> {
        self.core
            .lock()
            .expect("Poisoned")
            .as_ref()
            .cloned()
            .ok_or_else(|| Error::NotRootLeader(RootDesc::default(), 0, None))
    }
}

#[derive(Clone)]
struct RootCore {
    schema: Arc<Schema>,
    next_txn_id: Arc<AtomicU64>,
    max_txn_id: Arc<AtomicU64>,
}

impl RootCore {
    async fn bump_txn_id(&self) -> Result<()> {
        let txn_id = std::cmp::max(self.max_txn_id.load(Ordering::Relaxed), timestamp_nanos());
        let next_txn_id = txn_id + 5000000000;
        self.schema.set_txn_id(next_txn_id).await?;
        self.max_txn_id.store(next_txn_id, Ordering::Release);
        Ok(())
    }
}

impl Root {
    pub(crate) fn new(
        transport_manager: TransportManager,
        node_ident: &NodeIdent,
        cfg: Config,
    ) -> Self {
        let local_addr = cfg.addr.clone();
        let cfg_cpu_nums = cfg.cpu_nums;
        let cluster_stats = Arc::new(ClusterStats::default());
        let shared = Arc::new(RootShared {
            transport_manager,
            local_addr,
            cfg_cpu_nums,
            core: Mutex::new(None),
            node_ident: node_ident.to_owned(),
            watcher_hub: Default::default(),
        });
        let liveness =
            Arc::new(liveness::Liveness::new(Duration::from_secs(cfg.root.liveness_threshold_sec)));
        let info = Arc::new(SysAllocSource::new(shared.clone(), liveness.to_owned()));
        let alloc =
            Arc::new(allocator::Allocator::new(info, cluster_stats.clone(), cfg.root.to_owned()));
        let heartbeat_queue = Arc::new(HeartbeatQueue::default());
        let jobs =
            Arc::new(Jobs::new(shared.to_owned(), alloc.to_owned(), heartbeat_queue.to_owned()));
        let sched_ctx = schedule::ScheduleContext::new(
            shared.clone(),
            alloc.clone(),
            heartbeat_queue.clone(),
            cluster_stats.clone(),
            jobs.to_owned(),
            cfg.root.to_owned(),
        );
        let scheduler = Arc::new(schedule::ReconcileScheduler::new(sched_ctx));
        Root {
            cfg: cfg.root,
            alloc,
            shared,
            liveness,
            scheduler,
            heartbeat_queue,
            cluster_stats,
            jobs,
            task_group: TaskGroup::default(),
        }
    }

    pub fn is_root(&self) -> bool {
        self.shared.core.lock().unwrap().is_some()
    }

    pub fn current_node_id(&self) -> u64 {
        self.shared.node_ident.node_id
    }

    pub async fn bootstrap(&self, node: &Node) -> Result<Vec<NodeDesc>> {
        let root = self.clone();
        self.task_group.add_task(sekas_runtime::spawn(async move {
            root.run_heartbeat().await;
        }));
        let root = self.clone();
        self.task_group.add_task(sekas_runtime::spawn(async move {
            root.run_background_jobs().await;
        }));
        let replica_table = node.replica_table().clone();
        let root = self.clone();
        self.task_group.add_task(sekas_runtime::spawn(async move {
            root.run_schedule(replica_table).await;
        }));

        if let Some(replica) = node.replica_table().current_root_replica(None) {
            let engine = replica.group_engine();
            Ok(Schema::list_node_raw(engine).await?)
        } else {
            Ok(vec![])
        }
    }

    pub fn schema(&self) -> Result<Arc<Schema>> {
        self.shared.schema()
    }

    pub fn watcher_hub(&self) -> Arc<WatchHub> {
        self.shared.watcher_hub.clone()
    }

    // A Daemon task to:
    // - check root leadership
    // - schedule group/replica/shard
    // - schedule heartbeat sending
    async fn run_schedule(&self, replica_table: ReplicaRouteTable) -> ! {
        let mut bootstrapped = false;
        loop {
            let root_replica = fetch_root_replica(&replica_table).await;

            // Wait the current root replica becomes a leader.
            if let Ok(Some(_)) = root_replica.on_leader("root", false).await {
                match self
                    .step_leader(
                        &self.shared.local_addr,
                        self.shared.cfg_cpu_nums,
                        root_replica,
                        &mut bootstrapped,
                    )
                    .await
                {
                    Ok(()) | Err(Error::NotLeader(..)) => {
                        // Step follower
                        continue;
                    }
                    Err(err) => {
                        todo!("handle error: {}", err)
                    }
                }
            }
        }
    }

    // A Deamon task to finish handle task scheduled in heartbeat_queue and
    // reschedule for next heartbeat.
    async fn run_heartbeat(&self) -> ! {
        loop {
            if let Ok(schema) = self.schema() {
                let _timer = metrics::HEARTBEAT_STEP_DURATION_SECONDS.start_timer();
                let nodes = self.heartbeat_queue.try_poll().await;
                if !nodes.is_empty() {
                    metrics::HEARTBEAT_TASK_QUEUE_SIZE.set(nodes.len() as i64);
                    if let Err(err) = self.send_heartbeat(schema.to_owned(), &nodes).await {
                        warn!("send heartbeat: {err:?}");
                    }
                }
            }
            sekas_runtime::time::sleep(Duration::from_secs(1)).await;
        }
    }

    async fn run_background_jobs(&self) -> ! {
        loop {
            if self.schema().is_ok() {
                if let Err(err) = self.jobs.advance_jobs().await {
                    warn!("run background job: {err:?}");
                    sekas_runtime::time::sleep(Duration::from_secs(3)).await;
                    continue;
                }
                self.jobs.wait_more_jobs().await;
            } else {
                sekas_runtime::time::sleep(Duration::from_secs(1)).await;
            };
        }
    }

    async fn step_leader(
        &self,
        local_addr: &str,
        cfg_cpu_nums: u32,
        root_replica: Arc<Replica>,
        bootstrapped: &mut bool,
    ) -> Result<()> {
        let store = Arc::new(RootStore::new(root_replica.to_owned()));
        let mut schema = Schema::new(store.clone());

        // Only when the program is initialized is it checked for bootstrap, after which
        // the leadership change does not need to check for whether bootstrap or
        // not.
        if !*bootstrapped {
            let cluster_id = self.shared.node_ident.cluster_id.clone();
            if let Err(err) = schema.try_bootstrap_root(local_addr, cfg_cpu_nums, cluster_id).await
            {
                metrics::BOOTSTRAP_FAIL_TOTAL.inc();
                error!("boostrap: {err:?}");
                panic!("boostrap cluster failure")
            }
            *bootstrapped = true;
        }

        let max_txn_id = schema.max_txn_id().await?;
        let root_core = RootCore {
            schema: Arc::new(schema.to_owned()),
            next_txn_id: Arc::new(AtomicU64::new(max_txn_id)),
            max_txn_id: Arc::new(AtomicU64::new(max_txn_id)),
        };
        root_core.bump_txn_id().await?;

        let cloned_root_core = root_core.clone();
        let txn_bumper_handle = sekas_runtime::spawn(async move {
            const INTERVAL: Duration = Duration::from_secs(30);
            loop {
                sekas_runtime::time::sleep(INTERVAL).await;
                if let Err(err) = cloned_root_core.bump_txn_id().await {
                    warn!("bump txn id: {err:?}");
                    break;
                }
            }
        });

        {
            let mut core = self.shared.core.lock().unwrap();
            *core = Some(root_core.clone());
        }
        self::metrics::LEADER_STATE_INFO.set(1);

        self.cluster_stats.reset();
        self.heartbeat_queue.enable(true).await;
        self.jobs.on_step_leader().await?;

        let node_id = self.shared.node_ident.node_id;
        info!(
            "node {node_id} step root service leader, heartbeat_interval: {:?}, liveness_threshold: {:?}",
            self.cfg.heartbeat_interval(),
            Duration::from_secs(self.cfg.liveness_threshold_sec),
        );

        // try schedule a full cluster heartbeat when current node become new root
        // leader.
        let nodes = schema.list_node().await?;
        self.heartbeat_queue
            .try_schedule(
                nodes.iter().map(|n| HeartbeatTask { node_id: n.id }).collect::<Vec<_>>(),
                Instant::now(),
            )
            .await;

        while let Ok(Some(_)) = root_replica.to_owned().on_leader("root", true).await {
            let next_interval = self.scheduler.poll_and_schedule().await;
            sekas_runtime::time::sleep(next_interval).await;
            self.scheduler.wait_one_heartbeat_tick().await;
        }
        info!("node {node_id} current root node drop leader");

        // After that, RootCore needs to be set to None before returning.
        drop(txn_bumper_handle);
        // Notify txn allocators to exit.
        root_core.max_txn_id.store(0, Ordering::Release);
        self.heartbeat_queue.enable(false).await;
        self.jobs.on_drop_leader();
        self.cluster_stats.reset();
        {
            self.liveness.reset();

            let mut core = self.shared.core.lock().unwrap();
            *core = None;
        }

        self::metrics::LEADER_STATE_INFO.set(0);

        Ok(())
    }

    pub async fn cordon_node(&self, node_id: u64) -> Result<()> {
        let schema = self.schema()?;
        let mut node_desc = schema
            .get_node(node_id)
            .await?
            .ok_or_else(|| crate::Error::InvalidArgument("node not found".into()))?;

        let current_status = NodeStatus::from_i32(node_desc.status).unwrap();
        if !matches!(current_status, NodeStatus::Active) {
            return Err(crate::Error::InvalidArgument("node already cordoned".into()));
        }
        node_desc.status = NodeStatus::Cordoned as i32;
        schema.update_node(node_desc).await?; // TODO: cas
        Ok(())
    }

    pub async fn uncordon_node(&self, node_id: u64) -> Result<()> {
        let schema = self.schema()?;
        let mut node_desc = schema
            .get_node(node_id)
            .await?
            .ok_or_else(|| crate::Error::InvalidArgument("node not found".into()))?;

        let current_status = NodeStatus::from_i32(node_desc.status).unwrap();
        if !matches!(
            current_status,
            NodeStatus::Cordoned | NodeStatus::Drained | NodeStatus::Decommissioned
        ) {
            return Err(crate::Error::InvalidArgument("node status unsupport uncordon".into()));
        }

        node_desc.status = NodeStatus::Active as i32;
        schema.update_node(node_desc).await?; // TODO: cas
        Ok(())
    }

    pub async fn begin_drain(&self, node_id: u64) -> Result<()> {
        let schema = self.schema()?;

        if self.current_node_id() == node_id {
            info!("try to drain root leader and move root leadership out first");
            self.scheduler.sched_root_leader(node_id).await;
            return Err(crate::Error::InvalidArgument(
                "node is root leader, try again later".into(),
            ));
        }

        let mut node_desc = schema
            .get_node(node_id)
            .await?
            .ok_or_else(|| crate::Error::InvalidArgument("node not found".into()))?;

        let current_status = NodeStatus::from_i32(node_desc.status).unwrap();
        if !matches!(current_status, NodeStatus::Cordoned) {
            return Err(crate::Error::InvalidArgument(
                "only in cordoned status node can be drain".into(),
            ));
        }

        node_desc.status = NodeStatus::Draining as i32;
        schema.update_node(node_desc).await?; // TODO: cas

        self.scheduler.sched_leader(node_id).await;

        Ok(())
    }

    pub async fn node_status(&self, node_id: u64) -> Result<NodeStatus> {
        let schema = self.schema()?;
        let node_desc = schema
            .get_node(node_id)
            .await?
            .ok_or_else(|| crate::Error::InvalidArgument("node not found".into()))?;

        let current_status = NodeStatus::from_i32(node_desc.status).unwrap();

        Ok(current_status)
    }

    pub async fn nodes(&self) -> Option<u64> {
        if let Ok(schema) = self.shared.schema() {
            if let Ok(nodes) = schema.list_node().await {
                return Some(nodes.len() as u64);
            }
        }
        None
    }

    pub async fn job_state(&self) -> Result<String> {
        use serde_json::json;

        let schema = self.schema()?;
        let ongoing_jobs = schema.list_job().await?;
        let history_jobs = schema.list_history_job().await?;
        let ongoing = ongoing_jobs.iter().map(BackgroundJob::to_json).collect::<Vec<_>>();
        let history = history_jobs.iter().map(BackgroundJob::to_json).collect::<Vec<_>>();
        Ok(json!({"ongoing": ongoing, "history": history}).to_string())
    }

    pub async fn info(&self) -> Result<Metadata> {
        let schema = self.schema()?;
        let nodes = schema.list_node().await?;
        let groups = schema.list_group().await?;
        let replicas = groups
            .iter()
            .filter(|g| g.id != ROOT_GROUP_ID)
            .flat_map(|g| g.replicas.iter().map(|r| (r, g.id)).collect::<Vec<_>>())
            .collect::<Vec<_>>();
        let states = schema.list_replica_state().await?;
        let dbs = schema.list_database().await?;
        let tables = schema.list_table().await?;

        let balanced = !self.scheduler.need_reconcile().await?;

        use diagnosis::*;

        Ok(Metadata {
            nodes: nodes
                .iter()
                .map(|n| {
                    let replicas = replicas
                        .iter()
                        .filter(|(r, _)| r.node_id == n.id)
                        .map(|(r, g)| NodeReplica {
                            id: r.id,
                            group: g.to_owned(),
                            replica_role: r.role,
                            raft_role: states
                                .iter()
                                .find(|s| s.replica_id == r.id)
                                .map(|s| s.role)
                                .unwrap_or(-1),
                        })
                        .collect::<Vec<_>>();
                    let leaders = replicas
                        .iter()
                        .filter(|r| r.raft_role == RaftRole::Leader as i32)
                        .cloned()
                        .collect::<Vec<_>>();
                    Node { id: n.id, addr: n.addr.to_owned(), replicas, leaders, status: n.status }
                })
                .collect::<Vec<_>>(),
            databases: dbs
                .iter()
                .map(|d| Database {
                    id: d.id,
                    name: d.name.to_owned(),
                    tables: tables
                        .iter()
                        .filter(|c| c.db == d.id)
                        .map(|c| Table { id: c.id, name: c.name.to_owned() })
                        .collect::<Vec<_>>(),
                })
                .collect::<Vec<_>>(),
            groups: groups
                .iter()
                .map(|g| Group {
                    id: g.id,
                    epoch: g.epoch,
                    replicas: g
                        .replicas
                        .iter()
                        .map(|r| {
                            let s = states.iter().find(|s| s.replica_id == r.id);
                            GroupReplica {
                                id: r.id,
                                node: r.node_id,
                                replica_role: r.role,
                                raft_role: s.map(|s| s.role).unwrap_or(-1),
                                term: s.map(|s| s.term).unwrap_or(0),
                            }
                        })
                        .collect::<Vec<_>>(),
                    shards: g
                        .shards
                        .iter()
                        .map(|s| {
                            let range = s.range.as_ref().unwrap();
                            let range = format!("range: {:?} to {:?}", range.start, range.end);
                            GroupShard { id: s.id, table: s.table_id, range }
                        })
                        .collect::<Vec<_>>(),
                })
                .collect::<Vec<_>>(),
            balanced,
        })
    }
}

impl Root {
    pub async fn create_database(&self, name: String) -> Result<DatabaseDesc> {
        let desc = self
            .schema()?
            .create_database(DatabaseDesc { name: name.to_owned(), ..Default::default() })
            .await?;
        self.watcher_hub()
            .notify_updates(vec![UpdateEvent {
                event: Some(update_event::Event::Database(desc.to_owned())),
            }])
            .await;
        info!("create database. database_id={}, database={}", desc.id, name);
        Ok(desc)
    }

    pub async fn delete_database(&self, name: &str) -> Result<()> {
        let db = self.get_database(name).await?;
        if db.is_none() {
            return Err(Error::DatabaseNotFound(name.to_owned()));
        }
        let db = db.unwrap();
        if db.id == sekas_schema::system::db::ID {
            return Err(Error::InvalidArgument("not support delete system database".into()));
        }
        self.jobs.submit_purge_database_job(db.id, db.name.to_owned()).await?;
        let schema = self.schema()?;
        let id = schema.delete_database(&db).await?;
        self.watcher_hub()
            .notify_deletes(vec![DeleteEvent { event: Some(delete_event::Event::Database(id)) }])
            .await;
        info!("delete database. database={name}");
        Ok(())
    }

    pub async fn create_table(&self, name: String, database: String) -> Result<TableDesc> {
        let schema = self.schema()?;
        let db = schema
            .get_database(&database)
            .await?
            .ok_or_else(|| Error::DatabaseNotFound(database.to_owned()))?;

        let table = schema
            .prepare_create_table(TableDesc {
                name: name.to_owned(),
                db: db.id,
                properties: sekas_schema::system::table::default_user_properties(),
                ..Default::default()
            })
            .await?;
        info!("prepare create table. database={database}, table={table:?}, table_id={}", table.id);

        self.do_create_table(schema.to_owned(), table.to_owned()).await?;

        self.watcher_hub()
            .notify_updates(vec![UpdateEvent {
                event: Some(update_event::Event::Table(table.to_owned())),
            }])
            .await;

        Ok(table)
    }

    async fn do_create_table(&self, schema: Arc<Schema>, table: TableDesc) -> Result<()> {
        let wait_create = {
            let range = RangePartition { start: SHARD_MIN.to_owned(), end: SHARD_MAX.to_owned() };
            let id = schema.next_shard_id().await?;
            vec![ShardDesc { id, table_id: table.id.to_owned(), range: Some(range) }]
        };

        self.jobs.submit_create_table_job(table, wait_create).await
    }

    pub async fn delete_table(&self, name: &str, database: &DatabaseDesc) -> Result<()> {
        let schema = self.schema()?;
        let db = self
            .get_database(&database.name)
            .await?
            .ok_or_else(|| Error::DatabaseNotFound(database.name.clone()))?;
        let table = schema.get_table(db.id, name).await?;
        if let Some(table) = table {
            let table_id = table.id;
            if table_id < sekas_schema::FIRST_USER_TABLE_ID {
                return Err(Error::InvalidArgument("unsupported delete system table".into()));
            }
            self.jobs.submit_purge_table_job(&db, &table).await?;
            schema.delete_table(table).await?;
            self.watcher_hub()
                .notify_deletes(vec![DeleteEvent {
                    event: Some(delete_event::Event::Table(table_id)),
                }])
                .await;
        }
        info!("delete table, database {}, table={}", database.name, name);
        Ok(())
    }

    pub async fn list_database(&self) -> Result<Vec<DatabaseDesc>> {
        self.schema()?.list_database().await
    }

    pub async fn get_database(&self, name: &str) -> Result<Option<DatabaseDesc>> {
        self.schema()?.get_database(name).await
    }

    pub async fn list_table(&self, database: &DatabaseDesc) -> Result<Vec<TableDesc>> {
        let schema = self.schema()?;
        let db = schema
            .get_database(&database.name)
            .await?
            .ok_or_else(|| Error::DatabaseNotFound(database.name.clone()))?;
        Ok(schema.list_table().await?.iter().filter(|c| c.db == db.id).cloned().collect::<Vec<_>>())
    }

    pub async fn get_table(
        &self,
        name: &str,
        database: &DatabaseDesc,
    ) -> Result<Option<TableDesc>> {
        let db = self
            .get_database(&database.name)
            .await?
            .ok_or_else(|| Error::DatabaseNotFound(database.name.clone()))?;
        self.schema()?.get_table(db.id, name).await
    }

    pub async fn watch(&self, cur_groups: HashMap<u64, u64>) -> Result<Watcher> {
        let schema = self.schema()?;

        let watcher = {
            let hub = self.watcher_hub();
            let (watcher, mut initializer) = hub.create_watcher().await;
            let (updates, deletes) = schema.list_all_events(cur_groups).await?;
            initializer.set_init_resp(updates, deletes);
            watcher
        };
        Ok(watcher)
    }

    pub async fn join(
        &self,
        addr: String,
        capacity: NodeCapacity,
    ) -> Result<(Vec<u8>, NodeDesc, RootDesc)> {
        let schema = self.schema()?;
        let node = schema
            .add_node(NodeDesc { addr, capacity: Some(capacity), ..Default::default() })
            .await?;
        self.watcher_hub()
            .notify_updates(vec![UpdateEvent {
                event: Some(update_event::Event::Node(node.to_owned())),
            }])
            .await;

        let cluster_id = schema.cluster_id().await?.unwrap();
        let mut root = schema.get_root_desc().await?;
        root.root_nodes = {
            let mut nodes = ReplicaNodes(root.root_nodes);
            nodes.move_first(node.id);
            nodes.0
        };
        self.heartbeat_queue
            .try_schedule(vec![HeartbeatTask { node_id: node.id }], Instant::now())
            .await;
        info!("new node join cluster. node={}, addr={}", node.id, node.addr);
        Ok((cluster_id, node, root))
    }

    pub async fn report(&self, updates: Vec<GroupUpdates>) -> Result<()> {
        // mock report doesn't work.
        // return Ok(());

        let cluster_stats = self.cluster_stats.clone();
        let schema = self.schema()?;
        let mut update_events = Vec::new();
        let mut changed_group_states = Vec::new();
        for u in updates {
            let group_desc = if let Some(update_group) = &u.group_desc {
                match schema.get_group(u.group_id).await? {
                    Some(pre_group) if pre_group.epoch >= update_group.epoch => None,
                    _ => u.group_desc,
                }
            } else {
                None
            };

            let replica_state = if let Some(update_replica_state) = &u.replica_state {
                match schema.get_replica_state(u.group_id, update_replica_state.replica_id).await? {
                    Some(pre_rs)
                        if pre_rs.term > update_replica_state.term
                            || (pre_rs.term == update_replica_state.term
                                && pre_rs.role == update_replica_state.role) =>
                    {
                        None
                    }
                    _ => u.replica_state,
                }
            } else {
                None
            };
            schema.update_group_replica(group_desc.to_owned(), replica_state.to_owned()).await?;

            if let Some(sched_state) = u.schedule_state {
                cluster_stats.handle_schedule_update(&[sched_state], None);
            }

            if let Some(desc) = group_desc {
                info!("update group_desc from node report. group={}, desc={:?}", desc.id, desc);
                if desc.id == ROOT_GROUP_ID {
                    self.heartbeat_queue
                        .try_schedule(
                            vec![HeartbeatTask { node_id: self.current_node_id() }],
                            Instant::now(),
                        )
                        .await;
                }
                metrics::ROOT_UPDATE_GROUP_DESC_TOTAL.report.inc();
                update_events.push(UpdateEvent { event: Some(update_event::Event::Group(desc)) })
            }
            if let Some(state) = replica_state {
                info!(
                    "update replica_state from node report. group={}, replica={}, state={:?}",
                    state.group_id, state.replica_id, state
                );
                metrics::ROOT_UPDATE_REPLICA_STATE_TOTAL.report.inc();
                changed_group_states.push(state.group_id);
            }
        }

        let mut states = schema.list_group_state().await?; // TODO: fix poor performance.
        states.retain(|s| changed_group_states.contains(&s.group_id));
        for state in states {
            update_events.push(UpdateEvent { event: Some(update_event::Event::GroupState(state)) })
        }

        self.watcher_hub().notify_updates(update_events).await;

        Ok(())
    }

    pub async fn alloc_replica(
        &self,
        group_id: u64,
        epoch: u64,
        requested_cnt: u64,
    ) -> Result<Vec<ReplicaDesc>> {
        let schema = self.schema()?;
        let group_desc = schema.get_group(group_id).await?.ok_or(Error::GroupNotFound(group_id))?;
        if group_desc.epoch != epoch {
            return Err(Error::InvalidArgument("epoch not match".to_owned()));
        }
        let mut existing_replicas =
            group_desc.replicas.into_iter().map(|r| r.node_id).collect::<HashSet<u64>>();
        let replica_states = schema.group_replica_states(group_id).await?;
        for replica in replica_states {
            existing_replicas.insert(replica.node_id);
        }
        info!("attempt allocate {requested_cnt} replicas for exist group {group_id}");

        let nodes = self
            .alloc
            .allocate_group_replica(existing_replicas.into_iter().collect(), requested_cnt as usize)
            .await?;
        if nodes.len() != requested_cnt as usize {
            warn!("non enough nodes to allocate replicas, exist nodes: {}, requested: {requested_cnt}", nodes.len());
            return Err(Error::ResourceExhausted("no enough nodes".to_owned()));
        }

        let mut replicas = Vec::with_capacity(nodes.len());
        for n in &nodes {
            let replica_id = schema.next_replica_id().await?;
            replicas.push(ReplicaDesc {
                id: replica_id,
                node_id: n.id,
                role: ReplicaRole::Voter.into(),
            });
        }
        info!(
            "advise allocate new group {group_id} replicas in nodes: {:?}",
            replicas.iter().map(|r| r.node_id).collect::<Vec<_>>()
        );
        Ok(replicas)
    }

    pub async fn alloc_txn_id(&self, num_required: u64) -> Result<u64> {
        let root_core = self.shared.root_core()?;
        loop {
            let next_txn_id = root_core.next_txn_id.load(Ordering::Relaxed);
            let max_txn_id = root_core.max_txn_id.load(Ordering::Acquire);
            if max_txn_id == 0 {
                return Err(Error::NotLeader(0, 0, None));
            }

            if next_txn_id + num_required > max_txn_id {
                sekas_runtime::yield_now().await;
                continue;
            }
            if root_core
                .next_txn_id
                .compare_exchange(
                    next_txn_id,
                    next_txn_id + num_required,
                    Ordering::AcqRel,
                    Ordering::Relaxed,
                )
                .is_ok()
            {
                // TODO(walter) ensure leadership before return.
                return Ok(next_txn_id);
            }
        }
    }

    /// List the descripton of groups.
    pub async fn list_group(&self) -> Result<Vec<GroupDesc>> {
        self.schema()?.list_group().await
    }

    /// Get the description of the specified group.
    pub async fn get_group(&self, group_id: u64) -> Result<Option<GroupDesc>> {
        self.schema()?.get_group(group_id).await
    }

    /// List nodes.
    pub async fn list_node(&self) -> Result<Vec<NodeDesc>> {
        self.schema()?.list_node().await
    }

    /// Get the cluster stats.
    #[inline]
    pub fn get_cluster_stats(&self) -> &ClusterStats {
        &self.cluster_stats
    }
}

pub async fn fetch_root_replica(replica_table: &ReplicaRouteTable) -> Arc<Replica> {
    use futures::future::poll_fn;
    poll_fn(|ctx| match replica_table.current_root_replica(Some(ctx.waker().clone())) {
        Some(root_replica) => Poll::Ready(root_replica),
        None => Poll::Pending,
    })
    .await
}

#[derive(Debug)]
pub enum QueueTask {
    Heartbeat(HeartbeatTask),
    Sentinel(Sentinel),
}

#[derive(Debug)]
pub struct HeartbeatTask {
    pub node_id: u64,
}

#[derive(Debug)]
pub struct Sentinel {
    sender: futures::channel::oneshot::Sender<()>,
}

#[derive(Default)]
pub struct HeartbeatQueue {
    core: Arc<futures::lock::Mutex<HeartbeatQueueCore>>,
}

#[derive(Default)]
struct HeartbeatQueueCore {
    enable: bool,
    delay: delay_queue::DelayQueue<QueueTask>,
    node_scheduled: HashMap<u64, (delay_queue::Key, Instant)>,
}

impl HeartbeatQueue {
    pub async fn try_schedule(&self, tasks: Vec<HeartbeatTask>, when: Instant) {
        let mut core = self.core.lock().await;
        if !core.enable {
            return;
        }
        for (i, task) in tasks.into_iter().enumerate() {
            let node = task.node_id;
            if let Some((scheduled_key, old_when)) =
                core.node_scheduled.get(&node).map(ToOwned::to_owned)
            {
                if when < old_when {
                    metrics::HEARTBEAT_RESCHEDULE_EARLY_INTERVAL_SECONDS
                        .observe(old_when.saturating_duration_since(when).as_secs_f64());
                    core.delay.reset_at(&scheduled_key, when);
                    core.node_scheduled.insert(node, (scheduled_key, when));
                    trace!("update next heartbeat. node={node}, when={when:?}");
                }
            } else {
                let key = core.delay.insert_at(QueueTask::Heartbeat(task), when);
                core.node_scheduled.insert(node, (key, when));
                trace!("schedule next heartbeat. node={node}, when={when:?}");
            }
            if i % 10 == 0 {
                sekas_runtime::yield_now().await;
            }
        }
    }

    pub async fn wait_one_heartbeat_tick(&self) {
        let (sender, receiver) = futures::channel::oneshot::channel::<()>();
        let sentinel = Sentinel { sender };
        {
            let mut core = self.core.lock().await;
            if !core.enable {
                return;
            }
            core.delay.insert(QueueTask::Sentinel(sentinel), Duration::from_millis(0));
        }
        let _ = receiver.await;
    }

    async fn try_poll(&self) -> Vec<HeartbeatTask> {
        let mut core = self.core.lock().await;
        if !core.enable {
            return vec![];
        }
        let tasks = futures::future::poll_fn(|cx| {
            let mut tasks = Vec::new();
            while let Poll::Ready(Some(task)) = core.delay.poll_expired(cx) {
                tasks.push(task);
            }
            Poll::Ready(tasks)
        })
        .await;
        let tasks = tasks.into_iter().map(|e| e.into_inner()).collect::<Vec<_>>();
        let mut heartbeats = Vec::new();
        for task in tasks {
            match task {
                QueueTask::Heartbeat(task) => {
                    core.node_scheduled.remove(&task.node_id);
                    heartbeats.push(task);
                }
                QueueTask::Sentinel(sential) => {
                    let _ = sential.sender.send(());
                }
            }
        }
        heartbeats
    }

    async fn enable(&self, enable: bool) {
        let mut core = self.core.lock().await;
        if core.enable != enable {
            core.node_scheduled.clear();
            core.delay.clear();
            core.enable = enable;
        }
    }
}

#[cfg(test)]
mod root_test {
    use futures::StreamExt;
    use sekas_api::server::v1::watch_response::{update_event, UpdateEvent};
    use sekas_api::server::v1::{DatabaseDesc, GroupDesc};
    use sekas_rock::fn_name;
    use tempdir::TempDir;

    use super::Config;
    use crate::bootstrap::bootstrap_cluster;
    use crate::constants::{INITIAL_EPOCH, ROOT_GROUP_ID};
    use crate::engine::Engines;
    use crate::node::Node;
    use crate::root::Root;
    use crate::serverpb::v1::NodeIdent;
    use crate::transport::TransportManager;

    async fn create_root_and_node(config: &Config, node_ident: &NodeIdent) -> (Root, Node) {
        let engines = Engines::open(&config.root_dir, &config.db).unwrap();
        let root_list =
            if config.init { vec![config.addr.clone()] } else { config.join_list.clone() };
        let transport_manager = TransportManager::new(root_list, engines.state()).await;
        let root = Root::new(transport_manager.clone(), node_ident, config.clone());
        let node = Node::new(config.clone(), engines, transport_manager).await.unwrap();
        (root, node)
    }

    #[sekas_macro::test]
    async fn boostrap_root() {
        let tmp_dir = TempDir::new(fn_name!()).unwrap();
        let config = Config { root_dir: tmp_dir.path().to_owned(), ..Default::default() };
        let ident = NodeIdent { cluster_id: vec![], node_id: 1 };

        let (root, node) = create_root_and_node(&config, &ident).await;
        bootstrap_cluster(&node, "0.0.0.0:8888").await.unwrap();
        node.bootstrap(&ident).await.unwrap();
        root.bootstrap(&node).await.unwrap();
        // TODO: test on leader logic later.
    }

    #[sekas_macro::test]
    async fn bootstrap_pending_root_replica() {
        let tmp_dir = TempDir::new(fn_name!()).unwrap();
        let config = Config { root_dir: tmp_dir.path().to_owned(), ..Default::default() };
        let ident = NodeIdent { cluster_id: vec![], node_id: 1 };

        let (root, node) = create_root_and_node(&config, &ident).await;
        node.bootstrap(&ident).await.unwrap();
        node.create_replica(
            3,
            GroupDesc { id: ROOT_GROUP_ID, epoch: INITIAL_EPOCH, shards: vec![], replicas: vec![] },
        )
        .await
        .unwrap();
        root.bootstrap(&node).await.unwrap();
    }

    #[sekas_macro::test]
    async fn watch_hub() {
        let tmp_dir = TempDir::new(fn_name!()).unwrap();
        let ident = NodeIdent { cluster_id: vec![], node_id: 1 };
        let config = Config { root_dir: tmp_dir.path().to_owned(), ..Default::default() };
        let (root, _node) = create_root_and_node(&config, &ident).await;
        let hub = root.watcher_hub();
        let _create_db1_event =
            Some(update_event::Event::Database(DatabaseDesc { id: 1, name: "db1".into() }));
        let mut w = {
            let (w, mut initializer) = hub.create_watcher().await;
            initializer.set_init_resp(vec![UpdateEvent { event: _create_db1_event }], vec![]);
            w
        };
        let resp1 = w.next().await.unwrap().unwrap();
        assert!(matches!(&resp1.updates[0].event, _create_db1_event));

        let mut w2 = {
            let (w, _) = hub.create_watcher().await;
            w
        };

        let _create_db2_event =
            Some(update_event::Event::Database(DatabaseDesc { id: 2, name: "db2".into() }));
        hub.notify_updates(vec![UpdateEvent { event: _create_db2_event }]).await;
        let resp2 = w.next().await.unwrap().unwrap();
        assert!(matches!(&resp2.updates[0].event, _create_db2_event));
        let resp22 = w2.next().await.unwrap().unwrap();
        assert!(matches!(&resp22.updates[0].event, _create_db2_event));
        // hub.notify_error(Error::NotRootLeader(vec![])).await;
    }
}

pub mod diagnosis {
    use serde::{Deserialize, Serialize};

    #[derive(Serialize, Deserialize)]
    pub struct Metadata {
        pub databases: Vec<Database>,
        pub nodes: Vec<Node>,
        pub groups: Vec<Group>,
        pub balanced: bool,
    }

    #[derive(Serialize, Deserialize)]
    pub struct Database {
        pub id: u64,
        pub name: String,
        pub tables: Vec<Table>,
    }

    #[derive(Serialize, Deserialize)]
    pub struct Table {
        pub id: u64,
        pub name: String,
    }

    #[derive(Serialize, Deserialize)]
    pub struct Node {
        pub addr: String,
        pub id: u64,
        pub replicas: Vec<NodeReplica>,
        pub leaders: Vec<NodeReplica>,
        pub status: i32,
    }

    #[derive(Serialize, Deserialize, Clone)]
    pub struct NodeReplica {
        pub group: u64,
        pub id: u64,
        pub raft_role: i32,
        pub replica_role: i32,
    }

    #[derive(Serialize, Deserialize)]
    pub struct Group {
        pub epoch: u64,
        pub id: u64,
        pub replicas: Vec<GroupReplica>,
        pub shards: Vec<GroupShard>,
    }

    #[derive(Serialize, Deserialize)]
    pub struct GroupReplica {
        pub id: u64,
        pub node: u64,
        pub raft_role: i32,
        pub replica_role: i32,
        pub term: u64,
    }

    #[derive(Serialize, Deserialize)]
    pub struct GroupShard {
        pub table: u64,
        pub id: u64,
        pub range: String,
    }
}
