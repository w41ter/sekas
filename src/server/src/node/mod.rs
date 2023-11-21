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

pub mod metrics;

pub mod job;
pub mod migrate;
pub mod route_table;

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use futures::channel::mpsc;
use futures::lock::Mutex;
use log::{debug, info, warn};
use sekas_api::server::v1::*;
use sekas_client::ClientOptions;
use sekas_runtime::TaskGroup;

use self::job::StateChannel;
use self::migrate::MigrateController;
pub use self::route_table::{RaftRouteTable, ReplicaRouteTable};
use crate::constants::ROOT_GROUP_ID;
use crate::engine::{Engines, GroupEngine, RawDb, StateEngine};
use crate::raftgroup::snap::RecycleSnapMode;
use crate::raftgroup::{ChannelManager, RaftGroup, RaftManager, SnapManager};
use crate::replica::fsm::GroupStateMachine;
pub use crate::replica::Replica;
use crate::replica::{ExecCtx, LeaseState, LeaseStateObserver, ReplicaInfo};
use crate::schedule::MoveReplicasProvider;
use crate::serverpb::v1::*;
use crate::transport::TransportManager;
use crate::{Config, EngineConfig, Error, NodeConfig, Result};

struct ReplicaContext {
    #[allow(dead_code)]
    info: Arc<ReplicaInfo>,
    task_group: TaskGroup,
}

/// A structure holds the states of node. Eg create replica.
#[derive(Default)]
struct NodeState
where
    Self: Send,
{
    ident: Option<NodeIdent>,

    serving_replicas: HashMap<u64, ReplicaContext>,

    /// `serving_groups` used to record the groups that has replicas being
    /// served on this node. Only one replica of a group is allowed on a
    /// node.
    serving_groups: HashSet<u64>,

    root: RootDesc,
    channel: Option<Arc<StateChannel>>,
}

/// Node is used to manage replicas lifecycle, and provides replica query.
pub struct Node
where
    Self: Send + Sync,
{
    cfg: NodeConfig,
    raft_route_table: RaftRouteTable,
    replica_route_table: ReplicaRouteTable,

    raft_mgr: Arc<RaftManager>,
    migrate_ctrl: MigrateController,
    transport_manager: TransportManager,
    engines: Engines,
    state_engine: StateEngine,
    task_group: TaskGroup,

    /// Node related metadata, including serving replicas, root desc.
    node_state: Arc<Mutex<NodeState>>,

    /// A lock is used to ensure serialization of create/terminate replica
    /// operations.
    replica_mutation: Arc<Mutex<()>>,
}

impl Node {
    pub(crate) async fn new(
        cfg: Config,
        engines: Engines,
        transport_manager: TransportManager,
    ) -> Result<Self> {
        let raft_route_table = RaftRouteTable::new();
        let trans_mgr = Arc::new(ChannelManager::new(
            transport_manager.address_resolver(),
            raft_route_table.clone(),
        ));
        let snap_dir = engines.snap_dir();
        let snap_mgr = SnapManager::recovery(snap_dir).await?;
        let raft_mgr = Arc::new(
            RaftManager::open(cfg.raft.clone(), engines.log(), snap_mgr, trans_mgr).await?,
        );
        let migrate_ctrl = MigrateController::new(cfg.node.clone(), transport_manager.clone());
        let state_engine = engines.state();
        Ok(Node {
            cfg: cfg.node,
            transport_manager,
            raft_route_table,
            replica_route_table: ReplicaRouteTable::new(),
            raft_mgr,
            migrate_ctrl,
            engines,
            state_engine,
            task_group: TaskGroup::default(),
            node_state: Arc::new(Mutex::new(NodeState::default())),
            replica_mutation: Arc::default(),
        })
    }

    /// Bootstrap node and recover alive replicas.
    pub async fn bootstrap(&self, node_ident: &NodeIdent) -> Result<()> {
        use self::job::*;

        let mut node_state = self.node_state.lock().await;
        debug_assert!(
            node_state.serving_replicas.is_empty(),
            "some replicas are serving before recovery?"
        );

        node_state.ident = Some(node_ident.to_owned());
        let state_channel = Arc::new(setup_report_state(&self.transport_manager));

        let node_id = node_ident.node_id;
        for (group_id, replica_id, state) in self.state_engine.replica_states().await? {
            if state == ReplicaLocalState::Terminated {
                let destory_replica_handle =
                    setup_destory_replica(group_id, replica_id, self.engines.clone());
                self.task_group.add_task(destory_replica_handle);
            }
            if matches!(state, ReplicaLocalState::Tombstone | ReplicaLocalState::Terminated) {
                self.raft_mgr
                    .snapshot_manager()
                    .recycle_snapshots(replica_id, RecycleSnapMode::All);
                continue;
            }

            let desc = ReplicaDesc { id: replica_id, node_id, ..Default::default() };
            let context = self.serve_replica(group_id, desc, state, state_channel.clone()).await?;
            node_state.serving_replicas.insert(replica_id, context);
            node_state.serving_groups.insert(group_id);
        }
        node_state.channel = Some(state_channel);

        Ok(())
    }

    /// Create a replica. If this node has been bootstrapped, start the replica.
    ///
    /// The replica state is determined by the `GroupDesc`.
    ///
    /// NOTE: This function is idempotent.
    pub async fn create_replica(&self, replica_id: u64, group: GroupDesc) -> Result<()> {
        info!(
            "create replica {replica_id} group {} with {} members",
            group.id,
            group.replicas.len()
        );

        let group_id = group.id;
        let _mut_guard = self.replica_mutation.lock().await;
        if self.check_replica_existence(group_id, replica_id).await? {
            return Ok(());
        }

        // To ensure crash-recovery consistency, first create raft metadata, and then
        // save replica state. In this way, even if the node is restarted before
        // the group is successfully created, a replica can be recreated by
        // retrying.
        Replica::create(replica_id, &group, &self.raft_mgr.cfg, &self.raft_mgr.engine()).await?;
        self.state_engine
            .save_replica_state(group_id, replica_id, ReplicaLocalState::Initial)
            .await?;

        info!("group {group_id} create replica {replica_id} and write initial state success");

        // If this node has not completed initialization, then there is no need to
        // record `ReplicaInfo`. Because the recovery operation will be
        // performed later, `ReplicaMeta` will be read again and the
        // corresponding `ReplicaInfo` will be created.
        //
        // See `Node::bootstrap` for details.
        let mut node_state = self.node_state.lock().await;
        if node_state.is_bootstrapped() {
            let node_id = node_state.ident.as_ref().unwrap().node_id;
            let desc = ReplicaDesc { id: replica_id, node_id, ..Default::default() };
            let context = self
                .serve_replica(
                    group_id,
                    desc,
                    ReplicaLocalState::Initial,
                    node_state.channel.as_ref().unwrap().clone(),
                )
                .await?;
            node_state.serving_replicas.insert(replica_id, context);
            node_state.serving_groups.insert(group_id);
        }

        Ok(())
    }

    async fn check_replica_existence(&self, group_id: u64, replica_id: u64) -> Result<bool> {
        let node_state = self.node_state.lock().await;
        if node_state.serving_replicas.contains_key(&replica_id) {
            debug!("group {group_id} create replica {replica_id}: already exists");
            return Ok(true);
        }

        if node_state.serving_groups.contains(&group_id) {
            warn!("group {group_id} create replica {replica_id}: already exists another replica");
            return Err(Error::AlreadyExists(format!("group {group_id}")));
        }

        Ok(false)
    }

    /// Remove the specified replica.
    pub async fn remove_replica(&self, replica_id: u64, actual_desc: &GroupDesc) -> Result<()> {
        let _mut_guard = self.replica_mutation.lock().await;

        let group_id = actual_desc.id;
        debug!("group {group_id} remove replica {replica_id}");
        let replica = match self.replica_route_table.find(group_id) {
            Some(replica) => replica,
            None => {
                warn!("group {group_id} remove replica {replica_id}: replica not existed");
                return Ok(());
            }
        };

        replica.shutdown(actual_desc).await?;
        self.replica_route_table.remove(group_id);
        self.raft_route_table.delete(replica_id);

        let task_group = {
            let mut node_state = self.node_state.lock().await;
            node_state.serving_groups.remove(&group_id);
            let ctx = node_state
                .serving_replicas
                .remove(&replica_id)
                .expect("replica should exists before removing");
            ctx.task_group
        };
        drop(task_group);

        // This replica is shutdowned, we need to update and persisted states.
        self.state_engine
            .save_replica_state(group_id, replica_id, ReplicaLocalState::Terminated)
            .await?;

        self.raft_mgr.snapshot_manager().recycle_snapshots(replica_id, RecycleSnapMode::All);

        // Clean group engine data in asynchronously.
        let destory_replica_handle =
            self::job::setup_destory_replica(group_id, replica_id, self.engines.clone());
        self.task_group.add_task(destory_replica_handle);

        info!("group {group_id} remove replica {replica_id} success");

        Ok(())
    }

    /// Open, recover replica and start serving.
    async fn serve_replica(
        &self,
        group_id: u64,
        desc: ReplicaDesc,
        local_state: ReplicaLocalState,
        channel: Arc<StateChannel>,
    ) -> Result<ReplicaContext> {
        use crate::schedule::setup_scheduler;

        let group_engine =
            open_group_engine(&self.cfg.engine, self.engines.db(), group_id, desc.id, local_state)
                .await?;
        let task_group = TaskGroup::default();
        let (sender, receiver) = mpsc::unbounded();

        let info = Arc::new(ReplicaInfo::new(&desc, group_id, local_state));
        let lease_state = Arc::new(std::sync::Mutex::new(LeaseState::new(
            group_engine.descriptor(),
            group_engine.migration_state(),
            sender,
        )));
        let raft_node = start_raft_group(
            &self.cfg,
            &self.raft_mgr,
            info.clone(),
            lease_state.clone(),
            channel.clone(),
            group_engine.clone(),
            &task_group,
        )
        .await?;

        let replica_id = info.replica_id;
        let move_replicas_provider = Arc::new(MoveReplicasProvider::new());
        let schedule_state_observer =
            Arc::new(LeaseStateObserver::new(info.clone(), lease_state.clone(), channel));

        // TODO: config client options.
        let client = self.transport_manager.build_client(ClientOptions::default());
        let replica = Replica::new(
            info.clone(),
            lease_state,
            raft_node.clone(),
            group_engine,
            client,
            move_replicas_provider.clone(),
        );
        let replica = Arc::new(replica);
        self.replica_route_table.update(replica.clone());
        self.raft_route_table.update(replica_id, raft_node);

        // Setup jobs
        let migrate_handle = self.migrate_ctrl.watch_state_changes(replica.clone(), receiver);
        task_group.add_task(migrate_handle);

        let scheduler_handle = setup_scheduler(
            self.cfg.replica.clone(),
            replica.clone(),
            self.transport_manager.clone(),
            move_replicas_provider,
            schedule_state_observer,
        );
        task_group.add_task(scheduler_handle);

        // Now that all initialization work is done, the replica is ready to serve, mark
        // it as normal state.
        if matches!(local_state, ReplicaLocalState::Initial) {
            info.as_normal_state();
            self.state_engine
                .save_replica_state(group_id, replica_id, ReplicaLocalState::Normal)
                .await?;
        }

        info!("group {group_id} replica {replica_id} is ready for serving");

        Ok(ReplicaContext { info, task_group })
    }

    /// Get root desc that known by node.
    pub async fn get_root(&self) -> RootDesc {
        self.node_state.lock().await.root.clone()
    }

    // Update recent known root nodes.
    pub async fn update_root(&self, root_desc: RootDesc) -> Result<()> {
        let local_root_desc = self.get_root().await;
        if local_root_desc == root_desc {
            Ok(())
        } else {
            // TODO(walter) reject staled update root request.
            info!("update root from {local_root_desc:?} to {root_desc:?}");
            self.state_engine().save_root_desc(&root_desc).await?;
            self.reload_root_from_engine().await
        }
    }

    pub async fn reload_root_from_engine(&self) -> Result<()> {
        let root_desc = self
            .state_engine()
            .load_root_desc()
            .await?
            .ok_or_else(|| Error::InvalidData("root not found".into()))?;
        self.node_state.lock().await.root = root_desc;
        Ok(())
    }

    pub async fn execute_request(&self, request: &GroupRequest) -> Result<GroupResponse> {
        use crate::replica::retry::forwardable_execute;

        let replica = match self.replica_route_table.find(request.group_id) {
            Some(replica) => replica,
            None => {
                return Err(Error::GroupNotFound(request.group_id));
            }
        };

        forwardable_execute(&self.migrate_ctrl, &replica, &ExecCtx::default(), request).await
    }

    pub async fn forward(&self, request: ForwardRequest) -> Result<ForwardResponse> {
        use crate::replica::retry::execute;

        let replica = match self.replica_route_table.find(request.group_id) {
            Some(replica) => replica,
            None => {
                return Err(Error::GroupNotFound(request.group_id));
            }
        };

        let ingest_chunk = request.forward_data;
        match replica.ingest(request.shard_id, ingest_chunk, true).await {
            Ok(_) | Err(Error::ShardNotFound(_)) => {
                // Ingest success or shard is migrated.
            }
            Err(e) => return Err(e),
        }

        debug_assert!(request.request.is_some());
        let group_request =
            GroupRequest { group_id: request.group_id, epoch: 0, request: request.request };

        let exec_ctx = ExecCtx::forward(request.shard_id);
        let resp = execute(&replica, &exec_ctx, &group_request).await?;
        debug_assert!(resp.response.is_some());
        Ok(ForwardResponse { response: resp.response })
    }

    // This request is issued by dest group.
    pub async fn migrate(&self, event: MigrationEvent, desc: MigrationDesc) -> Result<()> {
        use crate::replica::retry::do_migration;

        if desc.shard_desc.is_none() {
            return Err(Error::InvalidArgument("MigrationDesc::shard_desc".to_owned()));
        }

        let group_id = desc.src_group_id;
        let replica = match self.replica_route_table.find(group_id) {
            Some(replica) => replica,
            None => {
                return Err(Error::GroupNotFound(group_id));
            }
        };

        do_migration(&replica, event, &desc).await?;
        Ok(())
    }

    #[inline]
    pub fn replica_table(&self) -> &ReplicaRouteTable {
        &self.replica_route_table
    }

    #[inline]
    pub fn raft_route_table(&self) -> &RaftRouteTable {
        &self.raft_route_table
    }

    #[inline]
    pub fn state_engine(&self) -> &StateEngine {
        &self.state_engine
    }

    #[inline]
    pub fn raft_manager(&self) -> &RaftManager {
        &self.raft_mgr
    }

    pub async fn collect_stats(&self, _req: &CollectStatsRequest) -> CollectStatsResponse {
        // TODO(walter) add read/write qps.
        let mut ns = NodeStats::default();
        let mut group_stats = vec![];
        let mut replica_stats = vec![];
        let group_id_list = self.serving_group_id_list().await;
        for group_id in group_id_list {
            if let Some(replica) = self.replica_route_table.find(group_id) {
                let info = replica.replica_info();
                if info.is_terminated() {
                    continue;
                }
                if info.group_id == ROOT_GROUP_ID {
                    continue;
                }
                let descriptor = replica.descriptor();
                if descriptor.replicas.is_empty() {
                    ns.orphan_replica_count += 1;
                }
                if descriptor.replicas.iter().any(|r| r.id == info.replica_id) {
                    // filter out the replica be removed by change_replica.
                    ns.group_count += 1;
                }
                let replica_state = replica.replica_state();
                if replica_state.role == RaftRole::Leader as i32 {
                    ns.leader_count += 1;
                    let gs = GroupStats {
                        group_id: info.group_id,
                        shard_count: descriptor.shards.len() as u64,
                        read_qps: 0.,
                        write_qps: 0.,
                    };
                    group_stats.push(gs);
                }
                let rs = ReplicaStats {
                    replica_id: info.replica_id,
                    group_id: info.group_id,
                    read_qps: 0.,
                    write_qps: 0.,
                };
                replica_stats.push(rs);
            }
        }

        CollectStatsResponse { node_stats: Some(ns), group_stats, replica_stats }
    }

    pub async fn collect_group_detail(
        &self,
        req: &CollectGroupDetailRequest,
    ) -> CollectGroupDetailResponse {
        let mut group_id_list = req.groups.clone();
        if group_id_list.is_empty() {
            group_id_list = self.serving_group_id_list().await;
        }

        let mut states = vec![];
        let mut descriptors = vec![];
        for group_id in group_id_list {
            if let Some(replica) = self.replica_route_table.find(group_id) {
                if replica.replica_info().is_terminated() {
                    continue;
                }

                let state = replica.replica_state();
                if state.role == RaftRole::Leader as i32 {
                    descriptors.push(replica.descriptor());
                }
                states.push(state);
            }
        }

        CollectGroupDetailResponse { replica_states: states, group_descs: descriptors }
    }

    pub async fn collect_migration_state(
        &self,
        req: &CollectMigrationStateRequest,
    ) -> CollectMigrationStateResponse {
        use collect_migration_state_response::State;

        let mut resp = CollectMigrationStateResponse { state: State::None as i32, desc: None };

        let group_id = req.group;
        if let Some(replica) = self.replica_route_table.find(group_id) {
            if !replica.replica_info().is_terminated() {
                if let Some(ms) = replica.migration_state() {
                    let mut state = match MigrationStep::from_i32(ms.step) {
                        Some(MigrationStep::Prepare) => State::Setup,
                        Some(MigrationStep::Migrated) => State::Migrated,
                        Some(MigrationStep::Migrating) => State::Migrating,
                        _ => State::None,
                    };
                    if ms.migration_desc.is_none() {
                        state = State::None;
                    }
                    resp.state = state as i32;
                    resp.desc = ms.migration_desc;
                }
            }
        }

        resp
    }

    pub async fn collect_schedule_state(
        &self,
        _req: &CollectScheduleStateRequest,
    ) -> CollectScheduleStateResponse {
        let mut resp = CollectScheduleStateResponse { schedule_states: vec![] };

        for group_id in self.serving_group_id_list().await {
            if let Some(replica) = self.replica_route_table.find(group_id) {
                if !replica.replica_info().is_terminated()
                    && replica.replica_state().role == RaftRole::Leader as i32
                {
                    resp.schedule_states.push(replica.schedule_state());
                }
            }
        }

        resp
    }

    #[inline]
    async fn serving_group_id_list(&self) -> Vec<u64> {
        let node_state = self.node_state.lock().await;
        node_state.serving_groups.iter().cloned().collect()
    }
}

impl NodeState {
    #[inline]
    fn is_bootstrapped(&self) -> bool {
        self.ident.is_some()
    }
}

async fn open_group_engine(
    cfg: &EngineConfig,
    raw_db: Arc<RawDb>,
    group_id: u64,
    replica_id: u64,
    replica_state: ReplicaLocalState,
) -> Result<GroupEngine> {
    match GroupEngine::open(cfg, raw_db.clone(), group_id, replica_id).await? {
        Some(group_engine) => Ok(group_engine),
        None if matches!(replica_state, ReplicaLocalState::Initial) => {
            GroupEngine::create(cfg, raw_db, group_id, replica_id).await
        }
        None => {
            panic!("group {group_id} replica {replica_id} open group engine: no such group engine exists");
        }
    }
}

async fn start_raft_group(
    cfg: &NodeConfig,
    raft_mgr: &RaftManager,
    info: Arc<ReplicaInfo>,
    lease_state: Arc<std::sync::Mutex<LeaseState>>,
    channel: Arc<StateChannel>,
    group_engine: GroupEngine,
    task_group: &TaskGroup,
) -> Result<RaftGroup> {
    let group_id = info.group_id;
    let state_observer =
        Box::new(LeaseStateObserver::new(info.clone(), lease_state.clone(), channel));
    let fsm = GroupStateMachine::new(
        cfg.replica.clone(),
        info.clone(),
        group_engine.clone(),
        state_observer.clone(),
    );
    raft_mgr
        .start_raft_group(group_id, info.replica_id, info.node_id, fsm, state_observer, task_group)
        .await
}

#[cfg(test)]
mod tests {
    use std::path::Path;
    use std::time::Duration;

    use sekas_api::server::v1::group_request_union::Request;
    use sekas_api::server::v1::group_response_union::Response;
    use sekas_api::server::v1::report_request::GroupUpdates;
    use sekas_api::server::v1::{ReplicaDesc, ReplicaRole};
    use sekas_rock::fn_name;
    use tempdir::TempDir;

    use super::*;
    use crate::constants::INITIAL_EPOCH;

    const COLLECTION_ID: u64 = 1;
    const NODE_ID: u64 = 2;
    const GROUP_ID: u64 = 3;
    const SHARD_ID: u64 = 4;
    const REPLICA_ID: u64 = 5;

    async fn create_node<P: AsRef<Path>>(root_dir: P) -> Node {
        let root_dir = root_dir.as_ref().to_owned();
        let config = Config { root_dir, ..Default::default() };

        let engines = Engines::open(&config.root_dir, &config.db).unwrap();
        let transport_manager = TransportManager::new(vec![], engines.state()).await;
        Node::new(config, engines, transport_manager).await.unwrap()
    }

    async fn bootstrap_node<P: AsRef<Path>>(root_dir: P) -> Node {
        let node = create_node(root_dir).await;
        let node_ident = NodeIdent { cluster_id: vec![], node_id: NODE_ID };
        node.bootstrap(&node_ident).await.unwrap();
        node
    }

    async fn create_first_replica(node: &Node) -> Arc<Replica> {
        let group_desc = group_descriptor();
        node.create_replica(REPLICA_ID, group_desc).await.unwrap();
        node.replica_table().find(GROUP_ID).unwrap()
    }

    async fn get_replica_state(node: Node, replica_id: u64) -> Option<ReplicaLocalState> {
        node.state_engine()
            .replica_states()
            .await
            .unwrap()
            .into_iter()
            .filter(|(_, id, _)| *id == replica_id)
            .map(|(_, _, state)| state)
            .next()
    }

    fn group_descriptor() -> GroupDesc {
        GroupDesc {
            id: GROUP_ID,
            epoch: INITIAL_EPOCH,
            shards: vec![ShardDesc::whole(SHARD_ID, COLLECTION_ID)],
            replicas: vec![ReplicaDesc {
                id: REPLICA_ID,
                node_id: NODE_ID,
                role: ReplicaRole::Voter.into(),
            }],
        }
    }

    #[sekas_macro::test]
    async fn create_replica_set_initial_state() {
        let dir = TempDir::new(fn_name!()).unwrap();
        let node = create_node(dir.path()).await;

        let group_desc = group_descriptor();
        node.create_replica(REPLICA_ID, group_desc).await.unwrap();

        assert!(matches!(
            get_replica_state(node, REPLICA_ID).await,
            Some(ReplicaLocalState::Initial),
        ));
    }

    #[sekas_macro::test]
    async fn create_then_bootstrap_replica() {
        let dir = TempDir::new(fn_name!()).unwrap();
        {
            // Create new replica.
            let node = create_node(dir.path()).await;

            let group_desc = group_descriptor();
            node.create_replica(REPLICA_ID, group_desc).await.unwrap();
        }

        {
            // Bootstrap replica after restart node.
            let node = create_node(dir.path()).await;
            let ident = NodeIdent { cluster_id: vec![], node_id: NODE_ID };
            node.bootstrap(&ident).await.unwrap();
        }
    }

    #[sekas_macro::test]
    async fn remove_replica() {
        let dir = TempDir::new(fn_name!()).unwrap();
        let node = create_node(dir.path()).await;

        let group = group_descriptor();
        node.create_replica(REPLICA_ID, group.clone()).await.unwrap();
        let ident = NodeIdent { cluster_id: vec![], node_id: NODE_ID };
        node.bootstrap(&ident).await.unwrap();

        sekas_runtime::time::sleep(Duration::from_millis(10)).await;

        node.remove_replica(REPLICA_ID, &group).await.unwrap();
    }

    /// After removing a replica, rejoin a new replica of the same group.
    #[sekas_macro::test]
    async fn remove_and_add_new_replicas() {
        let dir = TempDir::new(fn_name!()).unwrap();
        let node = create_node(dir.path()).await;

        let group = GroupDesc { id: GROUP_ID, epoch: INITIAL_EPOCH, ..Default::default() };
        node.create_replica(REPLICA_ID, group.clone()).await.unwrap();
        let ident = NodeIdent { cluster_id: vec![], node_id: NODE_ID };
        node.bootstrap(&ident).await.unwrap();

        sekas_runtime::time::sleep(Duration::from_millis(10)).await;

        node.remove_replica(REPLICA_ID, &group).await.unwrap();

        sekas_runtime::time::sleep(Duration::from_millis(10)).await;

        let new_replica_id = REPLICA_ID + 1;
        node.create_replica(new_replica_id, group.clone()).await.unwrap();
    }

    #[sekas_macro::test]
    async fn reject_add_two_replica_on_same_location() {
        let dir = TempDir::new(fn_name!()).unwrap();
        let node = create_node(dir.path()).await;

        let group = GroupDesc { id: GROUP_ID, epoch: INITIAL_EPOCH, ..Default::default() };
        node.create_replica(REPLICA_ID, group.clone()).await.unwrap();
        let ident = NodeIdent { cluster_id: vec![], node_id: NODE_ID };
        node.bootstrap(&ident).await.unwrap();

        let new_replica_id = REPLICA_ID + 1;
        let result = node.create_replica(new_replica_id, group.clone()).await;
        assert!(matches!(result, Err(Error::AlreadyExists(msg)) if msg.contains("group")));
    }

    #[sekas_macro::test]
    async fn report_replica_state_after_creating_replica() {
        let dir = TempDir::new(fn_name!()).unwrap();
        let node = create_node(dir.path()).await;

        let group = GroupDesc { id: GROUP_ID, epoch: INITIAL_EPOCH, ..Default::default() };
        node.create_replica(REPLICA_ID, group.clone()).await.unwrap();

        let (sender, mut receiver) = mpsc::unbounded();
        let replica_desc = ReplicaDesc { id: REPLICA_ID, ..Default::default() };
        let state_channel = Arc::new(StateChannel::without_handle(sender));
        node.serve_replica(GROUP_ID, replica_desc, ReplicaLocalState::Initial, state_channel)
            .await
            .unwrap();

        use futures::stream::StreamExt;

        let result = receiver.next().await;
        assert!(
            matches!(result, Some(GroupUpdates{ replica_state: Some(v), .. }) if v.replica_id == REPLICA_ID)
        );
    }

    fn build_put_request(key: &[u8], value: &[u8]) -> Request {
        Request::Write(ShardWriteRequest {
            shard_id: SHARD_ID,
            puts: vec![PutRequest {
                put_type: PutType::None.into(),
                key: key.to_vec(),
                value: value.to_vec(),
                take_prev_value: true,
                ..Default::default()
            }],
            ..Default::default()
        })
    }

    fn build_delete_request(key: &[u8]) -> Request {
        Request::Write(ShardWriteRequest {
            shard_id: SHARD_ID,
            deletes: vec![DeleteRequest {
                key: key.to_vec(),
                take_prev_value: true,
                ..Default::default()
            }],
            ..Default::default()
        })
    }

    fn build_read_request(key: &[u8]) -> Request {
        Request::Get(ShardGetRequest {
            shard_id: SHARD_ID,
            start_version: u64::MAX - 1,
            key: key.to_vec(),
        })
    }

    async fn execute_on_leader(replica: &Replica, request: Request) -> Response {
        assert!(replica.on_leader(fn_name!(), false).await.unwrap().is_some());
        let mut exec_ctx = ExecCtx::with_epoch(replica.epoch());
        replica.execute(&mut exec_ctx, &request).await.unwrap()
    }

    async fn execute_read_request(replica: &Replica, key: &[u8]) -> Option<Value> {
        let read_req = build_read_request(key);
        let response = execute_on_leader(replica, read_req).await;
        assert!(matches!(response, Response::Get(_)));
        let Response::Get(response) = response else { unreachable!() };
        response.value
    }

    // execute put request and return prev values.
    async fn execute_put_request(replica: &Replica, key: &[u8], value: &[u8]) -> Option<Value> {
        let write_req = build_put_request(key, value);
        let response = execute_on_leader(replica, write_req).await;
        assert!(matches!(response, Response::Write(_)));
        let Response::Write(response) = response else { unreachable!() };
        assert_eq!(response.puts.len(), 1);
        response.puts[0].prev_value.clone()
    }

    // execute delete request and return prev values.
    async fn execute_delete_request(replica: &Replica, key: &[u8]) -> Option<Value> {
        let write_req = build_delete_request(key);
        let response = execute_on_leader(replica, write_req).await;
        assert!(matches!(response, Response::Write(_)));
        let Response::Write(response) = response else { unreachable!() };
        assert_eq!(response.deletes.len(), 1);
        response.deletes[0].prev_value.clone()
    }

    #[sekas_macro::test]
    async fn create_replica_after_node_bootstraped() {
        // If node is bootstraped, create replica will start in auto.
        let dir = TempDir::new(fn_name!()).unwrap();
        let node = bootstrap_node(dir.path()).await;
        let replica = create_first_replica(&node).await;
        let term = replica.on_leader(fn_name!(), false).await.unwrap();
        assert!(term.is_some());
    }

    #[sekas_macro::test]
    async fn replica_read_and_write() {
        let dir = TempDir::new(fn_name!()).unwrap();
        let node = bootstrap_node(dir.path()).await;
        let replica = create_first_replica(&node).await;

        // Key not found
        let value = execute_read_request(&replica, b"123").await;
        assert!(value.is_none());

        execute_put_request(&replica, b"123", b"456").await;

        // read again.
        let value = execute_read_request(&replica, b"123").await;
        assert!(matches!(value, Some(v) if v.content.as_ref().unwrap() == b"456"));
    }

    #[sekas_macro::test]
    async fn replica_put_with_prev_value_and_read_tombstone() {
        let dir = TempDir::new(fn_name!()).unwrap();
        let node = bootstrap_node(dir.path()).await;
        let replica = create_first_replica(&node).await;

        let prev_value = execute_put_request(&replica, b"123", b"456").await;
        assert!(prev_value.is_none());

        let prev_value = execute_delete_request(&replica, b"123").await;
        assert!(matches!(prev_value, Some(v) if v.content.as_ref().unwrap() == b"456"));

        // Read a tombstone.
        let value = execute_read_request(&replica, b"123").await;
        assert!(matches!(&value, Some(v) if v.content.is_none()), "value: {value:?}");
    }

    #[sekas_macro::test]
    async fn create_after_removing_replica_with_same_group() {
        let dir = TempDir::new(fn_name!()).unwrap();
        let node = bootstrap_node(dir.path()).await;
        let group_desc = GroupDesc { id: GROUP_ID, epoch: INITIAL_EPOCH, ..Default::default() };
        let removed_replica_id = REPLICA_ID + 1;
        info!("create replica {removed_replica_id} and remove it immediately");
        node.create_replica(removed_replica_id, group_desc.clone()).await.unwrap();
        node.remove_replica(removed_replica_id, &group_desc).await.unwrap();

        info!("create first replica {REPLICA_ID}");
        let replica = create_first_replica(&node).await;
        execute_put_request(&replica, b"123", b"456").await;
    }

    #[sekas_macro::test]
    async fn bootstrap_and_create_after_removing_replica_with_same_group() {
        let dir = TempDir::new(fn_name!()).unwrap();
        {
            let node = bootstrap_node(dir.path()).await;
            let replica_id = REPLICA_ID + 1;
            let group_desc = GroupDesc { id: GROUP_ID, epoch: INITIAL_EPOCH, ..Default::default() };
            node.create_replica(replica_id, group_desc).await.unwrap();

            // Mark it as terminated.
            node.state_engine
                .save_replica_state(GROUP_ID, replica_id, ReplicaLocalState::Terminated)
                .await
                .unwrap();
        }

        {
            // Mock reboot.
            let node = bootstrap_node(dir.path()).await;
            let replica = create_first_replica(&node).await;
            execute_put_request(&replica, b"123", b"456").await;
        }
    }

    fn build_preapre_request(start_version: u64, key: &[u8], value: &[u8]) -> Request {
        Request::WriteIntent(WriteIntentRequest {
            start_version,
            write: Some(ShardWriteRequest {
                shard_id: SHARD_ID,
                puts: vec![PutRequest {
                    put_type: PutType::None.into(),
                    key: key.to_vec(),
                    value: value.to_vec(),
                    take_prev_value: true,
                    ..Default::default()
                }],
                ..Default::default()
            }),
        })
    }

    // execute prepare request and return prev values.
    async fn execute_prepare_request(
        replica: &Replica,
        start_version: u64,
        key: &[u8],
        value: &[u8],
    ) -> Option<Value> {
        let write_req = build_preapre_request(start_version, key, value);
        let response = execute_on_leader(replica, write_req).await;
        assert!(matches!(response, Response::WriteIntent(_)));
        let Response::WriteIntent(response) = response else { unreachable!() };
        assert!(response.write.is_some());
        let write = response.write.unwrap();
        assert_eq!(write.puts.len(), 1);
        write.puts[0].prev_value.clone()
    }

    fn build_commit_request(start_version: u64, commit_version: u64, key: &[u8]) -> Request {
        Request::CommitIntent(CommitIntentRequest {
            shard_id: SHARD_ID,
            start_version,
            commit_version,
            keys: vec![key.to_vec()],
        })
    }

    async fn execute_commit_request(
        replica: &Replica,
        start_version: u64,
        commit_version: u64,
        key: &[u8],
    ) {
        let write_req = build_commit_request(start_version, commit_version, key);
        let response = execute_on_leader(replica, write_req).await;
        assert!(matches!(response, Response::CommitIntent(_)));
    }

    fn build_abort_request(start_version: u64, key: &[u8]) -> Request {
        Request::ClearIntent(ClearIntentRequest {
            shard_id: SHARD_ID,
            start_version,
            keys: vec![key.to_vec()],
        })
    }

    async fn execute_abort_request(replica: &Replica, start_version: u64, key: &[u8]) {
        let write_req = build_abort_request(start_version, key);
        let response = execute_on_leader(replica, write_req).await;
        assert!(matches!(response, Response::ClearIntent(_)));
    }

    #[sekas_macro::test]
    async fn read_commit_value() {
        let dir = TempDir::new(fn_name!()).unwrap();
        let node = bootstrap_node(dir.path()).await;
        let replica = create_first_replica(&node).await;

        // Key not found
        let value = execute_read_request(&replica, b"123").await;
        assert!(value.is_none());

        let start_version = 123;
        let commit_version = 345;
        execute_prepare_request(&replica, start_version, b"123", b"456").await;
        execute_commit_request(&replica, start_version, commit_version, b"123").await;

        // read again.
        let value = execute_read_request(&replica, b"123").await;
        assert!(
            matches!(value, Some(v) if v.content.as_ref().unwrap() == b"456" && v.version == commit_version)
        );
    }

    #[sekas_macro::test]
    async fn read_abort_value() {
        let dir = TempDir::new(fn_name!()).unwrap();
        let node = bootstrap_node(dir.path()).await;
        let replica = create_first_replica(&node).await;

        // Key not found
        let value = execute_read_request(&replica, b"123").await;
        assert!(value.is_none());

        let start_version = 123;
        execute_prepare_request(&replica, start_version, b"123", b"456").await;
        execute_abort_request(&replica, start_version, b"123").await;

        // read again.
        let value = execute_read_request(&replica, b"123").await;
        assert!(value.is_none());
    }
}
