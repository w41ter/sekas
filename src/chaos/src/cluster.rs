// Copyright 2026-present The Sekas Authors.
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
use std::fs::{self, OpenOptions};
use std::future::Future;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::process::{ExitStatus, Stdio};
use std::sync::Arc;
use std::time::{Duration, Instant};

use sekas_api::server::v1::{ReplicaRole, ShardDesc};
use sekas_client::{
    ClientOptions, ConnManager, GroupClient, NodeClient, RootClient, Router, RouterGroupState,
    SekasClient, StaticServiceDiscovery,
};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::process::{Child, Command};

pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;
pub type Result<T> = std::result::Result<T, ClusterError>;

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct NodeId(pub u64);

impl std::fmt::Display for NodeId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct NodeSpec {
    pub id: NodeId,
    pub address: String,
    pub data_dir: PathBuf,
    pub bootstrap: bool,
    pub join: Vec<String>,
    pub extra_args: Vec<String>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ClusterSpec {
    pub binary: PathBuf,
    pub nodes: Vec<NodeSpec>,
    pub startup_timeout: Duration,
    pub shutdown_timeout: Duration,
    pub log_dir: PathBuf,
}

impl ClusterSpec {
    pub fn validate(&self) -> Result<()> {
        if self.nodes.is_empty() {
            return Err(ClusterError::Configuration("cluster has no nodes".to_string()));
        }
        if !self.binary.is_file() {
            return Err(ClusterError::Configuration(format!(
                "server binary does not exist: {}",
                self.binary.display()
            )));
        }
        if self.startup_timeout.is_zero() || self.shutdown_timeout.is_zero() {
            return Err(ClusterError::Configuration(
                "startup and shutdown timeouts must be non-zero".to_string(),
            ));
        }
        let mut ids = self.nodes.iter().map(|node| node.id).collect::<Vec<_>>();
        ids.sort_unstable();
        if ids.windows(2).any(|pair| pair[0] == pair[1]) {
            return Err(ClusterError::Configuration("node ids must be unique".to_string()));
        }
        if self.nodes.iter().filter(|node| node.bootstrap).count() != 1 {
            return Err(ClusterError::Configuration(
                "exactly one node must bootstrap the cluster".to_string(),
            ));
        }
        if self.nodes.iter().any(|node| !node.bootstrap && node.join.is_empty()) {
            return Err(ClusterError::Configuration(
                "every non-bootstrap node needs at least one join address".to_string(),
            ));
        }
        let mut addresses = self.nodes.iter().map(|node| &node.address).collect::<Vec<_>>();
        addresses.sort_unstable();
        if addresses.windows(2).any(|pair| pair[0] == pair[1]) {
            return Err(ClusterError::Configuration("node addresses must be unique".to_string()));
        }
        let mut data_dirs = self.nodes.iter().map(|node| &node.data_dir).collect::<Vec<_>>();
        data_dirs.sort_unstable();
        if data_dirs.windows(2).any(|pair| pair[0] == pair[1]) {
            return Err(ClusterError::Configuration(
                "node data directories must be unique".to_string(),
            ));
        }
        Ok(())
    }

    pub fn addresses(&self) -> Vec<String> {
        self.nodes.iter().map(|node| node.address.clone()).collect()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum NodeState {
    Starting,
    Running,
    Stopped,
    Paused,
    Exited,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct NodeStatus {
    pub state: NodeState,
    pub generation: u64,
    pub pid: Option<u32>,
    pub exit: Option<String>,
    pub stdout: PathBuf,
    pub stderr: PathBuf,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct ClusterCapabilities {
    pub graceful_stop: bool,
    pub kill: bool,
    pub pause: bool,
    pub failpoints: bool,
    pub network_faults: bool,
    pub disk_faults: bool,
    pub admin_actions: bool,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ClusterStatus {
    pub nodes: BTreeMap<NodeId, NodeStatus>,
    pub active_faults: Vec<FaultHandle>,
    pub capabilities: ClusterCapabilities,
}

impl ClusterStatus {
    pub fn unavailable_nodes(&self) -> usize {
        self.nodes.values().filter(|node| node.state != NodeState::Running).count()
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum DiskFault {
    NoSpace,
    ReadError,
    WriteError,
    SyncError,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum FaultSpec {
    NetworkPartition { left: Vec<NodeId>, right: Vec<NodeId> },
    NetworkDelay { node: NodeId, latency: Duration },
    DropTraffic { from: NodeId, to: NodeId },
    Disk { node: NodeId, fault: DiskFault },
    FailPoint { node: NodeId, name: String, action: String },
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct FaultHandle {
    pub id: u64,
    pub fault: FaultSpec,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum AdminAction {
    TransferLeader { group_id: u64, target_replica: Option<u64> },
    MoveShard { table_id: u64, key: Vec<u8>, target_group_id: u64 },
}

#[derive(Debug, Error)]
pub enum ClusterError {
    #[error("invalid cluster configuration: {0}")]
    Configuration(String),
    #[error("cluster has not been deployed")]
    NotDeployed,
    #[error("node {0} is not configured")]
    UnknownNode(NodeId),
    #[error("node {node} cannot perform {action} while {state:?}")]
    InvalidTransition { node: NodeId, action: &'static str, state: NodeState },
    #[error("node {node} has no child process while {state:?}")]
    MissingProcess { node: NodeId, state: NodeState },
    #[error("node {node} process operation {action} failed: {source}")]
    Process {
        node: NodeId,
        action: &'static str,
        #[source]
        source: std::io::Error,
    },
    #[error("node {node} did not {action} within {timeout:?}")]
    Timeout { node: NodeId, action: &'static str, timeout: Duration },
    #[error("cluster did not become ready within {0:?}")]
    ReadinessTimeout(Duration),
    #[error("fault is not supported by this cluster backend: {0:?}")]
    UnsupportedFault(FaultSpec),
    #[error("fault handle {0} is not active")]
    UnknownFault(u64),
    #[error("admin action failed: {0}")]
    Admin(String),
    #[error("cluster is already deployed")]
    AlreadyDeployed,
}

/// Readiness is kept separate from process state so unit tests and future
/// remote backends can use the same lifecycle implementation.
pub trait ReadinessProbe: Send + Sync {
    fn node_ready<'a>(&'a self, node: &'a NodeSpec) -> BoxFuture<'a, bool>;

    fn cluster_ready<'a>(
        &'a self,
        spec: &'a ClusterSpec,
        status: &'a ClusterStatus,
    ) -> BoxFuture<'a, bool>;
}

#[derive(Debug, Default)]
pub struct SekasReadinessProbe;

impl ReadinessProbe for SekasReadinessProbe {
    fn node_ready<'a>(&'a self, node: &'a NodeSpec) -> BoxFuture<'a, bool> {
        Box::pin(async move { NodeClient::connect(node.address.clone()).await.is_ok() })
    }

    fn cluster_ready<'a>(
        &'a self,
        spec: &'a ClusterSpec,
        status: &'a ClusterStatus,
    ) -> BoxFuture<'a, bool> {
        Box::pin(async move {
            if spec.nodes.iter().any(|node| {
                status.nodes.get(&node.id).map(|status| status.state) != Some(NodeState::Running)
            }) {
                return false;
            }
            for node in &spec.nodes {
                if !self.node_ready(node).await {
                    return false;
                }
            }
            SekasClient::new(ClientOptions::default(), spec.addresses()).await.is_ok()
        })
    }
}

/// Controls a deployed cluster without exposing a particular deployment tool.
pub trait ClusterController: Send {
    fn deploy<'a>(&'a mut self, spec: ClusterSpec) -> BoxFuture<'a, Result<()>>;
    fn start_node<'a>(&'a mut self, node: NodeId) -> BoxFuture<'a, Result<()>>;
    fn stop_node<'a>(&'a mut self, node: NodeId) -> BoxFuture<'a, Result<()>>;
    fn kill_node<'a>(&'a mut self, node: NodeId) -> BoxFuture<'a, Result<()>>;
    fn pause_node<'a>(&'a mut self, node: NodeId) -> BoxFuture<'a, Result<()>>;
    fn resume_node<'a>(&'a mut self, node: NodeId) -> BoxFuture<'a, Result<()>>;
    fn restart_node<'a>(&'a mut self, node: NodeId) -> BoxFuture<'a, Result<()>>;
    fn inject<'a>(&'a mut self, fault: FaultSpec) -> BoxFuture<'a, Result<FaultHandle>>;
    fn recover<'a>(&'a mut self, fault: FaultHandle) -> BoxFuture<'a, Result<()>>;
    fn admin<'a>(&'a mut self, action: AdminAction) -> BoxFuture<'a, Result<()>>;
    fn status<'a>(&'a mut self) -> BoxFuture<'a, Result<ClusterStatus>>;
    fn wait_ready<'a>(&'a mut self, timeout: Duration) -> BoxFuture<'a, Result<()>>;
    fn restore<'a>(&'a mut self) -> BoxFuture<'a, Result<()>>;
    fn shutdown<'a>(&'a mut self) -> BoxFuture<'a, Result<()>>;
}

struct LocalNode {
    spec: NodeSpec,
    child: Option<Child>,
    state: NodeState,
    generation: u64,
    last_exit: Option<ExitStatus>,
    stdout: PathBuf,
    stderr: PathBuf,
}

/// A cluster backend that runs every Sekas node as an independent OS process.
pub struct LocalProcessCluster {
    spec: Option<ClusterSpec>,
    nodes: BTreeMap<NodeId, LocalNode>,
    active_faults: BTreeMap<u64, FaultHandle>,
    probe: Arc<dyn ReadinessProbe>,
}

impl Default for LocalProcessCluster {
    fn default() -> Self {
        Self::new()
    }
}

impl LocalProcessCluster {
    pub fn new() -> Self {
        Self::with_probe(Arc::new(SekasReadinessProbe))
    }

    pub fn with_probe(probe: Arc<dyn ReadinessProbe>) -> Self {
        Self { spec: None, nodes: BTreeMap::new(), active_faults: BTreeMap::new(), probe }
    }

    pub fn spec(&self) -> Option<&ClusterSpec> {
        self.spec.as_ref()
    }

    fn capabilities() -> ClusterCapabilities {
        ClusterCapabilities {
            graceful_stop: true,
            kill: true,
            pause: cfg!(unix),
            admin_actions: true,
            ..ClusterCapabilities::default()
        }
    }

    fn spec_ref(&self) -> Result<&ClusterSpec> {
        self.spec.as_ref().ok_or(ClusterError::NotDeployed)
    }

    fn node_mut(&mut self, id: NodeId) -> Result<&mut LocalNode> {
        self.nodes.get_mut(&id).ok_or(ClusterError::UnknownNode(id))
    }

    fn spawn_node(&mut self, id: NodeId) -> Result<()> {
        let binary = self.spec_ref()?.binary.clone();
        let node = self.node_mut(id)?;
        if !matches!(node.state, NodeState::Stopped | NodeState::Exited) {
            return Err(ClusterError::InvalidTransition {
                node: id,
                action: "start",
                state: node.state,
            });
        }
        fs::create_dir_all(&node.spec.data_dir).map_err(|source| ClusterError::Process {
            node: id,
            action: "create data directory",
            source,
        })?;
        let stdout = log_file(&node.stdout, id, "open stdout")?;
        let stderr = log_file(&node.stderr, id, "open stderr")?;
        let mut command = Command::new(binary);
        command
            .arg("start")
            .arg("--addr")
            .arg(&node.spec.address)
            .arg("--db")
            .arg(&node.spec.data_dir)
            .args(&node.spec.extra_args)
            .stdout(Stdio::from(stdout))
            .stderr(Stdio::from(stderr))
            .kill_on_drop(true);
        if node.spec.bootstrap {
            command.arg("--init");
        } else if !node.spec.join.is_empty() {
            command.arg("--join").args(&node.spec.join);
        }
        let child = command.spawn().map_err(|source| ClusterError::Process {
            node: id,
            action: "spawn",
            source,
        })?;
        node.child = Some(child);
        node.state = NodeState::Starting;
        node.generation += 1;
        node.last_exit = None;
        Ok(())
    }

    async fn wait_node_ready(&mut self, id: NodeId, timeout: Duration) -> Result<()> {
        let deadline = Instant::now() + timeout;
        loop {
            self.refresh()?;
            let node = self.nodes.get(&id).ok_or(ClusterError::UnknownNode(id))?;
            if node.state == NodeState::Exited {
                return Err(ClusterError::Process {
                    node: id,
                    action: "wait for readiness",
                    source: std::io::Error::other(format!(
                        "process exited with {:?}",
                        node.last_exit
                    )),
                });
            }
            if self.probe.node_ready(&node.spec).await {
                self.node_mut(id)?.state = NodeState::Running;
                return Ok(());
            }
            if Instant::now() >= deadline {
                return Err(ClusterError::Timeout { node: id, action: "become ready", timeout });
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    }

    fn refresh(&mut self) -> Result<()> {
        for (id, node) in &mut self.nodes {
            let Some(child) = node.child.as_mut() else {
                continue;
            };
            match child.try_wait() {
                Ok(Some(exit)) => {
                    node.last_exit = Some(exit);
                    node.child = None;
                    node.state = NodeState::Exited;
                }
                Ok(None) => {}
                Err(source) => {
                    return Err(ClusterError::Process {
                        node: *id,
                        action: "query status",
                        source,
                    });
                }
            }
        }
        Ok(())
    }

    fn snapshot(&self) -> ClusterStatus {
        ClusterStatus {
            nodes: self
                .nodes
                .iter()
                .map(|(id, node)| {
                    (
                        *id,
                        NodeStatus {
                            state: node.state,
                            generation: node.generation,
                            pid: node.child.as_ref().and_then(Child::id),
                            exit: node.last_exit.map(|exit| exit.to_string()),
                            stdout: node.stdout.clone(),
                            stderr: node.stderr.clone(),
                        },
                    )
                })
                .collect(),
            active_faults: self.active_faults.values().cloned().collect(),
            capabilities: Self::capabilities(),
        }
    }

    async fn signal_node(&mut self, id: NodeId, signal: i32, action: &'static str) -> Result<()> {
        let node = self.node_mut(id)?;
        let pid = node
            .child
            .as_ref()
            .and_then(Child::id)
            .ok_or(ClusterError::MissingProcess { node: id, state: node.state })?;
        send_signal(pid, signal).map_err(|source| ClusterError::Process {
            node: id,
            action,
            source,
        })
    }

    async fn stop_node_inner(&mut self, id: NodeId, graceful: bool) -> Result<()> {
        self.refresh()?;
        let state = self.nodes.get(&id).ok_or(ClusterError::UnknownNode(id))?.state;
        if !matches!(state, NodeState::Running | NodeState::Starting | NodeState::Paused) {
            return Err(ClusterError::InvalidTransition {
                node: id,
                action: if graceful { "stop" } else { "kill" },
                state,
            });
        }
        if state == NodeState::Paused {
            self.signal_node(id, signal_continue(), "resume before stop").await?;
        }
        if graceful {
            self.signal_node(id, signal_interrupt(), "send interrupt").await?;
        } else {
            let node = self.node_mut(id)?;
            node.child
                .as_mut()
                .ok_or(ClusterError::MissingProcess { node: id, state })?
                .start_kill()
                .map_err(|source| ClusterError::Process { node: id, action: "kill", source })?;
        }
        let timeout = self.spec_ref()?.shutdown_timeout;
        let node = self.node_mut(id)?;
        let child = node
            .child
            .as_mut()
            .ok_or(ClusterError::MissingProcess { node: id, state: node.state })?;
        let exit = tokio::time::timeout(timeout, child.wait())
            .await
            .map_err(|_| ClusterError::Timeout {
                node: id,
                action: if graceful { "stop" } else { "exit after kill" },
                timeout,
            })?
            .map_err(|source| ClusterError::Process {
                node: id,
                action: "wait for exit",
                source,
            })?;
        node.last_exit = Some(exit);
        node.child = None;
        node.state = NodeState::Stopped;
        Ok(())
    }

    async fn sekas_clients(&self) -> Result<(Router, SekasClient)> {
        let spec = self.spec_ref()?;
        let discovery = Arc::new(StaticServiceDiscovery::new(spec.addresses()));
        let connections = ConnManager::new();
        let root = RootClient::new(discovery, connections.clone());
        let router = Router::new(root).await;
        let client = SekasClient::new(ClientOptions::default(), spec.addresses())
            .await
            .map_err(|err| ClusterError::Admin(err.to_string()))?;
        Ok((router, client))
    }

    async fn wait_router_group(router: &Router, group_id: u64) -> Result<RouterGroupState> {
        for _ in 0..1000 {
            if let Ok(group) = router.find_group(group_id)
                && group.leader_state.is_some()
            {
                return Ok(group);
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        Err(ClusterError::Admin(format!("group {group_id} not found or has no leader")))
    }

    async fn wait_router_shard(
        router: &Router,
        table_id: u64,
        key: &[u8],
    ) -> Result<(RouterGroupState, ShardDesc)> {
        for _ in 0..1000 {
            if let Ok((group, shard)) = router.find_shard(table_id, key) {
                return Ok((group, shard));
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        Err(ClusterError::Admin(format!("shard for table {table_id} key {key:?} not found")))
    }

    async fn execute_admin(&self, action: AdminAction) -> Result<()> {
        let (router, client) = self.sekas_clients().await?;
        match action {
            AdminAction::TransferLeader { group_id, target_replica } => {
                let group = Self::wait_router_group(&router, group_id).await?;
                let current = group.leader_state.map(|state| state.0);
                let target = if let Some(target) = target_replica {
                    target
                } else {
                    let mut candidates = group
                        .replicas
                        .values()
                        .filter(|replica| {
                            Some(replica.id) != current && replica.role == ReplicaRole::Voter as i32
                        })
                        .map(|replica| replica.id)
                        .collect::<Vec<_>>();
                    candidates.sort_unstable();
                    candidates.into_iter().next().ok_or_else(|| {
                        ClusterError::Admin(format!("group {group_id} has no transfer target"))
                    })?
                };
                GroupClient::lazy(group_id, client)
                    .transfer_leader(target)
                    .await
                    .map_err(|err| ClusterError::Admin(err.to_string()))?;
            }
            AdminAction::MoveShard { table_id, key, target_group_id } => {
                let (source, shard) = Self::wait_router_shard(&router, table_id, &key).await?;
                if source.id != target_group_id {
                    GroupClient::lazy(target_group_id, client)
                        .accept_shard(source.id, source.epoch, &shard)
                        .await
                        .map_err(|err| ClusterError::Admin(err.to_string()))?;
                }
            }
        }
        Ok(())
    }
}

impl ClusterController for LocalProcessCluster {
    fn deploy<'a>(&'a mut self, spec: ClusterSpec) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            if self.spec.is_some() {
                return Err(ClusterError::AlreadyDeployed);
            }
            spec.validate()?;
            fs::create_dir_all(&spec.log_dir).map_err(|source| ClusterError::Process {
                node: NodeId(0),
                action: "create log directory",
                source,
            })?;
            let mut nodes = spec.nodes.clone();
            nodes.sort_by_key(|node| (!node.bootstrap, node.id));
            for node in &nodes {
                self.nodes.insert(
                    node.id,
                    LocalNode {
                        spec: node.clone(),
                        child: None,
                        state: NodeState::Stopped,
                        generation: 0,
                        last_exit: None,
                        stdout: spec.log_dir.join(format!("node-{}.stdout.log", node.id.0)),
                        stderr: spec.log_dir.join(format!("node-{}.stderr.log", node.id.0)),
                    },
                );
            }
            let startup_timeout = spec.startup_timeout;
            self.spec = Some(spec);
            for node in nodes {
                self.spawn_node(node.id)?;
                self.wait_node_ready(node.id, startup_timeout).await?;
            }
            self.wait_ready(startup_timeout).await
        })
    }

    fn start_node<'a>(&'a mut self, node: NodeId) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            let timeout = self.spec_ref()?.startup_timeout;
            self.spawn_node(node)?;
            self.wait_node_ready(node, timeout).await
        })
    }

    fn stop_node<'a>(&'a mut self, node: NodeId) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move { self.stop_node_inner(node, true).await })
    }

    fn kill_node<'a>(&'a mut self, node: NodeId) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move { self.stop_node_inner(node, false).await })
    }

    fn pause_node<'a>(&'a mut self, node: NodeId) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            self.refresh()?;
            let state = self.node_mut(node)?.state;
            if state != NodeState::Running {
                return Err(ClusterError::InvalidTransition { node, action: "pause", state });
            }
            self.signal_node(node, signal_stop(), "pause").await?;
            self.node_mut(node)?.state = NodeState::Paused;
            Ok(())
        })
    }

    fn resume_node<'a>(&'a mut self, node: NodeId) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            self.refresh()?;
            let state = self.node_mut(node)?.state;
            if state != NodeState::Paused {
                return Err(ClusterError::InvalidTransition { node, action: "resume", state });
            }
            self.signal_node(node, signal_continue(), "resume").await?;
            self.node_mut(node)?.state = NodeState::Running;
            Ok(())
        })
    }

    fn restart_node<'a>(&'a mut self, node: NodeId) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            self.refresh()?;
            let state = self.node_mut(node)?.state;
            if matches!(state, NodeState::Running | NodeState::Starting | NodeState::Paused) {
                self.stop_node_inner(node, false).await?;
            }
            self.start_node(node).await
        })
    }

    fn inject<'a>(&'a mut self, fault: FaultSpec) -> BoxFuture<'a, Result<FaultHandle>> {
        Box::pin(async move { Err(ClusterError::UnsupportedFault(fault)) })
    }

    fn recover<'a>(&'a mut self, fault: FaultHandle) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            if self.active_faults.remove(&fault.id).is_none() {
                return Err(ClusterError::UnknownFault(fault.id));
            }
            Ok(())
        })
    }

    fn admin<'a>(&'a mut self, action: AdminAction) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move { self.execute_admin(action).await })
    }

    fn status<'a>(&'a mut self) -> BoxFuture<'a, Result<ClusterStatus>> {
        Box::pin(async move {
            self.spec_ref()?;
            self.refresh()?;
            Ok(self.snapshot())
        })
    }

    fn wait_ready<'a>(&'a mut self, timeout: Duration) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            let deadline = Instant::now() + timeout;
            loop {
                let status = self.status().await?;
                let spec = self.spec_ref()?;
                if self.probe.cluster_ready(spec, &status).await {
                    return Ok(());
                }
                if Instant::now() >= deadline {
                    return Err(ClusterError::ReadinessTimeout(timeout));
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        })
    }

    fn restore<'a>(&'a mut self) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            let faults = self.active_faults.values().cloned().collect::<Vec<_>>();
            for fault in faults {
                self.recover(fault).await?;
            }
            let status = self.status().await?;
            for (id, node) in status.nodes {
                match node.state {
                    NodeState::Running => {}
                    NodeState::Paused => self.resume_node(id).await?,
                    NodeState::Stopped | NodeState::Exited => self.start_node(id).await?,
                    NodeState::Starting => {
                        let timeout = self.spec_ref()?.startup_timeout;
                        self.wait_node_ready(id, timeout).await?;
                    }
                }
            }
            let timeout = self.spec_ref()?.startup_timeout;
            self.wait_ready(timeout).await
        })
    }

    fn shutdown<'a>(&'a mut self) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            if self.spec.is_none() {
                return Ok(());
            }
            self.active_faults.clear();
            let _ = self.refresh();
            let ids = self.nodes.keys().copied().collect::<Vec<_>>();
            let mut first_error = None;
            for id in ids {
                let state = self.nodes[&id].state;
                if matches!(state, NodeState::Running | NodeState::Starting | NodeState::Paused)
                    && let Err(err) = self.stop_node_inner(id, true).await
                {
                    let _ = self.stop_node_inner(id, false).await;
                    if first_error.is_none() {
                        first_error = Some(err);
                    }
                }
            }
            if let Some(err) = first_error { Err(err) } else { Ok(()) }
        })
    }
}

fn log_file(path: &Path, node: NodeId, action: &'static str) -> Result<std::fs::File> {
    OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .map_err(|source| ClusterError::Process { node, action, source })
}

#[cfg(unix)]
fn send_signal(pid: u32, signal: i32) -> std::io::Result<()> {
    // SAFETY: `kill` does not dereference pointers. The PID comes directly
    // from the owned child process and the signal is a platform constant.
    if unsafe { libc::kill(pid as libc::pid_t, signal) } == 0 {
        Ok(())
    } else {
        Err(std::io::Error::last_os_error())
    }
}

#[cfg(not(unix))]
fn send_signal(_pid: u32, _signal: i32) -> std::io::Result<()> {
    Err(std::io::Error::new(std::io::ErrorKind::Unsupported, "process signals require Unix"))
}

#[cfg(unix)]
const fn signal_interrupt() -> i32 {
    libc::SIGINT
}
#[cfg(not(unix))]
const fn signal_interrupt() -> i32 {
    0
}

#[cfg(unix)]
const fn signal_stop() -> i32 {
    libc::SIGSTOP
}
#[cfg(not(unix))]
const fn signal_stop() -> i32 {
    0
}

#[cfg(unix)]
const fn signal_continue() -> i32 {
    libc::SIGCONT
}
#[cfg(not(unix))]
const fn signal_continue() -> i32 {
    0
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use super::{
        BoxFuture, ClusterController, ClusterSpec, ClusterStatus, LocalProcessCluster, NodeId,
        NodeSpec, NodeState, ReadinessProbe,
    };

    struct AlwaysReady;

    impl ReadinessProbe for AlwaysReady {
        fn node_ready<'a>(&'a self, _node: &'a NodeSpec) -> BoxFuture<'a, bool> {
            Box::pin(async { true })
        }

        fn cluster_ready<'a>(
            &'a self,
            _spec: &'a ClusterSpec,
            status: &'a ClusterStatus,
        ) -> BoxFuture<'a, bool> {
            Box::pin(
                async move { status.nodes.values().all(|node| node.state == NodeState::Running) },
            )
        }
    }

    fn test_dir() -> std::path::PathBuf {
        std::env::temp_dir().join(format!(
            "sekas-chaos-cluster-test-{}-{}",
            std::process::id(),
            std::thread::current().name().unwrap_or("unnamed")
        ))
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn local_process_lifecycle_preserves_data_directory() {
        let root = test_dir();
        let script = root.join("fake-sekas.sh");
        std::fs::create_dir_all(&root).unwrap();
        std::fs::write(
            &script,
            "#!/bin/sh\ntrap 'exit 0' INT TERM\nwhile true; do sleep 1; done\n",
        )
        .unwrap();
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&script, std::fs::Permissions::from_mode(0o755)).unwrap();
        let data_dir = root.join("data");
        let spec = ClusterSpec {
            binary: script,
            nodes: vec![NodeSpec {
                id: NodeId(0),
                address: "127.0.0.1:1".to_string(),
                data_dir: data_dir.clone(),
                bootstrap: true,
                join: vec![],
                extra_args: vec![],
            }],
            startup_timeout: Duration::from_secs(2),
            shutdown_timeout: Duration::from_secs(2),
            log_dir: root.join("logs"),
        };
        let mut cluster = LocalProcessCluster::with_probe(Arc::new(AlwaysReady));
        cluster.deploy(spec).await.unwrap();
        let first = cluster.status().await.unwrap();
        assert_eq!(first.nodes[&NodeId(0)].generation, 1);
        cluster.pause_node(NodeId(0)).await.unwrap();
        assert_eq!(cluster.status().await.unwrap().nodes[&NodeId(0)].state, NodeState::Paused);
        cluster.resume_node(NodeId(0)).await.unwrap();
        cluster.kill_node(NodeId(0)).await.unwrap();
        std::fs::write(data_dir.join("sentinel"), b"preserved").unwrap();
        cluster.start_node(NodeId(0)).await.unwrap();
        assert_eq!(cluster.status().await.unwrap().nodes[&NodeId(0)].generation, 2);
        assert_eq!(std::fs::read(data_dir.join("sentinel")).unwrap(), b"preserved");
        cluster.shutdown().await.unwrap();
        let _ = std::fs::remove_dir_all(root);
    }
}
