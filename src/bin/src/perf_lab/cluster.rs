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

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use std::{fs, thread};

use anyhow::{Context as _, Result, anyhow, bail};
use sekas_api::server::v1::ReplicaRole;
use sekas_client::{
    AppError, ClientOptions, ConnManager, Database, NodeClient, RootClient, Router, SekasClient,
    StaticServiceDiscovery, TableDesc,
};
use sekas_runtime::{ExecutorOwner, ShutdownNotifier};
use sekas_server::{Config, NodeConfig};

use super::config::LabConfig;
use super::report::MetricsRecorder;

pub(crate) struct LabContext {
    pub(crate) config: LabConfig,
    pub(crate) run_id: String,
    root_dir: PathBuf,
    pub(crate) nodes: HashMap<u64, String>,
    notifiers: HashMap<u64, ShutdownNotifier>,
    handles: HashMap<u64, thread::JoinHandle<()>>,
    conn_manager: ConnManager,
    pub(crate) router: Router,
    pub(super) client: SekasClient,
    pub(super) metrics: MetricsRecorder,
}

impl LabContext {
    pub(super) async fn start(config: LabConfig, run_id: String) -> Result<Self> {
        let root_dir = config.environment.root_dir.join(&run_id);
        if config.environment.cleanup && root_dir.exists() {
            fs::remove_dir_all(&root_dir)
                .with_context(|| format!("remove old root dir {}", root_dir.display()))?;
        }
        fs::create_dir_all(&root_dir)
            .with_context(|| format!("create root dir {}", root_dir.display()))?;

        let mut lab = LabContext {
            config,
            run_id,
            root_dir,
            nodes: HashMap::new(),
            notifiers: HashMap::new(),
            handles: HashMap::new(),
            conn_manager: ConnManager::new(),
            router: Router::new(RootClient::new(
                Arc::new(StaticServiceDiscovery::new(vec![])),
                ConnManager::new(),
            ))
            .await,
            client: SekasClient::new(ClientOptions::default(), vec![]).await?,
            metrics: MetricsRecorder::default(),
        };
        lab.start_cluster().await?;
        Ok(lab)
    }

    async fn start_cluster(&mut self) -> Result<()> {
        let addrs = next_n_listen_addrs(self.config.cluster.nodes)?;
        let nodes = addrs
            .into_iter()
            .enumerate()
            .map(|(idx, addr)| (idx as u64, addr))
            .collect::<HashMap<_, _>>();
        let root_addr =
            nodes.get(&0).cloned().ok_or_else(|| anyhow!("cluster must contain node 0"))?;
        let mut ids = nodes.keys().copied().collect::<Vec<_>>();
        ids.sort_unstable();
        for id in ids {
            let addr = nodes.get(&id).unwrap().clone();
            let join_list = if id == 0 { vec![] } else { vec![root_addr.clone()] };
            self.spawn_server(id, addr.clone(), id == 0, join_list)?;
            node_client_with_retry(&addr).await?;
            self.nodes.insert(id, addr);
        }

        self.conn_manager = ConnManager::new();
        let discovery =
            Arc::new(StaticServiceDiscovery::new(self.nodes.values().cloned().collect()));
        let root_client = RootClient::new(discovery, self.conn_manager.clone());
        self.router = Router::new(root_client.clone()).await;
        self.client = SekasClient::build(
            ClientOptions {
                connect_timeout: Some(Duration::from_millis(500)),
                timeout: Some(Duration::from_secs(self.config.workload.request_timeout_secs)),
            },
            self.router.clone(),
            root_client,
            self.conn_manager.clone(),
        );
        self.wait_root_group_ready().await?;
        if self.config.cluster.root.enable_group_balance {
            self.wait_non_root_group_ready(self.config.cluster.root.replicas_per_group).await?;
        }
        Ok(())
    }

    fn spawn_server(
        &mut self,
        node_id: u64,
        addr: String,
        init: bool,
        join_list: Vec<String>,
    ) -> Result<()> {
        let node_root = self.node_root_dir(node_id);
        fs::create_dir_all(&node_root)
            .with_context(|| format!("create node root dir {}", node_root.display()))?;
        let cfg = Config {
            root_dir: node_root,
            addr: addr.clone(),
            cpu_nums: self.config.cluster.cpus_per_node as u32,
            init,
            enable_proxy_service: self.config.cluster.enable_proxy_service,
            join_list,
            node: NodeConfig {
                replica: self.config.cluster.node.replica.clone(),
                ..self.config.cluster.node.clone()
            },
            raft: self.config.cluster.raft.clone(),
            root: self.config.cluster.root.clone(),
            executor: Default::default(),
            db: self.config.cluster.db.clone(),
        };
        let notifier = ShutdownNotifier::new();
        let shutdown = notifier.subscribe();
        let worker_threads = self.config.cluster.cpus_per_node.max(1);
        let handle = thread::spawn(move || {
            let owner = ExecutorOwner::new(worker_threads);
            if let Err(err) = sekas_server::run(cfg, owner.executor(), shutdown) {
                panic!("perf-lab server {node_id} at {addr} exits with {err}");
            }
        });
        self.notifiers.insert(node_id, notifier);
        self.handles.insert(node_id, handle);
        Ok(())
    }

    pub(super) fn node_root_dir(&self, node_id: u64) -> PathBuf {
        let disks = &self.config.environment.disk_pools;
        if disks.is_empty() {
            self.root_dir.join(format!("node-{node_id}"))
        } else {
            let disk = &disks[node_id as usize % disks.len()];
            disk.join(&self.run_id).join(format!("node-{node_id}"))
        }
    }

    pub(super) fn shutdown(&mut self) {
        let _ = std::mem::take(&mut self.notifiers);
        for (_, handle) in std::mem::take(&mut self.handles) {
            handle.join().unwrap_or_default();
        }
        if self.config.environment.cleanup {
            let _ = fs::remove_dir_all(&self.root_dir);
            for disk in &self.config.environment.disk_pools {
                let _ = fs::remove_dir_all(disk.join(&self.run_id));
            }
        }
    }

    pub(super) async fn stop_server(&mut self, node_id: u64) -> Result<()> {
        self.notifiers.remove(&node_id);
        if let Some(handle) = self.handles.remove(&node_id) {
            handle.join().unwrap_or_default();
        }
        Ok(())
    }

    pub(crate) async fn add_server(&mut self) -> Result<u64> {
        let node_id = self.nodes.keys().copied().max().unwrap_or_default() + 1;
        let addr = next_n_listen_addrs(1)?.remove(0);
        let root_addr = self
            .nodes
            .iter()
            .find(|(id, _)| self.handles.contains_key(id))
            .map(|(_, addr)| addr.clone())
            .ok_or_else(|| anyhow!("no live seed node"))?;
        self.spawn_server(node_id, addr.clone(), false, vec![root_addr])?;
        node_client_with_retry(&addr).await?;
        self.nodes.insert(node_id, addr);
        Ok(node_id)
    }

    async fn wait_root_group_ready(&self) -> Result<()> {
        for _ in 0..1000 {
            if self.router.find_group(0).ok().and_then(|g| g.leader_state).is_some() {
                return Ok(());
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        bail!("root group has no leader");
    }

    async fn wait_non_root_group_ready(&self, voters: usize) -> Result<()> {
        for _ in 0..1000 {
            for group_id in 1..10000 {
                let Ok(group) = self.router.find_group(group_id) else {
                    continue;
                };
                let current_voters = group
                    .replicas
                    .values()
                    .filter(|replica| replica.role == ReplicaRole::Voter as i32)
                    .count();
                if current_voters >= voters && group.leader_state.is_some() {
                    return Ok(());
                }
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        bail!("no non-root group with {voters} voters is ready");
    }

    pub(crate) async fn database(&self) -> Result<Database> {
        match self.client.create_database(self.config.workload.database.clone()).await {
            Ok(db) => Ok(db),
            Err(AppError::AlreadyExists(_)) => {
                Ok(self.client.open_database(self.config.workload.database.clone()).await?)
            }
            Err(err) => Err(err.into()),
        }
    }

    pub(crate) async fn table(&self, db: &Database, name: &str) -> Result<TableDesc> {
        match db.create_table(name.to_owned()).await {
            Ok(table) => Ok(table),
            Err(AppError::AlreadyExists(_)) => Ok(db.open_table(name.to_owned()).await?),
            Err(err) => Err(err.into()),
        }
    }

    pub(crate) fn mark(&mut self, name: impl Into<String>) {
        self.metrics.mark(name.into())
    }
}

impl Drop for LabContext {
    fn drop(&mut self) {
        self.shutdown();
    }
}

impl LabContext {
    pub(super) async fn restart_server(&mut self, node: u64) -> Result<()> {
        let addr = self.nodes.get(&node).cloned().ok_or_else(|| anyhow!("unknown node {node}"))?;
        self.spawn_server(node, addr.clone(), false, vec![])?;
        node_client_with_retry(&addr).await?;
        Ok(())
    }
}

async fn node_client_with_retry(addr: &str) -> Result<NodeClient> {
    for _ in 0..1000 {
        match NodeClient::connect(addr.to_owned()).await {
            Ok(client) => return Ok(client),
            Err(_) => tokio::time::sleep(Duration::from_millis(50)).await,
        }
    }
    bail!("connect to {addr} timeout");
}

fn next_n_listen_addrs(n: usize) -> Result<Vec<String>> {
    let mut addrs = Vec::with_capacity(n);
    let listener = std::net::TcpListener::bind(("127.0.0.1", 0))?;
    let start = listener.local_addr()?.port();
    drop(listener);
    for offset in 0..n {
        addrs.push(format!("127.0.0.1:{}", start + offset as u16));
    }
    Ok(addrs)
}
