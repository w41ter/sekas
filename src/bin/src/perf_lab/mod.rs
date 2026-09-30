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

#![allow(clippy::result_large_err)]

mod cases;
mod config;
mod report;
mod workload;

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use std::{fs, thread};

use anyhow::{Context as _, Result, anyhow, bail};
use clap::{Parser, ValueEnum};
use log::warn;
use sekas_api::server::v1::*;
use sekas_client::{
    AppError, ClientOptions, ConnManager, Database, GroupClient, NodeClient, RootClient, Router,
    SekasClient, StaticServiceDiscovery, TableDesc,
};
use sekas_runtime::{ExecutorOwner, ShutdownNotifier};
use sekas_server::{Config, NodeConfig};
use tracing_subscriber::EnvFilter;

use self::cases::{
    AutoShardBalance, AutoSplitMerge, BatchTxnCommit, HotspotDirectWriteDiagnostics,
    HotspotUpdateDiagnostics, MixedReadWrite, MultiKeyTxn, MultiKeyTxnMatrix, MvccGcImpact,
    MvccVersionAccumulation, NodeJoinScaleOut, NodeOfflineUnderWrite, PointRead, PrefixScan,
    ReplicaChangeUnderWrite, ReplicaRemoveUnderWrite, RootFailoverMatrix, RootLeaderFailover,
    SchemaChurn, SchemaChurnScale, ShardMetaChurnUnderRw, ShardMigrationUnderWrite,
    SingleKeyUpdate, SnapshotForcedDiagnostics, SnapshotUnderWrite, TransferLeaderUnderWrite,
    TxnConflict, ValueSizeMatrix,
};
use self::config::LabConfig;
use self::report::{
    CaseReport, MetricsRecorder, SuiteReport, compare_with_baseline, read_baseline_reports,
};
use self::workload::WorkloadReport;

#[derive(Debug, Parser)]
#[clap(about = "Run in-process performance lab scenarios")]
pub struct Command {
    /// The built-in case to run.
    ///
    /// When omitted, every built-in case is run.
    #[clap(long, value_enum)]
    case: Option<CaseKind>,

    /// Sets a custom config file.
    #[clap(long, value_name = "FILE")]
    conf: Option<String>,

    /// Override report output directory.
    #[clap(long, value_name = "DIR")]
    out_dir: Option<String>,

    /// Compare against a previous suite JSON report.
    #[clap(long, value_name = "FILE")]
    baseline: Option<String>,

    /// Return non-zero when baseline regression thresholds are exceeded.
    #[clap(long)]
    fail_on_regression: bool,
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, ValueEnum)]
enum CaseKind {
    SingleKeyUpdate,
    BatchTxnCommit,
    HotspotUpdateDiagnostics,
    HotspotDirectWriteDiagnostics,
    PointRead,
    MixedReadWrite,
    PrefixScan,
    TxnConflict,
    MultiKeyTxn,
    MultiKeyTxnMatrix,
    ValueSizeMatrix,
    ReplicaChangeUnderWrite,
    ReplicaRemoveUnderWrite,
    NodeJoinScaleOut,
    RootLeaderFailover,
    RootFailoverMatrix,
    SnapshotUnderWrite,
    SnapshotForcedDiagnostics,
    MvccVersionAccumulation,
    MvccGcImpact,
    AutoShardBalance,
    AutoSplitMerge,
    SchemaChurn,
    SchemaChurnScale,
    TransferLeaderUnderWrite,
    NodeOfflineUnderWrite,
    ShardMigrationUnderWrite,
    ShardMetaChurnUnderRw,
}

const ALL_CASES: &[CaseKind] = &[
    CaseKind::SingleKeyUpdate,
    CaseKind::BatchTxnCommit,
    CaseKind::HotspotUpdateDiagnostics,
    CaseKind::HotspotDirectWriteDiagnostics,
    CaseKind::PointRead,
    CaseKind::MixedReadWrite,
    CaseKind::PrefixScan,
    CaseKind::TxnConflict,
    CaseKind::MultiKeyTxn,
    CaseKind::MultiKeyTxnMatrix,
    CaseKind::ValueSizeMatrix,
    CaseKind::ReplicaChangeUnderWrite,
    CaseKind::ReplicaRemoveUnderWrite,
    CaseKind::NodeJoinScaleOut,
    CaseKind::RootLeaderFailover,
    CaseKind::RootFailoverMatrix,
    CaseKind::SnapshotUnderWrite,
    CaseKind::SnapshotForcedDiagnostics,
    CaseKind::MvccVersionAccumulation,
    CaseKind::MvccGcImpact,
    CaseKind::AutoShardBalance,
    CaseKind::AutoSplitMerge,
    CaseKind::SchemaChurn,
    CaseKind::SchemaChurnScale,
    CaseKind::TransferLeaderUnderWrite,
    CaseKind::NodeOfflineUnderWrite,
    CaseKind::ShardMigrationUnderWrite,
    CaseKind::ShardMetaChurnUnderRw,
];

impl CaseKind {
    fn name(self) -> &'static str {
        match self {
            CaseKind::SingleKeyUpdate => "single-key-update",
            CaseKind::BatchTxnCommit => "batch-txn-commit",
            CaseKind::HotspotUpdateDiagnostics => "hotspot-update-diagnostics",
            CaseKind::HotspotDirectWriteDiagnostics => "hotspot-direct-write-diagnostics",
            CaseKind::PointRead => "point-read",
            CaseKind::MixedReadWrite => "mixed-read-write",
            CaseKind::PrefixScan => "prefix-scan",
            CaseKind::TxnConflict => "txn-conflict",
            CaseKind::MultiKeyTxn => "multi-key-txn",
            CaseKind::MultiKeyTxnMatrix => "multi-key-txn-matrix",
            CaseKind::ValueSizeMatrix => "value-size-matrix",
            CaseKind::ReplicaChangeUnderWrite => "replica-change-under-write",
            CaseKind::ReplicaRemoveUnderWrite => "replica-remove-under-write",
            CaseKind::NodeJoinScaleOut => "node-join-scale-out",
            CaseKind::RootLeaderFailover => "root-leader-failover",
            CaseKind::RootFailoverMatrix => "root-failover-matrix",
            CaseKind::SnapshotUnderWrite => "snapshot-under-write",
            CaseKind::SnapshotForcedDiagnostics => "snapshot-forced-diagnostics",
            CaseKind::MvccVersionAccumulation => "mvcc-version-accumulation",
            CaseKind::MvccGcImpact => "mvcc-gc-impact",
            CaseKind::AutoShardBalance => "auto-shard-balance",
            CaseKind::AutoSplitMerge => "auto-split-merge",
            CaseKind::SchemaChurn => "schema-churn",
            CaseKind::SchemaChurnScale => "schema-churn-scale",
            CaseKind::TransferLeaderUnderWrite => "transfer-leader-under-write",
            CaseKind::NodeOfflineUnderWrite => "node-offline-under-write",
            CaseKind::ShardMigrationUnderWrite => "shard-migration-under-write",
            CaseKind::ShardMetaChurnUnderRw => "shard-meta-churn-under-rw",
        }
    }

    fn from_report_name(name: &str) -> Option<Self> {
        ALL_CASES.iter().copied().find(|case| case.name() == name)
    }

    fn configure(self, config: &mut LabConfig) {
        if matches!(self, CaseKind::BatchTxnCommit | CaseKind::ShardMetaChurnUnderRw) {
            // These cases rely on a fixed table layout throughout the workload.
            config.cluster.root.enable_shard_balance = false;
            config.cluster.root.enable_auto_shard_split = false;
            config.cluster.root.enable_auto_shard_merge = false;
        }
        if matches!(self, CaseKind::ReplicaChangeUnderWrite | CaseKind::ReplicaRemoveUnderWrite) {
            config.cluster.node.replica.testing_knobs.disable_scheduler_durable_task = true;
        }
    }
}

impl Command {
    pub fn run(self) -> Result<()> {
        let specs = self.run_specs()?;
        let cfg = LabConfig::load(&self)?;
        let suite_id = run_id();
        init_logging(&cfg, &suite_id)?;
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .worker_threads(cfg.runner_threads)
            .build()
            .context("build perf lab runtime")?;

        runtime.block_on(async move {
            let multi_case = specs.len() > 1;
            if multi_case {
                println!("perf-lab suite: {} cases", specs.len());
            }

            let mut reports = Vec::new();
            let mut has_failed_regression = false;
            for (idx, case) in specs.into_iter().enumerate() {
                let mut cfg = cfg.clone();
                case.configure(&mut cfg);
                let run_id = if multi_case {
                    format!("{suite_id}-{:02}", idx + 1)
                } else {
                    suite_id.clone()
                };
                println!("perf-lab case: {}", case.name());

                let mut lab = LabContext::start(cfg, run_id).await?;
                let result = run_case(case, &mut lab).await;
                lab.shutdown();
                let result = result?;

                has_failed_regression |= compare_report(&result)?;
                reports.push(result);
            }

            write_suite_report(&suite_id, &reports)?;
            if has_failed_regression {
                bail!("perf-lab regression threshold exceeded");
            }
            Ok(())
        })
    }

    fn run_specs(&self) -> Result<Vec<CaseKind>> {
        if let Some(path) = &self.baseline {
            validate_baseline(Path::new(path))?;
        }

        if let Some(case) = self.case {
            return Ok(vec![case]);
        }

        Ok(ALL_CASES.to_vec())
    }
}

async fn run_case(case: CaseKind, lab: &mut LabContext) -> Result<CaseReport> {
    match case {
        CaseKind::SingleKeyUpdate => SingleKeyUpdate.run(lab).await,
        CaseKind::BatchTxnCommit => BatchTxnCommit.run(lab).await,
        CaseKind::HotspotUpdateDiagnostics => HotspotUpdateDiagnostics.run(lab).await,
        CaseKind::HotspotDirectWriteDiagnostics => HotspotDirectWriteDiagnostics.run(lab).await,
        CaseKind::PointRead => PointRead.run(lab).await,
        CaseKind::MixedReadWrite => MixedReadWrite.run(lab).await,
        CaseKind::PrefixScan => PrefixScan.run(lab).await,
        CaseKind::TxnConflict => TxnConflict.run(lab).await,
        CaseKind::MultiKeyTxn => MultiKeyTxn.run(lab).await,
        CaseKind::MultiKeyTxnMatrix => MultiKeyTxnMatrix.run(lab).await,
        CaseKind::ValueSizeMatrix => ValueSizeMatrix.run(lab).await,
        CaseKind::ReplicaChangeUnderWrite => ReplicaChangeUnderWrite.run(lab).await,
        CaseKind::ReplicaRemoveUnderWrite => ReplicaRemoveUnderWrite.run(lab).await,
        CaseKind::NodeJoinScaleOut => NodeJoinScaleOut.run(lab).await,
        CaseKind::RootLeaderFailover => RootLeaderFailover.run(lab).await,
        CaseKind::RootFailoverMatrix => RootFailoverMatrix.run(lab).await,
        CaseKind::SnapshotUnderWrite => SnapshotUnderWrite.run(lab).await,
        CaseKind::SnapshotForcedDiagnostics => SnapshotForcedDiagnostics.run(lab).await,
        CaseKind::MvccVersionAccumulation => MvccVersionAccumulation.run(lab).await,
        CaseKind::MvccGcImpact => MvccGcImpact.run(lab).await,
        CaseKind::AutoShardBalance => AutoShardBalance.run(lab).await,
        CaseKind::AutoSplitMerge => AutoSplitMerge.run(lab).await,
        CaseKind::SchemaChurn => SchemaChurn.run(lab).await,
        CaseKind::SchemaChurnScale => SchemaChurnScale.run(lab).await,
        CaseKind::TransferLeaderUnderWrite => TransferLeaderUnderWrite.run(lab).await,
        CaseKind::NodeOfflineUnderWrite => NodeOfflineUnderWrite.run(lab).await,
        CaseKind::ShardMigrationUnderWrite => ShardMigrationUnderWrite.run(lab).await,
        CaseKind::ShardMetaChurnUnderRw => ShardMetaChurnUnderRw.run(lab).await,
    }
}

fn compare_report(result: &CaseReport) -> Result<bool> {
    let baseline = result.config.report.baseline.as_ref();
    if let Some(path) = baseline {
        let Some(comparison) = compare_with_baseline(result, Path::new(path))? else {
            println!("perf-lab baseline: no report for case '{}', skip comparison", result.case);
            return Ok(false);
        };
        let failed = comparison.failed();
        println!("{}", serde_json::to_string_pretty(&comparison)?);
        return Ok(failed && result.config.report.fail_on_regression);
    }
    Ok(false)
}

fn write_suite_report(run_id: &str, reports: &[CaseReport]) -> Result<()> {
    let out_dir = reports
        .first()
        .map(|report| report.config.report.out_dir.as_path())
        .ok_or_else(|| anyhow!("suite report requires at least one case report"))?;
    fs::create_dir_all(out_dir)
        .with_context(|| format!("create report dir {}", out_dir.display()))?;
    let suite = SuiteReport { run_id: run_id.to_owned(), reports: reports.to_vec() };
    let suite_path = out_dir.join(format!("suite-{run_id}.json"));
    fs::write(&suite_path, serde_json::to_vec_pretty(&suite)?)
        .with_context(|| format!("write suite report {}", suite_path.display()))?;
    println!("perf-lab suite report: {}", suite_path.display());
    Ok(())
}

fn validate_baseline(path: &Path) -> Result<()> {
    let reports = read_baseline_reports(path)?;
    let mut seen = HashMap::new();
    for report in reports {
        let case = CaseKind::from_report_name(&report.case).ok_or_else(|| {
            anyhow!("unknown perf-lab case '{}' in {}", report.case, path.display())
        })?;
        if seen.insert(case, ()).is_some() {
            bail!(
                "baseline {} contains multiple reports for case '{}'",
                path.display(),
                case.name()
            );
        }
    }
    Ok(())
}

pub(crate) trait PerfCase {
    fn name(&self) -> &'static str;
    async fn run(&self, lab: &mut LabContext) -> Result<CaseReport>;
}

pub(crate) struct LabContext {
    pub(crate) config: LabConfig,
    pub(crate) run_id: String,
    root_dir: PathBuf,
    pub(crate) nodes: HashMap<u64, String>,
    notifiers: HashMap<u64, ShutdownNotifier>,
    handles: HashMap<u64, thread::JoinHandle<()>>,
    conn_manager: ConnManager,
    pub(crate) router: Router,
    client: SekasClient,
    metrics: MetricsRecorder,
}

impl LabContext {
    async fn start(config: LabConfig, run_id: String) -> Result<Self> {
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
                timeout: Some(Duration::from_secs(10)),
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

    fn node_root_dir(&self, node_id: u64) -> PathBuf {
        let disks = &self.config.environment.disk_pools;
        if disks.is_empty() {
            self.root_dir.join(format!("node-{node_id}"))
        } else {
            let disk = &disks[node_id as usize % disks.len()];
            disk.join(&self.run_id).join(format!("node-{node_id}"))
        }
    }

    fn shutdown(&mut self) {
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

    async fn stop_server(&mut self, node_id: u64) -> Result<()> {
        self.notifiers.remove(&node_id);
        if let Some(handle) = self.handles.remove(&node_id) {
            handle.join().unwrap_or_default();
        }
        Ok(())
    }

    pub(crate) async fn add_server(&mut self) -> Result<u64> {
        let node_id = self.nodes.keys().copied().max().unwrap_or_default() + 1;
        let addr = next_n_listen_addrs(1)?.remove(0);
        let root_addr =
            self.nodes.get(&0).cloned().ok_or_else(|| anyhow!("root node address is missing"))?;
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

    fn group(&self, group_id: u64) -> GroupClient {
        GroupClient::lazy(group_id, self.client.clone())
    }

    pub(crate) async fn group_for_key(
        &self,
        table_id: u64,
        key: &[u8],
    ) -> Result<(u64, ShardDesc)> {
        for _ in 0..1000 {
            if let Ok((group, shard)) = self.router.find_shard(table_id, key) {
                return Ok((group.id, shard));
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        bail!("no group for key {:?}", key);
    }

    /// Co-locate the two keys' shards by moving the second shard if necessary.
    /// The caller is responsible for disabling shard balancing.
    pub(crate) async fn ensure_same_group(
        &self,
        first: (u64, &[u8]),
        second: (u64, &[u8]),
    ) -> Result<u64> {
        let (group_id, _) = self.group_for_key(first.0, first.1).await?;
        self.migrate_shard_to_group(second.0, second.1, group_id).await?;
        let (first_group, _) = self.group_for_key(first.0, first.1).await?;
        let (second_group, _) = self.group_for_key(second.0, second.1).await?;
        if first_group != group_id || second_group != group_id {
            bail!("keys did not converge to group {group_id}: {first_group} vs {second_group}");
        }
        Ok(group_id)
    }

    /// Separate the two keys' shards by moving the second shard if necessary.
    /// Requires an existing spare user group; does not change balancing
    /// settings.
    pub(crate) async fn ensure_different_group(
        &self,
        first: (u64, &[u8]),
        second: (u64, &[u8]),
    ) -> Result<(u64, u64)> {
        let (first_group, first_shard) = self.group_for_key(first.0, first.1).await?;
        let (second_group, second_shard) = self.group_for_key(second.0, second.1).await?;
        if first_shard.id == second_shard.id {
            bail!("cannot place keys in the same shard {} in different groups", first_shard.id);
        }
        if first_group != second_group {
            return Ok((first_group, second_group));
        }
        let mut destination = None;
        for _ in 0..400 {
            destination = self.find_group_without_shard(second_group).await?;
            if destination.is_some() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        let destination = destination.ok_or_else(|| {
            anyhow!(
                "no ready spare user group for shard {} in group {second_group}",
                second_shard.id
            )
        })?;
        let migration = self.migrate_shard_to_group(second.0, second.1, destination).await?;
        let (current_first, _) = self.group_for_key(first.0, first.1).await?;
        let (current_second, _) = self.group_for_key(second.0, second.1).await?;
        if current_first != first_group || current_second != migration.dest_group {
            bail!("keys did not converge to different groups: {current_first} vs {current_second}");
        }
        Ok((current_first, current_second))
    }

    pub(crate) async fn direct_put(
        &self,
        table_id: u64,
        key: Vec<u8>,
        value: Vec<u8>,
    ) -> Result<()> {
        let (group_id, shard) = self.group_for_key(table_id, &key).await?;
        let mut group = self.group(group_id);
        group
            .request(&group_request_union::Request::Write(ShardWriteRequest {
                shard_id: shard.id,
                puts: vec![PutRequest { key, value, ..Default::default() }],
                ..Default::default()
            }))
            .await?;
        Ok(())
    }

    pub(crate) async fn group_leader(&self, group_id: u64) -> Result<u64> {
        for _ in 0..1000 {
            if let Ok(group) = self.router.find_group(group_id)
                && let Some((leader, _)) = group.leader_state
            {
                return Ok(leader);
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        bail!("group {group_id} has no leader");
    }

    pub(crate) async fn group_leader_node(&self, group_id: u64) -> Result<u64> {
        let leader = self.group_leader(group_id).await?;
        let group = self.router.find_group(group_id)?;
        group
            .replicas
            .values()
            .find(|replica| replica.id == leader)
            .map(|replica| replica.node_id)
            .ok_or_else(|| anyhow!("group {group_id} leader replica {leader} has no node"))
    }

    pub(crate) async fn transfer_group_leader(
        &self,
        group_id: u64,
    ) -> Result<LeaderTransferResult> {
        self.ensure_group_voters(group_id, 2).await?;
        let group = self.router.find_group(group_id)?;
        let leader = group.leader_state.map(|v| v.0);
        let target = group
            .replicas
            .values()
            .find(|replica| Some(replica.id) != leader && replica.role == ReplicaRole::Voter as i32)
            .ok_or_else(|| anyhow!("group {group_id} has no follower voter"))?;
        let mut client = self.group(group_id);
        let started = Instant::now();
        client.transfer_leader(target.id).await?;
        let rpc_duration = started.elapsed();
        let route_started = Instant::now();
        let converged = self.wait_group_leader(group_id, target.id).await?;
        Ok(LeaderTransferResult {
            rpc_duration,
            route_convergence: route_started.elapsed(),
            route_converged: converged,
            target_replica: target.id,
        })
    }

    pub(crate) async fn ensure_group_voters(&self, group_id: u64, voters: usize) -> Result<()> {
        for _ in 0..400 {
            if let Ok(group) = self.router.find_group(group_id) {
                let current = group
                    .replicas
                    .values()
                    .filter(|replica| replica.role == ReplicaRole::Voter as i32)
                    .count();
                if current >= voters && group.leader_state.is_some() {
                    return Ok(());
                }
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        bail!("group {group_id} does not have {voters} voters");
    }

    pub(crate) async fn add_group_replica(&self, group_id: u64, node_id: u64) -> Result<Duration> {
        let started = Instant::now();
        let replica_id = group_id * 1000 + node_id + 100;
        let incoming = ReplicaDesc { id: replica_id, node_id, role: ReplicaRole::Voter as i32 };
        self.move_group_replicas(group_id, vec![incoming], vec![]).await?;
        self.wait_group_voter_on_node(group_id, node_id).await?;
        Ok(started.elapsed())
    }

    pub(crate) async fn remove_group_replica_on_node(
        &self,
        group_id: u64,
        node_id: u64,
        wait: Duration,
    ) -> Result<ReplicaRemoveResult> {
        let started = Instant::now();
        let group = self.router.find_group(group_id)?;
        let replica =
            group
                .replicas
                .values()
                .find(|replica| replica.node_id == node_id)
                .cloned()
                .ok_or_else(|| anyhow!("group {group_id} has no replica on node {node_id}"))?;
        self.move_group_replicas(group_id, vec![], vec![replica]).await?;
        let deadline = Instant::now() + wait;
        let mut converged = false;
        while Instant::now() < deadline {
            if let Ok(group) = self.router.find_group(group_id)
                && !group.replicas.values().any(|replica| replica.node_id == node_id)
            {
                converged = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        let final_voters = self
            .router
            .find_group(group_id)
            .ok()
            .map(|group| {
                group
                    .replicas
                    .values()
                    .filter(|replica| replica.role == ReplicaRole::Voter as i32)
                    .count()
            })
            .unwrap_or_default();
        Ok(ReplicaRemoveResult { duration: started.elapsed(), converged, final_voters })
    }

    async fn wait_group_voter_on_node(&self, group_id: u64, node_id: u64) -> Result<()> {
        for _ in 0..400 {
            if let Ok(group) = self.router.find_group(group_id)
                && group.replicas.values().any(|replica| {
                    replica.node_id == node_id && replica.role == ReplicaRole::Voter as i32
                })
            {
                return Ok(());
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        bail!("group {group_id} does not contain voter on node {node_id}");
    }

    async fn move_group_replicas(
        &self,
        group_id: u64,
        incoming: Vec<ReplicaDesc>,
        outgoing: Vec<ReplicaDesc>,
    ) -> Result<()> {
        let mut last_err = None;
        for _ in 0..400 {
            let mut client = self.group(group_id);
            match client.move_replicas(incoming.clone(), outgoing.clone()).await {
                Ok(_) => return Ok(()),
                Err(sekas_client::Error::AlreadyExists(message))
                    if message == "config change" || message == "MoveReplicas task" =>
                {
                    last_err = Some(message);
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
                Err(err) => return Err(err.into()),
            }
        }

        bail!(
            "group {group_id} move replicas is still busy: {}",
            last_err.unwrap_or_else(|| "unknown".to_owned())
        );
    }

    pub(crate) async fn migrate_shard_to_new_group(
        &self,
        table_id: u64,
        key: &[u8],
    ) -> Result<ShardMigrationResult> {
        let (src_group, shard) = self.group_for_key(table_id, key).await?;
        let dest_group = self
            .find_group_without_shard(src_group)
            .await?
            .ok_or_else(|| anyhow!("no destination group without shard {}", shard.id))?;
        self.migrate_shard_to_group(table_id, key, dest_group).await
    }

    pub(crate) async fn migrate_shard_to_group(
        &self,
        table_id: u64,
        key: &[u8],
        dest_group: u64,
    ) -> Result<ShardMigrationResult> {
        let (src_group, shard) = self.group_for_key(table_id, key).await?;
        if src_group == dest_group {
            return Ok(ShardMigrationResult {
                duration: Duration::ZERO,
                route_convergence: Duration::ZERO,
                route_converged: true,
                src_group,
                dest_group,
                shard_id: shard.id,
            });
        }
        let started = Instant::now();
        for _ in 0..16 {
            let src_epoch = self.router.find_group(src_group)?.epoch;
            if self.group_contains_shard(dest_group, shard.id) {
                return Ok(ShardMigrationResult {
                    duration: started.elapsed(),
                    route_convergence: Duration::ZERO,
                    route_converged: true,
                    src_group,
                    dest_group,
                    shard_id: shard.id,
                });
            }
            let mut group = self.group(dest_group);
            match group.accept_shard(src_group, src_epoch, &shard).await {
                Ok(()) => {}
                Err(err) => {
                    warn!(
                        "accept shard {} from group {} to group {} failed: {}",
                        shard.id, src_group, dest_group, err
                    );
                    tokio::time::sleep(Duration::from_millis(50)).await;
                    continue;
                }
            }
            let route_started = Instant::now();
            for _ in 0..1000 {
                if self.group_contains_shard(dest_group, shard.id) {
                    return Ok(ShardMigrationResult {
                        duration: started.elapsed(),
                        route_convergence: route_started.elapsed(),
                        route_converged: true,
                        src_group,
                        dest_group,
                        shard_id: shard.id,
                    });
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        }
        bail!("migrate shard {} did not finish", shard.id);
    }

    pub(crate) async fn split_shard_for_key(
        &self,
        table_id: u64,
        key: &[u8],
    ) -> Result<SplitShardResult> {
        let (group_id, shard) = self.group_for_key(table_id, key).await?;
        let before_epoch = self.router.find_group(group_id)?.epoch;
        let new_shard_id = shard.id + 10_000_000;
        let started = Instant::now();
        let mut group = self.group(group_id);
        group.split_shard(shard.id, new_shard_id, Some(key.to_vec())).await?;
        let rpc_duration = started.elapsed();
        let route_started = Instant::now();
        let route_converged = self.wait_group_epoch_advance(group_id, before_epoch).await?;
        Ok(SplitShardResult {
            rpc_duration,
            route_convergence: route_started.elapsed(),
            route_converged,
            group_id,
            left_shard_id: shard.id,
            right_shard_id: new_shard_id,
        })
    }

    pub(crate) async fn merge_shards(
        &self,
        group_id: u64,
        left_shard_id: u64,
        right_shard_id: u64,
    ) -> Result<MergeShardResult> {
        let before_epoch = self.router.find_group(group_id)?.epoch;
        let started = Instant::now();
        let mut last_err = None;
        for attempts in 1..=20 {
            let mut group = self.group(group_id);
            match group.merge_shard(left_shard_id, right_shard_id).await {
                Ok(()) => {
                    let rpc_duration = started.elapsed();
                    let route_started = Instant::now();
                    let route_converged =
                        self.wait_group_epoch_advance(group_id, before_epoch).await?;
                    return Ok(MergeShardResult {
                        rpc_duration,
                        route_convergence: route_started.elapsed(),
                        route_converged,
                        attempts,
                    });
                }
                Err(err) => {
                    last_err = Some(err);
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
            }
        }
        if let Some(err) = last_err {
            return Err(err.into());
        }
        bail!("merge shard did not run")
    }

    async fn wait_group_epoch_advance(&self, group_id: u64, before_epoch: u64) -> Result<bool> {
        for _ in 0..200 {
            if let Ok(group) = self.router.find_group(group_id)
                && group.epoch > before_epoch
            {
                return Ok(true);
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        Ok(false)
    }

    async fn wait_group_leader(&self, group_id: u64, expected_leader: u64) -> Result<bool> {
        for _ in 0..200 {
            if let Ok(group) = self.router.find_group(group_id)
                && group.leader_state.map(|leader| leader.0) == Some(expected_leader)
            {
                return Ok(true);
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        Ok(false)
    }

    async fn find_group_without_shard(&self, src_group: u64) -> Result<Option<u64>> {
        let body = self.client.handle_statement("show groups").await?;
        let sekas_parser::ExecuteResult::Data(groups) = serde_json::from_slice(&body)? else {
            bail!("SHOW GROUPS did not return groups");
        };
        let mut ids =
            groups.rows.iter().map(|row| row.values[0].as_u64().unwrap()).collect::<Vec<_>>();
        ids.sort_unstable();
        for group_id in ids {
            if let Ok(group) = self.router.find_group(group_id)
                && group.id != 0
                && group.id != src_group
                && group.replicas.len() >= self.config.cluster.root.replicas_per_group
                && group.leader_state.is_some()
            {
                return Ok(Some(group_id));
            }
        }
        Ok(None)
    }

    fn group_contains_shard(&self, group_id: u64, shard_id: u64) -> bool {
        self.router.find_group_by_shard(shard_id).map(|group| group.id == group_id).unwrap_or(false)
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

pub(crate) struct ReplicaRemoveResult {
    pub(crate) duration: Duration,
    pub(crate) converged: bool,
    pub(crate) final_voters: usize,
}

pub(crate) struct LeaderTransferResult {
    pub(crate) rpc_duration: Duration,
    pub(crate) route_convergence: Duration,
    pub(crate) route_converged: bool,
    pub(crate) target_replica: u64,
}

pub(crate) struct ShardMigrationResult {
    pub(crate) duration: Duration,
    pub(crate) route_convergence: Duration,
    pub(crate) route_converged: bool,
    pub(crate) src_group: u64,
    pub(crate) dest_group: u64,
    pub(crate) shard_id: u64,
}

pub(crate) struct SplitShardResult {
    pub(crate) rpc_duration: Duration,
    pub(crate) route_convergence: Duration,
    pub(crate) route_converged: bool,
    pub(crate) group_id: u64,
    pub(crate) left_shard_id: u64,
    pub(crate) right_shard_id: u64,
}

pub(crate) struct MergeShardResult {
    pub(crate) rpc_duration: Duration,
    pub(crate) route_convergence: Duration,
    pub(crate) route_converged: bool,
    pub(crate) attempts: u64,
}

fn init_logging(config: &LabConfig, run_id: &str) -> Result<()> {
    if !config.log.enabled {
        return Ok(());
    }

    fs::create_dir_all(&config.log.dir)
        .with_context(|| format!("create log dir {}", config.log.dir.display()))?;
    let log_file = config.log.dir.join(format!("perf-lab-{run_id}.log"));
    let file = fs::File::create(&log_file)
        .with_context(|| format!("create log file {}", log_file.display()))?;
    let writer = move || {
        file.try_clone().expect("perf-lab log file should be cloneable after initialization")
    };
    let filter_layer = EnvFilter::try_from_default_env()
        .or_else(|_| EnvFilter::try_new(&config.log.filter))
        .with_context(|| format!("parse log filter {}", config.log.filter))?;

    tracing_subscriber::fmt()
        .with_env_filter(filter_layer)
        .with_ansi(false)
        .with_writer(writer)
        .init();
    println!("perf-lab log: {}", log_file.display());

    Ok(())
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

fn unix_millis() -> u128 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_millis()
}

fn run_id() -> String {
    format!("{}", unix_millis())
}
