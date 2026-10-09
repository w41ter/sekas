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

use std::fs;
use std::path::PathBuf;

use anyhow::{Context as _, Result, ensure};
use sekas_server::{DbConfig, NodeConfig, RaftConfig, RootConfig};
use serde::{Deserialize, Serialize};

use super::Command;

const DEFAULT_REPORT_DIR: &str = "target/perf-lab";
const DEFAULT_LOG_DIR: &str = "log";

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(default)]
pub(crate) struct LabConfig {
    pub(crate) build_profile: String,
    pub(crate) runner_threads: usize,
    pub(crate) environment: EnvironmentConfig,
    pub(crate) cluster: ClusterConfig,
    pub(crate) workload: WorkloadConfig,
    pub(crate) report: ReportConfig,
    pub(crate) log: LogConfig,
}

impl Default for LabConfig {
    fn default() -> Self {
        LabConfig {
            build_profile: build_profile(),
            runner_threads: num_cpus::get().max(2),
            environment: EnvironmentConfig::default(),
            cluster: ClusterConfig::default(),
            workload: WorkloadConfig::default(),
            report: ReportConfig::default(),
            log: LogConfig::default(),
        }
    }
}

impl LabConfig {
    pub(crate) fn load(cmd: &Command) -> Result<Self> {
        let mut cfg = LabConfig::default();
        if let Some(path) = &cmd.conf {
            let contents =
                fs::read_to_string(path).with_context(|| format!("read config {path}"))?;
            cfg = toml::from_str(&contents)
                .map_err(|err| anyhow::anyhow!("parse config {path}: {err}"))?;
        }
        if let Some(out_dir) = &cmd.out_dir {
            cfg.report.out_dir = PathBuf::from(out_dir);
        }
        if let Some(baseline) = &cmd.baseline {
            cfg.report.baseline = Some(baseline.clone());
        }
        cfg.report.fail_on_regression |= cmd.fail_on_regression;
        cfg.build_profile = build_profile();
        cfg.validate()?;
        Ok(cfg)
    }

    fn validate(&self) -> Result<()> {
        let w = &self.workload;
        ensure!(
            self.runner_threads > 0 && self.cluster.cpus_per_node > 0,
            "thread counts must be positive"
        );
        ensure!(
            self.cluster.nodes >= 3 && self.cluster.root.replicas_per_group == 3,
            "perf-lab requires at least three nodes and three voters per group"
        );
        ensure!(
            w.concurrency > 0 && w.duration_secs > 0 && w.key_space > 0 && w.value_size > 0,
            "concurrency, duration_secs, key_space and value_size must be positive"
        );
        ensure!(
            w.request_timeout_secs > 0 && w.event_timeout_secs > 0 && w.storage_duration_secs > 0,
            "timeouts and storage_duration_secs must be positive"
        );
        ensure!(
            self.cluster.root.liveness_threshold_sec > self.cluster.root.heartbeat_timeout_sec,
            "liveness_threshold_sec must exceed heartbeat_timeout_sec"
        );
        for values in [
            &w.version_counts,
            &w.txn_key_counts,
            &w.large_txn_key_counts,
            &w.group_counts,
            &w.hotset_sizes,
            &w.scan_limits,
            &w.metadata_counts,
        ] {
            ensure!(
                !values.is_empty() && values.iter().all(|v| *v > 0),
                "matrix values must be nonempty and positive"
            );
        }
        ensure!(
            w.txn_key_counts.iter().all(|n| *n <= w.key_space),
            "txn_key_counts must not exceed key_space"
        );
        ensure!(
            !w.concurrency_levels.is_empty() && w.concurrency_levels.iter().all(|v| *v > 0),
            "concurrency_levels must be positive"
        );
        ensure!(
            !w.offered_qps.is_empty() && w.offered_qps.iter().all(|v| v.is_finite() && *v > 0.0),
            "offered_qps must be finite and positive"
        );
        ensure!(
            w.gc_retention_ms > 0 && w.gc_interval_ms > 0,
            "GC retention and interval must be positive"
        );
        ensure!(
            w.write_ratios.iter().all(|v| v.is_finite() && *v > 0.0 && *v < 1.0)
                && !w.write_ratios.is_empty(),
            "write_ratios must be between zero and one (exclusive)"
        );
        ensure!(
            (0.0..=1.0).contains(&self.report.max_failure_rate),
            "max_failure_rate must be in [0, 1]"
        );
        Ok(())
    }
}

fn build_profile() -> String {
    if cfg!(debug_assertions) { "debug" } else { "release" }.to_owned()
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(default)]
pub(crate) struct EnvironmentConfig {
    pub(crate) root_dir: PathBuf,
    pub(crate) cleanup: bool,
    pub(crate) disk_pools: Vec<PathBuf>,
}

impl Default for EnvironmentConfig {
    fn default() -> Self {
        EnvironmentConfig {
            root_dir: std::env::temp_dir().join("sekas-perf-lab"),
            cleanup: true,
            disk_pools: Vec::new(),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(default)]
pub(crate) struct ClusterConfig {
    pub(crate) nodes: usize,
    pub(crate) cpus_per_node: usize,
    pub(crate) enable_proxy_service: bool,
    pub(crate) db: DbConfig,
    pub(crate) node: NodeConfig,
    pub(crate) raft: RaftConfig,
    pub(crate) root: RootConfig,
}

impl Default for ClusterConfig {
    fn default() -> Self {
        ClusterConfig {
            nodes: 3,
            cpus_per_node: 2,
            enable_proxy_service: false,
            db: DbConfig { max_background_jobs: 2, max_sub_compactions: 1, ..DbConfig::default() },
            node: NodeConfig::default(),
            raft: RaftConfig { tick_interval_ms: 100, ..RaftConfig::default() },
            root: RootConfig {
                enable_group_balance: true,
                enable_replica_balance: true,
                enable_leader_balance: false,
                enable_shard_balance: false,
                replicas_per_group: 3,
                schedule_interval_sec: 1,
                ..RootConfig::default()
            },
        }
    }
}

#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub(crate) enum ReadWorkingSet {
    #[default]
    Memory,
    Disk,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(default)]
pub(crate) struct WorkloadConfig {
    pub(crate) read_working_set: ReadWorkingSet,
    pub(crate) database: String,
    pub(crate) concurrency: usize,
    pub(crate) duration_secs: u64,
    pub(crate) warmup_secs: u64,
    pub(crate) cooldown_secs: u64,
    pub(crate) value_size: usize,
    pub(crate) key_space: u64,
    pub(crate) version_counts: Vec<u64>,
    pub(crate) txn_key_counts: Vec<u64>,
    pub(crate) large_txn_key_counts: Vec<u64>,
    pub(crate) group_counts: Vec<u64>,
    pub(crate) hotset_sizes: Vec<u64>,
    pub(crate) scan_limits: Vec<u64>,
    pub(crate) concurrency_levels: Vec<usize>,
    pub(crate) offered_qps: Vec<f64>,
    pub(crate) write_ratios: Vec<f64>,
    pub(crate) metadata_counts: Vec<u64>,
    pub(crate) request_timeout_secs: u64,
    pub(crate) event_timeout_secs: u64,
    pub(crate) storage_duration_secs: u64,
    pub(crate) gc_retention_ms: u64,
    pub(crate) gc_interval_ms: u64,
    pub(crate) conflict_hold_ms: u64,
    pub(crate) retry_limit: usize,
}

impl Default for WorkloadConfig {
    fn default() -> Self {
        WorkloadConfig {
            read_working_set: ReadWorkingSet::Memory,
            database: "perf_lab".to_owned(),
            concurrency: 32,
            duration_secs: 30,
            warmup_secs: 5,
            cooldown_secs: 5,
            value_size: 128,
            key_space: 10_000,
            version_counts: vec![1, 100, 1000],
            txn_key_counts: vec![1, 8, 32],
            large_txn_key_counts: vec![64, 256, 1024],
            group_counts: vec![1, 2, 4],
            hotset_sizes: vec![1, 16, 1024],
            scan_limits: vec![16, 128, 1024],
            concurrency_levels: vec![1, 4, 16, 64],
            offered_qps: vec![100.0, 1000.0, 5000.0],
            write_ratios: vec![0.05, 0.5],
            metadata_counts: vec![1, 10, 100],
            request_timeout_secs: 10,
            event_timeout_secs: 60,
            storage_duration_secs: 120,
            gc_retention_ms: 1000,
            gc_interval_ms: 100,
            conflict_hold_ms: 5,
            retry_limit: 8,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(default)]
pub(crate) struct ReportConfig {
    pub(crate) out_dir: PathBuf,
    pub(crate) baseline: Option<String>,
    pub(crate) fail_on_regression: bool,
    pub(crate) max_qps_drop_percent: f64,
    pub(crate) max_latency_increase_percent: f64,
    pub(crate) max_failure_rate: f64,
}

impl Default for ReportConfig {
    fn default() -> Self {
        ReportConfig {
            out_dir: PathBuf::from(DEFAULT_REPORT_DIR),
            baseline: None,
            fail_on_regression: false,
            max_qps_drop_percent: 5.0,
            max_latency_increase_percent: 10.0,
            max_failure_rate: 0.0,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(default)]
pub(crate) struct LogConfig {
    pub(crate) enabled: bool,
    pub(crate) dir: PathBuf,
    pub(crate) filter: String,
}

impl Default for LogConfig {
    fn default() -> Self {
        LogConfig { enabled: true, dir: PathBuf::from(DEFAULT_LOG_DIR), filter: "info".to_owned() }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn rejects_invalid_workload_and_matrix_parameters() {
        let mut cfg = LabConfig::default();
        assert!(cfg.validate().is_ok());
        cfg.workload.key_space = 0;
        assert!(cfg.validate().is_err());
        cfg.workload.key_space = 10_000;
        cfg.workload.offered_qps = vec![f64::NAN];
        assert!(cfg.validate().is_err());
        cfg.workload.offered_qps = vec![100.0];
        cfg.workload.group_counts = vec![];
        assert!(cfg.validate().is_err());
    }
}
