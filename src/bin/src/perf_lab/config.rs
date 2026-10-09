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
        ensure!(
            w.gc_retention_ms > 0 && w.gc_interval_ms > 0,
            "GC retention and interval must be positive"
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
            db: DbConfig {
                block_cache_size: 1 << 20,
                write_buffer_size: 256 << 10,
                max_write_buffer_number: 3,
                max_background_jobs: 2,
                max_sub_compactions: 1,
                ..DbConfig::default()
            },
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

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub(crate) struct WorkloadConfig {
    pub(crate) database: String,
    pub(crate) concurrency: usize,
    pub(crate) duration_secs: u64,
    pub(crate) warmup_secs: u64,
    pub(crate) cooldown_secs: u64,
    pub(crate) value_size: usize,
    pub(crate) key_space: u64,
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
            database: "perf_lab".to_owned(),
            concurrency: 32,
            duration_secs: 30,
            warmup_secs: 5,
            cooldown_secs: 5,
            value_size: 8192,
            key_space: 4096,
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
    fn example_configs_use_only_common_workload_parameters() {
        for contents in [
            include_str!("../../../../conf/perf-lab-smoke.toml"),
            include_str!("../../../../conf/perf-lab-snapshot.toml"),
            include_str!("../../../../conf/perf-lab-disk.toml"),
        ] {
            let config: LabConfig = toml::from_str(contents).unwrap();
            config.validate().unwrap();
        }
    }

    #[test]
    fn rejects_removed_case_parameters_in_workload_config_and_reports() {
        for name in [
            "version_counts",
            "txn_key_counts",
            "large_txn_key_counts",
            "group_counts",
            "hotset_sizes",
            "scan_limits",
            "write_ratios",
            "concurrency_levels",
            "offered_qps",
            "metadata_counts",
        ] {
            let contents = format!("[workload]\n{name} = [1]");
            let error = toml::from_str::<LabConfig>(&contents).unwrap_err();
            assert!(error.to_string().contains(name));

            let mut report_config = serde_json::to_value(LabConfig::default()).unwrap();
            report_config["workload"][name] = serde_json::json!([1]);
            assert!(serde_json::from_value::<LabConfig>(report_config).is_err());
        }
    }

    #[test]
    fn rejects_invalid_common_workload_parameters() {
        let mut cfg = LabConfig::default();
        assert!(cfg.validate().is_ok());
        cfg.workload.key_space = 0;
        assert!(cfg.validate().is_err());
        cfg.workload.key_space = 10_000;
        cfg.workload.request_timeout_secs = 0;
        assert!(cfg.validate().is_err());
    }
}
