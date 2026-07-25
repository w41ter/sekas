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

use anyhow::{Context as _, Result};
use sekas_server::{DbConfig, NodeConfig, RaftConfig, RootConfig};
use serde::{Deserialize, Serialize};

use super::Command;

const DEFAULT_REPORT_DIR: &str = "target/perf-lab";

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(default)]
pub(crate) struct LabConfig {
    pub(crate) runner_threads: usize,
    pub(crate) environment: EnvironmentConfig,
    pub(crate) cluster: ClusterConfig,
    pub(crate) workload: WorkloadConfig,
    pub(crate) report: ReportConfig,
}

impl Default for LabConfig {
    fn default() -> Self {
        LabConfig {
            runner_threads: num_cpus::get().max(2),
            environment: EnvironmentConfig::default(),
            cluster: ClusterConfig::default(),
            workload: WorkloadConfig::default(),
            report: ReportConfig::default(),
        }
    }
}

impl LabConfig {
    pub(crate) fn load(cmd: &Command) -> Result<Self> {
        let mut cfg = LabConfig::default();
        if let Some(path) = &cmd.conf {
            let contents =
                fs::read_to_string(path).with_context(|| format!("read config {path}"))?;
            cfg = toml::from_str(&contents).with_context(|| format!("parse config {path}"))?;
        }
        if let Some(out_dir) = &cmd.out_dir {
            cfg.report.out_dir = PathBuf::from(out_dir);
        }
        if let Some(baseline) = &cmd.baseline {
            cfg.report.baseline = Some(baseline.clone());
        }
        cfg.report.fail_on_regression |= cmd.fail_on_regression;
        Ok(cfg)
    }
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
        let mut root = RootConfig::default();
        root.enable_group_balance = true;
        root.enable_replica_balance = true;
        root.enable_leader_balance = false;
        root.enable_shard_balance = false;
        root.replicas_per_group = 3;
        root.schedule_interval_sec = 1;

        let mut raft = RaftConfig::default();
        raft.tick_interval_ms = 100;

        ClusterConfig {
            nodes: 3,
            cpus_per_node: 2,
            enable_proxy_service: false,
            db: DbConfig { max_background_jobs: 2, max_sub_compactions: 1, ..DbConfig::default() },
            node: NodeConfig::default(),
            raft,
            root,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(default)]
pub(crate) struct WorkloadConfig {
    pub(crate) database: String,
    pub(crate) table: String,
    pub(crate) second_table: String,
    pub(crate) concurrency: usize,
    pub(crate) duration_secs: u64,
    pub(crate) warmup_secs: u64,
    pub(crate) cooldown_secs: u64,
    pub(crate) value_size: usize,
    pub(crate) key_space: u64,
    pub(crate) report_interval_secs: u64,
}

impl Default for WorkloadConfig {
    fn default() -> Self {
        WorkloadConfig {
            database: "perf_lab".to_owned(),
            table: "t1".to_owned(),
            second_table: "t2".to_owned(),
            concurrency: 32,
            duration_secs: 30,
            warmup_secs: 5,
            cooldown_secs: 5,
            value_size: 128,
            key_space: 10_000,
            report_interval_secs: 5,
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
}

impl Default for ReportConfig {
    fn default() -> Self {
        ReportConfig {
            out_dir: PathBuf::from(DEFAULT_REPORT_DIR),
            baseline: None,
            fail_on_regression: false,
            max_qps_drop_percent: 5.0,
            max_latency_increase_percent: 10.0,
        }
    }
}
