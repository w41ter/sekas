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

use std::path::Path;

use anyhow::{Context, Result};
use clap::ValueEnum;
use serde::Deserialize;

#[derive(Clone, Copy, Debug, PartialEq, Eq, ValueEnum)]
pub(super) enum Phase {
    Load,
    Run,
}

#[derive(Debug, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub(super) struct Config {
    pub addrs: Vec<String>,
    pub database: String,
    pub table: String,
    pub threads: usize,
    pub workers: usize,
    pub operations: u64,
    pub report_interval_secs: u64,
    pub seed: Option<u64>,
    pub workload: WorkloadConfig,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            addrs: vec!["127.0.0.1:21805".into()],
            database: "db".into(),
            table: "benchmark".into(),
            threads: std::thread::available_parallelism().map_or(1, usize::from),
            workers: 1,
            operations: 100_000,
            report_interval_secs: 10,
            seed: None,
            workload: WorkloadConfig::default(),
        }
    }
}

impl Config {
    pub fn read(path: Option<&Path>) -> Result<Self> {
        if let Some(path) = path {
            let contents = std::fs::read_to_string(path)
                .with_context(|| format!("read benchmark config {}", path.display()))?;
            toml::from_str(&contents)
                .with_context(|| format!("parse benchmark config {}", path.display()))
        } else {
            Ok(Self::default())
        }
    }
}

#[derive(Clone, Copy, Debug, Default, Deserialize)]
#[serde(rename_all = "lowercase")]
pub(super) enum KeyDistribution {
    #[default]
    Uniform,
    Zipfian,
}

#[derive(Debug, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub(super) struct WorkloadConfig {
    pub records: u64,
    pub value_size: usize,
    pub distribution: KeyDistribution,
    pub zipfian_theta: f64,
    pub txn_keys: u64,
    pub scan_length: u64,
    pub insert_start: Option<u64>,
    pub shared_keys: u64,
    pub mix: Mix,
}

impl Default for WorkloadConfig {
    fn default() -> Self {
        Self {
            records: 10_000,
            value_size: 10,
            distribution: KeyDistribution::Uniform,
            zipfian_theta: 0.99,
            txn_keys: 1,
            scan_length: 10,
            insert_start: None,
            shared_keys: 1000,
            mix: Mix { read: 1.0, update: 1.0, ..Default::default() },
        }
    }
}

#[derive(Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub(super) struct Mix {
    pub read: f64,
    pub insert: f64,
    pub update: f64,
    pub scan: f64,
    pub cas: f64,
    pub insert_if_absent: f64,
    pub increment: f64,
}
