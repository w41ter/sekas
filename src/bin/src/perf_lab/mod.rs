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
mod cluster;
mod config;
mod observer;
mod report;
mod runner;
mod topology;
mod workload;

use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::Result;
use cases::{Case, CaseCategory, CaseKind};
use clap::{Parser, ValueEnum};
pub(crate) use cluster::LabContext;
use serde::{Deserialize, Serialize};
use workload::WorkloadReport;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize, ValueEnum)]
#[serde(rename_all = "kebab-case")]
pub(crate) enum RunProfile {
    Smoke,
    #[default]
    Regression,
    Characterization,
    Stress,
}

#[derive(Debug, Parser)]
#[clap(about = "Run in-process performance lab scenarios")]
pub struct Command {
    /// Select the execution scale. Case semantics remain owned by each case.
    #[clap(long, value_enum)]
    profile: Option<RunProfile>,

    /// The built-in case to run.
    ///
    /// When omitted, run the selected category (sentinel by default).
    #[clap(long, value_enum)]
    case: Option<CaseKind>,

    /// Run one semantic category. Mutually exclusive with --case and --all.
    #[clap(long, value_enum, conflicts_with_all = &["case", "all"])]
    category: Option<CaseCategory>,

    /// Run all categories, including long operational and capacity scenarios.
    #[clap(long, conflicts_with_all = &["case", "category"])]
    all: bool,

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

impl Command {
    fn build_cases(&self) -> Result<Vec<Case>> {
        if let Some(path) = &self.baseline {
            runner::validate_baseline(Path::new(path))?;
        }

        if let Some(case) = self.case {
            return Ok(vec![Case::build(case)]);
        }

        Ok(CaseKind::value_variants()
            .iter()
            .copied()
            .map(Case::build)
            .filter(|case| {
                self.all || case.category() == self.category.unwrap_or(CaseCategory::Sentinel)
            })
            .collect())
    }
}
fn unix_millis() -> u128 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_millis()
}

fn run_id() -> String {
    format!("{}", unix_millis())
}

fn trace_every_n() -> Option<u64> {
    std::env::var("SEKAS_PERF_LAB_TRACE_EVERY_N")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .filter(|value| *value > 0)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn selection_defaults_to_sentinel_and_rejects_ambiguous_flags() {
        let command = Command::try_parse_from(["perf-lab"]).unwrap();
        assert!(
            command
                .build_cases()
                .unwrap()
                .iter()
                .all(|case| case.category() == CaseCategory::Sentinel)
        );
        let command = Command::try_parse_from(["perf-lab", "--category", "operational"]).unwrap();
        assert_eq!(command.build_cases().unwrap().len(), 14);
        assert!(Command::try_parse_from(["perf-lab", "--case", "update", "--all"]).is_err());
        assert!(
            Command::try_parse_from(["perf-lab", "--category", "sentinel", "--case", "update",])
                .is_err()
        );
    }
}
