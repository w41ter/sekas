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

use std::collections::{BTreeMap, HashMap};
use std::fs;
use std::path::Path;

use anyhow::{Context as _, Result, anyhow, bail};
use tracing_subscriber::EnvFilter;

use super::config::LabConfig;
use super::report::{CaseReport, SuiteReport, compare_with_baseline, read_baseline_reports};
use super::{CaseKind, Command, LabContext, run_id};

impl Command {
    pub fn run(self) -> Result<()> {
        let specs = self.build_cases()?;
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
            let mut errors = BTreeMap::new();
            let out_dir = cfg.report.out_dir.clone();
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

                let mut lab = match LabContext::start(cfg, run_id).await {
                    Ok(lab) => lab,
                    Err(error) => {
                        let reason = format!("cluster startup: {error:#}");
                        eprintln!("perf-lab case {} failed: {reason}", case.name());
                        errors.insert(case.name().to_owned(), reason);
                        write_suite_report(&suite_id, &reports, &errors, &out_dir)?;
                        continue;
                    }
                };
                let result = case.run(&mut lab).await;
                lab.shutdown();
                let result = match result {
                    Ok(report) => report,
                    Err(error) => {
                        let reason = format!("{error:#}");
                        eprintln!("perf-lab case {} failed: {reason}", case.name());
                        errors.insert(case.name().to_owned(), reason);
                        write_suite_report(&suite_id, &reports, &errors, &out_dir)?;
                        continue;
                    }
                };

                has_failed_regression |= compare_report(&result)?;
                reports.push(result);
            }

            write_suite_report(&suite_id, &reports, &errors, &out_dir)?;
            if !errors.is_empty() {
                bail!(
                    "perf-lab cases failed: {}",
                    errors.keys().cloned().collect::<Vec<_>>().join(", ")
                );
            }
            if has_failed_regression {
                bail!("perf-lab regression threshold exceeded");
            }
            Ok(())
        })
    }
}

fn compare_report(result: &CaseReport) -> Result<bool> {
    let baseline = result.config.report.baseline.as_ref();
    if let Some(path) = baseline {
        let Some(comparison) = compare_with_baseline(result, Path::new(path))? else {
            if result.config.report.fail_on_regression {
                bail!("baseline has no report for case '{}'", result.case);
            }
            println!("perf-lab baseline: no report for case '{}', skip comparison", result.case);
            return Ok(false);
        };
        let failed = comparison.failed();
        println!("{}", serde_json::to_string_pretty(&comparison)?);
        return Ok(failed && result.config.report.fail_on_regression);
    }
    Ok(false)
}

fn write_suite_report(
    run_id: &str,
    reports: &[CaseReport],
    errors: &BTreeMap<String, String>,
    out_dir: &Path,
) -> Result<()> {
    fs::create_dir_all(out_dir)
        .with_context(|| format!("create report dir {}", out_dir.display()))?;
    let suite = SuiteReport {
        schema_version: 2,
        run_id: run_id.to_owned(),
        reports: reports.to_vec(),
        errors: errors.clone(),
    };
    let suite_path = out_dir.join(format!("suite-{run_id}.json"));
    fs::write(&suite_path, serde_json::to_vec_pretty(&suite)?)
        .with_context(|| format!("write suite report {}", suite_path.display()))?;
    println!("perf-lab suite report: {}", suite_path.display());
    Ok(())
}

pub(super) fn validate_baseline(path: &Path) -> Result<()> {
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
