use std::path::PathBuf;

use anyhow::{Context, Result};
use serde::Serialize;

use crate::histogram::Summary;

#[derive(Debug, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum Status {
    Ok,
    Skipped,
}

#[derive(Debug, Serialize)]
pub(crate) struct BenchmarkResult {
    pub(crate) category: String,
    pub(crate) benchmark: String,
    pub(crate) parameter: String,
    pub(crate) status: Status,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) summary: Option<Summary>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) message: Option<String>,
}

impl BenchmarkResult {
    pub(crate) fn ok(
        category: &str,
        benchmark: &str,
        parameter: impl Into<String>,
        summary: Summary,
    ) -> Self {
        Self {
            category: category.into(),
            benchmark: benchmark.into(),
            parameter: parameter.into(),
            status: Status::Ok,
            summary: Some(summary),
            message: None,
        }
    }

    pub(crate) fn skipped(
        category: &str,
        benchmark: &str,
        parameter: impl Into<String>,
        error: &anyhow::Error,
    ) -> Self {
        Self {
            category: category.into(),
            benchmark: benchmark.into(),
            parameter: parameter.into(),
            status: Status::Skipped,
            summary: None,
            message: Some(format!("{error:#}")),
        }
    }
}

#[derive(Debug, Serialize)]
pub(crate) struct Environment {
    pub(crate) hostname: String,
    pub(crate) kernel: String,
    pub(crate) cpu_model: String,
    pub(crate) logical_cpus: usize,
    pub(crate) cpu_governor: String,
    pub(crate) load_average: String,
    pub(crate) cgroup: String,
    pub(crate) disk_dir: PathBuf,
    pub(crate) disk_device: u64,
    pub(crate) filesystem_type: String,
    pub(crate) filesystem_block_size: i64,
}

#[derive(Debug, Serialize)]
pub(crate) struct Report {
    pub(crate) schema_version: u32,
    pub(crate) tool_version: &'static str,
    pub(crate) duration_secs: u64,
    pub(crate) warmup_secs: u64,
    pub(crate) environment: Environment,
    pub(crate) results: Vec<BenchmarkResult>,
}

pub(crate) fn print_table(report: &Report) {
    println!("Sekas local machine baseline");
    println!(
        "host={} cpu=\"{}\" kernel={}",
        report.environment.hostname, report.environment.cpu_model, report.environment.kernel
    );
    println!(
        "cpus={} governor={} load={} disk_dir={} fs={} block_size={}B",
        report.environment.logical_cpus,
        report.environment.cpu_governor,
        report.environment.load_average,
        report.environment.disk_dir.display(),
        report.environment.filesystem_type,
        report.environment.filesystem_block_size,
    );
    println!("duration_per_case={}s warmup={}s", report.duration_secs, report.warmup_secs);
    println!();
    println!(
        "{:<10} {:<27} {:<13} {:>10} {:>10} {:>11} {:>11} {:>11} {:>10}",
        "CATEGORY",
        "BENCHMARK",
        "PARAM",
        "AVG(us)",
        "P50(us)",
        "P99(us)",
        "P99.9(us)",
        "MAX(us)",
        "OPS/s"
    );
    for result in &report.results {
        match &result.summary {
            Some(summary) => println!(
                "{:<10} {:<27} {:<13} {:>10.3} {:>10.3} {:>11.3} {:>11.3} {:>11.3} {:>10.0}",
                result.category,
                result.benchmark,
                result.parameter,
                summary.avg_us,
                summary.p50_us,
                summary.p99_us,
                summary.p999_us,
                summary.max_us,
                summary.ops_per_sec,
            ),
            None => println!(
                "{:<10} {:<27} {:<13} {:>10}",
                result.category, result.benchmark, result.parameter, "SKIPPED"
            ),
        }
    }

    let skipped: Vec<_> = report.results.iter().filter(|result| result.summary.is_none()).collect();
    if !skipped.is_empty() {
        println!();
        println!("Skipped benchmarks:");
        for result in skipped {
            println!(
                "  {}.{}: {}",
                result.category,
                result.benchmark,
                result.message.as_deref().unwrap_or("unknown error")
            );
        }
    }
}

pub(crate) fn write_json(path: &PathBuf, report: &Report) -> Result<()> {
    let bytes = serde_json::to_vec_pretty(report)?;
    std::fs::write(path, bytes).with_context(|| format!("write JSON report to {}", path.display()))
}
