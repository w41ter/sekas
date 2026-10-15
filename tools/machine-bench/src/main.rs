mod histogram;
mod local;
mod report;
mod storage;
mod system;

use std::path::PathBuf;
use std::time::Duration;

use anyhow::{Context, Result};
use clap::Parser;
use report::Report;

#[derive(Debug, Parser)]
#[clap(
    name = "sekas-machine-bench",
    version,
    about = "Measure local machine latency primitives relevant to Sekas"
)]
struct Command {
    /// Measurement time for each benchmark case.
    #[clap(long, default_value = "3")]
    duration_secs: u64,

    /// Warm-up time excluded from each benchmark result.
    #[clap(long, default_value = "1")]
    warmup_secs: u64,

    /// Directory on the same filesystem as the intended Sekas data directory.
    #[clap(long, default_value = ".", value_name = "DIR")]
    disk_dir: PathBuf,

    /// Size of the temporary storage benchmark file.
    #[clap(long, default_value = "64")]
    file_size_mib: u64,

    /// Also write a machine-readable report with complete histogram buckets.
    #[clap(long, value_name = "FILE")]
    json: Option<PathBuf>,
}

fn main() -> Result<()> {
    let command = Command::parse();
    anyhow::ensure!(command.duration_secs > 0, "--duration-secs must be greater than zero");
    anyhow::ensure!(command.file_size_mib >= 4, "--file-size-mib must be at least 4");

    let environment = system::environment(&command.disk_dir)?;
    let warmup = Duration::from_secs(command.warmup_secs);
    let duration = Duration::from_secs(command.duration_secs);

    let mut results = local::run(warmup, duration);
    results.extend(
        storage::run(&environment.disk_dir, command.file_size_mib, warmup, duration)
            .context("prepare storage benchmarks")?,
    );

    let report = Report {
        schema_version: 1,
        tool_version: env!("CARGO_PKG_VERSION"),
        duration_secs: command.duration_secs,
        warmup_secs: command.warmup_secs,
        environment,
        results,
    };
    report::print_table(&report);
    if let Some(path) = command.json {
        report::write_json(&path, &report)?;
        println!();
        println!("JSON report: {}", path.display());
    }
    Ok(())
}
