// Copyright 2023-present The Sekas Authors.
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
use std::io::IsTerminal;
use std::net::SocketAddr;
use std::ops::Range;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result, bail, ensure};
use clap::Parser;
use log::info;
use sekas_client::{AppError, ClientOptions, Database, SekasClient};
use tokio::task::JoinSet;
use tokio::time::{Instant, MissedTickBehavior};
use tracing_subscriber::EnvFilter;

use super::config::{Config, Phase};
use super::report::Metrics;
use super::workload::Workload;
use super::{metrics, worker};

#[derive(Parser)]
#[clap(about = "Run a benchmark against a Sekas cluster")]
pub struct Command {
    /// Read benchmark settings from a TOML file
    #[clap(long, value_name = "FILE")]
    conf: Option<PathBuf>,

    /// Load the initial dataset or run a workload against it
    #[clap(long, value_enum, default_value = "run")]
    phase: Phase,

    /// Serve Prometheus metrics at /admin/metrics on this address while the
    /// benchmark runs
    #[clap(long, value_name = "ADDR")]
    metrics_addr: Option<SocketAddr>,
}

impl Command {
    pub fn run(self) -> Result<()> {
        let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
        tracing_subscriber::fmt()
            .with_env_filter(filter)
            .with_ansi(std::io::stderr().is_terminal())
            .init();

        let config = Config::read(self.conf.as_deref())?;
        ensure!(config.threads > 0, "threads must be positive");
        ensure!(config.workers > 0, "workers must be positive");
        ensure!(config.report_interval_secs > 0, "report_interval_secs must be positive");
        ensure!(!config.addrs.is_empty(), "addrs must contain a cluster address");
        let workload = Workload::new(&config.workload, self.phase, config.operations)?;

        tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .worker_threads(config.threads)
            .build()?
            .block_on(async move {
                let benchmark = run_benchmark(config, self.phase, workload);
                let Some(addr) = self.metrics_addr else {
                    return benchmark.await;
                };
                let server = metrics::bind(addr).await?;
                tokio::select! {
                    result = benchmark => result,
                    result = server.serve() => {
                        result.context("serve benchmark metrics")?;
                        bail!("benchmark metrics server stopped");
                    }
                }
            })
    }
}

async fn run_benchmark(config: Config, phase: Phase, workload: Workload) -> Result<()> {
    let interval = Duration::from_secs(config.report_interval_secs);
    let seed = config.seed.unwrap_or_else(rand::random);
    let (db, table) = open_target(&config, phase).await?;
    let worker_count = (config.workers as u64).min(workload.operations);
    info!(
        "{phase:?} on {}.{} with {worker_count} workers and seed {seed}",
        config.database, config.table
    );

    let workload = Arc::new(workload);
    let metrics = Arc::new(Metrics::new());
    let start = metrics.snapshot();
    let first_tick =
        Instant::now().checked_add(interval).context("report interval is too large")?;
    let mut workers = JoinSet::new();
    for index in 0..worker_count {
        let indexes = operation_range(workload.operations, worker_count, index);
        let seed = seed.wrapping_add(index);
        let (db, workload, metrics) = (db.clone(), workload.clone(), metrics.clone());
        workers.spawn(async move {
            worker::run(&db, table, &workload, &metrics, indexes, seed).await;
        });
    }

    let mut timer = tokio::time::interval_at(first_tick, interval);
    timer.set_missed_tick_behavior(MissedTickBehavior::Skip);
    let mut previous = start.clone();
    let interrupt = tokio::signal::ctrl_c();
    tokio::pin!(interrupt);
    let mut interrupted = false;
    while !workers.is_empty() {
        tokio::select! {
            Some(result) = workers.join_next() => result?,
            result = &mut interrupt => {
                result.context("listen for Ctrl-C")?;
                workers.shutdown().await;
                interrupted = true;
            }
            _ = timer.tick() => {
                let current = metrics.snapshot();
                current.display_since(&previous);
                previous = current;
            }
        }
    }

    println!("Benchmark totals");
    let end = metrics.snapshot();
    end.display_since(&start);
    ensure!(!interrupted, "benchmark interrupted");
    let failures = end.failures();
    ensure!(failures == 0, "{failures} benchmark operations failed");
    Ok(())
}

async fn open_target(config: &Config, phase: Phase) -> Result<(Database, u64)> {
    let options = ClientOptions {
        connect_timeout: Some(Duration::from_millis(200)),
        timeout: Some(Duration::from_millis(500)),
    };
    let client = SekasClient::new(options, config.addrs.clone()).await?;
    let database = config.database.clone();
    let table = config.table.clone();
    let db = match phase {
        Phase::Load => match client.create_database(database.clone()).await {
            Err(AppError::AlreadyExists(_)) => client.open_database(database).await,
            result => result,
        },
        Phase::Run => client.open_database(database).await,
    }
    .context("open benchmark database")?;
    let table = match phase {
        Phase::Load => match db.create_table(table.clone()).await {
            Err(AppError::AlreadyExists(_)) => db.open_table(table).await,
            result => result,
        },
        Phase::Run => db.open_table(table).await,
    }
    .context("open benchmark table")?;
    Ok((db, table.id))
}

fn operation_range(total: u64, workers: u64, worker: u64) -> Range<u64> {
    let share = total / workers;
    let remainder = total % workers;
    let start = worker * share + worker.min(remainder);
    start..start + share + u64::from(worker < remainder)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn worker_ranges_cover_remainder() {
        assert_eq!(operation_range(10, 3, 0), 0..4);
        assert_eq!(operation_range(10, 3, 1), 4..7);
        assert_eq!(operation_range(10, 3, 2), 7..10);
    }
}
