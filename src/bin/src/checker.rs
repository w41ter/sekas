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

use std::path::PathBuf;

use clap::{Parser, Subcommand};
use sekas_checker::report::RunReport;
use sekas_checker::{CheckOutcome, HistoryShrinker, LinearizabilityChecker};

#[derive(Parser)]
#[clap(about = "Inspect and shrink Sekas checker artifacts")]
pub struct CheckerCommand {
    #[clap(subcommand)]
    subcmd: CheckerSubCommand,
}

#[derive(Subcommand)]
enum CheckerSubCommand {
    Check(CheckCommand),
    Shrink(ShrinkCommand),
}

#[derive(Parser)]
#[clap(about = "Re-check a linearizability artifact")]
struct CheckCommand {
    #[clap(value_name = "FILE")]
    input: PathBuf,

    #[clap(long, default_value = "2048")]
    max_calls_per_key: usize,

    #[clap(long, default_value = "1000000")]
    max_states_per_key: usize,
}

#[derive(Parser)]
#[clap(about = "Minimize a failing linearizability artifact")]
struct ShrinkCommand {
    #[clap(value_name = "FILE")]
    input: PathBuf,

    #[clap(long, value_name = "FILE")]
    output: Option<PathBuf>,

    #[clap(long, default_value = "2048")]
    max_calls_per_key: usize,

    #[clap(long, default_value = "1000000")]
    max_states_per_key: usize,
}

impl CheckerCommand {
    pub fn run(self) -> anyhow::Result<()> {
        match self.subcmd {
            CheckerSubCommand::Check(cmd) => cmd.run(),
            CheckerSubCommand::Shrink(cmd) => cmd.run(),
        }
    }
}

impl CheckCommand {
    fn run(self) -> anyhow::Result<()> {
        let artifact = RunReport::read_json(&self.input)?;
        let checker = checker(self.max_calls_per_key, self.max_states_per_key);
        let report = checker.check(&artifact.history);
        println!("{}", serde_json::to_string_pretty(&report)?);
        if !matches!(report.outcome, CheckOutcome::Valid) {
            anyhow::bail!("history is not valid: {:?}", report.outcome);
        }
        Ok(())
    }
}

impl ShrinkCommand {
    fn run(self) -> anyhow::Result<()> {
        let artifact = RunReport::read_json(&self.input)?;
        let checker = checker(self.max_calls_per_key, self.max_states_per_key);
        let shrinker = HistoryShrinker::new(checker.clone());
        let Some(shrink) = shrinker.shrink_invalid(&artifact.history) else {
            anyhow::bail!("artifact is not an invalid linearizability history");
        };
        let report = checker.check(&shrink.history);
        let output = self.output.unwrap_or_else(|| self.input.with_extension("min.json"));
        RunReport {
            name: format!("{}-min", artifact.name),
            seed: artifact.seed,
            history: shrink.history,
            check: report,
        }
        .write_json(&output)?;
        println!(
            "shrunk {} calls to {} calls in {} iterations; wrote {}",
            shrink.original_calls,
            shrink.minimized_calls,
            shrink.iterations,
            output.display()
        );
        Ok(())
    }
}

fn checker(max_calls_per_key: usize, max_states_per_key: usize) -> LinearizabilityChecker {
    LinearizabilityChecker::new()
        .with_max_calls_per_key(max_calls_per_key)
        .with_max_states_per_key(max_states_per_key)
}
