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

use std::future::Future;
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};

use crate::cluster::{ClusterController, ClusterSpec, ClusterStatus};
use crate::nemesis::{ChaosEvent, EventOutcome, Nemesis, NemesisConfig, RandomNemesis};
use crate::runner::{WorkloadFailure, WorkloadReport, WorkloadRunner};

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ChaosConfig {
    pub duration: Duration,
    pub recovery_timeout: Duration,
}

impl Default for ChaosConfig {
    fn default() -> Self {
        Self { duration: Duration::from_secs(60), recovery_timeout: Duration::from_secs(30) }
    }
}

impl ChaosConfig {
    pub fn validate(&self) -> Result<(), String> {
        if self.duration.is_zero() || self.recovery_timeout.is_zero() {
            return Err("chaos duration and recovery timeout must be non-zero".to_string());
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum ChaosPhase {
    Deploy,
    SetupWorkload,
    Nemesis,
    Restore,
    FinalStatus,
    Shutdown,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ChaosFailure {
    pub phase: ChaosPhase,
    pub message: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ChaosReport {
    pub config: ChaosConfig,
    pub cluster: ClusterSpec,
    pub nemesis: NemesisConfig,
    pub workload: Option<WorkloadReport>,
    pub nemesis_events: Vec<ChaosEvent>,
    pub dropped_nemesis_events: u64,
    pub failures: Vec<ChaosFailure>,
    pub final_cluster_status: Option<ClusterStatus>,
    pub shutdown_complete: bool,
    pub elapsed: Duration,
}

impl ChaosReport {
    pub fn is_valid(&self) -> bool {
        self.failures.is_empty()
            && self.shutdown_complete
            && self.workload.as_ref().is_some_and(WorkloadReport::is_valid)
    }
}

/// Owns the complete deploy -> disturb -> restore -> verify -> shutdown flow.
pub struct ChaosRunner<C> {
    config: ChaosConfig,
    cluster_spec: ClusterSpec,
    cluster: C,
    nemesis: RandomNemesis,
}

impl<C: ClusterController> ChaosRunner<C> {
    pub fn new(
        config: ChaosConfig,
        cluster_spec: ClusterSpec,
        cluster: C,
        nemesis_config: NemesisConfig,
    ) -> Result<Self, String> {
        config.validate()?;
        cluster_spec.validate().map_err(|err| err.to_string())?;
        let nemesis = RandomNemesis::new(nemesis_config)?;
        Ok(Self { config, cluster_spec, cluster, nemesis })
    }

    /// Runs chaos after using `setup` to create the database, tables, executor,
    /// and workload against the freshly deployed cluster addresses.
    pub async fn run<F, Fut>(mut self, setup: F) -> ChaosReport
    where
        F: FnOnce(Vec<String>) -> Fut,
        Fut: Future<Output = Result<WorkloadRunner, WorkloadFailure>>,
    {
        let started = Instant::now();
        let mut failures = Vec::new();
        let mut workload_report = None;
        let mut final_cluster_status = None;
        let mut shutdown_complete = false;

        if let Err(err) = self.cluster.deploy(self.cluster_spec.clone()).await {
            failures.push(ChaosFailure { phase: ChaosPhase::Deploy, message: err.to_string() });
        } else {
            match setup(self.cluster_spec.addresses()).await {
                Ok(workload) => {
                    let handle = workload.start();
                    let deadline = Instant::now() + self.config.duration;
                    while Instant::now() < deadline {
                        let pause = self
                            .nemesis
                            .config()
                            .interval
                            .min(deadline.saturating_duration_since(Instant::now()));
                        tokio::time::sleep(pause).await;
                        if Instant::now() >= deadline {
                            break;
                        }
                        let status = match self.cluster.status().await {
                            Ok(status) => status,
                            Err(err) => {
                                failures.push(ChaosFailure {
                                    phase: ChaosPhase::Nemesis,
                                    message: err.to_string(),
                                });
                                break;
                            }
                        };
                        let Some(action) = self.nemesis.next(&status).await else {
                            continue;
                        };
                        let event = self.nemesis.apply(&mut self.cluster, action).await;
                        if !matches!(event.outcome, EventOutcome::Succeeded) {
                            failures.push(ChaosFailure {
                                phase: ChaosPhase::Nemesis,
                                message: format!(
                                    "nemesis event {} {:?}: {:?}",
                                    event.sequence, event.kind, event.outcome
                                ),
                            });
                            break;
                        }
                    }

                    handle.request_stop();
                    if let Err(err) = self.cluster.restore().await {
                        failures.push(ChaosFailure {
                            phase: ChaosPhase::Restore,
                            message: err.to_string(),
                        });
                    } else if let Err(err) =
                        self.cluster.wait_ready(self.config.recovery_timeout).await
                    {
                        failures.push(ChaosFailure {
                            phase: ChaosPhase::Restore,
                            message: err.to_string(),
                        });
                    }
                    workload_report = Some(handle.finish().await);
                }
                Err(err) => failures.push(ChaosFailure {
                    phase: ChaosPhase::SetupWorkload,
                    message: err.to_string(),
                }),
            }
            match self.cluster.status().await {
                Ok(status) => final_cluster_status = Some(status),
                Err(err) => failures.push(ChaosFailure {
                    phase: ChaosPhase::FinalStatus,
                    message: err.to_string(),
                }),
            }
        }

        match self.cluster.shutdown().await {
            Ok(()) => shutdown_complete = true,
            Err(err) => failures
                .push(ChaosFailure { phase: ChaosPhase::Shutdown, message: err.to_string() }),
        }

        ChaosReport {
            config: self.config,
            cluster: self.cluster_spec,
            nemesis: self.nemesis.config().clone(),
            workload: workload_report,
            nemesis_events: self.nemesis.events(),
            dropped_nemesis_events: self.nemesis.dropped_events(),
            failures,
            final_cluster_status,
            shutdown_complete,
            elapsed: started.elapsed(),
        }
    }
}
