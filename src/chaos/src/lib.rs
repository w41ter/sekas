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

//! Deterministic chaos framework for Sekas.
//!
//! The crate includes stable-state workloads, an independent-process cluster
//! backend, a seeded nemesis, and the full restore-before-verify lifecycle. See
//! `DESIGN.md` in the crate root for the complete design and remaining fault
//! backends.

pub mod cluster;
pub mod generator;
pub mod model;
pub mod nemesis;
pub mod orchestrator;
pub mod progress;
pub mod runner;
pub mod sekas;
pub mod workload;

pub use cluster::{
    AdminAction, BoxFuture, ClusterCapabilities, ClusterController, ClusterError, ClusterSpec,
    ClusterStatus, FaultHandle, FaultSpec, LocalProcessCluster, NodeId, NodeSpec, NodeState,
    NodeStatus, ReadinessProbe, Result as ClusterResult, SekasReadinessProbe,
};
pub use generator::{DeterministicGenerator, OperationWeights, SizeRange, WorkloadConfig};
pub use model::WriterModel;
pub use nemesis::{
    ActionWeights, ChaosEvent, ChaosEventKind, EventOutcome, Nemesis, NemesisConfig, RandomNemesis,
    ShardMoveCandidate,
};
pub use orchestrator::{ChaosConfig, ChaosFailure, ChaosPhase, ChaosReport, ChaosRunner};
pub use progress::{ProgressSnapshot, WriterProgress};
pub use runner::{
    WorkloadEvent, WorkloadEventKind, WorkloadFailure, WorkloadHandle, WorkloadReport,
    WorkloadRunner, WorkloadStats, WriterSnapshot,
};
pub use sekas::SekasExecutor;
pub use workload::{
    ExecuteResult, ExecutorCapabilities, KeyValue, LogicalKey, Mutation, Observation, Operation,
    OperationExecutor, OperationGenerator, OperationId, OperationPlan, OperationResult,
    ReconcileResult, VerificationTarget, WriterId,
};
