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

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::cluster::BoxFuture;
use crate::model::WriterModel;

pub type OperationResult<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct WriterId(pub u32);

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct OperationId {
    pub writer: WriterId,
    pub sequence: u64,
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct LogicalKey {
    pub table: u64,
    pub key: Vec<u8>,
}

impl LogicalKey {
    pub fn new(table: u64, key: Vec<u8>) -> Self {
        Self { table, key }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct KeyValue {
    pub key: LogicalKey,
    pub value: Vec<u8>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum Mutation {
    Put(KeyValue),
    Delete(LogicalKey),
}

impl Mutation {
    pub fn key(&self) -> &LogicalKey {
        match self {
            Mutation::Put(value) => &value.key,
            Mutation::Delete(key) => key,
        }
    }

    pub fn value(&self) -> Option<Vec<u8>> {
        match self {
            Mutation::Put(value) => Some(value.value.clone()),
            Mutation::Delete(_) => None,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum Operation {
    Put(KeyValue),
    Delete(LogicalKey),
    Batch(Vec<Mutation>),
    Transaction(Vec<Mutation>),
    AddI64 { key: LogicalKey, delta: i64 },
}

impl Operation {
    pub fn is_compound(&self) -> bool {
        matches!(self, Operation::Batch(_) | Operation::Transaction(_))
    }
}

/// What must be read as one logical verification unit.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct VerificationTarget {
    pub keys: Vec<LogicalKey>,
    /// Transactions and atomic batches require a single database snapshot.
    pub require_snapshot: bool,
}

impl VerificationTarget {
    pub fn new(mut keys: Vec<LogicalKey>, require_snapshot: bool) -> Self {
        keys.sort();
        keys.dedup();
        Self { keys, require_snapshot }
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct Observation {
    pub values: BTreeMap<LogicalKey, Option<Vec<u8>>>,
}

impl Observation {
    pub fn target(&self, require_snapshot: bool) -> VerificationTarget {
        VerificationTarget::new(self.values.keys().cloned().collect(), require_snapshot)
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct OperationPlan {
    pub id: OperationId,
    pub operation: Operation,
    /// Complete state of the verification target before the operation.
    pub before: Observation,
    /// Complete state of the verification target after the operation.
    pub after: Observation,
}

impl OperationPlan {
    pub fn verification(&self) -> VerificationTarget {
        self.after.target(self.operation.is_compound())
    }

    /// Classifies a stable observation made after executing this operation.
    pub fn reconcile(&self, observed: &Observation) -> ReconcileResult {
        if observed == &self.after {
            ReconcileResult::Applied
        } else if observed == &self.before {
            ReconcileResult::NotApplied
        } else {
            ReconcileResult::Violation(format!(
                "operation {:?} observed neither its pre-state nor post-state",
                self.id
            ))
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum ExecuteResult {
    Succeeded,
    /// The request may or may not have taken effect.
    Unknown(String),
    /// The request cannot succeed without changing the workload or cluster.
    Fatal(String),
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum ReconcileResult {
    Applied,
    NotApplied,
    Violation(String),
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ExecutorCapabilities {
    /// Ambiguous atomic adds can be resolved without applying the delta twice.
    pub resolvable_add_i64: bool,
}

/// Generates a deterministic operation from a deterministic stable model.
pub trait OperationGenerator: Send + Sync {
    fn generate(&self, seed: u64, id: OperationId, model: &WriterModel) -> OperationPlan;
}

/// Executes operations and observes all keys needed to reconcile them.
pub trait OperationExecutor: Send + Sync {
    fn capabilities(&self) -> ExecutorCapabilities {
        ExecutorCapabilities::default()
    }

    fn execute<'a>(&'a self, plan: &'a OperationPlan) -> BoxFuture<'a, ExecuteResult>;

    fn observe<'a>(
        &'a self,
        target: &'a VerificationTarget,
    ) -> BoxFuture<'a, OperationResult<Observation>>;
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::{
        KeyValue, LogicalKey, Observation, Operation, OperationId, OperationPlan, ReconcileResult,
        WriterId,
    };

    fn observation(key: &LogicalKey, value: &[u8]) -> Observation {
        Observation { values: BTreeMap::from([(key.clone(), Some(value.to_vec()))]) }
    }

    #[test]
    fn reconciliation_distinguishes_retry_success_and_violation() {
        let key = LogicalKey::new(1, b"k".to_vec());
        let plan = OperationPlan {
            id: OperationId { writer: WriterId(2), sequence: 7 },
            operation: Operation::Put(KeyValue { key: key.clone(), value: b"after".to_vec() }),
            before: observation(&key, b"before"),
            after: observation(&key, b"after"),
        };
        assert_eq!(plan.reconcile(&plan.before), ReconcileResult::NotApplied);
        assert_eq!(plan.reconcile(&plan.after), ReconcileResult::Applied);
        assert!(matches!(
            plan.reconcile(&observation(&key, b"partial")),
            ReconcileResult::Violation(_)
        ));
    }
}
