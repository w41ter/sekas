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

use sekas_client::{AppError, Database, Txn, WriteBuilder};

use crate::cluster::BoxFuture;
use crate::workload::{
    ExecuteResult, ExecutorCapabilities, Mutation, Observation, Operation, OperationExecutor,
    OperationPlan, OperationResult, VerificationTarget,
};

#[derive(Clone, Debug)]
pub struct SekasExecutor {
    database: Database,
}

impl SekasExecutor {
    pub fn new(database: Database) -> Self {
        Self { database }
    }

    #[allow(clippy::result_large_err)]
    async fn execute_inner(&self, plan: &OperationPlan) -> std::result::Result<(), AppError> {
        match &plan.operation {
            Operation::Put(value) => {
                self.database.put(value.key.table, value.key.key.clone(), value.value.clone()).await
            }
            Operation::Delete(key) => self.database.delete(key.table, key.key.clone()).await,
            Operation::Batch(mutations) => {
                let mut txn = self.database.begin_txn();
                add_mutations(&mut txn, mutations);
                txn.commit().await.map(|_| ())
            }
            Operation::Transaction(mutations) => {
                let mut txn = self.database.begin_txn();
                // Allocating the start version before buffering writes avoids
                // the local blind-write fast path and exercises the full txn
                // protocol even if all selected keys currently share a group.
                txn.start_version().await?;
                add_mutations(&mut txn, mutations);
                txn.commit().await.map(|_| ())
            }
            Operation::AddI64 { key, delta } => {
                let mut txn = self.database.begin_txn();
                txn.put(key.table, WriteBuilder::new(key.key.clone()).ensure_add(*delta));
                txn.commit().await.map(|_| ())
            }
        }
    }

    async fn observe_inner(&self, target: &VerificationTarget) -> OperationResult<Observation> {
        let mut values = BTreeMap::new();
        if target.require_snapshot {
            let txn = self.database.begin_txn();
            for key in &target.keys {
                let value = txn
                    .get(key.table, key.key.clone())
                    .await
                    .map_err(|err| Box::new(err) as Box<dyn std::error::Error + Send + Sync>)?;
                values.insert(key.clone(), value);
            }
        } else {
            for key in &target.keys {
                let value = self
                    .database
                    .get(key.table, key.key.clone())
                    .await
                    .map_err(|err| Box::new(err) as Box<dyn std::error::Error + Send + Sync>)?;
                values.insert(key.clone(), value);
            }
        }
        Ok(Observation { values })
    }
}

impl OperationExecutor for SekasExecutor {
    fn capabilities(&self) -> ExecutorCapabilities {
        // The current public client API does not expose a transaction identity
        // that resolves an ambiguous AddI64 without risking a duplicate delta.
        ExecutorCapabilities { resolvable_add_i64: false }
    }

    fn execute<'a>(&'a self, plan: &'a OperationPlan) -> BoxFuture<'a, ExecuteResult> {
        Box::pin(async move {
            match self.execute_inner(plan).await {
                Ok(()) => ExecuteResult::Succeeded,
                Err(
                    err @ (AppError::InvalidArgument(_)
                    | AppError::NotFound(_)
                    | AppError::AlreadyExists(_)
                    | AppError::CasFailed(..)),
                ) => ExecuteResult::Fatal(err.to_string()),
                Err(err) => ExecuteResult::Unknown(err.to_string()),
            }
        })
    }

    fn observe<'a>(
        &'a self,
        target: &'a VerificationTarget,
    ) -> BoxFuture<'a, OperationResult<Observation>> {
        Box::pin(self.observe_inner(target))
    }
}

fn add_mutations(txn: &mut Txn, mutations: &[Mutation]) {
    for mutation in mutations {
        match mutation {
            Mutation::Put(value) => txn.put(
                value.key.table,
                WriteBuilder::new(value.key.key.clone()).ensure_put(value.value.clone()),
            ),
            Mutation::Delete(key) => {
                txn.delete(key.table, WriteBuilder::new(key.key.clone()).ensure_delete())
            }
        }
    }
}
