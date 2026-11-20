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

use std::ops::Range as IndexRange;
use std::time::Instant;

use rand::SeedableRng;
use rand::rngs::StdRng;
use sekas_api::server::v1::PutRequest;
use sekas_client::{AppError, AppResult, Database, Range, RangeRequest, WriteBuilder};
use tokio_stream::StreamExt;

use super::report::{Metrics, Outcome};
use super::workload::{Request, Workload};

pub(super) async fn run(
    db: &Database,
    table: u64,
    workload: &Workload,
    metrics: &Metrics,
    indexes: IndexRange<u64>,
    seed: u64,
) {
    let mut rng = StdRng::seed_from_u64(seed);
    for index in indexes {
        let operation = workload.next(index, &mut rng);
        let start = Instant::now();
        let outcome = match execute(db, table, operation.request).await {
            Ok(()) => Outcome::Success,
            Err(AppError::CasFailed(..)) if operation.kind.is_conditional() => Outcome::Rejected,
            Err(error) => {
                tracing::debug!(operation = operation.kind.name(), %error, "benchmark operation failed");
                Outcome::Error
            }
        };
        metrics.record(operation.kind, outcome, start.elapsed());
    }
}

async fn execute(db: &Database, table: u64, request: Request) -> AppResult<()> {
    match request {
        Request::Read(key) => {
            db.get(table, key).await?;
        }
        Request::Write(puts) => write(db, table, puts).await?,
        Request::Cas { key, value } => {
            let current = db
                .get_raw_value(table, key.clone())
                .await?
                .filter(|current| current.content.is_some())
                .ok_or_else(|| AppError::NotFound("CAS fixture key".into()))?;
            let put = WriteBuilder::new(key).expect_version(current.version).put(value)?;
            write(db, table, [put]).await?;
        }
        Request::Scan { begin, end, limit } => {
            let mut stream = db
                .range(RangeRequest {
                    table_id: table,
                    range: Range::Range { begin: Some(begin), end: Some(end) },
                    limit,
                    total_limit: limit,
                    limit_bytes: 1024 * 1024,
                    ..Default::default()
                })
                .await?;
            while let Some(batch) = stream.next().await {
                batch?;
            }
        }
    }
    Ok(())
}

async fn write(
    db: &Database,
    table: u64,
    puts: impl IntoIterator<Item = PutRequest>,
) -> AppResult<()> {
    let mut txn = db.begin_txn();
    for put in puts {
        txn.put(table, put);
    }
    txn.commit().await?;
    Ok(())
}
