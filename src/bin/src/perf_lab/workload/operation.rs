// Copyright 2026-present The Sekas Authors.
// Licensed under the Apache License, Version 2.0.

use std::time::Duration;

use rand::Rng;
use rand::rngs::SmallRng;
use sekas_api::server::v1::ShardGetRequest;
use sekas_api::server::v1::group_request_union::Request;
use sekas_api::server::v1::group_response_union::Response;
use sekas_client::{
    AppError, AppResult, Database, GroupClient, Range, RangeRequest, SekasClient, WriteBuilder,
};
use tokio_stream::StreamExt;

use super::{Operation, Outcome, Target, TxnMode, Workload};

fn invalid(message: &str) -> AppError {
    AppError::InvalidArgument(message.to_owned())
}
fn check_value(
    value: Option<Vec<u8>>,
    present: bool,
    expected: Option<&Vec<u8>>,
) -> AppResult<u64> {
    if value.is_some() != present {
        return Err(invalid("fixture visibility mismatch"));
    }
    if let Some(expected) = expected
        && value.as_ref() != Some(expected)
    {
        return Err(invalid("snapshot value mismatch"));
    }
    Ok(value.map_or(0, |v| v.len() as u64))
}

pub(super) async fn execute(
    db: &Database,
    client: &SekasClient,
    spec: &Workload,
    rng: &mut SmallRng,
    worker: usize,
    sequence: u64,
    out: &mut Outcome,
) -> AppResult<()> {
    out.attempts = 1;
    let target = &spec.targets[rng.gen_range(0..spec.targets.len())];
    let space = if rng.gen_bool(spec.hot_probability) {
        spec.hot_keys.min(target.keys)
    } else {
        target.keys
    };
    let index = rng.gen_range(0..space);
    match &spec.operation {
        Operation::Read { present, version, expected } => {
            let key = target.key(index);
            let value = if let Some(version) = version {
                let (group, shard) = spec.router.find_shard(target.table, &key)?;
                let mut group = GroupClient::new(group, client.clone());
                group.set_timeout(spec.timeout);
                match group
                    .request(&Request::Get(ShardGetRequest {
                        shard_id: shard.id,
                        start_version: *version,
                        user_key: key,
                    }))
                    .await?
                {
                    Response::Get(response) => response.value.and_then(|v| v.content),
                    _ => return Err(invalid("unexpected get response")),
                }
            } else {
                db.get(target.table, key).await?
            };
            out.bytes = check_value(value, *present, expected.as_ref())?;
            out.rows = u64::from(*present);
        }
        Operation::Put | Operation::Insert => {
            let key = if matches!(spec.operation, Operation::Insert) {
                format!("{}insert-{worker:08}-{sequence:020}", target.prefix).into_bytes()
            } else {
                target.key(index)
            };
            let value = payload(rng, spec.value_size);
            db.put(target.table, key, value).await?;
            out.bytes = spec.value_size as u64;
            out.rows = 1;
        }
        Operation::Churn => {
            let key = target.key(index);
            if rng.gen_bool(0.25) {
                db.delete(target.table, key).await?;
            } else {
                db.put(target.table, key, payload(rng, spec.value_size)).await?;
                out.bytes = spec.value_size as u64;
            }
            out.rows = 1;
        }
        Operation::Delete => {
            let key = target.key(worker as u64 + sequence * spec.concurrency as u64);
            db.delete(target.table, key).await?;
            out.rows = 1;
        }
        Operation::Scan { limit, version, expected_rows, expected } => {
            let count = (*limit).min(target.keys);
            let begin = if version.is_some() || *limit >= target.keys {
                0
            } else {
                rng.gen_range(0..=target.keys - count)
            };
            let mut stream = db
                .range(RangeRequest {
                    table_id: target.table,
                    version: *version,
                    range: Range::Range {
                        begin: Some(target.key(begin)),
                        end: Some(target.key(target.keys)),
                    },
                    limit: *limit,
                    ..Default::default()
                })
                .await?;
            while let Some(batch) = stream.next().await {
                for row in batch? {
                    let value = row.values.first().and_then(|v| v.content.clone());
                    out.bytes += check_value(value, true, expected.as_ref())?;
                    out.rows += 1;
                }
            }
            if let Some(rows) = expected_rows
                && out.rows != *rows
            {
                return Err(invalid("scan row count mismatch"));
            }
        }
        Operation::Transaction { .. } => {
            execute_transaction(db, spec, rng, worker, target, out).await?;
        }
        Operation::Metadata => {
            db.list_table().await?;
        }
    }
    Ok(())
}

async fn execute_transaction(
    db: &Database,
    spec: &Workload,
    rng: &mut SmallRng,
    worker: usize,
    target: &Target,
    out: &mut Outcome,
) -> AppResult<()> {
    let Operation::Transaction { mode, keys } = &spec.operation else {
        unreachable!("transaction executor requires a transaction operation");
    };
    // Consecutive distinct keys; each participating table/group gets at least one.
    let keys = (*keys).max(spec.targets.len());
    let offset = if spec.disjoint_keys {
        let partition = target.keys / spec.concurrency as u64;
        let needed = keys.div_ceil(spec.targets.len()) as u64;
        worker as u64 * partition + rng.gen_range(0..=partition - needed)
    } else {
        rng.gen_range(0..target.keys)
    };
    let writes = transaction_keys(&spec.targets, keys, offset);
    let value = (!matches!(mode, TxnMode::ReadOnly)).then(|| payload(rng, spec.value_size));
    loop {
        let mut txn = db.begin_txn();
        if !matches!(mode, TxnMode::Blind) {
            for (table, key) in &writes {
                out.bytes += check_value(txn.get(*table, key.clone()).await?, true, None)?;
            }
            tokio::time::sleep(spec.hold).await;
        }
        if let Some(value) = &value {
            for (table, key) in &writes {
                txn.put(*table, WriteBuilder::new(key.clone()).ensure_put(value.clone()));
            }
            match txn.commit().await {
                Err(AppError::TxnConflict) => {
                    out.conflicts += 1;
                    if out.conflicts as usize > spec.retries {
                        return Err(AppError::TxnConflict);
                    }
                    out.attempts += 1;
                    // Deterministic stagger avoids synchronized retry storms.
                    tokio::time::sleep(Duration::from_micros(rng.gen_range(100..2000))).await;
                    continue;
                }
                result => {
                    result?;
                }
            }
            out.bytes += (keys * spec.value_size) as u64;
        }
        out.rows = keys as u64;
        break;
    }
    Ok(())
}

fn transaction_keys(targets: &[Target], keys: usize, offset: u64) -> Vec<(u64, Vec<u8>)> {
    (0..keys)
        .map(|i| {
            let target = &targets[i % targets.len()];
            (target.table, target.key((offset + (i / targets.len()) as u64) % target.keys))
        })
        .collect()
}
fn payload(rng: &mut SmallRng, size: usize) -> Vec<u8> {
    let mut value = vec![0; size];
    rng.fill(&mut value[..]);
    value
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn distinct_keys_cover_every_participant() {
        let targets = vec![
            Target { table: 1, prefix: "k".into(), keys: 4 },
            Target { table: 2, prefix: "k".into(), keys: 4 },
        ];
        let keys = transaction_keys(&targets, 8, 3);
        assert_eq!(keys.iter().collect::<std::collections::HashSet<_>>().len(), 8);
        assert_eq!(keys.iter().filter(|(t, _)| *t == 1).count(), 4);
    }
}
