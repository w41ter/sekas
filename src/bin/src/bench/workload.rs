// Copyright 2026-present The Sekas Authors.
// Licensed under the Apache License, Version 2.0.

use std::sync::atomic::{AtomicU64, Ordering};

use anyhow::{Context, Result, ensure};
use rand::distr::weighted::WeightedIndex;
use rand::prelude::*;
use sekas_api::server::v1::PutRequest;
use sekas_client::WriteBuilder;

use super::config::{KeyDistribution, Phase, WorkloadConfig};

#[derive(Clone, Copy)]
pub(super) enum OperationKind {
    Load,
    Read,
    Insert,
    Update,
    Scan,
    Cas,
    InsertIfAbsent,
    Increment,
}

impl OperationKind {
    pub const ALL: [Self; 8] = [
        Self::Load,
        Self::Read,
        Self::Insert,
        Self::Update,
        Self::Scan,
        Self::Cas,
        Self::InsertIfAbsent,
        Self::Increment,
    ];

    pub fn name(self) -> &'static str {
        match self {
            Self::Load => "LOAD",
            Self::Read => "READ",
            Self::Insert => "INSERT",
            Self::Update => "UPDATE",
            Self::Scan => "SCAN",
            Self::Cas => "CAS",
            Self::InsertIfAbsent => "INSERT_IF_ABSENT",
            Self::Increment => "INCREMENT",
        }
    }

    pub fn is_conditional(self) -> bool {
        matches!(self, Self::Cas | Self::InsertIfAbsent)
    }
}

pub(super) struct Operation {
    pub kind: OperationKind,
    pub request: Request,
}

pub(super) enum Request {
    Read(Vec<u8>),
    Write(Vec<PutRequest>),
    Scan { begin: Vec<u8>, end: Vec<u8>, limit: u64 },
    Cas { key: Vec<u8>, value: Vec<u8> },
}

pub(super) struct Workload {
    pub operations: u64,
    records: u64,
    txn_keys: u64,
    value_size: usize,
    run: Option<Run>,
}

struct Run {
    mix: WeightedIndex<f64>,
    records: KeySampler,
    shared_keys: KeySampler,
    next_insert: AtomicU64,
    scan_length: u64,
}

impl Workload {
    pub fn new(config: &WorkloadConfig, phase: Phase, operations: u64) -> Result<Self> {
        let WorkloadConfig { records, txn_keys, value_size, .. } = *config;
        ensure!(txn_keys > 0, "workload.txn_keys must be positive");
        ensure!(
            value_size <= sekas_api::MAX_VALUE_SIZE,
            "workload.value_size must be at most {} bytes",
            sekas_api::MAX_VALUE_SIZE
        );

        let (operations, run) = match phase {
            Phase::Load => {
                validate_batch(txn_keys.min(records), value_size)?;
                (records.div_ceil(txn_keys), None)
            }
            Phase::Run => {
                ensure!(operations > 0, "operations must be positive");
                (operations, Some(Run::new(config, operations)?))
            }
        };
        Ok(Self { operations, records, txn_keys, value_size, run })
    }

    pub fn next(&self, index: u64, rng: &mut StdRng) -> Operation {
        use OperationKind::*;

        let Some(run) = &self.run else {
            let start = index * self.txn_keys;
            let count = self.txn_keys.min(self.records - start);
            let request = self.write(start..start + count, rng);
            return Operation { kind: Load, request };
        };

        let kind = OperationKind::ALL[rng.sample(&run.mix)];
        let request = match kind {
            Read => Request::Read(key(run.records.sample(rng))),
            Insert => {
                let start = run.next_insert.fetch_add(self.txn_keys, Ordering::Relaxed);
                self.write(start..start + self.txn_keys, rng)
            }
            Update => {
                let start = run.records.sample(rng);
                let first = self.txn_keys.min(self.records - start);
                let indexes = (start..start + first).chain(0..self.txn_keys - first);
                self.write(indexes, rng)
            }
            Scan => Request::Scan {
                begin: key(run.records.sample(rng)),
                end: key(self.records),
                limit: run.scan_length,
            },
            Cas => Request::Cas { key: key(run.records.sample(rng)), value: self.value(rng) },
            InsertIfAbsent => {
                let key = shared_key(b'i', run.shared_keys.sample(rng));
                let put = WriteBuilder::new(key).expect_not_exists().ensure_put(self.value(rng));
                Request::Write(vec![put])
            }
            Increment => {
                let key = shared_key(b'c', run.shared_keys.sample(rng));
                Request::Write(vec![WriteBuilder::new(key).ensure_add(1)])
            }
            Load => unreachable!("load is a separate phase"),
        };
        Operation { kind, request }
    }

    fn write(&self, indexes: impl Iterator<Item = u64>, rng: &mut StdRng) -> Request {
        let puts = indexes
            .map(|index| WriteBuilder::new(key(index)).ensure_put(self.value(rng)))
            .collect();
        Request::Write(puts)
    }

    fn value(&self, rng: &mut StdRng) -> Vec<u8> {
        let mut value = vec![0; self.value_size];
        rng.fill(value.as_mut_slice());
        value
    }
}

impl Run {
    fn new(config: &WorkloadConfig, operations: u64) -> Result<Self> {
        let WorkloadConfig { records, value_size, txn_keys, scan_length, ref mix, .. } = *config;
        let weights = OperationKind::ALL.map(|kind| match kind {
            OperationKind::Load => 0.0,
            OperationKind::Read => mix.read,
            OperationKind::Insert => mix.insert,
            OperationKind::Update => mix.update,
            OperationKind::Scan => mix.scan,
            OperationKind::Cas => mix.cas,
            OperationKind::InsertIfAbsent => mix.insert_if_absent,
            OperationKind::Increment => mix.increment,
        });
        let mix_index = WeightedIndex::new(weights).context("invalid workload.mix weights")?;
        if mix.read > 0.0 || mix.update > 0.0 || mix.scan > 0.0 || mix.cas > 0.0 {
            ensure!(
                records > 0,
                "reads, updates, scans and CAS require workload.records to be positive"
            );
        }
        if mix.insert_if_absent > 0.0 || mix.increment > 0.0 {
            ensure!(config.shared_keys > 0, "workload.shared_keys must be positive");
        }
        if mix.scan > 0.0 {
            ensure!(scan_length > 0, "workload.scan_length must be positive");
        }
        if mix.update > 0.0 {
            ensure!(
                txn_keys <= records,
                "workload.txn_keys must fit inside workload.records for updates"
            );
        }
        if mix.insert > 0.0 || mix.update > 0.0 {
            validate_batch(txn_keys, value_size)?;
        }
        let insert_start = config.insert_start.unwrap_or(records);
        if mix.insert > 0.0 {
            ensure!(
                insert_start >= records,
                "workload.insert_start must be at least workload.records"
            );
            operations
                .checked_mul(txn_keys)
                .and_then(|rows| insert_start.checked_add(rows))
                .context("insert keys would exceed the u64 key space")?;
        }
        let theta = match config.distribution {
            KeyDistribution::Uniform => None,
            KeyDistribution::Zipfian => {
                let theta = config.zipfian_theta;
                ensure!(
                    theta.is_finite() && theta > 0.0,
                    "workload.zipfian_theta must be finite and positive"
                );
                Some(theta)
            }
        };
        Ok(Self {
            mix: mix_index,
            records: KeySampler::new(records, theta),
            shared_keys: KeySampler::new(config.shared_keys, theta),
            next_insert: AtomicU64::new(insert_start),
            scan_length,
        })
    }
}

struct KeySampler {
    keys: u64,
    zipfian: Option<Zipfian>,
}

impl KeySampler {
    fn new(keys: u64, theta: Option<f64>) -> Self {
        let zipfian = theta.filter(|_| keys > 0).map(|theta| Zipfian::new(keys, theta));
        Self { keys, zipfian }
    }

    fn sample(&self, rng: &mut StdRng) -> u64 {
        match &self.zipfian {
            Some(zipfian) => zipfian.sample(self.keys, rng),
            None => rng.random_range(0..self.keys),
        }
    }
}

fn key(index: u64) -> Vec<u8> {
    index.to_be_bytes().to_vec()
}

fn shared_key(prefix: u8, index: u64) -> Vec<u8> {
    // Keys extending the highest regular key sort beyond every fixture scan.
    let mut key = Vec::with_capacity(2 * size_of::<u64>() + 1);
    key.extend_from_slice(&u64::MAX.to_be_bytes());
    key.push(prefix);
    key.extend_from_slice(&index.to_be_bytes());
    key
}

fn validate_batch(keys: u64, value_size: usize) -> Result<()> {
    let row_size = size_of::<u64>() + value_size;
    ensure!(
        keys <= (sekas_api::MAX_TXN_SIZE / row_size) as u64,
        "the transaction can exceed {} bytes; reduce workload.txn_keys or workload.value_size",
        sekas_api::MAX_TXN_SIZE
    );
    Ok(())
}

struct Zipfian {
    theta: f64,
    buckets: WeightedIndex<f64>,
}

impl Zipfian {
    fn new(records: u64, theta: f64) -> Self {
        // Each bucket uses its first rank's weight as an upper bound for all
        // ranks in the bucket. There are at most 64 buckets.
        let weights = (0..=records.ilog2()).map(|shift| {
            let first = 1_u64 << shift;
            let count = first.min(records - first + 1);
            count as f64 * (first as f64).powf(-theta)
        });
        Self { theta, buckets: WeightedIndex::new(weights).unwrap() }
    }

    fn sample(&self, records: u64, rng: &mut StdRng) -> u64 {
        loop {
            let first = 1_u64 << rng.sample(&self.buckets);
            let last = first + (first - 1).min(records - first);
            let rank = rng.random_range(first..=last);
            if rank == first || rng.random_bool((first as f64 / rank as f64).powf(self.theta)) {
                return rank - 1;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use sekas_api::server::v1::{PutType, WriteConditionType};

    use super::super::config::Mix;
    use super::*;

    #[test]
    fn zipfian_sampling_weights_individual_ranks() {
        let mut rng = StdRng::seed_from_u64(17);
        assert_eq!(Zipfian::new(1, 0.99).sample(1, &mut rng), 0);
        let sampler = Zipfian::new(7, 1.0);
        let mut counts = [0; 7];
        for _ in 0..50_000 {
            counts[sampler.sample(7, &mut rng) as usize] += 1;
        }
        for (index, count) in counts.into_iter().enumerate().skip(1) {
            let ratio = f64::from(counts[0]) / f64::from(count);
            assert!((ratio - (index + 1) as f64).abs() < 0.4, "{counts:?}");
        }
    }

    #[test]
    fn load_covers_the_initial_records_including_a_partial_batch() {
        let config = WorkloadConfig { records: 5, txn_keys: 2, ..Default::default() };
        let workload = Workload::new(&config, Phase::Load, 0).unwrap();
        let mut rng = StdRng::seed_from_u64(1);
        let expected_batches = [0..2, 2..4, 4..5];
        assert_eq!(workload.operations, expected_batches.len() as u64);
        for (index, expected) in expected_batches.into_iter().enumerate() {
            let Request::Write(puts) = workload.next(index as u64, &mut rng).request else {
                panic!("expected a load transaction");
            };
            assert!(puts.into_iter().map(|put| put.key).eq(expected.map(key)));
        }
    }

    #[test]
    fn scan_bounds_exclude_insert_keys_across_byte_boundaries() {
        let config = WorkloadConfig {
            records: 255,
            mix: Mix { scan: 1.0, ..Default::default() },
            ..Default::default()
        };
        let workload = Workload::new(&config, Phase::Run, 1).unwrap();
        let mut rng = StdRng::seed_from_u64(1);
        let Request::Scan { begin, end, limit } = workload.next(0, &mut rng).request else {
            panic!("expected a scan");
        };
        assert!(begin < end && end < key(256));
        assert_eq!(end, key(config.records));
        assert_eq!(limit, config.scan_length);
    }

    #[test]
    fn create_once_keys_and_counters_use_separate_ranges() {
        let mut config = WorkloadConfig {
            records: 0,
            shared_keys: 1,
            mix: Mix { insert_if_absent: 1.0, ..Default::default() },
            ..Default::default()
        };
        let mut rng = StdRng::seed_from_u64(1);
        let insert = Workload::new(&config, Phase::Run, 2).unwrap();
        let Request::Write(puts) = insert.next(0, &mut rng).request else {
            panic!("expected an insert-if-absent write");
        };
        let [create] = puts.try_into().unwrap();
        assert_eq!(create.conditions.len(), 1);
        assert_eq!(create.conditions[0].r#type(), WriteConditionType::ExpectNotExists);
        let Request::Write(repeated) = insert.next(1, &mut rng).request else {
            panic!("expected a repeated insert-if-absent write");
        };
        assert_eq!(repeated[0].key, create.key);

        config.mix = Mix { increment: 1.0, ..Default::default() };
        let counter = Workload::new(&config, Phase::Run, 1).unwrap();
        let Request::Write(puts) = counter.next(0, &mut rng).request else {
            panic!("expected an atomic increment");
        };
        let [increment] = puts.try_into().unwrap();
        assert!(increment.conditions.is_empty());
        assert_eq!(increment.put_type(), PutType::AddI64);
        assert_eq!(increment.value, 1_i64.to_be_bytes());
        assert_ne!(increment.key, create.key);
        assert!(create.key > key(u64::MAX));
        assert!(increment.key > key(u64::MAX));
    }

    #[test]
    fn writes_allocate_unique_inserts_and_distinct_update_keys() {
        let config = WorkloadConfig {
            records: 3,
            txn_keys: 3,
            insert_start: Some(10),
            distribution: KeyDistribution::Zipfian,
            mix: Mix { insert: 1.0, update: 1.0, ..Default::default() },
            ..Default::default()
        };
        let workload = Workload::new(&config, Phase::Run, 128).unwrap();
        let mut rngs = [StdRng::seed_from_u64(1), StdRng::seed_from_u64(2)];
        let expected_updates: BTreeSet<_> = (0..3).map(key).collect();
        let mut next_insert = 10;
        let mut updates = 0;
        for index in 0..128 {
            let Operation { kind, request: Request::Write(puts) } =
                workload.next(index as u64, &mut rngs[index % 2])
            else {
                panic!("expected a write transaction");
            };
            assert!(puts.iter().all(|put| put.conditions.is_empty()));
            let keys = puts.into_iter().map(|put| put.key).collect::<BTreeSet<_>>();
            match kind {
                OperationKind::Insert => {
                    assert_eq!(keys, (next_insert..next_insert + 3).map(key).collect());
                    next_insert += 3;
                }
                OperationKind::Update => {
                    assert_eq!(keys, expected_updates);
                    updates += 1;
                }
                _ => panic!("unexpected operation"),
            }
        }
        assert!(next_insert > 10 && updates > 0);
    }
}
