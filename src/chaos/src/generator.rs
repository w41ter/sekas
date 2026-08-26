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
use std::time::Duration;

use rand::prelude::SmallRng;
use rand::seq::index;
use rand::{Rng, SeedableRng};
use serde::{Deserialize, Serialize};

use crate::WriterModel;
use crate::model::encode_value;
use crate::workload::{
    KeyValue, Mutation, Observation, Operation, OperationGenerator, OperationId, OperationPlan,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct SizeRange {
    pub min: usize,
    pub max: usize,
}

impl SizeRange {
    pub const fn new(min: usize, max: usize) -> Self {
        Self { min, max }
    }

    fn choose(self, rng: &mut SmallRng) -> usize {
        if self.min == self.max { self.min } else { rng.gen_range(self.min..=self.max) }
    }

    fn validate(self, name: &str, upper: Option<usize>) -> Result<(), String> {
        if self.min == 0 || self.min > self.max {
            return Err(format!("{name} must satisfy 0 < min <= max"));
        }
        if let Some(upper) = upper
            && self.max > upper
        {
            return Err(format!("{name}.max {} exceeds {upper}", self.max));
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct OperationWeights {
    pub put: u32,
    pub delete: u32,
    pub batch: u32,
    pub transaction: u32,
    pub add_i64: u32,
}

impl OperationWeights {
    fn total(self) -> u32 {
        self.put + self.delete + self.batch + self.transaction + self.add_i64
    }
}

impl Default for OperationWeights {
    fn default() -> Self {
        Self { put: 45, delete: 15, batch: 20, transaction: 20, add_i64: 0 }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct WorkloadConfig {
    pub run_id: String,
    pub tables: Vec<u64>,
    pub seed: u64,
    pub writers: usize,
    pub readers: usize,
    pub slots_per_writer: usize,
    pub operations_per_writer: Option<u64>,
    pub value_size: SizeRange,
    pub verify_width: SizeRange,
    pub batch_width: SizeRange,
    pub transaction_width: SizeRange,
    pub operation_weights: OperationWeights,
    pub retry_initial_backoff: Duration,
    pub retry_max_backoff: Duration,
    pub request_timeout: Duration,
    pub reconciliation_timeout: Duration,
    pub reader_pause: Duration,
    pub recent_operations: usize,
    pub max_recorded_events: usize,
}

impl WorkloadConfig {
    pub fn validate(&self) -> Result<(), String> {
        if self.run_id.is_empty() {
            return Err("run_id must not be empty".to_string());
        }
        if self.tables.is_empty() {
            return Err("at least one table is required".to_string());
        }
        if self.writers == 0 {
            return Err("at least one writer is required".to_string());
        }
        if self.slots_per_writer == 0 {
            return Err("at least one slot per writer is required".to_string());
        }
        if self.operation_weights.total() == 0 {
            return Err("at least one operation weight must be non-zero".to_string());
        }
        if self.operation_weights.add_i64 > 0 && self.slots_per_writer < 2 {
            return Err(
                "atomic add requires one counter slot and at least one data slot".to_string()
            );
        }
        self.value_size.validate("value_size", None)?;
        self.verify_width.validate("verify_width", Some(self.slots_per_writer))?;
        let data_slots = self.data_slots();
        self.batch_width.validate("batch_width", Some(data_slots))?;
        self.transaction_width.validate("transaction_width", Some(data_slots))?;
        if self.retry_initial_backoff > self.retry_max_backoff {
            return Err("retry_initial_backoff must not exceed retry_max_backoff".to_string());
        }
        if self.request_timeout.is_zero() {
            return Err("request_timeout must be non-zero".to_string());
        }
        if self.reconciliation_timeout.is_zero() {
            return Err("reconciliation_timeout must be non-zero".to_string());
        }
        if self.max_recorded_events == 0 {
            return Err("max_recorded_events must be non-zero".to_string());
        }
        Ok(())
    }

    pub fn data_slots(&self) -> usize {
        self.slots_per_writer - usize::from(self.operation_weights.add_i64 > 0)
    }
}

impl Default for WorkloadConfig {
    fn default() -> Self {
        Self {
            run_id: "default".to_string(),
            tables: Vec::new(),
            seed: 0x5eca5,
            writers: 4,
            readers: 4,
            slots_per_writer: 1024,
            operations_per_writer: None,
            value_size: SizeRange::new(16, 256),
            verify_width: SizeRange::new(1, 16),
            batch_width: SizeRange::new(2, 8),
            transaction_width: SizeRange::new(2, 8),
            operation_weights: OperationWeights::default(),
            retry_initial_backoff: Duration::from_millis(5),
            retry_max_backoff: Duration::from_secs(1),
            request_timeout: Duration::from_secs(5),
            reconciliation_timeout: Duration::from_secs(30),
            reader_pause: Duration::from_millis(1),
            recent_operations: 128,
            max_recorded_events: 100_000,
        }
    }
}

#[derive(Clone, Debug)]
pub struct DeterministicGenerator {
    config: WorkloadConfig,
}

impl DeterministicGenerator {
    pub fn new(config: WorkloadConfig) -> Self {
        Self { config }
    }

    fn rng(&self, seed: u64, id: OperationId) -> SmallRng {
        SmallRng::seed_from_u64(mix_seed(seed, id.writer.0 as u64, id.sequence))
    }

    fn select_slots(&self, rng: &mut SmallRng, count: usize, available: usize) -> Vec<usize> {
        index::sample(rng, available, count.min(available)).into_vec()
    }

    fn mutation(
        &self,
        rng: &mut SmallRng,
        id: OperationId,
        model: &WriterModel,
        slot: usize,
    ) -> Mutation {
        let key = model.slot(slot).clone();
        if rng.gen_ratio(1, 4) {
            Mutation::Delete(key)
        } else {
            Mutation::Put(KeyValue { key, value: self.value(rng, id, slot) })
        }
    }

    fn value(&self, rng: &mut SmallRng, id: OperationId, slot: usize) -> Vec<u8> {
        let size = self.config.value_size.choose(rng);
        let mut payload = vec![0; size];
        rng.fill(payload.as_mut_slice());
        encode_value(id.writer, id.sequence, slot, &payload)
    }

    fn plan_for_mutations(
        &self,
        id: OperationId,
        model: &WriterModel,
        operation: impl FnOnce(Vec<Mutation>) -> Operation,
        mutations: Vec<Mutation>,
    ) -> OperationPlan {
        let keys = mutations.iter().map(|mutation| mutation.key().clone()).collect::<Vec<_>>();
        let before = model.observe_keys(keys);
        let mut after = before.clone();
        for mutation in &mutations {
            after.values.insert(mutation.key().clone(), mutation.value());
        }
        OperationPlan { id, operation: operation(mutations), before, after }
    }
}

impl OperationGenerator for DeterministicGenerator {
    fn generate(&self, seed: u64, id: OperationId, model: &WriterModel) -> OperationPlan {
        debug_assert_eq!(id.writer, model.writer());
        debug_assert_eq!(id.sequence, model.completed());
        let mut rng = self.rng(seed, id);
        let selected = rng.gen_range(0..self.config.operation_weights.total());
        let weights = self.config.operation_weights;
        let data_slots = self.config.data_slots();

        if selected < weights.put {
            let slot = rng.gen_range(0..data_slots);
            let key = model.slot(slot).clone();
            let value = self.value(&mut rng, id, slot);
            let before = model.observe_keys([key.clone()]);
            let after =
                Observation { values: BTreeMap::from([(key.clone(), Some(value.clone()))]) };
            OperationPlan { id, operation: Operation::Put(KeyValue { key, value }), before, after }
        } else if selected < weights.put + weights.delete {
            let key = model.slot(rng.gen_range(0..data_slots)).clone();
            let before = model.observe_keys([key.clone()]);
            let after = Observation { values: BTreeMap::from([(key.clone(), None)]) };
            OperationPlan { id, operation: Operation::Delete(key), before, after }
        } else if selected < weights.put + weights.delete + weights.batch {
            let width = self.config.batch_width.choose(&mut rng);
            let mutations = self
                .select_slots(&mut rng, width, data_slots)
                .into_iter()
                .map(|slot| self.mutation(&mut rng, id, model, slot))
                .collect();
            self.plan_for_mutations(id, model, Operation::Batch, mutations)
        } else if selected < weights.put + weights.delete + weights.batch + weights.transaction {
            let width = self.config.transaction_width.choose(&mut rng);
            let mutations = self
                .select_slots(&mut rng, width, data_slots)
                .into_iter()
                .map(|slot| self.mutation(&mut rng, id, model, slot))
                .collect();
            self.plan_for_mutations(id, model, Operation::Transaction, mutations)
        } else {
            let slot = self.config.slots_per_writer - 1;
            let key = model.slot(slot).clone();
            let before_value = model
                .value(&key)
                .and_then(|value| value.as_slice().try_into().ok())
                .map(i64::from_be_bytes)
                .unwrap_or_default();
            let delta = if rng.gen_ratio(1, 2) { 1 } else { -1 };
            let after_value = before_value.saturating_add(delta).to_be_bytes().to_vec();
            let before = model.observe_keys([key.clone()]);
            let after = Observation { values: BTreeMap::from([(key.clone(), Some(after_value))]) };
            OperationPlan { id, operation: Operation::AddI64 { key, delta }, before, after }
        }
    }
}

fn mix_seed(seed: u64, component: u64, sequence: u64) -> u64 {
    let mut value = seed ^ component.wrapping_mul(0x9e37_79b9_7f4a_7c15) ^ sequence.rotate_left(31);
    value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}

#[cfg(test)]
mod tests {
    use super::{DeterministicGenerator, OperationWeights, WorkloadConfig};
    use crate::model::WriterModel;
    use crate::workload::{OperationGenerator, OperationId, WriterId};

    #[test]
    fn same_model_and_id_generate_same_plan() {
        let config = WorkloadConfig {
            run_id: "deterministic".to_string(),
            tables: vec![10, 11],
            operation_weights: OperationWeights {
                put: 1,
                delete: 1,
                batch: 1,
                transaction: 1,
                add_i64: 0,
            },
            ..WorkloadConfig::default()
        };
        let generator = DeterministicGenerator::new(config.clone());
        let model =
            WriterModel::new(&config.run_id, WriterId(3), &config.tables, config.slots_per_writer);
        let id = OperationId { writer: WriterId(3), sequence: 0 };
        assert_eq!(
            generator.generate(config.seed, id, &model),
            generator.generate(config.seed, id, &model)
        );
    }
}
