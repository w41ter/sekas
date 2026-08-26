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

use crate::workload::{LogicalKey, Observation, OperationPlan, WriterId};

const VALUE_MAGIC: &[u8; 4] = b"SKCH";
const VALUE_HEADER_LEN: usize = 4 + 4 + 8 + 4;
const CHECKSUM_LEN: usize = 4;

/// The expected stable state owned by one writer.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WriterModel {
    writer: WriterId,
    completed: u64,
    slots: Vec<LogicalKey>,
    values: BTreeMap<LogicalKey, Option<Vec<u8>>>,
}

impl WriterModel {
    pub fn new(run_id: &str, writer: WriterId, tables: &[u64], slot_count: usize) -> Self {
        assert!(!tables.is_empty(), "at least one table is required");
        assert!(slot_count > 0, "at least one slot is required");

        let slots = (0..slot_count)
            .map(|slot| {
                let table = tables[slot % tables.len()];
                LogicalKey::new(table, make_key(run_id, writer, slot))
            })
            .collect::<Vec<_>>();
        let values = slots.iter().cloned().map(|key| (key, None)).collect();
        Self { writer, completed: 0, slots, values }
    }

    pub fn writer(&self) -> WriterId {
        self.writer
    }

    pub fn completed(&self) -> u64 {
        self.completed
    }

    pub fn slot_count(&self) -> usize {
        self.slots.len()
    }

    pub fn slot(&self, index: usize) -> &LogicalKey {
        &self.slots[index]
    }

    pub fn value(&self, key: &LogicalKey) -> Option<&Vec<u8>> {
        self.values.get(key).and_then(Option::as_ref)
    }

    pub fn observe_keys(&self, keys: impl IntoIterator<Item = LogicalKey>) -> Observation {
        Observation {
            values: keys
                .into_iter()
                .map(|key| {
                    let value = self.values.get(&key).cloned().unwrap_or_default();
                    (key, value)
                })
                .collect(),
        }
    }

    pub fn range(&self, start: usize, len: usize) -> Observation {
        let end = start.saturating_add(len).min(self.slots.len());
        self.observe_keys(self.slots[start..end].iter().cloned())
    }

    pub fn all(&self) -> Observation {
        Observation { values: self.values.clone() }
    }

    /// Applies a reconciled plan to the model.
    pub fn apply(&mut self, plan: &OperationPlan) -> Result<(), String> {
        if plan.id.writer != self.writer || plan.id.sequence != self.completed {
            return Err(format!(
                "operation {:?} does not follow writer {:?} progress {}",
                plan.id, self.writer, self.completed
            ));
        }
        let current = self.observe_keys(plan.before.values.keys().cloned());
        if current != plan.before {
            return Err(format!("operation {:?} pre-state does not match writer model", plan.id));
        }
        for (key, value) in &plan.after.values {
            let Some(slot) = self.values.get_mut(key) else {
                return Err(format!("operation {:?} targets an unowned key", plan.id));
            };
            *slot = value.clone();
        }
        self.completed += 1;
        Ok(())
    }
}

pub fn make_key(run_id: &str, writer: WriterId, slot: usize) -> Vec<u8> {
    format!("chaos/{run_id}/writer/{}/slot/{slot:08}", writer.0).into_bytes()
}

/// Encodes a self-describing value that detects stale, foreign, and corrupted
/// data without relying on human-readable payloads.
pub fn encode_value(writer: WriterId, sequence: u64, slot: usize, payload: &[u8]) -> Vec<u8> {
    let mut value = Vec::with_capacity(VALUE_HEADER_LEN + payload.len() + CHECKSUM_LEN);
    value.extend_from_slice(VALUE_MAGIC);
    value.extend_from_slice(&writer.0.to_be_bytes());
    value.extend_from_slice(&sequence.to_be_bytes());
    value.extend_from_slice(&(slot as u32).to_be_bytes());
    value.extend_from_slice(payload);
    let checksum = crc32fast::hash(&value);
    value.extend_from_slice(&checksum.to_be_bytes());
    value
}

pub fn verify_value(value: &[u8]) -> bool {
    if value.len() < VALUE_HEADER_LEN + CHECKSUM_LEN || &value[..4] != VALUE_MAGIC {
        return false;
    }
    let body_len = value.len() - CHECKSUM_LEN;
    let expected = u32::from_be_bytes(value[body_len..].try_into().unwrap());
    crc32fast::hash(&value[..body_len]) == expected
}

#[cfg(test)]
mod tests {
    use super::{WriterModel, encode_value, verify_value};
    use crate::workload::WriterId;

    #[test]
    fn generated_keys_are_disjoint() {
        let first = WriterModel::new("run", WriterId(1), &[10, 11], 4);
        let second = WriterModel::new("run", WriterId(2), &[10, 11], 4);
        assert_ne!(first.slot(0), second.slot(0));
        assert_eq!(first.slot(0).table, 10);
        assert_eq!(first.slot(1).table, 11);
    }

    #[test]
    fn values_detect_corruption() {
        let mut value = encode_value(WriterId(3), 17, 9, b"payload");
        assert!(verify_value(&value));
        value[8] ^= 1;
        assert!(!verify_value(&value));
    }
}
