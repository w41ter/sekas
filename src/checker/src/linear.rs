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

use std::collections::{BTreeMap, HashSet};

use serde::{Deserialize, Serialize};

use crate::history::{Call, CallResult, History, KvOp, KvValue};

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum CheckOutcome {
    Valid,
    Invalid,
    Unsupported(String),
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct KeyReport {
    pub key: Vec<u8>,
    pub outcome: CheckOutcome,
    pub calls: usize,
    pub linearization: Vec<u64>,
    pub reason: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct CheckReport {
    pub outcome: CheckOutcome,
    pub keys: Vec<KeyReport>,
}

impl CheckReport {
    pub fn is_valid(&self) -> bool {
        matches!(self.outcome, CheckOutcome::Valid)
    }
}

#[derive(Clone, Debug)]
pub struct LinearizabilityChecker {
    max_calls_per_key: usize,
    max_states_per_key: usize,
}

impl Default for LinearizabilityChecker {
    fn default() -> Self {
        LinearizabilityChecker { max_calls_per_key: 2048, max_states_per_key: 1_000_000 }
    }
}

impl LinearizabilityChecker {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_max_calls_per_key(mut self, max: usize) -> Self {
        self.max_calls_per_key = max;
        self
    }

    pub fn with_max_states_per_key(mut self, max: usize) -> Self {
        self.max_states_per_key = max;
        self
    }

    pub fn check(&self, history: &History) -> CheckReport {
        let mut by_key = BTreeMap::<Vec<u8>, Vec<Call>>::new();
        for call in &history.calls {
            if matches!(call.result, CallResult::Fail(_)) {
                continue;
            }
            if matches!(call.result, CallResult::Info(_)) && matches!(call.op, KvOp::Get { .. }) {
                continue;
            }
            by_key.entry(call.key().to_vec()).or_default().push(call.clone());
        }

        let mut reports = Vec::with_capacity(by_key.len());
        for (key, mut calls) in by_key {
            calls.sort_by_key(|call| (call.invoke_time, call.complete_time, call.id));
            reports.push(self.check_key(key, calls));
        }

        let outcome = if reports.iter().all(|r| matches!(r.outcome, CheckOutcome::Valid)) {
            CheckOutcome::Valid
        } else if reports.iter().any(|r| matches!(r.outcome, CheckOutcome::Invalid)) {
            CheckOutcome::Invalid
        } else {
            CheckOutcome::Unsupported("at least one key history is unsupported".to_string())
        };
        CheckReport { outcome, keys: reports }
    }

    fn check_key(&self, key: Vec<u8>, calls: Vec<Call>) -> KeyReport {
        if calls.len() > self.max_calls_per_key {
            return KeyReport {
                key,
                outcome: CheckOutcome::Unsupported(format!(
                    "key has {} calls, max supported is {}",
                    calls.len(),
                    self.max_calls_per_key
                )),
                calls: calls.len(),
                linearization: Vec::new(),
                reason: None,
            };
        }

        let mut predecessors = vec![CallSet::empty(calls.len()); calls.len()];
        for (idx, call) in calls.iter().enumerate() {
            for (other_idx, other) in calls.iter().enumerate() {
                if idx != other_idx && other.complete_time < call.invoke_time {
                    predecessors[idx].insert(other_idx);
                }
            }
        }

        let optional = calls.iter().enumerate().filter(|(_, call)| call.is_info()).fold(
            CallSet::empty(calls.len()),
            |mut mask, (idx, _)| {
                mask.insert(idx);
                mask
            },
        );
        let required = CallSet::full(calls.len()).without_all(&optional);

        let mut search = Search {
            calls: &calls,
            predecessors,
            required,
            optional,
            seen: HashSet::new(),
            path: Vec::with_capacity(calls.len()),
            linearization: Vec::new(),
            max_states: self.max_states_per_key,
            budget_exhausted: false,
        };

        if search.run(None, CallSet::empty(calls.len()), CallSet::empty(calls.len())) {
            KeyReport {
                key,
                outcome: CheckOutcome::Valid,
                calls: calls.len(),
                linearization: search.linearization,
                reason: None,
            }
        } else if search.budget_exhausted {
            KeyReport {
                key,
                outcome: CheckOutcome::Unsupported(format!(
                    "state budget exhausted after {} states",
                    self.max_states_per_key
                )),
                calls: calls.len(),
                linearization: Vec::new(),
                reason: None,
            }
        } else {
            KeyReport {
                key,
                outcome: CheckOutcome::Invalid,
                calls: calls.len(),
                linearization: Vec::new(),
                reason: Some("no legal linearization found".to_string()),
            }
        }
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct CallSet {
    bits: Vec<u64>,
}

impl CallSet {
    fn empty(len: usize) -> Self {
        CallSet { bits: vec![0; len.div_ceil(64)] }
    }

    fn full(len: usize) -> Self {
        let mut set = CallSet { bits: vec![u64::MAX; len.div_ceil(64)] };
        let extra = set.bits.len() * 64 - len;
        if extra > 0
            && let Some(last) = set.bits.last_mut()
        {
            *last &= u64::MAX >> extra;
        }
        set
    }

    fn insert(&mut self, idx: usize) {
        self.bits[idx / 64] |= 1u64 << (idx % 64);
    }

    fn contains(&self, idx: usize) -> bool {
        self.bits[idx / 64] & (1u64 << (idx % 64)) != 0
    }

    fn union(&self, other: &Self) -> Self {
        debug_assert_eq!(self.bits.len(), other.bits.len());
        CallSet {
            bits: self.bits.iter().zip(&other.bits).map(|(left, right)| left | right).collect(),
        }
    }

    fn without_all(&self, other: &Self) -> Self {
        debug_assert_eq!(self.bits.len(), other.bits.len());
        CallSet {
            bits: self.bits.iter().zip(&other.bits).map(|(left, right)| left & !right).collect(),
        }
    }

    fn is_subset_of(&self, other: &Self) -> bool {
        debug_assert_eq!(self.bits.len(), other.bits.len());
        self.bits.iter().zip(&other.bits).all(|(left, right)| left & !right == 0)
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct SearchState {
    value: Option<KvValue>,
    done: CallSet,
    skipped: CallSet,
}

struct Search<'a> {
    calls: &'a [Call],
    predecessors: Vec<CallSet>,
    required: CallSet,
    optional: CallSet,
    seen: HashSet<SearchState>,
    path: Vec<u64>,
    linearization: Vec<u64>,
    max_states: usize,
    budget_exhausted: bool,
}

impl<'a> Search<'a> {
    fn run(&mut self, value: Option<KvValue>, done: CallSet, skipped: CallSet) -> bool {
        if self.required.is_subset_of(&done) {
            self.linearization = self.path.clone();
            return true;
        }

        let state =
            SearchState { value: value.clone(), done: done.clone(), skipped: skipped.clone() };
        if !self.seen.insert(state) {
            return false;
        }
        if self.seen.len() > self.max_states {
            self.budget_exhausted = true;
            return false;
        }

        let resolved = done.union(&skipped);
        for idx in 0..self.calls.len() {
            if resolved.contains(idx) {
                continue;
            }
            if !self.predecessors[idx].is_subset_of(&resolved) {
                continue;
            }

            if self.optional.contains(idx) {
                let mut next_skipped = skipped.clone();
                next_skipped.insert(idx);
                if self.run(value.clone(), done.clone(), next_skipped) {
                    return true;
                }
            }

            if self.budget_exhausted {
                return false;
            }

            let Some(next_value) = apply(&value, &self.calls[idx]) else {
                continue;
            };
            self.path.push(self.calls[idx].id);
            let mut next_done = done.clone();
            next_done.insert(idx);
            if self.run(next_value, next_done, skipped.clone()) {
                return true;
            }
            self.path.pop();
        }

        false
    }
}

fn apply(value: &Option<KvValue>, call: &Call) -> Option<Option<KvValue>> {
    match (&call.op, &call.result) {
        (KvOp::Get { .. }, CallResult::Get(read_value)) => {
            if read_value == value {
                Some(value.clone())
            } else {
                None
            }
        }
        (KvOp::Put { value: next, .. }, CallResult::Put | CallResult::Info(_)) => {
            Some(Some(next.clone()))
        }
        (KvOp::Delete { .. }, CallResult::Delete | CallResult::Info(_)) => Some(None),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::{CheckOutcome, LinearizabilityChecker};
    use crate::history::{Call, CallResult, History, KvOp};

    fn call(id: u64, op: KvOp, result: CallResult, invoke: u64, complete: u64) -> Call {
        Call { id, process: id as usize, op, result, invoke_time: invoke, complete_time: complete }
    }

    #[test]
    fn accepts_sequential_register_history() {
        let key = b"k".to_vec();
        let history = History::from_calls(vec![
            call(1, KvOp::Put { key: key.clone(), value: b"v1".to_vec() }, CallResult::Put, 1, 2),
            call(2, KvOp::Get { key: key.clone() }, CallResult::Get(Some(b"v1".to_vec())), 3, 4),
            call(3, KvOp::Delete { key: key.clone() }, CallResult::Delete, 5, 6),
            call(4, KvOp::Get { key }, CallResult::Get(None), 7, 8),
        ]);

        let report = LinearizabilityChecker::new().check(&history);
        assert_eq!(report.outcome, CheckOutcome::Valid);
    }

    #[test]
    fn rejects_stale_read_after_completed_write() {
        let key = b"k".to_vec();
        let history = History::from_calls(vec![
            call(1, KvOp::Put { key: key.clone(), value: b"v1".to_vec() }, CallResult::Put, 1, 2),
            call(2, KvOp::Get { key }, CallResult::Get(None), 3, 4),
        ]);

        let report = LinearizabilityChecker::new().check(&history);
        assert_eq!(report.outcome, CheckOutcome::Invalid);
    }

    #[test]
    fn allows_overlapping_read_before_write_linearization() {
        let key = b"k".to_vec();
        let history = History::from_calls(vec![
            call(1, KvOp::Put { key: key.clone(), value: b"v1".to_vec() }, CallResult::Put, 1, 10),
            call(2, KvOp::Get { key }, CallResult::Get(None), 2, 3),
        ]);

        let report = LinearizabilityChecker::new().check(&history);
        assert_eq!(report.outcome, CheckOutcome::Valid);
    }

    #[test]
    fn treats_info_write_as_optional() {
        let key = b"k".to_vec();
        let history = History::from_calls(vec![
            call(
                1,
                KvOp::Put { key: key.clone(), value: b"v1".to_vec() },
                CallResult::Info("timeout".to_string()),
                1,
                2,
            ),
            call(2, KvOp::Get { key: key.clone() }, CallResult::Get(Some(b"v1".to_vec())), 3, 4),
            call(3, KvOp::Get { key }, CallResult::Get(None), 5, 6),
        ]);

        let report = LinearizabilityChecker::new().check(&history);
        assert_eq!(report.outcome, CheckOutcome::Invalid);
    }
}
