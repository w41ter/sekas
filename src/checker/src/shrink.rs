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

use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};

use crate::history::History;
use crate::linear::{CheckOutcome, LinearizabilityChecker};

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ShrinkReport {
    pub original_calls: usize,
    pub minimized_calls: usize,
    pub iterations: usize,
    pub history: History,
}

#[derive(Clone, Debug)]
pub struct HistoryShrinker {
    checker: LinearizabilityChecker,
}

impl HistoryShrinker {
    pub fn new(checker: LinearizabilityChecker) -> Self {
        HistoryShrinker { checker }
    }

    pub fn shrink_invalid(&self, history: &History) -> Option<ShrinkReport> {
        if !self.is_invalid(history) {
            return None;
        }

        let original_calls = history.calls.len();
        let mut calls = self.failed_key_calls(history)?;
        let mut iterations = 0;
        let mut chunk = (calls.len() / 2).max(1);

        while chunk > 0 {
            let mut changed = false;
            let mut start = 0;
            while start < calls.len() {
                let end = (start + chunk).min(calls.len());
                let mut candidate = calls.clone();
                candidate.drain(start..end);
                let candidate_history = History::from_calls(candidate.clone());
                iterations += 1;
                if self.is_invalid(&candidate_history) {
                    calls = candidate;
                    changed = true;
                } else {
                    start += chunk;
                }
            }
            if !changed {
                chunk /= 2;
            }
        }

        Some(ShrinkReport {
            original_calls,
            minimized_calls: calls.len(),
            iterations,
            history: History::from_calls(calls),
        })
    }

    fn failed_key_calls(&self, history: &History) -> Option<Vec<crate::history::Call>> {
        let report = self.checker.check(history);
        let failed_keys = report
            .keys
            .into_iter()
            .filter(|key| matches!(key.outcome, CheckOutcome::Invalid))
            .map(|key| key.key)
            .collect::<BTreeSet<_>>();
        if failed_keys.is_empty() {
            return None;
        }
        Some(
            history.calls.iter().filter(|call| failed_keys.contains(call.key())).cloned().collect(),
        )
    }

    fn is_invalid(&self, history: &History) -> bool {
        matches!(self.checker.check(history).outcome, CheckOutcome::Invalid)
    }
}

#[cfg(test)]
mod tests {
    use super::HistoryShrinker;
    use crate::history::{Call, CallResult, History, KvOp};
    use crate::linear::{CheckOutcome, LinearizabilityChecker};

    fn call(id: u64, op: KvOp, result: CallResult, invoke: u64, complete: u64) -> Call {
        Call { id, process: id as usize, op, result, invoke_time: invoke, complete_time: complete }
    }

    #[test]
    fn shrink_invalid_history_preserves_failure() {
        let key = b"k".to_vec();
        let noise = b"noise".to_vec();
        let history = History::from_calls(vec![
            call(1, KvOp::Put { key: noise.clone(), value: b"n".to_vec() }, CallResult::Put, 1, 2),
            call(2, KvOp::Put { key: key.clone(), value: b"v".to_vec() }, CallResult::Put, 3, 4),
            call(3, KvOp::Get { key: key.clone() }, CallResult::Get(None), 5, 6),
            call(4, KvOp::Get { key: noise }, CallResult::Get(Some(b"n".to_vec())), 7, 8),
        ]);
        let checker = LinearizabilityChecker::new();
        assert!(matches!(checker.check(&history).outcome, CheckOutcome::Invalid));

        let shrink = HistoryShrinker::new(checker.clone()).shrink_invalid(&history).unwrap();
        assert!(shrink.minimized_calls < shrink.original_calls);
        assert!(matches!(checker.check(&shrink.history).outcome, CheckOutcome::Invalid));
    }
}
