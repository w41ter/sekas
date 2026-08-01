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

use std::collections::{BTreeMap, BTreeSet, HashMap};

use serde::{Deserialize, Serialize};

use crate::history::{CallId, KvValue, ProcessId};

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum TxnOp {
    Get { key: Vec<u8>, value: Option<KvValue> },
    Put { key: Vec<u8>, value: KvValue },
    Delete { key: Vec<u8> },
}

impl TxnOp {
    fn key(&self) -> &[u8] {
        match self {
            TxnOp::Get { key, .. } | TxnOp::Put { key, .. } | TxnOp::Delete { key } => key,
        }
    }

    fn write_value(&self) -> Option<Option<KvValue>> {
        match self {
            TxnOp::Get { .. } => None,
            TxnOp::Put { value, .. } => Some(Some(value.clone())),
            TxnOp::Delete { .. } => Some(None),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum TxnResult {
    Committed,
    Aborted,
    Info(String),
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct TxnCall {
    pub id: CallId,
    pub process: ProcessId,
    pub snapshot: u64,
    pub commit_version: Option<u64>,
    pub ops: Vec<TxnOp>,
    pub result: TxnResult,
    pub invoke_time: u64,
    pub complete_time: u64,
}

impl TxnCall {
    fn is_committed(&self) -> bool {
        matches!(self.result, TxnResult::Committed)
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct TxnHistory {
    pub initial: BTreeMap<Vec<u8>, Option<KvValue>>,
    pub txns: Vec<TxnCall>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum SnapshotIsolationOutcome {
    Valid,
    Invalid { violations: Vec<SnapshotIsolationViolation> },
    Unsupported(String),
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum SnapshotIsolationViolation {
    MissingCommitVersion {
        txn: CallId,
    },
    DuplicateCommitVersion {
        version: u64,
        txns: Vec<CallId>,
    },
    CommitBeforeSnapshot {
        txn: CallId,
        snapshot: u64,
        commit_version: u64,
    },
    StaleRead {
        txn: CallId,
        key: Vec<u8>,
        expected: Option<KvValue>,
        observed: Option<KvValue>,
        snapshot: u64,
    },
    WriteWriteConflict {
        first: CallId,
        second: CallId,
        key: Vec<u8>,
        first_commit_version: u64,
        second_snapshot: u64,
        second_commit_version: u64,
    },
}

#[derive(Clone, Debug, Default)]
pub struct SnapshotIsolationChecker {
    max_committed_txns: usize,
}

impl SnapshotIsolationChecker {
    pub fn new() -> Self {
        SnapshotIsolationChecker { max_committed_txns: 1024 }
    }

    pub fn with_max_committed_txns(mut self, max: usize) -> Self {
        self.max_committed_txns = max;
        self
    }

    pub fn check(&self, history: &TxnHistory) -> SnapshotIsolationOutcome {
        let committed = history.txns.iter().filter(|txn| txn.is_committed()).collect::<Vec<_>>();
        if committed.len() > self.max_committed_txns {
            return SnapshotIsolationOutcome::Unsupported(format!(
                "history has {} committed txns, max supported is {}",
                committed.len(),
                self.max_committed_txns
            ));
        }

        let mut violations = Vec::new();
        let mut by_version = HashMap::<u64, Vec<CallId>>::new();
        for txn in &committed {
            match txn.commit_version {
                Some(commit_version) => {
                    by_version.entry(commit_version).or_default().push(txn.id);
                    if commit_version <= txn.snapshot {
                        violations.push(SnapshotIsolationViolation::CommitBeforeSnapshot {
                            txn: txn.id,
                            snapshot: txn.snapshot,
                            commit_version,
                        });
                    }
                }
                None => violations
                    .push(SnapshotIsolationViolation::MissingCommitVersion { txn: txn.id }),
            }
        }
        for (version, txns) in by_version {
            if txns.len() > 1 {
                violations
                    .push(SnapshotIsolationViolation::DuplicateCommitVersion { version, txns });
            }
        }

        let mut ordered = committed
            .into_iter()
            .filter_map(|txn| txn.commit_version.map(|version| (version, txn)))
            .collect::<Vec<_>>();
        ordered.sort_by_key(|(version, txn)| (*version, txn.id));

        for (_, txn) in &ordered {
            for op in &txn.ops {
                if let TxnOp::Get { key, value } = op {
                    let expected = visible_value(history, &ordered, key, txn.snapshot);
                    if &expected != value {
                        violations.push(SnapshotIsolationViolation::StaleRead {
                            txn: txn.id,
                            key: key.clone(),
                            expected,
                            observed: value.clone(),
                            snapshot: txn.snapshot,
                        });
                    }
                }
            }
        }

        let mut last_writer_by_key = BTreeMap::<Vec<u8>, (CallId, u64)>::new();
        for (commit_version, txn) in &ordered {
            let mut written_keys = BTreeSet::new();
            for op in &txn.ops {
                if op.write_value().is_none() || !written_keys.insert(op.key().to_vec()) {
                    continue;
                }
                if let Some((prev_txn, prev_commit_version)) = last_writer_by_key.get(op.key())
                    && *prev_commit_version > txn.snapshot
                {
                    violations.push(SnapshotIsolationViolation::WriteWriteConflict {
                        first: *prev_txn,
                        second: txn.id,
                        key: op.key().to_vec(),
                        first_commit_version: *prev_commit_version,
                        second_snapshot: txn.snapshot,
                        second_commit_version: *commit_version,
                    });
                }
                last_writer_by_key.insert(op.key().to_vec(), (txn.id, *commit_version));
            }
        }

        if violations.is_empty() {
            SnapshotIsolationOutcome::Valid
        } else {
            SnapshotIsolationOutcome::Invalid { violations }
        }
    }
}

fn visible_value(
    history: &TxnHistory,
    ordered: &[(u64, &TxnCall)],
    key: &[u8],
    snapshot: u64,
) -> Option<KvValue> {
    let mut value = history.initial.get(key).cloned().flatten();
    for (commit_version, txn) in ordered {
        if *commit_version > snapshot {
            break;
        }
        for op in &txn.ops {
            if op.key() == key
                && let Some(next) = op.write_value()
            {
                value = next;
            }
        }
    }
    value
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::{
        SnapshotIsolationChecker, SnapshotIsolationOutcome, SnapshotIsolationViolation, TxnCall,
        TxnHistory, TxnOp, TxnResult,
    };

    fn txn(id: u64, snapshot: u64, commit_version: u64, ops: Vec<TxnOp>) -> TxnCall {
        TxnCall {
            id,
            process: id as usize,
            snapshot,
            commit_version: Some(commit_version),
            ops,
            result: TxnResult::Committed,
            invoke_time: id,
            complete_time: id + 1,
        }
    }

    #[test]
    fn accepts_snapshot_read_from_visible_version() {
        let key = b"k".to_vec();
        let history = TxnHistory {
            initial: BTreeMap::new(),
            txns: vec![
                txn(1, 0, 10, vec![TxnOp::Put { key: key.clone(), value: b"v1".to_vec() }]),
                txn(2, 10, 20, vec![TxnOp::Get { key: key.clone(), value: Some(b"v1".to_vec()) }]),
            ],
        };

        let outcome = SnapshotIsolationChecker::new().check(&history);
        assert_eq!(outcome, SnapshotIsolationOutcome::Valid);
    }

    #[test]
    fn rejects_stale_snapshot_read() {
        let key = b"k".to_vec();
        let history = TxnHistory {
            initial: BTreeMap::new(),
            txns: vec![
                txn(1, 0, 10, vec![TxnOp::Put { key: key.clone(), value: b"v1".to_vec() }]),
                txn(2, 10, 20, vec![TxnOp::Get { key: key.clone(), value: None }]),
            ],
        };

        let outcome = SnapshotIsolationChecker::new().check(&history);
        assert!(matches!(
            outcome,
            SnapshotIsolationOutcome::Invalid { violations }
                if matches!(violations.first(), Some(SnapshotIsolationViolation::StaleRead { .. }))
        ));
    }

    #[test]
    fn rejects_first_committer_wins_violation() {
        let key = b"k".to_vec();
        let history = TxnHistory {
            initial: BTreeMap::new(),
            txns: vec![
                txn(1, 0, 10, vec![TxnOp::Put { key: key.clone(), value: b"v1".to_vec() }]),
                txn(2, 0, 20, vec![TxnOp::Put { key: key.clone(), value: b"v2".to_vec() }]),
            ],
        };

        let outcome = SnapshotIsolationChecker::new().check(&history);
        assert!(matches!(
            outcome,
            SnapshotIsolationOutcome::Invalid { violations }
                if matches!(
                    violations.first(),
                    Some(SnapshotIsolationViolation::WriteWriteConflict { .. })
                )
        ));
    }
}
