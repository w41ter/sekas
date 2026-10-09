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

mod basic;
mod capacity;
mod contention;
mod mixed;
mod mvcc;
mod storage;
mod support;
mod topology;
mod transaction;

use anyhow::Result;
use basic::BasicCase;
use capacity::CapacityCase;
use clap::ValueEnum;
use contention::ContentionCase;
use mixed::MixedCase;
use mvcc::MvccCase;
use storage::StorageCase;
use topology::TopologyCase;
use transaction::{KeyMatrix, Placement, TransactionCase};

use super::LabContext;
use super::config::LabConfig;
use super::report::CaseReport;
use super::workload::TxnMode;

#[derive(Clone, Copy, Debug, PartialEq, Eq, ValueEnum)]
pub(super) enum Theme {
    Basic,
    Mvcc,
    Transaction,
    Contention,
    Storage,
    Mixed,
    Topology,
    Capacity,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, ValueEnum)]
pub(super) enum CaseKind {
    PointHit,
    PointMiss,
    Insert,
    Update,
    Delete,
    RangeScan,
    CrossShardScan,
    LatestVersions,
    SnapshotPoint,
    SnapshotScan,
    TombstoneRead,
    ReadOnlyLocal,
    ReadOnlyDistributed,
    BlindLocal,
    BlindDistributed,
    ReadWriteLocal,
    ReadWriteDistributed,
    LargeTxn,
    Hotset,
    SiConflict,
    SiConflictRetry,
    LongShortTxn,
    GcBacklog,
    GcSteady,
    RetentionBoundary,
    InsertCompaction,
    ChurnCompaction,
    ReadUpdate,
    ReadScan,
    SmallLargeTxn,
    HotCold,
    ImportOnline,
    LeaderTransfer,
    DataLeaderFailover,
    FollowerRecovery,
    ReplicaAdd,
    ReplicaRemove,
    SnapshotCatchup,
    ShardMigration,
    SplitMerge,
    RootFailover,
    Concurrency,
    OfferedLoad,
    GroupScale,
    NodeScale,
    MetadataScale,
}

pub(super) const ALL_CASES: &[CaseKind] = &[
    CaseKind::PointHit,
    CaseKind::PointMiss,
    CaseKind::Insert,
    CaseKind::Update,
    CaseKind::Delete,
    CaseKind::RangeScan,
    CaseKind::CrossShardScan,
    CaseKind::LatestVersions,
    CaseKind::SnapshotPoint,
    CaseKind::SnapshotScan,
    CaseKind::TombstoneRead,
    CaseKind::ReadOnlyLocal,
    CaseKind::ReadOnlyDistributed,
    CaseKind::BlindLocal,
    CaseKind::BlindDistributed,
    CaseKind::ReadWriteLocal,
    CaseKind::ReadWriteDistributed,
    CaseKind::LargeTxn,
    CaseKind::Hotset,
    CaseKind::SiConflict,
    CaseKind::SiConflictRetry,
    CaseKind::LongShortTxn,
    CaseKind::GcBacklog,
    CaseKind::GcSteady,
    CaseKind::RetentionBoundary,
    CaseKind::InsertCompaction,
    CaseKind::ChurnCompaction,
    CaseKind::ReadUpdate,
    CaseKind::ReadScan,
    CaseKind::SmallLargeTxn,
    CaseKind::HotCold,
    CaseKind::ImportOnline,
    CaseKind::LeaderTransfer,
    CaseKind::DataLeaderFailover,
    CaseKind::FollowerRecovery,
    CaseKind::ReplicaAdd,
    CaseKind::ReplicaRemove,
    CaseKind::SnapshotCatchup,
    CaseKind::ShardMigration,
    CaseKind::SplitMerge,
    CaseKind::RootFailover,
    CaseKind::Concurrency,
    CaseKind::OfferedLoad,
    CaseKind::GroupScale,
    CaseKind::NodeScale,
    CaseKind::MetadataScale,
];

impl CaseKind {
    pub(super) fn name(self) -> &'static str {
        match self {
            Self::PointHit => "point-hit",
            Self::PointMiss => "point-miss",
            Self::Insert => "insert",
            Self::Update => "update",
            Self::Delete => "delete",
            Self::RangeScan => "range-scan",
            Self::CrossShardScan => "cross-shard-scan",
            Self::LatestVersions => "latest-versions",
            Self::SnapshotPoint => "snapshot-point",
            Self::SnapshotScan => "snapshot-scan",
            Self::TombstoneRead => "tombstone-read",
            Self::ReadOnlyLocal => "read-only-local",
            Self::ReadOnlyDistributed => "read-only-distributed",
            Self::BlindLocal => "blind-local",
            Self::BlindDistributed => "blind-distributed",
            Self::ReadWriteLocal => "read-write-local",
            Self::ReadWriteDistributed => "read-write-distributed",
            Self::LargeTxn => "large-txn",
            Self::Hotset => "hotset",
            Self::SiConflict => "si-conflict",
            Self::SiConflictRetry => "si-conflict-retry",
            Self::LongShortTxn => "long-short-txn",
            Self::GcBacklog => "gc-backlog",
            Self::GcSteady => "gc-steady",
            Self::RetentionBoundary => "retention-boundary",
            Self::InsertCompaction => "insert-compaction",
            Self::ChurnCompaction => "churn-compaction",
            Self::ReadUpdate => "read-update",
            Self::ReadScan => "read-scan",
            Self::SmallLargeTxn => "small-large-txn",
            Self::HotCold => "hot-cold",
            Self::ImportOnline => "import-online",
            Self::LeaderTransfer => "leader-transfer",
            Self::DataLeaderFailover => "data-leader-failover",
            Self::FollowerRecovery => "follower-recovery",
            Self::ReplicaAdd => "replica-add",
            Self::ReplicaRemove => "replica-remove",
            Self::SnapshotCatchup => "snapshot-catchup",
            Self::ShardMigration => "shard-migration",
            Self::SplitMerge => "split-merge",
            Self::RootFailover => "root-failover",
            Self::Concurrency => "concurrency",
            Self::OfferedLoad => "offered-load",
            Self::GroupScale => "group-scale",
            Self::NodeScale => "node-scale",
            Self::MetadataScale => "metadata-scale",
        }
    }
    pub(super) fn from_report_name(name: &str) -> Option<Self> {
        ALL_CASES.iter().copied().find(|case| case.name() == name)
    }
}

/// A built scenario. Identity stays stable while execution uses theme-specific
/// types.
#[derive(Clone, Copy)]
pub(super) struct Case {
    kind: CaseKind,
    scenario: Scenario,
}

#[derive(Clone, Copy)]
enum Scenario {
    Basic(BasicCase),
    Mvcc(MvccCase),
    Transaction(TransactionCase),
    Contention(ContentionCase),
    Storage(StorageCase),
    Mixed(MixedCase),
    Topology(TopologyCase),
    Capacity(CapacityCase),
}

impl Case {
    /// Resolve CLI identity into execution semantics without starting a
    /// cluster.
    pub(super) fn build(kind: CaseKind) -> Self {
        let scenario = match kind {
            CaseKind::PointHit => Scenario::Basic(BasicCase::PointHit),
            CaseKind::PointMiss => Scenario::Basic(BasicCase::PointMiss),
            CaseKind::Insert => Scenario::Basic(BasicCase::Insert),
            CaseKind::Update => Scenario::Basic(BasicCase::Update),
            CaseKind::Delete => Scenario::Basic(BasicCase::Delete),
            CaseKind::RangeScan => Scenario::Basic(BasicCase::RangeScan),
            CaseKind::CrossShardScan => Scenario::Basic(BasicCase::CrossShardScan),
            CaseKind::LatestVersions => Scenario::Mvcc(MvccCase::LatestVersions),
            CaseKind::SnapshotPoint => Scenario::Mvcc(MvccCase::SnapshotPoint),
            CaseKind::SnapshotScan => Scenario::Mvcc(MvccCase::SnapshotScan),
            CaseKind::TombstoneRead => Scenario::Mvcc(MvccCase::TombstoneRead),
            CaseKind::ReadOnlyLocal => Scenario::Transaction(TransactionCase {
                mode: TxnMode::ReadOnly,
                placement: Placement::Local,
                key_matrix: KeyMatrix::Regular,
            }),
            CaseKind::ReadOnlyDistributed => Scenario::Transaction(TransactionCase {
                mode: TxnMode::ReadOnly,
                placement: Placement::Distributed,
                key_matrix: KeyMatrix::Regular,
            }),
            CaseKind::BlindLocal => Scenario::Transaction(TransactionCase {
                mode: TxnMode::Blind,
                placement: Placement::Local,
                key_matrix: KeyMatrix::Regular,
            }),
            CaseKind::BlindDistributed => Scenario::Transaction(TransactionCase {
                mode: TxnMode::Blind,
                placement: Placement::Distributed,
                key_matrix: KeyMatrix::Regular,
            }),
            CaseKind::ReadWriteLocal => Scenario::Transaction(TransactionCase {
                mode: TxnMode::ReadWrite,
                placement: Placement::Local,
                key_matrix: KeyMatrix::Regular,
            }),
            CaseKind::ReadWriteDistributed => Scenario::Transaction(TransactionCase {
                mode: TxnMode::ReadWrite,
                placement: Placement::Distributed,
                key_matrix: KeyMatrix::Regular,
            }),
            CaseKind::LargeTxn => Scenario::Transaction(TransactionCase {
                mode: TxnMode::Blind,
                placement: Placement::Matrix,
                key_matrix: KeyMatrix::Large,
            }),
            CaseKind::Hotset => Scenario::Contention(ContentionCase::Hotset),
            CaseKind::SiConflict => Scenario::Contention(ContentionCase::SiConflict),
            CaseKind::SiConflictRetry => Scenario::Contention(ContentionCase::SiConflictRetry),
            CaseKind::LongShortTxn => Scenario::Contention(ContentionCase::LongShortTxn),
            CaseKind::GcBacklog => Scenario::Storage(StorageCase::GcBacklog),
            CaseKind::GcSteady => Scenario::Storage(StorageCase::GcSteady),
            CaseKind::RetentionBoundary => Scenario::Storage(StorageCase::RetentionBoundary),
            CaseKind::InsertCompaction => Scenario::Storage(StorageCase::InsertCompaction),
            CaseKind::ChurnCompaction => Scenario::Storage(StorageCase::ChurnCompaction),
            CaseKind::ReadUpdate => Scenario::Mixed(MixedCase::ReadUpdate),
            CaseKind::ReadScan => Scenario::Mixed(MixedCase::ReadScan),
            CaseKind::SmallLargeTxn => Scenario::Mixed(MixedCase::SmallLargeTxn),
            CaseKind::HotCold => Scenario::Mixed(MixedCase::HotCold),
            CaseKind::ImportOnline => Scenario::Mixed(MixedCase::ImportOnline),
            CaseKind::LeaderTransfer => Scenario::Topology(TopologyCase::LeaderTransfer),
            CaseKind::DataLeaderFailover => Scenario::Topology(TopologyCase::DataLeaderFailover),
            CaseKind::FollowerRecovery => Scenario::Topology(TopologyCase::FollowerRecovery),
            CaseKind::ReplicaAdd => Scenario::Topology(TopologyCase::ReplicaAdd),
            CaseKind::ReplicaRemove => Scenario::Topology(TopologyCase::ReplicaRemove),
            CaseKind::SnapshotCatchup => Scenario::Topology(TopologyCase::SnapshotCatchup),
            CaseKind::ShardMigration => Scenario::Topology(TopologyCase::ShardMigration),
            CaseKind::SplitMerge => Scenario::Topology(TopologyCase::SplitMerge),
            CaseKind::RootFailover => Scenario::Topology(TopologyCase::RootFailover),
            CaseKind::Concurrency => Scenario::Capacity(CapacityCase::Concurrency),
            CaseKind::OfferedLoad => Scenario::Capacity(CapacityCase::OfferedLoad),
            CaseKind::GroupScale => Scenario::Capacity(CapacityCase::GroupScale),
            CaseKind::NodeScale => Scenario::Capacity(CapacityCase::NodeScale),
            CaseKind::MetadataScale => Scenario::Capacity(CapacityCase::MetadataScale),
        };
        Self { kind, scenario }
    }

    pub(super) fn name(&self) -> &'static str {
        self.kind.name()
    }

    pub(super) fn theme(&self) -> Theme {
        match self.scenario {
            Scenario::Basic(_) => Theme::Basic,
            Scenario::Mvcc(_) => Theme::Mvcc,
            Scenario::Transaction(_) => Theme::Transaction,
            Scenario::Contention(_) => Theme::Contention,
            Scenario::Storage(_) => Theme::Storage,
            Scenario::Mixed(_) => Theme::Mixed,
            Scenario::Topology(_) => Theme::Topology,
            Scenario::Capacity(_) => Theme::Capacity,
        }
    }

    pub(super) fn configure(&self, cfg: &mut LabConfig) {
        // Fix layout and version state unless the scenario explicitly changes them.
        cfg.cluster.root.enable_shard_balance = false;
        cfg.cluster.root.enable_auto_shard_split = false;
        cfg.cluster.root.enable_auto_shard_merge = false;
        cfg.cluster.root.enable_leader_balance = false;
        cfg.cluster.root.enable_replica_balance = false;
        cfg.cluster.root.enable_group_balance = true;
        cfg.cluster.root.mvcc_gc_retention_ms = 0;
        cfg.cluster.node.mvcc_gc_interval_ms = 0;
        cfg.cluster.node.mvcc_gc_retention_ms = 0;
        cfg.cluster.db.mvcc_gc_retention_ms = 0;
        match self.scenario {
            Scenario::Storage(case) => case.configure(cfg),
            Scenario::Topology(case) => case.configure(cfg),
            Scenario::Capacity(case) => case.configure(cfg),
            _ => {}
        }
    }

    pub(super) async fn run(&self, lab: &mut LabContext) -> Result<CaseReport> {
        match self.scenario {
            Scenario::Basic(case) => case.run(self.name(), lab).await,
            Scenario::Mvcc(case) => case.run(self.name(), lab).await,
            Scenario::Transaction(case) => case.run(self.name(), lab).await,
            Scenario::Contention(case) => case.run(self.name(), lab).await,
            Scenario::Storage(case) => case.run(self.name(), lab).await,
            Scenario::Mixed(case) => case.run(self.name(), lab).await,
            Scenario::Topology(case) => case.run(self.name(), lab).await,
            Scenario::Capacity(case) => case.run(self.name(), lab).await,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn registry_has_unique_names_and_all_themes() {
        let names: std::collections::HashSet<_> =
            ALL_CASES.iter().map(|case| case.name()).collect();
        assert_eq!(names.len(), ALL_CASES.len());
        assert_eq!(ALL_CASES, CaseKind::value_variants());
        for (theme, count) in [
            (Theme::Basic, 7),
            (Theme::Mvcc, 4),
            (Theme::Transaction, 7),
            (Theme::Contention, 4),
            (Theme::Storage, 5),
            (Theme::Mixed, 5),
            (Theme::Topology, 9),
            (Theme::Capacity, 5),
        ] {
            assert_eq!(
                ALL_CASES.iter().filter(|kind| Case::build(**kind).theme() == theme).count(),
                count
            );
        }
        assert!(CaseKind::from_report_name("txn-conflict").is_none());
    }
    #[test]
    fn scenarios_enable_their_required_events() {
        let mut cfg = LabConfig::default();
        Case::build(CaseKind::SnapshotCatchup).configure(&mut cfg);
        assert!(cfg.cluster.raft.testing_knobs.force_new_peer_receiving_snapshot);
        let mut cfg = LabConfig::default();
        Case::build(CaseKind::GcBacklog).configure(&mut cfg);
        assert!(
            cfg.cluster.root.mvcc_gc_retention_ms > 0 && cfg.cluster.node.mvcc_gc_interval_ms > 0
        );
        Case::build(CaseKind::InsertCompaction).configure(&mut cfg);
        assert_eq!(cfg.cluster.db.write_buffer_size, 256 * 1024);
        assert_eq!(cfg.cluster.db.level0_file_num_compaction_trigger, 2);
        let mut cfg = LabConfig::default();
        Case::build(CaseKind::NodeScale).configure(&mut cfg);
        assert!(cfg.cluster.node.replica.testing_knobs.disable_scheduler_durable_task);
        let mut cfg = LabConfig::default();
        Case::build(CaseKind::SnapshotPoint).configure(&mut cfg);
        assert_eq!(cfg.cluster.root.mvcc_gc_retention_ms, 0);
        assert_eq!(cfg.cluster.db.mvcc_gc_retention_ms, 0);
        assert!(!cfg.cluster.root.enable_auto_shard_split);
    }
}
