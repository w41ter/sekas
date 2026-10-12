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

#[derive(Clone, Copy, Debug, PartialEq, Eq, ValueEnum, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "kebab-case")]
pub(super) enum CaseCategory {
    Sentinel,
    Scenario,
    Operational,
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
        Self::value_variants().iter().copied().find(|case| case.name() == name)
    }

    pub(super) fn description(self) -> &'static str {
        match self {
            Self::PointHit => "point reads from a disk-sized populated working set",
            Self::PointMiss => "point reads for keys that do not exist",
            Self::Insert => "new-key write throughput and latency",
            Self::Update => "existing-key update throughput and latency",
            Self::Delete => "finite deletion pass with visibility verification",
            Self::RangeScan => "bounded single-shard range scan latency",
            Self::CrossShardScan => "complete range scan across two shards and groups",
            Self::LatestVersions => "latest-value reads across MVCC version depths",
            Self::SnapshotPoint => "historical point reads across MVCC version depths",
            Self::SnapshotScan => "historical scan visibility across tombstones and new keys",
            Self::TombstoneRead => "latest and historical reads through tombstones",
            Self::ReadOnlyLocal => "read-only transactions placed in one group",
            Self::ReadOnlyDistributed => "read-only transactions spanning groups",
            Self::BlindLocal => "blind-write transactions placed in one group",
            Self::BlindDistributed => "blind-write transactions spanning groups",
            Self::ReadWriteLocal => "read-write transactions placed in one group",
            Self::ReadWriteDistributed => "read-write transactions spanning groups",
            Self::LargeTxn => "large transaction matrix across participant groups",
            Self::Hotset => "writes against progressively larger hot key sets",
            Self::SiConflict => "snapshot-isolation conflicts under fixed contention",
            Self::SiConflictRetry => "snapshot-isolation conflict retry effectiveness",
            Self::LongShortTxn => "long transactions competing with short transactions",
            Self::GcBacklog => "foreground writes while an MVCC GC backlog is reclaimed",
            Self::GcSteady => "foreground writes during sustained MVCC GC activity",
            Self::RetentionBoundary => "snapshot visibility before and after GC retention",
            Self::InsertCompaction => "insert traffic during flush and compaction activity",
            Self::ChurnCompaction => "update-delete churn during compaction activity",
            Self::ReadUpdate => "fixed-rate read and update mixture",
            Self::ReadScan => "foreground reads under background scan interference",
            Self::SmallLargeTxn => "small transactions under large-transaction interference",
            Self::HotCold => "cold reads under hot-key write interference",
            Self::ImportOnline => "foreground reads during online insert traffic",
            Self::LeaderTransfer => "foreground availability during leader transfer",
            Self::DataLeaderFailover => "foreground availability during data leader failure",
            Self::FollowerRecovery => "foreground availability while a follower recovers",
            Self::ReplicaAdd => "foreground availability while adding a replica",
            Self::ReplicaRemove => "foreground availability while removing a replica",
            Self::SnapshotCatchup => "foreground availability during snapshot catch-up",
            Self::ShardMigration => "foreground availability during shard migration",
            Self::SplitMerge => "foreground availability during split and merge",
            Self::RootFailover => "data and metadata availability during root failover",
            Self::Concurrency => "throughput and latency over a concurrency curve",
            Self::OfferedLoad => "throughput and latency over an offered-load curve",
            Self::GroupScale => "throughput as the number of groups increases",
            Self::NodeScale => "throughput before and after explicit node scale-out",
            Self::MetadataScale => "metadata and data latency as table count increases",
        }
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

    pub(super) fn category(&self) -> CaseCategory {
        match self.scenario {
            Scenario::Basic(_) | Scenario::Mvcc(_) | Scenario::Transaction(_) => {
                CaseCategory::Sentinel
            }
            Scenario::Contention(_) | Scenario::Mixed(_) => CaseCategory::Scenario,
            Scenario::Storage(_) | Scenario::Topology(_) => CaseCategory::Operational,
            Scenario::Capacity(_) => CaseCategory::Capacity,
        }
    }

    pub(super) fn version(&self) -> u32 {
        1
    }

    pub(super) fn description(&self) -> &'static str {
        self.kind.description()
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
    fn registry_has_unique_names_and_all_categories() {
        let names: std::collections::HashSet<_> =
            CaseKind::value_variants().iter().map(|case| case.name()).collect();
        assert_eq!(names.len(), CaseKind::value_variants().len());
        for (category, count) in [
            (CaseCategory::Sentinel, 18),
            (CaseCategory::Scenario, 9),
            (CaseCategory::Operational, 14),
            (CaseCategory::Capacity, 5),
        ] {
            assert_eq!(
                CaseKind::value_variants()
                    .iter()
                    .filter(|kind| Case::build(**kind).category() == category)
                    .count(),
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
