// Copyright 2026-present The Sekas Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant};

use sekas_api::Epoch;
use sekas_api::server::v1::{
    GroupDesc, GroupStats, GroupStatsSample, MoveShardDesc, RaftRole, RangePartition, ReplicaState,
    ShardTraffic,
};

/// Use the latest available size and traffic estimates for planning and SHOW.
/// Keep older estimates through report expiry, leader changes, splits, and
/// moves so estimates stay available between reports. Use the last complete
/// group total for any metric that cannot be summed from its shards.
/// Leader and migration state require a current report.
#[derive(Default)]
pub(super) struct ShowStats {
    groups: HashMap<u64, GroupSample>,
    shard_metrics: HashMap<u64, ShardMetrics>,
    group_metrics: HashMap<u64, ShowMetrics>,
    merge_quiet: Option<MergeQuiet>,
}

struct MergeQuiet {
    write_qps: f64,
    max_gap: Duration,
}

struct ShardMetrics {
    group_id: u64,
    epoch: u64,
    metrics: ShowMetrics,
    request_sample: Option<ShardRequestSample>,
}

struct ShardRequestSample {
    received_at: Instant,
    range: Option<RangePartition>,
    units: RequestUnits,
}

struct GroupSample {
    metadata: GroupStatsSample,
    node_id: u64,
    received_at: Instant,
    traffic: HashMap<u64, ShardSample>,
    request_rates: Arc<RequestRates>,
}

struct ShardSample {
    traffic: ShardTraffic,
    range: Option<RangePartition>,
    quiet_since: Option<Instant>,
}

struct RequestRates {
    shards: HashMap<u64, RequestUnits>,
    total: RequestUnits,
    shard_count: u64,
    quiet_for: HashMap<u64, Duration>,
}

#[derive(Clone, Copy, Default)]
pub(in crate::root) struct RequestUnits {
    pub read: Option<f64>,
    pub write: Option<f64>,
}

impl RequestUnits {
    pub const ZERO: Self = Self { read: Some(0.), write: Some(0.) };

    pub fn add(self, other: Self) -> Self {
        Self {
            read: self.read.zip(other.read).map(|(a, b)| a + b),
            write: self.write.zip(other.write).map(|(a, b)| a + b),
        }
    }

    pub fn total(self) -> Option<f64> {
        Some(self.read? + self.write?)
    }

    pub fn with_fallback(self, fallback: Self) -> Self {
        Self { read: self.read.or(fallback.read), write: self.write.or(fallback.write) }
    }

    pub fn get(self, kind: crate::BalanceDimension) -> Option<f64> {
        match kind {
            crate::BalanceDimension::ReadUnits => self.read,
            crate::BalanceDimension::WriteUnits => self.write,
            _ => unreachable!("expected a request dimension"),
        }
    }
}

#[derive(Clone, Copy)]
pub(in crate::root) struct TrafficRates {
    pub read_qps: f64,
    pub write_qps: f64,
    pub read_bytes_per_sec: f64,
    pub write_bytes_per_sec: f64,
    pub read_units_per_sec: Option<f64>,
    pub write_units_per_sec: Option<f64>,
}

impl Default for TrafficRates {
    fn default() -> Self {
        Self {
            read_qps: 0.,
            write_qps: 0.,
            read_bytes_per_sec: 0.,
            write_bytes_per_sec: 0.,
            read_units_per_sec: Some(0.),
            write_units_per_sec: Some(0.),
        }
    }
}

#[derive(Clone, Copy)]
pub(in crate::root) struct ShowMetrics {
    pub num_shards: usize,
    pub bytes: Option<u64>,
    pub rates: Option<TrafficRates>,
}

pub(in crate::root) struct ShowSnapshot {
    shards: HashMap<u64, ShowMetrics>,
    group_metrics: HashMap<u64, ShowMetrics>,
    migration_states: HashMap<u64, bool>,
    migration_report_issues: HashMap<u64, String>,
    pub(in crate::root) leaders: HashMap<u64, ReplicaState>,
    request_rates: HashMap<u64, Arc<RequestRates>>,
    recent_shard_requests: HashMap<u64, RequestUnits>,
}

impl ShowStats {
    pub(super) fn with_merge_quiet_tracking(write_qps: f64, max_gap: Duration) -> Self {
        Self { merge_quiet: Some(MergeQuiet { write_qps, max_gap }), ..Self::default() }
    }

    pub(super) fn reported_migration(
        &self,
        group: u64,
        max_age: Duration,
    ) -> Option<MoveShardDesc> {
        let report = self.groups.get(&group)?;
        if report.received_at.elapsed() > max_age {
            return None;
        }
        report.metadata.shard_migration.as_ref()?.desc.clone()
    }

    pub(super) fn update(&mut self, stats: &GroupStats, node_id: u64) -> bool {
        let Some(new) = stats.sample.as_ref() else { return false };
        let previous = self.groups.get(&stats.group_id);
        let Ok(interval) = previous.map_or(Ok(None), |old| old.sample_interval(new)) else {
            return false;
        };
        let epoch = new.group_epoch;
        let now = Instant::now();
        let mut traffic = HashMap::with_capacity(stats.shard_stats.len());
        let mut request_rates = HashMap::new();
        let mut request_total = RequestUnits::ZERO;
        let mut quiet_shards = HashMap::new();
        let mut total = ShowMetrics::default();
        for shard in &stats.shard_stats {
            let id = shard.shard_id;
            let old_shard = previous.and_then(|sample| sample.traffic.get(&id));
            let rates = interval.and_then(|interval| {
                TrafficRates::between(old_shard?.traffic, shard.traffic?, interval)
            });
            if let Some(rates) = rates
                && previous.is_some_and(|old| old.metadata.group_epoch == epoch)
            {
                let units = rates.request_units();
                request_rates.insert(id, units);
                request_total = request_total.add(units);
            }
            if let Some(counters) = shard.traffic {
                let quiet_since = self.merge_quiet.as_ref().and_then(|rule| {
                    let previous = previous?;
                    let old_shard = old_shard?;
                    (rates?.write_qps < rule.write_qps
                        && now.duration_since(previous.received_at) <= rule.max_gap
                        && shard.range.is_some()
                        && old_shard.range == shard.range)
                        .then(|| old_shard.quiet_since.unwrap_or(previous.received_at))
                });
                if let Some(since) = quiet_since {
                    quiet_shards.insert(id, now.duration_since(since));
                }
                traffic.insert(
                    id,
                    ShardSample { traffic: counters, range: shard.range.clone(), quiet_since },
                );
            }

            let mut metrics = ShowMetrics { num_shards: 1, bytes: Some(shard.shard_size), rates };
            let cached = self.shard_metrics.entry(id).or_insert(ShardMetrics {
                group_id: stats.group_id,
                epoch,
                metrics,
                request_sample: None,
            });
            metrics.fill_missing(cached.metrics);
            // Both groups can report the shard during a move. Prefer the newer epoch.
            if epoch >= cached.epoch {
                cached.group_id = stats.group_id;
                cached.epoch = epoch;
                cached.metrics = metrics;
            } else {
                cached.metrics.fill_missing(metrics);
            }
            if let Some(&units) = request_rates.get(&id) {
                cached.request_sample = Some(ShardRequestSample {
                    received_at: now,
                    range: shard.range.clone(),
                    units,
                });
            }
            total.add(metrics);
        }
        if stats.shard_count == 0 {
            self.group_metrics.remove(&stats.group_id);
        } else if total.num_shards as u64 == stats.shard_count {
            if let Some(&cached) = self.group_metrics.get(&stats.group_id) {
                total.fill_missing(cached);
            }
            self.group_metrics.insert(stats.group_id, total);
        }
        self.groups.insert(
            stats.group_id,
            GroupSample {
                metadata: new.clone(),
                node_id,
                received_at: now,
                traffic,
                request_rates: Arc::new(RequestRates {
                    total: (request_rates.len() as u64 == stats.shard_count)
                        .then_some(request_total)
                        .unwrap_or_default(),
                    shards: request_rates,
                    shard_count: stats.shard_count,
                    quiet_for: quiet_shards,
                }),
            },
        );
        true
    }

    pub(super) fn snapshot(&self, groups: &[GroupDesc], max_age: Duration) -> ShowSnapshot {
        let now = Instant::now();
        let mut shards = HashMap::new();
        let mut group_metrics = HashMap::new();
        let mut migration_states = HashMap::new();
        let mut migration_report_issues = HashMap::new();
        let mut leaders = HashMap::new();
        let mut request_rates = HashMap::new();
        let mut recent_shard_requests = HashMap::new();
        for group in groups {
            for shard in &group.shards {
                if let Some(sample) = self.shard_metrics.get(&shard.id) {
                    shards.insert(shard.id, sample.metrics);
                    if let Some(request) = &sample.request_sample
                        && request.range == shard.range
                        && now.duration_since(request.received_at) <= max_age
                    {
                        recent_shard_requests.insert(shard.id, request.units);
                    }
                }
            }
            if let Some(&metrics) = self.group_metrics.get(&group.id) {
                group_metrics.insert(group.id, metrics);
            }

            let Some(report) = self.groups.get(&group.id) else {
                migration_report_issues.insert(group.id, "no leader stats received".to_owned());
                continue;
            };
            let age = now.duration_since(report.received_at);
            if age > max_age {
                migration_report_issues
                    .insert(group.id, format!("leader stats expired ({}s old)", age.as_secs()));
                continue;
            }
            if !report.matches_reporter(group) {
                let reported_config = Epoch(report.metadata.group_epoch).config_epoch();
                let reason = if reported_config != Epoch(group.epoch).config_epoch() {
                    format!(
                        "reported configuration {}, current configuration {}",
                        reported_config,
                        Epoch(group.epoch).config_epoch()
                    )
                } else {
                    "reporting replica is outside the current configuration".to_owned()
                };
                migration_report_issues.insert(group.id, reason);
                continue;
            }
            migration_states.insert(group.id, report.metadata.shard_migration.is_some());
            if report.metadata.group_epoch == group.epoch {
                request_rates.insert(group.id, report.request_rates.clone());
            }
            leaders.insert(
                group.id,
                ReplicaState {
                    group_id: group.id,
                    replica_id: report.metadata.replica_id,
                    node_id: report.node_id,
                    term: report.metadata.term,
                    role: RaftRole::Leader as i32,
                    ..Default::default()
                },
            );
        }
        ShowSnapshot {
            shards,
            group_metrics,
            migration_states,
            migration_report_issues,
            leaders,
            request_rates,
            recent_shard_requests,
        }
    }

    pub(super) fn retain(&mut self, groups: &[GroupDesc]) {
        let group_epochs: HashMap<_, _> =
            groups.iter().map(|group| (group.id, group.epoch)).collect();
        let shards_alive: HashSet<_> =
            groups.iter().flat_map(|group| group.shards.iter().map(|shard| shard.id)).collect();
        self.groups.retain(|group, _| group_epochs.contains_key(group));
        self.group_metrics.retain(|group, _| group_epochs.contains_key(group));
        self.shard_metrics.retain(|shard, sample| {
            // A report can include a new shard before the catalog sees the split.
            shards_alive.contains(shard)
                || group_epochs.get(&sample.group_id).is_some_and(|epoch| *epoch < sample.epoch)
        });
    }

    pub(super) fn clear(&mut self) {
        self.groups.clear();
        self.shard_metrics.clear();
        self.group_metrics.clear();
    }
}

impl GroupSample {
    /// Reject out-of-order reports. An accepted report can start a new
    /// baseline.
    fn sample_interval(&self, new: &GroupStatsSample) -> Result<Option<Duration>, ()> {
        let old = &self.metadata;
        if new.term < old.term || new.group_epoch < old.group_epoch {
            return Err(());
        }
        if new.term != old.term || new.replica_id != old.replica_id {
            return Ok(None);
        }
        if new.elapsed_nanos > old.elapsed_nanos {
            return Ok(Some(Duration::from_nanos(new.elapsed_nanos - old.elapsed_nanos)));
        }
        if new.group_epoch == old.group_epoch {
            return Err(());
        }
        Ok(None)
    }

    fn matches_reporter(&self, group: &GroupDesc) -> bool {
        Epoch(self.metadata.group_epoch).config_epoch() == Epoch(group.epoch).config_epoch()
            && group.replicas.iter().any(|replica| {
                replica.id == self.metadata.replica_id && replica.node_id == self.node_id
            })
    }
}

impl TrafficRates {
    pub(in crate::root) fn request_units(self) -> RequestUnits {
        RequestUnits { read: self.read_units_per_sec, write: self.write_units_per_sec }
    }

    fn between(old: ShardTraffic, new: ShardTraffic, interval: Duration) -> Option<Self> {
        let seconds = interval.as_secs_f64();
        let units_per_sec =
            |old: Option<u64>, new: Option<u64>| Some(new?.checked_sub(old?)? as f64 / seconds);
        Some(Self {
            read_qps: new.reads.checked_sub(old.reads)? as f64 / seconds,
            write_qps: new.writes.checked_sub(old.writes)? as f64 / seconds,
            read_bytes_per_sec: new.read_bytes.checked_sub(old.read_bytes)? as f64 / seconds,
            write_bytes_per_sec: new.write_bytes.checked_sub(old.write_bytes)? as f64 / seconds,
            read_units_per_sec: units_per_sec(old.read_units, new.read_units),
            write_units_per_sec: units_per_sec(old.write_units, new.write_units),
        })
    }

    pub(in crate::root) fn add(self, other: Self) -> Self {
        Self {
            read_qps: self.read_qps + other.read_qps,
            write_qps: self.write_qps + other.write_qps,
            read_bytes_per_sec: self.read_bytes_per_sec + other.read_bytes_per_sec,
            write_bytes_per_sec: self.write_bytes_per_sec + other.write_bytes_per_sec,
            read_units_per_sec: self
                .read_units_per_sec
                .zip(other.read_units_per_sec)
                .map(|(a, b)| a + b),
            write_units_per_sec: self
                .write_units_per_sec
                .zip(other.write_units_per_sec)
                .map(|(a, b)| a + b),
        }
    }
}

impl Default for ShowMetrics {
    fn default() -> Self {
        Self { num_shards: 0, bytes: Some(0), rates: Some(TrafficRates::default()) }
    }
}

impl ShowMetrics {
    fn fill_missing(&mut self, cached: Self) {
        self.bytes = self.bytes.or(cached.bytes);
        match (&mut self.rates, cached.rates) {
            (Some(current), Some(last)) => {
                current.read_units_per_sec = current.read_units_per_sec.or(last.read_units_per_sec);
                current.write_units_per_sec =
                    current.write_units_per_sec.or(last.write_units_per_sec);
            }
            (None, last) => self.rates = last,
            _ => {}
        }
    }

    pub(in crate::root) fn unavailable(num_shards: usize) -> Self {
        Self { num_shards, bytes: None, rates: None }
    }

    pub(in crate::root) fn add(&mut self, other: Self) {
        self.num_shards += other.num_shards;
        self.bytes = self.bytes.zip(other.bytes).map(|(a, b)| a.saturating_add(b));
        self.rates = self.rates.zip(other.rates).map(|(a, b)| a.add(b));
    }
}

impl ShowSnapshot {
    pub(in crate::root) fn shard_request_units(&self, group: u64, shard: u64) -> RequestUnits {
        self.leader(group)
            .and_then(|_| self.request_rates.get(&group)?.shards.get(&shard))
            .copied()
            .unwrap_or_default()
    }

    pub(in crate::root) fn shard_is_quiet(&self, group: u64, shard: u64, period: Duration) -> bool {
        self.leader(group).is_some()
            && self
                .request_rates
                .get(&group)
                .and_then(|rates| rates.quiet_for.get(&shard))
                .is_some_and(|&observed| observed >= period)
    }

    pub(in crate::root) fn group_request_units(&self, group: &GroupDesc) -> RequestUnits {
        self.leader(group.id)
            .and_then(|_| self.request_rates.get(&group.id))
            .filter(|samples| samples.shard_count == group.shards.len() as u64)
            .map(|samples| samples.total)
            .unwrap_or_default()
    }

    pub(in crate::root) fn recent_shard_requests(&self, shard: u64) -> RequestUnits {
        self.recent_shard_requests.get(&shard).copied().unwrap_or_default()
    }

    pub(in crate::root) fn migration_report_issue(&self, group: u64) -> Option<&str> {
        self.migration_report_issues.get(&group).map(String::as_str)
    }

    pub(in crate::root) fn migration_idle(&self, group_id: u64) -> bool {
        self.migration_states.get(&group_id) == Some(&false)
    }

    pub(in crate::root) fn leader(&self, group_id: u64) -> Option<ReplicaState> {
        self.leaders.get(&group_id).copied()
    }

    pub(in crate::root) fn shard(&self, shard_id: u64) -> ShowMetrics {
        self.shards.get(&shard_id).copied().unwrap_or(ShowMetrics::unavailable(1))
    }

    pub(in crate::root) fn group(&self, group: &GroupDesc) -> ShowMetrics {
        let mut result = ShowMetrics::default();
        for shard in &group.shards {
            result.add(self.shard(shard.id));
        }
        if let Some(&cached) = self.group_metrics.get(&group.id) {
            result.fill_missing(cached);
        }
        result
    }

    pub(in crate::root) fn tables(&self, groups: &[GroupDesc]) -> HashMap<u64, ShowMetrics> {
        // Both groups can list the same shard during a move.
        let mut seen = HashSet::new();
        let mut tables = HashMap::<u64, ShowMetrics>::new();
        for group in groups {
            for shard in &group.shards {
                if seen.insert(shard.id) {
                    tables.entry(shard.table_id).or_default().add(self.shard(shard.id));
                }
            }
        }
        tables
    }
}

#[cfg(test)]
mod tests {
    use sekas_api::server::v1::{ReplicaDesc, ShardDesc, ShardStats};

    use super::*;

    #[test]
    fn merge_requires_observed_quiet_ranges_without_gaps_or_writes() {
        let mut shard = ShardDesc::with_range(1, 2, b"a".to_vec(), b"z".to_vec());
        let group = |shard: &ShardDesc| GroupDesc {
            id: 1,
            epoch: 1,
            shards: vec![shard.clone()],
            replicas: vec![ReplicaDesc { id: 1, node_id: 1, ..Default::default() }],
            ..Default::default()
        };
        let sample = |shard: &ShardDesc, term, seconds: u64, writes| GroupStats {
            group_id: 1,
            shard_count: 1,
            sample: Some(GroupStatsSample {
                group_epoch: 1,
                replica_id: 1,
                term,
                elapsed_nanos: 1_000_000_000 * seconds,
                ..Default::default()
            }),
            shard_stats: vec![ShardStats {
                shard_id: shard.id,
                table_id: shard.table_id,
                range: shard.range.clone(),
                traffic: Some(ShardTraffic { writes, ..Default::default() }),
                ..Default::default()
            }],
            ..Default::default()
        };
        let age = Duration::from_secs(30);
        let quiet = Duration::from_secs(20);
        let mut stats = ShowStats::with_merge_quiet_tracking(10., age);
        stats.update(&sample(&shard, 1, 1, 0), 1);
        stats.groups.get_mut(&1).unwrap().received_at -= quiet;
        assert!(!stats.snapshot(&[group(&shard)], age).shard_is_quiet(1, 1, quiet));
        stats.update(&sample(&shard, 1, 21, 5), 1);
        assert!(stats.snapshot(&[group(&shard)], age).shard_is_quiet(1, 1, quiet));
        stats.update(&sample(&shard, 1, 22, 15), 1);
        assert!(!stats.snapshot(&[group(&shard)], age).shard_is_quiet(1, 1, Duration::ZERO));

        stats.groups.get_mut(&1).unwrap().received_at -= age + Duration::from_secs(1);
        stats.update(&sample(&shard, 1, 53, 15), 1);
        assert!(!stats.snapshot(&[group(&shard)], age).shard_is_quiet(1, 1, Duration::ZERO));

        stats.update(&sample(&shard, 2, 54, 15), 1);
        assert!(!stats.snapshot(&[group(&shard)], age).shard_is_quiet(1, 1, Duration::ZERO));
        shard.range.as_mut().unwrap().end = b"m".to_vec();
        stats.update(&sample(&shard, 2, 55, 15), 1);
        assert!(!stats.snapshot(&[group(&shard)], age).shard_is_quiet(1, 1, Duration::ZERO));
    }

    #[test]
    fn samples_follow_shard_ownership_and_leader_term() {
        let shard = ShardDesc::whole(1, 2);
        let group = |id, epoch| GroupDesc {
            id,
            epoch,
            shards: vec![shard.clone()],
            replicas: vec![ReplicaDesc { id, node_id: id, ..Default::default() }],
            ..Default::default()
        };
        let groups = [group(1, 1), group(2, 2)];
        let sample = |id, epoch, term, sec, reads| GroupStats {
            group_id: id,
            shard_count: 1,
            sample: Some(GroupStatsSample {
                group_epoch: epoch,
                replica_id: id,
                term,
                elapsed_nanos: Duration::from_secs(sec).as_nanos() as u64,
                shard_migration: None,
            }),
            shard_stats: vec![ShardStats {
                shard_id: shard.id,
                table_id: shard.table_id,
                shard_size: 1024 * id,
                traffic: Some(ShardTraffic { reads, read_bytes: 20 * reads, ..Default::default() }),
                range: shard.range.clone(),
            }],
            ..Default::default()
        };
        let table = |stats: &ShowStats| {
            stats.snapshot(&groups, Duration::from_secs(5)).tables(&groups)[&shard.table_id]
        };
        let mut stats = ShowStats::default();
        stats.update(&sample(1, 1, 1, 1, 10), 1);
        assert_eq!(table(&stats).num_shards, 1);
        assert_eq!(table(&stats).bytes, Some(1024));

        stats.update(&sample(2, 2, 1, 1, 10), 2);
        assert_eq!(table(&stats).bytes, Some(2048));
        assert!(table(&stats).rates.is_none());
        stats.update(&sample(2, 2, 1, 3, 11), 2);
        let rates = table(&stats).rates.unwrap();
        assert_eq!(rates.read_qps, 0.5);
        assert_eq!(rates.read_bytes_per_sec, 10.);
        stats.update(&sample(1, 1, 1, 4, 100), 1);
        assert_eq!(table(&stats).bytes, Some(2048));
        assert_eq!(table(&stats).rates.unwrap().read_qps, 0.5);

        stats.update(&sample(2, 2, 2, 4, 11), 2);
        assert!(!stats.update(&sample(2, 2, 1, 5, 100), 2));
        assert_eq!(table(&stats).rates.unwrap().read_qps, 0.5);
        stats.update(&sample(2, 2, 2, 6, 11), 2);
        assert_eq!(table(&stats).rates.unwrap().read_qps, 0.);

        stats.groups.get_mut(&2).unwrap().received_at -= Duration::from_secs(6);
        let snapshot = stats.snapshot(&groups, Duration::from_secs(5));
        assert!(snapshot.leader(2).is_none());
        assert!(!snapshot.migration_idle(2));
        let metrics = snapshot.tables(&groups)[&shard.table_id];
        assert_eq!(metrics.bytes, Some(2048));
        assert_eq!(metrics.rates.unwrap().read_qps, 0.);
    }

    #[test]
    fn shard_estimates_and_rate_baselines_survive_a_split() {
        let mut group = GroupDesc {
            id: 1,
            epoch: 1,
            replicas: vec![ReplicaDesc { id: 1, node_id: 1, ..Default::default() }],
            shards: vec![
                ShardDesc::with_range(1, 2, vec![], b"m".to_vec()),
                ShardDesc::with_range(2, 2, b"m".to_vec(), vec![]),
            ],
            ..Default::default()
        };
        let sample = |group: &GroupDesc, second: u64, reads: u64| GroupStats {
            group_id: group.id,
            shard_count: group.shards.len() as u64,
            sample: Some(GroupStatsSample {
                group_epoch: group.epoch,
                replica_id: 1,
                term: 1,
                elapsed_nanos: second * 1_000_000_000,
                shard_migration: None,
            }),
            shard_stats: group
                .shards
                .iter()
                .map(|shard| ShardStats {
                    shard_id: shard.id,
                    table_id: shard.table_id,
                    shard_size: 1024,
                    traffic: Some(ShardTraffic {
                        reads,
                        read_bytes: reads * 20,
                        ..Default::default()
                    }),
                    range: shard.range.clone(),
                })
                .collect(),
            ..Default::default()
        };
        let age = Duration::from_secs(30);
        let mut stats = ShowStats::default();
        stats.update(&sample(&group, 1, 10), 1);
        stats.update(&sample(&group, 2, 12), 1);

        let before_split = group.clone();
        group.epoch = Epoch(group.epoch).apply_shard_delta().0;
        group.shards[1].range.as_mut().unwrap().end = b"t".to_vec();
        group.shards.push(ShardDesc::with_range(3, 2, b"t".to_vec(), vec![]));
        let before = stats.snapshot(std::slice::from_ref(&group), age);
        assert!(before.migration_idle(group.id));
        assert_eq!(before.shard(2).bytes, Some(1024));
        assert_eq!(before.shard(2).rates.unwrap().read_qps, 2.);
        assert!(before.shard(3).bytes.is_none());
        let group_before = before.group(&group);
        assert_eq!(group_before.num_shards, 3);
        assert_eq!(group_before.bytes, Some(2048));
        assert_eq!(group_before.rates.unwrap().read_qps, 4.);

        let mut split = sample(&group, 3, 18);
        split.shard_stats[1].shard_size = 512;
        split.shard_stats[2].shard_size = 512;
        stats.update(&split, 1);
        stats.retain(&[before_split]);
        let after = stats.snapshot(std::slice::from_ref(&group), age);
        assert_eq!(after.shard(1).rates.unwrap().read_qps, 6.);
        assert_eq!(after.shard(2).bytes, Some(512));
        assert_eq!(after.shard(2).rates.unwrap().read_qps, 6.);
        assert_eq!(after.shard(3).bytes, Some(512));
        assert!(after.shard(3).rates.is_none());
        assert_eq!(after.group(&group).bytes, Some(2048));
        assert_eq!(after.group(&group).rates.unwrap().read_qps, 4.);

        stats.update(&sample(&group, 4, 21), 1);
        group.epoch = Epoch(group.epoch).apply_config_delta().0;
        let snapshot = stats.snapshot(std::slice::from_ref(&group), age);
        assert!(!snapshot.migration_idle(group.id));
        assert_eq!(snapshot.group(&group).rates.unwrap().read_qps, 9.);
    }
}
