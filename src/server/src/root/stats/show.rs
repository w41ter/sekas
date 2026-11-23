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
use std::time::{Duration, Instant};

use sekas_api::Epoch;
use sekas_api::server::v1::{
    GroupDesc, GroupStats, GroupStatsSample, ShardDesc, ShardStats, ShardTraffic,
};

#[derive(Default)]
pub(super) struct ShowStats {
    groups: HashMap<u64, GroupSample>,
    displayed_shards: HashMap<u64, DisplayedShard>,
    displayed_groups: HashMap<u64, ShowMetrics>,
}

struct DisplayedShard {
    table_id: u64,
    epoch: u64,
    metrics: ShowMetrics,
}

struct GroupSample {
    metadata: Option<GroupStatsSample>,
    node_id: u64,
    received_at: Instant,
    shards: HashMap<u64, ShardSample>,
}

struct ShardSample {
    stats: ShardStats,
    rates: Option<TrafficRates>,
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
    shards: HashMap<(u64, u64), ShowMetrics>,
    displayed_groups: HashMap<u64, ShowMetrics>,
    migration_states: HashMap<u64, bool>,
    migration_report_issues: HashMap<u64, String>,
    pub migration_in_progress: bool,
    pub has_current_migration_status: bool,
    leaders: HashMap<u64, (u64, u64)>,
}

impl ShowStats {
    pub(super) fn update(&mut self, stats: &GroupStats, node_id: u64) -> bool {
        let previous = self.groups.get(&stats.group_id);
        let mut interval = None;
        if let (Some(old), Some(new)) = (previous.and_then(|s| s.metadata.as_ref()), &stats.sample)
        {
            if new.term < old.term || new.group_epoch < old.group_epoch {
                return false;
            }
            if new.term == old.term && new.replica_id == old.replica_id {
                let same_epoch = new.group_epoch == old.group_epoch;
                if new.elapsed_nanos > old.elapsed_nanos {
                    interval = Some((
                        Duration::from_nanos(new.elapsed_nanos - old.elapsed_nanos),
                        same_epoch,
                    ));
                } else if same_epoch {
                    return false;
                }
            }
        }

        let shards: HashMap<_, _> = stats
            .shard_stats
            .iter()
            .map(|stats| {
                let rates = interval.and_then(|(interval, same_epoch)| {
                    let past = previous?.shards.get(&stats.shard_id)?;
                    let same_range = match (&past.stats.range, &stats.range) {
                        (Some(old), Some(new)) => old == new,
                        _ => same_epoch,
                    };
                    if past.stats.table_id != stats.table_id || !same_range {
                        return None;
                    }
                    TrafficRates::between(past.stats.traffic?, stats.traffic?, interval)
                });
                (stats.shard_id, ShardSample { stats: stats.clone(), rates })
            })
            .collect();
        let epoch = stats.sample.map_or(0, |sample| sample.group_epoch);
        for (&id, sample) in &shards {
            let mut metrics = sample.metrics();
            if let Some(last) = self.displayed_shards.get(&id)
                && last.table_id == sample.stats.table_id
            {
                if last.epoch > epoch {
                    continue;
                }
                metrics.fill_missing(last.metrics);
            }
            self.displayed_shards
                .insert(id, DisplayedShard { table_id: sample.stats.table_id, epoch, metrics });
        }
        if shards.len() as u64 == stats.shard_count {
            let mut metrics = ShowMetrics::default();
            for sample in shards.values() {
                metrics.add(sample.metrics());
            }
            self.remember_group(stats.group_id, &mut metrics);
        }
        self.groups.insert(
            stats.group_id,
            GroupSample { metadata: stats.sample, node_id, received_at: Instant::now(), shards },
        );
        true
    }

    pub(super) fn snapshot(&self, groups: &[GroupDesc], max_age: Duration) -> ShowSnapshot {
        let now = Instant::now();
        let mut shards = HashMap::new();
        let mut migration_states = HashMap::new();
        let mut migration_report_issues = HashMap::new();
        let mut leaders = HashMap::new();
        for group in groups {
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
                let reason = if let Some(sample) = report.metadata
                    && Epoch(sample.group_epoch).config_epoch() != Epoch(group.epoch).config_epoch()
                {
                    format!(
                        "reported configuration {}, current configuration {}",
                        Epoch(sample.group_epoch).config_epoch(),
                        Epoch(group.epoch).config_epoch()
                    )
                } else {
                    "reporting replica is outside the current configuration".to_owned()
                };
                migration_report_issues.insert(group.id, reason);
                continue;
            }
            if let Some(active) =
                report.metadata.and_then(|sample| sample.shard_migration_in_progress)
            {
                migration_states.insert(group.id, active);
            } else {
                migration_report_issues
                    .insert(group.id, "leader report omitted migration status".to_owned());
            }
            if let Some(metadata) = report.metadata {
                leaders.insert(group.id, (metadata.term, report.node_id));
            }
            for shard in &group.shards {
                if let Some(sample) = report.shards.get(&shard.id)
                    && sample.stats.table_id == shard.table_id
                    && match &sample.stats.range {
                        Some(range) => shard.range.as_ref() == Some(range),
                        None => {
                            report.metadata.is_none_or(|sample| sample.group_epoch == group.epoch)
                        }
                    }
                {
                    shards.insert((group.id, shard.id), sample.metrics());
                }
            }
        }
        ShowSnapshot {
            shards,
            displayed_groups: HashMap::new(),
            migration_in_progress: migration_states.values().any(|&active| active),
            has_current_migration_status: migration_states.len() == groups.len(),
            migration_states,
            migration_report_issues,
            leaders,
        }
    }

    pub(super) fn display_snapshot(
        &mut self,
        groups: &[GroupDesc],
        max_age: Duration,
    ) -> ShowSnapshot {
        let mut snapshot = self.snapshot(groups, max_age);
        for group in groups {
            for shard in &group.shards {
                if let Some(last) = self.displayed_shards.get(&shard.id)
                    && last.table_id == shard.table_id
                {
                    snapshot
                        .shards
                        .entry((group.id, shard.id))
                        .or_insert_with(|| ShowMetrics::unavailable(1))
                        .fill_missing(last.metrics);
                }
            }
            let mut metrics = snapshot.group(group);
            self.remember_group(group.id, &mut metrics);
            snapshot.displayed_groups.insert(group.id, metrics);
        }
        snapshot
    }

    fn remember_group(&mut self, group: u64, metrics: &mut ShowMetrics) {
        if metrics.num_shards == 0 {
            self.displayed_groups.remove(&group);
            return;
        }
        let cached =
            self.displayed_groups.entry(group).or_insert_with(|| ShowMetrics::unavailable(0));
        metrics.fill_missing(*cached);
        *cached = *metrics;
    }

    pub(super) fn retain(&mut self, groups: &[GroupDesc]) {
        let groups_alive: HashSet<_> = groups.iter().map(|group| group.id).collect();
        let shards_alive: HashSet<_> = groups
            .iter()
            .flat_map(|group| group.shards.iter().map(|shard| (shard.id, shard.table_id)))
            .collect();
        self.groups.retain(|group, _| groups_alive.contains(group));
        self.displayed_groups.retain(|group, _| groups_alive.contains(group));
        self.displayed_shards
            .retain(|&shard, sample| shards_alive.contains(&(shard, sample.table_id)));
    }

    pub(super) fn clear(&mut self) {
        self.groups.clear();
        self.displayed_shards.clear();
        self.displayed_groups.clear();
    }
}

impl ShardSample {
    fn metrics(&self) -> ShowMetrics {
        ShowMetrics { num_shards: 1, bytes: Some(self.stats.shard_size), rates: self.rates }
    }
}

impl GroupSample {
    fn matches_reporter(&self, group: &GroupDesc) -> bool {
        if let Some(sample) = &self.metadata {
            // A split or merge changes only the shard epoch. Verify the
            // reporting replica and its configuration separately from ranges.
            return Epoch(sample.group_epoch).config_epoch() == Epoch(group.epoch).config_epoch()
                && group.replicas.iter().any(|r| r.id == sample.replica_id);
        }
        // Older replicas still report approximate sizes.
        group.replicas.iter().any(|r| r.node_id == self.node_id)
    }
}

impl TrafficRates {
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

    fn add(self, other: Self) -> Self {
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
    pub(in crate::root) fn migration_report_issue(&self, group: u64) -> Option<&str> {
        self.migration_report_issues.get(&group).map(String::as_str)
    }

    pub(in crate::root) fn migration_report_wait(&self, groups: &[GroupDesc]) -> String {
        let issues = groups
            .iter()
            .filter_map(|group| {
                Some(format!("g{} {}", group.id, self.migration_report_issue(group.id)?))
            })
            .collect::<Vec<_>>()
            .join(", ");
        format!("Waiting for current shard migration status\n{issues}")
    }

    pub(in crate::root) fn active_migration_groups(&self) -> Vec<u64> {
        let mut groups: Vec<_> = self
            .migration_states
            .iter()
            .filter_map(|(&group, &active)| active.then_some(group))
            .collect();
        groups.sort_unstable();
        groups
    }

    pub(in crate::root) fn migration_idle(&self, group_id: u64) -> bool {
        self.migration_states.get(&group_id) == Some(&false)
    }

    pub(in crate::root) fn leader(&self, group_id: u64) -> Option<(u64, u64)> {
        self.leaders.get(&group_id).copied()
    }

    pub(in crate::root) fn shard(&self, group_id: u64, shard_id: u64) -> ShowMetrics {
        self.shards.get(&(group_id, shard_id)).copied().unwrap_or(ShowMetrics::unavailable(1))
    }

    pub(in crate::root) fn group(&self, group: &GroupDesc) -> ShowMetrics {
        if let Some(displayed) = self.displayed_groups.get(&group.id) {
            return *displayed;
        }
        let mut result = ShowMetrics::default();
        for shard in &group.shards {
            result.add(self.shard(group.id, shard.id));
        }
        result
    }

    pub(in crate::root) fn tables(&self, groups: &[GroupDesc]) -> HashMap<u64, ShowMetrics> {
        // During a move both descriptors can contain the same shard. Match the
        // router's preference for the group with the newer descriptor epoch.
        let mut owners = HashMap::<u64, (&GroupDesc, &ShardDesc)>::new();
        for group in groups {
            for shard in &group.shards {
                let entry = owners.entry(shard.id).or_insert((group, shard));
                if group.epoch > entry.0.epoch {
                    *entry = (group, shard);
                }
            }
        }
        let mut tables = HashMap::<u64, ShowMetrics>::new();
        for (group, shard) in owners.into_values() {
            tables.entry(shard.table_id).or_default().add(self.shard(group.id, shard.id));
        }
        tables
    }
}

#[cfg(test)]
mod tests {
    use sekas_api::server::v1::ReplicaDesc;

    use super::*;

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
            sample: Some(GroupStatsSample {
                group_epoch: epoch,
                replica_id: id,
                term,
                elapsed_nanos: Duration::from_secs(sec).as_nanos() as u64,
                shard_migration_in_progress: None,
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
        assert_eq!(table(&stats).bytes, None);

        stats.update(&sample(2, 2, 1, 1, 10), 2);
        assert_eq!(table(&stats).bytes, Some(2048));
        assert!(table(&stats).rates.is_none());
        stats.update(&sample(2, 2, 1, 3, 11), 2);
        let rates = table(&stats).rates.unwrap();
        assert_eq!(rates.read_qps, 0.5);
        assert_eq!(rates.read_bytes_per_sec, 10.);

        stats.update(&sample(2, 2, 2, 4, 11), 2);
        stats.update(&sample(2, 2, 1, 5, 100), 2);
        assert!(table(&stats).rates.is_none());
        let displayed = stats.display_snapshot(&groups, Duration::from_secs(5));
        assert_eq!(displayed.tables(&groups)[&shard.table_id].rates.unwrap().read_qps, 0.5);
        stats.update(&sample(2, 2, 2, 6, 11), 2);
        assert_eq!(table(&stats).rates.unwrap().read_qps, 0.);

        stats.groups.get_mut(&2).unwrap().received_at -= Duration::from_secs(6);
        assert_eq!(table(&stats).bytes, None);
        assert!(table(&stats).rates.is_none());
        let displayed = stats.display_snapshot(&groups, Duration::from_secs(5));
        let metrics = displayed.tables(&groups)[&shard.table_id];
        assert_eq!(metrics.bytes, Some(2048));
        assert_eq!(metrics.rates.unwrap().read_qps, 0.);
    }

    #[test]
    fn unchanged_shards_keep_their_rate_baseline_across_a_split() {
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
                shard_migration_in_progress: Some(false),
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

        group.epoch = Epoch(group.epoch).apply_shard_delta().0;
        group.shards[1].range.as_mut().unwrap().end = b"t".to_vec();
        group.shards.push(ShardDesc::with_range(3, 2, b"t".to_vec(), vec![]));
        let before = stats.snapshot(std::slice::from_ref(&group), age);
        assert!(before.migration_idle(group.id));
        assert_eq!(before.shard(group.id, 1).rates.unwrap().read_qps, 2.);
        assert!(before.shard(group.id, 2).bytes.is_none());
        assert!(before.group(&group).rates.is_none());

        stats.update(&sample(&group, 3, 18), 1);
        let after = stats.snapshot(std::slice::from_ref(&group), age);
        assert_eq!(after.shard(group.id, 1).rates.unwrap().read_qps, 6.);
        assert!(after.shard(group.id, 2).rates.is_none());
        assert!(after.shard(group.id, 3).rates.is_none());
        let displayed = stats.display_snapshot(std::slice::from_ref(&group), age).group(&group);
        assert_eq!(displayed.num_shards, 3);
        assert_eq!(displayed.bytes, Some(3072));
        assert_eq!(displayed.rates.unwrap().read_qps, 4.);

        stats.update(&sample(&group, 4, 21), 1);
        let displayed = stats.display_snapshot(std::slice::from_ref(&group), age).group(&group);
        assert_eq!(displayed.rates.unwrap().read_qps, 9.);

        group.epoch = Epoch(group.epoch).apply_config_delta().0;
        assert!(!stats.snapshot(std::slice::from_ref(&group), age).migration_idle(group.id));
    }
}
