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

use std::collections::HashMap;
use std::time::{Duration, Instant};

use sekas_api::server::v1::{
    GroupDesc, GroupStats, GroupStatsSample, ShardDesc, ShardStats, ShardTraffic,
};

#[derive(Default)]
pub(super) struct ShowStats {
    groups: HashMap<u64, GroupSample>,
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

#[derive(Default, Clone, Copy)]
pub(in crate::root) struct TrafficRates {
    pub read_qps: f64,
    pub write_qps: f64,
    pub read_bytes_per_sec: f64,
    pub write_bytes_per_sec: f64,
}

#[derive(Clone, Copy)]
pub(in crate::root) struct ShowMetrics {
    pub num_shards: usize,
    pub bytes: Option<u64>,
    pub rates: Option<TrafficRates>,
}

pub(in crate::root) struct ShowSnapshot {
    shards: HashMap<(u64, u64), ShowMetrics>,
}

impl ShowStats {
    pub(super) fn update(&mut self, stats: &GroupStats, node_id: u64) {
        let previous = self.groups.get(&stats.group_id);
        let mut interval = None;
        if let (Some(old), Some(new)) = (previous.and_then(|s| s.metadata.as_ref()), &stats.sample)
        {
            if new.term < old.term || new.group_epoch < old.group_epoch {
                return;
            }
            if new.term == old.term
                && new.replica_id == old.replica_id
                && new.group_epoch == old.group_epoch
            {
                if new.elapsed_nanos <= old.elapsed_nanos {
                    return;
                }
                interval = Some(Duration::from_nanos(new.elapsed_nanos - old.elapsed_nanos));
            }
        }

        let shards = stats
            .shard_stats
            .iter()
            .map(|stats| {
                let rates = interval.and_then(|interval| {
                    let past = previous?.shards.get(&stats.shard_id)?;
                    if past.stats.table_id != stats.table_id {
                        return None;
                    }
                    TrafficRates::between(past.stats.traffic?, stats.traffic?, interval)
                });
                (stats.shard_id, ShardSample { stats: *stats, rates })
            })
            .collect();
        self.groups.insert(
            stats.group_id,
            GroupSample { metadata: stats.sample, node_id, received_at: Instant::now(), shards },
        );
    }

    pub(super) fn snapshot(&self, groups: &[GroupDesc], max_age: Duration) -> ShowSnapshot {
        let now = Instant::now();
        let mut shards = HashMap::new();
        for group in groups {
            let Some(report) = self.groups.get(&group.id) else { continue };
            if now.duration_since(report.received_at) > max_age || !report.matches(group) {
                continue;
            }
            for shard in &group.shards {
                if let Some(sample) = report.shards.get(&shard.id)
                    && sample.stats.table_id == shard.table_id
                {
                    shards.insert(
                        (group.id, shard.id),
                        ShowMetrics {
                            num_shards: 1,
                            bytes: Some(sample.stats.shard_size),
                            rates: sample.rates,
                        },
                    );
                }
            }
        }
        ShowSnapshot { shards }
    }

    pub(super) fn clear(&mut self) {
        self.groups.clear();
    }
}

impl GroupSample {
    fn matches(&self, group: &GroupDesc) -> bool {
        if let Some(sample) = &self.metadata {
            return sample.group_epoch == group.epoch
                && group.replicas.iter().any(|r| r.id == sample.replica_id);
        }
        // Older replicas still report approximate sizes.
        group.replicas.iter().any(|r| r.node_id == self.node_id)
    }
}

impl TrafficRates {
    fn between(old: ShardTraffic, new: ShardTraffic, interval: Duration) -> Option<Self> {
        let seconds = interval.as_secs_f64();
        Some(Self {
            read_qps: new.reads.checked_sub(old.reads)? as f64 / seconds,
            write_qps: new.writes.checked_sub(old.writes)? as f64 / seconds,
            read_bytes_per_sec: new.read_bytes.checked_sub(old.read_bytes)? as f64 / seconds,
            write_bytes_per_sec: new.write_bytes.checked_sub(old.write_bytes)? as f64 / seconds,
        })
    }

    fn add(self, other: Self) -> Self {
        Self {
            read_qps: self.read_qps + other.read_qps,
            write_qps: self.write_qps + other.write_qps,
            read_bytes_per_sec: self.read_bytes_per_sec + other.read_bytes_per_sec,
            write_bytes_per_sec: self.write_bytes_per_sec + other.write_bytes_per_sec,
        }
    }
}

impl Default for ShowMetrics {
    fn default() -> Self {
        Self { num_shards: 0, bytes: Some(0), rates: Some(TrafficRates::default()) }
    }
}

impl ShowMetrics {
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
    pub(in crate::root) fn shard(&self, group_id: u64, shard_id: u64) -> ShowMetrics {
        self.shards.get(&(group_id, shard_id)).copied().unwrap_or(ShowMetrics::unavailable(1))
    }

    pub(in crate::root) fn group(&self, group: &GroupDesc) -> ShowMetrics {
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
            }),
            shard_stats: vec![ShardStats {
                shard_id: shard.id,
                table_id: shard.table_id,
                shard_size: 1024 * id,
                traffic: Some(ShardTraffic { reads, read_bytes: 20 * reads, ..Default::default() }),
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
        stats.update(&sample(2, 2, 2, 6, 11), 2);
        assert_eq!(table(&stats).rates.unwrap().read_qps, 0.);

        stats.groups.get_mut(&2).unwrap().received_at -= Duration::from_secs(6);
        assert_eq!(table(&stats).bytes, None);
        assert!(table(&stats).rates.is_none());
    }
}
