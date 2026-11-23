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

use std::cmp::Reverse;
use std::collections::{HashMap, HashSet};
use std::time::Duration;

use sekas_api::server::v1::GroupDesc;

use super::{BalanceDecision, BalanceThresholds, ReallocateShard, ShardSizePlacement};
use crate::root::ClusterStats;
use crate::root::stats::ShowSnapshot;

pub(super) struct ShardSizeBalance {
    sizes: ShowSnapshot,
    totals: HashMap<u64, u128>,
    mean: u128,
    tolerance: u128,
    move_size_limit: u64,
}

impl ShardSizeBalance {
    pub fn thresholds(&self) -> BalanceThresholds {
        let max = self.totals.values().copied().max().unwrap_or(0);
        BalanceThresholds::new(self.mean as f64, self.tolerance as f64, max as f64)
    }

    pub fn new(
        groups: &[GroupDesc],
        stats: &ClusterStats,
        max_age: Duration,
        min_tolerance: u64,
        move_size_limit: u64,
    ) -> Result<Self, BalanceDecision> {
        let sizes = stats.show_snapshot(groups, max_age);
        if sizes.migration_in_progress
            && let Some(reason) = stats.shard_migration_wait(&sizes.active_migration_groups())
        {
            return Err(BalanceDecision::Deferred(reason.into()));
        }
        let mut totals = HashMap::with_capacity(groups.len());
        let mut seen = HashSet::new();
        for group in groups {
            let mut total = 0;
            for shard in &group.shards {
                if !seen.insert(shard.id) {
                    return Err(BalanceDecision::Deferred(
                        "waiting for shard ownership to settle".into(),
                    ));
                }
                let Some(bytes) = sizes.shard(group.id, shard.id).bytes else {
                    return Err(BalanceDecision::Deferred(
                        "waiting for current shard size samples".into(),
                    ));
                };
                total += u128::from(bytes);
            }
            totals.insert(group.id, total);
        }
        let mean = totals.values().sum::<u128>() / groups.len().max(1) as u128;
        let tolerance = (mean / 10).max(u128::from(min_tolerance));
        Ok(Self { sizes, totals, mean, tolerance, move_size_limit })
    }

    pub fn compute_balance(
        &self,
        groups: &[GroupDesc],
        stats: &ClusterStats,
        allow_split: bool,
    ) -> (Option<ReallocateShard>, BalanceDecision) {
        let Some((&target, &min)) = self
            .totals
            .iter()
            .filter(|&(&id, _)| self.sizes.migration_idle(id))
            .min_by_key(|&(&id, &bytes)| (bytes, id))
        else {
            return (
                None,
                BalanceDecision::Deferred(self.sizes.migration_report_wait(groups).into()),
            );
        };
        let max = self.totals.values().copied().max().unwrap_or(min);
        let mut best_move = None;
        let mut best_score = (false, 0, Reverse(u64::MAX), Reverse(u64::MAX), Reverse(u64::MAX));
        for group in groups
            .iter()
            .filter(|group| self.totals[&group.id] > min && self.sizes.migration_idle(group.id))
        {
            // The penalty is convex, so adding a shard to the lightest group
            // costs no more than adding the same shard to any other group.
            for shard in &group.shards {
                let Some(size) = self.sizes.shard(group.id, shard.id).bytes else { continue };
                if stats.is_shard_scheduled(shard.id) {
                    continue;
                }
                let direct = size <= self.move_size_limit;
                if !direct && (!allow_split || stats.is_shard_split_cooling_down(shard.id)) {
                    continue;
                }
                let bytes = self.projected_move_size(group.id, target, size);
                let improvement = self.move_improvement(group.id, target, bytes);
                if improvement == 0 {
                    continue;
                }
                let score =
                    (direct, improvement, Reverse(bytes), Reverse(group.id), Reverse(shard.id));
                if score > best_score {
                    best_score = score;
                    best_move = Some((
                        ReallocateShard {
                            shard: shard.id,
                            source_group: group.id,
                            target_group: target,
                        },
                        bytes,
                        direct,
                    ));
                }
            }
        }
        let smallest = self.totals.values().copied().min().unwrap_or(min);
        let summary = format!(
            "user_groups={}, mean_bytes={}, tolerance_bytes={}\nrange_bytes={smallest}..{max}",
            groups.len(),
            self.mean,
            self.tolerance
        );
        match best_move {
            Some((action, bytes, direct)) => {
                let detail = if direct {
                    format!(
                        "move shard {} ({bytes} bytes): g{} -> g{}",
                        action.shard, action.source_group, action.target_group
                    )
                } else {
                    format!(
                        "split shard {} in g{} before moving up to {bytes} bytes to g{}",
                        action.shard, action.source_group, action.target_group
                    )
                };
                let decision = BalanceDecision::ActionNeeded(format!("{detail}\n{summary}"));
                (Some(action), decision)
            }
            None => {
                let within_tolerance = self.totals.values().all(|&bytes| self.penalty(bytes) == 0);
                if !within_tolerance && !self.sizes.has_current_migration_status {
                    return (
                        None,
                        BalanceDecision::Deferred(self.sizes.migration_report_wait(groups).into()),
                    );
                }
                let reason = if within_tolerance {
                    "within byte tolerance"
                } else {
                    "no eligible move improves byte balance"
                };
                (None, BalanceDecision::NoAction(format!("{reason}\n{summary}")))
            }
        }
    }

    pub fn preserves_balance(&self, source: u64, target: u64, shard: u64) -> bool {
        self.sizes.migration_idle(source)
            && self.sizes.migration_idle(target)
            && self.totals.contains_key(&target)
            && self
                .move_bytes(source, shard)
                .is_some_and(|bytes| self.preserves_group_penalties(source, target, bytes))
    }

    pub fn validate_placement(
        &self,
        action: &ReallocateShard,
        require_improvement: bool,
        allow_split: bool,
    ) -> Option<ShardSizePlacement> {
        let size = self.sizes.shard(action.source_group, action.shard).bytes?;
        if !self.sizes.migration_idle(action.source_group)
            || !self.sizes.migration_idle(action.target_group)
            || !self.totals.contains_key(&action.target_group)
            || (size > self.move_size_limit && !(require_improvement && allow_split))
        {
            return None;
        }
        let bytes = self.projected_move_size(action.source_group, action.target_group, size);
        let placement = if size <= self.move_size_limit {
            ShardSizePlacement::Move
        } else {
            ShardSizePlacement::Split(bytes)
        };
        let accepted = if require_improvement {
            self.move_improvement(action.source_group, action.target_group, bytes) > 0
        } else {
            self.preserves_group_penalties(action.source_group, action.target_group, bytes)
        };
        accepted.then_some(placement)
    }

    fn projected_move_size(&self, source: u64, target: u64, shard_size: u64) -> u64 {
        if shard_size <= self.move_size_limit {
            return shard_size;
        }
        let difference = self.totals[&source].saturating_sub(self.totals[&target]);
        (difference / 2).min(u128::from(self.move_size_limit)) as u64
    }

    fn move_bytes(&self, group: u64, shard: u64) -> Option<u64> {
        self.sizes.shard(group, shard).bytes.filter(|bytes| *bytes <= self.move_size_limit)
    }

    fn move_penalties(&self, source: u64, target: u64, bytes: u64) -> ([u128; 2], [u128; 2]) {
        let source = self.totals[&source];
        let target = self.totals[&target];
        let bytes = u128::from(bytes);
        (
            [self.penalty(source), self.penalty(target)],
            [self.penalty(source - bytes), self.penalty(target + bytes)],
        )
    }

    fn move_improvement(&self, source: u64, target: u64, bytes: u64) -> u128 {
        let (before, after) = self.move_penalties(source, target, bytes);
        before.into_iter().sum::<u128>().saturating_sub(after.into_iter().sum())
    }

    fn preserves_group_penalties(&self, source: u64, target: u64, bytes: u64) -> bool {
        let (before, after) = self.move_penalties(source, target, bytes);
        before.into_iter().zip(after).all(|(before, after)| after <= before)
    }

    fn penalty(&self, bytes: u128) -> u128 {
        bytes.abs_diff(self.mean).saturating_sub(self.tolerance)
    }
}
