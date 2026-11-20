// Copyright 2023-present The Sekas Authors.
// Copyright 2022 The Engula Authors.
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
use std::sync::Arc;

use log::debug;
use sekas_api::server::v1::{GroupDesc, ShardDesc};

use super::{
    AllocSource, BalanceDecision, BalanceStatus, CountBalance, ReallocateShard, ShardAction,
};
use crate::Result;
use crate::constants::ROOT_GROUP_ID;

pub struct ShardCountPolicy<T: AllocSource> {
    alloc_source: Arc<T>,
}

impl<T: AllocSource> ShardCountPolicy<T> {
    pub fn with(alloc_source: Arc<T>) -> Self {
        Self { alloc_source }
    }

    pub fn allocate_shard(&self, n: usize, required_replicas: usize) -> Result<Vec<GroupDesc>> {
        let groups = self.current_user_groups();
        Ok(select_groups_for_shard(groups, n, required_replicas))
    }

    pub fn compute_balance(&self) -> (Vec<ShardAction>, BalanceDecision) {
        let mut candidates: Vec<_> = self
            .current_user_groups()
            .into_iter()
            .map(|group| {
                let count = group.shards.len() as u64;
                (group, count)
            })
            .collect();
        let balance = CountBalance::new(&candidates, |_| 0.5);
        candidates.sort_by_key(|(_, count)| Reverse(*count));
        debug!(
            "group ranked by shard count. mean={}, scored_groups={:?}",
            balance.mean,
            candidates
                .iter()
                .map(|(g, count)| (g.id, count, balance.classify(*count)))
                .collect::<Vec<_>>(),
        );
        for (src_group, count) in &candidates {
            if balance.classify(*count) != BalanceStatus::Overfull {
                break;
            }
            if let Some(action) = self.rebalance_target(src_group, &candidates, &balance) {
                let decision = BalanceDecision::ActionNeeded(format!(
                    "move shard {}: g{} -> g{}",
                    action.shard, action.source_group, action.target_group
                ));
                return (vec![ShardAction::Migrate(action)], decision);
            }
        }

        (vec![], balance.no_action("user_groups", &candidates))
    }

    fn rebalance_target(
        &self,
        source_group: &GroupDesc,
        ranked_candidates: &[(GroupDesc, u64)],
        balance: &CountBalance,
    ) -> Option<ReallocateShard> {
        for (target, count) in ranked_candidates.iter().rev() {
            if balance.classify(*count) != BalanceStatus::Underfull {
                break;
            }
            if balance.classify(count + 1) == BalanceStatus::Overfull {
                continue;
            }
            let source_shard = self.preferred_remove_shard(source_group, target)?;
            return Some(ReallocateShard {
                shard: source_shard.id,
                source_group: source_group.id,
                target_group: target.id,
            });
        }
        debug!("skip balance group:{} due to no suitable target", source_group.id);
        None
    }

    fn preferred_remove_shard(
        &self,
        src_group: &GroupDesc,
        _target_group: &GroupDesc,
    ) -> Option<ShardDesc> {
        let replicas = src_group.shards.to_owned();
        // TODO: ranking shards and choose the preferred one
        replicas.first().map(ToOwned::to_owned)
    }

    fn current_user_groups(&self) -> Vec<GroupDesc> {
        let groups = self.alloc_source.groups();
        groups.values().filter(|g| g.id != ROOT_GROUP_ID).map(ToOwned::to_owned).collect()
    }
}

fn select_groups_for_shard(
    mut groups: Vec<GroupDesc>,
    n: usize,
    required_replicas: usize,
) -> Vec<GroupDesc> {
    groups.retain(|group| group.replicas.len() == required_replicas);
    groups.sort_by(|g1, g2| g1.shards.len().cmp(&g2.shards.len()));
    groups.into_iter().take(n).collect()
}

#[cfg(test)]
mod tests {
    use sekas_api::server::v1::ReplicaDesc;

    use super::*;

    fn group(id: u64, replicas: usize, shards: usize) -> GroupDesc {
        GroupDesc {
            id,
            replicas: (0..replicas).map(|_| ReplicaDesc::default()).collect(),
            shards: (0..shards).map(|_| ShardDesc::default()).collect(),
            ..Default::default()
        }
    }

    #[test]
    fn select_groups_for_shard_requires_exact_replica_count() {
        let groups = vec![group(1, 1, 0), group(2, 3, 2), group(3, 3, 1), group(4, 4, 0)];

        let selected = select_groups_for_shard(groups, 2, 3);

        assert_eq!(selected.iter().map(|group| group.id).collect::<Vec<_>>(), vec![3, 2]);
    }
}
