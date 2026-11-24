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
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use sekas_api::server::v1::{NodeDesc, ReplicaDesc};

use super::source::NodeFilter;
use super::{
    AllocSource, BalanceDecision, BalanceStatus, BalanceThresholds, CountBalance,
    ReallocateReplica, ReplicaAction,
};
use crate::Result;
use crate::constants::{REPLICA_PER_GROUP, ROOT_GROUP_ID};
use crate::root::ClusterStats;

pub struct ReplicaCountPolicy<T: AllocSource> {
    alloc_source: Arc<T>,
    cluster_stats: Arc<ClusterStats>,
}

impl<T: AllocSource> ReplicaCountPolicy<T> {
    pub fn with(alloc_source: Arc<T>, cluster_stats: Arc<ClusterStats>) -> Self {
        Self { alloc_source, cluster_stats }
    }

    pub fn allocate_group_replica(
        &self,
        existing_replica_nodes: Vec<u64>,
        wanted_count: usize,
    ) -> Result<Vec<NodeDesc>> {
        let mut candidate_nodes = self.alloc_source.nodes(NodeFilter::Schedulable);

        // skip the nodes already have group replicas.
        candidate_nodes.retain(|n| !existing_replica_nodes.contains(&n.id));

        // sort by alloc score
        candidate_nodes.sort_by(|n1, n2| {
            self.node_alloc_score(n2).partial_cmp(&self.node_alloc_score(n1)).unwrap()
        });

        Ok(candidate_nodes.into_iter().take(wanted_count).collect())
    }

    pub fn compute_balance(&self) -> (Vec<ReplicaAction>, BalanceDecision, BalanceThresholds) {
        let mut candidates: Vec<_> = self
            .alloc_source
            .nodes(NodeFilter::Schedulable)
            .into_iter()
            .map(|node| {
                let count = self.node_replica_count(&node);
                (node, count)
            })
            .collect();
        let balance = CountBalance::new(&candidates, |mean| (mean * 0.05).max(2.0));
        candidates.sort_by_key(|(_, count)| Reverse(*count));
        let thresholds = balance.thresholds(&candidates);
        tracing::debug!(
            scored_nodes = ?candidates.iter().map(|(n, count)| (n.id, count, balance.classify(*count))).collect::<Vec<_>>(),
            mean = balance.mean,
            "node ranked by replica count",
        );
        for (src_node, count) in &candidates {
            if balance.classify(*count) != BalanceStatus::Overfull {
                break;
            }
            if let Some(action) = self.rebalance_target(src_node, &candidates, &balance) {
                let decision = BalanceDecision::ActionNeeded {
                    detail: format!(
                        "move replica {} of group {}: n{} -> n{}",
                        action.source_replica,
                        action.group,
                        action.source_node,
                        action.target_node.id
                    ),
                    reason: format!(
                        "Source node has {count} replicas, above the upper threshold of {}",
                        thresholds.high
                    ),
                };
                return (vec![ReplicaAction::Migrate(action)], decision, thresholds);
            }
        }

        (Vec::new(), balance.no_action("schedulable_nodes", &candidates), thresholds)
    }

    fn rebalance_target(
        &self,
        src: &NodeDesc,
        ranked_nodes: &[(NodeDesc, u64)],
        balance: &CountBalance,
    ) -> Option<ReallocateReplica> {
        let mut groups = self
            .alloc_source
            .groups()
            .into_iter()
            .map(|(group, desc)| {
                (group, desc.replicas.iter().map(|r| r.node_id).collect::<HashSet<u64>>())
            })
            .collect::<HashMap<_, _>>();

        let replica_states = self.alloc_source.replica_states();
        for replica_state in replica_states {
            if let Some(g) = groups.get_mut(&replica_state.group_id) {
                g.insert(replica_state.node_id);
            }
        }

        for (target, count) in ranked_nodes.iter().rev() {
            if balance.classify(*count) != BalanceStatus::Underfull {
                break;
            }
            if balance.classify(count + 1) == BalanceStatus::Overfull {
                continue;
            }
            let (source_replica, group) = self.preferred_remove_replica(src, target, &groups)?;
            return Some(ReallocateReplica {
                group,
                source_node: source_replica.node_id,
                source_replica: source_replica.id,
                target_node: target.to_owned(),
            });
        }
        None
    }

    fn preferred_remove_replica(
        &self,
        src: &NodeDesc,
        target: &NodeDesc,
        group_nodes: &HashMap<u64, HashSet<u64>>,
    ) -> Option<(ReplicaDesc, u64)> {
        // TODO: sort & rank replica
        self.alloc_source.node_replicas(&src.id).into_iter().find(|(_, g)| {
            if *g == ROOT_GROUP_ID {
                return false;
            }
            if let Some(exist_nodes) = group_nodes.get(g) {
                if exist_nodes.len() < REPLICA_PER_GROUP {
                    return false;
                }
                if !exist_nodes.contains(&target.id) {
                    return true;
                }
            }
            false
        })
    }

    fn node_alloc_score(&self, n: &NodeDesc) -> f64 {
        // TODO: add more rule to calculate score.
        -(self.node_replica_count(n) as f64)
    }

    fn node_replica_count(&self, n: &NodeDesc) -> u64 {
        let mut cnt = n.capacity.as_ref().unwrap().replica_count as i64;
        let delta = self.cluster_stats.get_node_delta(n.id);
        cnt += delta.replica_count;
        if cnt < 0 {
            cnt = 0;
        }
        cnt as u64
    }
}
