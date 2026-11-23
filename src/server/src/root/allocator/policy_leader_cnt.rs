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
use std::collections::HashMap;
use std::sync::Arc;

use log::debug;
use sekas_api::server::v1::{NodeDesc, RaftRole, ReplicaDesc, ReplicaRole};

use super::source::NodeFilter;
use super::{
    AllocSource, BalanceDecision, BalanceStatus, BalanceThresholds, CountBalance, LeaderAction,
    TransferLeader,
};
use crate::constants::ROOT_GROUP_ID;

pub struct LeaderCountPolicy<T: AllocSource> {
    alloc_source: Arc<T>,
}

impl<T: AllocSource> LeaderCountPolicy<T> {
    pub fn with(alloc_source: Arc<T>) -> Self {
        Self { alloc_source }
    }

    pub fn compute_balance(&self) -> (LeaderAction, BalanceDecision, BalanceThresholds) {
        let mut candidates: Vec<_> = self
            .alloc_source
            .nodes(NodeFilter::Schedulable)
            .into_iter()
            .map(|node| {
                let count = node.capacity.as_ref().unwrap().leader_count;
                (node, count)
            })
            .collect();
        let balance = CountBalance::new(&candidates, |_| 0.5);
        candidates.sort_by_key(|(_, count)| Reverse(*count));
        let thresholds = balance.thresholds(&candidates);
        debug!(
            "node ranked by leader count. mean={}, scored_nodes={:?}",
            balance.mean,
            candidates
                .iter()
                .map(|(n, count)| (n.id, count, balance.classify(*count)))
                .collect::<Vec<_>>(),
        );
        for (n, count) in &candidates {
            if balance.classify(*count) != BalanceStatus::Overfull {
                break;
            }
            if let Some(action) = self.try_transfer_leader(n, &candidates, &balance) {
                let decision = BalanceDecision::ActionNeeded(format!(
                    "transfer leader of group {}: n{} -> n{}",
                    action.group, action.src_node, action.target_node
                ));
                return (LeaderAction::Shed(action), decision, thresholds);
            }
        }
        (LeaderAction::Noop, balance.no_action("schedulable_nodes", &candidates), thresholds)
    }

    fn try_transfer_leader(
        &self,
        n: &NodeDesc,
        ranked_nodes: &[(NodeDesc, u64)],
        balance: &CountBalance,
    ) -> Option<TransferLeader> {
        let node_replicas = self.alloc_source.node_replicas(&n.id);
        let groups = self.alloc_source.groups();
        for (replica, group_id) in node_replicas
            .iter()
            .filter(|(r, g)| *g != ROOT_GROUP_ID && r.role == ReplicaRole::Voter as i32)
        {
            let replica_state = self.alloc_source.replica_state(&replica.id);
            if replica_state.is_none() {
                // The replica existed in group_desc, but not found in replica_state, the
                // reason(if no code bug) should be: 1. "group_desc update" has
                // be taken effect, but "replica_state update" is delayed(e.g.
                // report net fail and heartbeat still wait next turn)
                //
                // It's very low probability for a not exist replica_state be a leader, so we
                // choose try other replicas here without waiting new
                // replica_state update.
                continue;
            }
            if replica_state.as_ref().unwrap().role != RaftRole::Leader as i32 {
                continue;
            }

            let group =
                groups.get(group_id).expect("group {group_id} inconsistent with node-group index");
            let exist_replica_in_nodes = group
                .replicas
                .iter()
                .filter(|r| r.id != replica.id)
                .map(|r| (r.node_id, r.to_owned()))
                .collect::<HashMap<u64, ReplicaDesc>>();

            for (target_node, count) in ranked_nodes
                .iter()
                .rev()
                .take_while(|(_, count)| balance.classify(*count) == BalanceStatus::Underfull)
            {
                if balance.classify(count + 1) == BalanceStatus::Overfull {
                    continue;
                }
                let target_replica = exist_replica_in_nodes.get(&target_node.id);
                if target_replica.is_none() {
                    continue;
                }
                let target_replica = target_replica.unwrap();
                return Some(TransferLeader {
                    group: group_id.to_owned(),
                    src_replica: replica.id,
                    target_replica: target_replica.id,
                    src_node: replica.node_id,
                    target_node: target_replica.node_id,
                });
            }
        }
        None
    }
}
