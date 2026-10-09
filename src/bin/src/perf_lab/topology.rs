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

use std::time::{Duration, Instant};

use anyhow::{Result, anyhow, bail, ensure};
use log::warn;
use sekas_api::server::v1::*;
use sekas_client::GroupClient;

use super::LabContext;

impl LabContext {
    pub(super) fn group(&self, group_id: u64) -> GroupClient {
        GroupClient::lazy(group_id, self.client.clone())
    }

    pub(crate) async fn group_for_key(
        &self,
        table_id: u64,
        key: &[u8],
    ) -> Result<(u64, ShardDesc)> {
        for _ in 0..1000 {
            if let Ok((group, shard)) = self.router.find_shard(table_id, key) {
                return Ok((group.id, shard));
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        bail!("no group for key {:?}", key);
    }

    /// Co-locate the two keys' shards by moving the second shard if necessary.
    /// The caller is responsible for disabling shard balancing.
    pub(crate) async fn ensure_same_group(
        &self,
        first: (u64, &[u8]),
        second: (u64, &[u8]),
    ) -> Result<u64> {
        let (group_id, _) = self.group_for_key(first.0, first.1).await?;
        self.migrate_shard_to_group(second.0, second.1, group_id).await?;
        let (first_group, _) = self.group_for_key(first.0, first.1).await?;
        let (second_group, _) = self.group_for_key(second.0, second.1).await?;
        if first_group != group_id || second_group != group_id {
            bail!("keys did not converge to group {group_id}: {first_group} vs {second_group}");
        }
        Ok(group_id)
    }

    pub(crate) async fn group_leader(&self, group_id: u64) -> Result<u64> {
        for _ in 0..1000 {
            if let Ok(group) = self.router.find_group(group_id)
                && let Some((leader, _)) = group.leader_state
            {
                return Ok(leader);
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        bail!("group {group_id} has no leader");
    }

    pub(crate) async fn group_leader_node(&self, group_id: u64) -> Result<u64> {
        let leader = self.group_leader(group_id).await?;
        let group = self.router.find_group(group_id)?;
        group
            .replicas
            .values()
            .find(|replica| replica.id == leader)
            .map(|replica| replica.node_id)
            .ok_or_else(|| anyhow!("group {group_id} leader replica {leader} has no node"))
    }

    pub(crate) async fn transfer_group_leader(
        &self,
        group_id: u64,
    ) -> Result<LeaderTransferResult> {
        self.ensure_group_voters(group_id, 2).await?;
        let group = self.router.find_group(group_id)?;
        let leader = group.leader_state.map(|v| v.0);
        let target = group
            .replicas
            .values()
            .find(|replica| Some(replica.id) != leader && replica.role == ReplicaRole::Voter as i32)
            .ok_or_else(|| anyhow!("group {group_id} has no follower voter"))?;
        let mut client = self.group(group_id);
        let started = Instant::now();
        client.transfer_leader(target.id).await?;
        let rpc_duration = started.elapsed();
        let route_started = Instant::now();
        self.wait_group_leader(group_id, target.id).await?;
        Ok(LeaderTransferResult {
            rpc_duration,
            route_convergence: route_started.elapsed(),
            target_replica: target.id,
        })
    }

    pub(crate) async fn ensure_group_voters(&self, group_id: u64, voters: usize) -> Result<()> {
        for _ in 0..400 {
            if let Ok(group) = self.router.find_group(group_id) {
                let current = group
                    .replicas
                    .values()
                    .filter(|replica| replica.role == ReplicaRole::Voter as i32)
                    .count();
                if current >= voters && group.leader_state.is_some() {
                    return Ok(());
                }
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        bail!("group {group_id} does not have {voters} voters");
    }

    pub(crate) async fn add_group_replica(&self, group_id: u64, node_id: u64) -> Result<()> {
        let replica_id = group_id * 1000 + node_id + 100;
        let incoming = ReplicaDesc { id: replica_id, node_id, role: ReplicaRole::Voter as i32 };
        self.move_group_replicas(group_id, vec![incoming], vec![]).await?;
        self.wait_group_voter_on_node(group_id, node_id).await
    }

    /// Remove the extra member and restore the lab fixture to three voters.
    pub(crate) async fn remove_extra_replica(
        &self,
        group_id: u64,
        node_id: u64,
        wait: Duration,
    ) -> Result<Duration> {
        let started = Instant::now();
        let group = self.router.find_group(group_id)?;
        let replica =
            group
                .replicas
                .values()
                .find(|replica| replica.node_id == node_id)
                .cloned()
                .ok_or_else(|| anyhow!("group {group_id} has no replica on node {node_id}"))?;
        self.move_group_replicas(group_id, vec![], vec![replica]).await?;
        let deadline = Instant::now() + wait;
        while Instant::now() < deadline {
            if let Ok(group) = self.router.find_group(group_id)
                && !group.replicas.values().any(|replica| replica.node_id == node_id)
            {
                let voters = group
                    .replicas
                    .values()
                    .filter(|replica| replica.role == ReplicaRole::Voter as i32)
                    .count();
                ensure!(
                    voters == 3,
                    "group {group_id} has {voters} voters after removing extra replica, expected three"
                );
                return Ok(started.elapsed());
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        bail!("group {group_id} still contains replica on node {node_id}")
    }

    async fn wait_group_voter_on_node(&self, group_id: u64, node_id: u64) -> Result<()> {
        for _ in 0..400 {
            if let Ok(group) = self.router.find_group(group_id)
                && group.replicas.values().any(|replica| {
                    replica.node_id == node_id && replica.role == ReplicaRole::Voter as i32
                })
            {
                return Ok(());
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        bail!("group {group_id} does not contain voter on node {node_id}");
    }

    async fn move_group_replicas(
        &self,
        group_id: u64,
        incoming: Vec<ReplicaDesc>,
        outgoing: Vec<ReplicaDesc>,
    ) -> Result<()> {
        let mut last_err = None;
        for _ in 0..400 {
            let mut client = self.group(group_id);
            match client.move_replicas(incoming.clone(), outgoing.clone()).await {
                Ok(_) => return Ok(()),
                Err(sekas_client::Error::AlreadyExists(message))
                    if message == "config change" || message == "MoveReplicas task" =>
                {
                    last_err = Some(message);
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
                Err(err) => return Err(err.into()),
            }
        }

        bail!(
            "group {group_id} move replicas is still busy: {}",
            last_err.unwrap_or_else(|| "unknown".to_owned())
        );
    }

    pub(crate) async fn migrate_shard_to_new_group(
        &self,
        table_id: u64,
        key: &[u8],
    ) -> Result<ShardMigrationResult> {
        let (src_group, shard) = self.group_for_key(table_id, key).await?;
        let dest_group = self
            .find_group_without_shard(src_group)
            .await?
            .ok_or_else(|| anyhow!("no destination group without shard {}", shard.id))?;
        self.migrate_shard_to_group(table_id, key, dest_group).await
    }

    pub(crate) async fn migrate_shard_to_group(
        &self,
        table_id: u64,
        key: &[u8],
        dest_group: u64,
    ) -> Result<ShardMigrationResult> {
        let (src_group, shard) = self.group_for_key(table_id, key).await?;
        if src_group == dest_group {
            return Ok(ShardMigrationResult {
                duration: Duration::ZERO,
                route_convergence: Duration::ZERO,
                src_group,
                dest_group,
                shard_id: shard.id,
            });
        }
        let source_state = self.router.find_group(src_group)?;
        let started = Instant::now();
        for _ in 0..16 {
            let src_epoch = self.router.find_group(src_group)?.epoch;
            if self.group_contains_shard(dest_group, shard.id) {
                self.wait_source_release(&source_state, shard.id).await?;
                return Ok(ShardMigrationResult {
                    duration: started.elapsed(),
                    route_convergence: Duration::ZERO,
                    src_group,
                    dest_group,
                    shard_id: shard.id,
                });
            }
            let mut group = self.group(dest_group);
            match group.accept_shard(src_group, src_epoch, &shard).await {
                Ok(()) => {}
                Err(err) => {
                    warn!(
                        "accept shard {} from group {} to group {} failed: {}",
                        shard.id, src_group, dest_group, err
                    );
                    tokio::time::sleep(Duration::from_millis(50)).await;
                    continue;
                }
            }
            let route_started = Instant::now();
            for _ in 0..1000 {
                if self.group_contains_shard(dest_group, shard.id) {
                    self.wait_source_release(&source_state, shard.id).await?;
                    return Ok(ShardMigrationResult {
                        duration: started.elapsed(),
                        route_convergence: route_started.elapsed(),
                        src_group,
                        dest_group,
                        shard_id: shard.id,
                    });
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        }
        bail!("migrate shard {} did not finish", shard.id);
    }

    async fn wait_source_release(
        &self,
        source: &sekas_client::RouterGroupState,
        shard: u64,
    ) -> Result<()> {
        let deadline =
            Instant::now() + Duration::from_secs(self.config.workload.event_timeout_secs);
        while Instant::now() < deadline {
            // Use the pre-migration epoch and an accurate-epoch RPC. The new
            // descriptor proves removal without issuing an invalid current-epoch
            // request against a shard that no longer exists.
            let mut client = GroupClient::new(source.clone(), self.client.clone());
            let result = tokio::time::timeout(
                Duration::from_secs(1),
                client.get_split_key(shard, None, None),
            )
            .await;
            if let Ok(Err(sekas_client::Error::EpochNotMatch(desc))) = result
                && !desc.shards.iter().any(|s| s.id == shard)
            {
                return Ok(());
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        bail!("source group {} did not release shard {shard}", source.id)
    }

    pub(crate) async fn split_shard_for_key(
        &self,
        table_id: u64,
        key: &[u8],
    ) -> Result<SplitShardResult> {
        let (group_id, shard) = self.group_for_key(table_id, key).await?;
        let before_epoch = self.router.find_group(group_id)?.epoch;
        let new_shard_id = shard.id + 10_000_000;
        let started = Instant::now();
        let mut group = self.group(group_id);
        group.split_shard(shard.id, new_shard_id, Some(key.to_vec())).await?;
        let rpc_duration = started.elapsed();
        let route_started = Instant::now();
        self.wait_group_epoch_advance(group_id, before_epoch).await?;
        Ok(SplitShardResult {
            rpc_duration,
            route_convergence: route_started.elapsed(),
            group_id,
            left_shard_id: shard.id,
            right_shard_id: new_shard_id,
        })
    }

    pub(crate) async fn merge_shards(
        &self,
        group_id: u64,
        left_shard_id: u64,
        right_shard_id: u64,
    ) -> Result<MergeShardResult> {
        let before_epoch = self.router.find_group(group_id)?.epoch;
        let started = Instant::now();
        let mut last_err = None;
        for attempts in 1..=20 {
            let mut group = self.group(group_id);
            match group.merge_shard(left_shard_id, right_shard_id).await {
                Ok(()) => {
                    let rpc_duration = started.elapsed();
                    let route_started = Instant::now();
                    self.wait_group_epoch_advance(group_id, before_epoch).await?;
                    return Ok(MergeShardResult {
                        rpc_duration,
                        route_convergence: route_started.elapsed(),
                        attempts,
                    });
                }
                Err(err) => {
                    last_err = Some(err);
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
            }
        }
        if let Some(err) = last_err {
            return Err(err.into());
        }
        bail!("merge shard did not run")
    }

    async fn wait_group_epoch_advance(&self, group_id: u64, before_epoch: u64) -> Result<()> {
        for _ in 0..200 {
            if let Ok(group) = self.router.find_group(group_id)
                && group.epoch > before_epoch
            {
                return Ok(());
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        bail!("group {group_id} epoch did not advance beyond {before_epoch}")
    }

    pub(super) async fn wait_group_leader(
        &self,
        group_id: u64,
        expected_leader: u64,
    ) -> Result<()> {
        for _ in 0..200 {
            if let Ok(group) = self.router.find_group(group_id)
                && group.leader_state.map(|leader| leader.0) == Some(expected_leader)
            {
                return Ok(());
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        bail!("group {group_id} leader did not converge to replica {expected_leader}")
    }

    async fn find_group_without_shard(&self, src_group: u64) -> Result<Option<u64>> {
        let body = self.client.handle_statement("show groups").await?;
        let sekas_parser::ExecuteResult::Data(groups) = serde_json::from_slice(&body)? else {
            bail!("SHOW GROUPS did not return groups");
        };
        let mut ids =
            groups.rows.iter().map(|row| row.values[0].as_u64().unwrap()).collect::<Vec<_>>();
        ids.sort_unstable();
        for group_id in ids {
            if let Ok(group) = self.router.find_group(group_id)
                && group.id != 0
                && group.id != src_group
                && group.replicas.len() >= self.config.cluster.root.replicas_per_group
                && group.leader_state.is_some()
            {
                return Ok(Some(group_id));
            }
        }
        Ok(None)
    }

    fn group_contains_shard(&self, group_id: u64, shard_id: u64) -> bool {
        self.router.find_group_by_shard(shard_id).map(|group| group.id == group_id).unwrap_or(false)
    }
}
pub(crate) struct LeaderTransferResult {
    pub(crate) rpc_duration: Duration,
    pub(crate) route_convergence: Duration,
    pub(crate) target_replica: u64,
}

pub(crate) struct ShardMigrationResult {
    pub(crate) duration: Duration,
    pub(crate) route_convergence: Duration,
    pub(crate) src_group: u64,
    pub(crate) dest_group: u64,
    pub(crate) shard_id: u64,
}

pub(crate) struct SplitShardResult {
    pub(crate) rpc_duration: Duration,
    pub(crate) route_convergence: Duration,
    pub(crate) group_id: u64,
    pub(crate) left_shard_id: u64,
    pub(crate) right_shard_id: u64,
}

pub(crate) struct MergeShardResult {
    pub(crate) rpc_duration: Duration,
    pub(crate) route_convergence: Duration,
    pub(crate) attempts: u64,
}
