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

use std::collections::VecDeque;
use std::time::{Duration, Instant};

use rand::prelude::SmallRng;
use rand::{Rng, SeedableRng};
use serde::{Deserialize, Serialize};

use crate::cluster::{
    AdminAction, BoxFuture, ClusterController, ClusterStatus, FaultHandle, FaultSpec, NodeId,
    NodeState,
};

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum ChaosEventKind {
    Start(NodeId),
    Stop(NodeId),
    Kill(NodeId),
    Pause(NodeId),
    Resume(NodeId),
    Restart(NodeId),
    Inject(FaultSpec),
    Recover(FaultHandle),
    Admin(AdminAction),
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum EventOutcome {
    Succeeded,
    Failed(String),
    TimedOut,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ChaosEvent {
    pub sequence: u64,
    pub started_micros: u64,
    pub completed_micros: u64,
    pub kind: ChaosEventKind,
    pub outcome: EventOutcome,
    pub before: Option<ClusterStatus>,
    pub after: Option<ClusterStatus>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ActionWeights {
    pub stop: u32,
    pub kill: u32,
    pub pause: u32,
    pub recover: u32,
    pub leader_transfer: u32,
    pub shard_move: u32,
}

impl Default for ActionWeights {
    fn default() -> Self {
        Self { stop: 2, kill: 4, pause: 2, recover: 8, leader_transfer: 2, shard_move: 1 }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ShardMoveCandidate {
    pub table_id: u64,
    pub key: Vec<u8>,
    pub target_group_id: u64,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct NemesisConfig {
    pub seed: u64,
    pub interval: Duration,
    pub action_timeout: Duration,
    pub max_unavailable_nodes: usize,
    pub weights: ActionWeights,
    pub leader_groups: Vec<u64>,
    pub shard_moves: Vec<ShardMoveCandidate>,
    pub max_recorded_events: usize,
}

impl Default for NemesisConfig {
    fn default() -> Self {
        Self {
            seed: 0x5eca5 ^ 0x6e65_6d65_7369_7300,
            interval: Duration::from_secs(1),
            action_timeout: Duration::from_secs(10),
            max_unavailable_nodes: 1,
            weights: ActionWeights::default(),
            leader_groups: Vec::new(),
            shard_moves: Vec::new(),
            max_recorded_events: 10_000,
        }
    }
}

impl NemesisConfig {
    pub fn validate(&self) -> Result<(), String> {
        if self.interval.is_zero() || self.action_timeout.is_zero() {
            return Err("nemesis interval and action timeout must be non-zero".to_string());
        }
        if self.max_unavailable_nodes == 0 {
            return Err("max_unavailable_nodes must be at least one".to_string());
        }
        if self.max_recorded_events == 0 {
            return Err("max_recorded_events must be non-zero".to_string());
        }
        let weights = self.weights;
        if weights.stop
            + weights.kill
            + weights.pause
            + weights.recover
            + weights.leader_transfer
            + weights.shard_move
            == 0
        {
            return Err("at least one nemesis action weight must be non-zero".to_string());
        }
        Ok(())
    }
}

/// Selects and applies only actions valid for the current cluster state.
pub trait Nemesis: Send {
    fn next<'a>(&'a mut self, status: &'a ClusterStatus) -> BoxFuture<'a, Option<ChaosEventKind>>;

    fn apply<'a>(
        &'a mut self,
        controller: &'a mut dyn ClusterController,
        action: ChaosEventKind,
    ) -> BoxFuture<'a, ChaosEvent>;
}

pub struct RandomNemesis {
    config: NemesisConfig,
    rng: SmallRng,
    start: Instant,
    next_sequence: u64,
    events: VecDeque<ChaosEvent>,
    dropped_events: u64,
}

impl RandomNemesis {
    pub fn new(config: NemesisConfig) -> Result<Self, String> {
        config.validate()?;
        Ok(Self {
            rng: SmallRng::seed_from_u64(config.seed),
            start: Instant::now(),
            next_sequence: 0,
            events: VecDeque::with_capacity(config.max_recorded_events),
            dropped_events: 0,
            config,
        })
    }

    pub fn config(&self) -> &NemesisConfig {
        &self.config
    }

    pub fn events(&self) -> Vec<ChaosEvent> {
        self.events.iter().cloned().collect()
    }

    pub fn dropped_events(&self) -> u64 {
        self.dropped_events
    }

    fn record(&mut self, event: ChaosEvent) {
        if self.events.len() == self.config.max_recorded_events {
            self.events.pop_front();
            self.dropped_events += 1;
        }
        self.events.push_back(event);
    }

    fn candidates(&self, status: &ClusterStatus) -> Vec<(u32, ChaosEventKind)> {
        let mut candidates = Vec::new();
        let unavailable = status.unavailable_nodes();
        for (id, node) in &status.nodes {
            match node.state {
                NodeState::Running if unavailable < self.config.max_unavailable_nodes => {
                    if status.capabilities.graceful_stop && self.config.weights.stop > 0 {
                        candidates.push((self.config.weights.stop, ChaosEventKind::Stop(*id)));
                    }
                    if status.capabilities.kill && self.config.weights.kill > 0 {
                        candidates.push((self.config.weights.kill, ChaosEventKind::Kill(*id)));
                    }
                    if status.capabilities.pause && self.config.weights.pause > 0 {
                        candidates.push((self.config.weights.pause, ChaosEventKind::Pause(*id)));
                    }
                }
                NodeState::Stopped => {
                    candidates
                        .push((self.config.weights.recover.max(1), ChaosEventKind::Start(*id)));
                }
                NodeState::Exited => candidates
                    .push((self.config.weights.recover.max(1), ChaosEventKind::Restart(*id))),
                NodeState::Paused => candidates
                    .push((self.config.weights.recover.max(1), ChaosEventKind::Resume(*id))),
                NodeState::Starting | NodeState::Running => {}
            }
        }
        for fault in &status.active_faults {
            candidates
                .push((self.config.weights.recover.max(1), ChaosEventKind::Recover(fault.clone())));
        }
        if unavailable == 0 && status.capabilities.admin_actions {
            if self.config.weights.leader_transfer > 0 {
                candidates.extend(self.config.leader_groups.iter().map(|group_id| {
                    (
                        self.config.weights.leader_transfer,
                        ChaosEventKind::Admin(AdminAction::TransferLeader {
                            group_id: *group_id,
                            target_replica: None,
                        }),
                    )
                }));
            }
            if self.config.weights.shard_move > 0 {
                candidates.extend(self.config.shard_moves.iter().map(|candidate| {
                    (
                        self.config.weights.shard_move,
                        ChaosEventKind::Admin(AdminAction::MoveShard {
                            table_id: candidate.table_id,
                            key: candidate.key.clone(),
                            target_group_id: candidate.target_group_id,
                        }),
                    )
                }));
            }
        }
        candidates
    }

    async fn dispatch(
        controller: &mut dyn ClusterController,
        action: &ChaosEventKind,
    ) -> crate::cluster::Result<()> {
        match action {
            ChaosEventKind::Start(node) => controller.start_node(*node).await,
            ChaosEventKind::Stop(node) => controller.stop_node(*node).await,
            ChaosEventKind::Kill(node) => controller.kill_node(*node).await,
            ChaosEventKind::Pause(node) => controller.pause_node(*node).await,
            ChaosEventKind::Resume(node) => controller.resume_node(*node).await,
            ChaosEventKind::Restart(node) => controller.restart_node(*node).await,
            ChaosEventKind::Inject(fault) => controller.inject(fault.clone()).await.map(|_| ()),
            ChaosEventKind::Recover(fault) => controller.recover(fault.clone()).await,
            ChaosEventKind::Admin(action) => controller.admin(action.clone()).await,
        }
    }
}

impl Nemesis for RandomNemesis {
    fn next<'a>(&'a mut self, status: &'a ClusterStatus) -> BoxFuture<'a, Option<ChaosEventKind>> {
        Box::pin(async move {
            let candidates = self.candidates(status);
            let total = candidates.iter().map(|(weight, _)| *weight as u64).sum::<u64>();
            if total == 0 {
                return None;
            }
            let mut selected = self.rng.gen_range(0..total);
            for (weight, action) in candidates {
                if selected < weight as u64 {
                    return Some(action);
                }
                selected -= weight as u64;
            }
            unreachable!("weighted nemesis selection exhausted")
        })
    }

    fn apply<'a>(
        &'a mut self,
        controller: &'a mut dyn ClusterController,
        action: ChaosEventKind,
    ) -> BoxFuture<'a, ChaosEvent> {
        Box::pin(async move {
            let sequence = self.next_sequence;
            self.next_sequence += 1;
            let started_micros = self.start.elapsed().as_micros() as u64;
            let before = controller.status().await.ok();
            let outcome = match tokio::time::timeout(
                self.config.action_timeout,
                Self::dispatch(controller, &action),
            )
            .await
            {
                Ok(Ok(())) => EventOutcome::Succeeded,
                Ok(Err(err)) => EventOutcome::Failed(err.to_string()),
                Err(_) => EventOutcome::TimedOut,
            };
            let after = controller.status().await.ok();
            let event = ChaosEvent {
                sequence,
                started_micros,
                completed_micros: self.start.elapsed().as_micros() as u64,
                kind: action,
                outcome,
                before,
                after,
            };
            self.record(event.clone());
            event
        })
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::path::PathBuf;
    use std::time::Duration;

    use super::{ActionWeights, ChaosEventKind, Nemesis, NemesisConfig, RandomNemesis};
    use crate::cluster::{
        AdminAction, BoxFuture, ClusterCapabilities, ClusterController, ClusterError, ClusterSpec,
        ClusterStatus, FaultHandle, FaultSpec, NodeId, NodeState, NodeStatus, Result,
    };

    struct FakeCluster {
        status: ClusterStatus,
    }

    impl FakeCluster {
        fn new(nodes: usize) -> Self {
            let nodes = (0..nodes)
                .map(|id| {
                    (
                        NodeId(id as u64),
                        NodeStatus {
                            state: NodeState::Running,
                            generation: 1,
                            pid: None,
                            exit: None,
                            stdout: PathBuf::new(),
                            stderr: PathBuf::new(),
                        },
                    )
                })
                .collect::<BTreeMap<_, _>>();
            Self {
                status: ClusterStatus {
                    nodes,
                    active_faults: vec![],
                    capabilities: ClusterCapabilities {
                        graceful_stop: true,
                        kill: true,
                        pause: true,
                        admin_actions: true,
                        ..ClusterCapabilities::default()
                    },
                },
            }
        }

        fn set(&mut self, node: NodeId, state: NodeState) -> Result<()> {
            self.status.nodes.get_mut(&node).ok_or(ClusterError::UnknownNode(node))?.state = state;
            Ok(())
        }
    }

    impl ClusterController for FakeCluster {
        fn deploy<'a>(&'a mut self, _spec: ClusterSpec) -> BoxFuture<'a, Result<()>> {
            Box::pin(async { Ok(()) })
        }
        fn start_node<'a>(&'a mut self, node: NodeId) -> BoxFuture<'a, Result<()>> {
            Box::pin(async move { self.set(node, NodeState::Running) })
        }
        fn stop_node<'a>(&'a mut self, node: NodeId) -> BoxFuture<'a, Result<()>> {
            Box::pin(async move { self.set(node, NodeState::Stopped) })
        }
        fn kill_node<'a>(&'a mut self, node: NodeId) -> BoxFuture<'a, Result<()>> {
            Box::pin(async move { self.set(node, NodeState::Stopped) })
        }
        fn pause_node<'a>(&'a mut self, node: NodeId) -> BoxFuture<'a, Result<()>> {
            Box::pin(async move { self.set(node, NodeState::Paused) })
        }
        fn resume_node<'a>(&'a mut self, node: NodeId) -> BoxFuture<'a, Result<()>> {
            Box::pin(async move { self.set(node, NodeState::Running) })
        }
        fn restart_node<'a>(&'a mut self, node: NodeId) -> BoxFuture<'a, Result<()>> {
            Box::pin(async move { self.set(node, NodeState::Running) })
        }
        fn inject<'a>(&'a mut self, fault: FaultSpec) -> BoxFuture<'a, Result<FaultHandle>> {
            Box::pin(async move {
                let handle = FaultHandle { id: 1, fault };
                self.status.active_faults.push(handle.clone());
                Ok(handle)
            })
        }
        fn recover<'a>(&'a mut self, fault: FaultHandle) -> BoxFuture<'a, Result<()>> {
            Box::pin(async move {
                self.status.active_faults.retain(|active| active.id != fault.id);
                Ok(())
            })
        }
        fn admin<'a>(&'a mut self, _action: AdminAction) -> BoxFuture<'a, Result<()>> {
            Box::pin(async { Ok(()) })
        }
        fn status<'a>(&'a mut self) -> BoxFuture<'a, Result<ClusterStatus>> {
            Box::pin(async move { Ok(self.status.clone()) })
        }
        fn wait_ready<'a>(&'a mut self, _timeout: Duration) -> BoxFuture<'a, Result<()>> {
            Box::pin(async { Ok(()) })
        }
        fn restore<'a>(&'a mut self) -> BoxFuture<'a, Result<()>> {
            Box::pin(async move {
                for node in self.status.nodes.values_mut() {
                    node.state = NodeState::Running;
                }
                self.status.active_faults.clear();
                Ok(())
            })
        }
        fn shutdown<'a>(&'a mut self) -> BoxFuture<'a, Result<()>> {
            Box::pin(async { Ok(()) })
        }
    }

    #[tokio::test]
    async fn same_seed_and_state_produce_same_actions() {
        let config = NemesisConfig {
            interval: Duration::from_millis(1),
            action_timeout: Duration::from_secs(1),
            ..NemesisConfig::default()
        };
        let mut left = RandomNemesis::new(config.clone()).unwrap();
        let mut right = RandomNemesis::new(config).unwrap();
        let mut left_cluster = FakeCluster::new(3);
        let mut right_cluster = FakeCluster::new(3);
        for _ in 0..20 {
            let left_status = left_cluster.status().await.unwrap();
            let right_status = right_cluster.status().await.unwrap();
            let left_action = left.next(&left_status).await.unwrap();
            let right_action = right.next(&right_status).await.unwrap();
            assert_eq!(left_action, right_action);
            assert_eq!(
                left.apply(&mut left_cluster, left_action).await.outcome,
                right.apply(&mut right_cluster, right_action).await.outcome
            );
        }
        assert_eq!(left.events().len(), 20);
    }

    #[tokio::test]
    async fn disruption_budget_forces_recovery() {
        let mut nemesis = RandomNemesis::new(NemesisConfig::default()).unwrap();
        let mut cluster = FakeCluster::new(3);
        cluster.set(NodeId(0), NodeState::Paused).unwrap();
        let status = cluster.status().await.unwrap();
        assert!(matches!(nemesis.next(&status).await, Some(ChaosEventKind::Resume(NodeId(0)))));
    }

    #[tokio::test]
    async fn admin_actions_are_selected_only_when_cluster_is_fully_available() {
        let config = NemesisConfig {
            interval: Duration::from_millis(1),
            action_timeout: Duration::from_secs(1),
            weights: ActionWeights {
                stop: 0,
                kill: 0,
                pause: 0,
                recover: 0,
                leader_transfer: 1,
                shard_move: 1,
            },
            leader_groups: vec![1],
            shard_moves: vec![super::ShardMoveCandidate {
                table_id: 1024,
                key: b"admin-key".to_vec(),
                target_group_id: 2,
            }],
            ..NemesisConfig::default()
        };
        let mut nemesis = RandomNemesis::new(config).unwrap();
        let mut cluster = FakeCluster::new(3);

        let status = cluster.status().await.unwrap();
        assert!(matches!(
            nemesis.next(&status).await,
            Some(ChaosEventKind::Admin(AdminAction::TransferLeader { .. }))
                | Some(ChaosEventKind::Admin(AdminAction::MoveShard { .. }))
        ));

        cluster.set(NodeId(0), NodeState::Paused).unwrap();
        let status = cluster.status().await.unwrap();
        assert!(matches!(nemesis.next(&status).await, Some(ChaosEventKind::Resume(NodeId(0)))));
    }
}
