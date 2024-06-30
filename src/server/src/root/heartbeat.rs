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

use std::collections::HashSet;
use std::ops::Add;
use std::sync::Arc;
use std::vec;

use log::{info, trace, warn};
use sekas_api::server::v1::watch_response::{update_event, UpdateEvent};
use sekas_api::server::v1::*;
use tokio::time::Instant;

use super::{HeartbeatTask, Root, Schema};
use crate::constants::ROOT_GROUP_ID;
use crate::root::metrics;
use crate::root::schema::ReplicaNodes;
use crate::Result;

impl Root {
    pub async fn send_heartbeat(&self, schema: Arc<Schema>, tasks: &[HeartbeatTask]) -> Result<()> {
        let cur_node_id = self.current_node_id();
        let all_nodes = schema.list_node().await?;
        let nodes = all_nodes
            .iter()
            .filter(|n| tasks.iter().any(|t| t.node_id == n.id))
            .collect::<Vec<_>>();

        let mut piggybacks = Vec::new();

        // TODO: no need piggyback root info everytime.
        if true {
            let mut root = schema.get_root_desc().await?;
            root.root_nodes = {
                let mut nodes = ReplicaNodes(root.root_nodes);
                nodes.move_first(cur_node_id);
                nodes.0
            };
            trace!(
                "sync root info with heartbeat. root={:?}",
                root.root_nodes.iter().map(|n| n.id).collect::<Vec<_>>(),
            );
            piggybacks.push(PiggybackRequest {
                info: Some(piggyback_request::Info::SyncRoot(SyncRootRequest { root: Some(root) })),
            });
            piggybacks.push(PiggybackRequest {
                info: Some(piggyback_request::Info::CollectGroupDetail(
                    CollectGroupDetailRequest { groups: vec![] },
                )),
            });
            piggybacks.push(PiggybackRequest {
                info: Some(piggyback_request::Info::CollectStats(CollectStatsRequest {
                    field_mask: None,
                })),
            });
            piggybacks.push(PiggybackRequest {
                info: Some(piggyback_request::Info::CollectScheduleState(
                    CollectScheduleStateRequest {},
                )),
            })
        }

        let resps = {
            let _timer = metrics::HEARTBEAT_NODES_RPC_DURATION_SECONDS.start_timer();
            metrics::HEARTBEAT_NODES_BATCH_SIZE.set(nodes.len() as i64);
            let mut handles = Vec::new();
            for node in &nodes {
                trace!(
                    "send heartbeat to node {}, addr: {}, status: {}",
                    node.id,
                    node.addr,
                    node.status
                );
                let piggybacks = piggybacks.to_owned();
                let client = self.shared.transport_manager.get_node_client(node.addr.to_owned())?;
                let handle = sekas_runtime::spawn(async move {
                    client
                        .root_heartbeat(HeartbeatRequest {
                            piggybacks,
                            timestamp: 0, // TODO: use hlc
                        })
                        .await
                });
                handles.push(handle);
            }
            let mut resps = Vec::with_capacity(handles.len());
            for handle in handles.into_iter() {
                resps.push(handle.await?)
            }
            resps
        };

        let last_heartbeat = Instant::now();
        let mut heartbeat_tasks = Vec::new();
        let groups = schema.list_group().await?;
        for (i, resp) in resps.iter().enumerate() {
            let n = nodes.get(i).unwrap();
            match resp {
                Ok(res) => {
                    self.liveness.renew(n.id);
                    for resp in &res.piggybacks {
                        match resp.info.as_ref().unwrap() {
                            piggyback_response::Info::SyncRoot(_)
                            | piggyback_response::Info::CollectMovingShardState(_) => {}
                            piggyback_response::Info::CollectStats(ref resp) => {
                                self.handle_collect_stats(&schema, resp, n.to_owned()).await?
                            }
                            piggyback_response::Info::CollectGroupDetail(ref resp) => {
                                self.handle_group_detail(&schema, resp, &groups).await?
                            }
                            piggyback_response::Info::CollectScheduleState(ref resp) => {
                                self.handle_schedule_state(resp).await?
                            }
                        }
                    }
                }
                Err(err) => {
                    super::metrics::HEARTBEAT_TASK_FAIL_TOTAL
                        .with_label_values(&[&n.id.to_string()])
                        .inc();
                    self.liveness.init_node_if_first_seen(n.id);
                    warn!("send heartbeat error: {err:?}. node={}, target={}", n.id, n.addr);
                }
            }
            heartbeat_tasks.push(HeartbeatTask { node_id: n.id });
            if i % 10 == 0 {
                sekas_runtime::yield_now().await;
            }
        }
        self.heartbeat_queue
            .try_schedule(heartbeat_tasks, last_heartbeat.add(self.cfg.heartbeat_interval()))
            .await;

        Ok(())
    }

    async fn handle_collect_stats(
        &self,
        schema: &Schema,
        resp: &CollectStatsResponse,
        node: &NodeDesc,
    ) -> Result<()> {
        if let Some(ns) = &resp.node_stats {
            let mut node = node.to_owned();
            let _timer = super::metrics::HEARTBEAT_HANDLE_NODE_STATS_DURATION_SECONDS.start_timer();
            let new_group_count = ns.group_count as u64;
            let new_leader_count = ns.leader_count as u64;
            let mut cap = node.capacity.take().unwrap();
            if new_group_count != cap.replica_count || new_leader_count != cap.leader_count {
                super::metrics::HEARTBEAT_UPDATE_NODE_STATS_TOTAL.inc();
                cap.replica_count = new_group_count;
                cap.leader_count = new_leader_count;
                info!(
                    "update node stats by heartbeat response. node={}, replica_count={}, leader_count={}",
                    node.id,
                    cap.replica_count,
                    cap.leader_count,
                );
                node.capacity = Some(cap);
                schema.update_node(node).await?;
            }
        }
        for gs in &resp.group_stats {
            self.cluster_stats.handle_group_stats(gs.clone());
        }
        Ok(())
    }

    async fn handle_group_detail(
        &self,
        schema: &Schema,
        resp: &CollectGroupDetailResponse,
        groups: &[GroupDesc],
    ) -> Result<()> {
        let _timer = super::metrics::HEARTBEAT_HANDLE_GROUP_DETAIL_DURATION_SECONDS.start_timer();
        let mut update_events = Vec::new();
        for desc in &resp.group_descs {
            if let Some(ex) = groups.iter().find(|g| g.id == desc.id) {
                if desc.epoch <= ex.epoch {
                    continue;
                }
            }
            schema.update_group_replica(Some(desc.to_owned()), None).await?;
            metrics::ROOT_UPDATE_GROUP_DESC_TOTAL.heartbeat.inc();
            info!("update group_desc from heartbeat response. group={}, desc={:?}", desc.id, desc);
            if desc.id == ROOT_GROUP_ID {
                self.heartbeat_queue
                    .try_schedule(
                        vec![HeartbeatTask { node_id: self.current_node_id() }],
                        Instant::now(),
                    )
                    .await;
            }
            update_events
                .push(UpdateEvent { event: Some(update_event::Event::Group(desc.to_owned())) })
        }

        let mut changed_group_states = HashSet::new();
        for state in &resp.replica_states {
            if let Some(pre_state) =
                schema.get_replica_state(state.group_id, state.replica_id).await?
            {
                if state.term < pre_state.term
                    || (state.term == pre_state.term && state.role == pre_state.role)
                {
                    continue;
                }
            }
            schema.update_group_replica(None, Some(state.to_owned())).await?;
            metrics::ROOT_UPDATE_REPLICA_STATE_TOTAL.heartbeat.inc();
            info!(
                "attempt update replica_state from heartbeat response. group={}, replica={}, state={:?}",
                state.group_id,
                state.replica_id,
                state,
            );
            changed_group_states.insert(state.group_id);
        }

        let mut states = schema.list_group_state().await?; // TODO: fix poor performance.
        states.retain(|s| changed_group_states.contains(&s.group_id));
        for state in states {
            update_events.push(UpdateEvent { event: Some(update_event::Event::GroupState(state)) })
        }

        if !update_events.is_empty() {
            self.watcher_hub().notify_updates(update_events).await;
        }

        Ok(())
    }

    async fn handle_schedule_state(&self, resp: &CollectScheduleStateResponse) -> Result<()> {
        self.cluster_stats.handle_schedule_update(&resp.schedule_states, None);
        Ok(())
    }
}
