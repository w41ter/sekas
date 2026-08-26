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

use std::net::TcpListener;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use sekas_chaos::{
    ActionWeights, AdminAction, ChaosConfig, ChaosEventKind, ChaosRunner, ClusterController,
    ClusterSpec, EventOutcome, LocalProcessCluster, Nemesis, NemesisConfig, NodeId, NodeSpec,
    OperationWeights, RandomNemesis, SekasExecutor, ShardMoveCandidate, SizeRange, WorkloadConfig,
    WorkloadFailure, WorkloadRunner,
};
use sekas_client::{
    AppError, ClientOptions, ConnManager, RootClient, Router, RouterGroupState, SekasClient,
    StaticServiceDiscovery,
};

fn addresses(count: usize) -> Vec<String> {
    (0..count)
        .map(|_| {
            let listener = TcpListener::bind("127.0.0.1:0").unwrap();
            format!("127.0.0.1:{}", listener.local_addr().unwrap().port())
        })
        .collect()
}

fn cluster_spec(binary: PathBuf, root: &std::path::Path, addrs: &[String]) -> ClusterSpec {
    let nodes = addrs
        .iter()
        .enumerate()
        .map(|(index, address)| NodeSpec {
            id: NodeId(index as u64),
            address: address.clone(),
            data_dir: root.join(format!("node-{index}")),
            bootstrap: index == 0,
            join: if index == 0 { vec![] } else { vec![addrs[0].clone()] },
            extra_args: vec!["--cpu-nums".to_string(), "1".to_string()],
        })
        .collect();
    ClusterSpec {
        binary,
        nodes,
        startup_timeout: Duration::from_secs(30),
        shutdown_timeout: Duration::from_secs(10),
        log_dir: root.join("logs"),
    }
}

async fn workload_runner(
    addresses: Vec<String>,
    run_id: String,
) -> Result<(WorkloadRunner, u64, sekas_client::Database), WorkloadFailure> {
    let client = SekasClient::new(ClientOptions::default(), addresses)
        .await
        .map_err(|err| WorkloadFailure::Task(err.to_string()))?;
    let database = match client.create_database(format!("{run_id}-db")).await {
        Ok(database) => database,
        Err(AppError::AlreadyExists(_)) => client
            .open_database(format!("{run_id}-db"))
            .await
            .map_err(|err| WorkloadFailure::Task(err.to_string()))?,
        Err(err) => return Err(WorkloadFailure::Task(err.to_string())),
    };
    let table = database
        .create_table(format!("{run_id}-table"))
        .await
        .map_err(|err| WorkloadFailure::Task(err.to_string()))?;
    tokio::time::sleep(Duration::from_secs(2)).await;
    let config = WorkloadConfig {
        run_id,
        tables: vec![table.id],
        writers: 2,
        readers: 2,
        slots_per_writer: 32,
        value_size: SizeRange::new(8, 32),
        verify_width: SizeRange::new(1, 8),
        batch_width: SizeRange::new(2, 4),
        transaction_width: SizeRange::new(2, 4),
        operation_weights: OperationWeights {
            put: 4,
            delete: 2,
            batch: 2,
            transaction: 2,
            add_i64: 0,
        },
        reconciliation_timeout: Duration::from_secs(30),
        ..WorkloadConfig::default()
    };
    let runner = WorkloadRunner::new(config, Arc::new(SekasExecutor::new(database.clone())))?;
    Ok((runner, table.id, database))
}

struct RouteProbe {
    router: Router,
}

impl RouteProbe {
    async fn new(addresses: Vec<String>) -> Self {
        let discovery = Arc::new(StaticServiceDiscovery::new(addresses));
        let root = RootClient::new(discovery, ConnManager::new());
        Self { router: Router::new(root).await }
    }

    async fn wait_group_voters(&self, group_id: u64, min_voters: usize) -> RouterGroupState {
        for _ in 0..600 {
            if let Ok(group) = self.router.find_group(group_id) {
                let voters = group
                    .replicas
                    .values()
                    .filter(|replica| {
                        replica.role == sekas_api::server::v1::ReplicaRole::Voter as i32
                    })
                    .count();
                if voters >= min_voters && group.leader_state.is_some() {
                    return group;
                }
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        panic!("group {group_id} did not reach {min_voters} voters");
    }

    async fn wait_leader_changed(&self, group_id: u64, old_leader: u64) {
        for _ in 0..600 {
            if let Ok(group) = self.router.find_group(group_id)
                && let Some((leader, _)) = group.leader_state
                && leader != old_leader
            {
                return;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        panic!("group {group_id} leader did not change from {old_leader}");
    }

    async fn wait_shard_source_and_target(&self, table_id: u64, key: &[u8]) -> (u64, u64) {
        for _ in 0..600 {
            if let Ok((source, _)) = self.router.find_shard(table_id, key) {
                for target in 1..=8 {
                    if target != source.id && self.router.find_group(target).is_ok() {
                        return (source.id, target);
                    }
                }
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        panic!("no movable shard found for table {table_id} and key {key:?}");
    }

    async fn wait_shard_group(&self, table_id: u64, key: &[u8], group_id: u64) {
        for _ in 0..600 {
            if let Ok((group, _)) = self.router.find_shard(table_id, key)
                && group.id == group_id
            {
                return;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        panic!("table {table_id} key {key:?} did not move to group {group_id}");
    }
}

/// Runs a real three-process cluster. It is ignored by default because it
/// requires a pre-built `sekas` binary and performs crash/restart cycles.
#[tokio::test]
#[ignore = "set SEKAS_CHAOS_BINARY to a built sekas binary"]
async fn workload_survives_process_nemesis_and_final_verification() {
    let binary = PathBuf::from(
        std::env::var_os("SEKAS_CHAOS_BINARY")
            .expect("SEKAS_CHAOS_BINARY must point to a built sekas binary"),
    );
    let run_id = format!("process-e2e-{}", std::process::id());
    let root = std::env::temp_dir().join(format!("sekas-chaos-{run_id}"));
    let addrs = addresses(3);
    let cluster_spec = cluster_spec(binary, &root, &addrs);
    let nemesis = NemesisConfig {
        seed: 0x5eca5,
        interval: Duration::from_millis(300),
        action_timeout: Duration::from_secs(15),
        max_unavailable_nodes: 1,
        weights: ActionWeights {
            stop: 1,
            kill: 3,
            pause: 1,
            recover: 8,
            leader_transfer: 0,
            shard_move: 0,
        },
        ..NemesisConfig::default()
    };
    let runner = ChaosRunner::new(
        ChaosConfig { duration: Duration::from_secs(5), recovery_timeout: Duration::from_secs(30) },
        cluster_spec,
        LocalProcessCluster::new(),
        nemesis,
    )
    .unwrap();

    let setup_run_id = run_id.clone();
    let report = runner
        .run(move |addresses| async move {
            workload_runner(addresses, setup_run_id).await.map(|(runner, _, _)| runner)
        })
        .await;

    let disrupted = report.nemesis_events.iter().any(|event| {
        matches!(
            event.kind,
            ChaosEventKind::Stop(_) | ChaosEventKind::Kill(_) | ChaosEventKind::Pause(_)
        )
    });
    let recovered = report.nemesis_events.iter().any(|event| {
        matches!(
            event.kind,
            ChaosEventKind::Start(_) | ChaosEventKind::Restart(_) | ChaosEventKind::Resume(_)
        )
    });
    let valid = report.is_valid() && disrupted && recovered;
    let diagnostics = format!(
        "disrupted={disrupted}, recovered={recovered}, chaos failures={:#?}, workload failures={:#?}",
        report.failures,
        report.workload.as_ref().map(|workload| &workload.failures)
    );
    let _ = std::fs::remove_dir_all(&root);
    assert!(valid, "{diagnostics}");
}

/// Runs real admin actions through the nemesis dispatch path. It is ignored by
/// default because it starts a real cluster and migrates shard ownership.
#[tokio::test]
#[ignore = "set SEKAS_CHAOS_BINARY to a built sekas binary"]
async fn admin_nemesis_transfers_leader_and_moves_shard() {
    let binary = PathBuf::from(
        std::env::var_os("SEKAS_CHAOS_BINARY")
            .expect("SEKAS_CHAOS_BINARY must point to a built sekas binary"),
    );
    let run_id = format!("admin-e2e-{}", std::process::id());
    let root = std::env::temp_dir().join(format!("sekas-chaos-{run_id}"));
    let addrs = addresses(3);
    let cluster_spec = cluster_spec(binary, &root, &addrs);
    let mut cluster = LocalProcessCluster::new();
    cluster.deploy(cluster_spec).await.unwrap();

    let (workload, table_id, database) =
        workload_runner(addrs.clone(), run_id.clone()).await.unwrap();
    let workload = workload.start();
    let probe = RouteProbe::new(addrs).await;

    let group_id = 1;
    let group = probe.wait_group_voters(group_id, 2).await;
    let old_leader = group.leader_state.unwrap().0;
    let key = b"admin-move-key".to_vec();
    let value = b"admin-move-value".to_vec();
    database.put(table_id, key.clone(), value.clone()).await.unwrap();
    let (source_group, target_group) = probe.wait_shard_source_and_target(table_id, &key).await;

    let mut nemesis = RandomNemesis::new(NemesisConfig {
        seed: 0xad_1e_55,
        interval: Duration::from_millis(100),
        action_timeout: Duration::from_secs(30),
        max_unavailable_nodes: 1,
        weights: ActionWeights {
            stop: 0,
            kill: 0,
            pause: 0,
            recover: 0,
            leader_transfer: 1,
            shard_move: 1,
        },
        leader_groups: vec![group_id],
        shard_moves: vec![ShardMoveCandidate {
            table_id,
            key: key.clone(),
            target_group_id: target_group,
        }],
        ..NemesisConfig::default()
    })
    .unwrap();

    let transfer = nemesis
        .apply(
            &mut cluster,
            ChaosEventKind::Admin(AdminAction::TransferLeader { group_id, target_replica: None }),
        )
        .await;
    assert_eq!(transfer.outcome, EventOutcome::Succeeded, "{transfer:#?}");
    probe.wait_leader_changed(group_id, old_leader).await;

    let move_shard = nemesis
        .apply(
            &mut cluster,
            ChaosEventKind::Admin(AdminAction::MoveShard {
                table_id,
                key: key.clone(),
                target_group_id: target_group,
            }),
        )
        .await;
    assert_eq!(move_shard.outcome, EventOutcome::Succeeded, "{move_shard:#?}");
    probe.wait_shard_group(table_id, &key, target_group).await;
    assert_eq!(database.get(table_id, key).await.unwrap(), Some(value));

    workload.request_stop();
    cluster.restore().await.unwrap();
    let workload = workload.finish().await;
    cluster.shutdown().await.unwrap();

    let admin_events = nemesis.events();
    let _ = std::fs::remove_dir_all(&root);
    assert!(
        workload.is_valid(),
        "source_group={source_group}, target_group={target_group}, failures={:#?}",
        workload.failures
    );
    assert_eq!(admin_events.len(), 2);
}
