// Copyright 2024-present The Sekas Authors.
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

use std::collections::HashMap;
use std::time::Duration;

use futures::future::try_join_all;
use log::warn;
use sekas_api::server::v1::*;
use sekas_parser::{ColumnResult, ConfigStatement, ExecuteResult, Row, ShowStatement};
use sekas_rock::ascii::escape_bytes;
use sekas_rock::time::timestamp_millis;
use serde_json::Value;

use super::Root;
use super::allocator::BalanceDecision;
use super::schedule::Policy;
use super::stats::{ShowMetrics, ShowSnapshot};
use crate::{Error, Result};

impl Root {
    /// Handle statement and return with json.
    pub async fn handle_statement(&self, input: &str) -> Result<Vec<u8>> {
        let result = self.handle_statement_inner(input).await?;
        match serde_json::to_vec(&result) {
            Ok(bytes) => Ok(bytes),
            Err(err) => {
                warn!("serialize result {:?}: {:?}", result, err);
                Ok(br#"{"Msg":"internal error, serialize execute result failed"}"#.to_vec())
            }
        }
    }

    async fn handle_statement_inner(&self, input: &str) -> Result<ExecuteResult> {
        use sekas_parser::Statement::*;

        let Some(stmt) = sekas_parser::parse(input).unwrap() else {
            return Ok(ExecuteResult::None);
        };
        match stmt {
            Config(config) => self.handle_config_stmt(config).await,
            Show(show) => self.handle_show_stmt(show).await,
            CreateDb(_) | CreateTable(_) | Debug(_) | Echo(_) | Help(_) | Get(_) | Put(_)
            | Delete(_) => {
                Err(Error::InvalidArgument(", local stmt is sent to root server".to_owned()))
            }
        }
    }

    async fn handle_config_stmt(&self, config_stmt: ConfigStatement) -> Result<ExecuteResult> {
        let invalid_utf8 = |_| Error::InvalidArgument("CONFIG expects UTF-8 text".into());
        let key = std::str::from_utf8(&config_stmt.key).map_err(invalid_utf8)?;
        let Some(key) = key.strip_prefix("balancer.") else {
            return Err(Error::InvalidArgument(format!("unknown CONFIG setting {key}")));
        };
        let value = std::str::from_utf8(&config_stmt.value).map_err(invalid_utf8)?.trim();
        self.cfg.balance.clone().set(key, value)?;
        self.schema()?.set_balance_config(key, value).await?;
        Ok(ExecuteResult::Msg(format!("balancer.{key} updated")))
    }

    async fn handle_show_stmt(&self, show_stmt: ShowStatement) -> Result<ExecuteResult> {
        match show_stmt.property.as_str() {
            "databases" => self.handle_show_databases(show_stmt).await,
            "tables" => self.handle_show_tables(show_stmt).await,
            "groups" => self.handle_show_groups(show_stmt).await,
            "replicas" => self.handle_show_replicas(show_stmt).await,
            "shards" => self.handle_show_shards(show_stmt).await,
            "node" | "nodes" => self.handle_show_nodes(show_stmt).await,
            "balancer" | "scheduler" | "scheduler_tasks" => {
                self.handle_show_scheduling(show_stmt).await
            }
            others => Ok(ExecuteResult::Msg(format!("unknown property: {others}"))),
        }
    }

    async fn handle_show_scheduling(&self, show_stmt: ShowStatement) -> Result<ExecuteResult> {
        let property = show_stmt.property.as_str();
        if show_stmt.from.is_some() {
            return Ok(ExecuteResult::Msg(format!(
                "FROM clause is not supported by '{property}' property"
            )));
        }
        let schema = self.schema()?;

        let now = timestamp_millis();
        let data = match property {
            "balancer" => {
                let config = schema.balance_config(self.cfg.balance.clone()).await?;
                let priorities: HashMap<_, _> = config
                    .dimensions()
                    .filter(|kind| kind.enabled(&self.cfg))
                    .enumerate()
                    .map(|(index, kind)| (kind as usize, index + 1))
                    .collect();
                let snapshot = self.scheduler.balance_status();
                let rows = snapshot
                    .iter()
                    .map(|(policy, report)| {
                        let (status, decision) = match report {
                            BalanceDecision::Unknown => ("unknown", "awaiting first check"),
                            BalanceDecision::Disabled => ("disabled", "-"),
                            BalanceDecision::Deferred(reason) => ("deferred", reason.as_ref()),
                            BalanceDecision::NoAction(detail) => ("no_action", detail.as_str()),
                            BalanceDecision::ActionNeeded(detail) => {
                                ("action_needed", detail.as_str())
                            }
                            BalanceDecision::Unsupported(detail) => {
                                ("unsupported", detail.as_str())
                            }
                            BalanceDecision::Error(detail) => ("error", detail.as_str()),
                        };
                        let (priority, deadline) = match policy {
                            Policy::GroupCount => {
                                (self.cfg.enable_group_balance.then_some(0), None)
                            }
                            Policy::Placement(kind) => {
                                (priorities.get(&(kind as usize)).copied(), config.deadline(kind))
                            }
                        };
                        let display_load = |load| match policy {
                            Policy::Placement(crate::BalanceDimension::ShardSize) => {
                                display_balance_bytes(load).into()
                            }
                            _ => Value::from(load),
                        };
                        let deadline = deadline.map_or_else(|| "unlimited".into(), display_load);
                        let thresholds = snapshot.thresholds(policy);
                        Row {
                            values: vec![
                                policy.name().into(),
                                status.into(),
                                priority.into(),
                                deadline,
                                thresholds.map(|values| display_load(values.avg)).into(),
                                thresholds.map(|values| display_load(values.low)).into(),
                                thresholds.map(|values| display_load(values.high)).into(),
                                thresholds.map(|values| display_load(values.max)).into(),
                                thresholds.map(|values| display_load(values.tolerance)).into(),
                                decision.into(),
                            ],
                        }
                    })
                    .collect();
                show_columns(
                    [
                        "policy",
                        "status",
                        "priority",
                        "deadline",
                        "avg",
                        "low",
                        "high",
                        "max",
                        "tolerance",
                        "decision",
                    ],
                    rows,
                )
            }
            "scheduler" => {
                let summary = self.scheduler.scheduler_status(now);
                let row = Row {
                    values: vec![
                        summary.ready.into(),
                        summary.running.into(),
                        summary.waiting.into(),
                        self.jobs.pending_jobs().into(),
                    ],
                };
                show_columns(["ready", "running", "waiting", "background_jobs"], vec![row])
            }
            "scheduler_tasks" => {
                let mut rows: Vec<_> = self
                    .scheduler
                    .task_statuses()
                    .into_iter()
                    .map(|task| {
                        let state = task.state(now).name();
                        let age = now.saturating_sub(task.created_at) / 1000;
                        let ready_in = if task.fire_at > now {
                            (task.fire_at - now).div_ceil(1000).into()
                        } else {
                            "-".into()
                        };
                        Row {
                            values: vec![
                                task.kind.into(),
                                task.target.into(),
                                state.into(),
                                age.into(),
                                task.retry_count.into(),
                                ready_in,
                            ],
                        }
                    })
                    .collect();
                let running =
                    |kind: &'static str, target: String, age_s: u64, state: &'static str| Row {
                        values: vec![
                            kind.into(),
                            target.into(),
                            state.into(),
                            age_s.into(),
                            "-".into(),
                            "-".into(),
                        ],
                    };
                for repair in self.cluster_stats.replica_changes() {
                    let kind = if repair.repair { "replica_repair" } else { "replica_change" };
                    let peers = |sign, mut replicas: Vec<ReplicaDesc>| {
                        replicas.sort_unstable_by_key(|replica| replica.id);
                        replicas.into_iter().map(move |replica| {
                            format!("{sign}r{}@n{}", replica.id, replica.node_id)
                        })
                    };
                    let replicas = peers('+', repair.change.incoming_replicas)
                        .chain(peers('-', repair.change.outgoing_replicas))
                        .collect::<Vec<_>>()
                        .join(", ");
                    rows.push(running(
                        kind,
                        format!("g{} {replicas}", repair.group),
                        repair.age_s,
                        "running",
                    ));
                }
                for migration in self.cluster_stats.shard_moves() {
                    if let Some(submitted) = migration.submitted_at {
                        rows.push(running(
                            "migrate_shard",
                            format!(
                                "shard {} g{} -> g{}",
                                migration.shard, migration.source, migration.destination
                            ),
                            submitted.elapsed().as_secs(),
                            "awaiting_confirmation",
                        ));
                    }
                }
                show_columns(
                    ["task", "target", "state", "age_s", "error_retries", "ready_in_s"],
                    rows,
                )
            }
            _ => unreachable!("unknown scheduling property {property}"),
        };
        Ok(ExecuteResult::Data(data))
    }

    async fn handle_show_databases(&self, show_stmt: ShowStatement) -> Result<ExecuteResult> {
        if show_stmt.from.is_some() {
            return Ok(ExecuteResult::Msg(
                "FROM clause is not required by 'databases' property".to_owned(),
            ));
        }
        let databases = self.list_database().await?;
        let tables = self.schema()?.list_table().await?;
        let groups = self.list_group().await?;
        let stats = self.show_snapshot(&groups).tables(&groups);
        let mut db_stats = HashMap::<u64, ShowMetrics>::new();
        for table in tables {
            let table_stats = stats.get(&table.id).copied().unwrap_or(ShowMetrics::unavailable(0));
            db_stats.entry(table.db).or_default().add(table_stats);
        }
        let rows = databases
            .into_iter()
            .map(|db| {
                let mut values = vec![db.id.into(), db.name.into()];
                append_table_metrics(
                    &mut values,
                    db_stats.get(&db.id).copied().unwrap_or_default(),
                );
                Row { values }
            })
            .collect();
        Ok(ExecuteResult::Data(show_columns(
            [
                "id",
                "name",
                "bytes",
                "read_qps",
                "write_qps",
                "num_shards",
                "read_bytes_per_sec",
                "write_bytes_per_sec",
                "read_units_per_sec",
                "write_units_per_sec",
            ],
            rows,
        )))
    }

    async fn handle_show_tables(&self, show_stmt: ShowStatement) -> Result<ExecuteResult> {
        let Some(db) = show_stmt.from.as_ref() else {
            return Ok(ExecuteResult::Msg(
                "the database is not specified, add it via the FROM clause".to_owned(),
            ));
        };
        let Some(db_desc) = self.get_database(db).await? else {
            return Ok(ExecuteResult::Msg(format!("database '{db}' is not exists")));
        };

        let tables = self.list_table(&db_desc).await?;
        let groups = self.list_group().await?;
        let stats = self.show_snapshot(&groups).tables(&groups);
        let table_to_row = |table: TableDesc| -> Row {
            use sekas_schema::property::*;
            let mut properties = vec![];
            for (key, value) in &table.properties {
                if !matches!(key.as_str(), REPLICATION | REPLICAS_PER_GROUP | TABLE_TYPE) {
                    properties.push(format!("{key}:{value}"));
                }
            }
            properties.sort_unstable();
            let mut values = vec![
                table.id.into(),
                table.name.into(),
                table.properties.get(TABLE_TYPE).cloned().unwrap_or_default().into(),
                table.properties.get(REPLICATION).cloned().unwrap_or_default().into(),
                table.properties.get(REPLICAS_PER_GROUP).cloned().unwrap_or_default().into(),
                properties.join(", ").into(),
            ];
            append_table_metrics(
                &mut values,
                stats.get(&table.id).copied().unwrap_or(ShowMetrics::unavailable(0)),
            );
            Row { values }
        };
        let rows = tables.into_iter().map(table_to_row).collect();
        Ok(ExecuteResult::Data(show_columns(
            [
                "id",
                "name",
                "type",
                "replication",
                "replicas_per_group",
                "properties",
                "bytes",
                "read_qps",
                "write_qps",
                "num_shards",
                "read_bytes_per_sec",
                "write_bytes_per_sec",
                "read_units_per_sec",
                "write_units_per_sec",
            ],
            rows,
        )))
    }

    async fn handle_show_groups(&self, show_stmt: ShowStatement) -> Result<ExecuteResult> {
        if show_stmt.from.is_some() {
            return Ok(ExecuteResult::Msg(
                "FROM clause is not required by 'groups' property".to_owned(),
            ));
        }
        let (groups, nodes) = tokio::try_join!(self.list_group(), self.list_node())?;
        let stats = self.show_snapshot(&groups);
        let max_age = Duration::from_secs(self.cfg.liveness_threshold_sec);
        let mut replica_issues = HashMap::<u64, Vec<String>>::new();
        for issue in self.cluster_stats.replica_issues(&groups, &nodes, &self.liveness, max_age) {
            replica_issues
                .entry(issue.group)
                .or_default()
                .push(format!("r{}@n{} {}", issue.replica, issue.node, issue.reason));
        }
        let group_to_row = |group: GroupDesc| -> Row {
            let (shard_epoch, config_epoch) = (group.epoch >> 32, group.epoch & ((1 << 32) - 1));
            let metrics = stats.group(&group);
            let mut issues = replica_issues.remove(&group.id).unwrap_or_default();
            let voters = group
                .replicas
                .iter()
                .filter(|replica| replica.role == ReplicaRole::Voter as i32)
                .count();
            let missing = self.cfg.replicas_per_group.saturating_sub(voters);
            if missing > 0 {
                issues.insert(0, format!("{missing} unfilled voter slots"));
            }
            let issues = if issues.is_empty() { "-".to_owned() } else { issues.join(", ") };
            let mut values = vec![
                group.id.into(),
                shard_epoch.into(),
                config_epoch.into(),
                group.replicas.len().into(),
                metrics.num_shards.into(),
                display_size(metrics.bytes).into(),
            ];
            append_rates(&mut values, metrics);
            values.push(issues.into());
            Row { values }
        };
        let rows = groups.into_iter().map(group_to_row).collect();
        Ok(ExecuteResult::Data(show_columns(
            [
                "id",
                "shard_epoch",
                "config_epoch",
                "num_replicas",
                "num_shards",
                "size",
                "read_qps",
                "write_qps",
                "read_bytes_per_sec",
                "write_bytes_per_sec",
                "bad_replicas",
            ],
            rows,
        )))
    }

    async fn handle_show_replicas(&self, show_stmt: ShowStatement) -> Result<ExecuteResult> {
        let Some(from) = show_stmt.from else {
            return Ok(ExecuteResult::Msg(
                "FROM clause is required by 'replicas' property".to_owned(),
            ));
        };

        let group_id: u64 = match from.parse() {
            Ok(group_id) => group_id,
            Err(_) => {
                return Ok(ExecuteResult::Msg(
                    "The value of FROM clause is not a valid u64 numeric".to_owned(),
                ));
            }
        };

        let schema = self.schema()?;
        let Some(group) = schema.get_group(group_id).await? else {
            return Ok(ExecuteResult::Msg("No such group exists".to_owned()));
        };

        let timeout = Duration::from_secs(self.cfg.heartbeat_timeout_sec);
        let transport = &self.shared.transport_manager;
        let stats = &self.cluster_stats;
        let rows = try_join_all(group.replicas.into_iter().map(|replica| {
            let schema = &schema;
            async move {
                let progress = if let Some(node) = schema.get_node(replica.node_id).await?
                    && let Ok(client) = transport.get_node_client(node.addr)
                {
                    client
                        .get_replica_progress(
                            GetReplicaProgressRequest { group_id, replica_id: replica.id },
                            timeout,
                        )
                        .await
                        .ok()
                        .flatten()
                } else {
                    None
                };
                let progress = stats.display_replica_progress(group_id, &replica, progress);
                let role = ReplicaRole::try_from(replica.role)
                    .unwrap_or_default()
                    .as_str_name()
                    .to_owned();
                Ok::<_, Error>(Row {
                    values: vec![
                        replica.id.into(),
                        replica.node_id.into(),
                        role.into(),
                        progress.map(|p| p.wal_index).into(),
                        progress.map(|p| p.committed_index).into(),
                        progress.map(|p| p.applied_index).into(),
                    ],
                })
            }
        }))
        .await?;
        Ok(ExecuteResult::Data(show_columns(
            ["id", "node_id", "role", "wal_index", "committed_index", "applied_index"],
            rows,
        )))
    }

    async fn handle_show_shards(&self, show_stmt: ShowStatement) -> Result<ExecuteResult> {
        let Some(from) = show_stmt.from else {
            return Ok(ExecuteResult::Msg(
                "FROM clause is required by 'shards' property".to_owned(),
            ));
        };

        let group_id: u64 = match from.parse() {
            Ok(group_id) => group_id,
            Err(_) => {
                return Ok(ExecuteResult::Msg(
                    "The value of FROM clause is not a valid u64 numeric".to_owned(),
                ));
            }
        };

        let Some(group) = self.get_group(group_id).await? else {
            return Ok(ExecuteResult::Msg("No such group exists".to_owned()));
        };

        let stats = self.show_snapshot(std::slice::from_ref(&group));
        let shard_to_row = |shard: ShardDesc| -> Row {
            let (start, end) = match shard.range {
                Some(range) => (range.start, range.end),
                None => (vec![], vec![]),
            };
            let (start, end) = (escape_bytes(&start), escape_bytes(&end));
            let metrics = stats.shard(group_id, shard.id);
            let mut values = vec![
                shard.id.into(),
                shard.table_id.into(),
                start.into(),
                end.into(),
                display_size(metrics.bytes).into(),
            ];
            append_rates(&mut values, metrics);
            Row { values }
        };
        let rows = group.shards.into_iter().map(shard_to_row).collect();
        Ok(ExecuteResult::Data(show_columns(
            [
                "id",
                "table_id",
                "start",
                "end",
                "size",
                "read_qps",
                "write_qps",
                "read_bytes_per_sec",
                "write_bytes_per_sec",
            ],
            rows,
        )))
    }

    async fn handle_show_nodes(&self, show_stmt: ShowStatement) -> Result<ExecuteResult> {
        if show_stmt.from.is_some() {
            return Ok(ExecuteResult::Msg(
                "FROM clause is not required by 'nodes' property".to_owned(),
            ));
        }

        let (groups, nodes) = tokio::try_join!(self.list_group(), self.list_node())?;

        let resources = self.cluster_stats.display_node_resources();
        let max_age = Duration::from_secs(self.cfg.liveness_threshold_sec);
        let mut bad_counts = HashMap::<u64, usize>::new();
        for issue in self.cluster_stats.replica_issues(&groups, &nodes, &self.liveness, max_age) {
            *bad_counts.entry(issue.node).or_default() += 1;
        }

        let node_to_row = |node: NodeDesc| -> Row {
            let capacity = node.capacity.unwrap_or_default();
            let status = NodeStatus::try_from(node.status).unwrap_or_default();
            let usage = resources.get(&node.id).copied().unwrap_or_default();
            let cpu_total = (usage.cpu_capacity > 0.).then(|| usage.cpu_capacity.to_string());
            let cpu = (usage.cpu_used.is_some() || cpu_total.is_some()).then(|| {
                let used =
                    usage.cpu_used.map_or_else(|| "-".to_owned(), |used| format!("{used:.2}"));
                format!("{used}/{}", cpu_total.as_deref().unwrap_or("-"))
            });
            Row {
                values: vec![
                    node.id.into(),
                    status.as_str_name().to_owned().into(),
                    node.addr.into(),
                    (capacity.cpu_nums as u32).into(),
                    capacity.leader_count.into(),
                    capacity.replica_count.into(),
                    cpu.into(),
                    display_storage_usage(usage.memory_used, usage.memory_capacity).into(),
                    display_storage_usage(usage.disk_used, usage.disk_capacity).into(),
                    bad_counts.get(&node.id).copied().unwrap_or(0).into(),
                ],
            }
        };
        let rows = nodes.into_iter().map(node_to_row).collect::<Vec<_>>();
        Ok(ExecuteResult::Data(show_columns(
            [
                "id",
                "status",
                "addr",
                "cpu_nums",
                "leader_count",
                "replica_count",
                "cpu",
                "mem",
                "disk",
                "bad_replica_count",
            ],
            rows,
        )))
    }

    fn show_snapshot(&self, groups: &[GroupDesc]) -> ShowSnapshot {
        let max_age = Duration::from_secs(self.cfg.liveness_threshold_sec);
        self.cluster_stats.display_snapshot(groups, max_age)
    }
}

fn show_columns<const N: usize>(columns: [&str; N], rows: Vec<Row>) -> ColumnResult {
    ColumnResult { columns: columns.into_iter().map(str::to_owned).collect(), rows }
}

fn display_storage_usage(used: u64, total: u64) -> Option<String> {
    const BYTES_PER_GIB: f64 = (1_u64 << 30) as f64;
    (total > 0).then(|| {
        format!("{:.2}/{:.2} GiB", used as f64 / BYTES_PER_GIB, total as f64 / BYTES_PER_GIB)
    })
}

fn display_balance_bytes(bytes: f64) -> String {
    for (unit, factor) in [
        ("PiB", 1_u64 << 50),
        ("TiB", 1_u64 << 40),
        ("GiB", 1_u64 << 30),
        ("MiB", 1_u64 << 20),
        ("KiB", 1_u64 << 10),
    ] {
        if bytes >= factor as f64 {
            return format!("{:.2} {unit}", bytes / factor as f64);
        }
    }
    format!("{bytes:.0} B")
}

fn append_table_metrics(values: &mut Vec<Value>, metrics: ShowMetrics) {
    values.push(metrics.bytes.into());
    values.push(metrics.rates.map(|rates| rates.read_qps).into());
    values.push(metrics.rates.map(|rates| rates.write_qps).into());
    values.push(metrics.num_shards.into());
    values.push(metrics.rates.map(|rates| rates.read_bytes_per_sec).into());
    values.push(metrics.rates.map(|rates| rates.write_bytes_per_sec).into());
    values.push(metrics.rates.and_then(|rates| rates.read_units_per_sec).into());
    values.push(metrics.rates.and_then(|rates| rates.write_units_per_sec).into());
}

fn append_rates(values: &mut Vec<Value>, metrics: ShowMetrics) {
    values.extend([
        metrics.rates.map(|rates| rates.read_qps).into(),
        metrics.rates.map(|rates| rates.write_qps).into(),
        metrics.rates.map(|rates| rates.read_bytes_per_sec).into(),
        metrics.rates.map(|rates| rates.write_bytes_per_sec).into(),
    ]);
}

/// Convert bytes size into readable unit.
fn display_size(size: Option<u64>) -> String {
    const KB: u64 = 1024;
    const MB: u64 = 1024 * KB;
    const GB: u64 = 1024 * MB;
    let Some(size) = size else { return "-".to_owned() };
    match size {
        0..KB => format!("{size}"),
        KB..MB => format!("{}KB", size / KB),
        MB..GB => format!("{}MB", size / MB),
        _ => format!("{}GB", size / GB),
    }
}
