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
        let _ = config_stmt;
        Ok(ExecuteResult::Msg("the CONFIG statement is not supported yet".to_owned()))
    }

    async fn handle_show_stmt(&self, show_stmt: ShowStatement) -> Result<ExecuteResult> {
        match show_stmt.property.as_str() {
            "databases" => self.handle_show_databases(show_stmt).await,
            "tables" => self.handle_show_tables(show_stmt).await,
            "groups" => self.handle_show_groups(show_stmt).await,
            "replicas" => self.handle_show_replicas(show_stmt).await,
            "shards" => self.handle_show_shards(show_stmt).await,
            "nodes" => self.handle_show_nodes(show_stmt).await,
            "balancer" | "scheduler" | "scheduler_tasks" => self.handle_show_scheduling(show_stmt),
            others => Ok(ExecuteResult::Msg(format!("unknown property: {others}"))),
        }
    }

    fn handle_show_scheduling(&self, show_stmt: ShowStatement) -> Result<ExecuteResult> {
        let property = show_stmt.property.as_str();
        if show_stmt.from.is_some() {
            return Ok(ExecuteResult::Msg(format!(
                "FROM clause is not supported by '{property}' property"
            )));
        }
        self.schema()?;

        let now = timestamp_millis();
        let data = match property {
            "balancer" => {
                let snapshot = self.scheduler.balance_status();
                let rows = snapshot
                    .iter()
                    .map(|(policy, report)| {
                        let (status, decision) = match report {
                            BalanceDecision::Unknown => ("unknown", "awaiting first check"),
                            BalanceDecision::Disabled => ("disabled", "-"),
                            BalanceDecision::Deferred(reason) => ("deferred", *reason),
                            BalanceDecision::NoAction(detail) => ("no_action", detail.as_str()),
                            BalanceDecision::ActionNeeded(detail) => {
                                ("action_needed", detail.as_str())
                            }
                            BalanceDecision::Unsupported(detail) => {
                                ("unsupported", detail.as_str())
                            }
                            BalanceDecision::Error(detail) => ("error", detail.as_str()),
                        };
                        Row { values: vec![policy.name().into(), status.into(), decision.into()] }
                    })
                    .collect();
                show_columns(["policy", "status", "decision"], rows)
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
                let rows = self
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
        let groups = self.list_group().await?;
        let stats = self.show_snapshot(&groups);
        let group_to_row = |group: GroupDesc| -> Row {
            let (shard_epoch, config_epoch) = (group.epoch >> 32, group.epoch & ((1 << 32) - 1));
            let metrics = stats.group(&group);
            let mut values = vec![
                group.id.into(),
                shard_epoch.into(),
                config_epoch.into(),
                group.replicas.len().into(),
                metrics.num_shards.into(),
                display_size(metrics.bytes).into(),
            ];
            append_rates(&mut values, metrics);
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

        let nodes = self.list_node().await?;

        let columns = ["id", "status", "addr", "cpu_nums", "leader_count", "replica_count"]
            .into_iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>();

        let node_to_row = |node: NodeDesc| -> Row {
            let capacity = node.capacity.unwrap_or_default();
            let status = NodeStatus::try_from(node.status).unwrap_or_default();
            Row {
                values: vec![
                    node.id.into(),
                    status.as_str_name().to_owned().into(),
                    node.addr.into(),
                    (capacity.cpu_nums as u32).into(),
                    capacity.leader_count.into(),
                    capacity.replica_count.into(),
                ],
            }
        };
        let rows = nodes.into_iter().map(node_to_row).collect::<Vec<_>>();
        Ok(ExecuteResult::Data(ColumnResult { columns, rows }))
    }

    fn show_snapshot(&self, groups: &[GroupDesc]) -> ShowSnapshot {
        let max_age = Duration::from_secs(self.cfg.liveness_threshold_sec);
        self.get_cluster_stats().show_snapshot(groups, max_age)
    }
}

fn show_columns<const N: usize>(columns: [&str; N], rows: Vec<Row>) -> ColumnResult {
    ColumnResult { columns: columns.into_iter().map(str::to_owned).collect(), rows }
}

fn append_table_metrics(values: &mut Vec<Value>, metrics: ShowMetrics) {
    values.push(metrics.bytes.into());
    values.push(metrics.rates.map(|rates| rates.read_qps).into());
    values.push(metrics.rates.map(|rates| rates.write_qps).into());
    values.push(metrics.num_shards.into());
    values.push(metrics.rates.map(|rates| rates.read_bytes_per_sec).into());
    values.push(metrics.rates.map(|rates| rates.write_bytes_per_sec).into());
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
