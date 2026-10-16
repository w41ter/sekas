// Copyright 2026-present The Sekas Authors.
// Licensed under the Apache License, Version 2.0.

use std::collections::{BTreeMap, BTreeSet};
use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail, ensure};
use sekas_api::server::v1::group_request_union::Request;
use sekas_api::server::v1::group_response_union::Response;
use sekas_api::server::v1::{LocalTxnWriteRequest, ShardWriteRequest, write_result};
use sekas_client::{Database, Error as ClientError, WriteBuilder};
use sekas_rock::time::timestamp_nanos;

use crate::perf_lab::report::CaseReport;
use crate::perf_lab::workload::{
    LoadModel, Operation, Target, Workload, WorkloadReport, spawn_workload,
};
use crate::perf_lab::{LabContext, RunProfile};

#[derive(Clone, Copy)]
pub(super) struct FixtureSpec {
    pub(super) keys: u64,
    pub(super) value_size: usize,
}

pub(super) fn fixture_spec(
    lab: &LabContext,
    smoke: FixtureSpec,
    regression: FixtureSpec,
    characterization: FixtureSpec,
    stress: FixtureSpec,
) -> FixtureSpec {
    match lab.config.profile {
        RunProfile::Smoke => smoke,
        RunProfile::Regression => regression,
        RunProfile::Characterization => characterization,
        RunProfile::Stress => stress,
    }
}

pub(super) fn closed_loop(concurrency: usize) -> LoadModel {
    LoadModel::ClosedLoop { concurrency }
}

pub(super) fn open_loop(offered_qps: f64, max_inflight: usize) -> LoadModel {
    LoadModel::OpenLoop { offered_qps, max_inflight }
}

pub(super) async fn fixture(
    lab: &LabContext,
    name: &str,
    seed: bool,
    fixture_spec: FixtureSpec,
) -> Result<(Database, Target)> {
    let db = lab.database().await?;
    let table = lab.table(&db, name).await?;
    let target = Target { table: table.id, prefix: "key-".to_owned(), keys: fixture_spec.keys };
    let (_, shard) = lab.group_for_key(target.table, &target.key(0)).await?;
    ensure!(
        shard.range.as_ref().is_some_and(|r| r.start.is_empty() && r.end.is_empty()),
        "fixture requires an unsplit table"
    );
    if seed {
        seed_random_target(&db, &target, fixture_spec.value_size).await?;
    }
    Ok((db, target))
}

async fn seed_random_target(db: &Database, target: &Target, value_size: usize) -> Result<()> {
    use rand::rngs::StdRng;
    use rand::{RngExt, SeedableRng};
    let mut rng = StdRng::seed_from_u64(0x5ECA5);
    let mut first = None;
    // Distinct high-entropy values keep compression from turning the disk
    // working set into a tiny repeated-value fixture.
    for offset in (0..target.keys).step_by(64) {
        let mut txn = db.begin_txn();
        for index in offset..(offset + 64).min(target.keys) {
            let mut value = vec![0; value_size];
            rng.fill(&mut value[..]);
            if index == 0 {
                first = Some(value.clone());
            }
            txn.put(target.table, WriteBuilder::new(target.key(index)).ensure_put(value));
        }
        txn.commit().await?;
    }
    ensure!(db.get(target.table, target.key(0)).await? == first, "random seed mismatch");
    Ok(())
}

pub(super) async fn seed_target(
    lab: &LabContext,
    db: &Database,
    target: &Target,
    value: &[u8],
) -> Result<()> {
    for offset in (0..target.keys).step_by(64) {
        let (group_id, shard) = lab.group_for_key(target.table, &target.key(offset)).await?;
        let mut puts = Vec::new();
        for index in offset..(offset + 64).min(target.keys) {
            puts.push(WriteBuilder::new(target.key(index)).ensure_put(value.to_vec()));
        }
        let request =
            Request::Write(ShardWriteRequest { shard_id: shard.id, puts, ..Default::default() });
        let response = lab
            .group(group_id)
            .request(&request)
            .await
            .with_context(|| format!("seed write table {} offset {offset}", target.table))?;
        let Response::Write(response) = response else {
            bail!(
                "seed write table {} offset {offset} returned an unexpected response",
                target.table
            );
        };
        ensure!(
            response.puts.len() == (offset + 64).min(target.keys).saturating_sub(offset) as usize
                && response
                    .puts
                    .iter()
                    .all(|result| matches!(result.result, Some(write_result::Result::Response(_)))),
            "seed write table {} offset {offset} was not fully applied: {:?}",
            target.table,
            response.puts
        );
    }
    ensure!(
        db.get(target.table, target.key(0))
            .await
            .with_context(|| format!("seed read table {}", target.table))?
            == Some(value.to_vec()),
        "seed value mismatch"
    );
    Ok(())
}

async fn wait_shard_local_txn_ready(lab: &LabContext, target: &Target) -> Result<()> {
    let deadline = Instant::now() + Duration::from_secs(lab.config.execution.event_timeout_secs);
    loop {
        let key = target.key(0);
        let (group_id, shard) = lab.group_for_key(target.table, &key).await?;
        let request = Request::LocalTxnWrite(LocalTxnWriteRequest {
            commit_version: timestamp_nanos(),
            writes: vec![ShardWriteRequest {
                shard_id: shard.id,
                puts: vec![WriteBuilder::new(key).ensure_nop()],
                ..Default::default()
            }],
        });
        match lab.group(group_id).request(&request).await {
            Ok(Response::LocalTxnWrite(_)) => return Ok(()),
            Ok(response) => bail!("local transaction readiness returned {response:?}"),
            Err(
                ClientError::LocalTxnNotAllowed
                | ClientError::EpochNotMatch(_)
                | ClientError::GroupNotAccessable(_),
            ) if Instant::now() < deadline => {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
            Err(err) => return Err(err.into()),
        }
    }
}

pub(super) fn workload(
    lab: &LabContext,
    name: &str,
    target: Target,
    operation: Operation,
    value_size: usize,
    load: LoadModel,
) -> Workload {
    Workload {
        name: name.to_owned(),
        operation,
        targets: vec![target],
        load,
        value_size,
        timeout: Duration::from_secs(lab.config.execution.request_timeout_secs),
        hold: Duration::ZERO,
        retries: 0,
        hot_probability: 0.0,
        hot_keys: 1,
        allow_conflicts: false,
        allow_overload: false,
        disjoint_keys: false,
        trace_every_n: crate::perf_lab::trace_every_n(),
    }
}

pub(super) async fn measure(
    lab: &mut LabContext,
    db: &Database,
    spec: Workload,
) -> Result<WorkloadReport> {
    measure_for(lab, db, spec, Duration::from_secs(lab.config.execution.duration_secs)).await
}

pub(super) async fn measure_for(
    lab: &mut LabContext,
    db: &Database,
    spec: Workload,
    duration: Duration,
) -> Result<WorkloadReport> {
    if lab.config.execution.warmup_secs > 0
        && !matches!(spec.operation, Operation::Delete)
        && !matches!(spec.load, LoadModel::Once)
    {
        let mut warmup_spec = spec.clone();
        if matches!(spec.operation, Operation::Insert) {
            for target in &mut warmup_spec.targets {
                target.prefix.push_str("warmup-");
            }
        }
        let warmup = spawn_workload(db.clone(), warmup_spec);
        tokio::time::sleep(Duration::from_secs(lab.config.execution.warmup_secs)).await;
        let report = warmup.stop().await;
        validate(lab, &report)?;
    }
    lab.mark(format!("{}_start", spec.name));
    let handle = spawn_workload(db.clone(), spec.clone());
    let deadline = Instant::now() + duration;
    while !handle.finished() && Instant::now() < deadline {
        tokio::time::sleep(
            deadline.saturating_duration_since(Instant::now()).min(Duration::from_millis(20)),
        )
        .await;
    }
    let report = handle.stop().await;
    lab.mark(format!("{}_end", spec.name));
    validate(lab, &report)?;
    Ok(report)
}

pub(super) fn validate(_lab: &LabContext, report: &WorkloadReport) -> Result<()> {
    ensure!(report.operations > 0, "{} produced no operations", report.name);
    let unexpected = report.unexpected_failures();
    ensure!(
        report.successes > 0 || (report.failures == report.operations && unexpected == 0),
        "{} produced no successes; errors: {:?}",
        report.name,
        report.errors
    );
    ensure!(unexpected == 0, "{} unexpected failures: {:?}", report.name, report.errors);
    Ok(())
}

pub(super) async fn user_groups(lab: &LabContext) -> Result<Vec<u64>> {
    let body = lab.client.handle_statement("show groups").await?;
    let sekas_parser::ExecuteResult::Data(groups) = serde_json::from_slice(&body)? else {
        bail!("SHOW GROUPS returned no data");
    };
    let mut ids: Vec<_> = groups
        .rows
        .iter()
        .filter_map(|r| r.values[0].as_u64())
        .filter(|id| *id != 0)
        .filter(|id| {
            lab.router
                .find_group(*id)
                .is_ok_and(|g| g.leader_state.is_some() && g.replicas.len() >= 3)
        })
        .collect();
    ids.sort_unstable();
    Ok(ids)
}

pub(super) async fn ensure_groups(lab: &mut LabContext, count: usize) -> Result<Vec<u64>> {
    let deadline = Instant::now() + Duration::from_secs(lab.config.execution.event_timeout_secs);
    loop {
        let groups = user_groups(lab).await?;
        if groups.len() >= count {
            return Ok(groups[..count].to_vec());
        }
        if Instant::now() >= deadline {
            bail!("only {} ready groups, need {count}", groups.len());
        }
        // The allocator's desired group count includes the root group.  To
        // obtain `count` user groups, the lab therefore needs at least one
        // additional node beyond that count.
        if lab.nodes.len() < count.saturating_add(1).max(3) {
            lab.add_server().await?;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

pub(super) async fn layout(
    lab: &mut LabContext,
    name: &str,
    participants: usize,
    fixture_spec: FixtureSpec,
) -> Result<(Database, Vec<Target>)> {
    ensure!(
        participants > 0 && participants as u64 <= fixture_spec.keys,
        "layout needs at least one key per participating group"
    );
    let groups = ensure_groups(lab, participants).await?;
    let mut targets = Vec::new();
    let db = lab.database().await?;
    let seed_value = vec![b's'; fixture_spec.value_size];
    for (i, group) in groups.iter().enumerate() {
        let (_, mut target) = fixture(lab, &format!("{name}_{i}"), false, fixture_spec).await?;
        target.keys = (fixture_spec.keys / participants as u64).max(1);
        lab.migrate_shard_to_group(target.table, &target.key(0), *group)
            .await
            .with_context(|| format!("place table {} in group {group}", target.table))?;
        targets.push(target);
    }
    // Hold the final placement at the same global steady-state boundary used
    // by the rest of perf-lab before starting the measured workload.
    lab.wait_cluster_stable().await?;
    for target in &targets {
        // Fixture setup uses committed shard writes instead of transactions:
        // a newly moved shard may still forward briefly, and fixture creation
        // must not leave asynchronous intents for the measured workload.
        seed_target(lab, &db, target, &seed_value).await?;
        wait_shard_local_txn_ready(lab, target).await?;
    }
    let mut actual = BTreeSet::new();
    for target in &targets {
        actual.insert(lab.group_for_key(target.table, &target.key(0)).await?.0);
    }
    ensure!(
        actual.len() == participants,
        "transaction participants were not placed in distinct groups"
    );
    Ok((db, targets))
}

pub(super) fn observations(report: &CaseReport, names: &[&str]) -> BTreeMap<String, f64> {
    names.iter().map(|name| ((*name).to_owned(), report.counter_delta(name))).collect()
}

pub(super) async fn raw_versions(
    lab: &LabContext,
    target: &Target,
) -> Result<Vec<sekas_api::server::v1::Value>> {
    use sekas_api::server::v1::ShardScanRequest;
    use sekas_api::server::v1::group_request_union::Request;
    use sekas_api::server::v1::group_response_union::Response;
    let key = target.key(0);
    let (group, shard) = lab.group_for_key(target.table, &key).await?;
    let response = lab
        .group(group)
        .request(&Request::Scan(ShardScanRequest {
            shard_id: shard.id,
            start_version: u64::MAX - 1,
            limit: 1,
            start_key: Some(key.clone()),
            end_key: Some(key.clone()),
            include_raw_data: true,
            ..Default::default()
        }))
        .await?;
    match response {
        Response::Scan(response) => Ok(response
            .data
            .into_iter()
            .find(|v| v.user_key == key)
            .map(|v| v.values)
            .unwrap_or_default()),
        _ => bail!("unexpected raw scan response"),
    }
}
