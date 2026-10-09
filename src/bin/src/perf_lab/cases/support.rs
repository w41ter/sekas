// Copyright 2026-present The Sekas Authors.
// Licensed under the Apache License, Version 2.0.

use std::collections::{BTreeMap, BTreeSet};
use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail, ensure};
use sekas_client::{Database, WriteBuilder};

use crate::perf_lab::LabContext;
use crate::perf_lab::report::CaseReport;
use crate::perf_lab::workload::{Operation, Target, Workload, WorkloadReport, spawn_workload};

pub(super) async fn fixture(
    lab: &LabContext,
    name: &str,
    seed: bool,
) -> Result<(Database, Target)> {
    let db = lab.database().await?;
    let table = lab.table(&db, name).await?;
    let target =
        Target { table: table.id, prefix: "key-".to_owned(), keys: lab.config.workload.key_space };
    let (_, shard) = lab.group_for_key(target.table, &target.key(0)).await?;
    ensure!(
        shard.range.as_ref().is_some_and(|r| r.start.is_empty() && r.end.is_empty()),
        "fixture requires an unsplit table"
    );
    if seed {
        if lab.config.workload.read_working_set == crate::perf_lab::config::ReadWorkingSet::Disk {
            seed_disk_target(&db, &target, lab.config.workload.value_size).await?;
        } else {
            seed_target(&db, &target, &vec![b's'; lab.config.workload.value_size]).await?;
        }
    }
    Ok((db, target))
}

async fn seed_disk_target(db: &Database, target: &Target, value_size: usize) -> Result<()> {
    use rand::rngs::SmallRng;
    use rand::{RngCore, SeedableRng};
    let mut rng = SmallRng::seed_from_u64(0x5ECA5);
    let mut first = None;
    // Distinct high-entropy values keep compression from turning the disk
    // working set into a tiny repeated-value fixture.
    for offset in (0..target.keys).step_by(64) {
        let mut txn = db.begin_txn();
        for index in offset..(offset + 64).min(target.keys) {
            let mut value = vec![0; value_size];
            rng.fill_bytes(&mut value);
            if index == 0 {
                first = Some(value.clone());
            }
            txn.put(target.table, WriteBuilder::new(target.key(index)).ensure_put(value));
        }
        txn.commit().await?;
    }
    ensure!(db.get(target.table, target.key(0)).await? == first, "disk seed mismatch");
    Ok(())
}

pub(super) async fn seed_target(db: &Database, target: &Target, value: &[u8]) -> Result<()> {
    for offset in (0..target.keys).step_by(64) {
        let mut txn = db.begin_txn();
        for index in offset..(offset + 64).min(target.keys) {
            txn.put(target.table, WriteBuilder::new(target.key(index)).ensure_put(value.to_vec()));
        }
        txn.commit()
            .await
            .with_context(|| format!("seed commit table {} offset {offset}", target.table))?;
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

pub(super) fn workload(
    lab: &LabContext,
    name: &str,
    target: Target,
    operation: Operation,
) -> Workload {
    let cfg = &lab.config.workload;
    Workload {
        name: name.to_owned(),
        operation,
        targets: vec![target],
        concurrency: cfg.concurrency,
        value_size: cfg.value_size,
        timeout: Duration::from_secs(cfg.request_timeout_secs),
        offered_qps: None,
        hold: Duration::ZERO,
        retries: 0,
        hot_probability: 0.0,
        hot_keys: 1,
        allow_conflicts: false,
        allow_overload: false,
        disjoint_keys: false,
        router: lab.router.clone(),
    }
}

pub(super) async fn measure(
    lab: &mut LabContext,
    db: &Database,
    spec: Workload,
) -> Result<WorkloadReport> {
    measure_for(lab, db, spec, Duration::from_secs(lab.config.workload.duration_secs)).await
}

pub(super) async fn measure_for(
    lab: &mut LabContext,
    db: &Database,
    spec: Workload,
    duration: Duration,
) -> Result<WorkloadReport> {
    if lab.config.workload.warmup_secs > 0 && !matches!(spec.operation, Operation::Delete) {
        let mut warmup_spec = spec.clone();
        if matches!(spec.operation, Operation::Insert) {
            for target in &mut warmup_spec.targets {
                target.prefix.push_str("warmup-");
            }
        }
        let warmup = spawn_workload(db.clone(), lab.client.clone(), warmup_spec);
        tokio::time::sleep(Duration::from_secs(lab.config.workload.warmup_secs)).await;
        let report = warmup.stop().await;
        validate(lab, &report)?;
    }
    lab.mark(format!("{}_start", spec.name));
    let handle = spawn_workload(db.clone(), lab.client.clone(), spec.clone());
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

pub(super) fn validate(lab: &LabContext, report: &WorkloadReport) -> Result<()> {
    ensure!(report.operations > 0, "{} produced no operations", report.name);
    let unexpected = report.unexpected_failures();
    ensure!(
        report.successes > 0 || (report.failures == report.operations && unexpected == 0),
        "{} produced no successes and unexpected errors",
        report.name
    );
    ensure!(
        unexpected as f64 / report.operations as f64 <= lab.config.report.max_failure_rate,
        "{} unexpected failures: {:?}",
        report.name,
        report.errors
    );
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
    let deadline = Instant::now() + Duration::from_secs(lab.config.workload.event_timeout_secs);
    loop {
        let groups = user_groups(lab).await?;
        if groups.len() >= count {
            return Ok(groups[..count].to_vec());
        }
        if Instant::now() >= deadline {
            bail!("only {} ready groups, need {count}", groups.len());
        }
        if lab.nodes.len() < count.max(3) {
            lab.add_server().await?;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

pub(super) async fn layout(
    lab: &mut LabContext,
    name: &str,
    participants: usize,
) -> Result<(Database, Vec<Target>)> {
    ensure!(
        participants > 0 && participants as u64 <= lab.config.workload.key_space,
        "layout needs at least one key per participating group"
    );
    let groups = ensure_groups(lab, participants).await?;
    let mut targets = Vec::new();
    let db = lab.database().await?;
    for (i, group) in groups.iter().enumerate() {
        let (_, mut target) = fixture(lab, &format!("{name}_{i}"), false).await?;
        target.keys = (lab.config.workload.key_space / participants as u64).max(1);
        lab.migrate_shard_to_group(target.table, &target.key(0), *group)
            .await
            .with_context(|| format!("place table {} in group {group}", target.table))?;
        seed_target(&db, &target, &vec![b's'; lab.config.workload.value_size]).await?;
        targets.push(target);
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
    names.iter().map(|name| ((*name).to_owned(), report.counter_delta_contains(name))).collect()
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
