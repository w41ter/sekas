// Copyright 2026-present The Sekas Authors.
// Licensed under the Apache License, Version 2.0.

use std::fs;
use std::path::Path;

use anyhow::{Context as _, Result, bail};
use serde::{Deserialize, Serialize};

/// A machine-level primitive that may be used by a case latency model.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Primitive {
    TokioTaskPingPong,
    TcpLoopback64B,
    TcpLoopback4KiB,
    PageCacheRead4KiB,
    ODirectWrite4KiB,
}

impl Primitive {
    pub(super) fn name(self) -> &'static str {
        match self {
            Self::TokioTaskPingPong => "tokio_task_ping_pong",
            Self::TcpLoopback64B => "tcp_loopback_64b",
            Self::TcpLoopback4KiB => "tcp_loopback_4kib",
            Self::PageCacheRead4KiB => "page_cache_read_4kib",
            Self::ODirectWrite4KiB => "odirect_write_4kib",
        }
    }

    fn selector(self) -> (&'static str, &'static str, &'static str) {
        match self {
            Self::TokioTaskPingPong => ("scheduler", "tokio_task_ping_pong", "2 tasks"),
            Self::TcpLoopback64B => ("network", "tcp_loopback", "64 B"),
            Self::TcpLoopback4KiB => ("network", "tcp_loopback", "4 KiB"),
            Self::PageCacheRead4KiB => ("storage", "page_cache_read", "4 KiB random"),
            Self::ODirectWrite4KiB => ("storage", "odirect_write", "4 KiB"),
        }
    }
}

/// The JSON emitted by `sekas-machine-bench`.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct MachineProfile {
    pub(crate) schema_version: u32,
    pub(crate) tool_version: String,
    pub(crate) duration_secs: u64,
    pub(crate) warmup_secs: u64,
    pub(crate) environment: MachineEnvironment,
    pub(crate) results: Vec<MachineBenchmarkResult>,
    /// The source file is attached by perf-lab and is not present in the
    /// machine-bench report itself.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) source: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct MachineEnvironment {
    pub(crate) hostname: String,
    pub(crate) kernel: String,
    pub(crate) cpu_model: String,
    pub(crate) logical_cpus: usize,
    pub(crate) cpu_governor: String,
    pub(crate) load_average: String,
    pub(crate) cgroup: String,
    pub(crate) disk_dir: String,
    pub(crate) disk_device: u64,
    pub(crate) filesystem_type: String,
    pub(crate) filesystem_block_size: i64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct MachineBenchmarkResult {
    pub(crate) category: String,
    pub(crate) benchmark: String,
    pub(crate) parameter: String,
    pub(crate) status: MachineBenchmarkStatus,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) summary: Option<MachineSummary>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) message: Option<String>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum MachineBenchmarkStatus {
    Ok,
    Skipped,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct MachineSummary {
    pub(crate) samples: u64,
    pub(crate) operations: u64,
    pub(crate) elapsed_secs: f64,
    pub(crate) avg_us: f64,
    pub(crate) p50_us: f64,
    pub(crate) p99_us: f64,
    pub(crate) p999_us: f64,
    pub(crate) max_us: f64,
    pub(crate) ops_per_sec: f64,
    pub(crate) mib_per_sec: f64,
    /// Histogram buckets are deliberately ignored by perf-lab's cost model.
    /// Keep the field generic so machine-bench can evolve its bucket payload.
    #[serde(default)]
    pub(crate) buckets: Vec<serde_json::Value>,
}

impl MachineProfile {
    pub(crate) fn load(path: &Path) -> Result<Self> {
        let bytes =
            fs::read(path).with_context(|| format!("read machine profile {}", path.display()))?;
        let mut profile: MachineProfile = serde_json::from_slice(&bytes)
            .with_context(|| format!("parse machine profile {}", path.display()))?;
        if profile.schema_version != 1 {
            bail!("unsupported machine profile schema {}, expected 1", profile.schema_version);
        }
        profile.source = Some(path.display().to_string());
        Ok(profile)
    }

    pub(super) fn p50_us(&self, primitive: Primitive) -> Result<f64> {
        let (category, benchmark, parameter) = primitive.selector();
        let matched = self
            .results
            .iter()
            .filter(|result| {
                result.category == category
                    && result.benchmark == benchmark
                    && result.parameter == parameter
            })
            .collect::<Vec<_>>();
        let [result] = matched.as_slice() else {
            bail!(
                "machine profile requires exactly one {} result, found {}",
                primitive.name(),
                matched.len()
            );
        };
        if result.status != MachineBenchmarkStatus::Ok {
            bail!(
                "machine primitive {} was skipped: {}",
                primitive.name(),
                result.message.as_deref().unwrap_or("no reason reported")
            );
        }
        let summary = result.summary.as_ref().ok_or_else(|| {
            anyhow::anyhow!("machine primitive {} has no summary", primitive.name())
        })?;
        if !summary.p50_us.is_finite() || summary.p50_us < 0.0 {
            bail!("machine primitive {} has invalid p50", primitive.name());
        }
        Ok(summary.p50_us)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn profile() -> MachineProfile {
        serde_json::from_value(serde_json::json!({
            "schema_version": 1,
            "tool_version": "0.5.0",
            "duration_secs": 3,
            "warmup_secs": 1,
            "environment": {
                "hostname": "test",
                "kernel": "test",
                "cpu_model": "test",
                "logical_cpus": 8,
                "cpu_governor": "performance",
                "load_average": "0 0 0",
                "cgroup": "test",
                "disk_dir": "/tmp",
                "disk_device": 1,
                "filesystem_type": "test",
                "filesystem_block_size": 4096
            },
            "results": [{
                "category": "network",
                "benchmark": "tcp_loopback",
                "parameter": "64 B",
                "status": "ok",
                "summary": {
                    "samples": 10,
                    "operations": 10,
                    "elapsed_secs": 1.0,
                    "avg_us": 20.0,
                    "p50_us": 19.5,
                    "p99_us": 30.0,
                    "p999_us": 40.0,
                    "max_us": 50.0,
                    "ops_per_sec": 10.0,
                    "mib_per_sec": 0.0,
                    "buckets": []
                }
            }]
        }))
        .unwrap()
    }

    #[test]
    fn selects_a_machine_primitive_by_semantics() {
        assert_eq!(profile().p50_us(Primitive::TcpLoopback64B).unwrap(), 19.5);
        assert!(profile().p50_us(Primitive::TcpLoopback4KiB).is_err());
    }
}
