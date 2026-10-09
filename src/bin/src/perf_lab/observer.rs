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

use std::fs;
use std::path::Path;

use anyhow::Result;

use super::LabContext;

#[derive(Default)]
pub(super) struct StorageStats {
    pub(super) flushes: u64,
    pub(super) compactions: u64,
    pub(super) stalls: u64,
    pub(super) sst_bytes: u64,
}
impl LabContext {
    pub(super) fn storage_stats(&self) -> Result<StorageStats> {
        fn visit(path: &Path, out: &mut StorageStats) -> Result<()> {
            if !path.exists() {
                return Ok(());
            }
            for entry in fs::read_dir(path)? {
                let entry = entry?;
                let path = entry.path();
                if entry.file_type()?.is_dir() {
                    visit(&path, out)?;
                    continue;
                }
                if path.extension().is_some_and(|e| e == "sst") {
                    out.sst_bytes += entry.metadata()?.len();
                }
                if path.file_name().is_some_and(|n| n.to_string_lossy().starts_with("LOG")) {
                    let content = fs::read_to_string(&path)?;
                    for line in content.lines() {
                        if let Some((_, json)) = line.split_once("EVENT_LOG_v1 ")
                            && let Ok(value) = serde_json::from_str::<serde_json::Value>(json)
                        {
                            match value["event"].as_str() {
                                Some("flush_finished") => out.flushes += 1,
                                Some("compaction_finished") => out.compactions += 1,
                                _ => {}
                            }
                        }
                        if line.contains("Stalling") || line.contains("Stopping writes") {
                            out.stalls += 1;
                        }
                    }
                }
            }
            Ok(())
        }
        let mut stats = StorageStats::default();
        for node in self.nodes.keys() {
            visit(&self.node_root_dir(*node).join("db"), &mut stats)?;
        }
        Ok(stats)
    }
}

pub(super) fn metric_counter(name: &str) -> f64 {
    prometheus::gather()
        .iter()
        .filter(|f| f.get_name() == name)
        .flat_map(|f| f.get_metric())
        .map(|m| m.get_counter().get_value())
        .sum()
}
