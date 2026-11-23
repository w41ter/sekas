// Copyright 2026-present The Sekas Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::ffi::{CStr, CString};
use std::os::unix::ffi::OsStrExt;
use std::path::Path;
use std::time::Instant;

use sekas_api::server::v1::NodeResourceUsage;
use sysinfo::{ProcessRefreshKind, ProcessesToUpdate, System};

#[derive(Default)]
pub(super) struct ResourceSampler {
    system: System,
    last_cpu_refresh: Option<Instant>,
    cpu: Option<(f64, Option<f64>)>,
}

impl ResourceSampler {
    pub(super) fn sample(&mut self, data_path: &Path) -> NodeResourceUsage {
        let now = Instant::now();
        self.system.refresh_memory();
        if self
            .last_cpu_refresh
            .is_none_or(|last| now.duration_since(last) >= sysinfo::MINIMUM_CPU_UPDATE_INTERVAL)
        {
            self.system.refresh_cpu_usage();
            let process_cpu = self.refresh_process();
            if self.last_cpu_refresh.replace(now).is_some() && !self.system.cpus().is_empty() {
                let host_cpu = f64::from(self.system.global_cpu_usage())
                    * self.system.cpus().len() as f64
                    / 100.;
                self.cpu = Some((host_cpu, process_cpu));
            }
        }
        let memory_capacity = self.system.total_memory();
        let process_memory_used = sysinfo::get_current_pid()
            .ok()
            .and_then(|pid| self.system.process(pid))
            .map_or(0, |process| process.memory());
        let (disk_used, disk_capacity) = CString::new(data_path.as_os_str().as_bytes())
            .ok()
            .and_then(|path| disk_usage(&path))
            .unwrap_or_default();
        NodeResourceUsage {
            cpu_used: self.cpu.map(|(host, _)| host),
            cpu_capacity: self.system.cpus().len() as f64,
            process_cpu_used: self.cpu.and_then(|(_, process)| process),
            memory_used: memory_capacity.saturating_sub(self.system.available_memory()),
            memory_capacity,
            process_memory_used,
            disk_used,
            disk_capacity,
        }
    }

    fn refresh_process(&mut self) -> Option<f64> {
        let pid = sysinfo::get_current_pid().ok()?;
        self.system.refresh_processes_specifics(
            ProcessesToUpdate::Some(&[pid]),
            true,
            ProcessRefreshKind::nothing().with_cpu().with_memory(),
        );
        Some(f64::from(self.system.process(pid)?.cpu_usage()) / 100.)
    }
}

fn disk_usage(path: &CStr) -> Option<(u64, u64)> {
    let mut info = std::mem::MaybeUninit::<libc::statvfs>::uninit();
    // statvfs initializes the output structure on success.
    if unsafe { libc::statvfs(path.as_ptr(), info.as_mut_ptr()) } != 0 {
        return None;
    }
    let info = unsafe { info.assume_init() };
    let block_size = info.f_frsize as u64;
    let capacity = (info.f_blocks as u64).saturating_mul(block_size);
    let available = (info.f_bavail as u64).saturating_mul(block_size);
    Some((capacity.saturating_sub(available), capacity))
}
