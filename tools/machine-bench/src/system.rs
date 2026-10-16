use std::fs;
use std::mem::MaybeUninit;
use std::os::unix::fs::MetadataExt;
use std::path::Path;

use anyhow::{Context, Result};

use crate::report::Environment;

fn read_trimmed(path: &str) -> String {
    fs::read_to_string(path).map(|s| s.trim().to_owned()).unwrap_or_else(|_| "unknown".into())
}

fn cpu_model() -> String {
    fs::read_to_string("/proc/cpuinfo")
        .ok()
        .and_then(|contents| {
            contents.lines().find_map(|line| {
                let (key, value) = line.split_once(':')?;
                (key.trim() == "model name").then(|| value.trim().to_owned())
            })
        })
        .unwrap_or_else(|| "unknown".into())
}

pub(crate) fn environment(disk_dir: &Path) -> Result<Environment> {
    let disk_dir = disk_dir
        .canonicalize()
        .with_context(|| format!("canonicalize disk directory {}", disk_dir.display()))?;
    let metadata = fs::metadata(&disk_dir)
        .with_context(|| format!("read metadata for {}", disk_dir.display()))?;
    if !metadata.is_dir() {
        anyhow::bail!("disk directory is not a directory: {}", disk_dir.display());
    }

    let mut stat = MaybeUninit::<libc::statfs>::uninit();
    let path = std::ffi::CString::new(disk_dir.as_os_str().as_encoded_bytes())?;
    // SAFETY: path is NUL terminated and stat points to writable memory.
    if unsafe { libc::statfs(path.as_ptr(), stat.as_mut_ptr()) } != 0 {
        return Err(std::io::Error::last_os_error()).context("statfs disk directory");
    }
    // SAFETY: successful statfs initialized the structure.
    let stat = unsafe { stat.assume_init() };

    Ok(Environment {
        hostname: read_trimmed("/etc/hostname"),
        kernel: read_trimmed("/proc/sys/kernel/osrelease"),
        cpu_model: cpu_model(),
        logical_cpus: std::thread::available_parallelism().map(usize::from).unwrap_or(1),
        cpu_governor: read_trimmed("/sys/devices/system/cpu/cpu0/cpufreq/scaling_governor"),
        load_average: read_trimmed("/proc/loadavg"),
        cgroup: read_trimmed("/proc/self/cgroup"),
        disk_dir,
        disk_device: metadata.dev(),
        filesystem_type: format!("0x{:x}", stat.f_type),
        filesystem_block_size: stat.f_bsize.into(),
    })
}
