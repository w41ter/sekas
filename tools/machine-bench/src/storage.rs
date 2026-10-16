use std::alloc::{Layout, alloc_zeroed, dealloc};
use std::fs::{File, OpenOptions};
use std::os::fd::AsRawFd;
#[cfg(target_os = "linux")]
use std::os::unix::fs::OpenOptionsExt;
use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::{Context, Result};

use crate::histogram::{Summary, measure};
use crate::report::BenchmarkResult;

const BLOCK_SIZE: usize = 4096;

struct TestFile(PathBuf);

impl Drop for TestFile {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.0);
    }
}

struct AlignedBuffer {
    ptr: *mut u8,
    layout: Layout,
}

impl AlignedBuffer {
    fn new(len: usize) -> Result<Self> {
        let layout = Layout::from_size_align(len, BLOCK_SIZE)?;
        // SAFETY: layout has non-zero size and valid power-of-two alignment.
        let ptr = unsafe { alloc_zeroed(layout) };
        if ptr.is_null() {
            anyhow::bail!("allocate {len} byte aligned buffer");
        }
        Ok(Self { ptr, layout })
    }
}

impl Drop for AlignedBuffer {
    fn drop(&mut self) {
        // SAFETY: ptr was allocated with this exact layout and has not been freed.
        unsafe { dealloc(self.ptr, self.layout) };
    }
}

pub(crate) fn run(
    disk_dir: &Path,
    file_size_mib: u64,
    warmup: Duration,
    duration: Duration,
) -> Result<Vec<BenchmarkResult>> {
    let test_file = prepare_test_file(disk_dir, file_size_mib)?;
    let file_size = file_size_mib * 1024 * 1024;
    let mut results = Vec::new();

    results.push(case("odirect_read", "4 KiB random", || {
        direct_io(&test_file.0, file_size, false, false, warmup, duration)
    }));
    results.push(case("odirect_write", "4 KiB", || {
        direct_io(&test_file.0, file_size, true, false, warmup, duration)
    }));
    results.push(case("odirect_dsync_write", "4 KiB", || {
        direct_io(&test_file.0, file_size, true, true, warmup, duration)
    }));
    results.push(case("page_cache_read", "4 KiB random", || {
        page_cache_read(&test_file.0, file_size, warmup, duration)
    }));
    results.push(case("fdatasync_write", "4 KiB", || {
        fdatasync_write(&test_file.0, file_size, 1, warmup, duration)
    }));
    results.push(case("fdatasync_write", "16 x 4 KiB", || {
        fdatasync_write(&test_file.0, file_size, 16, warmup, duration)
    }));

    Ok(results)
}

fn case<F>(benchmark: &str, parameter: &str, run: F) -> BenchmarkResult
where
    F: FnOnce() -> Result<Summary>,
{
    match run() {
        Ok(summary) => BenchmarkResult::ok("storage", benchmark, parameter, summary),
        Err(error) => BenchmarkResult::skipped("storage", benchmark, parameter, &error),
    }
}

fn prepare_test_file(disk_dir: &Path, file_size_mib: u64) -> Result<TestFile> {
    let unique = format!(
        ".sekas-machine-bench-{}-{}",
        std::process::id(),
        std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH)?.as_nanos()
    );
    let path = disk_dir.join(unique);
    let mut options = OpenOptions::new();
    options.read(true).write(true).create_new(true);
    let file = options
        .open(&path)
        .with_context(|| format!("create storage benchmark file at {}", path.display()))?;
    let guard = TestFile(path);
    let size = file_size_mib * 1024 * 1024;
    file.set_len(size)?;

    // Materialize sparse extents before timing and warm page-cache read data.
    let payload = vec![0xa5_u8; 1024 * 1024];
    for offset in (0..size).step_by(payload.len()) {
        write_all_at(&file, &payload[..payload.len().min((size - offset) as usize)], offset)?;
    }
    file.sync_all()?;
    Ok(guard)
}

fn open_direct(path: &Path, dsync: bool) -> Result<File> {
    #[cfg(target_os = "linux")]
    {
        let mut options = OpenOptions::new();
        options
            .read(true)
            .write(true)
            .custom_flags(libc::O_DIRECT | if dsync { libc::O_DSYNC } else { 0 });
        return options
            .open(path)
            .with_context(|| format!("open {} with O_DIRECT", path.display()));
    }

    #[cfg(not(target_os = "linux"))]
    {
        let _ = (path, dsync);
        anyhow::bail!("O_DIRECT benchmark is only supported on Linux")
    }
}

fn direct_io(
    path: &Path,
    file_size: u64,
    write: bool,
    dsync: bool,
    warmup: Duration,
    duration: Duration,
) -> Result<Summary> {
    let file = open_direct(path, dsync)?;
    let buffer = AlignedBuffer::new(BLOCK_SIZE)?;
    let blocks = file_size / BLOCK_SIZE as u64;
    let mut sequence = 0x9e3779b97f4a7c15_u64;
    let mut write_block = 0_u64;
    measure(warmup, duration, BLOCK_SIZE as u64, || {
        let offset = if write {
            let block = write_block;
            write_block = (write_block + 1) % blocks;
            block * BLOCK_SIZE as u64
        } else {
            sequence ^= sequence << 13;
            sequence ^= sequence >> 7;
            sequence ^= sequence << 17;
            sequence % blocks * BLOCK_SIZE as u64
        };
        // SAFETY: the buffer and offset satisfy O_DIRECT alignment requirements, and
        // the file remains open for the duration of the call.
        let result = unsafe {
            if write {
                libc::pwrite(file.as_raw_fd(), buffer.ptr.cast(), BLOCK_SIZE, offset as libc::off_t)
            } else {
                libc::pread(file.as_raw_fd(), buffer.ptr.cast(), BLOCK_SIZE, offset as libc::off_t)
            }
        };
        if result != BLOCK_SIZE as isize {
            if result < 0 {
                return Err(std::io::Error::last_os_error()).context("O_DIRECT I/O");
            }
            anyhow::bail!("short O_DIRECT I/O: {result} of {BLOCK_SIZE} bytes");
        }
        Ok(())
    })
}

fn page_cache_read(
    path: &Path,
    file_size: u64,
    warmup: Duration,
    duration: Duration,
) -> Result<Summary> {
    let file = File::open(path)?;
    let blocks = file_size / BLOCK_SIZE as u64;
    let mut buffer = vec![0_u8; BLOCK_SIZE];
    let mut sequence = 0x243f6a8885a308d3_u64;
    measure(warmup, duration, BLOCK_SIZE as u64, || {
        sequence ^= sequence << 13;
        sequence ^= sequence >> 7;
        sequence ^= sequence << 17;
        let offset = sequence % blocks * BLOCK_SIZE as u64;
        read_exact_at(&file, &mut buffer, offset)
    })
}

fn fdatasync_write(
    path: &Path,
    file_size: u64,
    blocks_per_sync: u64,
    warmup: Duration,
    duration: Duration,
) -> Result<Summary> {
    let file = OpenOptions::new().write(true).open(path)?;
    let payload = vec![0x5a_u8; BLOCK_SIZE];
    let total_blocks = file_size / BLOCK_SIZE as u64;
    let mut block = 0_u64;
    measure(warmup, duration, blocks_per_sync * BLOCK_SIZE as u64, || {
        for _ in 0..blocks_per_sync {
            write_all_at(&file, &payload, block * BLOCK_SIZE as u64)?;
            block = (block + 1) % total_blocks;
        }
        file.sync_data().context("sync file data")?;
        Ok(())
    })
}

fn write_all_at(file: &File, mut data: &[u8], mut offset: u64) -> Result<()> {
    use std::os::unix::fs::FileExt;
    while !data.is_empty() {
        let written = file.write_at(data, offset)?;
        if written == 0 {
            anyhow::bail!("zero-length positional write");
        }
        data = &data[written..];
        offset += written as u64;
    }
    Ok(())
}

fn read_exact_at(file: &File, mut data: &mut [u8], mut offset: u64) -> Result<()> {
    use std::os::unix::fs::FileExt;
    while !data.is_empty() {
        let read = file.read_at(data, offset)?;
        if read == 0 {
            anyhow::bail!("unexpected EOF during positional read");
        }
        data = &mut data[read..];
        offset += read as u64;
    }
    Ok(())
}
