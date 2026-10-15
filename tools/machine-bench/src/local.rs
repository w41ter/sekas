use std::hint::black_box;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::{Arc, Mutex, mpsc};
use std::thread;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};

use crate::histogram::{Histogram, Summary, measure, measure_batched};
use crate::report::BenchmarkResult;

fn result<F>(
    category: &str,
    benchmark: &str,
    parameter: impl Into<String>,
    run: F,
) -> BenchmarkResult
where
    F: FnOnce() -> Result<Summary>,
{
    let parameter = parameter.into();
    match run() {
        Ok(summary) => BenchmarkResult::ok(category, benchmark, parameter, summary),
        Err(error) => BenchmarkResult::skipped(category, benchmark, parameter, &error),
    }
}

pub(crate) fn run(warmup: Duration, duration: Duration) -> Vec<BenchmarkResult> {
    let mut results = Vec::new();

    results.push(result("runtime", "clock_gettime", "Instant::now", || {
        let mut last = Instant::now();
        measure_batched(warmup, duration, 256, 0, |count| {
            for _ in 0..count {
                last = black_box(Instant::now());
            }
            black_box(last);
            Ok(())
        })
    }));

    results.push(result("sync", "mutex_uncontended", "1 thread", || {
        let mutex = Mutex::new(0_u64);
        measure_batched(warmup, duration, 256, 0, |count| {
            for _ in 0..count {
                let mut value = mutex.lock().unwrap();
                *value = black_box(*value).wrapping_add(1);
            }
            Ok(())
        })
    }));

    results.push(result("scheduler", "thread_ping_pong", "2 threads", || {
        thread_ping_pong(warmup, duration)
    }));
    results.push(result("scheduler", "tokio_task_ping_pong", "2 tasks", || {
        tokio_ping_pong(warmup, duration)
    }));

    for size in [64_usize, 4096, 65536] {
        results.push(result("network", "tcp_loopback", format_size(size), || {
            tcp_loopback(warmup, duration, size)
        }));
    }

    for size in [4096_usize, 65536] {
        results.push(result("memory", "memcpy", format_size(size), || {
            memory_copy(warmup, duration, size)
        }));
        results.push(result("cpu", "crc32", format_size(size), || crc32(warmup, duration, size)));
    }

    results
}

fn format_size(size: usize) -> String {
    if size >= 1024 { format!("{} KiB", size / 1024) } else { format!("{size} B") }
}

fn thread_ping_pong(warmup: Duration, duration: Duration) -> Result<Summary> {
    let (request_tx, request_rx) = mpsc::sync_channel::<bool>(0);
    let (response_tx, response_rx) = mpsc::sync_channel::<()>(0);
    let worker = thread::spawn(move || {
        while request_rx.recv().unwrap_or(false) {
            if response_tx.send(()).is_err() {
                break;
            }
        }
    });

    let measured = measure(warmup, duration, 0, || {
        request_tx.send(true).context("send thread ping")?;
        response_rx.recv().context("receive thread pong")?;
        Ok(())
    });
    let _ = request_tx.send(false);
    worker.join().map_err(|_| anyhow::anyhow!("thread ping-pong worker panicked"))?;
    measured
}

fn tokio_ping_pong(warmup: Duration, duration: Duration) -> Result<Summary> {
    let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build()?;
    runtime.block_on(async move {
        let (request_tx, mut request_rx) = tokio::sync::mpsc::channel::<bool>(1);
        let (response_tx, mut response_rx) = tokio::sync::mpsc::channel::<()>(1);
        let worker = tokio::spawn(async move {
            while request_rx.recv().await.unwrap_or(false) {
                if response_tx.send(()).await.is_err() {
                    break;
                }
            }
        });

        let warmup_deadline = Instant::now() + warmup;
        while Instant::now() < warmup_deadline {
            request_tx.send(true).await?;
            response_rx.recv().await.context("receive Tokio warmup pong")?;
        }

        let mut histogram = Histogram::default();
        let started = Instant::now();
        let deadline = started + duration;
        let mut operations = 0;
        while Instant::now() < deadline {
            let before = Instant::now();
            request_tx.send(true).await?;
            response_rx.recv().await.context("receive Tokio pong")?;
            histogram.observe_n(before.elapsed().as_nanos().min(u128::from(u64::MAX)) as u64, 1);
            operations += 1;
        }
        request_tx.send(false).await?;
        worker.await?;
        Ok(histogram.summarize(operations, started.elapsed(), 0))
    })
}

fn tcp_loopback(warmup: Duration, duration: Duration, size: usize) -> Result<Summary> {
    let listener = TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0))?;
    let address = listener.local_addr()?;
    let worker = thread::spawn(move || -> std::io::Result<()> {
        let (mut stream, _) = listener.accept()?;
        stream.set_nodelay(true)?;
        let mut payload = vec![0_u8; size];
        loop {
            match stream.read_exact(&mut payload) {
                Ok(()) => stream.write_all(&payload)?,
                Err(error) if error.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(()),
                Err(error) => return Err(error),
            }
        }
    });

    let mut stream = TcpStream::connect(address)?;
    stream.set_nodelay(true)?;
    let tx = vec![0xa5_u8; size];
    let mut rx = vec![0_u8; size];
    let measured = measure(warmup, duration, (size * 2) as u64, || {
        stream.write_all(&tx)?;
        stream.read_exact(&mut rx)?;
        Ok(())
    });
    drop(stream);
    worker
        .join()
        .map_err(|_| anyhow::anyhow!("TCP echo worker panicked"))?
        .context("TCP echo worker")?;
    if tx != rx {
        anyhow::bail!("TCP echo payload mismatch");
    }
    measured
}

fn memory_copy(warmup: Duration, duration: Duration, size: usize) -> Result<Summary> {
    let source = vec![0xa5_u8; size];
    let mut destination = vec![0_u8; size];
    measure_batched(warmup, duration, 256, size as u64, |count| {
        for _ in 0..count {
            destination.copy_from_slice(black_box(&source));
            black_box(&destination);
        }
        Ok(())
    })
}

fn crc32(warmup: Duration, duration: Duration, size: usize) -> Result<Summary> {
    let payload = Arc::new(vec![0xa5_u8; size]);
    measure_batched(warmup, duration, 64, size as u64, |count| {
        for _ in 0..count {
            black_box(crc32fast::hash(black_box(payload.as_slice())));
        }
        Ok(())
    })
}
