// Copyright 2024-present The Sekas Authors.
// Copyright 2022 The Engula Authors.
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

use std::sync::Arc;
use std::thread::{Builder, JoinHandle};
use std::time::Instant;

use futures::StreamExt;
use futures::channel::{mpsc, oneshot};
use futures::stream::FusedStream;
use log::error;

use crate::Result;
use crate::raftgroup::metrics::{
    RAFTGROUP_LOG_WRITER_BATCH_BYTES_SIZE, RAFTGROUP_LOG_WRITER_BATCH_REQUESTS_SIZE,
    RAFTGROUP_LOG_WRITER_QUEUE_DURATION_SECONDS, RAFTGROUP_LOG_WRITER_WRITE_DURATION_SECONDS,
    elapsed_seconds,
};

#[derive(Clone)]
pub struct LogWriter {
    sender: mpsc::Sender<LogRequest>,
    _inner: Arc<WriterInner>,
}

struct WriterInner {
    handle: Option<JoinHandle<()>>,
}

struct LogRequest {
    batch: raft_engine::LogBatch,
    enqueued_at: Instant,
    sender: oneshot::Sender<LogResponse>,
}

type LogResponse = Result<(), String>;

impl LogWriter {
    pub fn new(max_io_batch_size: u64, engine: Arc<raft_engine::Engine>) -> LogWriter {
        let (join_handle, sender) = start_log_writer(max_io_batch_size, engine);
        LogWriter { sender, _inner: Arc::new(WriterInner { handle: Some(join_handle) }) }
    }

    pub fn submit(&mut self, batch: raft_engine::LogBatch) -> oneshot::Receiver<LogResponse> {
        let (sender, receiver) = oneshot::channel();
        let req = LogRequest { batch, enqueued_at: Instant::now(), sender };
        match self.sender.start_send(req) {
            Ok(()) => {}
            Err(err) => {
                // Ignore disconnect error, since a available Sender is still hold.
                if err.is_full() {
                    panic!(
                        "submit log request to log writer: channel capacity must enough to receive requests"
                    );
                }
            }
        }
        receiver
    }
}

impl Drop for LogWriter {
    fn drop(&mut self) {
        let _ = self.sender.clone();
    }
}

impl Drop for WriterInner {
    fn drop(&mut self) {
        if let Some(handle) = self.handle.take() {
            handle.join().unwrap_or_default();
        }
    }
}

fn start_log_writer(
    max_io_batch_size: u64,
    engine: Arc<raft_engine::Engine>,
) -> (std::thread::JoinHandle<()>, mpsc::Sender<LogRequest>) {
    // Each worker sends at most one request, 1024 is large enough.
    let (sender, receiver) = mpsc::channel::<LogRequest>(1024);
    let handle = Builder::new()
        .name("log:writer".to_owned())
        .spawn(move || {
            futures::executor::block_on(async move {
                log_writer_main(max_io_batch_size as usize, engine, receiver).await;
            })
        })
        .unwrap();
    (handle, sender)
}

async fn log_writer_main(
    max_io_batch_size: usize,
    engine: Arc<raft_engine::Engine>,
    receiver: mpsc::Receiver<LogRequest>,
) {
    let mut receiver = receiver;
    let mut estimated_size = 0.0;
    while !receiver.is_terminated() {
        let Some(req) = receiver.next().await else {
            break;
        };

        RAFTGROUP_LOG_WRITER_QUEUE_DURATION_SECONDS.observe(elapsed_seconds(req.enqueued_at));
        let LogRequest { batch, sender, .. } = req;
        let mut log_batch = batch;
        let mut senders = vec![sender];
        estimated_size = estimate_size(estimated_size, log_batch.approximate_size());
        while log_batch.approximate_size() + (estimated_size as usize) <= max_io_batch_size {
            let Ok(Some(mut req)) = receiver.try_next() else {
                break;
            };
            RAFTGROUP_LOG_WRITER_QUEUE_DURATION_SECONDS.observe(elapsed_seconds(req.enqueued_at));
            estimated_size = estimate_size(estimated_size, req.batch.approximate_size());
            log_batch.merge(&mut req.batch).expect("size wont exceeds u32::MAX");
            senders.push(req.sender);
        }

        RAFTGROUP_LOG_WRITER_BATCH_REQUESTS_SIZE.observe(senders.len() as f64);
        RAFTGROUP_LOG_WRITER_BATCH_BYTES_SIZE.observe(log_batch.approximate_size() as f64);
        let write_start = Instant::now();
        match engine.write(&mut log_batch, false) {
            Ok(_) => {
                RAFTGROUP_LOG_WRITER_WRITE_DURATION_SECONDS.observe(elapsed_seconds(write_start));
                for sender in senders {
                    sender.send(Ok(())).unwrap_or_default();
                }
            }
            Err(err) => {
                RAFTGROUP_LOG_WRITER_WRITE_DURATION_SECONDS.observe(elapsed_seconds(write_start));
                // Since `raft_engine::Error` is not `Clone`, converts err to string for message
                // passing.
                error!("write log batch to raft engine: {err:?}");
                let msg = err.to_string();
                for sender in senders {
                    sender.send(Err(msg.clone())).unwrap_or_default();
                }
            }
        };
    }
}

#[inline]
fn estimate_size(origin: f64, approximate_size: usize) -> f64 {
    const BETA: f64 = 0.99;
    BETA * origin + (1.0 - BETA) * (approximate_size as f64)
}
