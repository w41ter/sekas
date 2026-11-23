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

use std::collections::HashMap;
use std::sync::OnceLock;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering::Relaxed;
use std::time::Instant;

use dashmap::DashMap;
use sekas_api::server::v1::group_request_union::Request;
use sekas_api::server::v1::group_response_union::Response;
use sekas_api::server::v1::{ShardTraffic, ShardWriteRequest, Value, WriteResult, write_result};

pub(super) struct ReplicaTraffic {
    started_at: Instant,
    shards: OnceLock<DashMap<u64, TrafficCounters>>,
}

#[derive(Default)]
struct TrafficCounters {
    reads: AtomicU64,
    writes: AtomicU64,
    read_bytes: AtomicU64,
    write_bytes: AtomicU64,
    read_units: AtomicU64,
    write_units: AtomicU64,
}

impl Default for ReplicaTraffic {
    fn default() -> Self {
        Self { started_at: Instant::now(), shards: OnceLock::new() }
    }
}

impl ReplicaTraffic {
    pub(super) fn elapsed_nanos(&self) -> u64 {
        self.started_at.elapsed().as_nanos() as u64
    }

    pub(super) fn snapshot(&self, shard_id: u64) -> ShardTraffic {
        match self.shards.get().and_then(|shards| shards.get(&shard_id)) {
            Some(counters) => ShardTraffic {
                reads: counters.reads.load(Relaxed),
                writes: counters.writes.load(Relaxed),
                read_bytes: counters.read_bytes.load(Relaxed),
                write_bytes: counters.write_bytes.load(Relaxed),
                read_units: Some(counters.read_units.load(Relaxed)),
                write_units: Some(counters.write_units.load(Relaxed)),
            },
            None => {
                ShardTraffic { read_units: Some(0), write_units: Some(0), ..Default::default() }
            }
        }
    }

    pub(super) fn retain_shards(&self, mut keep: impl FnMut(u64) -> bool) {
        if let Some(shards) = self.shards.get() {
            shards.retain(|shard_id, _| keep(*shard_id));
        }
    }

    pub(super) fn record(&self, request: &Request, response: &Response) {
        match (request, response) {
            (Request::Get(req), Response::Get(resp)) => {
                self.record_read(req.shard_id, resp.value.as_ref().map_or(0, value_bytes));
            }
            (Request::Scan(req), Response::Scan(resp)) => {
                // Migration pulls and watch initialization use this raw scan mode.
                if req.include_raw_data && req.ignore_txn_intent && req.allow_scan_moving_shard {
                    return;
                }
                let bytes = resp
                    .data
                    .iter()
                    .map(|entry| {
                        entry.user_key.len() as u64
                            + entry.values.iter().map(value_bytes).sum::<u64>()
                    })
                    .sum();
                self.record_read(req.shard_id, bytes);
            }
            (Request::Write(req), Response::Write(resp))
                if resp.deletes.iter().chain(&resp.puts).all(write_succeeded) =>
            {
                if let Some(bytes) = write_bytes(req) {
                    self.record_write(req.shard_id, bytes);
                }
            }
            (Request::LocalTxnWrite(req), Response::LocalTxnWrite(resp))
                if resp.writes.iter().all(write_succeeded) =>
            {
                // Local transactions are atomic and return one result per key.
                self.record_batch(req.writes.iter());
            }
            (Request::WriteIntent(req), Response::WriteIntent(resp)) => {
                // Intent responses have one result per request entry, including forwarded
                // writes.
                let successful = req
                    .writes
                    .iter()
                    .zip(&resp.writes)
                    .filter_map(|(write, result)| write_succeeded(result).then_some(write));
                self.record_batch(successful);
            }
            _ => {}
        }
    }

    fn record_batch<'a>(&self, writes: impl Iterator<Item = &'a ShardWriteRequest>) {
        let mut successful = writes.filter_map(|write| Some((write.shard_id, write_bytes(write)?)));
        let Some((first_shard, mut first_bytes)) = successful.next() else { return };
        // HashMap allocates on first insert, so single-shard batches stay
        // allocation-free.
        let mut other_shards = HashMap::<u64, u64>::new();
        for (shard, bytes) in successful {
            if shard == first_shard {
                first_bytes += bytes;
            } else {
                *other_shards.entry(shard).or_default() += bytes;
            }
        }
        self.record_write(first_shard, first_bytes);
        for (shard, bytes) in other_shards {
            self.record_write(shard, bytes);
        }
    }

    fn record_read(&self, shard_id: u64, bytes: u64) {
        self.update(shard_id, |counters| {
            counters.reads.fetch_add(1, Relaxed);
            counters.read_bytes.fetch_add(bytes, Relaxed);
            counters.read_units.fetch_add(request_units(bytes), Relaxed);
        });
    }

    fn record_write(&self, shard_id: u64, bytes: u64) {
        self.update(shard_id, |counters| {
            counters.writes.fetch_add(1, Relaxed);
            counters.write_bytes.fetch_add(bytes, Relaxed);
            counters.write_units.fetch_add(request_units(bytes), Relaxed);
        });
    }

    fn update(&self, shard_id: u64, record: impl FnOnce(&TrafficCounters)) {
        let shards = self.shards.get_or_init(|| DashMap::with_shard_amount(4));
        if let Some(counters) = shards.get(&shard_id) {
            record(&counters);
            return;
        }
        record(&shards.entry(shard_id).or_default());
    }
}

fn request_units(bytes: u64) -> u64 {
    bytes.div_ceil(1024).max(1)
}

fn value_bytes(value: &Value) -> u64 {
    value.content.as_ref().map_or(0, |content| content.len() as u64)
}

fn write_succeeded(result: &WriteResult) -> bool {
    matches!(result.result, Some(write_result::Result::Response(_)))
}

fn write_bytes(write: &ShardWriteRequest) -> Option<u64> {
    if write.puts.is_empty() && write.deletes.is_empty() {
        return None;
    }
    let puts = write.puts.iter().map(|put| (put.key.len() + put.value.len()) as u64).sum::<u64>();
    let deletes = write.deletes.iter().map(|delete| delete.key.len() as u64).sum::<u64>();
    Some(puts + deletes)
}

#[cfg(test)]
mod tests {
    use sekas_api::server::v1::{
        Error, LocalTxnWriteRequest, LocalTxnWriteResponse, PutRequest, WriteIntentRequest,
        WriteIntentResponse, WriteResponse,
    };

    use super::*;

    #[test]
    fn counts_successful_write_payload_once_per_shard_per_request() {
        let traffic = ReplicaTraffic::default();
        let put = PutRequest { key: vec![1], value: vec![2, 3], ..Default::default() };
        let write = |shard_id, puts| ShardWriteRequest { shard_id, puts, ..Default::default() };
        let success = WriteResult::ok(WriteResponse::default());
        let failure = WriteResult::err(Error::default());

        let req = Request::LocalTxnWrite(LocalTxnWriteRequest {
            writes: vec![write(1, vec![put.clone(), put.clone()]), write(2, vec![put.clone()])],
            ..Default::default()
        });
        traffic.record(
            &req,
            &Response::LocalTxnWrite(LocalTxnWriteResponse {
                writes: vec![success.clone(), success.clone(), failure.clone()],
                ..Default::default()
            }),
        );
        assert_eq!(traffic.snapshot(1).write_units, Some(0));
        assert_eq!(traffic.snapshot(1).writes, 0);

        traffic.record(
            &req,
            &Response::LocalTxnWrite(LocalTxnWriteResponse {
                writes: vec![success.clone(); 3],
                ..Default::default()
            }),
        );
        let req = Request::WriteIntent(WriteIntentRequest {
            writes: vec![
                write(1, vec![put.clone()]),
                write(1, vec![put.clone()]),
                write(2, vec![put]),
            ],
            ..Default::default()
        });
        traffic.record(
            &req,
            &Response::WriteIntent(WriteIntentResponse {
                writes: vec![success.clone(), success, failure],
            }),
        );
        assert_eq!(
            traffic.snapshot(1),
            ShardTraffic {
                writes: 2,
                write_bytes: 12,
                read_units: Some(0),
                write_units: Some(2),
                ..Default::default()
            }
        );
        assert_eq!(
            traffic.snapshot(2),
            ShardTraffic {
                writes: 1,
                write_bytes: 3,
                read_units: Some(0),
                write_units: Some(1),
                ..Default::default()
            }
        );
    }

    #[test]
    fn request_units_round_each_request() {
        let traffic = ReplicaTraffic::default();
        for bytes in [0, 1, 1024, 1025, 2048, 2049] {
            traffic.record_read(1, bytes);
        }
        assert_eq!(traffic.snapshot(1).read_units, Some(10));
        traffic.record_write(1, 1);
        traffic.record_write(1, 2047);
        traffic.record_write(2, 1024);
        traffic.record_write(2, 1024);
        assert_eq!(traffic.snapshot(1).write_bytes, traffic.snapshot(2).write_bytes);
        assert_eq!(traffic.snapshot(1).write_units, Some(3));
        assert_eq!(traffic.snapshot(2).write_units, Some(2));
    }
}
