// Copyright 2024-present The Sekas Authors.
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

use futures::lock::Mutex;
use futures::StreamExt;
use sekas_client::{AppError, AppResult, WriteBatchRequest, WriteBuilder};

pub use crate::consts::*;
use crate::etcd::v3::*;

/// A structure to serve etcd request.
pub struct KvStore {
    client: sekas_client::SekasClient,
    handle: Mutex<Option<Arc<SekasHandle>>>,
}

struct SekasHandle {
    db: sekas_client::Database,
    table_id: u64,
}

impl KvStore {
    /// Create or return the handle of the underlying store used by etcd kv
    /// store.
    async fn ensure_handle(&self) -> AppResult<Arc<SekasHandle>> {
        let mut handle_guard = self.handle.lock().await;
        if let Some(handle) = handle_guard.as_ref() {
            return Ok(handle.clone());
        }

        // create and take database.
        let db = match self.client.open_database(DATABASE_NAME.to_owned()).await {
            Ok(db) => db,
            Err(AppError::NotFound(_)) => {
                self.client.create_database(DATABASE_NAME.to_owned()).await?
            }
            Err(err) => return Err(err),
        };
        let table = match db.open_table(KV_NAME.to_owned()).await {
            Ok(table) => table,
            Err(AppError::NotFound(_)) => db.create_table(KV_NAME.to_owned()).await?,
            Err(err) => return Err(err),
        };

        // TODO(walter) configure the split boundary of the etcd:kv table.

        let handle = Arc::new(SekasHandle { db, table_id: table.id });
        *handle_guard = Some(handle.clone());
        Ok(handle)
    }

    pub async fn put(&self, request: &PutRequest) -> AppResult<PutResponse> {
        let kv_store = self.ensure_handle().await?;

        loop {
            let range_req = sekas_client::RangeRequest {
                table_id: kv_store.table_id,
                range: sekas_client::Range::Prefix(request.key.clone()),
                ..Default::default()
            };
            let mut range_stream = kv_store.db.range(range_req, None)?;
            let mut _exists = false;
            let mut version: u64 = 0;
            let mut version_value_version: u64 = 0;
            let mut lease_id: u64 = 0;
            let mut _prev_value: Vec<u8> = Vec::default();
            while let Some(values) = range_stream.next().await {
                let values = values?;
                for value_set in values {
                    let Some(got_value) = value_set.values.last() else { continue };
                    let (etcd_user_key, record) = keys::revert_record_key(&value_set.user_key);
                    if etcd_user_key != request.key {
                        return Err(AppError::Internal(
                            "the result of range scan with prefix is out of range".into(),
                        ));
                    }
                    _exists = true;
                    if &record == keys::VERSION {
                        version_value_version = got_value.version;
                    }
                    if let Some(content) = &got_value.content {
                        match &record {
                            keys::LEASE => lease_id = values::u64_from_bytes(content)?,
                            keys::VALUE => {
                                _prev_value = content.clone();
                            }
                            keys::VERSION => {
                                version = values::u64_from_bytes(content)?;
                            }
                            _ => {
                                return Err(AppError::Internal(
                                    format!("unknown record name: {record:?}").into(),
                                ));
                            }
                        }
                    }
                }
            }

            let version_key = keys::record_key(&request.key, keys::VERSION);
            let lease_key = keys::record_key(&request.key, keys::LEASE);
            let value_key = keys::record_key(&request.value, keys::VALUE);

            let wb = WriteBatchRequest {
                puts: vec![
                    (
                        kv_store.table_id,
                        WriteBuilder::new(version_key)
                            .expect_version(version_value_version)
                            .ensure_put(values::u64_as_bytes(version + 1)),
                    ),
                    (
                        kv_store.table_id,
                        WriteBuilder::new(lease_key).ensure_put(values::u64_as_bytes(lease_id)),
                    ),
                    (
                        kv_store.table_id,
                        WriteBuilder::new(value_key).ensure_put(request.value.clone()),
                    ),
                ],
                ..Default::default()
            };
            let resp = match kv_store.db.write_batch(wb).await {
                Ok(resp) => resp,
                Err(AppError::CasFailed(_, _, _)) => continue,
                Err(err) => return Err(err),
            };
            return Ok(PutResponse {
                header: Some(ResponseHeader {
                    revision: resp.version as i64,
                    ..Default::default()
                }),
                ..Default::default()
            });
        }
    }

    pub async fn range(&self, request: &RangeRequest) -> AppResult<RangeResponse> {
        let kv_store = self.ensure_handle().await?;

        if let Some(range_end) = &request.range_end {
            let range_req = sekas_client::RangeRequest {
                table_id: kv_store.table_id,
                range: sekas_client::Range::Range {
                    begin: Some(request.key.clone()),
                    end: Some(range_end.clone()),
                },
                ..Default::default()
            };
            let mut range_stream = kv_store.db.range(range_req, None).unwrap();
            while let Some(_values) = range_stream.next().await {}
        } else {
            // only look up key
        }
        todo!()
    }

    pub async fn delete_range(&self, _req: &DeleteRangeRequest) -> AppResult<DeleteRangeResponse> {
        todo!()
    }

    pub async fn txn(&self, _req: &TxnRequest) -> AppResult<TxnResponse> {
        todo!()
    }
}

mod keys {
    pub const LEASE: &[u8; 8] = b"LEASE\x00\x00\x00";
    pub const VALUE: &[u8; 8] = b"VALUE\x00\x00\x00";
    pub const VERSION: &[u8; 8] = b"VERSION\x00";

    /// Generate record key with the memcomparable format.
    pub fn record_key(key: &[u8], record: &[u8; 8]) -> Vec<u8> {
        use std::io::{Cursor, Read};

        debug_assert!(!key.is_empty());
        let actual_len = (((key.len() - 1) / 8) + 1) * 9;
        let buf_len = core::mem::size_of::<[u8; 8]>() + actual_len;
        let mut buf = Vec::with_capacity(buf_len);
        let mut cursor = Cursor::new(key);
        while !cursor.is_empty() {
            let mut group = [0u8; 8];
            let mut size = cursor.read(&mut group[..]).unwrap() as u8;
            debug_assert_ne!(size, 0);
            if size == 8 && !cursor.is_empty() {
                size += 1;
            }
            buf.extend_from_slice(group.as_slice());
            buf.push(b'0' + size);
        }
        buf.extend_from_slice(record.as_slice());
        buf
    }

    /// Revert the record key, return the user key and record name.
    pub fn revert_record_key(key: &[u8]) -> (Vec<u8>, [u8; 8]) {
        use std::io::{Cursor, Read};

        const L: usize = core::mem::size_of::<[u8; 8]>();
        let len = key.len();
        debug_assert!(len > L);
        let encoded_user_key = &key[..(len - L)];

        debug_assert_eq!(encoded_user_key.len() % 9, 0);
        let num_groups = encoded_user_key.len() / 9;
        let mut buf = Vec::with_capacity(num_groups * 8);
        let mut cursor = Cursor::new(encoded_user_key);
        while !cursor.is_empty() {
            let mut group = [0u8; 9];
            let _ = cursor.read(&mut group[..]).unwrap();
            let num_element = std::cmp::min((group[8] - b'0') as usize, 8);
            buf.extend_from_slice(&group[..num_element]);
        }

        let mut record_name = [0u8; 8];
        record_name.copy_from_slice(&key[(len - L)..]);
        (buf, record_name)
    }
}

mod values {
    use sekas_rock::num::*;

    use super::{AppError, AppResult};

    pub fn u64_as_bytes(val: u64) -> Vec<u8> {
        val.to_be_bytes().as_slice().to_owned()
    }

    pub fn u64_from_bytes(bytes: &[u8]) -> AppResult<u64> {
        decode_u64(bytes).ok_or_else(|| AppError::Internal("invalid numeric format".into()))
    }
}
