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
use prost::Message;
use sekas_client::{AppError, AppResult, WriteBatchRequest, WriteBuilder};
use sekas_rock::lexical::{lexical_next, lexical_next_boundary};

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
    /// Create [`KvStore`] instance.
    pub fn new(client: sekas_client::SekasClient) -> Self {
        KvStore { client, handle: Mutex::new(None) }
    }

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
        let table = match db.open_table(KV_TABLE.to_owned()).await {
            Ok(table) => table,
            Err(AppError::NotFound(_)) => db.create_table(KV_TABLE.to_owned()).await?,
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
            // read old value.
            let mut key_value = match self.get_key_value(&kv_store, request.key.clone()).await? {
                Some(key_value) => key_value,
                None if request.ignore_lease || request.ignore_value => {
                    // See below links for details:
                    // - https://github.com/etcd-io/etcd/blob/main/server/etcdserver/api/v3rpc/util.go#L96
                    // - https://github.com/etcd-io/etcd/blob/main/api/v3rpc/rpctypes/error.go
                    todo!("return key not found")
                }
                None => KeyValue::default(),
            };

            let mut put_resp = PutResponse::default();
            if request.prev_kv {
                put_resp.prev_kv = Some(key_value.clone());
            }
            if request.lease != 0 {
                // with lease
                todo!("support put with lease");
            } else {
                // put without lease.

                // apply request
                if !request.ignore_value {
                    key_value.value = request.value.clone();
                }
                if !request.ignore_lease {
                    if key_value.lease != 0 {
                        todo!("detach key from origin lease");
                    }
                    if request.lease != 0 {
                        todo!("attach key to lease")
                    }
                    key_value.lease = request.lease;
                }
            }

            key_value.version += 1;
            let mod_revision = key_value.mod_revision;
            let value = ValueRecord::from(key_value).encode_to_vec();
            let put_value = if mod_revision == 0 {
                WriteBuilder::new(request.key.clone()).expect_not_exists().ensure_put(value)
            } else {
                WriteBuilder::new(request.key.clone())
                    .expect_version(mod_revision as u64)
                    .ensure_put(value)
            };

            let wb = WriteBatchRequest {
                puts: vec![(kv_store.table_id, put_value)],
                ..Default::default()
            };
            let resp = match kv_store.db.write_batch(wb).await {
                Ok(resp) => resp,
                Err(AppError::CasFailed(_, _, _)) => continue,
                Err(err) => return Err(err),
            };
            put_resp.header = Some(ResponseHeader::with_revision(resp.version as i64));
            return Ok(put_resp);
        }
    }

    pub async fn range(&self, request: &RangeRequest) -> AppResult<RangeResponse> {
        let kv_store = self.ensure_handle().await?;
        if request.sort_order != 0 {
            return Err(AppError::InvalidArgument(
                "RangeRequest::sort_order is not supported yet".into(),
            ));
        }
        if request.sort_target != 0 {
            return Err(AppError::InvalidArgument(
                "RangeRequest::sort_target is not supported yet".into(),
            ));
        }

        let version = if request.revision > 0 { Some(request.revision as u64) } else { None };
        let range = build_request_range(&request.key, request.range_end.as_deref());
        let range_req = sekas_client::RangeRequest {
            table_id: kv_store.table_id,
            range,
            version,
            limit: request.limit as u64,
            ..Default::default()
        };
        let mut range_stream = kv_store.db.range(range_req, None)?;
        let mut range_resp = RangeResponse::default();
        'OUTER: while let Some(value_sets) = range_stream.next().await {
            let value_sets = value_sets?;
            for value_set in value_sets {
                if value_set.values.is_empty() {
                    return Err(AppError::Internal("value set is empty".into()));
                }
                let value = &value_set.values[0];
                let data = value.content.as_ref().ok_or_else(|| {
                    AppError::Internal("value content of range request should not be None".into())
                })?;
                let key_value = ValueRecord::decode_to_key_value(
                    &value_set.user_key,
                    data,
                    value.version as i64,
                )?;
                if request.min_create_revision > 0
                    && request.min_create_revision > key_value.create_revision
                {
                    continue;
                }
                if request.max_create_revision > 0
                    && key_value.create_revision > request.max_create_revision
                {
                    continue;
                }
                if request.min_mod_revision > 0 && request.min_mod_revision > key_value.mod_revision
                {
                    continue;
                }
                if request.max_mod_revision > 0 && key_value.mod_revision > request.max_mod_revision
                {
                    continue;
                }
                range_resp.count += 1;
                if !request.count_only {
                    let mut key_value = key_value;
                    if request.keys_only {
                        key_value.value.clear();
                    }
                    range_resp.kvs.push(key_value);
                }
                if range_resp.count == request.limit {
                    // It will skip zero limit automatically.
                    range_resp.more = true;
                    break 'OUTER;
                }
            }
        }
        Ok(range_resp)
    }

    pub async fn delete_range(&self, req: &DeleteRangeRequest) -> AppResult<DeleteRangeResponse> {
        let kv_store = self.ensure_handle().await?;

        loop {
            let range = build_request_range(&req.key, req.range_end.as_deref());
            // TODO: make this range limit as a knob.
            let range_req = sekas_client::RangeRequest {
                table_id: kv_store.table_id,
                range,
                limit: 128, // reduce txn overhead.
                ..Default::default()
            };

            let mut range_stream = kv_store.db.range(range_req, None)?;
            let mut delete_resp = DeleteRangeResponse::default();
            let mut deletes = Vec::default();
            while let Some(value_sets) = range_stream.next().await {
                for value_set in value_sets? {
                    let Some((data, version)) = value_set
                        .values
                        .first()
                        .map(|v| v.content.as_ref().map(|c| (c, v.version)))
                        .flatten()
                    else {
                        continue;
                    };
                    let key_value = ValueRecord::decode_to_key_value(
                        &value_set.user_key,
                        data,
                        version as i64,
                    )?;
                    deletes.push(
                        WriteBuilder::new(key_value.key.clone())
                            .expect_version(key_value.mod_revision as u64)
                            .ensure_delete(),
                    );
                    if req.prev_kv {
                        delete_resp.prev_kvs.push(key_value);
                    }
                }
            }

            let wb = WriteBatchRequest {
                deletes: deletes.into_iter().map(|v| (kv_store.table_id, v)).collect(),
                ..Default::default()
            };
            let resp = match kv_store.db.write_batch(wb).await {
                Ok(resp) => resp,
                Err(AppError::CasFailed(_, _, _)) => continue,
                Err(err) => return Err(err),
            };
            delete_resp.header = Some(ResponseHeader::with_revision(resp.version as i64));
            return Ok(delete_resp);
        }
    }

    pub async fn txn(&self, _req: &TxnRequest) -> AppResult<TxnResponse> {
        todo!()
    }
}

impl KvStore {
    async fn get_key_value(
        &self,
        kv_store: &SekasHandle,
        value_key: Vec<u8>,
    ) -> AppResult<Option<KeyValue>> {
        if let Some(raw_value) =
            kv_store.db.get_raw_value(kv_store.table_id, value_key.to_owned()).await?
        {
            let Some(content) = raw_value.content else { return Ok(None) };
            let key_value =
                ValueRecord::decode_to_key_value(&value_key, &content, raw_value.version as i64)?;
            Ok(Some(key_value))
        } else {
            Ok(None)
        }
    }
}

impl ValueRecord {
    fn decode_to_key_value(key: &[u8], content: &[u8], revision: i64) -> AppResult<KeyValue> {
        let mut record = ValueRecord::decode(content).unwrap();
        if record.create == 0 {
            record.create = revision;
        }
        Ok(KeyValue {
            create_revision: record.create,
            mod_revision: revision,
            version: record.version,
            value: record.data,
            lease: record.lease,
            key: key.to_owned(),
        })
    }
}

/// Build range from the specified key and range_end.
///
/// range_end is the upper bound on the requested range [key, range_end).
/// If range_end is not given, the request only looks up key
/// If range_end is '\0', the range is all keys >= key.
/// If range_end is key plus one (e.g., "aa"+1 == "ab", "a\xff"+1 == "b"),
/// then the range request gets all keys prefixed with key.
/// If both key and range_end are '\0', then the range request returns all keys.
fn build_request_range(key: &[u8], range_end: Option<&[u8]>) -> sekas_client::Range {
    if let Some(range_end) = range_end.filter(|v| v.is_empty()) {
        if range_end[0] == b'\0' {
            if !key.is_empty() && key[0] == b'\0' {
                // both key and range_end are '\0'.
                sekas_client::Range::Range { begin: None, end: None }
            } else {
                // range_end is '\0'.
                sekas_client::Range::Range { begin: Some(key.to_owned()), end: None }
            }
        } else if range_end == lexical_next_boundary(key) {
            // range_end is key plus one.
            sekas_client::Range::Prefix(key.to_owned())
        } else {
            sekas_client::Range::Range {
                begin: Some(key.to_owned()),
                end: Some(range_end.to_owned()),
            }
        }
    } else {
        // range_end is not given
        sekas_client::Range::Range { begin: Some(key.to_owned()), end: Some(lexical_next(key)) }
    }
}
