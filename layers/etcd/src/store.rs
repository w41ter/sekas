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

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use async_recursion::async_recursion;
use futures::channel::mpsc;
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

#[derive(Clone)]
struct SekasHandle {
    db: sekas_client::Database,
    table_id: u64,
}

struct TxnContext {
    #[allow(dead_code)]
    txn_version: u64,
    puts: Vec<sekas_client::PutRequest>,
    deletes: Vec<sekas_client::DeleteRequest>,
}

impl TxnContext {
    fn new() -> Self {
        TxnContext { txn_version: 0, puts: Vec::default(), deletes: Vec::default() }
    }
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
            let mut txn_ctx = TxnContext::new();
            let mut put_resp = kv_store.apply_put(&mut txn_ctx, request).await?;
            let resp = match kv_store.commit(txn_ctx).await {
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
        kv_store.apply_range(request).await
    }

    pub async fn delete_range(&self, req: &DeleteRangeRequest) -> AppResult<DeleteRangeResponse> {
        let kv_store = self.ensure_handle().await?;

        loop {
            let mut txn_ctx = TxnContext::new();
            let mut delete_resp = kv_store.apply_delete_range(&mut txn_ctx, req).await?;
            let resp = match kv_store.commit(txn_ctx).await {
                Ok(resp) => resp,
                Err(AppError::CasFailed(_, _, _)) => continue,
                Err(err) => return Err(err),
            };
            delete_resp.header = Some(ResponseHeader::with_revision(resp.version as i64));
            return Ok(delete_resp);
        }
    }

    pub async fn txn(&self, req: &TxnRequest) -> AppResult<TxnResponse> {
        let kv_store = self.ensure_handle().await?;
        loop {
            let mut txn_ctx = TxnContext::new();
            let mut txn_resp = kv_store.apply_txn(&mut txn_ctx, req).await?;
            let resp = match kv_store.commit(txn_ctx).await {
                Ok(resp) => resp,
                Err(AppError::CasFailed(_, _, _)) => continue,
                Err(err) => return Err(err),
            };
            txn_resp.header = Some(ResponseHeader::with_revision(resp.version as i64));
            return Ok(txn_resp);
        }
    }
}

impl SekasHandle {
    async fn get_key_value(&self, value_key: Vec<u8>) -> AppResult<Option<KeyValue>> {
        if let Some(raw_value) = self.db.get_raw_value(self.table_id, value_key.to_owned()).await? {
            let Some(content) = raw_value.content else { return Ok(None) };
            let key_value =
                ValueRecord::decode_to_key_value(&value_key, &content, raw_value.version as i64)?;
            Ok(Some(key_value))
        } else {
            Ok(None)
        }
    }

    async fn apply_put(
        &self,
        ctx: &mut TxnContext,
        request: &PutRequest,
    ) -> AppResult<PutResponse> {
        // read old value.
        let mut key_value = match self.get_key_value(request.key.clone()).await? {
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
        ctx.puts.push(put_value);
        Ok(put_resp)
    }

    #[async_recursion]
    async fn apply_txn(&self, ctx: &mut TxnContext, req: &TxnRequest) -> AppResult<TxnResponse> {
        // verify arguments and collect compare target keys.
        let mut collected_keys = HashSet::new();
        for compare in &req.compare {
            if compare.key.is_empty() {
                return Err(AppError::InvalidArgument("empty compare key is not allowed".into()));
            }

            let Some(target_union) = compare.target_union.as_ref() else {
                return Err(AppError::InvalidArgument("target_union is not set".into()));
            };

            let compare_target: compare::CompareTarget = target_union.into();
            if compare_target as i32 != compare.target {
                return Err(AppError::InvalidArgument(
                    "target_union is not aligned with target".into(),
                ));
            }

            // Add in etcd version 3.3
            if compare.range_end.is_empty() {
                // TODO(walter) one day we could support this API but limit the num of keys in
                // the range.
                return Err(AppError::InvalidArgument("range end is not allowed".into()));
            }

            if !collected_keys.contains(&compare.key) {
                collected_keys.insert(compare.key.clone());
            }
        }

        // read all keys in the requests.
        let mut collected_key_values = HashMap::with_capacity(collected_keys.len());
        let (sender, mut receiver) = mpsc::channel(collected_keys.len());
        let mut task_handles = Vec::with_capacity(collected_keys.len());
        for key in collected_keys {
            let mut sender_clone = sender.clone();
            let store = self.clone();
            let handle = sekas_runtime::spawn(async move {
                // TODO(walter) support read with one version.
                let result = store.db.get_raw_value(store.table_id, key.clone()).await;
                let _ = sender_clone.start_send((key, result));
            });
            task_handles.push(handle);
        }
        while let Some((key, get_raw_value_result)) = receiver.next().await {
            let key_value = get_raw_value_result?
                .map(|value| value.content.map(|c| (c, value.version)))
                .flatten()
                .map(|(content, version)| {
                    ValueRecord::decode_to_key_value(&key, &content, version as i64)
                })
                .transpose()?;
            collected_key_values.insert(key, key_value);
        }

        // compare key-values.
        let mut passed = true;
        for compare in &req.compare {
            let target_union = compare.target_union.as_ref().expect("already checked");
            let key_value_opt = collected_key_values.get(&compare.key).ok_or_else(|| {
                AppError::Internal("the key value of the compare target not exists".into())
            })?;

            // The target key is not exists.
            let Some(key_value) = key_value_opt else {
                passed = false;
                break;
            };

            if !compare.result().is_matched(key_value.compare(target_union)) {
                passed = true;
                break;
            }
        }

        let operations = if passed { &req.success } else { &req.failure };

        // execute operations.
        let mut txn_resp = TxnResponse::default();
        txn_resp.succeeded = passed;
        for op in operations {
            let resp = match &op.request {
                Some(request_op::Request::RequestPut(put)) => {
                    ResponseOp::put(self.apply_put(ctx, &put).await?)
                }
                Some(request_op::Request::RequestRange(range)) => {
                    ResponseOp::range(self.apply_range(range).await?)
                }
                Some(request_op::Request::RequestDeleteRange(delete)) => {
                    ResponseOp::delete_range(self.apply_delete_range(ctx, delete).await?)
                }
                Some(request_op::Request::RequestTxn(txn)) => {
                    ResponseOp::txn(self.apply_txn(ctx, &txn).await?)
                }
                None => {
                    return Err(AppError::InvalidArgument(
                        "the request field of RequestOp is not set".into(),
                    ));
                }
            };
            txn_resp.responses.push(resp);
        }
        Ok(txn_resp)
    }

    async fn apply_range(&self, request: &RangeRequest) -> AppResult<RangeResponse> {
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
            table_id: self.table_id,
            range,
            version,
            limit: request.limit as u64,
            ..Default::default()
        };
        let mut range_stream = self.db.range(range_req, None)?;
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

    async fn apply_delete_range(
        &self,
        ctx: &mut TxnContext,
        req: &DeleteRangeRequest,
    ) -> AppResult<DeleteRangeResponse> {
        let range = build_request_range(&req.key, req.range_end.as_deref());
        // TODO: make this range limit as a knob.
        let range_req = sekas_client::RangeRequest {
            table_id: self.table_id,
            range,
            limit: 128, // reduce txn overhead.
            ..Default::default()
        };

        let mut range_stream = self.db.range(range_req, None)?;
        let mut delete_resp = DeleteRangeResponse::default();
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
                let key_value =
                    ValueRecord::decode_to_key_value(&value_set.user_key, data, version as i64)?;
                ctx.deletes.push(
                    WriteBuilder::new(key_value.key.clone())
                        .expect_version(key_value.mod_revision as u64)
                        .ensure_delete(),
                );
                if req.prev_kv {
                    delete_resp.prev_kvs.push(key_value);
                }
            }
        }

        return Ok(delete_resp);
    }

    async fn commit(&self, txn_ctx: TxnContext) -> AppResult<sekas_client::WriteBatchResponse> {
        let wb = WriteBatchRequest {
            puts: txn_ctx.puts.into_iter().map(|v| (self.table_id, v)).collect(),
            deletes: txn_ctx.deletes.into_iter().map(|v| (self.table_id, v)).collect(),
        };
        self.db.write_batch(wb).await
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
