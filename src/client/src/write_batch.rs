// Copyright 2023-present The Sekas Authors.
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

use std::time::Duration;

use log::{trace, warn};
use sekas_api::server::v1::group_request_union::Request;
use sekas_api::server::v1::group_response_union::Response;
use sekas_api::server::v1::*;

use crate::{
    Error, GroupClient, Result, RetryState, SekasClient, TxnStateTable, WriteBatchRequest,
    WriteBatchResponse,
};

/// A structure to hold the context about single write request.
struct WriteContext {
    /// The request.
    request: WriteRequest,
    /// The response.
    response: Option<WriteResponse>,
    /// The index in the requests.
    index: usize,
    /// Is this request has been accepted.
    done: bool,
}

/// A structure to hold the context about a write batch request.
pub struct WriteBatchContext {
    client: SekasClient,
    desc: CollectionDesc,

    writes: Vec<WriteContext>,
    /// The number of doing requests.
    num_doing_writes: usize,
    /// The number of delete requests in this batch.
    num_deletes: usize,

    start_version: u64,
    commit_version: u64,

    retry_state: RetryState,
}

impl WriteContext {
    fn with_put((index, put): (usize, PutRequest)) -> Self {
        WriteContext { request: WriteRequest::Put(put), response: None, index, done: false }
    }

    fn with_delete((index, delete): (usize, DeleteRequest)) -> Self {
        WriteContext { request: WriteRequest::Delete(delete), response: None, index, done: false }
    }

    fn user_key(&self) -> &[u8] {
        match &self.request {
            WriteRequest::Put(put) => &put.key,
            WriteRequest::Delete(del) => &del.key,
        }
    }
}

impl WriteBatchContext {
    pub fn new(
        desc: CollectionDesc,
        request: WriteBatchRequest,
        client: SekasClient,
        timeout: Option<Duration>,
    ) -> Self {
        let num_deletes = request.deletes.len();
        let num_puts = request.puts.len();
        let num_doing_writes = num_deletes + num_puts;
        let mut writes = Vec::with_capacity(num_doing_writes);
        writes.extend(request.deletes.into_iter().enumerate().map(WriteContext::with_delete));
        writes.extend(request.puts.into_iter().enumerate().map(WriteContext::with_put));

        WriteBatchContext {
            client,
            desc,
            writes,
            num_deletes,
            num_doing_writes,
            start_version: 0,
            commit_version: 0,
            retry_state: RetryState::new(timeout),
        }
    }

    pub async fn commit(mut self) -> Result<WriteBatchResponse> {
        // TODO: check parameters

        // TODO: handle errors to abort txn.
        log::info!("try alloc txn version");
        self.start_version = self.alloc_txn_version().await?;
        log::info!("alloc txn version {}", self.start_version);
        self.start_txn().await?;
        log::info!("start txn {}", self.start_version);

        let start_version = self.start_version;
        let txn_table = TxnStateTable::new(self.client.clone(), self.retry_state.timeout());

        tokio::select! {
            _ = Self::lease_txn(txn_table, start_version) => {
                unreachable!()
            },
            resp = self.commit_inner() => {
                resp
            }
        }
    }

    async fn lease_txn(txn_table: TxnStateTable, start_version: u64) -> ! {
        loop {
            if let Err(err) = txn_table.heartbeat(start_version).await {
                warn!("txn {start_version} lease heartbeat: {err}");
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    async fn commit_inner(mut self) -> Result<WriteBatchResponse> {
        self.prepare_intents().await?;
        log::info!("prepare intents {}", self.start_version);
        self.commit_version = self.alloc_txn_version().await?;
        log::info!("allocate commit txn version {} {}", self.start_version, self.commit_version);
        self.commit_txn().await?;
        log::info!("commit txn version {} {}", self.start_version, self.commit_version);
        let version = self.commit_version;

        let mut deletes = Vec::with_capacity(self.num_deletes);
        let mut puts = Vec::with_capacity(self.writes.len() - self.num_deletes);
        for write in &mut self.writes {
            match &write.request {
                WriteRequest::Delete(_) => {
                    deletes.push(write.response.take().and_then(|v| v.prev_value));
                }
                WriteRequest::Put(_) => {
                    puts.push(write.response.take().and_then(|v| v.prev_value));
                }
            }
        }

        self.commit_intents();
        log::info!("commit intents");
        Ok(WriteBatchResponse { version, deletes, puts })
    }

    async fn alloc_txn_version(&mut self) -> Result<u64> {
        let root_client = self.client.root_client();
        loop {
            match root_client.alloc_txn_id(1, self.retry_state.timeout()).await {
                Ok(value) => {
                    return Ok(value);
                }
                Err(err) => {
                    self.retry_state.retry(err).await?;
                }
            }
        }
    }

    async fn start_txn(&mut self) -> Result<()> {
        TxnStateTable::new(self.client.clone(), self.retry_state.timeout())
            .begin_txn(self.start_version)
            .await
    }

    async fn prepare_intents(&mut self) -> Result<()> {
        loop {
            if !self.prepare_intents_inner().await? {
                return Ok(());
            }
            self.retry_state.force_retry().await?;
        }
    }

    async fn prepare_intents_inner(&mut self) -> Result<bool> {
        let router = self.client.router();
        let mut handles = Vec::with_capacity(self.writes.len());
        for (index, write) in self.writes.iter().enumerate() {
            if write.done {
                continue;
            }
            let (group_state, shard_desc) =
                router.find_shard(self.desc.clone(), write.user_key())?;
            let mut client = GroupClient::new(group_state, self.client.clone());
            let req = Request::WriteIntent(WriteIntentRequest {
                start_version: self.start_version,
                shard_id: shard_desc.id,
                write: Some(write.request.clone()),
            });
            if let Some(duration) = self.retry_state.timeout() {
                client.set_timeout(duration);
            }
            let handle = tokio::spawn(async move {
                match client.request(&req).await? {
                    Response::WriteIntent(WriteIntentResponse { write: Some(resp) }) => {
                        Ok((resp, index))
                    }
                    _ => Err(Error::Internal(
                        "invalid response type, Get is required".to_string().into(),
                    )),
                }
            });
            handles.push(handle);
        }

        for handle in handles {
            match handle.await? {
                Ok((resp, index)) => {
                    self.num_doing_writes =
                        self.num_doing_writes.checked_sub(1).expect("out of range");
                    let write = &mut self.writes[index];
                    write.done = true;
                    write.response = Some(resp);
                }
                Err(err) => {
                    // FIXME(walter) UPDATE THE CAS FAIELD INDEX.
                    trace!("txn {} write intent: {err:?}", self.start_version);
                    if !self.retry_state.is_retryable(&err) {
                        return Err(err);
                    }
                }
            }
        }
        trace!("txn {} write intent left {} writes", self.start_version, self.num_doing_writes);
        Ok(self.num_doing_writes > 0)
    }

    async fn commit_txn(&mut self) -> Result<()> {
        TxnStateTable::new(self.client.clone(), self.retry_state.timeout())
            .commit_txn(self.start_version, self.commit_version)
            .await
    }

    #[allow(unused)]
    async fn abort_txn(&mut self) -> Result<()> {
        TxnStateTable::new(self.client.clone(), self.retry_state.timeout())
            .abort_txn(self.start_version)
            .await
    }

    fn commit_intents(mut self) {
        tokio::spawn(async move {
            self.num_doing_writes = self.writes.len();
            for write in &mut self.writes {
                write.done = false;
            }

            for i in [1, 3, 5] {
                match self.commit_intents_inner().await {
                    Ok(false) => break,
                    Ok(true) => tokio::time::sleep(Duration::from_millis(i)).await,
                    Err(err) => {
                        warn!("txn {} commit intents: {}", self.start_version, err);
                        break;
                    }
                }
            }
        });
    }

    async fn commit_intents_inner(&mut self) -> Result<bool> {
        let router = self.client.router();

        let mut handles = Vec::with_capacity(self.writes.len());
        for write in &self.writes {
            if write.done {
                continue;
            }

            let user_key = write.user_key();
            let (group_state, shard_desc) = router.find_shard(self.desc.clone(), user_key)?;
            let req = CommitIntentRequest {
                shard_id: shard_desc.id,
                start_version: self.start_version,
                commit_version: self.commit_version,
                user_key: user_key.to_vec(),
            };
            let index = write.index;
            let mut client = GroupClient::new(group_state, self.client.clone());
            let handle = tokio::spawn(async move {
                match client.request(&Request::CommitIntent(req)).await {
                    Ok(Response::CommitIntent(CommitIntentResponse {})) => Ok(index),
                    _ => Err(Error::Internal(
                        "invalid response, `CommitIntent` is required".to_string().into(),
                    )),
                }
            });
            handles.push(handle);
        }
        for handle in handles {
            match handle.await? {
                Ok(index) => {
                    self.writes[index].done = true;
                    self.num_doing_writes =
                        self.num_doing_writes.checked_sub(1).expect("out of range");
                }
                Err(err) => {
                    if !self.retry_state.is_retryable(&err) {
                        return Err(err);
                    }
                }
            }
        }
        trace!("txn {} commit intent left {} writes", self.start_version, self.num_doing_writes);
        Ok(self.num_doing_writes > 0)
    }

    #[allow(unused)]
    async fn clear_intents(&mut self) -> Result<()> {
        todo!()
    }
}
