// Copyright 2023 The Sekas Authors.
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

use sekas_api::server::v1::group_request_union::Request;
use sekas_api::server::v1::group_response_union::Response;
use sekas_api::server::v1::*;
use sekas_api::v1::*;
use sekas_rock::time::timestamp_millis;

use crate::{AppResult, ConnManager, Error, GroupClient, RootClient, Router};

/// The txn record.
pub struct TxnRecord {
    /// The txn state.
    pub state: TxnState,
    /// Txn timeout, in seconds.
    pub timeout: u64,
    /// The commit version of txn.
    pub version: Option<u64>,
}

/// An client to access, manipulate txn collection.
pub struct TxnClient {
    root_client: RootClient,
    router: Router,
    conn_manager: ConnManager,

    timeout: Option<Duration>,
}

impl TxnClient {
    pub fn new(root_client: RootClient, router: Router, conn_manager: ConnManager) -> TxnClient {
        TxnClient { root_client, router, conn_manager, timeout: None }
    }

    pub fn set_timeout(&mut self, timeout: Duration) {
        self.timeout = Some(timeout);
    }

    pub async fn write(&mut self, _req: WriteBatchRequest) -> AppResult<WriteBatchResponse> {
        let txn_id = self.root_client.alloc_txn_id(1).await?;
        self.start_txn(txn_id).await?;
        // Begin txn

        let version = self.root_client.alloc_txn_id(1).await?;
        self.commit_txn(txn_id, version).await?;
        todo!()
    }

    pub async fn start_txn(&mut self, txn_id: u64) -> crate::Result<()> {
        let desc = Self::txn_collection();

        let (group, shard) = self.router.find_shard(desc, &txn_key_prefix(txn_id))?;
        let mut client = GroupClient::new(group, self.router.clone(), self.conn_manager.clone());

        let timeout = timestamp_millis() + 5000; // for 5 seconds

        // TODO(walter): extract common codes.
        let req = Request::BatchWrite(BatchWriteRequest {
            puts: vec![
                ShardPutRequest {
                    shard_id: shard.id,
                    put: Some(PutRequest {
                        key: txn_state_key(txn_id),
                        value: (TxnState::Running as u32).to_le_bytes().to_vec(),
                        ttl: u64::MAX,
                        conditions: vec![WriteCondition {
                            r#type: WriteConditionType::NotExists.into(),
                            ..Default::default()
                        }],
                        op: PutOperation::None.into(),
                    }),
                },
                ShardPutRequest {
                    shard_id: shard.id,
                    put: Some(PutRequest {
                        key: txn_timeout_key(txn_id),
                        value: timeout.to_le_bytes().to_vec(),
                        ttl: u64::MAX,
                        ..Default::default()
                    }),
                },
            ],
            ..Default::default()
        });
        if let Some(duration) = self.timeout {
            client.set_timeout(duration);
        }
        client.request(&req).await?;
        Ok(())
    }

    pub async fn commit_txn(&mut self, txn_id: u64, version: u64) -> crate::Result<()> {
        let desc = Self::txn_collection();

        let (group, shard) = self.router.find_shard(desc, &txn_key_prefix(txn_id))?;
        let mut client = GroupClient::new(group, self.router.clone(), self.conn_manager.clone());

        let req = Request::BatchWrite(BatchWriteRequest {
            puts: vec![
                ShardPutRequest {
                    shard_id: shard.id,
                    put: Some(PutRequest {
                        key: txn_state_key(txn_id),
                        value: (TxnState::Committed as u32).to_le_bytes().to_vec(),
                        ttl: u64::MAX,
                        conditions: vec![WriteCondition {
                            r#type: WriteConditionType::ExpectValue as i32,
                            value: (TxnState::Running as u32).to_le_bytes().to_vec(),
                            ..Default::default()
                        }],
                        op: PutOperation::None.into(),
                    }),
                },
                ShardPutRequest {
                    shard_id: shard.id,
                    put: Some(PutRequest {
                        key: txn_version_key(txn_id),
                        value: version.to_le_bytes().to_vec(),
                        ttl: u64::MAX,
                        ..Default::default()
                    }),
                },
            ],
            ..Default::default()
        });
        if let Some(duration) = self.timeout {
            client.set_timeout(duration);
        }
        client.request(&req).await?;
        Ok(())
    }

    pub async fn abort_txn(&mut self, txn_id: u64) -> crate::Result<()> {
        let desc = Self::txn_collection();

        let (group, shard) = self.router.find_shard(desc, &txn_key_prefix(txn_id))?;
        let mut client = GroupClient::new(group, self.router.clone(), self.conn_manager.clone());

        let req = Request::Put(ShardPutRequest {
            shard_id: shard.id,
            put: Some(PutRequest {
                key: txn_state_key(txn_id),
                value: (TxnState::Aborted as u32).to_le_bytes().to_vec(),
                ttl: u64::MAX,
                conditions: vec![WriteCondition {
                    r#type: WriteConditionType::ExpectValue as i32,
                    value: (TxnState::Running as u32).to_le_bytes().to_vec(),
                    ..Default::default()
                }],
                op: PutOperation::None.into(),
            }),
        });
        if let Some(duration) = self.timeout {
            client.set_timeout(duration);
        }
        client.request(&req).await?;
        Ok(())
    }

    pub async fn clean_txn(&mut self, txn_id: u64) -> crate::Result<()> {
        let desc = Self::txn_collection();

        let (group, shard) = self.router.find_shard(desc, &txn_key_prefix(txn_id))?;
        let mut client = GroupClient::new(group, self.router.clone(), self.conn_manager.clone());

        let req = Request::BatchWrite(BatchWriteRequest {
            deletes: vec![
                ShardDeleteRequest {
                    shard_id: shard.id,
                    delete: Some(DeleteRequest {
                        key: txn_state_key(txn_id),
                        ..Default::default()
                    }),
                },
                ShardDeleteRequest {
                    shard_id: shard.id,
                    delete: Some(DeleteRequest {
                        key: txn_timeout_key(txn_id),
                        ..Default::default()
                    }),
                },
                ShardDeleteRequest {
                    shard_id: shard.id,
                    delete: Some(DeleteRequest {
                        key: txn_version_key(txn_id),
                        ..Default::default()
                    }),
                },
            ],
            ..Default::default()
        });
        if let Some(duration) = self.timeout {
            client.set_timeout(duration);
        }
        client.request(&req).await?;
        Ok(())
    }

    pub async fn get_txn_record(&self, txn_id: u64) -> crate::Result<Option<TxnRecord>> {
        let desc = Self::txn_collection();

        let prefix = txn_key_prefix(txn_id);
        let (group, shard) = self.router.find_shard(desc, &prefix)?;
        let mut client = GroupClient::new(group, self.router.clone(), self.conn_manager.clone());
        let req = Request::Scan(ShardScanRequest {
            shard_id: shard.id,
            limit: u64::MAX,
            limit_bytes: u64::MAX,
            prefix: Some(prefix.clone()),
            ..Default::default()
        });
        let resp = match client.request(&req).await? {
            Response::Scan(scan) => scan,
            _ => {
                return Err(Error::Internal(
                    "invalid response type, `ShardScanResponse` is required".into(),
                ))
            }
        };
        if resp.data.is_empty() {
            return Ok(None);
        }

        // FIXME(walter) support scan during migration.
        let mut txn_record = TxnRecord { state: TxnState::Aborted, timeout: 0, version: None };
        for item in resp.data {
            match item.key.strip_prefix(prefix.as_slice()) {
                Some(b"state") => {
                    txn_record.state = to_fixed_bytes(&item.value)
                        .map(i32::from_le_bytes)
                        .and_then(TxnState::from_i32)
                        .ok_or_else(|| {
                            Error::Internal(
                                format!("txn state has illiged value: {:?}", item.value).into(),
                            )
                        })?;
                }
                Some(b"timeout") => {
                    txn_record.timeout =
                        to_fixed_bytes(&item.value).map(u64::from_le_bytes).ok_or_else(|| {
                            Error::Internal(
                                format!("txn timeout has illiged value: {:?}", item.value).into(),
                            )
                        })?;
                }
                Some(b"version") => {
                    let version =
                        to_fixed_bytes(&item.value).map(u64::from_le_bytes).ok_or_else(|| {
                            Error::Internal(
                                format!("txn timeout has illiged value: {:?}", item.value).into(),
                            )
                        })?;
                    txn_record.version = Some(version);
                }
                _ => {
                    return Err(Error::Internal(format!("unknown txn key: {:?}", item.key).into()));
                }
            }
        }
        Ok(Some(txn_record))
    }

    fn txn_collection() -> CollectionDesc {
        // FIXME(walter): fetch txn collection desc.
        CollectionDesc {
            id: 1,
            name: "txn".into(),
            db: 1,
            partition: Some(collection_desc::Partition::Hash(collection_desc::HashPartition {
                slots: 256,
            })),
        }
    }
}

#[inline]
fn txn_state_key(txn_id: u64) -> Vec<u8> {
    let mut buf = Vec::with_capacity(16);
    buf.extend_from_slice(txn_id.to_be_bytes().as_slice());
    buf.extend_from_slice(b"state");
    buf
}

#[inline]
fn txn_timeout_key(txn_id: u64) -> Vec<u8> {
    let mut buf = Vec::with_capacity(16);
    buf.extend_from_slice(txn_id.to_be_bytes().as_slice());
    buf.extend_from_slice(b"timeout");
    buf
}

#[inline]
fn txn_version_key(txn_id: u64) -> Vec<u8> {
    let mut buf = Vec::with_capacity(16);
    buf.extend_from_slice(txn_id.to_be_bytes().as_slice());
    buf.extend_from_slice(b"version");
    buf
}

#[inline]
fn txn_key_prefix(txn_id: u64) -> Vec<u8> {
    txn_id.to_be_bytes().to_vec()
}

#[inline]
fn to_fixed_bytes<const V: usize>(bytes: &[u8]) -> Option<[u8; V]> {
    if bytes.len() != V {
        None
    } else {
        let mut buf = [0u8; V];
        buf[..].copy_from_slice(bytes);
        Some(buf)
    }
}
