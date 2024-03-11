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

#![feature(cursor_remaining)]

mod consts;
mod etcd;
mod service;
mod store;

use std::sync::Arc;

pub use crate::store::KvStore;

pub fn make_etcd_kv_service(kv_store: Arc<KvStore>) -> etcd::v3::kv_server::KvServer<service::Kv> {
    etcd::v3::kv_server::KvServer::new(service::Kv::new(kv_store))
}

pub fn make_etcd_watch_service(
    _kv_store: Arc<KvStore>,
) -> etcd::v3::watch_server::WatchServer<service::Watch> {
    todo!()
}

pub fn make_etcd_lease_service(
    _kv_store: Arc<KvStore>,
) -> etcd::v3::lease_server::LeaseServer<service::Lease> {
    todo!()
}

pub fn make_etcd_store(client: sekas_client::SekasClient) -> Arc<store::KvStore> {
    Arc::new(store::KvStore::new(client))
}
