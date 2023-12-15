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

mod kv;
mod lease;
mod watch;

pub use self::kv::Kv;
pub use self::lease::Lease;
pub use self::watch::Watch;

pub mod etcd {
    pub mod etcdserverpb {
        #![allow(clippy::all)]
        tonic::include_proto!("etcdserverpb");
    }
    pub mod authpb {
        #![allow(clippy::all)]
        tonic::include_proto!("authpb");
    }
    pub mod v3 {
        pub use super::etcdserverpb::*;
    }
}

pub fn make_etcd_kv_service() -> etcd::v3::kv_server::KvServer<Kv> {
    todo!()
}

pub fn make_etcd_watch_service() -> etcd::v3::watch_server::WatchServer<Watch> {
    todo!()
}

pub fn make_etcd_lease_service() -> etcd::v3::lease_server::LeaseServer<Lease> {
    todo!()
}
