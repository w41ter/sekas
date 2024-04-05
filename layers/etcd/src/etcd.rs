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

pub mod etcdserverpb {
    #![allow(clippy::all)]
    tonic::include_proto!("etcdserverpb");

    impl From<&compare::TargetUnion> for compare::CompareTarget {
        fn from(value: &compare::TargetUnion) -> Self {
            match value {
                compare::TargetUnion::Version(_) => compare::CompareTarget::Version,
                compare::TargetUnion::CreateRevision(_) => compare::CompareTarget::Create,
                compare::TargetUnion::ModRevision(_) => compare::CompareTarget::Mod,
                compare::TargetUnion::Value(_) => compare::CompareTarget::Value,
                compare::TargetUnion::Lease(_) => compare::CompareTarget::Lease,
            }
        }
    }

    impl compare::CompareResult {
        pub fn is_matched(&self, ord: std::cmp::Ordering) -> bool {
            match (self, ord) {
                (compare::CompareResult::Equal, std::cmp::Ordering::Equal)
                | (compare::CompareResult::Less, std::cmp::Ordering::Less)
                | (compare::CompareResult::Greater, std::cmp::Ordering::Greater)
                | (compare::CompareResult::NotEqual, std::cmp::Ordering::Greater)
                | (compare::CompareResult::NotEqual, std::cmp::Ordering::Less) => true,
                _ => false,
            }
        }
    }

    impl KeyValue {
        pub fn not_exists(key: &[u8]) -> Self {
            KeyValue {
                key: key.to_vec(),
                // create_revision 0 means that the key is not exists.
                create_revision: 0,
                mod_revision: 0,
                version: 0,
                lease: 0,
                value: Vec::default(),
            }
        }

        pub fn compare(&self, target_union: &compare::TargetUnion) -> std::cmp::Ordering {
            match target_union {
                compare::TargetUnion::Version(version) => self.version.cmp(version),
                compare::TargetUnion::CreateRevision(create_revision) => {
                    self.create_revision.cmp(create_revision)
                }
                compare::TargetUnion::ModRevision(mod_revision) => {
                    self.mod_revision.cmp(mod_revision)
                }
                compare::TargetUnion::Value(value) => self.value.cmp(value),
                compare::TargetUnion::Lease(lease) => self.lease.cmp(lease),
            }
        }
    }

    impl ResponseOp {
        pub fn put(response: PutResponse) -> Self {
            ResponseOp { response: Some(response_op::Response::ResponsePut(response)) }
        }

        pub fn range(response: RangeResponse) -> Self {
            ResponseOp { response: Some(response_op::Response::ResponseRange(response)) }
        }

        pub fn delete_range(response: DeleteRangeResponse) -> Self {
            ResponseOp { response: Some(response_op::Response::ResponseDeleteRange(response)) }
        }

        pub fn txn(response: TxnResponse) -> Self {
            ResponseOp { response: Some(response_op::Response::ResponseTxn(response)) }
        }
    }
}

pub mod authpb {
    #![allow(clippy::all)]
    tonic::include_proto!("authpb");
}

pub mod recordpb {
    #![allow(clippy::all)]
    tonic::include_proto!("recordpb");

    use super::etcdserverpb::KeyValue;

    impl From<KeyValue> for ValueRecord {
        fn from(key_value: KeyValue) -> Self {
            ValueRecord {
                create: key_value.create_revision,
                data: key_value.value,
                lease: key_value.lease,
                version: key_value.version,
            }
        }
    }
}

pub mod v3 {
    pub use super::etcdserverpb::*;
    pub use super::recordpb::*;

    impl ResponseHeader {
        #[inline]
        pub fn with_revision(revision: i64) -> Self {
            ResponseHeader { revision, ..Default::default() }
        }
    }
}
