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
use paste::paste;
use sekas_api::server::v1::*;
use sekas_api::v1::*;

use crate::LOCAL_COLLECTION_ID;

macro_rules! decl_hash_col {
    ($name:ident, $col_id:expr, $slots:expr) => {
        paste! {
            pub const [<$name:upper _NAME>]: &str = stringify!($name);
            pub const [<$name:upper _ID>]: u64 = $col_id;
            pub const [<$name:upper _SLOTS>]: u32 = $slots;

            pub fn [<$name:lower _desc>]() -> CollectionDesc {
                CollectionDesc {
                    id: $col_id,
                    name: stringify!($name).to_owned(),
                    db: crate::system::db::ID,
                    partition: Some(collection_desc::Partition::Hash(collection_desc::HashPartition {
                        slots: $slots,
                    })),
                }
            }
        }
    };
}

macro_rules! decl_unity_range_col {
    ($name:ident, $col_id:expr) => {
        paste! {
            pub const [<$name:upper _NAME>]: &str = stringify!($name);
            pub const [<$name:upper _ID>]: u64 = $col_id;
            pub const [<$name:upper _SHARD_ID>]: u64 = $col_id;

            pub fn [<$name:lower _desc>]() -> CollectionDesc {
                CollectionDesc {
                    id: $col_id,
                    name: stringify!($name).to_owned(),
                    db: crate::system::db::ID,
                    partition: Some(collection_desc::Partition::Range(collection_desc::RangePartition {})),
                }
            }

            pub fn [<$name:lower _shard_desc>]() -> ShardDesc {
                ShardDesc {
                    id: $col_id,
                    collection_id: $col_id,
                    partition: Some(shard_desc::Partition::Range(RangePartition {
                        start: crate::shard::SHARD_MIN.to_owned(),
                        end: crate::shard::SHARD_MAX.to_owned(),
                    })),
                }
            }
        }
    };
}

// ATTN: The col id must large than `LOCAL_COLLECTION_ID`.

decl_unity_range_col!(database, 1);
decl_unity_range_col!(collection, 2);
decl_unity_range_col!(meta, 3);
decl_unity_range_col!(node, 4);
decl_unity_range_col!(group, 5);
decl_unity_range_col!(replica_state, 6);
decl_unity_range_col!(job, 6);
decl_unity_range_col!(job_history, 8);
decl_unity_range_col!(end_unity_col, 100);

decl_hash_col!(txn, crate::FIRST_TXN_SHARD_ID, 256);

/// Whether the collection is an unity col (which, only contains one shard).
pub fn is_unity_col(col_id: u64) -> bool {
    LOCAL_COLLECTION_ID < col_id && col_id < END_UNITY_COL_ID
}

/// The associated shard id of a collection.
///
/// See [`decl_range_col`] for details.
#[inline]
pub fn shard_id(col_id: u64) -> u64 {
    assert!(is_unity_col(col_id));
    col_id
}
