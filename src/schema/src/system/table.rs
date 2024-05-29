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

use crate::LOCAL_TABLE_ID;

macro_rules! decl_unity_range_table {
    ($name:ident, $table_id:expr) => {
        paste! {
            pub const [<$name:upper _NAME>]: &str = stringify!($name);
            pub const [<$name:upper _ID>]: u64 = $table_id;
            pub const [<$name:upper _SHARD_ID>]: u64 = $table_id;

            pub fn [<$name:lower _desc>]() -> TableDesc {
                TableDesc {
                    id: $table_id,
                    name: stringify!($name).to_owned(),
                    db: crate::system::db::ID,
                }
            }

            pub fn [<$name:lower _shard_desc>]() -> ShardDesc {
                ShardDesc {
                    id: $table_id,
                    table_id: $table_id,
                    range: Some(RangePartition {
                        start: crate::shard::SHARD_MIN.to_owned(),
                        end: crate::shard::SHARD_MAX.to_owned(),
                    }),
                }
            }
        }
    };
}

// ATTN: The table id must large than `LOCAL_TABLE_ID`.

decl_unity_range_table!(database, 1);
decl_unity_range_table!(table, 2);
decl_unity_range_table!(meta, 3);
decl_unity_range_table!(node, 4);
decl_unity_range_table!(group, 5);
decl_unity_range_table!(replica_state, 6);
decl_unity_range_table!(job, 7);
decl_unity_range_table!(job_history, 8);
decl_unity_range_table!(end_unity_table, 100);

decl_unity_range_table!(txn, crate::FIRST_TXN_SHARD_ID);

/// Whether the table is an unity table (which, only contains one shard).
pub fn is_unity_table(table_id: u64) -> bool {
    LOCAL_TABLE_ID < table_id && table_id < END_UNITY_TABLE_ID
}

/// The associated shard id of a table.
///
/// See [`decl_range_table`] for details.
#[inline]
pub fn shard_id(table_id: u64) -> u64 {
    assert!(is_unity_table(table_id));
    table_id
}

#[inline]
pub fn txn_table_id() -> u64 {
    TXN_ID
}
