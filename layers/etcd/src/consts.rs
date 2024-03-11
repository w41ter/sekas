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

/// The sekas database name of etcd kv store.
pub const DATABASE_NAME: &str = "etcd";
/// The kv table saves the key value records of the etcd service.
pub const KV_TABLE: &str = "kv";
/// The lease table saves the lease records of the etcd service.
#[allow(dead_code)]
pub const LEASE_TABLE: &str = "lease";
/// The lease keys table save the keys belong to one lease records.
///   key_1 => lease_1
///   key_2 => lease_1
///   ...
///   key_n => lease_m
#[allow(dead_code)]
pub const LEASE_KEYS_TABLE: &str = "lease_keys";
