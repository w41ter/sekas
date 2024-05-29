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

pub const REPLICAS_PER_GROUP: &str = "replicas_per_group";

/// The replication mode property.
pub const REPLICATION: &str = "replication";
pub const REPLICATION_MAJORITY: &str = "majority";
pub const REPLICATION_ASYNC: &str = "async";

/// The table type property.
pub const TABLE_TYPE: &str = "table_type";
pub const TABLE_TYPE_SYSTEM: &str = "system";
pub const TABLE_TYPE_USER: &str = "user";
