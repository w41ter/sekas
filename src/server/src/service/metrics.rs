// Copyright 2023-present The Engula Authors.
// Copyright 2022 The Engula Authors.
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
use lazy_static::lazy_static;
use prometheus::*;
use prometheus_static_metric::make_static_metric;
use sekas_api::server::v1::*;

make_static_metric! {
    pub struct GroupRequestTotal: IntCounter {
        "type" => {
            get,
            scan,
            write,
            write_intent,
            commit_intent,
            clear_intent,
            transfer,
            split_shard,
            merge_shard,
            accept_shard,
            create_shard,
            move_replicas,
            change_replicas,
            watch_key,
        }
    }
    pub struct GroupRequestDuration: Histogram {
        "type" => {
            get,
            scan,
            write,
            write_intent,
            commit_intent,
            clear_intent,
            transfer,
            split_shard,
            merge_shard,
            accept_shard,
            create_shard,
            move_replicas,
            change_replicas,
        }
    }
}

// For group request
lazy_static! {
    pub static ref NODE_SERVICE_GROUP_REQUEST_TOTAL_VEC: IntCounterVec = register_int_counter_vec!(
        "node_service_group_request_total",
        "The total group requests of node service",
        &["type"]
    )
    .unwrap();
    pub static ref NODE_SERVICE_GROUP_REQUEST_TOTAL: GroupRequestTotal =
        GroupRequestTotal::from(&NODE_SERVICE_GROUP_REQUEST_TOTAL_VEC);
    pub static ref NODE_SERVICE_GROUP_REQUEST_DURATION_SECONDS_VEC: HistogramVec =
        register_histogram_vec!(
            "node_service_group_request_duration_seconds",
            "The intervals of group requests of node service",
            &["type"],
            exponential_buckets(0.00005, 1.8, 26).unwrap(),
        )
        .unwrap();
    pub static ref NODE_SERVICE_GROUP_REQUEST_DURATION_SECONDS: GroupRequestDuration =
        GroupRequestDuration::from(&NODE_SERVICE_GROUP_REQUEST_DURATION_SECONDS_VEC);
}

pub fn take_group_request_metrics(request: &GroupRequest) -> Option<&'static Histogram> {
    use group_request_union::Request;

    match request.request.as_ref().and_then(|v| v.request.as_ref()) {
        Some(Request::Get(_)) => {
            NODE_SERVICE_GROUP_REQUEST_TOTAL.get.inc();
            Some(&NODE_SERVICE_GROUP_REQUEST_DURATION_SECONDS.get)
        }
        Some(Request::Scan(_)) => {
            NODE_SERVICE_GROUP_REQUEST_TOTAL.scan.inc();
            Some(&NODE_SERVICE_GROUP_REQUEST_DURATION_SECONDS.scan)
        }
        Some(Request::Write(_)) => {
            NODE_SERVICE_GROUP_REQUEST_TOTAL.write.inc();
            Some(&NODE_SERVICE_GROUP_REQUEST_DURATION_SECONDS.write)
        }
        Some(Request::AcceptShard(_)) => {
            NODE_SERVICE_GROUP_REQUEST_TOTAL.accept_shard.inc();
            Some(&NODE_SERVICE_GROUP_REQUEST_DURATION_SECONDS.accept_shard)
        }
        Some(Request::CreateShard(_)) => {
            NODE_SERVICE_GROUP_REQUEST_TOTAL.create_shard.inc();
            Some(&NODE_SERVICE_GROUP_REQUEST_DURATION_SECONDS.create_shard)
        }
        Some(Request::ChangeReplicas(_)) => {
            NODE_SERVICE_GROUP_REQUEST_TOTAL.change_replicas.inc();
            Some(&NODE_SERVICE_GROUP_REQUEST_DURATION_SECONDS.change_replicas)
        }
        Some(Request::Transfer(_)) => {
            NODE_SERVICE_GROUP_REQUEST_TOTAL.transfer.inc();
            Some(&NODE_SERVICE_GROUP_REQUEST_DURATION_SECONDS.transfer)
        }
        Some(Request::MoveReplicas(_)) => {
            NODE_SERVICE_GROUP_REQUEST_TOTAL.move_replicas.inc();
            Some(&NODE_SERVICE_GROUP_REQUEST_DURATION_SECONDS.move_replicas)
        }
        Some(Request::WriteIntent(_)) => {
            NODE_SERVICE_GROUP_REQUEST_TOTAL.write_intent.inc();
            Some(&NODE_SERVICE_GROUP_REQUEST_DURATION_SECONDS.write_intent)
        }
        Some(Request::CommitIntent(_)) => {
            NODE_SERVICE_GROUP_REQUEST_TOTAL.commit_intent.inc();
            Some(&NODE_SERVICE_GROUP_REQUEST_DURATION_SECONDS.commit_intent)
        }
        Some(Request::ClearIntent(_)) => {
            NODE_SERVICE_GROUP_REQUEST_TOTAL.clear_intent.inc();
            Some(&NODE_SERVICE_GROUP_REQUEST_DURATION_SECONDS.clear_intent)
        }
        Some(Request::WatchKey(_)) => {
            NODE_SERVICE_GROUP_REQUEST_TOTAL.watch_key.inc();
            None
        }
        Some(Request::SplitShard(_)) => {
            NODE_SERVICE_GROUP_REQUEST_TOTAL.split_shard.inc();
            Some(&NODE_SERVICE_GROUP_REQUEST_DURATION_SECONDS.split_shard)
        }
        Some(Request::MergeShard(_)) => {
            NODE_SERVICE_GROUP_REQUEST_TOTAL.merge_shard.inc();
            Some(&NODE_SERVICE_GROUP_REQUEST_DURATION_SECONDS.merge_shard)
        }
        None => None,
    }
}

macro_rules! simple_node_method {
    ($name: ident) => {
        paste::paste! {
            lazy_static! {
                pub static ref [<NODE_SERVICE_ $name:upper _REQUEST_TOTAL>]: IntCounter = register_int_counter!(
                    concat!("node_service_", stringify!($name), "_request_total"),
                    concat!("The total ", stringify!($name), " requests of node service")
                )
                .unwrap();
                pub static ref [<NODE_SERVICE_ $name:upper _REQUEST_DURATION_SECONDS>]: Histogram =
                    register_histogram!(
                        concat!("node_service_", stringify!($name), "_request_duration_seconds"),
                        concat!("The intervals of ", stringify!($name), " requests of node service"),
                        exponential_buckets(0.00005, 1.8, 26).unwrap(),
                    )
                    .unwrap();
            }

            pub fn [<take_ $name _request_metrics>]() -> &'static Histogram {
                [<NODE_SERVICE_ $name:upper _REQUEST_TOTAL>].inc();
                &*[<NODE_SERVICE_ $name:upper _REQUEST_DURATION_SECONDS>]
            }
        }
    };
}

simple_node_method!(get_root);
simple_node_method!(create_replica);
simple_node_method!(remove_replica);
simple_node_method!(root_heartbeat);
simple_node_method!(migrate);
simple_node_method!(forward);

macro_rules! simple_root_method {
    ($name: ident) => {
        paste::paste! {
            lazy_static! {
                pub static ref [<ROOT_SERVICE_ $name:upper _REQUEST_TOTAL>]: IntCounter = register_int_counter!(
                    concat!("root_service_", stringify!($name), "_request_total"),
                    concat!("The total ", stringify!($name), " requests of root service")
                )
                .unwrap();
                pub static ref [<ROOT_SERVICE_ $name:upper _REQUEST_DURATION_SECONDS>]: Histogram =
                    register_histogram!(
                        concat!("root_service_", stringify!($name), "_request_duration_seconds"),
                        concat!("The intervals of ", stringify!($name), " requests of root service"),
                        exponential_buckets(0.00005, 1.8, 26).unwrap(),
                    )
                    .unwrap();
            }

            pub fn [<take_ $name _request_metrics>]() -> &'static Histogram {
                [<ROOT_SERVICE_ $name:upper _REQUEST_TOTAL>].inc();
                &*[<ROOT_SERVICE_ $name:upper _REQUEST_DURATION_SECONDS>]
            }
        }
    };
}

simple_root_method!(report);
simple_root_method!(watch);
simple_root_method!(admin);
simple_root_method!(join);
simple_root_method!(alloc_replica);

lazy_static! {
    pub static ref RAFT_SERVICE_MSG_REQUEST_TOTAL: IntCounter = register_int_counter!(
        "raft_service_msg_request_total",
        "The total msg requests of raft service",
    )
    .unwrap();
    pub static ref RAFT_SERVICE_SNAPSHOT_REQUEST_TOTAL: IntCounter = register_int_counter!(
        "raft_service_snapshot_request_total",
        "The total snapshot requests of raft service",
    )
    .unwrap();
    pub static ref RAFT_SERVICE_MSG_BATCH_SIZE: Histogram = register_histogram!(
        "raft_service_msg_batch_size",
        "The batch size of msg requests of raft service",
        exponential_buckets(1.0, 1.8, 22).unwrap(),
    )
    .unwrap();
}

make_static_metric! {
    pub struct DatabaseRequestTotal: IntCounter {
        "type" => {
            get,
            put,
            delete,
            batch,
        }
    }
    pub struct DatabaseRequestDuration: Histogram {
        "type" => {
            get,
            put,
            delete,
            batch,
        }
    }
}

lazy_static! {
    pub static ref PROXY_SERVICE_DATABASE_REQUEST_TOTAL_VEC: IntCounterVec =
        register_int_counter_vec!(
            "proxy_service_database_request_total",
            "The total database requests of proxy service",
            &["type"]
        )
        .unwrap();
    pub static ref PROXY_SERVICE_DATABASE_REQUEST_TOTAL: DatabaseRequestTotal =
        DatabaseRequestTotal::from(&PROXY_SERVICE_DATABASE_REQUEST_TOTAL_VEC);
    pub static ref PROXY_SERVICE_DATABASE_REQUEST_DURATION_SECONDS_VEC: HistogramVec =
        register_histogram_vec!(
            "proxy_service_database_request_duration_seconds",
            "The intervals of database requests of proxy service",
            &["type"],
            exponential_buckets(0.00005, 1.8, 26).unwrap(),
        )
        .unwrap();
    pub static ref PROXY_SERVICE_DATABASE_REQUEST_DURATION_SECONDS: DatabaseRequestDuration =
        DatabaseRequestDuration::from(&PROXY_SERVICE_DATABASE_REQUEST_DURATION_SECONDS_VEC);
}

#[macro_export]
macro_rules! record_latency {
    ($metrics:expr) => {
        let _timer = $metrics.start_timer();
    };
}

#[macro_export]
macro_rules! record_latency_opt {
    ($metrics_opt:expr) => {
        let _timer = $metrics_opt.map(|m| m.start_timer());
    };
}
