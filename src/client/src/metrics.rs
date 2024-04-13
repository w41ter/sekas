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

            prepare_intent,
            commit_intent,
            clear_intent,

            transfer,
            accept_shard,
            create_shard,
            move_replicas,
            change_replicas,

            watch,
        }
    }
    pub struct GroupRequestDuration: Histogram {
        "type" => {
            get,
            scan,
            write,

            prepare_intent,
            commit_intent,
            clear_intent,

            transfer,
            accept_shard,
            create_shard,
            move_replicas,
            change_replicas,
        }
    }
}

// For group request
lazy_static! {
    pub static ref GROUP_CLIENT_GROUP_REQUEST_TOTAL_VEC: IntCounterVec = register_int_counter_vec!(
        "group_client_group_request_total",
        "The total group requests issued by group client",
        &["type"]
    )
    .unwrap();
    pub static ref GROUP_CLIENT_GROUP_REQUEST_TOTAL: GroupRequestTotal =
        GroupRequestTotal::from(&GROUP_CLIENT_GROUP_REQUEST_TOTAL_VEC);
    pub static ref GROUP_CLIENT_GROUP_REQUEST_DURATION_SECONDS_VEC: HistogramVec =
        register_histogram_vec!(
            "group_client_group_request_duration_seconds",
            "The intervals of group requests issued by group client",
            &["type"],
            exponential_buckets(0.00005, 1.8, 26).unwrap(),
        )
        .unwrap();
    pub static ref GROUP_CLIENT_GROUP_REQUEST_DURATION_SECONDS: GroupRequestDuration =
        GroupRequestDuration::from(&GROUP_CLIENT_GROUP_REQUEST_DURATION_SECONDS_VEC);
    pub static ref GROUP_CLIENT_RETRY_TOTAL: IntCounter =
        register_int_counter!("group_client_retry_total", "The total retries of group client",)
            .unwrap();
}

pub fn take_group_request_metrics(
    request: &group_request_union::Request,
) -> Option<&'static Histogram> {
    use group_request_union::Request;

    match request {
        Request::Get(_) => {
            GROUP_CLIENT_GROUP_REQUEST_TOTAL.get.inc();
            Some(&GROUP_CLIENT_GROUP_REQUEST_DURATION_SECONDS.get)
        }
        Request::Scan(_) => {
            GROUP_CLIENT_GROUP_REQUEST_TOTAL.scan.inc();
            Some(&GROUP_CLIENT_GROUP_REQUEST_DURATION_SECONDS.scan)
        }
        Request::Write(_) => {
            GROUP_CLIENT_GROUP_REQUEST_TOTAL.write.inc();
            Some(&GROUP_CLIENT_GROUP_REQUEST_DURATION_SECONDS.write)
        }
        Request::WriteIntent(_) => {
            GROUP_CLIENT_GROUP_REQUEST_TOTAL.prepare_intent.inc();
            Some(&GROUP_CLIENT_GROUP_REQUEST_DURATION_SECONDS.prepare_intent)
        }
        Request::CommitIntent(_) => {
            GROUP_CLIENT_GROUP_REQUEST_TOTAL.commit_intent.inc();
            Some(&GROUP_CLIENT_GROUP_REQUEST_DURATION_SECONDS.commit_intent)
        }
        Request::ClearIntent(_) => {
            GROUP_CLIENT_GROUP_REQUEST_TOTAL.clear_intent.inc();
            Some(&GROUP_CLIENT_GROUP_REQUEST_DURATION_SECONDS.clear_intent)
        }
        Request::AcceptShard(_) => {
            GROUP_CLIENT_GROUP_REQUEST_TOTAL.accept_shard.inc();
            Some(&GROUP_CLIENT_GROUP_REQUEST_DURATION_SECONDS.accept_shard)
        }
        Request::CreateShard(_) => {
            GROUP_CLIENT_GROUP_REQUEST_TOTAL.create_shard.inc();
            Some(&GROUP_CLIENT_GROUP_REQUEST_DURATION_SECONDS.create_shard)
        }
        Request::ChangeReplicas(_) => {
            GROUP_CLIENT_GROUP_REQUEST_TOTAL.change_replicas.inc();
            Some(&GROUP_CLIENT_GROUP_REQUEST_DURATION_SECONDS.change_replicas)
        }
        Request::Transfer(_) => {
            GROUP_CLIENT_GROUP_REQUEST_TOTAL.transfer.inc();
            Some(&GROUP_CLIENT_GROUP_REQUEST_DURATION_SECONDS.transfer)
        }
        Request::MoveReplicas(_) => {
            GROUP_CLIENT_GROUP_REQUEST_TOTAL.move_replicas.inc();
            Some(&GROUP_CLIENT_GROUP_REQUEST_DURATION_SECONDS.move_replicas)
        }
        Request::WatchKey(_) => {
            GROUP_CLIENT_GROUP_REQUEST_TOTAL.watch.inc();
            None
        }
    }
}

make_static_metric! {
    pub struct DatabaseRequestTotal: IntCounter {
        "type" => {
            get,
            put,
            delete,
        }
    }
    pub struct DatabaseRequestDuration: Histogram {
        "type" => {
            get,
            put,
            delete,
        }
    }
    pub struct DatabaseBytesTotal: IntCounter {
        "type" => {
            rx,
            tx,
        }
    }
}

lazy_static! {
    pub static ref CLIENT_DATABASE_REQUEST_TOTAL_VEC: IntCounterVec = register_int_counter_vec!(
        "client_database_request_total",
        "The total database requests of client",
        &["type"]
    )
    .unwrap();
    pub static ref CLIENT_DATABASE_REQUEST_TOTAL: DatabaseRequestTotal =
        DatabaseRequestTotal::from(&CLIENT_DATABASE_REQUEST_TOTAL_VEC);
    pub static ref CLIENT_DATABASE_REQUEST_DURATION_SECONDS_VEC: HistogramVec =
        register_histogram_vec!(
            "client_database_request_duration_seconds",
            "The intervals of database requests of client",
            &["type"],
            exponential_buckets(0.00005, 1.8, 26).unwrap(),
        )
        .unwrap();
    pub static ref CLIENT_DATABASE_REQUEST_DURATION_SECONDS: DatabaseRequestDuration =
        DatabaseRequestDuration::from(&CLIENT_DATABASE_REQUEST_DURATION_SECONDS_VEC);
    pub static ref CLIENT_DATABASE_BYTES_TOTAL_VEC: IntCounterVec = register_int_counter_vec!(
        "client_database_bytes_total",
        "The total bytes of client database receive/send",
        &["type"],
    )
    .unwrap();
    pub static ref CLIENT_DATABASE_BYTES_TOTAL: DatabaseBytesTotal =
        DatabaseBytesTotal::from(&CLIENT_DATABASE_BYTES_TOTAL_VEC);
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
