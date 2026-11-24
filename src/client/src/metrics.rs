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
use prost::Message;
use sekas_api::server::v1::*;

make_static_metric! {
    pub struct RetryTotal: IntCounter {
        "reason" => {
            not_found,
            epoch_not_match,
            group_not_accessable,
            not_leader,
            not_root_leader,
            group_not_found,
            connect,
            other,
        }
    }
    label_enum GroupRequestType {
        get,
        scan,
        write,
        query_intent,
        prepare_intent,
        commit_intent,
        clear_intent,
        transfer,
        split_shard,
        merge_shard,
        accept_shard,
        get_split_key,
        inspect_group,
        create_shard,
        delete_shard,
        move_replicas,
        change_replicas,
        watch,
    }
    struct GroupRequestTotal: IntCounter {
        "type" => GroupRequestType,
    }
    struct GroupRequestDuration: Histogram {
        "type" => GroupRequestType,
    }
    struct GroupRequestBytes: IntCounter {
        "type" => GroupRequestType,
        "direction" => { sent, received },
    }
    pub struct TxnDuration: Histogram {
        "path" => { commit, local },
    }
    pub struct TxnBytes: IntCounter {
        "path" => { commit, local },
    }
    pub struct TxnPhaseDuration: Histogram {
        "phase" => {
            allocate_start_version,
            begin,
            prepare,
            allocate_version,
            commit,
            dispatch_commit_intents,
            finish_commit_intents,
            abort,
            clear_intents,
        },
    }
}

// For group request
lazy_static! {
    static ref GROUP_CLIENT_GROUP_REQUEST_TOTAL_VEC: IntCounterVec = register_int_counter_vec!(
        "group_client_group_request_total",
        "The total group requests issued by group client",
        &["type"]
    )
    .unwrap();
    static ref GROUP_CLIENT_GROUP_REQUEST_TOTAL: GroupRequestTotal =
        GroupRequestTotal::from(&GROUP_CLIENT_GROUP_REQUEST_TOTAL_VEC);
    static ref GROUP_CLIENT_GROUP_REQUEST_DURATION_SECONDS_VEC: HistogramVec =
        register_histogram_vec!(
            "group_client_group_request_duration_seconds",
            "The intervals of group requests issued by group client",
            &["type"],
            exponential_buckets(0.00005, 1.8, 26).unwrap(),
        )
        .unwrap();
    static ref GROUP_CLIENT_GROUP_REQUEST_DURATION_SECONDS: GroupRequestDuration =
        GroupRequestDuration::from(&GROUP_CLIENT_GROUP_REQUEST_DURATION_SECONDS_VEC);
    static ref GROUP_CLIENT_GROUP_REQUEST_BYTES_TOTAL_VEC: IntCounterVec = register_int_counter_vec!(
        "group_client_group_request_bytes_total",
        "Encoded Group RPC message bytes by operation and direction, excluding transport framing",
        &["type", "direction"],
    )
    .unwrap();
    static ref GROUP_CLIENT_GROUP_REQUEST_BYTES_TOTAL: GroupRequestBytes =
        GroupRequestBytes::from(&GROUP_CLIENT_GROUP_REQUEST_BYTES_TOTAL_VEC);
    pub static ref GROUP_CLIENT_RETRY_TOTAL: IntCounter =
        register_int_counter!("group_client_retry_total", "The total retries of group client",)
            .unwrap();
    pub static ref GROUP_CLIENT_RETRY_REASON_TOTAL_VEC: IntCounterVec = register_int_counter_vec!(
        "group_client_retry_reason_total",
        "The total retries of group client by reason",
        &["reason"],
    )
    .unwrap();
    pub static ref GROUP_CLIENT_RETRY_REASON_TOTAL: RetryTotal =
        RetryTotal::from(&GROUP_CLIENT_RETRY_REASON_TOTAL_VEC);
    pub static ref GROUP_CLIENT_INVOKE_DURATION_SECONDS: Histogram = register_histogram!(
        "group_client_invoke_duration_seconds",
        "The total time spent by GroupClient::invoke_with_opt for a successful request",
        exponential_buckets(0.00005, 1.8, 26).unwrap(),
    )
    .unwrap();
    pub static ref GROUP_CLIENT_RECOMMEND_CLIENT_DURATION_SECONDS: Histogram = register_histogram!(
        "group_client_recommend_client_duration_seconds",
        "The time spent choosing or fetching a node client in GroupClient",
        exponential_buckets(0.00005, 1.8, 26).unwrap(),
    )
    .unwrap();
    pub static ref GROUP_CLIENT_RPC_BUILD_DURATION_SECONDS: Histogram = register_histogram!(
        "group_client_rpc_build_duration_seconds",
        "The time spent building GroupRequest before issuing the node RPC",
        exponential_buckets(0.00005, 1.8, 26).unwrap(),
    )
    .unwrap();
    pub static ref NODE_CLIENT_GROUP_OPEN_STREAM_DURATION_SECONDS: Histogram = register_histogram!(
        "node_client_group_open_stream_duration_seconds",
        "The time spent opening the node Group server-streaming RPC",
        exponential_buckets(0.00005, 1.8, 26).unwrap(),
    )
    .unwrap();
    pub static ref NODE_CLIENT_GROUP_FIRST_MESSAGE_DURATION_SECONDS: Histogram =
        register_histogram!(
            "node_client_group_first_message_duration_seconds",
            "The time spent waiting for the first message from the node Group stream",
            exponential_buckets(0.00005, 1.8, 26).unwrap(),
        )
        .unwrap();
    static ref CLIENT_TXN_DURATION_SECONDS_VEC: HistogramVec = register_histogram_vec!(
        "client_txn_duration_seconds",
        "Client transaction commit attempt and local fast-path duration",
        &["path"],
        exponential_buckets(0.00005, 1.8, 26).unwrap(),
    )
    .unwrap();
    pub static ref CLIENT_TXN_DURATION_SECONDS: TxnDuration =
        TxnDuration::from(&CLIENT_TXN_DURATION_SECONDS_VEC);
    static ref CLIENT_TXN_COMMITTED_BYTES_TOTAL_VEC: IntCounterVec = register_int_counter_vec!(
        "client_txn_committed_bytes_total",
        "Key and value bytes in successful client transaction commits by path",
        &["path"],
    )
    .unwrap();
    pub static ref CLIENT_TXN_COMMITTED_BYTES_TOTAL: TxnBytes =
        TxnBytes::from(&CLIENT_TXN_COMMITTED_BYTES_TOTAL_VEC);
    pub static ref CLIENT_TXN_COMMIT_KEYS: Histogram = register_histogram!(
        "client_txn_commit_keys",
        "Number of distinct written keys submitted per client transaction commit attempt",
        {
            let mut buckets = vec![0.0];
            buckets.extend(exponential_buckets(1.0, 2.0, 18).unwrap());
            buckets
        },
    )
    .unwrap();
    static ref CLIENT_TXN_PHASE_DURATION_SECONDS_VEC: HistogramVec = register_histogram_vec!(
        "client_txn_phase_duration_seconds",
        "Time spent in a client transaction phase, including its retries",
        &["phase"],
        exponential_buckets(0.00005, 1.8, 26).unwrap(),
    )
    .unwrap();
    pub static ref CLIENT_TXN_PHASE_DURATION_SECONDS: TxnPhaseDuration =
        TxnPhaseDuration::from(&CLIENT_TXN_PHASE_DURATION_SECONDS_VEC);
    pub static ref CLIENT_TXN_LOCAL_PREPARE_GROUP_DURATION_SECONDS: Histogram =
        register_histogram!(
            "client_txn_local_prepare_group_duration_seconds",
            "The time spent preparing the local transaction target group",
            exponential_buckets(0.00005, 1.8, 26).unwrap(),
        )
        .unwrap();
    pub static ref CLIENT_TXN_LOCAL_ALLOC_VERSION_DURATION_SECONDS: Histogram =
        register_histogram!(
            "client_txn_local_alloc_version_duration_seconds",
            "The time spent allocating a commit version for local transactions",
            exponential_buckets(0.00005, 1.8, 26).unwrap(),
        )
        .unwrap();
    pub static ref CLIENT_TXN_LOCAL_GROUP_REQUEST_DURATION_SECONDS: Histogram =
        register_histogram!(
            "client_txn_local_group_request_duration_seconds",
            "The time spent issuing LocalTxnWrite to the target group",
            exponential_buckets(0.00005, 1.8, 26).unwrap(),
        )
        .unwrap();
    pub static ref ROOT_CLIENT_ALLOC_TXN_ID_DURATION_SECONDS: Histogram = register_histogram!(
        "root_client_alloc_txn_id_duration_seconds",
        "The total client-side time spent in RootClient::alloc_txn_id",
        exponential_buckets(0.00005, 1.8, 26).unwrap(),
    )
    .unwrap();
    pub static ref ROOT_CLIENT_CORE_LOCK_DURATION_SECONDS: Histogram = register_histogram!(
        "root_client_core_lock_duration_seconds",
        "The time spent locking and cloning the cached root client core",
        exponential_buckets(0.00005, 1.8, 26).unwrap(),
    )
    .unwrap();
    pub static ref ROOT_CLIENT_GET_CLIENT_DURATION_SECONDS: Histogram = register_histogram!(
        "root_client_get_client_duration_seconds",
        "The time spent fetching a cached RPC RootClient",
        exponential_buckets(0.00005, 1.8, 26).unwrap(),
    )
    .unwrap();
    pub static ref ROOT_CLIENT_RPC_DURATION_SECONDS: Histogram = register_histogram!(
        "root_client_rpc_duration_seconds",
        "The time spent awaiting the root RPC future",
        exponential_buckets(0.00005, 1.8, 26).unwrap(),
    )
    .unwrap();
}

pub fn record_retry_reason(err: &crate::Error) {
    use crate::Error;

    GROUP_CLIENT_RETRY_TOTAL.inc();
    match err {
        Error::NotFound(_) => GROUP_CLIENT_RETRY_REASON_TOTAL.not_found.inc(),
        Error::EpochNotMatch(_) => GROUP_CLIENT_RETRY_REASON_TOTAL.epoch_not_match.inc(),
        Error::GroupNotAccessable(_) => GROUP_CLIENT_RETRY_REASON_TOTAL.group_not_accessable.inc(),
        Error::NotLeader(..) => GROUP_CLIENT_RETRY_REASON_TOTAL.not_leader.inc(),
        Error::NotRootLeader(..) => GROUP_CLIENT_RETRY_REASON_TOTAL.not_root_leader.inc(),
        Error::GroupNotFound(_) => GROUP_CLIENT_RETRY_REASON_TOTAL.group_not_found.inc(),
        Error::Connect(_) => GROUP_CLIENT_RETRY_REASON_TOTAL.connect.inc(),
        _ => GROUP_CLIENT_RETRY_REASON_TOTAL.other.inc(),
    }
}

pub(crate) struct GroupRequestMetrics {
    pub duration: Option<&'static Histogram>,
    response_bytes: &'static IntCounter,
}

impl GroupRequestMetrics {
    pub fn record_response(&self, response: &GroupResponse) {
        self.response_bytes.inc_by(response.encoded_len() as u64);
    }
}

pub(crate) fn record_group_request(request: &GroupRequest) -> Option<GroupRequestMetrics> {
    use group_request_union::Request;

    let kind = match request.request.as_ref()?.request.as_ref()? {
        Request::Get(_) => GroupRequestType::get,
        Request::QueryIntent(_) => GroupRequestType::query_intent,
        Request::GetSplitKey(_) => GroupRequestType::get_split_key,
        Request::InspectGroup(_) => GroupRequestType::inspect_group,
        Request::Scan(_) => GroupRequestType::scan,
        Request::Write(_) | Request::LocalTxnWrite(_) => GroupRequestType::write,
        Request::WriteIntent(_) => GroupRequestType::prepare_intent,
        Request::CommitIntent(_) => GroupRequestType::commit_intent,
        Request::ClearIntent(_) => GroupRequestType::clear_intent,
        Request::Transfer(_) => GroupRequestType::transfer,
        Request::SplitShard(_) => GroupRequestType::split_shard,
        Request::MergeShard(_) => GroupRequestType::merge_shard,
        Request::AcceptShard(_) => GroupRequestType::accept_shard,
        Request::CreateShard(_) => GroupRequestType::create_shard,
        Request::DeleteShard(_) => GroupRequestType::delete_shard,
        Request::MoveReplicas(_) => GroupRequestType::move_replicas,
        Request::ChangeReplicas(_) => GroupRequestType::change_replicas,
        Request::WatchKey(_) => GroupRequestType::watch,
    };

    GROUP_CLIENT_GROUP_REQUEST_TOTAL.get(kind).inc();
    let bytes = GROUP_CLIENT_GROUP_REQUEST_BYTES_TOTAL.get(kind);
    bytes.sent.inc_by(request.encoded_len() as u64);
    Some(GroupRequestMetrics {
        duration: (kind != GroupRequestType::watch)
            .then(|| GROUP_CLIENT_GROUP_REQUEST_DURATION_SECONDS.get(kind)),
        response_bytes: &bytes.received,
    })
}

make_static_metric! {
    pub label_enum DatabaseOperation {
        get,
        put,
        delete,
        scan,
        commit,
    }
    struct DatabaseRequestTotal: IntCounter {
        "type" => DatabaseOperation,
    }
    struct DatabaseRequestDuration: Histogram {
        "type" => DatabaseOperation,
    }
    struct DatabaseOperationBytes: IntCounter {
        "type" => DatabaseOperation,
        "direction" => { input, output },
    }
}

lazy_static! {
    static ref CLIENT_DATABASE_REQUEST_TOTAL_VEC: IntCounterVec = register_int_counter_vec!(
        "client_database_request_total",
        "The total database requests of client",
        &["type"]
    )
    .unwrap();
    static ref CLIENT_DATABASE_REQUEST_TOTAL: DatabaseRequestTotal =
        DatabaseRequestTotal::from(&CLIENT_DATABASE_REQUEST_TOTAL_VEC);
    static ref CLIENT_DATABASE_REQUEST_DURATION_SECONDS_VEC: HistogramVec =
        register_histogram_vec!(
            "client_database_request_duration_seconds",
            "The intervals of database requests of client",
            &["type"],
            exponential_buckets(0.00005, 1.8, 26).unwrap(),
        )
        .unwrap();
    static ref CLIENT_DATABASE_REQUEST_DURATION_SECONDS: DatabaseRequestDuration =
        DatabaseRequestDuration::from(&CLIENT_DATABASE_REQUEST_DURATION_SECONDS_VEC);
    static ref CLIENT_DATABASE_OPERATION_BYTES_TOTAL_VEC: IntCounterVec =
        register_int_counter_vec!(
            "client_database_operation_bytes_total",
            "Logical input and output payload bytes by database operation",
            &["type", "direction"],
        )
        .unwrap();
    static ref CLIENT_DATABASE_OPERATION_BYTES_TOTAL: DatabaseOperationBytes =
        DatabaseOperationBytes::from(&CLIENT_DATABASE_OPERATION_BYTES_TOTAL_VEC);
}

impl DatabaseOperation {
    pub fn record_request(self, bytes: usize) -> HistogramTimer {
        CLIENT_DATABASE_REQUEST_TOTAL.get(self).inc();
        CLIENT_DATABASE_OPERATION_BYTES_TOTAL.get(self).input.inc_by(bytes as u64);
        CLIENT_DATABASE_REQUEST_DURATION_SECONDS.get(self).start_timer()
    }

    pub fn record_response(self, bytes: usize) {
        CLIENT_DATABASE_OPERATION_BYTES_TOTAL.get(self).output.inc_by(bytes as u64);
    }
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
