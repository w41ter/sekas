#!/usr/bin/python3

import sys

from grafanalib._gen import write_dashboard
from grafanalib.core import (
    BYTES_FORMAT,
    Dashboard,
    GRAPH_TOOLTIP_MODE_SHARED_CROSSHAIR,
    GridPos,
    NO_FORMAT,
    REFRESH_ON_TIME_RANGE_CHANGE,
    RowPanel,
    SECONDS_FORMAT,
    Stat,
    Target,
    Template,
    Templating,
    Time,
    TimeSeries,
)
from grafanalib.formatunits import BYTES_SEC, OPS_PER_SEC, REQUESTS_PER_SEC

PROMETHEUS = {"type": "prometheus", "uid": "${prometheus}"}
JOB_MATCHER = 'job=~"${job:regex}"'
# Older Prometheus parsers reject composite durations such as Grafana's 1m0s.
RATE_INTERVAL = "1m"


def metric_selector(metric, *matchers):
    return metric + "{" + ", ".join((JOB_MATCHER, *matchers)) + "}"


def metric_rate(metric):
    return f"rate({metric_selector(metric)}[{RATE_INTERVAL}])"


def quantile(metric, value, *group):
    buckets = metric_rate(f"{metric}_bucket")
    labels = ", ".join(("le", *group))
    return f"histogram_quantile({value}, sum by ({labels}) ({buckets}))"


def time_series(title, queries, unit=NO_FORMAT, description=""):
    return TimeSeries(
        title=title,
        description=description,
        dataSource=PROMETHEUS,
        targets=[
            Target(
                datasource=PROMETHEUS,
                expr=expr,
                legendFormat=legend,
                refId=chr(ord("A") + index),
            )
            for index, (expr, legend) in enumerate(queries)
        ],
        unit=unit,
        maxDataPoints=600,
        tooltipMode="multi",
    )


def row_panels(title, *panels):
    return RowPanel(title=title, collapsed=True, panels=list(panels))


def legend_for(label):
    return "{{" + label + "}}"


def raw_gauge_ts(title, metric, legend="instance", unit=NO_FORMAT):
    expr = metric_selector(metric)
    if legend != "instance":
        expr = f"sum by ({legend}) ({expr})"
    return time_series(title, [(expr, legend_for(legend))], unit)


def simple_total(title, metric, unit=OPS_PER_SEC):
    return time_series(title, [(metric_rate(metric), "{{instance}}")], unit)


def vector_total(title, metric, group, unit=OPS_PER_SEC):
    expr = f"sum by ({group}) ({metric_rate(metric)})"
    return time_series(title, [(expr, legend_for(group))], unit)


def raw_histogram(title, metric, unit, *group):
    prefix = " / ".join(map(legend_for, group))
    if prefix:
        prefix += " · "
    aggregate = f"sum by ({', '.join(group)})" if group else "sum"
    total = f"{aggregate} ({metric_rate(f'{metric}_sum')})"
    count = f"{aggregate} ({metric_rate(f'{metric}_count')})"
    return time_series(title, [
        (quantile(metric, 0.9999, *group), f"{prefix}p99.99"),
        (quantile(metric, 0.99, *group), f"{prefix}p99"),
        (f"{total} / {count}", f"{prefix}Mean"),
    ], unit)


def simple_duration_seconds(title, metric):
    return raw_histogram(title, metric, SECONDS_FORMAT)


def simple_histogram_size(title, metric, unit=NO_FORMAT):
    return raw_histogram(title, metric, unit)


def vector_duration_seconds(title, metric, *group):
    return raw_histogram(title, metric, SECONDS_FORMAT, *group)


def overview_stat(title, expr, description, thresholds=()):
    return Stat(
        title=title,
        description=description,
        dataSource=PROMETHEUS,
        targets=[
            Target(
                datasource=PROMETHEUS,
                expr=expr,
                legendFormat="{{job}}",
                refId="A",
                instant=True,
            ),
        ],
        color={"mode": "thresholds"} if thresholds else None,
        colorMode="value" if thresholds else "none",
        graphMode="none",
        reduceCalc="lastNotNull",
        textMode="value",
        noValue="No data",
        thresholds=[
            {"color": color, "value": value}
            for value, color in thresholds
        ],
    )


def overview_sections():
    up = metric_selector("up")
    root_leader = metric_selector("root_service_node_as_leader_info")
    schedulable = metric_selector("cluster_node_total", 'type="schedulable"')
    raft_groups = metric_selector("cluster_group_total")
    group_requests = "node_service_group_request_total"
    group_latency = "node_service_group_request_duration_seconds"

    return [
        ("Cluster health", 4, [
            overview_stat(
                "Scrape targets down",
                f"sum by (job) ({up} == bool 0)",
                "Number of configured Sekas targets whose last Prometheus scrape failed.",
                [(None, "green"), (1, "red")],
            ),
            overview_stat(
                "Root leaders observed",
                f"sum by (job) ({root_leader} and on (job, instance) ({up} == 1))"
                f" or (0 * count by (job) ({up} == 1))",
                "Root leaders reported by successfully scraped targets. "
                "A zero means no reachable target reports leadership.",
                [(None, "red"), (1, "green"), (2, "orange")],
            ),
            overview_stat(
                "Schedulable nodes",
                f"max by (job) ({schedulable})",
                "Nodes marked active in cluster metadata, as reported by the root leader.",
            ),
            overview_stat(
                "Raft groups",
                f"max by (job) ({raft_groups})",
                "Group count from cluster metadata, as reported by the root leader.",
            ),
        ]),
        ("Workload performance", 8, [
            time_series(
                "Group RPC throughput",
                [(f"sum by (type) ({metric_rate(group_requests)})", "{{type}}")],
                REQUESTS_PER_SEC,
                "Server-side group RPCs per second by operation type.",
            ),
            time_series(
                "Group RPC latency · p99",
                [(quantile(group_latency, 0.99, "type"), "{{type}}")],
                SECONDS_FORMAT,
                "Server-side latency by operation type. Buckets are pooled across the selected targets.",
            ),
        ]),
        ("Triage", 8, [
            time_series(
                "Heartbeat failures",
                [(f"sum by (node) ({metric_rate('root_heartbeat_fail_total')})", "Node {{node}}")],
                OPS_PER_SEC,
                "Failed root heartbeat RPCs per second by destination node. "
                "Series appear after a node records a failure and there are enough samples for a rate.",
            ),
            time_series(
                "Group-client retries",
                [(f"sum by (reason) ({metric_rate('group_client_retry_reason_total')})", "{{reason}}")],
                OPS_PER_SEC,
                "Retries per second recorded by the group client on the selected scrape targets.",
            ),
            time_series(
                "Raft write path · p99",
                [
                    (quantile("raftgroup_worker_request_in_queue_duration_seconds", 0.99), "Worker queue"),
                    (quantile("raftgroup_log_writer_queue_duration_seconds", 0.99), "Log writer queue"),
                    (quantile("raftgroup_log_writer_write_duration_seconds", 0.99), "Log write"),
                    (quantile("raftgroup_worker_apply_duration_seconds", 0.99), "Apply"),
                ],
                SECONDS_FORMAT,
                "Latency of individual Raft stages across selected targets.",
            ),
        ]),
    ]


def proxy_service_db_panels():
    return row_panels(
        "Proxy services - DB",
        vector_total("request qps", "proxy_service_database_request_total",
                     "type"),
        vector_duration_seconds(
            "request duration",
            "proxy_service_database_request_duration_seconds", "type"),
    )


def node_service_batch_panels():
    return row_panels(
        "Node services - Batch",
        simple_total("batch qps", "node_service_batch_request_total"),
        simple_duration_seconds("batch duration",
                                "node_service_batch_request_duration_seconds"),
        vector_total("group request qps", "node_service_group_request_total",
                     "type"),
        vector_duration_seconds("group request duration",
                                "node_service_group_request_duration_seconds",
                                "type"),
        simple_histogram_size("requests per batch",
                              "node_service_batch_request_size"),
    )


def node_service_shard_migration_panels():
    return row_panels(
        "Node services - Shard migration",
        simple_total("migrate qps", "node_service_migrate_request_total"),
        simple_duration_seconds(
            "migration duration",
            "node_service_migrate_request_duration_seconds"),
        simple_total("pull qps", "node_service_pull_request_total"),
        simple_duration_seconds("pull duration",
                                "node_service_pull_request_duration_seconds"),
        simple_total("forward qps", "node_service_forward_request_total"),
        simple_duration_seconds(
            "forward duration",
            "node_service_forward_request_duration_seconds"),
    )


def node_service_metadata_panels():
    return row_panels(
        "Node services - Metadata",
        simple_total("get_root qps", "node_service_get_root_request_total"),
        simple_duration_seconds(
            "get_root duration",
            "node_service_get_root_request_duration_seconds"),
        simple_total("create_replica qps",
                     "node_service_create_replica_request_total"),
        simple_duration_seconds(
            "create_replica duration",
            "node_service_create_replica_request_duration_seconds"),
        simple_total("remove_replica qps",
                     "node_service_remove_replica_request_total"),
        simple_duration_seconds(
            "remove_replica duration",
            "node_service_remove_replica_request_duration_seconds"),
        simple_total("root_heartbeat qps",
                     "node_service_root_heartbeat_request_total"),
        simple_duration_seconds(
            "root_heartbeat duration",
            "node_service_root_heartbeat_request_duration_seconds"),
    )


def raft_service_panels():
    return row_panels(
        "Raft services",
        simple_total("raft msg qps", "raft_service_msg_request_total"),
        simple_total("raft snapshot qps",
                     "raft_service_snapshot_request_total"),
        simple_histogram_size("batch per msg", "raft_service_msg_batch_size"),
    )


def raft_group_worker_panels():
    return row_panels(
        "Raft Group - Worker",
        simple_total("advance total",
                     "raftgroup_worker_advance_total"),
        simple_histogram_size("advance accumulated bytes",
                              "raftgroup_worker_accumulated_bytes_size",
                              unit=BYTES_FORMAT),
        vector_duration_seconds("Worker queue latency by instance",
                                "raftgroup_worker_request_in_queue_duration_seconds",
                                "instance"),
        simple_duration_seconds("consume requests duration",
                                "raftgroup_worker_consume_requests_duration_seconds"),
        simple_duration_seconds("advance node duration",
                                "raftgroup_worker_advance_duration_seconds"),
        simple_duration_seconds("worker write duration",
                                "raftgroup_worker_write_duration_seconds"),
        vector_duration_seconds("Worker apply latency by instance",
                                "raftgroup_worker_apply_duration_seconds",
                                "instance"),
        simple_histogram_size("apply entries batch size",
                              "raftgroup_worker_apply_entries_size"),
        simple_duration_seconds("compact log duration",
                                "raftgroup_worker_compact_log_duration_seconds"),
    )

def raft_group_request_panels():
    return row_panels(
        "Raft Group - Request",
        vector_total("raft propose qps", "raftgroup_propose_total", "type"),
        vector_duration_seconds("raft propose duration",
                                "raftgroup_propose_duration_seconds", "type"),
        vector_total("raft read qps", "raftgroup_read_total", "type"),
        vector_duration_seconds("raft read duration",
                                "raftgroup_read_duration_seconds", "type"),
        simple_total("raft config change qps",
                     "raftgroup_config_change_total"),
        simple_total("raft transfer leader qps",
                     "raftgroup_transfer_leader_total"),
        simple_total("raft unreachable qps", "raftgroup_unreachable_total"),
    )


def raft_group_snapshot_panels():
    return row_panels(
        "Raft Group - Snapshot",
        simple_total("create qps", "raftgroup_create_snapshot_total"),
        simple_duration_seconds("create duration",
                                "raftgroup_create_snapshot_duration_seconds"),
        simple_total("apply qps", "raftgroup_apply_snapshot_total"),
        simple_duration_seconds("apply duration",
                                "raftgroup_apply_snapshot_duration_seconds"),
        simple_total("send qps", "raftgroup_send_snapshot_total"),
        simple_total("snapshot send rate", "raftgroup_send_snapshot_bytes_total",
                     BYTES_SEC),
        simple_total("download qps", "raftgroup_download_snapshot_total"),
        simple_total("snapshot download rate",
                     "raftgroup_download_snapshot_bytes_total", BYTES_SEC),
        simple_duration_seconds(
            "download duration",
            "raftgroup_download_snapshot_duration_seconds"),
    )


def raft_engine_panels():
    return row_panels(
        "Raft Engine",
        raw_gauge_ts("Raft engine memory", "raft_engine_memory_usage", unit=BYTES_FORMAT),
        raw_gauge_ts("Log entries by queue", "raft_engine_log_entry_count", "type"),
        simple_histogram_size("write size",
                              "raft_engine_write_size",
                              unit=BYTES_FORMAT),
        simple_total("log write throughput",
                     "raft_engine_log_write_bytes_total",
                     unit=BYTES_SEC),
        simple_duration_seconds("write duration",
                                "raft_engine_write_duration_seconds"),
        simple_duration_seconds("apply log entries duration",
                                "raft_engine_write_apply_duration_seconds"),
        simple_duration_seconds("sync log file duration",
                                "raft_engine_sync_log_duration_seconds"),
        simple_duration_seconds("rotate log file duration",
                                "raft_engine_rotate_log_duration_seconds"),
        simple_duration_seconds("allocate log file duration",
                                "raft_engine_allocate_log_duration_seconds"),
    )


def node_panels():
    return row_panels(
        "Node",
        simple_total("replica retry qps", "node_retry_total"),
    )


def node_job_panels():
    return row_panels(
        "Node - Jobs",
        simple_total("pull shard qps", "node_pull_shard_total"),
        simple_total("ingest chunk qps", "node_ingest_chunk_total"),
        simple_duration_seconds("pull shard duration",
                                "node_pull_shard_duration_seconds"),
        simple_total("report qps", "node_report_total"),
        simple_duration_seconds("report duration",
                                "node_report_duration_seconds"),
        simple_total("destroy replica qps", "node_destory_replica_total"),
        simple_duration_seconds("destroy replica duration",
                                "node_destory_replica_duration_seconds"),
    )


def root_service_panels():
    methods = ['report', 'watch', 'admin', 'join', 'alloc_replica']
    panels = []
    for method in methods:
        panels.append(
            simple_total('{} qps'.format(method),
                         'root_service_{}_request_total'.format(method)))
        panels.append(
            simple_duration_seconds(
                '{} duration'.format(method),
                'root_service_{}_request_duration_seconds'.format(method)))
    return row_panels("Root - Services", *panels)


def root_cluster_overview_panels():
    return row_panels(
        "Root - Cluster Overview",
        raw_gauge_ts("node count", "cluster_node_total", "type"),
        raw_gauge_ts("group count", "cluster_group_total", "job"),
        raw_gauge_ts("root reconcile cluster count-based balanced status",
                     "root_reconcile_already_balanced_info", "type"),
        raw_gauge_ts("node replica count",
                     "cluster_node_replica_total", "node"),
        raw_gauge_ts("node leader count", "cluster_node_leader_total", "node"),
        raw_gauge_ts("group shard count",
                     "cluster_group_shard_total", "group"),
    )


def root_misc_panels():
    return row_panels(
        "Root - Misc",
        raw_gauge_ts("node as leader", "root_service_node_as_leader_info"),
        simple_total("root bootstrap failure rate", "root_boostrap_fail_total"),
        simple_duration_seconds(
            "root bootstrap duration", "root_bootstrap_duration_seconds"),
        raw_gauge_ts("root watch table size", "root_watch_table_size", "job"),
        simple_duration_seconds(
            "root notify watcher duration", "root_watch_notify_duration_seconds"),
    )


def root_reconcile_panels():
    return row_panels(
        "Root - Reconcile",
        simple_duration_seconds("root reconcile check and prepare task duration",
                                "root_reconcile_scheduler_check_task_duration_seconds"),
        simple_duration_seconds(
            "root reconcile per tick execute duration", "root_reconcile_step_duration_seconds"),
        raw_gauge_ts("root reconcile scheduler task queue size",
                     "root_reconcile_scheduler_task_queue_size", "job"),
        vector_total(
            "root reconcile handle task rate", "root_reconcile_scheduler_task_handle_total", "type"),
        vector_duration_seconds("root reconcile handle per task duration",
                                "root_reconcile_scheduler_task_handle_duration_seconds", "type"),
        vector_total("root reconcile retry rate by task type",
                     "root_reconcile_scheduler_task_retry_total", "type"),
        vector_duration_seconds("root reconcile create new group per step duration",
                                "root_reconcile_scheduler_create_group_step_duration_seconds", "type"),
        vector_duration_seconds("root reconcile reallocate replica per step duration",
                                "root_reconcile_scheduler_reallocate_replica_step_duration_seconds", "type"),
        vector_duration_seconds("root reconcile create table shards per step duration",
                                "root_reconcile_scheduler_create_table_step_duration_seconds", "type"),
    )


def root_heartbeat_report_panels():
    return row_panels(
        "Root - Heartbeat Report",
        simple_duration_seconds("root heartbeat per tick duration",
                                "root_heartbeat_step_duration_seconds"),
        vector_total("root heartbeat failure rate", "root_heartbeat_fail_total", "node"),
        simple_duration_seconds("root heartbeat rpc duration",
                                "root_heartbeat_rpc_nodes_duration_seconds"),
        simple_duration_seconds("root heartbeat handle group_detail duration",
                                "root_heartbeat_handle_group_detail_seconds"),
        simple_duration_seconds(
            "root heartbeat handle node_stats duration", "root_heartbeat_handle_node_stats_seconds"),
        simple_total("root heartbeat node stats update rate",
                     "root_heartbeat_update_node_stats_total"),
        vector_total("root heartbeat or report group descriptor update rate",
                     "root_update_group_desc_total", "type"),
        vector_total("root heartbeat or report replica state update rate",
                     "root_update_replica_state_total", "type"),
    )


def client_database_panels():
    return row_panels(
        "Client - Database",
        vector_total("database request qps",
            "client_database_request_total", "type"),
        vector_duration_seconds(
            "database request duration",
            "client_database_request_duration_seconds", "type"),
        vector_total("database I/O throughput",
            "client_database_bytes_total", "type", BYTES_SEC),
    )


def client_group_panels():
    return row_panels(
        "Client - Group",
        vector_total("group request qps",
            "group_client_group_request_total", "type"),
        vector_duration_seconds("group request duration",
            "group_client_group_request_duration_seconds", "type"),
        simple_total("group client retry rate",
            "group_client_retry_total"),
    )


def executor_panels():
    return row_panels(
        "Executor",
        simple_total("Park rate", "executor_park_total"),
        vector_total("Task spawn rate", "executor_spawn_total", "priority"),
        simple_duration_seconds("First poll duration",
                                "executor_task_first_poll_duration_seconds"),
        simple_duration_seconds("Poll duration",
                                "executor_task_poll_duration_seconds"),
        simple_duration_seconds("Execute duration",
                                "executor_task_execute_duration_seconds"),
    )


def layout_dashboard(overview, details):
    panels = []
    y = 0
    for title, height, charts in overview:
        panels.append(RowPanel(title=title, gridPos=GridPos(h=1, w=24, x=0, y=y)))
        y += 1
        width = 24 // len(charts)
        for index, chart in enumerate(charts):
            chart.gridPos = GridPos(h=height, w=width, x=index * width, y=y)
        panels.extend(charts)
        y += height

    for row in details:
        row.gridPos = GridPos(h=1, w=24, x=0, y=y)
        y += 1
        for index, chart in enumerate(row.panels):
            chart.gridPos = GridPos(h=9, w=12, x=index % 2 * 12, y=y + index // 2 * 9)
    return panels + details


dashboard = Dashboard(
    title="Sekas Server",
    description="Cluster health, server workload performance, and subsystem diagnostics for a Sekas scrape job.",
    tags=['sekas', 'server'],
    timezone="browser",
    schemaVersion=39,
    time=Time("now-1h", "now"),
    graphTooltip=GRAPH_TOOLTIP_MODE_SHARED_CROSSHAIR,
    templating=Templating(list=[
        Template(name="prometheus", label="Prometheus", type="datasource", query="prometheus"),
        Template(
            name="job",
            label="Sekas job",
            query="label_values(metrics_rpc_requests_total, job)",
            dataSource=PROMETHEUS,
            refresh=REFRESH_ON_TIME_RANGE_CHANGE,
        ),
    ]),
    panels=[
        proxy_service_db_panels(),
        node_service_batch_panels(),
        node_service_shard_migration_panels(),
        node_service_metadata_panels(),
        node_panels(),
        node_job_panels(),
        raft_service_panels(),
        raft_group_worker_panels(),
        raft_group_request_panels(),
        raft_group_snapshot_panels(),
        raft_engine_panels(),
        root_cluster_overview_panels(),
        root_service_panels(),
        root_reconcile_panels(),
        root_heartbeat_report_panels(),
        root_misc_panels(),
        client_database_panels(),
        client_group_panels(),
        executor_panels(),
    ],
).auto_panel_ids()

# Keep the existing detail panel IDs when adding panels above them.
dashboard.panels = layout_dashboard(overview_sections(), dashboard.panels)
dashboard = dashboard.auto_panel_ids()

if __name__ == "__main__":
    write_dashboard(dashboard, sys.stdout)
