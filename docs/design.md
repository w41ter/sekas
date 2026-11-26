# Design

See [Raft and replica design](replica.md) for request evaluation, replication, and recovery,
[transaction design](txn.md) for MVCC, commit paths, and intent resolution, and the
[Root scheduler design proposal](root-scheduler.md) for placement and task recovery.

## What is Sekas?

Sekas is a distributed key-value store, used as a cache, database, and storage engine.

It has the following advantages:

- Elasticity

    sekas supports automatic data moving and balancing, which allows you easily to scale up or down based on you application needs.

- Fault tolerance

    sekas replicates data to multiple machines, the raft consensus is employed to ensure consistency between replicas and provide automatically fault recovery.

- Easy to use

    sekas is architected and designed with ease of use in mind. In deployment, there is only one binary, the topology is simple, and other nodes such as proxy, master, etc. are not needed. In addition, sekas automatically completes resource configuration as much as possible without requiring users to configure various parameters.

## Architecture

In order to reduce the complexity of deployment and operation and maintenance, all the functions of sekas are concentrated on a binary.

![topology][topology]

[topology]: ./img/topology.drawio.svg

As shown in the figure above, all nodes in a cluster are sekas servers, which internally coordinate and elect nodes that serve metadata.

In sekas, a replication set is called a **Group**. A leader is elected within a Group to serve read and write requests targeting this Group. Groups can be migrated between servers to achieve load balancing.

![architecture][architecture]

[architecture]: ./img/architecture.drawio.svg

Group 0 is called the **Root Group**, which is responsible for balancing, schema maintenance, and providing routing information.

Apart from the root, the storage part of the sekas server is called **Node**. Node is responsible for data access, log replication and shard moving. (A shard is the smallest granularity of data and will be introduced later.)

In addition, node is also responsible for providing group-level scheduling, including automatic maintaining num of replicas, automatic replace bad replicas, and replica moving between servers.

## Automatic placement

Root balances node reads and writes at the same priority, followed by CPU, memory, disk,
Group traffic, shard size, and counts. Raft health, draining, and hard capacity limits take
precedence. Lower-priority work preserves the higher-priority balance, and every copy must fit
at its peak usage.

The `read_units` and `write_units` policies each aggregate their operation across current user
Group leaders and divide by each node's relative service capacity. Each move is scored against
both node distributions. Group improvements break ties between comparable node gains, and
ordinary Group traffic placement can proceed once both node distributions are within tolerance.
`balancer.deadline.read_units` and `balancer.deadline.write_units` cap Group traffic.
Weights default to one. Configure heterogeneous nodes by
catalog node ID under `[root.balance.node_request_weights]`. A weight of two targets twice the
request load of a weight-one node. All Root candidates use the same weights. Co-located nodes may
name the same `cpu`, `memory`, or `disk` domain under `[root.balance.resource_domains]`. A shared
resource is measured once and a copy within the same domain still consumes temporary capacity.

Root first tries a leader transfer, then a complete shard move. If neither can improve request
placement, Root can split a hot range using the storage engine's existing split estimator. Once
the new descriptor and fresh shard rates are available, the next round may move a piece or narrow
the hot range further. Failed split attempts back off. Automatic merge requires both adjacent
ranges to fit the size limit and stay below `root.shard_merge_write_qps` for
`root.shard_merge_quiet_period_sec`, which default to 10 write QPS and 180 seconds. Missing or
interrupted observations and changes to the observed range restart the quiet window.

These schedulers are enabled by default. `SHOW NODES` reports the latest available request
estimates for the observed serving leaders, the weight, and leader counts. `SHOW BALANCER`
retains the last complete measurements while showing the current decision and any wait for
fresh data. The `root_reconcile_node_request_load_fresh` metric indicates whether the latest
planning round had complete request measurements for each operation.
When `root.enable_node_request_balance` is disabled, `balancer.priority` configures the legacy
placement order. During a rolling upgrade, the new split size checks take effect on each Group
leader when it runs the upgraded binary.

## Data organization

![data organization][data-organization]

[data-organization]: ./img/data-organization.drawio.svg

Sekas provides two levels of data organization: database and table.

Database is used to provide isolation between tenants; table is used to provide data isolation within tenants.

A table is partitioned into key-range **Shards** so the cluster can distribute and rebalance
data. Sekas supports splitting and merging shards.

Shards are managed by groups, and the replicas of group is also replicas of shards.

![group and shard][group-and-shard]

[group-and-shard]: ./img/group-and-shard.drawio.svg

As shown in the figure above, a group may be responsible for managing multiple shards. Shards can also be migrated online between groups.
