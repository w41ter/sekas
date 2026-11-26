# perf-lab 输出消融实验

日期：2026-10-09。问题是：哪些输出可以删除，同时保留场景结论、可复现参数和回归判断所需的证据？本实验不以短窗口 QPS 上升作为删除字段的依据。

## 实验设计

三个已保存的 debug 二进制使用相同 smoke 配置，每次启动新集群：

| 版本 | 消融内容 | 保留内容 |
| --- | --- | --- |
| A / original | 无 | 全量 Prometheus counter、gauge、histogram，以及 workload、derived、配置和错误 |
| B / counters | 从 A 删除 interval histogram 和 gauge | 所有 counter；其余报告字段 |
| C / focused | 从 B 删除无场景消费者的 counter | GC 删除版本数、snapshot 发送/下载字节数及 apply 次数；其余报告字段 |

八个代表 case 对应八个 theme：`point-hit`、`snapshot-point`、`read-only-local`、`si-conflict`、`insert-compaction`、`read-update`、`leader-transfer`、`offered-load`。每版本每场景三次，共 72 次。每轮按 A/B/C、B/C/A、C/A/B 排序，顺序执行，避免多组集群并发干扰。

配置为并发 2、16 个 key、1024 字节 value、测量窗口 1 秒、无 warmup。compaction 的存储窗口为 12 秒，拓扑和矩阵场景有多个测量阶段。二进制与配置 SHA256、每次退出码、错误和原始报告路径保存于 `runs.json`。只对成功运行比较报告契约和统计大小；失败记录独立保留。

契约比较包括 build profile、runner threads、cluster/workload 配置、实际 workload 名称/参数/预期错误、workload 字段、两种 latency 字段、阶段名称/字段、derived 指标名称和 metric interval 边界。运行计数、耗时和时间戳允许不同。

另将 A 的每份成功报告投影成 A/B/C 三份，以相同 JSON 格式编码。逐份检查除被删除诊断项以外的证据完全相等，包括所有 workload、derived、配置、错误、时间边界和保留 counter。这样才能把纯输出缩减与不同运行的数值波动分开。离线投影不测量运行时采集开销。

C 的源码还包含 case 列表去重，因此实时 A/C 不是严格只改变一个因素的性能试验；离线投影仅改变输出字段。当前 perf-lab 源码与保存的 C 快照逐文件一致。

## 结果

<!-- RESULTS -->

## 为什么删除这些字段

1. **删除 Prometheus interval histogram。** 它们描述内部子步骤，不等同于 workload 端到端延迟。旧转换统一使用秒到微秒的换算和 `_us` 字段，但实测报告包含 `raft_engine_write_size`、`raft_service_msg_batch_size`、`raft_engine_write_compression_ratio` 等非时间 histogram，单位会被错误表达。用于基线比较的 AVG/P99 来自独立 workload 统计，不依赖这些 histogram。需要内部定位时可查看专门的指标和日志；精简会减少离线诊断信息。
2. **删除 gauge。** 旧实现把 gauge 的终点值放在名为 `counters` 的集合中，它不是区间增量。当前场景与 baseline comparator 均不消费这些值。删除比继续混用快照和增量语义更清楚。
3. **删除没有场景消费者的 counter。** 场景只读取四类维护/快照 counter。保留每个带标签 series 的差分，之后再汇总，避免先聚合导致一个副本的重置抵消另一副本的增量。重置仍按原口径把负差截为零；这不代表可以恢复重置期间丢失的计数。

## 为什么保留其他输出

| 输出 | 保留理由 |
| --- | --- |
| workload 延迟、成功延迟、AVG/P99 及尾延迟 | 端到端性能与成功请求的体验；内部 Prometheus histogram 无法替代 |
| 成功/尝试 QPS、成功/失败数、重试、冲突、错误种类 | 避免把快速失败计为吞吐；区分预期 SI abort 与真正异常 |
| phase_summaries、event duration、max_success_gap | 总体均值可能掩盖拓扑扰动期间的停顿与恢复过程 |
| rows/bytes、实际 parameters、config、build profile | 定义扫描量、事务参与者、工作集和构建条件，防止不兼容结果被比较 |
| derived | 虽然部分数值与 workload 重复，现有 baseline comparator、diff 脚本和 compact baseline 使用它；删除会破坏工具契约 |
| metric interval 边界与四类维护 counter | 保留 GC/snapshot 的分阶段证据；仅有总吞吐不能回答维护是否发生 |
| suite errors、schema_version、run_id | 保留失败事实、解析契约与实验身份，避免不完整套件被当作 baseline |

本次普通场景未触发保留的 GC/snapshot series，故不能用它们证明 GC/snapshot-catchup 场景的行为。四类指标名称与服务端定义及场景消费者核对一致；单测覆盖 series 重置、新 series、阶段边界、旧报告读取与精确名称匹配。完整 GC/snapshot 场景验证仍有必要，不能用合成计数器测试代替。

## 验证与局限

`cargo test -p sekas perf_lab --offline`：12 项通过。
`cargo clippy -p sekas --all-targets --no-deps --offline -- -D warnings` 通过。
依赖 `sekas-server` 有既存 unused imports 警告，不属于这次输出实验。

这些短 debug 测量不能证明性能等价或提升。C 仍调用 `prometheus::gather()` 采集全 registry，再筛选四类 counter；减少了保留、转换与序列化量，不能声称消除了完整采集开销。当前 schema v2 reader 可忽略旧 histogram 字段，compact baseline 本来就清空 interval；依赖已删除 JSON 字段的外部脚本仍需调整。

选择 C 的依据是输出缩减、保留核心测量契约、减少单位歧义和明确的消费路径。不是某次 QPS 更高，也不代表全部 46 个 case 或生产长窗口已验证。

## 复现与证据

```sh
python3 scripts/perf-lab-ablation.py \
  --variant original=/private/tmp/sekas-output-original \
  --variant counters=/private/tmp/sekas-output-counters \
  --variant focused=/private/tmp/sekas-output-focused \
  --conf conf/perf-lab-smoke.toml \
  --out-dir target/perf-lab-output-new --repeats 3 \
  --cases point-hit snapshot-point read-only-local si-conflict \
          insert-compaction read-update leader-transfer offered-load

python3 scripts/perf-lab-output-audit.py \
  target/perf-lab-output-new/runs.json --out-dir target/perf-lab-output-projections-new
```

二进制为本会话已有快照，重做时需从以下源码快照分别构建。新输出目录必须不存在；中断后使用 `--resume`，要求配置和二进制 SHA256 一致，已有记录（含失败）不重跑，未入账的目录重命名保留。

证据目录为 `target/perf-lab-output-ablation-20261009/`：

- `source/{original,counters,focused}/` 与 `structure.json`：源码和哈希。
- `validated-runs/{runs,summary}.json`：有效实验及汇总，各运行目录有报告和日志。
- `projections/summary.json`：同份报告离线消融大小；子目录保存完整投影。
- `runs/`：早先沙箱执行失败记录，系统配置访问错误导致启动失败，不计入有效场景运行。`validated-runs` 中一个未入账的 SI-conflict 目录也保留为 interrupted 记录。
