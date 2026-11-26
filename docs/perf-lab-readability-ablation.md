# perf-lab 可读性重构与消融实验

日期：2026-10-09。目标是缩短理解场景与测量过程所需的阅读路径，保留明确的场景步骤和测量口径。

## 实现

入口 `mod.rs` 保留 CLI、case 选择和时间工具。`runner.rs` 管理套件执行、错误汇总、报告输出与基线比较；`cluster.rs` 管理节点生命周期；`topology.rs` 管理 group/shard 操作；`observer.rs` 读取 RocksDB 日志和 Prometheus 计数器。

负载模块分为配置与句柄、`scheduler.rs` 的开环/闭环调度、`operation.rs` 的 KV 操作与事务执行、`stats.rs` 的统计和报告数据。事务重试循环独立成函数，预期失败扣除统一通过 `WorkloadReport::unexpected_failures()` 计算。

拓扑主题保留一个显式事件分支，每种事件用具名函数实现。外层负责 baseline、disturbance、recovery、验证和清理；事件指标统一在外层添加阶段前缀。点写和分布式事务直接由名称与 `Operation` 配对表达。

## 三个对照版本

- A / `original`：本次重构开始前保存的源码和 debug 二进制，包含上一轮的迁移/MVCC 修复。
- B / `refactored`：拆分职责、事件函数和事务执行，统一失败统计；保留原有收敛返回字段。
- C / `simplified`：在 B 上删去重复收敛状态，采用成功返回即满足操作后置条件的接口。

三者使用相同配置和存储实现。每次启动新集群，使用 `conf/perf-lab-smoke.toml`：并发 2、16 个 key、1024 字节 value、测量 1 秒、无 warmup。三个版本的顺序逐轮轮换：A/B/C、B/C/A、C/A/B。每版本每场景运行 3 次；共 6 个场景、54 次运行。

比较成功套件的 build profile、runner threads、集群配置、workload 配置、实际 workload 名称和参数、预期错误策略、derived 指标名称。时间戳、运行时计数和耗时不要求完全相等。该比较验证报告契约；场景内部的数据与拓扑断言验证实际操作。

## 删除和保留的设计

| 设计 | 实验或检查结果 | 最终处理 |
| --- | --- | --- |
| 五个 `converged` / `route_converged` 返回字段 | `Result` 与布尔值重复表示失败，调用方还要再次判断 | 删除字段；收敛超时返回 `Err`，成功返回保证收敛 |
| `ReplicaRemoveResult` | 所有调用都要求移除额外副本并恢复三个 voter，结构体只传递耗时和重复校验状态 | 改为 `remove_extra_replica() -> Result<Duration>`，内部验证副本移除及三个 voter |
| 加副本返回耗时 | 所有调用方都忽略返回值，事件已有完整计时 | 改为 `Result<()>` |
| `metric_counter(&self, ...)` | 完全不访问实例状态 | 改为普通函数 |
| `WorkloadHandle::Drop` 清理 | 临时删除后，请求资源释放测试超时失败 | 保留；作用域退出会取消在途任务 |
| 停止时 drain 在途请求 | 临时改成 abort 后，报告未计入已发出的请求，回归测试失败 | 保留；停止继续发请求后等待已发请求完成或超时 |

两个负向消融均产生预期的测试失败（exit code 101），随后恢复原实现。最终两个测试均通过。没有保留负向实验对源码的临时修改。

负载模式继续由 `offered_qps: Option<f64>` 表达，事务参数保留直接字段；具名调度函数和独立事务执行函数已经明确其用途，额外的模式枚举、参数包装器或策略 trait 没有带来新的约束。case 注册也继续使用显式列表与分支。

## 结构变化

| 指标 | A | B | C |
| --- | ---: | ---: | ---: |
| 入口 `mod.rs` 总行数 | 1033 | 108 | 108 |
| 拓扑主题 `run()` 行数（含签名和空行） | 253 | 76 | 76 |
| 最长具名函数行数 | 253 | 155 | 147 |
| 收敛布尔返回字段数 | 5 | 5 | 0 |
| 生产代码非空非注释行数（排除测试模块） | 3813 | 3880 | 3839 |

C 比 B 少 41 行生产代码。相对 A 的总量小幅增加来自模块导入和具名函数边界；主要收益是阅读边界清晰和重复状态减少。统计只涵盖 perf-lab Rust 源码，不含实验脚本。

## 运行结果

54 次运行全部成功，报告契约比较没有发现差异。C 另外补验了
`snapshot-point`、`snapshot-scan`、`split-merge`、`insert-compaction`，4 次均成功。
最终 11 个 perf-lab 单元测试及限定 sekas 包的 Clippy 检查通过。以下为每组 3 次采样的成功 QPS 中位数；完整报告同时保存范围、P99、成功数和失败数。

| 场景 / 代表 workload | A | B | C |
| --- | ---: | ---: | ---: |
| point-hit / `point-hit` | 3258.79 | 3221.85 | 3402.74 |
| cross-shard-scan / `cross_shard_full_scan` | 1850.61 | 2037.40 | 1966.39 |
| read-update / `foreground_ratio_0.5` | 25.75 | 25.73 | 24.99 |
| si-conflict / `hotset_1` | 85.53 | 85.34 | 85.21 |
| leader-transfer / `point_write` | 1836.22 | 1830.05 | 1829.33 |
| node-scale / `after_scale_out` | 2688.34 | 2939.83 | 2650.27 |

`si-conflict` 的预期 SI abort 单独记录，未计入成功吞吐。`read-update` 的 foreground 是固定到达率负载，低 QPS 是配置所控制的到达率。

这些短窗口 debug 结果有波动，不能据此证明性能等价或性能提升。保留 C 的依据是更清楚的接口、减少的重复状态、通过的场景验证和负向消融，而不是某个采样数字更高。性能回归判断需要同机 release 构建、长窗口和更多重复。

本次不代表全部 46 个 case 已通过。上一轮完整 smoke 中的 11 项失败仍需要定位；没有放宽失败阈值或跳过其验证。

## 复现与证据

实验驱动只使用 Python 标准库，二进制路径作为参数传入；执行顺序交错、每次报告独立保存，保留失败运行。

```sh
python3 scripts/perf-lab-ablation.py \
  --variant original=/tmp/sekas-perf-lab-before \
  --variant refactored=/tmp/sekas-perf-lab-refactored \
  --variant simplified=/tmp/sekas-perf-lab-simplified \
  --conf conf/perf-lab-smoke.toml \
  --out-dir target/perf-lab-readability-ablation-new --repeats 3
```

输出目录必须不存在，避免混用旧报告。本次二进制是会话内保存的文件；源码快照与 SHA256 已记录。重做实验时，需要分别构建相应版本。

- `target/perf-lab-readability-ablation-20261009/runs.json`：配置路径、配置和二进制 SHA256、54 次运行退出码与原始报告路径。
- 同目录 `summary.json`：报告契约比较、QPS/P99 和成功失败计数。
- 同目录 `source/`、`structure.json`：三版本源码快照与结构统计。
- `target/perf-lab-readability-controls/`：两个负向实验的日志与结果。

恢复后的验证命令：

```sh
cargo test -p sekas perf_lab --offline
cargo clippy -p sekas --all-targets --no-deps --offline -- -D warnings
```

## 后续：场景对象化

在上述 C 版本基础上，将场景描述恢复为 `Case { kind, scenario }`。
`Case::build()` 将稳定的 CLI 标识转换为具体场景类型；theme 从 `Scenario`
推导，配置要求由对应场景拥有，执行通过场景方法完成。事务场景使用
`TransactionCase { mode, placement, key_matrix }`，运行时不再解释全局
`CaseKind`。其余七个 theme 使用各自的枚举限定合法场景。

`Command::build_cases()` 返回已经构建的对象。runner 继续管理启动、关闭、
错误汇总和报告比较；数据准备仍由 fixture/layout 和具名拓扑方法完成。
没有引入动态 trait、异步回调、注册宏或通用步骤解释器。

11 项 perf-lab 单测、`cargo clippy -p sekas --all-targets --no-deps --offline -- -D warnings`
和 `git diff --check` 通过。注册测试检查全部 CLI case 的覆盖及八个 theme 的
数量；配置测试覆盖 GC、compaction、snapshot catchup 和 node scale 的设置。
与 C 的源码快照比较，集群、配置、观测、报告、拓扑、负载和 support 共 10 个
执行模块保持一致。

使用保存的 C 二进制和场景对象版本进行逐项对照：

| 对照 | 运行数 | 通过 | 失败 | 成功报告契约差异 |
| --- | ---: | ---: | ---: | ---: |
| 八个 theme 的 11 个代表 case | 22 | 22 | 0 | 0 |
| 七种事务 case，并发 2，大事务矩阵 `[2, 4]` | 14 | 10 | 4 | 0 |
| `blind-distributed`、`large-txn`，并发 1，大事务矩阵 `[2, 4]` | 4 | 4 | 0 | 0 |

合计 40 次有效场景运行，36 次通过。双并发补验中，两个版本的
`blind-distributed` 和 `large-txn` 均在 `groups_2_keys_2` 出现一次
`deadline_exceeded`，共四次失败，未放宽验证或请求超时限制。单并发对照
两者均通过，仍需单独定位双并发超时的原因。失败报告没有计入成功报告
契约比较。短 debug 运行验证结构迁移和测量口径，不证明性能等价，也不
替代完整 46-case 或原始大事务矩阵的测试。

原始报告、日志、配置与二进制 SHA256、汇总分别位于：

- `target/perf-lab-case-object-20261009/`
- `target/perf-lab-case-object-transactions-20261009/`
- `target/perf-lab-case-object-transactions-serial-retry-20261009/`

事务补验配置为 `target/perf-lab-case-object-txn-smoke.toml` 和
`target/perf-lab-case-object-txn-serial.toml`。单并发首次尝试受沙箱禁止监听
本地端口影响，四次启动失败记录保留在
`target/perf-lab-case-object-transactions-serial-20261009/`，未计入上述场景运行；
随后在允许监听本地端口的环境重跑。
