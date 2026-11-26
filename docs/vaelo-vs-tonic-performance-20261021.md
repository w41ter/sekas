# Vaelo 与 tonic 性能验证（2026-10-21 至 2026-10-22）

## 结论

vaelo 迁移版现已能稳定完成三节点、三副本的 `point-hit` 准备与测量，两个有效
样本均为零失败。此前准备阶段 deadline 的根因不是 sekas 业务语义，而是 vaelo
gRPC framing 持有 HTTP/2 flow-control credit 直到完整消息解码：单条消息超过
65,535-byte 初始窗口时，接收端等待剩余消息，发送端等待 `WINDOW_UPDATE`，形成
死锁。请求和响应两侧均已修复，并增加 128 KiB 双向 unary 回归。

在当前 5 秒 smoke 窗口的两个样本中，vaelo 的单并发 QPS 均值比 tonic 高约
26.6%，双并发 QPS 均值高约 44.6%。双并发延迟分位数也明显更低；单并发 p99/p999
波动较大，与 tonic 基本持平。因此这些结果可以说明当前 vaelo 没有出现端到端性能
退化，并显示出吞吐优势，但样本数和窗口长度仍不足以作为稳定的生产容量结论。

## 方法

- tonic：`f80bf95ef63d7ee48c65bda9ae6858b8b1765c9b` 的干净 detached worktree。
- vaelo：同一 commit 上当前未提交的完整 vaelo 迁移。
- 两边均为 release 构建，使用同一台机器、同一份 perf-lab 配置。
- case：`point-hit`；集群为 3 nodes / 3 voters。
- warmup 1 秒，measurement 5 秒；每个实现记录两个有效样本。
- 配置：`/private/tmp/sekas-perf-ab.toml`。
- 原始结果：`/private/tmp/sekas-perf-ab-results/`。

## 原始样本

### concurrency 1

| implementation | run | QPS | p50 | p99 | p999 | failures |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| tonic | 1 | 4,845.32 | 196 us | 324 us | 648 us | 0 |
| tonic | 2 | 4,752.35 | 198 us | 352 us | 784 us | 0 |
| vaelo | 1 | 6,229.04 | 152 us | 276 us | 444 us | 0 |
| vaelo | 2 | 5,917.23 | 158 us | 396 us | 1,008 us | 0 |

两个样本的均值：tonic 4,798.84 QPS，vaelo 6,073.14 QPS，vaelo 高 26.6%。
p50 均值从 197 us 降到 155 us；p99 和 p999 均值分别为 338/716 us 与
336/726 us，尾延迟在这组短窗口中基本持平。

### concurrency 2

| implementation | run | QPS | p50 | p99 | p999 | failures |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| tonic | 1 | 7,457.61 | 260 us | 396 us | 872 us | 0 |
| tonic | 2 | 7,232.10 | 268 us | 416 us | 880 us | 0 |
| vaelo | 1 | 9,721.63 | 200 us | 316 us | 484 us | 0 |
| vaelo | 2 | 11,514.89 | 168 us | 332 us | 608 us | 0 |

两个样本的均值：tonic 7,344.86 QPS，vaelo 10,618.26 QPS，vaelo 高 44.6%。
p50/p99/p999 均值从 264/406/876 us 降到 184/324/546 us。

原始有效报告：

- `/private/tmp/sekas-perf-ab-results/tonic-r1-point-hit/suite-1792578182322.json`
- `/private/tmp/sekas-perf-ab-results/tonic-rerun-point-hit/suite-1792638954609.json`
- `/private/tmp/sekas-perf-ab-results/vaelo-flow-credit-fix/suite-1792637527048.json`
- `/private/tmp/sekas-perf-ab-results/vaelo-rerun-point-hit/suite-1792638994558.json`

## 验证与限制

- vaelo 全门禁通过：workspace tests、clippy、docs、tonic/vaelo interop。
- sekas `test_client` 5/5、`snapshot_send`、`cluster_rw_watch_key` 和 point-hit 均通过。
- sekas 全 workspace 在 `admin_delete` 一节点测试中不会结束；迁移前 tonic 基线的
  同一精确用例也在 60 秒后保持运行。日志明确显示测试只启动一个节点，但建表要求
  三副本并持续得到 `no group with 3 replicas`，不是本次 vaelo 迁移回归。
- 目前仅覆盖 point read，且 measurement 只有 5 秒。写复制、跨组事务、CPU 和 RSS
  尚未纳入对比；正式容量判断应扩展到更长窗口并交替运行至少 3 轮。
