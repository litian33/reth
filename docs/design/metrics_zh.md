## 指标 (Metrics)

Reth 公开了一些指标，可以通过添加 `--metrics` 标志从 HTTP 端点提供。

### 指标 vs 追踪 (Metrics vs traces)

**指标 (Metric)** 是在时间间隔内测量的数据的数字表示。指标易于进行统计变换，如采样、聚合和相关性分析，这使其适用于报告系统的整体健康状况。

**追踪 (Trace)** 是对一系列因果相关的分布式事件的表示，这些事件编码了关于分布式系统中端到端请求流的信息。追踪用于识别应用程序中每一层完成的工作量，同时保留因果关系。

因此，指标和追踪之间的主要区别在于指标是以系统为中心的，而追踪是以请求为中心的：指标让你了解特定系统的运行情况，而追踪帮助团队识别请求经过各种服务的路径。

**对于大多数情况，你可能需要指标**，除了以下两种场景：

- 对于贡献者，追踪是很好的分析工具
- 对于运行复杂基础设施的终端用户，RPC 组件中的追踪是有意义的

### 如何添加指标

要添加指标，请使用 [`metrics`][metrics] crate。

1. 添加发射指标的代码。
2. 在 crate 的指标描述模块中添加指标说明，例如：[网络指标描述](https://github.com/paradigmxyz/reth/blob/main/crates/net/network/src/metrics.rs)。
3. 在此文件中记录该指标。

#### 指标解剖

指标有三种类型：

- **计数器 (Counters)**：表示（理想情况下）单调递增的值，例如发生的错误数量、处理的区块数量等。
- **仪表盘 (Gauges)**：表示随时间任意上下波动的指标。通常用于测量资源使用情况（内存、CPU ...）和吞吐量。
- **直方图 (Histograms)**：用于存储特定测量的任意数量观测值，并对观测值提供统计分析。典型的用例是某些操作的延迟（写入磁盘、响应请求 ...）。

每个指标由一个 [`Key`][metrics.Key] 标识，该 Key 本身由一个 [`KeyName`][metrics.KeyName] 和任意数量的 [`Label`][metrics.Label] 组成。

`KeyName` 代表实际的指标名称，标签用于进一步细分指标。

例如，代表阶段进度的指标将具有 `stage_progress` 的键名和 `stage` 标签，该标签可用于获取各个阶段的进度。

每个指标 `KeyName` 仅存在一个描述；不可能为标签或 `KeyName`/`Label` 组合添加描述。

#### 创建指标

`metrics` crate 为每种指标变体提供了三个宏：`register_<metric>!`、`<metric>!` 和 `describe_<metric>!`。尽可能优先使用这些宏，因为它们生成了在各种条件下注册和更新指标所需的代码。

- `register_<metric>!` 宏只是创建指标并返回其句柄（例如 `Counter`）。这些指标结构体是线程安全的且克隆成本低。
- `<metric>!` 宏如果指标不存在则注册它，并更新其值。
- `describe_<metric>!` 宏为指标添加终端用户描述。

指标如何暴露给终端用户由 CLI 决定。

### 指标最佳实践

- 使用 `.` 为指标划分命名空间
  - 顶级命名空间 **不** 应该是 `reth` [^1]
- 指标名称不应包含空格
- 在适当的情况下为指标添加单位
  - 使用 Prometheus [基本单位][prom_base_units]
- 不要添加速率指标 (rate-metrics)
  - 速率可以由 Prometheus 等即时计算
- 避免重复指标
  - 例如为连接添加两个指标：`reth.p2p.connections` 用于当前连接，`reth.p2p.connections.total` 用于总连接。其中一个指标可以用来推断另一个。

[^1]: 顶级命名空间由 CLI 使用 [`metrics_util::layers::PrefixLayer`][metrics_util.PrefixLayer] 添加。

### 指标示例
- [交易池指标](../../crates/transaction-pool/src/metrics.rs)
- [网络指标](../../crates/net/network/src/metrics.rs)
- [区块头下载器指标](../../crates/net/downloaders/src/metrics.rs)

### 指标仪表板
- [Grafana 仪表板](../../etc/grafana/dashboards)

[metrics]: https://docs.rs/metrics
[metrics.Key]: https://docs.rs/metrics/latest/metrics/struct.Key.html
[metrics.KeyName]: https://docs.rs/metrics/latest/metrics/struct.KeyName.html
[metrics.Label]: https://docs.rs/metrics/latest/metrics/struct.Label.html
[prom_base_units]: https://prometheus.io/docs/practices/naming/#base-units
[metrics_util.PrefixLayer]: https://docs.rs/metrics-util/latest/metrics_util/layers/struct.PrefixLayer.html
