# 其他代码库审查

本文档包含我们对其他代码库如何设计其技术栈各个部分的一些研究。

## P2P

* [`Sentry`](https://docs.erigon.tech/fundamentals/modules/sentry)，一个遵循 [Erigon gRPC 架构](https://erigon.substack.com/p/current-status-of-silkworm-and-silkrpc)的可插拔 P2P 节点：
    * [`vorot93`](https://github.com/vorot93/) 首先开始在 [`devp2p`](https://github.com/vorot93/devp2p) 中实现 rust devp2p 栈。
    * vorot93 随后开始开发 sentry，使用 devp2p 来满足通过 gRPC 连接模块化组件的 Erigon 架构。
    * rust-ethereum/devp2p 的代码被合并到 sentry 中，rust-ethereum/devp2p 被归档。
    * vorot93 同时在开发 akula，它使用 sentry 连接到网络。
    * sentry 的代码被合并到 akula 中，sentry 被删除（因此 sentry 链接会出现 404）。

* [`Ranger`](https://github.com/Rjected/ranger)，一个能够与对等节点交互而无需全节点的以太坊 P2P 客户端：
    * [Rjected](https://github.com/Rjected/) 为 P2P 网络研究构建了 Ranger。
    * 它需要将 devp2p-rs 从 Akula 的 sentry 目录提取到单独的仓库中（保留 GPL 许可证），以便在 Ranger 中使用（同样采用 GPL 许可证）。
    * 它还需要创建 [`ethp2p`](https://github.com/Rjected/ethp2p)，这是 [eth wire](https://github.com/ethereum/devp2p/blob/master/caps/eth.md) 协议的一个干净实现，用于 Ranger。

## 数据库 (Database)

* [Erigon 的数据库演练](https://github.com/ledgerwatch/erigon/blob/12ee33a492f5d240458822d052820d9998653a63/docs/programmers_guide/db_walkthrough.MD) 包含一个概述。他们在减少存储方面取得了最显著的改进。
* [Gio 的 erigon-db 表宏](https://github.com/gio256/erigon-db) + [Akula 的宏](https://github.com/akula-bft/akula/blob/74b172ee1d2d2a4f04ce057b5a76679c1b83df9c/src/kv/tables.rs#L61)。

## 区块头下载器 (Header Downloaders)

* Erigon 区块头下载器：
    * 区块头下载器算法在 [`erigon#1016`](https://github.com/ledgerwatch/erigon/pull/1016) 中引入，并在 [`erigon#1145`](https://github.com/ledgerwatch/erigon/pull/1145) 中完成。在高层面上，下载器通过哈希并发请求区块头，然后对响应进行排序、验证并融合成链段。随着它们之间的间隔被填满，较小的段被融合成较大的段。该下载器还用于维护硬编码哈希（后来重命名为预验证哈希）以引导同步。
    * 该下载器经过多次重构：[`erigon#1471`](https://github.com/ledgerwatch/erigon/pull/1471)、[`erigon#1559`](https://github.com/ledgerwatch/erigon/pull/1559) 和 [`erigon#2035`](https://github.com/ledgerwatch/erigon/pull/2035)。
    * 随着 PoS 转型，在 [`erigon#3075`](https://github.com/ledgerwatch/erigon/pull/3075) 中引入了终端总难度 (terminal td) 到算法中以停止向前同步。对于向下同步（合并后），下载器现在委托给 [`EthBackendServer`](https://github.com/ledgerwatch/erigon/blob/3c95db00788dc740849c2207d886fe4db5a8c473/ethdb/privateapi/ethbackend.go#L245)。
    * 在 [`erigon#3092`](https://github.com/ledgerwatch/erigon/pull/3092) 中引入了真正的反向 PoS 下载器，它从链尖开始下载区块头批次，直到到达本地头。后来在 [`erigon#3340`](https://github.com/ledgerwatch/erigon/pull/3340) 和 [`erigon#3717`](https://github.com/ledgerwatch/erigon/pull/3717) 中进行了重构。

* Akula 区块头与阶段下载器：
    * 第一个工作版本似乎是在 [`akula#89`](https://github.com/akula-bft/akula/pull/89) 中整合在一起的。区块头阶段调用了下载器，下载器为 [区块头下载](https://github.com/akula-bft/akula/blob/7dfdca134557993fe47fa54750616d3d167187c7/src/downloader/headers/downloader_linear.rs#L135-L149) 创建了一个分阶段流。该过程的大致描述是：请求 -> 接收响应 -> 必要时重试 -> 验证响应 -> 验证附件 -> 保存 -> 重新填充（刷新？）。与 Erigon 一样，它过去依靠预验证哈希来引导下载过程。
    * 在 [`akula#bbde8d`](https://github.com/akula-bft/akula/commit/bbde8d778184c87621ef9ffdbb0cb15f0e17964f) 中引入了并发下载器。它分派多个请求，收集并验证响应，然后插入区块头。同样的逻辑在 [`akula#38381e`](https://github.com/akula-bft/akula/commit/38381e0b1de752a46216bf1cb0afad5547b87733) 中进行了重构。
    * 在 [`akula#cdc083`](https://github.com/akula-bft/akula/commit/cdc083ff24c0666e29257a714fd2899ed699bee6) 中引入了观察链尖变化。
    * 在 [`akula#fcc1a08`](https://github.com/akula-bft/akula/commit/fcc1a08e4a7ec4955360276d6c8b381ddb82af42) 中引入了真正的共识引擎以及反向下载。此提交中的大部分代码就是我们今天所知道的 Akula 区块头阶段。
