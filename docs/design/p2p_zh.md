# P2P

> [Reth P2P 栈](../../crates/net/p2p) 设计过程说明

* 我们的初始设计探索始于 [#64](https://github.com/paradigmxyz/reth/issues/64)，重点是将相关的子协议分层为通用的异步流 (async streams)，然后使用这些流来构建更高级别的网络抽象。
* 遵循上述设计，我们随后实现了 `P2PStream` 和 `EthStream`，分别对应 `p2p` 和 `eth` 子协议。
* 用于在 `EthStream` 中解码消息的有线协议 (wire protocol) 来自 ethp2p，这使得让整个栈工作变得容易。
