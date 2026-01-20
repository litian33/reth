# 区块头下载器 (Headers Downloader) 与阶段 (Stage)

> Reth 区块头下载器和阶段的工作原理说明

* 我们首先勾勒了一个通用的区块头下载器接口，以便我们可以支持实现该接口的多种下载策略。参见 [`reth#58`](https://github.com/paradigmxyz/reth/pull/58) 和 [`reth#118`](https://github.com/paradigmxyz/reth/pull/118)。
* 首先，我们实现了反向线性下载 (reverse linear download)。它接收当前链尖 (chain tip) 和本地头 (local head) 作为参数，并从链尖开始分批请求区块，并在请求失败时重试。参见 [`reth#58`](https://github.com/paradigmxyz/reth/pull/58) 和 [`reth#119`](https://github.com/paradigmxyz/reth/pull/119)。
* 区块头阶段 (headers stage) 的第一个完整实现在 [`reth#126`](https://github.com/paradigmxyz/reth/pull/126) 中引入。该阶段查找本地头，向共识层查询链尖，并调用下载器，将它们作为参数传递。下载完成后，该阶段将按照升序通过将条目追加到相应表中来插入区块头。
* 原始下载器在 [`reth#249`](https://github.com/paradigmxyz/reth/pull/249) 中进行了重构，返回一个 `Future`，该 Future 在下载完成或轮询期间发生错误时解析。此 Future 始终保留指向当前请求的指针，允许在失败时重试请求。区块头阶段的插入逻辑保持不变。
    * 注意：到此为止，区块头阶段在开始插入之前，会等待完整范围的区块（从本地头到链尖）下载完成。
* [`reth#296`](https://github.com/paradigmxyz/reth/pull/296) 引入了下载的 `Stream` 实现以及区块头阶段的提交阈值 (commit threshold)。`Stream` 实现一旦接收到并验证了区块头就会产出它们。它会分派下一批区块头的请求，直到到达头部。区块头阶段现在具有可配置的提交阈值，允许配置插入批次大小。通过此更改，区块头阶段不再等待下载完成，而是从流中收集区块头直到达到提交阈值参数。收集完成后，该阶段开始插入该批次。此过程重复进行，直到流耗尽。此时，我们填充了除 HeadersTD（总难度）之外的所有表，因为总难度必须按线性升序计算。该阶段开始遍历已填充的区块头表，并计算并插入新的总难度值。
* 此区块头实现是独特的，因为它被实现为流 (Stream)，一旦区块头可用就会产出（而不是等待下载完成），并且它在缓冲区中仅保留一个区块头（用于形成下一个区块头请求）。
