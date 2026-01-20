# Stages（流水线阶段）

`stages` 库在节点同步、状态维护、数据库更新等方面扮演核心角色。Reth pipeline 中涉及的各个 stage 会被排队并存放在 pipeline 内部。在默认配置下，pipeline 会按顺序运行以下阶段：

- EraStage（可选，用于导入 ERA1）
- HeaderStage
- BodyStage
- SenderRecoveryStage
- ExecutionStage
- PruneSenderRecoveryStage（如果启用了 sender recovery 的裁剪）
- MerkleStage（unwind）
- AccountHashingStage
- StorageHashingStage
- MerkleStage（execute）
- MerkleChangeSets
- TransactionLookupStage
- IndexStorageHistoryStage
- IndexAccountHistoryStage
- PruneStage
- FinishStage

当节点首次启动时，会初始化一个新的 `Pipeline`，并把所有阶段加入到 `Pipeline.stages`。随后调用 `Pipeline::run` 来启动 pipeline：它会在一个无限循环中不断执行所有 stage。这个过程会持续同步链数据，并让本地状态保持与链 tip 一致。

pipeline 中的每个 stage 都实现了 `Stage` trait。该 trait 提供了获取 stage id、执行 stage，以及当 stage 执行过程中出现问题时回滚（unwind）数据库变更等接口。

为了更直观地理解 pipeline 每个阶段在做什么，我们从 `EraStage` 开始，逐步梳理 stage 执行时的底层过程。

<br>

## EraStage

`EraStage` 是一个可选阶段，用于从 [ERA1 文件](https://github.com/eth-clients/e2store-format-specs/blob/main/formats/era1.md) 导入 Merge 之前的历史区块数据。ERA1 是一种标准化格式，用于存储以太坊历史链数据；节点可以通过导入预先同步好的数据来快速启动，而无需从 peers 下载全部历史数据。

启用后，`EraStage` 会从 ERA1 文件中读取区块头与区块体（来源可以是本地目录，或从远端 HTTP 主机下载），并把它们直接写入 static files。对于同步 Merge 之前的链段，这是一个比 P2P 下载更快的方案。

该 stage 会按顺序处理 ERA1 文件，从 genesis 一直提取 headers/bodies 到最后一个 Merge 前区块。注意：ERA1 文件不包含 receipts；这些会在后续的 `ExecutionStage` 中生成。

如果未配置 ERA1 来源，或者所有 ERA1 数据都已经导入完成，该 stage 只会“透传”（pass through），让后续 stage 继续基于 P2P 的同步流程。

<br>

## HeaderStage

`HeaderStage` 负责同步区块头、验证区块头完整性，并将 headers 写入存储。stage 运行时会先计算本地 head 与 tip 的同步缺口，然后通过一个 `HeaderDownloader` stream 以“倒序”（从 tip 向本地 head 回溯）的方式下载 headers。headers 会先缓存在 ETL collector 中，随后一次性写入 static files，并将 `HeaderNumbers` 写入数据库。

`HeaderStage` 依赖 downloader stream 以降序返回 headers：从链 tip 一直回到数据库中最新的区块高度。与 pipeline 中其他 stage 往往从数据库最新区块向 tip 前进不同，`HeaderStage` 采用倒序同步以降低 [长程攻击（long-range attacks）](https://messari.io/report/long-range-attack) 风险：如果节点按升序下载 headers，直到接近最新区块时才可能意识到自己在被长程攻击。为此，`HeaderStage` 从 tip 开始，先验证 tip，再沿着 parent hash 逐步向后回溯。

在 downloader 把 header yield 出来之前，会验证每个 header 能正确挂接到其 parent，并满足共识预期。下载完成后 headers 会被写入存储。如果某个 header 无效或 stream 遇到其它错误，错误会向上传递，数据库变更会被 unwind，并从最近的有效状态继续执行该 stage。

这个过程会一直持续到所有 headers 下载并写入完成。最后函数会返回类似 `Ok(ExecOutput { checkpoint: StageCheckpoint::new(last_header_number).with_headers_stage_checkpoint(...), done: true })` 的结果，表示 header 同步已成功完成。

<br>

## BodyStage

当 `HeaderStage` 成功完成后，`BodyStage` 开始执行。该阶段为新下载并写入数据库的区块头下载对应的区块体。`BodyStage` 会先根据 `header.ommers_hash` 与 `header.transaction_root` 判断哪些区块需要下载区块体。

ommers hash 是对区块中 ommers 列表的 Keccak 256-bit 哈希。如果你不熟悉 ommers，可以 [点这里了解](https://ethereum.org/en/glossary/#ommer)。需要注意的是：在 PoW 时代 ommers 很重要；但在 PoS（Merge 之后）每次只选出一个 proposer 出块，因此 Merge 后不再需要 ommers。

transactions root 是由区块中交易列表计算得到的值：先基于交易列表构建 [merkle tree](https://blog.ethereum.org/2015/11/15/merkling-in-ethereum)，再对 merkle 树根节点做 Keccak 256-bit 哈希得到 root。

当 `BodyStage` 扫描 headers 来决定哪些区块需要下载时，会跳过 `header.ommers_hash` 与 `header.transaction_root` 为空的区块；这通常表示该区块也是空区块。

一旦 `BodyStage` 确定了要抓取的区块体范围，就会创建一个新的 `bodies_stream`，从 `starting_block` 一直下载到 `target_block`。每次 `bodies_stream` yield 出值，就会收到一个响应，表示该区块要么为空区块，要么包含完整区块体可写入。

`BodyStage` 会把收到的区块体写入存储。区块体相对于 header 的正确性验证由 downloader 以及后续的 execution/consensus 阶段进一步保证。该过程对每个下载到的区块体重复进行，并通过返回 `Ok(ExecOutput { checkpoint: StageCheckpoint::new(highest_block).with_entities_stage_checkpoint(...), done: ... })` 表示进度/完成情况。

<br>

## SenderRecoveryStage

`BodyStage` 成功后，`SenderRecoveryStage` 开始执行。它负责为新写入数据库的交易恢复发送者（sender）。在执行函数开始时，会先从数据库取出所有相关交易；随后遍历每笔交易，通过交易签名与交易哈希来恢复 signer。交易哈希通过对 RLP 编码后的交易字节做 Keccak 256-bit 哈希得到，然后传给 `recover_signer`。

在 [ECDSA（椭圆曲线数字签名算法）](https://wikipedia.org/wiki/Elliptic_Curve_Digital_Signature_Algorithm) 签名中，`r`、`s`、`v` 是三段用于数学验证签名真实性的数据。ECDSA 广泛用于生成与验证数字签名，在以太坊等加密货币中尤为常见。

- `r` 是签名过程中计算得到的椭圆曲线点的 x 坐标。
- `s` 是签名过程中的 s 值，由私钥与被签名消息共同决定。
- `v` 是“恢复值”（recovery value），用于从签名中恢复公钥，由签名与被签名消息推导得到。

当交易 signer 被恢复后，会将 signer 写入数据库。该过程对所有读取到的交易重复进行；与前面的阶段类似，会返回 `Ok(ExecOutput { checkpoint: StageCheckpoint::new(end_block).with_entities_stage_checkpoint(...), done: ... })` 来表示该阶段成功完成。

<br>

## ExecutionStage

在 headers、bodies、senders 都写入数据库后，`ExecutionStage` 开始执行。该阶段负责执行所有交易并更新数据库中保存的状态。

当 headers 以及对应交易全部执行完成后，执行产生的所有状态变更会被写入数据库：包括账户余额、账户字节码以及其他状态更新。在 Merge 后的以太坊中，执行层没有通胀式区块奖励；费用/小费（priority tips）在交易执行过程中处理。

在 `execute()` 结束时，会返回类似 `Ok(ExecOutput { checkpoint: StageCheckpoint::new(stage_progress).with_execution_stage_checkpoint(...), done: ... })` 的值，表示 `ExecutionStage` 成功完成。

<br>

## MerkleUnwindStage

`MerkleUnwindStage` 负责在发生 reorg 或需要回滚状态变更时，对 Merkle Patricia Trie 进行 unwind。它会把 unwind 点之后的变更回滚，以确保 trie 与 canonical 历史一致。该阶段通常在 hashing 阶段之前运行，用于在 reorg/rollback 时撤销 trie 状态。

## MerkleExecuteStage

`MerkleExecuteStage` 在 `AccountHashingStage` 与 `StorageHashingStage` 之后运行，负责基于最新的 account/storage 哈希数据构建或更新 state root。它会处理交易执行产生的状态变更，并维护写入区块头的 state root。

<br>

## AccountHashingStage

`AccountHashingStage` 负责计算账户状态的哈希。它遍历状态中的所有账户并计算其加密哈希，这些哈希是构建 state trie 的关键。该阶段对于维护状态完整性以及高效验证状态证明（proof）非常重要。

<br>

## StorageHashingStage

`StorageHashingStage` 负责计算合约存储（storage）的哈希。与 `AccountHashingStage` 类似，它遍历智能合约的 storage slots，生成用于 state trie 的加密哈希。该阶段确保合约存储能被高效验证与证明。

<br>

## MerkleChangeSets

`MerkleChangeSets` 阶段在 `MerkleStage`（execute 模式）之后对 Merkle 相关的变更集进行汇总与最终落盘，确保 trie 更新与 checkpoint 一致。

<br>

## TransactionLookupStage

`TransactionLookupStage` 构建并维护交易查询索引，使得可以通过交易哈希或区块位置高效查询交易。该阶段对 RPC 功能很关键，用户无需扫描整条链即可快速检索交易信息。

<br>

## IndexStorageHistoryStage

`IndexStorageHistoryStage` 为历史合约存储状态建立索引，追踪合约 storage 值随时间的变化，从而支持历史状态查询。这对状态调试、交易追踪以及访问历史状态等功能很重要。

<br>

## IndexAccountHistoryStage

`IndexAccountHistoryStage` 为账户历史建立索引，追踪账户状态（余额、nonce、code）随时间的变化。与 storage history 类似，它支持在任意区块高度对账户状态进行历史查询，是调试与分析工具的重要基础。

<br>

## PruneSenderRecoveryStage

`PruneSenderRecoveryStage` 会根据配置的 prune 模式从 `TransactionSenders` 中移除条目。通常在启用 sender recovery 的裁剪时，于 `ExecutionStage` 之后运行。

<br>

## PruneStage

`PruneStage` 会基于 `PruneModes` 对配置的 segments（例如历史表）执行裁剪。它在 hashing/merkle 与 history indexing 阶段之后运行。

<br>

## FinishStage

`FinishStage` 是 pipeline 的最后阶段，负责清理与校验工作。它确保之前所有阶段都已成功完成，并保证节点状态一致。该阶段也可能更新各类 metrics 与状态指标，以反映一次同步循环的完成。

<br>

# 下一章（Next Chapter）

现在我们已经覆盖了 `Pipeline` 中目前包含的所有阶段，你已经了解 Reth 客户端如何保持与链 tip 同步，并把新的 headers、bodies、senders 以及状态变更写入数据库。虽然本章提供了 pipeline stages 的整体概览，但后续章节会更深入地介绍数据库、网络栈以及 reth 代码库中更有趣的部分。你可以随时去查看本章提到的任何代码；当你准备好了，下一章将进入 `database`。

[下一章](db_zh.md)

