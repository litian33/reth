//! Trie changeset 的计算与缓存工具。
//!
//! 本模块提供按区块计算 trie changeset 的能力：changeset 表示“该区块被处理之前”的 trie 节点旧值。
//!
//! 同时提供一个高效的内存缓存，用于保存这些 changeset，主要用于：
//! - **重组（reorg）支持**：链发生重组时，可快速获取 changeset 以回滚区块
//! - **内存效率**：通过驱逐（eviction）机制控制内存占用上限

use crate::{DatabaseHashedPostState, DatabaseStateRoot, DatabaseTrieCursorFactory};
use alloy_primitives::{map::B256Map, BlockNumber, B256};
use parking_lot::RwLock;
use reth_storage_api::{BlockNumReader, ChangeSetReader, DBProvider, StageCheckpointReader};
use reth_storage_errors::provider::{ProviderError, ProviderResult};
use reth_trie::{
    changesets::compute_trie_changesets,
    trie_cursor::{InMemoryTrieCursorFactory, TrieCursor, TrieCursorFactory},
    HashedPostStateSorted, KeccakKeyHasher, StateRoot, TrieInputSorted,
};
use reth_trie_common::updates::{StorageTrieUpdatesSorted, TrieUpdatesSorted};
use std::{
    collections::{BTreeMap, HashMap},
    ops::RangeInclusive,
    sync::Arc,
    time::Instant,
};
use tracing::debug;

#[cfg(feature = "metrics")]
use reth_metrics::{
    metrics::{Counter, Gauge},
    Metrics,
};

/// 计算指定区块的 trie changeset。
///
/// # 算法
///
/// 对区块 N：
/// 1. 查询区块 N-1 的累积 `HashedPostState` revert（从 db tip 回滚到“处理完 N-1 之后”的状态）
/// 2. 基于该 revert 计算区块 N-1 的累积 `TrieUpdates` revert
/// 3. 查询区块 N 的逐块（per-block）`HashedPostState` revert
/// 4. 基于第 3 步的逐块 revert 构建 prefix sets
/// 5. 使用区块 N-1 的“累积 trie updates + 累积 state revert”创建 overlay
/// 6. 使用 overlay 与区块 N 的逐块 `HashedPostState` 计算区块 N 的 trie updates
/// 7. 使用区块 N-1 的 overlay + 第 6 步得到的 trie updates 计算 changeset
///
/// # 参数
///
/// * `provider` - 可访问 changeset 的数据库 provider
/// * `block_number` - 需要计算 changeset 的区块号
///
/// # 返回值
///
/// 指定区块的 changeset（trie 节点旧值）
///
/// # 错误
///
/// 可能返回错误的情况：
/// - 区块号超过数据库 tip（基于 Finish stage checkpoint）
/// - 数据库访问失败
/// - state root 计算失败
pub fn compute_block_trie_changesets<Provider>(
    provider: &Provider,
    block_number: BlockNumber,
) -> Result<TrieUpdatesSorted, ProviderError>
where
    Provider: DBProvider + StageCheckpointReader + ChangeSetReader + BlockNumReader,
{
    debug!(
        target: "trie::changeset_cache",
        block_number,
        "Computing block trie changesets from database state"
    );

    // 第 1 步：收集/计算 state reverts

    // 仅包含该区块自身的变更
    let individual_state_revert = HashedPostStateSorted::from_reverts::<KeccakKeyHasher>(
        provider,
        block_number..=block_number,
    )?;

    // 从 db tip 回滚到“处理完该区块之后”的状态（回滚该区块之后的所有变更）
    let cumulative_state_revert =
        HashedPostStateSorted::from_reverts::<KeccakKeyHasher>(provider, (block_number + 1)..)?;

    // 从 db tip 回滚到“处理完 block-1 之后”的状态
    let mut cumulative_state_revert_prev = cumulative_state_revert.clone();
    cumulative_state_revert_prev.extend_ref_and_sort(&individual_state_revert);

    // 第 2 步：计算 block-1 的累积 trie updates revert
    // 这会得到“处理完 block-1 之后”的 trie 状态
    let prefix_sets_prev = cumulative_state_revert_prev.construct_prefix_sets();
    let input_prev = TrieInputSorted::new(
        Arc::default(),
        Arc::new(cumulative_state_revert_prev),
        prefix_sets_prev,
    );

    let cumulative_trie_updates_prev =
        StateRoot::overlay_root_from_nodes_with_updates(provider.tx_ref(), input_prev)
            .map_err(ProviderError::other)?
            .1
            .into_sorted();

    // 第 2 步：从逐块 revert 构建 prefix sets（仅包含该区块变更过的路径）
    let prefix_sets = individual_state_revert.construct_prefix_sets();

    // 第 3 步：计算该区块的 trie updates
    // 使用 block-1 的累积 trie updates 作为节点 overlay，并使用该区块对应的累积 state revert
    let input = TrieInputSorted::new(
        Arc::new(cumulative_trie_updates_prev.clone()),
        Arc::new(cumulative_state_revert),
        prefix_sets,
    );

    let trie_updates = StateRoot::overlay_root_from_nodes_with_updates(provider.tx_ref(), input)
        .map_err(ProviderError::other)?
        .1
        .into_sorted();

    // 第 4 步：以 block-1 的累积 trie updates 作为 overlay 来计算 changeset
    // 创建一个 overlay cursor factory，它代表“处理完 block-1 之后”的 trie 状态
    let db_cursor_factory = DatabaseTrieCursorFactory::new(provider.tx_ref());
    let overlay_factory =
        InMemoryTrieCursorFactory::new(db_cursor_factory, &cumulative_trie_updates_prev);

    let changesets =
        compute_trie_changesets(&overlay_factory, &trie_updates).map_err(ProviderError::other)?;

    debug!(
        target: "trie::changeset_cache",
        block_number,
        num_account_nodes = changesets.account_nodes_ref().len(),
        num_storage_tries = changesets.storage_tries_ref().len(),
        "Computed block trie changesets successfully"
    );

    Ok(changesets)
}

/// 使用 changeset 缓存计算区块的 trie updates。
///
/// # 算法
///
/// 对区块 N：
/// 1. 通过缓存获取从 N+1 到 db tip 的累积 trie reverts
/// 2. 使用这些 reverts 创建 overlay cursor factory（代表“处理完区块 N 之后”的 trie 状态）
/// 3. 遍历区块 N 的 account trie changeset
/// 4. 对每个变更路径，用 overlay cursor 查询当前节点值
/// 5. 遍历区块 N 的 storage trie changeset
/// 6. 对每个变更路径，用 overlay cursor 查询当前节点值
/// 7. 返回收集到的 trie updates
///
/// # 参数
///
/// * `cache` - changeset 缓存句柄（用于获取 trie reverts）
/// * `provider` - 数据库 provider（用于访问 changeset 与区块数据）
/// * `block_number` - 需要计算 trie updates 的区块号
///
/// # 返回值
///
/// 表示“处理完该区块之后”的 trie 节点状态的 trie updates
///
/// # 错误
///
/// 可能返回错误的情况：
/// - 区块号超过数据库 tip
/// - 数据库访问失败
/// - 缓存读取/计算失败
pub fn compute_block_trie_updates<Provider>(
    cache: &ChangesetCache,
    provider: &Provider,
    block_number: BlockNumber,
) -> ProviderResult<TrieUpdatesSorted>
where
    Provider: DBProvider + StageCheckpointReader + ChangeSetReader + BlockNumReader,
{
    let tx = provider.tx_ref();

    // 获取数据库 tip（最新已完成的区块号）
    let db_tip_block = provider
        .get_stage_checkpoint(reth_stages_types::StageId::Finish)?
        .as_ref()
        .map(|chk| chk.block_number)
        .ok_or_else(|| ProviderError::InsufficientChangesets {
            requested: block_number,
            available: 0..=0,
        })?;

    // 第 1 步：获取目标区块的 hash
    let block_hash = provider.block_hash(block_number)?.ok_or_else(|| {
        ProviderError::other(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            format!("block hash not found for block number {}", block_number),
        ))
    })?;

    // 第 2 步：从缓存中获取目标区块的 trie changeset
    let changesets = cache.get_or_compute(block_hash, block_number, provider)?;

    // 第 3 步：通过缓存获取“目标区块之后状态”的 trie reverts
    let reverts = cache.get_or_compute_range(provider, (block_number + 1)..=db_tip_block)?;

    // 第 4 步：用这些 reverts 构建 InMemoryTrieCursorFactory
    // 这会得到“处理完目标区块之后”的 trie 状态
    let db_cursor_factory = DatabaseTrieCursorFactory::new(tx);
    let cursor_factory = InMemoryTrieCursorFactory::new(db_cursor_factory, &reverts);

    // 第 5 步：收集目标区块中发生变更的 account trie 节点
    let mut account_nodes = Vec::new();
    let mut account_cursor = cursor_factory.account_trie_cursor()?;

    // 遍历 changeset 里的 account 节点
    for (nibbles, _old_node) in changesets.account_nodes_ref() {
        // 使用 overlay cursor 查询该 trie 节点的当前值
        let node_value = account_cursor.seek_exact(*nibbles)?.map(|(_, node)| node);
        account_nodes.push((*nibbles, node_value));
    }

    // 第 6 步：收集目标区块中发生变更的 storage trie 节点
    let mut storage_tries = B256Map::default();

    // 遍历 changeset 里的 storage tries
    for (hashed_address, storage_changeset) in changesets.storage_tries_ref() {
        let mut storage_cursor = cursor_factory.storage_trie_cursor(*hashed_address)?;
        let mut storage_nodes = Vec::new();

        // 遍历该账户的 storage 节点
        for (nibbles, _old_node) in storage_changeset.storage_nodes_ref() {
            // 查询该 storage trie 节点的当前值
            let node_value = storage_cursor.seek_exact(*nibbles)?.map(|(_, node)| node);
            storage_nodes.push((*nibbles, node_value));
        }

        storage_tries.insert(
            *hashed_address,
            StorageTrieUpdatesSorted { storage_nodes, is_deleted: storage_changeset.is_deleted },
        );
    }

    Ok(TrieUpdatesSorted::new(account_nodes, storage_tries))
}

/// 线程安全的 changeset 缓存。
///
/// 该类型封装了对缓存内部结构的共享可变引用。
/// `RwLock` 允许并发读，同时确保写操作的独占访问。
#[derive(Debug, Clone)]
pub struct ChangesetCache {
    inner: Arc<RwLock<ChangesetCacheInner>>,
}

impl Default for ChangesetCache {
    fn default() -> Self {
        Self::new()
    }
}

impl ChangesetCache {
    /// 创建一个新的缓存。
    ///
    /// 该缓存不设置容量上限，依赖显式调用 `evict()` 来控制内存占用。
    pub fn new() -> Self {
        Self { inner: Arc::new(RwLock::new(ChangesetCacheInner::new())) }
    }

    /// 通过区块 hash 获取 changeset。
    ///
    /// 若缓存中不存在（被驱逐或从未计算过）则返回 `None`。
    /// 同时会更新命中/未命中的统计指标。
    pub fn get(&self, block_hash: &B256) -> Option<Arc<TrieUpdatesSorted>> {
        self.inner.read().get(block_hash)
    }

    /// 将区块的 changeset 插入缓存。
    ///
    /// 本方法不会执行驱逐；需要显式调用 `evict()` 触发驱逐。
    ///
    /// # 参数
    ///
    /// * `block_hash` - 区块 hash
    /// * `block_number` - 区块号（用于追踪与驱逐）
    /// * `changesets` - 需要缓存的 trie changeset
    pub fn insert(&self, block_hash: B256, block_number: u64, changesets: Arc<TrieUpdatesSorted>) {
        self.inner.write().insert(block_hash, block_number, changesets)
    }

    /// 驱逐区块号小于给定值的 changeset。
    ///
    /// 建议在区块持久化到数据库后调用，用于释放缓存中不再需要的 changeset 占用的内存。
    ///
    /// # 参数
    ///
    /// * `up_to_block` - 驱逐区块号 < 此值的条目；区块号 >= 此值的条目会被保留。
    pub fn evict(&self, up_to_block: BlockNumber) {
        self.inner.write().evict(up_to_block)
    }

    /// 从缓存获取 changeset；若缺失则即时计算并写入缓存。
    ///
    /// 这是获取 changeset 的主要 API：当缓存未命中时，会基于数据库状态计算 changeset 并填充缓存。
    ///
    /// # 参数
    ///
    /// * `block_hash` - 目标区块 hash
    /// * `block_number` - 目标区块号（用于写入缓存与日志）
    /// * `provider` - 数据库 provider（用于 DB 访问）
    ///
    /// # 返回值
    ///
    /// 目标区块的 changeset（来自缓存或即时计算）
    pub fn get_or_compute<P>(
        &self,
        block_hash: B256,
        block_number: u64,
        provider: &P,
    ) -> ProviderResult<Arc<TrieUpdatesSorted>>
    where
        P: DBProvider + StageCheckpointReader + ChangeSetReader + BlockNumReader,
    {
        // 优先尝试从缓存读取（读锁）
        {
            let cache = self.inner.read();
            if let Some(changesets) = cache.get(&block_hash) {
                debug!(
                    target: "trie::changeset_cache",
                    ?block_hash,
                    block_number,
                    "Changeset cache HIT"
                );
                return Ok(changesets);
            }
        }

        // 缓存未命中：从数据库计算
        debug!(
            target: "trie::changeset_cache",
            ?block_hash,
            block_number,
            "Changeset cache MISS, computing from database"
        );

        let start = Instant::now();

        // 计算 changeset
        let changesets =
            compute_block_trie_changesets(provider, block_number).map_err(ProviderError::other)?;

        let changesets = Arc::new(changesets);
        let elapsed = start.elapsed();

        debug!(
            target: "trie::changeset_cache",
            ?elapsed,
            block_number,
            ?block_hash,
            "Changeset computed from database and inserting into cache"
        );

        // 写入缓存（写锁）
        {
            let mut cache = self.inner.write();
            cache.insert(block_hash, block_number, Arc::clone(&changesets));
        }

        debug!(
            target: "trie::changeset_cache",
            ?block_hash,
            block_number,
            "Changeset successfully cached"
        );

        Ok(changesets)
    }

    /// 获取或计算一个区块范围内累积的 trie reverts。
    ///
    /// 该方法会获取并累积指定区块范围（闭区间）内的所有 trie changeset（revert）。
    /// changeset 会按从新到旧的顺序（倒序）累积，使得发生冲突时较旧的值具有更高优先级（覆盖较新的值）。
    ///
    /// # 参数
    ///
    /// * `provider` - 数据库 provider（用于 DB 访问与区块查找）
    /// * `range` - 需要累积 reverts 的区块范围（闭区间）
    ///
    /// # 返回值
    ///
    /// 指定范围内所有区块累积后的 trie reverts
    ///
    /// # 错误
    ///
    /// 可能返回错误的情况：
    /// - 范围内任意区块超过数据库 tip
    /// - 数据库访问失败
    /// - 区块 hash 查找失败
    /// - changeset 计算失败
    pub fn get_or_compute_range<P>(
        &self,
        provider: &P,
        range: RangeInclusive<BlockNumber>,
    ) -> ProviderResult<TrieUpdatesSorted>
    where
        P: DBProvider + StageCheckpointReader + ChangeSetReader + BlockNumReader,
    {
        // 获取数据库 tip（最新已完成的区块号）
        let db_tip_block = provider
            .get_stage_checkpoint(reth_stages_types::StageId::Finish)?
            .as_ref()
            .map(|chk| chk.block_number)
            .ok_or_else(|| ProviderError::InsufficientChangesets {
                requested: *range.start(),
                available: 0..=0,
            })?;

        let start_block = *range.start();
        let end_block = *range.end();

        // 若 range 结束区块超过 tip，则返回错误
        if end_block > db_tip_block {
            return Err(ProviderError::InsufficientChangesets {
                requested: end_block,
                available: 0..=db_tip_block,
            });
        }

        let timer = Instant::now();

        debug!(
            target: "trie::changeset_cache",
            start_block,
            end_block,
            db_tip_block,
            "Starting get_or_compute_range"
        );

        // 逐块使用缓存获取并累积 reverts。
        // 以从新到旧的顺序迭代，使得冲突时较旧的 changeset 具有更高优先级（覆盖较新的值）。
        let mut accumulated_reverts = TrieUpdatesSorted::default();

        for block_number in range.rev() {
            // 获取该区块号对应的区块 hash
            let block_hash = provider.block_hash(block_number)?.ok_or_else(|| {
                ProviderError::other(std::io::Error::new(
                    std::io::ErrorKind::NotFound,
                    format!("block hash not found for block number {}", block_number),
                ))
            })?;

            debug!(
                target: "trie::changeset_cache",
                block_number,
                ?block_hash,
                "Looked up block hash for block number in range"
            );

            // 从缓存获取 changeset（或即时计算）
            let changesets = self.get_or_compute(block_hash, block_number, provider)?;

            // 将该区块的 changeset 叠加到累积 reverts 之上。
            // 由于我们从新到旧迭代，较旧的值会在最后写入，并覆盖冲突的较新值（较旧值优先）。
            accumulated_reverts.extend_ref_and_sort(&changesets);
        }

        let elapsed = timer.elapsed();

        let num_account_nodes = accumulated_reverts.account_nodes_ref().len();
        let num_storage_tries = accumulated_reverts.storage_tries_ref().len();

        debug!(
            target: "trie::changeset_cache",
            ?elapsed,
            start_block,
            end_block,
            num_blocks = end_block.saturating_sub(start_block).saturating_add(1),
            num_account_nodes,
            num_storage_tries,
            "Finished accumulating trie reverts for block range"
        );

        Ok(accumulated_reverts)
    }
}

/// 具有显式驱逐策略的内存 trie changeset 缓存。
///
/// 保存已校验但尚未持久化的区块 changeset；以区块 hash 作为 key，便于 reorg 时快速查找。
/// 持久化完成后由 engine API tree handler 显式触发驱逐。
///
/// ## 驱逐策略
///
/// 与常见的“自动驱逐”缓存不同，该缓存需要显式调用驱逐。
/// engine API tree handler 会在区块持久化到数据库之后调用 `evict(block_number)`，
/// 确保在对应区块安全落盘之前 changeset 始终可用。
///
/// ## 指标（metrics）
///
/// 缓存提供若干观测指标：
/// - `hits`：缓存命中次数
/// - `misses`：缓存未命中次数
/// - `evictions`：被驱逐的区块数量
/// - `size`：当前缓存的区块数量
#[derive(Debug)]
struct ChangesetCacheInner {
    /// 缓存条目：block hash -> (block number, changesets)
    entries: HashMap<B256, (u64, Arc<TrieUpdatesSorted>)>,

    /// 用于驱逐的映射：block number -> hashes
    block_numbers: BTreeMap<u64, Vec<B256>>,

    /// 用于监控缓存行为的指标
    #[cfg(feature = "metrics")]
    metrics: ChangesetCacheMetrics,
}

#[cfg(feature = "metrics")]
/// changeset 缓存指标。
///
/// 这些指标用于观测缓存性能，并帮助定位诸如未命中率过高等潜在问题。
#[derive(Metrics, Clone)]
#[metrics(scope = "trie.changeset_cache")]
struct ChangesetCacheMetrics {
    /// 缓存命中计数
    hits: Counter,

    /// 缓存未命中计数
    misses: Counter,

    /// 驱逐计数
    evictions: Counter,

    /// 当前缓存大小（条目数）
    size: Gauge,
}

impl Default for ChangesetCacheInner {
    fn default() -> Self {
        Self::new()
    }
}

impl ChangesetCacheInner {
    /// 创建一个空的 changeset 缓存。
    ///
    /// 该缓存不设置容量上限，依赖显式调用 `evict()` 来控制内存占用。
    fn new() -> Self {
        Self {
            entries: HashMap::new(),
            block_numbers: BTreeMap::new(),
            #[cfg(feature = "metrics")]
            metrics: Default::default(),
        }
    }

    fn get(&self, block_hash: &B256) -> Option<Arc<TrieUpdatesSorted>> {
        match self.entries.get(block_hash) {
            Some((_, changesets)) => {
                #[cfg(feature = "metrics")]
                self.metrics.hits.increment(1);
                Some(Arc::clone(changesets))
            }
            None => {
                #[cfg(feature = "metrics")]
                self.metrics.misses.increment(1);
                None
            }
        }
    }

    fn insert(&mut self, block_hash: B256, block_number: u64, changesets: Arc<TrieUpdatesSorted>) {
        debug!(
            target: "trie::changeset_cache",
            ?block_hash,
            block_number,
            cache_size_before = self.entries.len(),
            "Inserting changeset into cache"
        );

        // 写入条目
        self.entries.insert(block_hash, (block_number, changesets));

        // 将 block hash 加入 block_numbers 映射
        self.block_numbers.entry(block_number).or_default().push(block_hash);

        // 更新 size 指标
        #[cfg(feature = "metrics")]
        self.metrics.size.set(self.entries.len() as f64);

        debug!(
            target: "trie::changeset_cache",
            ?block_hash,
            block_number,
            cache_size_after = self.entries.len(),
            "Changeset inserted into cache"
        );
    }

    fn evict(&mut self, up_to_block: BlockNumber) {
        debug!(
            target: "trie::changeset_cache",
            up_to_block,
            cache_size_before = self.entries.len(),
            "Starting cache eviction"
        );

        // 找出需要驱逐的所有区块号（< up_to_block）
        let blocks_to_evict: Vec<u64> =
            self.block_numbers.range(..up_to_block).map(|(num, _)| *num).collect();

        // 删除所有低于阈值的区块号对应的条目
        #[cfg(feature = "metrics")]
        let mut evicted_count = 0;
        #[cfg(not(feature = "metrics"))]
        let mut evicted_count = 0;

        for block_number in &blocks_to_evict {
            if let Some(hashes) = self.block_numbers.remove(block_number) {
                debug!(
                    target: "trie::changeset_cache",
                    block_number,
                    num_hashes = hashes.len(),
                    "Evicting block from cache"
                );
                for hash in hashes {
                    if self.entries.remove(&hash).is_some() {
                        evicted_count += 1;
                    }
                }
            }
        }

        debug!(
            target: "trie::changeset_cache",
            up_to_block,
            evicted_count,
            cache_size_after = self.entries.len(),
            "Finished cache eviction"
        );

        // 若发生驱逐则更新指标
        #[cfg(feature = "metrics")]
        if evicted_count > 0 {
            self.metrics.evictions.increment(evicted_count as u64);
            self.metrics.size.set(self.entries.len() as f64);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_primitives::map::B256Map;

    // 测试辅助函数：创建一个空的 TrieUpdatesSorted
    fn create_test_changesets() -> Arc<TrieUpdatesSorted> {
        Arc::new(TrieUpdatesSorted::new(vec![], B256Map::default()))
    }

    #[test]
    fn test_insert_and_retrieve_single_entry() {
        let mut cache = ChangesetCacheInner::new();
        let hash = B256::random();
        let changesets = create_test_changesets();

        cache.insert(hash, 100, Arc::clone(&changesets));

        // 应当能够取回
        let retrieved = cache.get(&hash);
        assert!(retrieved.is_some());
        assert_eq!(cache.entries.len(), 1);
    }

    #[test]
    fn test_insert_multiple_entries() {
        let mut cache = ChangesetCacheInner::new();

        // 插入 10 个区块
        let mut hashes = Vec::new();
        for i in 0..10 {
            let hash = B256::random();
            cache.insert(hash, 100 + i, create_test_changesets());
            hashes.push(hash);
        }

        // 应当能够全部取回
        assert_eq!(cache.entries.len(), 10);
        for hash in &hashes {
            assert!(cache.get(hash).is_some());
        }
    }

    #[test]
    fn test_eviction_when_explicitly_called() {
        let mut cache = ChangesetCacheInner::new();

        // 插入 15 个区块（0-14）
        let mut hashes = Vec::new();
        for i in 0..15 {
            let hash = B256::random();
            cache.insert(hash, i, create_test_changesets());
            hashes.push((i, hash));
        }

        // 所有区块都应存在（没有自动驱逐）
        assert_eq!(cache.entries.len(), 15);

        // 显式驱逐区块号 < 4 的条目
        cache.evict(4);

        // 区块 0-3 应当被驱逐
        assert_eq!(cache.entries.len(), 11); // blocks 4-14 = 11 blocks

        // 验证区块 0-3 已被驱逐
        for i in 0..4 {
            assert!(cache.get(&hashes[i as usize].1).is_none(), "Block {} should be evicted", i);
        }

        // 验证区块 4-14 仍然存在
        for i in 4..15 {
            assert!(cache.get(&hashes[i as usize].1).is_some(), "Block {} should be present", i);
        }
    }

    #[test]
    fn test_eviction_with_persistence_watermark() {
        let mut cache = ChangesetCacheInner::new();

        // 插入区块 100-165
        let mut hashes = std::collections::HashMap::new();
        for i in 100..=165 {
            let hash = B256::random();
            cache.insert(hash, i, create_test_changesets());
            hashes.insert(i, hash);
        }

        // 所有区块都应存在（没有自动驱逐）
        assert_eq!(cache.entries.len(), 66);

        // 模拟持久化到区块 164，并保留 64 个区块的窗口
        // 驱逐阈值 = 164 - 64 = 100
        cache.evict(100);

        // 区块 100-165 应保留（66 个区块）
        assert_eq!(cache.entries.len(), 66);

        // 模拟持久化到区块 165
        // 驱逐阈值 = 165 - 64 = 101
        cache.evict(101);

        // 区块 101-165 应保留（65 个区块）
        assert_eq!(cache.entries.len(), 65);
        assert!(cache.get(&hashes[&100]).is_none());
        assert!(cache.get(&hashes[&101]).is_some());
    }

    #[test]
    fn test_out_of_order_inserts_with_explicit_eviction() {
        let mut cache = ChangesetCacheInner::new();

        // 以随机顺序插入区块
        let hash_10 = B256::random();
        cache.insert(hash_10, 10, create_test_changesets());

        let hash_5 = B256::random();
        cache.insert(hash_5, 5, create_test_changesets());

        let hash_15 = B256::random();
        cache.insert(hash_15, 15, create_test_changesets());

        let hash_3 = B256::random();
        cache.insert(hash_3, 3, create_test_changesets());

        // 所有区块都应存在（没有自动驱逐）
        assert_eq!(cache.entries.len(), 4);

        // 显式驱逐区块号 < 5 的条目
        cache.evict(5);

        assert!(cache.get(&hash_3).is_none(), "Block 3 should be evicted");
        assert!(cache.get(&hash_5).is_some(), "Block 5 should be present");
        assert!(cache.get(&hash_10).is_some(), "Block 10 should be present");
        assert!(cache.get(&hash_15).is_some(), "Block 15 should be present");
    }

    #[test]
    fn test_multiple_blocks_same_number() {
        let mut cache = ChangesetCacheInner::new();

        // 插入多个相同高度的区块（侧链）
        let hash_1a = B256::random();
        let hash_1b = B256::random();
        cache.insert(hash_1a, 100, create_test_changesets());
        cache.insert(hash_1b, 100, create_test_changesets());

        // 两个都应可取回
        assert!(cache.get(&hash_1a).is_some());
        assert!(cache.get(&hash_1b).is_some());
        assert_eq!(cache.entries.len(), 2);
    }

    #[test]
    fn test_eviction_removes_all_side_chains() {
        let mut cache = ChangesetCacheInner::new();

        // 插入多个相同高度的区块（侧链）
        let hash_10a = B256::random();
        let hash_10b = B256::random();
        let hash_10c = B256::random();
        cache.insert(hash_10a, 10, create_test_changesets());
        cache.insert(hash_10b, 10, create_test_changesets());
        cache.insert(hash_10c, 10, create_test_changesets());

        let hash_20 = B256::random();
        cache.insert(hash_20, 20, create_test_changesets());

        assert_eq!(cache.entries.len(), 4);

        // 驱逐区块号 < 15 的条目：应移除高度为 10 的三条侧链
        cache.evict(15);

        assert_eq!(cache.entries.len(), 1);
        assert!(cache.get(&hash_10a).is_none());
        assert!(cache.get(&hash_10b).is_none());
        assert!(cache.get(&hash_10c).is_none());
        assert!(cache.get(&hash_20).is_some());
    }
}
