//! 引擎树（Engine tree）配置。

use alloy_eips::merge::EPOCH_SLOTS;

/// 当内存中的规范区块数量超过此阈值时触发持久化。
pub const DEFAULT_PERSISTENCE_THRESHOLD: u64 = 2;

/// 距离规范链头多少个区块开始持久化。
pub const DEFAULT_MEMORY_BLOCK_BUFFER_TARGET: u64 = 0;

/// 允许显式配置的最小工作线程数量。
pub const MIN_WORKER_COUNT: usize = 32;

/// 根据可用并行度返回默认的存储工作线程数量。
fn default_storage_worker_count() -> usize {
    #[cfg(feature = "std")]
    {
        std::thread::available_parallelism().map_or(8, |n| n.get() * 2).min(MIN_WORKER_COUNT)
    }
    #[cfg(not(feature = "std"))]
    {
        8
    }
}

/// 返回默认的账户工作线程数量。
///
/// 账户工作线程协调存储证明收集和账户 trie 遍历。
/// 为了简单起见，它们被设置为与存储工作线程相同的数量。
fn default_account_worker_count() -> usize {
    default_storage_worker_count()
}

/// 在一次多重证明计算中派生的证明目标分块大小。
pub const DEFAULT_MULTIPROOF_TASK_CHUNK_SIZE: usize = 60;

/// 为非 reth 进程预留的默认 CPU 核心数量。
///
/// 这将从 reth 全局线程池的线程计数中扣除。
pub const DEFAULT_RESERVED_CPU_CORES: usize = 1;

/// 预热任务的默认最大并发度。
pub const DEFAULT_PREWARM_MAX_CONCURRENCY: usize = 16;

const DEFAULT_BLOCK_BUFFER_LIMIT: u32 = EPOCH_SLOTS as u32 * 2;
const DEFAULT_MAX_INVALID_HEADER_CACHE_LENGTH: u32 = 256;
const DEFAULT_MAX_EXECUTE_BLOCK_BATCH_SIZE: usize = 4;
const DEFAULT_CROSS_BLOCK_CACHE_SIZE: u64 = 4 * 1024 * 1024 * 1024;

/// 确定主机是否有足够的并行度来运行 payload 处理器。
///
/// 至少需要 5 个并行线程：
/// - 主线程中的引擎，负责派生状态根任务。
/// - payload 处理器中的多重证明（Multiproof）任务。
/// - payload 处理器中的稀疏 Trie（Sparse Trie）任务。
/// - payload 处理器中派生的多重证明计算。
/// - 并行 trie 证明中派生的存储根计算。
pub fn has_enough_parallelism() -> bool {
    #[cfg(feature = "std")]
    {
        std::thread::available_parallelism().is_ok_and(|num| num.get() >= 5)
    }
    #[cfg(not(feature = "std"))]
    false
}

/// 引擎树的配置结构体。
#[derive(Debug, Clone)]
pub struct TreeConfig {
    /// 在不触发持久化的情况下，内存中保留的最大区块高度。
    persistence_threshold: u64,
    /// 距离规范链头多少个区块开始持久化。代表为了快速访问和处理重组（reorg）而保留在内存中的理想最新区块数量。
    ///
    /// 注意：此值应小于或等于 `persistence_threshold`。
    memory_block_buffer_target: u64,
    /// 由于缺少父区块而无法执行并保留在缓存中的待处理区块数量。
    block_buffer_limit: u32,
    /// 缓存中保留的无效区块头数量。
    max_invalid_header_cache_length: u32,
    /// 批量顺序执行区块的最大数量。
    ///
    /// 这用于在接收到一批下载的区块时，防止长时间运行的顺序区块执行。
    max_execute_block_batch_size: usize,
    /// 是否使用旧的状态根计算方法，而不是新的状态根任务。
    legacy_state_root: bool,
    /// 是否始终将状态根任务的 trie 更新与常规状态根计算的 trie 更新进行比较。
    always_compare_trie_updates: bool,
    /// 是否禁用状态缓存。
    disable_state_cache: bool,
    /// 是否禁用并行预热。
    disable_prewarming: bool,
    /// 是否禁用并行稀疏 Trie 状态根算法。
    disable_parallel_sparse_trie: bool,
    /// 是否启用状态提供者指标。
    state_provider_metrics: bool,
    /// 跨区块缓存大小（字节）。
    cross_block_cache_size: u64,
    /// 主机是否有足够的并行度来运行状态根任务。
    has_enough_parallelism: bool,
    /// 多重证明任务是否应该对证明目标进行分块。
    multiproof_chunking_enabled: bool,
    /// 多重证明任务的证明目标分块大小。
    multiproof_chunk_size: usize,
    /// 为非 reth 进程预留的 CPU 核心数量。
    reserved_cpu_cores: usize,
    /// 是否禁用预编译缓存。
    precompile_cache_disabled: bool,
    /// 是否使用状态根回退（用于测试）。
    state_root_fallback: bool,
    /// 即使 `forkchoiceState.headBlockHash` 已经是规范链头或其祖先，是否也始终处理 payload 属性并开始 payload 构建过程。
    ///
    /// Engine API 规范通常规定，如果 `forkchoiceState.headBlockHash` 引用了规范链头的“有效（VALID）”祖先，客户端软件“绝不能（MUST NOT）”开始 payload 构建过程。
    /// 参见：<https://github.com/ethereum/execution-apis/blob/main/src/engine/paris.md#engine_forkchoiceupdatedv1>（规则 2）
    ///
    /// 此标志允许覆盖该行为。这对于特定链配置（例如 OP Stack，其中提议者可以重组自己的链）、各种自定义链，
    /// 或者在链头未改变或移动到祖先时仍希望立即重新生成 payload 的开发/测试目的非常有用。
    always_process_payload_attributes_on_canonical_head: bool,
    /// 预热任务的最大并发度。
    prewarm_max_concurrency: usize,
    /// 在分叉选择更新期间是否允许将规范区块头回滚到祖先。
    allow_unwind_canonical_header: bool,
    /// 存储证明工作线程数量。
    storage_worker_count: usize,
    /// 账户证明工作线程数量。
    account_worker_count: usize,
    /// 是否启用 V2 存储证明。
    enable_proof_v2: bool,
}


impl Default for TreeConfig {
    fn default() -> Self {
        Self {
            persistence_threshold: DEFAULT_PERSISTENCE_THRESHOLD,
            memory_block_buffer_target: DEFAULT_MEMORY_BLOCK_BUFFER_TARGET,
            block_buffer_limit: DEFAULT_BLOCK_BUFFER_LIMIT,
            max_invalid_header_cache_length: DEFAULT_MAX_INVALID_HEADER_CACHE_LENGTH,
            max_execute_block_batch_size: DEFAULT_MAX_EXECUTE_BLOCK_BATCH_SIZE,
            legacy_state_root: false,
            always_compare_trie_updates: false,
            disable_state_cache: false,
            disable_prewarming: false,
            disable_parallel_sparse_trie: false,
            state_provider_metrics: false,
            cross_block_cache_size: DEFAULT_CROSS_BLOCK_CACHE_SIZE,
            has_enough_parallelism: has_enough_parallelism(),
            multiproof_chunking_enabled: true,
            multiproof_chunk_size: DEFAULT_MULTIPROOF_TASK_CHUNK_SIZE,
            reserved_cpu_cores: DEFAULT_RESERVED_CPU_CORES,
            precompile_cache_disabled: false,
            state_root_fallback: false,
            always_process_payload_attributes_on_canonical_head: false,
            prewarm_max_concurrency: DEFAULT_PREWARM_MAX_CONCURRENCY,
            allow_unwind_canonical_header: false,
            storage_worker_count: default_storage_worker_count(),
            account_worker_count: default_account_worker_count(),
            enable_proof_v2: false,
        }
    }
}

impl TreeConfig {
    /// Create engine tree configuration.
    #[expect(clippy::too_many_arguments)]
    pub const fn new(
        persistence_threshold: u64,
        memory_block_buffer_target: u64,
        block_buffer_limit: u32,
        max_invalid_header_cache_length: u32,
        max_execute_block_batch_size: usize,
        legacy_state_root: bool,
        always_compare_trie_updates: bool,
        disable_state_cache: bool,
        disable_prewarming: bool,
        disable_parallel_sparse_trie: bool,
        state_provider_metrics: bool,
        cross_block_cache_size: u64,
        has_enough_parallelism: bool,
        multiproof_chunking_enabled: bool,
        multiproof_chunk_size: usize,
        reserved_cpu_cores: usize,
        precompile_cache_disabled: bool,
        state_root_fallback: bool,
        always_process_payload_attributes_on_canonical_head: bool,
        prewarm_max_concurrency: usize,
        allow_unwind_canonical_header: bool,
        storage_worker_count: usize,
        account_worker_count: usize,
        enable_proof_v2: bool,
    ) -> Self {
        Self {
            persistence_threshold,
            memory_block_buffer_target,
            block_buffer_limit,
            max_invalid_header_cache_length,
            max_execute_block_batch_size,
            legacy_state_root,
            always_compare_trie_updates,
            disable_state_cache,
            disable_prewarming,
            disable_parallel_sparse_trie,
            state_provider_metrics,
            cross_block_cache_size,
            has_enough_parallelism,
            multiproof_chunking_enabled,
            multiproof_chunk_size,
            reserved_cpu_cores,
            precompile_cache_disabled,
            state_root_fallback,
            always_process_payload_attributes_on_canonical_head,
            prewarm_max_concurrency,
            allow_unwind_canonical_header,
            storage_worker_count,
            account_worker_count,
            enable_proof_v2,
        }
    }

    /// Return the persistence threshold.
    pub const fn persistence_threshold(&self) -> u64 {
        self.persistence_threshold
    }

    /// Return the memory block buffer target.
    pub const fn memory_block_buffer_target(&self) -> u64 {
        self.memory_block_buffer_target
    }

    /// Return the block buffer limit.
    pub const fn block_buffer_limit(&self) -> u32 {
        self.block_buffer_limit
    }

    /// Return the maximum invalid cache header length.
    pub const fn max_invalid_header_cache_length(&self) -> u32 {
        self.max_invalid_header_cache_length
    }

    /// Return the maximum execute block batch size.
    pub const fn max_execute_block_batch_size(&self) -> usize {
        self.max_execute_block_batch_size
    }

    /// Return whether the multiproof task chunking is enabled.
    pub const fn multiproof_chunking_enabled(&self) -> bool {
        self.multiproof_chunking_enabled
    }

    /// Return the multiproof task chunk size.
    pub const fn multiproof_chunk_size(&self) -> usize {
        self.multiproof_chunk_size
    }

    /// Return the number of reserved CPU cores for non-reth processes
    pub const fn reserved_cpu_cores(&self) -> usize {
        self.reserved_cpu_cores
    }

    /// Returns whether to use the legacy state root calculation method instead
    /// of the new state root task
    pub const fn legacy_state_root(&self) -> bool {
        self.legacy_state_root
    }

    /// Returns whether or not state provider metrics are enabled.
    pub const fn state_provider_metrics(&self) -> bool {
        self.state_provider_metrics
    }

    /// Returns whether or not the parallel sparse trie is disabled.
    pub const fn disable_parallel_sparse_trie(&self) -> bool {
        self.disable_parallel_sparse_trie
    }

    /// Returns whether or not state cache is disabled.
    pub const fn disable_state_cache(&self) -> bool {
        self.disable_state_cache
    }

    /// Returns whether or not parallel prewarming is disabled.
    pub const fn disable_prewarming(&self) -> bool {
        self.disable_prewarming
    }

    /// Returns whether to always compare trie updates from the state root task to the trie updates
    /// from the regular state root calculation.
    pub const fn always_compare_trie_updates(&self) -> bool {
        self.always_compare_trie_updates
    }

    /// Returns the cross-block cache size.
    pub const fn cross_block_cache_size(&self) -> u64 {
        self.cross_block_cache_size
    }

    /// Returns whether precompile cache is disabled.
    pub const fn precompile_cache_disabled(&self) -> bool {
        self.precompile_cache_disabled
    }

    /// Returns whether to use state root fallback.
    pub const fn state_root_fallback(&self) -> bool {
        self.state_root_fallback
    }

    /// Sets whether to always process payload attributes when the FCU head is already canonical.
    pub const fn with_always_process_payload_attributes_on_canonical_head(
        mut self,
        always_process_payload_attributes_on_canonical_head: bool,
    ) -> Self {
        self.always_process_payload_attributes_on_canonical_head =
            always_process_payload_attributes_on_canonical_head;
        self
    }

    /// Returns true if payload attributes should always be processed even when the FCU head is
    /// canonical.
    pub const fn always_process_payload_attributes_on_canonical_head(&self) -> bool {
        self.always_process_payload_attributes_on_canonical_head
    }

    /// Returns true if canonical header should be unwound to ancestor during forkchoice updates.
    pub const fn unwind_canonical_header(&self) -> bool {
        self.allow_unwind_canonical_header
    }

    /// Setter for persistence threshold.
    pub const fn with_persistence_threshold(mut self, persistence_threshold: u64) -> Self {
        self.persistence_threshold = persistence_threshold;
        self
    }

    /// Setter for memory block buffer target.
    pub const fn with_memory_block_buffer_target(
        mut self,
        memory_block_buffer_target: u64,
    ) -> Self {
        self.memory_block_buffer_target = memory_block_buffer_target;
        self
    }

    /// Setter for block buffer limit.
    pub const fn with_block_buffer_limit(mut self, block_buffer_limit: u32) -> Self {
        self.block_buffer_limit = block_buffer_limit;
        self
    }

    /// Setter for maximum invalid header cache length.
    pub const fn with_max_invalid_header_cache_length(
        mut self,
        max_invalid_header_cache_length: u32,
    ) -> Self {
        self.max_invalid_header_cache_length = max_invalid_header_cache_length;
        self
    }

    /// Setter for maximum execute block batch size.
    pub const fn with_max_execute_block_batch_size(
        mut self,
        max_execute_block_batch_size: usize,
    ) -> Self {
        self.max_execute_block_batch_size = max_execute_block_batch_size;
        self
    }

    /// Setter for whether to use the legacy state root calculation method.
    pub const fn with_legacy_state_root(mut self, legacy_state_root: bool) -> Self {
        self.legacy_state_root = legacy_state_root;
        self
    }

    /// Setter for whether to disable state cache.
    pub const fn without_state_cache(mut self, disable_state_cache: bool) -> Self {
        self.disable_state_cache = disable_state_cache;
        self
    }

    /// Setter for whether to disable parallel prewarming.
    pub const fn without_prewarming(mut self, disable_prewarming: bool) -> Self {
        self.disable_prewarming = disable_prewarming;
        self
    }

    /// Setter for whether to always compare trie updates from the state root task to the trie
    /// updates from the regular state root calculation.
    pub const fn with_always_compare_trie_updates(
        mut self,
        always_compare_trie_updates: bool,
    ) -> Self {
        self.always_compare_trie_updates = always_compare_trie_updates;
        self
    }

    /// Setter for cross block cache size.
    pub const fn with_cross_block_cache_size(mut self, cross_block_cache_size: u64) -> Self {
        self.cross_block_cache_size = cross_block_cache_size;
        self
    }

    /// Setter for has enough parallelism.
    pub const fn with_has_enough_parallelism(mut self, has_enough_parallelism: bool) -> Self {
        self.has_enough_parallelism = has_enough_parallelism;
        self
    }

    /// Setter for state provider metrics.
    pub const fn with_state_provider_metrics(mut self, state_provider_metrics: bool) -> Self {
        self.state_provider_metrics = state_provider_metrics;
        self
    }

    /// Setter for whether to disable the parallel sparse trie
    pub const fn with_disable_parallel_sparse_trie(
        mut self,
        disable_parallel_sparse_trie: bool,
    ) -> Self {
        self.disable_parallel_sparse_trie = disable_parallel_sparse_trie;
        self
    }

    /// Setter for whether multiproof task should chunk proof targets.
    pub const fn with_multiproof_chunking_enabled(
        mut self,
        multiproof_chunking_enabled: bool,
    ) -> Self {
        self.multiproof_chunking_enabled = multiproof_chunking_enabled;
        self
    }

    /// Setter for multiproof task chunk size for proof targets.
    pub const fn with_multiproof_chunk_size(mut self, multiproof_chunk_size: usize) -> Self {
        self.multiproof_chunk_size = multiproof_chunk_size;
        self
    }

    /// Setter for the number of reserved CPU cores for any non-reth processes
    pub const fn with_reserved_cpu_cores(mut self, reserved_cpu_cores: usize) -> Self {
        self.reserved_cpu_cores = reserved_cpu_cores;
        self
    }

    /// Setter for whether to disable the precompile cache.
    pub const fn without_precompile_cache(mut self, precompile_cache_disabled: bool) -> Self {
        self.precompile_cache_disabled = precompile_cache_disabled;
        self
    }

    /// Setter for whether to use state root fallback, useful for testing.
    pub const fn with_state_root_fallback(mut self, state_root_fallback: bool) -> Self {
        self.state_root_fallback = state_root_fallback;
        self
    }

    /// Setter for whether to unwind canonical header to ancestor during forkchoice updates.
    pub const fn with_unwind_canonical_header(mut self, unwind_canonical_header: bool) -> Self {
        self.allow_unwind_canonical_header = unwind_canonical_header;
        self
    }

    /// Whether or not to use state root task
    pub const fn use_state_root_task(&self) -> bool {
        self.has_enough_parallelism && !self.legacy_state_root
    }

    /// Setter for prewarm max concurrency.
    pub const fn with_prewarm_max_concurrency(mut self, prewarm_max_concurrency: usize) -> Self {
        self.prewarm_max_concurrency = prewarm_max_concurrency;
        self
    }

    /// Return the prewarm max concurrency.
    pub const fn prewarm_max_concurrency(&self) -> usize {
        self.prewarm_max_concurrency
    }

    /// Return the number of storage proof worker threads.
    pub const fn storage_worker_count(&self) -> usize {
        self.storage_worker_count
    }

    /// Setter for the number of storage proof worker threads.
    pub fn with_storage_worker_count(mut self, storage_worker_count: usize) -> Self {
        self.storage_worker_count = storage_worker_count.max(MIN_WORKER_COUNT);
        self
    }

    /// Return the number of account proof worker threads.
    pub const fn account_worker_count(&self) -> usize {
        self.account_worker_count
    }

    /// Setter for the number of account proof worker threads.
    pub fn with_account_worker_count(mut self, account_worker_count: usize) -> Self {
        self.account_worker_count = account_worker_count.max(MIN_WORKER_COUNT);
        self
    }

    /// Return whether V2 storage proofs are enabled.
    pub const fn enable_proof_v2(&self) -> bool {
        self.enable_proof_v2
    }

    /// Setter for whether to enable V2 storage proofs.
    pub const fn with_enable_proof_v2(mut self, enable_proof_v2: bool) -> Self {
        self.enable_proof_v2 = enable_proof_v2;
        self
    }
}
