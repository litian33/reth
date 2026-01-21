//! 配置文件相关定义。
use reth_network_types::{PeersConfig, SessionsConfig};
use reth_prune_types::PruneModes;
use reth_stages_types::ExecutionStageThresholds;
use reth_static_file_types::{StaticFileMap, StaticFileSegment};
use std::{
    path::{Path, PathBuf},
    time::Duration,
};
use url::Url;

#[cfg(feature = "serde")]
const EXTENSION: &str = "toml";

/// 默认的裁剪（prune）区块间隔。
pub const DEFAULT_BLOCK_INTERVAL: usize = 5;

/// reth 节点配置。
#[derive(Debug, Clone, Default, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde", serde(default))]
pub struct Config {
    /// 流水线（pipeline）中各个阶段（stage）的配置。
    pub stages: StageConfig,
    /// 裁剪（prune）相关配置。
    #[cfg_attr(feature = "serde", serde(default))]
    pub prune: PruneConfig,
    /// 节点发现（discovery）服务的配置。
    pub peers: PeersConfig,
    /// 对等节点（peer）会话配置。
    pub sessions: SessionsConfig,
    /// 静态文件（static files）配置。
    #[cfg_attr(feature = "serde", serde(default))]
    pub static_files: StaticFilesConfig,
}

impl Config {
    /// 设置裁剪（prune）配置。
    pub fn set_prune_config(&mut self, prune_config: PruneConfig) {
        self.prune = prune_config;
    }
}

#[cfg(feature = "serde")]
impl Config {
    /// 从指定路径加载 [`Config`]。
    ///
    /// 如果配置文件不存在，会创建一个包含默认值的新配置文件。
    pub fn from_path(path: impl AsRef<Path>) -> eyre::Result<Self> {
        let path = path.as_ref();
        match std::fs::read_to_string(path) {
            Ok(cfg_string) => {
                toml::from_str(&cfg_string).map_err(|e| eyre::eyre!("Failed to parse TOML: {e}"))
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                if let Some(parent) = path.parent() {
                    std::fs::create_dir_all(parent)
                        .map_err(|e| eyre::eyre!("Failed to create directory: {e}"))?;
                }
                let cfg = Self::default();
                let s = toml::to_string_pretty(&cfg)
                    .map_err(|e| eyre::eyre!("Failed to serialize to TOML: {e}"))?;
                std::fs::write(path, s)
                    .map_err(|e| eyre::eyre!("Failed to write configuration file: {e}"))?;
                Ok(cfg)
            }
            Err(e) => Err(eyre::eyre!("Failed to load configuration: {e}")),
        }
    }

    /// 返回节点的 [`PeersConfig`]。
    ///
    /// 如果提供了 peers 文件，则会将文件里的 basic nodes 合并到配置中。
    pub fn peers_config_with_basic_nodes_from_file(
        &self,
        peers_file: Option<&Path>,
    ) -> PeersConfig {
        self.peers
            .clone()
            .with_basic_nodes_from_file(peers_file)
            .unwrap_or_else(|_| self.peers.clone())
    }

    /// 将配置保存为 toml 文件。
    pub fn save(&self, path: &Path) -> Result<(), std::io::Error> {
        if path.extension() != Some(std::ffi::OsStr::new(EXTENSION)) {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!("reth config file extension must be '{EXTENSION}'"),
            ));
        }

        std::fs::write(
            path,
            toml::to_string(self)
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string()))?,
        )
    }
}

/// 流水线（pipeline）中各个阶段（stage）的配置。
#[derive(Debug, Clone, Default, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde", serde(default))]
pub struct StageConfig {
    /// ERA 阶段配置。
    pub era: EraConfig,
    /// Header 阶段配置。
    pub headers: HeadersConfig,
    /// Body 阶段配置。
    pub bodies: BodiesConfig,
    /// Sender Recovery 阶段配置。
    pub sender_recovery: SenderRecoveryConfig,
    /// Execution 阶段配置。
    pub execution: ExecutionConfig,
    /// Prune 阶段配置。
    pub prune: PruneStageConfig,
    /// Account Hashing 阶段配置。
    pub account_hashing: HashingConfig,
    /// Storage Hashing 阶段配置。
    pub storage_hashing: HashingConfig,
    /// Merkle 阶段配置。
    pub merkle: MerkleConfig,
    /// Transaction Lookup 阶段配置。
    pub transaction_lookup: TransactionLookupConfig,
    /// Index Account History 阶段配置。
    pub index_account_history: IndexHistoryConfig,
    /// Index Storage History 阶段配置。
    pub index_storage_history: IndexHistoryConfig,
    /// 通用 ETL 相关配置。
    pub etl: EtlConfig,
}

impl StageConfig {
    /// 在 `MerkleStage`、`AccountHashingStage`、`StorageHashingStage` 三者之间切换“增量计算/全量计算”
    /// 时使用的最高阈值（按区块数计）。
    ///
    /// 该值用于在后续 pipeline 运行的 `ExecutionStage` 中判断是否可以裁剪（prune）changesets。
    pub fn execution_external_clean_threshold(&self) -> u64 {
        self.merkle
            .incremental_threshold
            .max(self.account_hashing.clean_threshold)
            .max(self.storage_hashing.clean_threshold)
    }
}

/// ERA stage 配置。
#[derive(Debug, Clone, Default, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde", serde(default))]
pub struct EraConfig {
    /// 本地 ERA1 文件所在目录路径。
    ///
    /// 与 `url` 冲突（不可同时设置）。
    pub path: Option<PathBuf>,
    /// 用于下载 ERA1 文件的主机基础 URL。
    ///
    /// 与 `path` 冲突（不可同时设置）。
    pub url: Option<Url>,
    /// 从 `url` 下载的文件在被处理前的临时保存目录。
    ///
    /// 当设置了 `url` 时必须提供。
    pub folder: Option<PathBuf>,
}

impl EraConfig {
    /// 将临时下载目录 `folder` 设置为 `dir` 下名为 "era" 的子目录。
    pub fn with_datadir(mut self, dir: impl AsRef<Path>) -> Self {
        self.folder = Some(dir.as_ref().join("era"));
        self
    }
}

/// Header stage 配置。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde", serde(default))]
pub struct HeadersConfig {
    /// 并发发送请求的最大数量。
    ///
    /// 默认：100
    pub downloader_max_concurrent_requests: usize,
    /// 并发发送请求的最小数量。
    ///
    /// 默认：5
    pub downloader_min_concurrent_requests: usize,
    /// 内部最多缓存的响应数量（每个响应可能包含多个 headers）。
    pub downloader_max_buffered_responses: usize,
    /// 单次向某个 peer 请求的最大 header 数量。
    pub downloader_request_limit: u64,
    /// 在将进度提交到数据库前，最多下载的 header 数量。
    pub commit_threshold: u64,
}

impl Default for HeadersConfig {
    fn default() -> Self {
        Self {
            commit_threshold: 10_000,
            downloader_request_limit: 1_000,
            downloader_max_concurrent_requests: 100,
            downloader_min_concurrent_requests: 5,
            downloader_max_buffered_responses: 100,
        }
    }
}

/// Body stage 配置。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde", serde(default))]
pub struct BodiesConfig {
    /// 每次请求中“非空区块”的批量大小。
    ///
    /// 默认：200
    pub downloader_request_limit: u64,
    /// 流式返回时一次返回的区块体（block body）最大数量。
    ///
    /// 默认：`1_000`
    pub downloader_stream_batch_size: usize,
    /// 内部区块缓冲区大小（字节）。
    ///
    /// 默认：2GB
    pub downloader_max_buffered_blocks_size_bytes: usize,
    /// 并发发送请求的最小数量。
    ///
    /// 默认：5
    pub downloader_min_concurrent_requests: usize,
    /// 并发发送请求的最大数量（等于最大 peer 数）。
    ///
    /// 默认：100
    pub downloader_max_concurrent_requests: usize,
}

impl Default for BodiesConfig {
    fn default() -> Self {
        Self {
            downloader_request_limit: 200,
            downloader_stream_batch_size: 1_000,
            downloader_max_buffered_blocks_size_bytes: 2 * 1024 * 1024 * 1024, // 约 2GB
            downloader_min_concurrent_requests: 5,
            downloader_max_concurrent_requests: 100,
        }
    }
}

/// Sender recovery stage 配置。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde", serde(default))]
pub struct SenderRecoveryConfig {
    /// 在将进度提交到数据库前，最多处理的交易数量。
    pub commit_threshold: u64,
}

impl Default for SenderRecoveryConfig {
    fn default() -> Self {
        Self { commit_threshold: 5_000_000 }
    }
}

/// Execution stage 配置。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde", serde(default))]
pub struct ExecutionConfig {
    /// 在提交前最多处理的区块数量。
    pub max_blocks: Option<u64>,
    /// 在提交前最多保留在内存中的状态变更数量。
    pub max_changes: Option<u64>,
    /// 在提交前最多处理的累计 gas 总量。
    pub max_cumulative_gas: Option<u64>,
    /// 在提交前最多用于处理区块的时间。
    #[cfg_attr(
        feature = "serde",
        serde(
            serialize_with = "humantime_serde::serialize",
            deserialize_with = "deserialize_duration"
        )
    )]
    pub max_duration: Option<Duration>,
}

impl Default for ExecutionConfig {
    fn default() -> Self {
        Self {
            max_blocks: Some(500_000),
            max_changes: Some(5_000_000),
            // 5 万个 3000 万 gas 的完整区块
            max_cumulative_gas: Some(30_000_000 * 50_000),
            // 10 分钟
            max_duration: Some(Duration::from_secs(10 * 60)),
        }
    }
}

impl From<ExecutionConfig> for ExecutionStageThresholds {
    fn from(config: ExecutionConfig) -> Self {
        Self {
            max_blocks: config.max_blocks,
            max_changes: config.max_changes,
            max_cumulative_gas: config.max_cumulative_gas,
            max_duration: config.max_duration,
        }
    }
}

/// Prune stage 配置。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde", serde(default))]
pub struct PruneStageConfig {
    /// 在将进度提交到数据库前，最多裁剪的条目数量。
    pub commit_threshold: usize,
}

impl Default for PruneStageConfig {
    fn default() -> Self {
        Self { commit_threshold: 1_000_000 }
    }
}

/// Hashing stage 配置。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde", serde(default))]
pub struct HashingConfig {
    /// 在“增量 hashing / 全量 hashing”之间切换的阈值（按区块数计）。
    pub clean_threshold: u64,
    /// 在将进度提交到数据库前，最多处理的实体数量。
    pub commit_threshold: u64,
}

impl Default for HashingConfig {
    fn default() -> Self {
        Self { clean_threshold: 500_000, commit_threshold: 100_000 }
    }
}

/// Merkle stage 配置。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde", serde(default))]
pub struct MerkleConfig {
    /// 当 merkle stage 需要追赶大量区块时，连续使用“增量 root”方法的区块数量。
    ///
    /// 当追赶的区块很多时，“增量 root”只能在有限的区块范围内使用，否则可能导致节点 OOM。
    /// 该值决定了我们会连续运行多少个区块的增量 root 方法。
    pub incremental_threshold: u64,
    /// 从“基于变更的增量 trie 构建”切换到“全量重建”的阈值（按区块数计）。
    pub rebuild_threshold: u64,
}

impl Default for MerkleConfig {
    fn default() -> Self {
        Self { incremental_threshold: 7_000, rebuild_threshold: 100_000 }
    }
}

/// Transaction Lookup stage 配置。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde", serde(default))]
pub struct TransactionLookupConfig {
    /// 写入磁盘前最多处理的交易数量。
    pub chunk_size: u64,
}

impl Default for TransactionLookupConfig {
    fn default() -> Self {
        Self { chunk_size: 5_000_000 }
    }
}

/// 通用 ETL 相关配置。
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde", serde(default))]
pub struct EtlConfig {
    /// 创建临时文件的数据目录。
    pub dir: Option<PathBuf>,
    /// 在刷写到磁盘文件之前，允许驻留在内存中的数据最大值（字节）。
    pub file_size: usize,
}

impl Default for EtlConfig {
    fn default() -> Self {
        Self { dir: None, file_size: Self::default_file_size() }
    }
}

impl EtlConfig {
    /// 创建一个 ETL 配置。
    pub const fn new(dir: Option<PathBuf>, file_size: usize) -> Self {
        Self { dir, file_size }
    }

    /// 根据 datadir 路径返回默认的 ETL 目录。
    pub fn from_datadir(path: &Path) -> PathBuf {
        path.join("etl-tmp")
    }

    /// 在刷写到磁盘文件之前，默认允许驻留在内存中的数据大小（字节）。
    pub const fn default_file_size() -> usize {
        // 500 MB
        500 * (1024 * 1024)
    }
}

/// 静态文件（static files）配置。
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde", serde(default))]
pub struct StaticFilesConfig {
    /// 每个 segment 的“每个文件包含的区块数”配置。
    pub blocks_per_file: BlocksPerFileConfig,
}

/// 各个 segment 的“每个文件包含的区块数”配置。
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde", serde(default))]
pub struct BlocksPerFileConfig {
    /// headers segment 的每文件区块数。
    pub headers: Option<u64>,
    /// transactions segment 的每文件区块数。
    pub transactions: Option<u64>,
    /// receipts segment 的每文件区块数。
    pub receipts: Option<u64>,
    /// transaction senders segment 的每文件区块数。
    pub transaction_senders: Option<u64>,
    /// account changesets segment 的每文件区块数。
    pub account_change_sets: Option<u64>,
}

impl StaticFilesConfig {
    /// 校验静态文件配置。
    ///
    /// 如果任意“每文件区块数”为 0，则返回错误。
    pub fn validate(&self) -> eyre::Result<()> {
        let BlocksPerFileConfig {
            headers,
            transactions,
            receipts,
            transaction_senders,
            account_change_sets,
        } = self.blocks_per_file;
        eyre::ensure!(headers != Some(0), "Headers segment blocks per file must be greater than 0");
        eyre::ensure!(
            transactions != Some(0),
            "Transactions segment blocks per file must be greater than 0"
        );
        eyre::ensure!(
            receipts != Some(0),
            "Receipts segment blocks per file must be greater than 0"
        );
        eyre::ensure!(
            transaction_senders != Some(0),
            "Transaction senders segment blocks per file must be greater than 0"
        );
        eyre::ensure!(
            account_change_sets != Some(0),
            "Account changesets segment blocks per file must be greater than 0"
        );
        Ok(())
    }

    /// 将 blocks-per-file 配置转换为 [`StaticFileMap`]。
    pub fn as_blocks_per_file_map(&self) -> StaticFileMap<u64> {
        let BlocksPerFileConfig {
            headers,
            transactions,
            receipts,
            transaction_senders,
            account_change_sets,
        } = self.blocks_per_file;

        let mut map = StaticFileMap::default();
        // 遍历所有可能的 segment，保证这里的 match 是穷尽的，避免未来新增 segment 时忘记配置。
        for segment in StaticFileSegment::iter() {
            let blocks_per_file = match segment {
                StaticFileSegment::Headers => headers,
                StaticFileSegment::Transactions => transactions,
                StaticFileSegment::Receipts => receipts,
                StaticFileSegment::TransactionSenders => transaction_senders,
                StaticFileSegment::AccountChangeSets => account_change_sets,
            };

            if let Some(blocks_per_file) = blocks_per_file {
                map.insert(segment, blocks_per_file);
            }
        }
        map
    }
}

/// History stage 配置。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde", serde(default))]
pub struct IndexHistoryConfig {
    /// 在将进度提交到数据库前，最多处理的区块数量。
    pub commit_threshold: u64,
}

impl Default for IndexHistoryConfig {
    fn default() -> Self {
        Self { commit_threshold: 100_000 }
    }
}

/// 裁剪（prune）配置。
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde", serde(default))]
pub struct PruneConfig {
    /// 最小裁剪间隔（按区块数计）。
    pub block_interval: usize,
    /// 可裁剪数据各个部分的裁剪策略配置。
    #[cfg_attr(feature = "serde", serde(alias = "parts"))]
    pub segments: PruneModes,
}

impl Default for PruneConfig {
    fn default() -> Self {
        Self { block_interval: DEFAULT_BLOCK_INTERVAL, segments: PruneModes::default() }
    }
}

impl PruneConfig {
    /// 判断当前配置是否为默认配置。
    pub fn is_default(&self) -> bool {
        self == &Self::default()
    }

    /// 判断是否存在任何 receipts 裁剪相关配置。
    pub fn has_receipts_pruning(&self) -> bool {
        self.segments.has_receipts_pruning()
    }

    /// 将 `other` 的值合并到 `self` 中。
    /// - `Option<PruneMode>` 字段：仅当 `self` 为 `None` 时，才从 `other` 赋值。
    /// - `block_interval`：仅当 `self.block_interval == DEFAULT_BLOCK_INTERVAL` 时，才从 `other` 赋值。
    /// - `receipts_log_filter`：仅当 `self` 为空且 `other` 非空时，才从 `other` 赋值。
    pub fn merge(&mut self, other: Self) {
        let Self {
            block_interval,
            segments:
                PruneModes {
                    sender_recovery,
                    transaction_lookup,
                    receipts,
                    account_history,
                    storage_history,
                    bodies_history,
                    receipts_log_filter,
                },
        } = other;

        // 合并 block_interval：仅当当前仍为默认值时才更新
        if self.block_interval == DEFAULT_BLOCK_INTERVAL {
            self.block_interval = block_interval;
        }

        // 合并各个 segment 的裁剪模式（prune mode）
        self.segments.sender_recovery = self.segments.sender_recovery.or(sender_recovery);
        self.segments.transaction_lookup = self.segments.transaction_lookup.or(transaction_lookup);
        self.segments.receipts = self.segments.receipts.or(receipts);
        self.segments.account_history = self.segments.account_history.or(account_history);
        self.segments.storage_history = self.segments.storage_history.or(storage_history);
        self.segments.bodies_history = self.segments.bodies_history.or(bodies_history);

        if self.segments.receipts_log_filter.0.is_empty() && !receipts_log_filter.0.is_empty() {
            self.segments.receipts_log_filter = receipts_log_filter;
        }
    }
}

/// 用于兼容旧版本 `Duration` 反序列化格式的辅助类型。
#[cfg(feature = "serde")]
fn deserialize_duration<'de, D>(deserializer: D) -> Result<Option<Duration>, D::Error>
where
    D: serde::de::Deserializer<'de>,
{
    #[derive(serde::Deserialize)]
    #[serde(untagged)]
    enum AnyDuration {
        #[serde(deserialize_with = "humantime_serde::deserialize")]
        Human(Option<Duration>),
        Duration(Option<Duration>),
    }

    <AnyDuration as serde::Deserialize>::deserialize(deserializer).map(|d| match d {
        AnyDuration::Human(duration) | AnyDuration::Duration(duration) => duration,
    })
}

#[cfg(all(test, feature = "serde"))]
mod tests {
    use super::{Config, EXTENSION};
    use crate::PruneConfig;
    use alloy_primitives::Address;
    use reth_network_peers::TrustedPeer;
    use reth_prune_types::{PruneMode, PruneModes, ReceiptsLogPruneConfig};
    use std::{collections::BTreeMap, path::Path, str::FromStr, time::Duration};

    fn with_tempdir(filename: &str, proc: fn(&std::path::Path)) {
        let temp_dir = tempfile::tempdir().unwrap();
        let config_path = temp_dir.path().join(filename).with_extension(EXTENSION);

        proc(&config_path);

        temp_dir.close().unwrap()
    }

    /// 使用临时的配置文件路径作为夹具（fixture）来运行测试函数。
    fn with_config_path(test_fn: fn(&Path)) {
        // 为配置文件创建临时目录
        let config_dir = tempfile::tempdir().expect("creating test fixture failed");
        // 生成配置文件路径
        let config_path =
            config_dir.path().join("example-app").join("example-config").with_extension("toml");
        // 以该配置路径运行测试函数
        test_fn(&config_path);
        config_dir.close().expect("removing test fixture failed");
    }

    #[test]
    fn test_load_path_works() {
        with_config_path(|path| {
            let config = Config::from_path(path).expect("load_path failed");
            assert_eq!(config, Config::default());
        })
    }

    #[test]
    fn test_load_path_reads_existing_config() {
        with_config_path(|path| {
            let config = Config::default();

            // 如果父目录不存在则创建
            if let Some(parent) = path.parent() {
                std::fs::create_dir_all(parent).expect("Failed to create directories");
            }

            // 将配置写入文件
            std::fs::write(path, toml::to_string(&config).unwrap())
                .expect("Failed to write config");

            // 从文件加载配置并对比
            let loaded = Config::from_path(path).expect("load_path failed");
            assert_eq!(config, loaded);
        })
    }

    #[test]
    fn test_load_path_fails_on_invalid_toml() {
        with_config_path(|path| {
            let invalid_toml = "invalid toml data";

            // 如果父目录不存在则创建
            if let Some(parent) = path.parent() {
                std::fs::create_dir_all(parent).expect("Failed to create directories");
            }

            // 写入非法 TOML 数据
            std::fs::write(path, invalid_toml).expect("Failed to write invalid TOML");

            // 尝试加载配置应当失败
            let result = Config::from_path(path);
            assert!(result.is_err());
        })
    }

    #[test]
    fn test_load_path_creates_directory_if_not_exists() {
        with_config_path(|path| {
            // 确保目录不存在
            let parent = path.parent().unwrap();
            assert!(!parent.exists());

            // 加载配置：应创建目录并写入默认配置文件
            let config = Config::from_path(path).expect("load_path failed");
            assert_eq!(config, Config::default());

            // 此时目录与文件应当已存在
            assert!(parent.exists());
            assert!(path.exists());
        });
    }

    #[test]
    fn test_store_config() {
        with_tempdir("config-store-test", |config_path| {
            let config = Config::default();
            std::fs::write(
                config_path,
                toml::to_string(&config).expect("Failed to serialize config"),
            )
            .expect("Failed to write config file");
        })
    }

    #[test]
    fn test_store_config_method() {
        with_tempdir("config-store-test-method", |config_path| {
            let config = Config::default();
            config.save(config_path).expect("Failed to store config");
        })
    }

    #[test]
    fn test_load_config() {
        with_tempdir("config-load-test", |config_path| {
            let config = Config::default();

            // 将配置写入文件
            std::fs::write(
                config_path,
                toml::to_string(&config).expect("Failed to serialize config"),
            )
            .expect("Failed to write config file");

            // 从文件加载配置
            let loaded_config = Config::from_path(config_path).unwrap();

            // 对比加载后的配置与原始配置
            assert_eq!(config, loaded_config);
        })
    }

    #[test]
    fn test_load_execution_stage() {
        with_tempdir("config-load-test", |config_path| {
            let mut config = Config::default();
            config.stages.execution.max_duration = Some(Duration::from_secs(10 * 60));

            // 将配置写入文件
            std::fs::write(
                config_path,
                toml::to_string(&config).expect("Failed to serialize config"),
            )
            .expect("Failed to write config file");

            // 从文件加载配置
            let loaded_config = Config::from_path(config_path).unwrap();

            // 对比加载后的配置与原始配置
            assert_eq!(config, loaded_config);
        })
    }

    // 确保配置反序列化对旧版本保持兼容
    #[test]
    fn test_backwards_compatibility() {
        let alpha_0_0_8 = r"#
[stages.headers]
downloader_max_concurrent_requests = 100
downloader_min_concurrent_requests = 5
downloader_max_buffered_responses = 100
downloader_request_limit = 1000
commit_threshold = 10000

[stages.bodies]
downloader_request_limit = 200
downloader_stream_batch_size = 1000
downloader_max_buffered_blocks_size_bytes = 2147483648
downloader_min_concurrent_requests = 5
downloader_max_concurrent_requests = 100

[stages.sender_recovery]
commit_threshold = 5000000

[stages.execution]
max_blocks = 500000
max_changes = 5000000

[stages.account_hashing]
clean_threshold = 500000
commit_threshold = 100000

[stages.storage_hashing]
clean_threshold = 500000
commit_threshold = 100000

[stages.merkle]
clean_threshold = 50000

[stages.transaction_lookup]
chunk_size = 5000000

[stages.index_account_history]
commit_threshold = 100000

[stages.index_storage_history]
commit_threshold = 100000

[peers]
refill_slots_interval = '1s'
trusted_nodes = []
connect_trusted_nodes_only = false
max_backoff_count = 5
ban_duration = '12h'

[peers.connection_info]
max_outbound = 100
max_inbound = 30

[peers.reputation_weights]
bad_message = -16384
bad_block = -16384
bad_transactions = -16384
already_seen_transactions = 0
timeout = -4096
bad_protocol = -2147483648
failed_to_connect = -25600
dropped = -4096

[peers.backoff_durations]
low = '30s'
medium = '3m'
high = '15m'
max = '1h'

[sessions]
session_command_buffer = 32
session_event_buffer = 260

[sessions.limits]

[sessions.initial_internal_request_timeout]
secs = 20
nanos = 0

[sessions.protocol_breach_request_timeout]
secs = 120
nanos = 0

[prune]
block_interval = 5

[prune.parts]
sender_recovery = { distance = 16384 }
transaction_lookup = 'full'
receipts = { before = 1920000 }
account_history = { distance = 16384 }
storage_history = { distance = 16384 }
[prune.parts.receipts_log_filter]
'0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48' = { before = 17000000 }
'0xdac17f958d2ee523a2206206994597c13d831ec7' = { distance = 1000 }
#";
        let _conf: Config = toml::from_str(alpha_0_0_8).unwrap();

        let alpha_0_0_11 = r"#
[prune.segments]
sender_recovery = { distance = 16384 }
transaction_lookup = 'full'
receipts = { before = 1920000 }
account_history = { distance = 16384 }
storage_history = { distance = 16384 }
[prune.segments.receipts_log_filter]
'0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48' = { before = 17000000 }
'0xdac17f958d2ee523a2206206994597c13d831ec7' = { distance = 1000 }
#";
        let _conf: Config = toml::from_str(alpha_0_0_11).unwrap();

        let alpha_0_0_18 = r"#
[stages.headers]
downloader_max_concurrent_requests = 100
downloader_min_concurrent_requests = 5
downloader_max_buffered_responses = 100
downloader_request_limit = 1000
commit_threshold = 10000

[stages.total_difficulty]
commit_threshold = 100000

[stages.bodies]
downloader_request_limit = 200
downloader_stream_batch_size = 1000
downloader_max_buffered_blocks_size_bytes = 2147483648
downloader_min_concurrent_requests = 5
downloader_max_concurrent_requests = 100

[stages.sender_recovery]
commit_threshold = 5000000

[stages.execution]
max_blocks = 500000
max_changes = 5000000
max_cumulative_gas = 1500000000000
[stages.execution.max_duration]
secs = 600
nanos = 0

[stages.account_hashing]
clean_threshold = 500000
commit_threshold = 100000

[stages.storage_hashing]
clean_threshold = 500000
commit_threshold = 100000

[stages.merkle]
clean_threshold = 50000

[stages.transaction_lookup]
commit_threshold = 5000000

[stages.index_account_history]
commit_threshold = 100000

[stages.index_storage_history]
commit_threshold = 100000

[peers]
refill_slots_interval = '5s'
trusted_nodes = []
connect_trusted_nodes_only = false
max_backoff_count = 5
ban_duration = '12h'

[peers.connection_info]
max_outbound = 100
max_inbound = 30
max_concurrent_outbound_dials = 10

[peers.reputation_weights]
bad_message = -16384
bad_block = -16384
bad_transactions = -16384
already_seen_transactions = 0
timeout = -4096
bad_protocol = -2147483648
failed_to_connect = -25600
dropped = -4096
bad_announcement = -1024

[peers.backoff_durations]
low = '30s'
medium = '3m'
high = '15m'
max = '1h'

[sessions]
session_command_buffer = 32
session_event_buffer = 260

[sessions.limits]

[sessions.initial_internal_request_timeout]
secs = 20
nanos = 0

[sessions.protocol_breach_request_timeout]
secs = 120
nanos = 0
#";
        let conf: Config = toml::from_str(alpha_0_0_18).unwrap();
        assert_eq!(conf.stages.execution.max_duration, Some(Duration::from_secs(10 * 60)));

        let alpha_0_0_19 = r"#
[stages.headers]
downloader_max_concurrent_requests = 100
downloader_min_concurrent_requests = 5
downloader_max_buffered_responses = 100
downloader_request_limit = 1000
commit_threshold = 10000

[stages.total_difficulty]
commit_threshold = 100000

[stages.bodies]
downloader_request_limit = 200
downloader_stream_batch_size = 1000
downloader_max_buffered_blocks_size_bytes = 2147483648
downloader_min_concurrent_requests = 5
downloader_max_concurrent_requests = 100

[stages.sender_recovery]
commit_threshold = 5000000

[stages.execution]
max_blocks = 500000
max_changes = 5000000
max_cumulative_gas = 1500000000000
max_duration = '10m'

[stages.account_hashing]
clean_threshold = 500000
commit_threshold = 100000

[stages.storage_hashing]
clean_threshold = 500000
commit_threshold = 100000

[stages.merkle]
clean_threshold = 50000

[stages.transaction_lookup]
commit_threshold = 5000000

[stages.index_account_history]
commit_threshold = 100000

[stages.index_storage_history]
commit_threshold = 100000

[peers]
refill_slots_interval = '5s'
trusted_nodes = []
connect_trusted_nodes_only = false
max_backoff_count = 5
ban_duration = '12h'

[peers.connection_info]
max_outbound = 100
max_inbound = 30
max_concurrent_outbound_dials = 10

[peers.reputation_weights]
bad_message = -16384
bad_block = -16384
bad_transactions = -16384
already_seen_transactions = 0
timeout = -4096
bad_protocol = -2147483648
failed_to_connect = -25600
dropped = -4096
bad_announcement = -1024

[peers.backoff_durations]
low = '30s'
medium = '3m'
high = '15m'
max = '1h'

[sessions]
session_command_buffer = 32
session_event_buffer = 260

[sessions.limits]

[sessions.initial_internal_request_timeout]
secs = 20
nanos = 0

[sessions.protocol_breach_request_timeout]
secs = 120
nanos = 0
#";
        let _conf: Config = toml::from_str(alpha_0_0_19).unwrap();
    }

    // 确保 prune 配置反序列化对旧版本保持兼容
    #[test]
    fn test_backwards_compatibility_prune_full() {
        let s = r"#
[prune]
block_interval = 5

[prune.segments]
sender_recovery = { distance = 16384 }
transaction_lookup = 'full'
receipts = { distance = 16384 }
#";
        let _conf: Config = toml::from_str(s).unwrap();
    }

    #[test]
    fn test_prune_config_merge() {
        let mut config1 = PruneConfig {
            block_interval: 5,
            segments: PruneModes {
                sender_recovery: Some(PruneMode::Full),
                transaction_lookup: None,
                receipts: Some(PruneMode::Distance(1000)),
                account_history: None,
                storage_history: Some(PruneMode::Before(5000)),
                bodies_history: None,
                receipts_log_filter: ReceiptsLogPruneConfig(BTreeMap::from([(
                    Address::random(),
                    PruneMode::Full,
                )])),
            },
        };

        let config2 = PruneConfig {
            block_interval: 10,
            segments: PruneModes {
                sender_recovery: Some(PruneMode::Distance(500)),
                transaction_lookup: Some(PruneMode::Full),
                receipts: Some(PruneMode::Full),
                account_history: Some(PruneMode::Distance(2000)),
                storage_history: Some(PruneMode::Distance(3000)),
                bodies_history: None,
                receipts_log_filter: ReceiptsLogPruneConfig(BTreeMap::from([
                    (Address::random(), PruneMode::Distance(1000)),
                    (Address::random(), PruneMode::Before(2000)),
                ])),
            },
        };

        let original_filter = config1.segments.receipts_log_filter.clone();
        config1.merge(config2);

        // 检查配置已合并：config1 中已存在的配置不应被 config2 覆盖
        assert_eq!(config1.block_interval, 10);
        assert_eq!(config1.segments.sender_recovery, Some(PruneMode::Full));
        assert_eq!(config1.segments.transaction_lookup, Some(PruneMode::Full));
        assert_eq!(config1.segments.receipts, Some(PruneMode::Distance(1000)));
        assert_eq!(config1.segments.account_history, Some(PruneMode::Distance(2000)));
        assert_eq!(config1.segments.storage_history, Some(PruneMode::Before(5000)));
        assert_eq!(config1.segments.receipts_log_filter, original_filter);
    }

    #[test]
    fn test_conf_trust_nodes_only() {
        let trusted_nodes_only = r"#
[peers]
trusted_nodes_only = true
#";
        let conf: Config = toml::from_str(trusted_nodes_only).unwrap();
        assert!(conf.peers.trusted_nodes_only);

        let trusted_nodes_only = r"#
[peers]
connect_trusted_nodes_only = true
#";
        let conf: Config = toml::from_str(trusted_nodes_only).unwrap();
        assert!(conf.peers.trusted_nodes_only);
    }

    #[test]
    fn test_can_support_dns_in_trusted_nodes() {
        let reth_toml = r#"
    [peers]
    trusted_nodes = [
        "enode://0401e494dbd0c84c5c0f72adac5985d2f2525e08b68d448958aae218f5ac8198a80d1498e0ebec2ce38b1b18d6750f6e61a56b4614c5a6c6cf0981c39aed47dc@34.159.32.127:30303",
        "enode://e9675164b5e17b9d9edf0cc2bd79e6b6f487200c74d1331c220abb5b8ee80c2eefbf18213989585e9d0960683e819542e11d4eefb5f2b4019e1e49f9fd8fff18@berav2-bootnode.staketab.org:30303"
    ]
    "#;

        let conf: Config = toml::from_str(reth_toml).unwrap();
        assert_eq!(conf.peers.trusted_nodes.len(), 2);

        let expected_enodes = vec![
            "enode://0401e494dbd0c84c5c0f72adac5985d2f2525e08b68d448958aae218f5ac8198a80d1498e0ebec2ce38b1b18d6750f6e61a56b4614c5a6c6cf0981c39aed47dc@34.159.32.127:30303",
            "enode://e9675164b5e17b9d9edf0cc2bd79e6b6f487200c74d1331c220abb5b8ee80c2eefbf18213989585e9d0960683e819542e11d4eefb5f2b4019e1e49f9fd8fff18@berav2-bootnode.staketab.org:30303",
        ];

        for enode in expected_enodes {
            let node = TrustedPeer::from_str(enode).unwrap();
            assert!(conf.peers.trusted_nodes.contains(&node));
        }
    }
}
