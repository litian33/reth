//! 内置的 [`StageSet`] 集合。
//!
//! 最简单的用法是 [`DefaultStages`]：它提供运行一个 reth 实例所需的全部 stages。
//!
//! 如果运行环境中已经具备所需数据，也可以只单独运行 reth 的某些部分，例如 [`ExecutionStages`] 或
//! [`HashingStages`]。
//!
//! # 示例
//!
//! ```no_run
//! # use reth_stages::Pipeline;
//! # use reth_stages::sets::{OfflineStages};
//! # use reth_chainspec::MAINNET;
//! # use reth_prune_types::PruneModes;
//! # use reth_evm_ethereum::EthEvmConfig;
//! # use reth_evm::ConfigureEvm;
//! # use reth_provider::StaticFileProviderFactory;
//! # use reth_provider::test_utils::{create_test_provider_factory, MockNodeTypesWithDB};
//! # use reth_static_file::StaticFileProducer;
//! # use reth_config::config::StageConfig;
//! # use reth_ethereum_primitives::EthPrimitives;
//! # use std::sync::Arc;
//! # use reth_consensus::FullConsensus;
//!
//! # fn create(exec: impl ConfigureEvm<Primitives = EthPrimitives> + 'static, consensus: impl FullConsensus<EthPrimitives> + 'static) {
//!
//! let provider_factory = create_test_provider_factory();
//! let static_file_producer =
//!     StaticFileProducer::new(provider_factory.clone(), PruneModes::default());
//! // 构建一个仅包含离线 stages 的 pipeline。
//! let pipeline = Pipeline::<MockNodeTypesWithDB>::builder()
//!     .add_stages(OfflineStages::new(exec, Arc::new(consensus), StageConfig::default(), PruneModes::default()))
//!     .build(provider_factory, static_file_producer);
//!
//! # }
//! ```
use crate::{
    stages::{
        AccountHashingStage, BodyStage, EraImportSource, EraStage, ExecutionStage, FinishStage,
        HeaderStage, IndexAccountHistoryStage, IndexStorageHistoryStage, MerkleStage,
        PruneSenderRecoveryStage, PruneStage, SenderRecoveryStage, StorageHashingStage,
        TransactionLookupStage,
    },
    StageSet, StageSetBuilder,
};
use alloy_primitives::B256;
use reth_config::config::StageConfig;
use reth_consensus::FullConsensus;
use reth_evm::ConfigureEvm;
use reth_network_p2p::{bodies::downloader::BodyDownloader, headers::downloader::HeaderDownloader};
use reth_primitives_traits::{Block, NodePrimitives};
use reth_provider::HeaderSyncGapProvider;
use reth_prune_types::PruneModes;
use reth_stages_api::Stage;
use std::sync::Arc;
use tokio::sync::watch;

/// 运行一个“完整同步（full sync）”reth 实例所需的 stages 集合。
///
/// 按顺序组合如下集合：
///
/// - [`OnlineStages`]
/// - [`OfflineStages`]
/// - [`FinishStage`]
///
/// 展开后对应如下 stage 序列：
/// - [`EraStage`]（可选，用于 ERA1 导入）
/// - [`HeaderStage`]
/// - [`BodyStage`]
/// - [`SenderRecoveryStage`]
/// - [`ExecutionStage`]
/// - [`PruneSenderRecoveryStage`]（execute）
/// - [`MerkleStage`]（unwind）
/// - [`AccountHashingStage`]
/// - [`StorageHashingStage`]
/// - [`MerkleStage`]（execute）
/// - [`TransactionLookupStage`]
/// - [`IndexStorageHistoryStage`]
/// - [`IndexAccountHistoryStage`]
/// - [`PruneStage`]（execute）
/// - [`FinishStage`]
#[derive(Debug)]
pub struct DefaultStages<Provider, H, B, E>
where
    H: HeaderDownloader,
    B: BodyDownloader,
    E: ConfigureEvm,
{
    /// 在线 stages 的配置
    online: OnlineStages<Provider, H, B>,
    /// execution stage 所需的 EVM 配置/执行器工厂
    evm_config: E,
    /// 共识实现实例
    consensus: Arc<dyn FullConsensus<E::Primitives>>,
    /// pipeline 中各个 stage 的配置
    stages_config: StageConfig,
    /// 各个可裁剪 segment 的裁剪配置
    prune_modes: PruneModes,
}

impl<Provider, H, B, E> DefaultStages<Provider, H, B, E>
where
    H: HeaderDownloader,
    B: BodyDownloader,
    E: ConfigureEvm<Primitives: NodePrimitives<BlockHeader = H::Header, Block = B::Block>>,
{
    /// 使用给定参数创建默认 stages 集合。
    #[expect(clippy::too_many_arguments)]
    pub fn new(
        provider: Provider,
        tip: watch::Receiver<B256>,
        consensus: Arc<dyn FullConsensus<E::Primitives>>,
        header_downloader: H,
        body_downloader: B,
        evm_config: E,
        stages_config: StageConfig,
        prune_modes: PruneModes,
        era_import_source: Option<EraImportSource>,
    ) -> Self {
        Self {
            online: OnlineStages::new(
                provider,
                tip,
                header_downloader,
                body_downloader,
                stages_config.clone(),
                era_import_source,
            ),
            evm_config,
            consensus,
            stages_config,
            prune_modes,
        }
    }
}

impl<P, H, B, E> DefaultStages<P, H, B, E>
where
    E: ConfigureEvm,
    H: HeaderDownloader,
    B: BodyDownloader,
{
    /// 在给定 builder 后追加默认的离线 stages 与默认的 finish stage。
    pub fn add_offline_stages<Provider>(
        default_offline: StageSetBuilder<Provider>,
        evm_config: E,
        consensus: Arc<dyn FullConsensus<E::Primitives>>,
        stages_config: StageConfig,
        prune_modes: PruneModes,
    ) -> StageSetBuilder<Provider>
    where
        OfflineStages<E>: StageSet<Provider>,
    {
        StageSetBuilder::default()
            .add_set(default_offline)
            .add_set(OfflineStages::new(evm_config, consensus, stages_config, prune_modes))
            .add_stage(FinishStage)
    }
}

impl<P, H, B, E, Provider> StageSet<Provider> for DefaultStages<P, H, B, E>
where
    P: HeaderSyncGapProvider + 'static,
    H: HeaderDownloader + 'static,
    B: BodyDownloader + 'static,
    E: ConfigureEvm,
    OnlineStages<P, H, B>: StageSet<Provider>,
    OfflineStages<E>: StageSet<Provider>,
{
    fn builder(self) -> StageSetBuilder<Provider> {
        Self::add_offline_stages(
            self.online.builder(),
            self.evm_config,
            self.consensus,
            self.stages_config.clone(),
            self.prune_modes,
        )
    }
}

/// 默认需要网络访问的 stages 集合。
///
/// 如果指定的 downloader 本身支持离线模式，这些 stages 也可以在无网络的情况下运行。
#[derive(Debug)]
pub struct OnlineStages<Provider, H, B>
where
    H: HeaderDownloader,
    B: BodyDownloader,
{
    /// Header 阶段用于处理同步缺口（sync gap）的 provider。
    provider: Provider,
    /// Header 阶段的 tip（链头目标）。
    tip: watch::Receiver<B256>,

    /// 区块 header 下载器
    header_downloader: H,
    /// 区块 body 下载器
    body_downloader: B,
    /// pipeline 中各个 stage 的配置
    stages_config: StageConfig,
    /// ERA1 文件来源（可选）。未指定时，`EraStage` 不执行任何操作。
    era_import_source: Option<EraImportSource>,
}

impl<Provider, H, B> OnlineStages<Provider, H, B>
where
    H: HeaderDownloader,
    B: BodyDownloader,
{
    /// 创建在线 stages 集合。
    pub const fn new(
        provider: Provider,
        tip: watch::Receiver<B256>,
        header_downloader: H,
        body_downloader: B,
        stages_config: StageConfig,
        era_import_source: Option<EraImportSource>,
    ) -> Self {
        Self { provider, tip, header_downloader, body_downloader, stages_config, era_import_source }
    }
}

impl<P, H, B> OnlineStages<P, H, B>
where
    P: HeaderSyncGapProvider + 'static,
    H: HeaderDownloader<Header = <B::Block as Block>::Header> + 'static,
    B: BodyDownloader + 'static,
{
    /// 使用给定的 headers stage 创建一个新的 builder。
    pub fn builder_with_headers<Provider>(
        headers: HeaderStage<P, H>,
        body_downloader: B,
    ) -> StageSetBuilder<Provider>
    where
        HeaderStage<P, H>: Stage<Provider>,
        BodyStage<B>: Stage<Provider>,
    {
        StageSetBuilder::default().add_stage(headers).add_stage(BodyStage::new(body_downloader))
    }

    /// 使用给定的 bodies stage 创建一个新的 builder。
    pub fn builder_with_bodies<Provider>(
        bodies: BodyStage<B>,
        provider: P,
        tip: watch::Receiver<B256>,
        header_downloader: H,
        stages_config: StageConfig,
    ) -> StageSetBuilder<Provider>
    where
        BodyStage<B>: Stage<Provider>,
        HeaderStage<P, H>: Stage<Provider>,
    {
        StageSetBuilder::default()
            .add_stage(HeaderStage::new(provider, header_downloader, tip, stages_config.etl))
            .add_stage(bodies)
    }
}

impl<Provider, P, H, B> StageSet<Provider> for OnlineStages<P, H, B>
where
    P: HeaderSyncGapProvider + 'static,
    H: HeaderDownloader<Header = <B::Block as Block>::Header> + 'static,
    B: BodyDownloader + 'static,
    HeaderStage<P, H>: Stage<Provider>,
    BodyStage<B>: Stage<Provider>,
    EraStage<<B::Block as Block>::Header, <B::Block as Block>::Body, EraImportSource>:
        Stage<Provider>,
{
    fn builder(self) -> StageSetBuilder<Provider> {
        let mut builder = StageSetBuilder::default();

        if self.era_import_source.is_some() {
            builder = builder
                .add_stage(EraStage::new(self.era_import_source, self.stages_config.etl.clone()));
        }

        builder
            .add_stage(HeaderStage::new(
                self.provider,
                self.header_downloader,
                self.tip,
                self.stages_config.etl.clone(),
            ))
            .add_stage(BodyStage::new(self.body_downloader))
    }
}

/// 不需要网络访问的 stages 集合。
///
/// 按顺序组合如下集合：
///
/// - [`ExecutionStages`]
/// - [`PruneSenderRecoveryStage`]
/// - [`HashingStages`]
/// - [`HistoryIndexingStages`]
/// - [`PruneStage`]
#[derive(Debug)]
#[non_exhaustive]
pub struct OfflineStages<E: ConfigureEvm> {
    /// execution stage 所需的 EVM 配置/执行器工厂
    evm_config: E,
    /// 用于校验区块的共识实现实例。
    consensus: Arc<dyn FullConsensus<E::Primitives>>,
    /// pipeline 中各个 stage 的配置
    stages_config: StageConfig,
    /// 各个可裁剪 segment 的裁剪配置
    prune_modes: PruneModes,
}

impl<E: ConfigureEvm> OfflineStages<E> {
    /// 创建离线 stages 集合。
    pub const fn new(
        evm_config: E,
        consensus: Arc<dyn FullConsensus<E::Primitives>>,
        stages_config: StageConfig,
        prune_modes: PruneModes,
    ) -> Self {
        Self { evm_config, consensus, stages_config, prune_modes }
    }
}

impl<E, Provider> StageSet<Provider> for OfflineStages<E>
where
    E: ConfigureEvm,
    ExecutionStages<E>: StageSet<Provider>,
    PruneSenderRecoveryStage: Stage<Provider>,
    HashingStages: StageSet<Provider>,
    HistoryIndexingStages: StageSet<Provider>,
    PruneStage: Stage<Provider>,
{
    fn builder(self) -> StageSetBuilder<Provider> {
        ExecutionStages::new(self.evm_config, self.consensus, self.stages_config.clone())
            .builder()
            // 若设置了 sender recovery 的裁剪模式，则加入 sender recovery 裁剪 stage。
            .add_stage_opt(self.prune_modes.sender_recovery.map(|prune_mode| {
                PruneSenderRecoveryStage::new(prune_mode, self.stages_config.prune.commit_threshold)
            }))
            .add_set(HashingStages { stages_config: self.stages_config.clone() })
            .add_set(HistoryIndexingStages {
                stages_config: self.stages_config.clone(),
                prune_modes: self.prune_modes.clone(),
            })
            // Prune stage 应当放在所有 hashing stages 之后，否则可能会提前删除后续 stage 需要的数据。
            .add_stage(PruneStage::new(
                self.prune_modes.clone(),
                self.stages_config.prune.commit_threshold,
            ))
    }
}

/// 执行（execute）已有区块数据所需的 stages 集合。
#[derive(Debug)]
#[non_exhaustive]
pub struct ExecutionStages<E: ConfigureEvm> {
    /// 用于创建执行器的 EVM 配置/执行器工厂。
    evm_config: E,
    /// 用于校验区块的共识实现实例。
    consensus: Arc<dyn FullConsensus<E::Primitives>>,
    /// pipeline 中各个 stage 的配置
    stages_config: StageConfig,
}

impl<E: ConfigureEvm> ExecutionStages<E> {
    /// 创建 execution stages 集合。
    pub const fn new(
        executor_provider: E,
        consensus: Arc<dyn FullConsensus<E::Primitives>>,
        stages_config: StageConfig,
    ) -> Self {
        Self { evm_config: executor_provider, consensus, stages_config }
    }
}

impl<E, Provider> StageSet<Provider> for ExecutionStages<E>
where
    E: ConfigureEvm + 'static,
    SenderRecoveryStage: Stage<Provider>,
    ExecutionStage<E>: Stage<Provider>,
{
    fn builder(self) -> StageSetBuilder<Provider> {
        StageSetBuilder::default()
            .add_stage(SenderRecoveryStage::new(self.stages_config.sender_recovery))
            .add_stage(ExecutionStage::from_config(
                self.evm_config,
                self.consensus,
                self.stages_config.execution,
                self.stages_config.execution_external_clean_threshold(),
            ))
    }
}

/// 对账户/存储状态做 hashing 所需的 stages 集合。
///
/// 包含：
/// - [`MerkleStage`]（unwind）
/// - [`AccountHashingStage`]
/// - [`StorageHashingStage`]
/// - [`MerkleStage`]（execute）
#[derive(Debug, Default)]
#[non_exhaustive]
pub struct HashingStages {
    /// pipeline 中各个 stage 的配置
    stages_config: StageConfig,
}

impl<Provider> StageSet<Provider> for HashingStages
where
    MerkleStage: Stage<Provider>,
    AccountHashingStage: Stage<Provider>,
    StorageHashingStage: Stage<Provider>,
{
    fn builder(self) -> StageSetBuilder<Provider> {
        StageSetBuilder::default()
            .add_stage(MerkleStage::default_unwind())
            .add_stage(AccountHashingStage::new(
                self.stages_config.account_hashing,
                self.stages_config.etl.clone(),
            ))
            .add_stage(StorageHashingStage::new(
                self.stages_config.storage_hashing,
                self.stages_config.etl.clone(),
            ))
            .add_stage(MerkleStage::new_execution(
                self.stages_config.merkle.rebuild_threshold,
                self.stages_config.merkle.incremental_threshold,
            ))
    }
}

/// 为历史状态做额外索引构建的 stages 集合。
#[derive(Debug, Default)]
#[non_exhaustive]
pub struct HistoryIndexingStages {
    /// pipeline 中各个 stage 的配置
    stages_config: StageConfig,
    /// 各个可裁剪 segment 的裁剪配置
    prune_modes: PruneModes,
}

impl<Provider> StageSet<Provider> for HistoryIndexingStages
where
    TransactionLookupStage: Stage<Provider>,
    IndexStorageHistoryStage: Stage<Provider>,
    IndexAccountHistoryStage: Stage<Provider>,
{
    fn builder(self) -> StageSetBuilder<Provider> {
        StageSetBuilder::default()
            .add_stage(TransactionLookupStage::new(
                self.stages_config.transaction_lookup,
                self.stages_config.etl.clone(),
                self.prune_modes.transaction_lookup,
            ))
            .add_stage(IndexStorageHistoryStage::new(
                self.stages_config.index_storage_history,
                self.stages_config.etl.clone(),
                self.prune_modes.storage_history,
            ))
            .add_stage(IndexAccountHistoryStage::new(
                self.stages_config.index_account_history,
                self.stages_config.etl.clone(),
                self.prune_modes.account_history,
            ))
    }
}
