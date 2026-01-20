//! Ethereum 节点类型与组件/附加功能（add-ons）配置。
//!
//! 这份文件主要做两件事：
//! 1) 定义 `EthereumNode`：一个实现了 `NodeTypes` 的“节点类型标记”，用于告诉 `NodeBuilder`
//!    “我要启动一套标准的以太坊节点组件（pool/network/consensus/evm/payload…）”。
//!    注意：`EthereumNode` 本身是零字段类型，`EthereumNode::default()` 只是构造一个标记值，
//!    真正的节点组件会在 builder 启动流程中被创建并装配。
//! 2) 定义以太坊节点默认的 add-ons（尤其是 RPC/Engine API 相关的 `EthereumAddOns`），
//!    以及交易池、网络、共识、payload 校验器等 builder 的默认实现。
//!
//! 从 `bin/reth/src/main.rs` 的视角看，关键链路是：
//! `builder.node(EthereumNode::default()).launch...`
//! 这里的 `EthereumNode` 就决定了后续组件组合与 add-ons 行为。

pub use crate::{payload::EthereumPayloadBuilder, EthereumEngineValidator};
use crate::{EthEngineTypes, EthEvmConfig};
use alloy_eips::{eip7840::BlobParams, merge::EPOCH_SLOTS};
use alloy_network::Ethereum;
use alloy_rpc_types_engine::ExecutionData;
use reth_chainspec::{ChainSpec, EthChainSpec, EthereumHardforks, Hardforks};
use reth_engine_local::LocalPayloadAttributesBuilder;
use reth_engine_primitives::EngineTypes;
use reth_ethereum_consensus::EthBeaconConsensus;
use reth_ethereum_engine_primitives::{
    EthBuiltPayload, EthPayloadAttributes, EthPayloadBuilderAttributes,
};
use reth_ethereum_primitives::{EthPrimitives, TransactionSigned};
use reth_evm::{
    eth::spec::EthExecutorSpec, ConfigureEvm, EvmFactory, EvmFactoryFor, NextBlockEnvAttributes,
};
use reth_network::{primitives::BasicNetworkPrimitives, NetworkHandle, PeersInfo};
use reth_node_api::{
    AddOnsContext, FullNodeComponents, HeaderTy, NodeAddOns, NodePrimitives,
    PayloadAttributesBuilder, PrimitivesTy, TxTy,
};
use reth_node_builder::{
    components::{
        BasicPayloadServiceBuilder, ComponentsBuilder, ConsensusBuilder, ExecutorBuilder,
        NetworkBuilder, PoolBuilder, TxPoolBuilder,
    },
    node::{FullNodeTypes, NodeTypes},
    rpc::{
        BasicEngineApiBuilder, BasicEngineValidatorBuilder, EngineApiBuilder, EngineValidatorAddOn,
        EngineValidatorBuilder, EthApiBuilder, EthApiCtx, Identity, PayloadValidatorBuilder,
        RethRpcAddOns, RpcAddOns, RpcHandle,
    },
    BuilderContext, DebugNode, Node, NodeAdapter,
};
use reth_payload_primitives::PayloadTypes;
use reth_provider::{providers::ProviderFactoryBuilder, EthStorage};
use reth_rpc::{
    eth::core::{EthApiFor, EthRpcConverterFor},
    TestingApi, ValidationApi,
};
use reth_rpc_api::servers::{BlockSubmissionValidationApiServer, TestingApiServer};
use reth_rpc_builder::{config::RethRpcServerConfig, middleware::RethRpcMiddleware};
use reth_rpc_eth_api::{
    helpers::{
        config::{EthConfigApiServer, EthConfigHandler},
        pending_block::BuildPendingEnv,
    },
    RpcConvert, RpcTypes, SignableTxRequest,
};
use reth_rpc_eth_types::{error::FromEvmError, EthApiError};
use reth_rpc_server_types::RethRpcModule;
use reth_tracing::tracing::{debug, info};
use reth_transaction_pool::{
    blobstore::DiskFileBlobStore, EthTransactionPool, PoolPooledTx, PoolTransaction,
    TransactionPool, TransactionValidationTaskExecutor,
};
use revm::context::TxEnv;
use std::{marker::PhantomData, sync::Arc, time::SystemTime};

/// Type configuration for a regular Ethereum node.
#[derive(Debug, Default, Clone, Copy)]
#[non_exhaustive]
pub struct EthereumNode;

impl EthereumNode {
    /// Returns a [`ComponentsBuilder`] configured for a regular Ethereum node.
    ///
    /// 这是“标准以太坊节点”的组件组合预设：
    /// - pool：交易池（`EthereumPoolBuilder`）
    /// - executor/evm：EVM 配置与执行器（`EthereumExecutorBuilder`）
    /// - payload：payload 构建服务（`BasicPayloadServiceBuilder<EthereumPayloadBuilder>`）
    /// - network：P2P 网络（`EthereumNetworkBuilder`）
    /// - consensus：共识实现（`EthereumConsensusBuilder`，这里是 EthBeaconConsensus）
    ///
    /// 这个函数返回的是一个 builder（纯配置），并不会真正启动节点。
    pub fn components<Node>() -> ComponentsBuilder<
        Node,
        EthereumPoolBuilder,
        BasicPayloadServiceBuilder<EthereumPayloadBuilder>,
        EthereumNetworkBuilder,
        EthereumExecutorBuilder,
        EthereumConsensusBuilder,
    >
    where
        Node: FullNodeTypes<
            Types: NodeTypes<
                ChainSpec: Hardforks + EthereumHardforks + EthExecutorSpec,
                Primitives = EthPrimitives,
            >,
        >,
        <Node::Types as NodeTypes>::Payload: PayloadTypes<
            BuiltPayload = EthBuiltPayload,
            PayloadAttributes = EthPayloadAttributes,
            PayloadBuilderAttributes = EthPayloadBuilderAttributes,
        >,
    {
        ComponentsBuilder::default()
            .node_types::<Node>()
            .pool(EthereumPoolBuilder::default())
            .executor(EthereumExecutorBuilder::default())
            .payload(BasicPayloadServiceBuilder::default())
            .network(EthereumNetworkBuilder::default())
            .consensus(EthereumConsensusBuilder::default())
    }

    /// Instantiates the [`ProviderFactoryBuilder`] for an ethereum node.
    ///
    /// # Open a Providerfactory in read-only mode from a datadir
    ///
    /// See also: [`ProviderFactoryBuilder`] and
    /// [`ReadOnlyConfig`](reth_provider::providers::ReadOnlyConfig).
    ///
    /// ```no_run
    /// use reth_chainspec::MAINNET;
    /// use reth_node_ethereum::EthereumNode;
    ///
    /// let factory = EthereumNode::provider_factory_builder()
    ///     .open_read_only(MAINNET.clone(), "datadir")
    ///     .unwrap();
    /// ```
    ///
    /// # Open a Providerfactory manually with all required components
    ///
    /// ```no_run
    /// use reth_chainspec::ChainSpecBuilder;
    /// use reth_db::open_db_read_only;
    /// use reth_node_ethereum::EthereumNode;
    /// use reth_provider::providers::{RocksDBProvider, StaticFileProvider};
    /// use std::sync::Arc;
    ///
    /// let factory = EthereumNode::provider_factory_builder()
    ///     .db(Arc::new(open_db_read_only("db", Default::default()).unwrap()))
    ///     .chainspec(ChainSpecBuilder::mainnet().build().into())
    ///     .static_file(StaticFileProvider::read_only("db/static_files", false).unwrap())
    ///     .rocksdb_provider(RocksDBProvider::builder("db/rocksdb").build().unwrap())
    ///     .build_provider_factory();
    /// ```
    pub fn provider_factory_builder() -> ProviderFactoryBuilder<Self> {
        // ProviderFactoryBuilder 负责组装 provider（数据库、static files、rocksdb 等）相关组件。
        // 这里返回默认 builder，具体参数由调用方（或 NodeBuilder）继续配置。
        ProviderFactoryBuilder::default()
    }
}

impl NodeTypes for EthereumNode {
    // 这里把“以太坊节点”的类型族固定下来：
    // - Primitives：以太坊原语类型（区块/交易/收据等）
    // - ChainSpec：链配置/分叉规则
    // - Storage：以太坊存储抽象
    // - Payload：Engine API 相关 payload 类型（执行层与共识层交互所需）
    type Primitives = EthPrimitives;
    type ChainSpec = ChainSpec;
    type Storage = EthStorage;
    type Payload = EthEngineTypes;
}

/// Builds [`EthApi`](reth_rpc::EthApi) for Ethereum.
///
/// 这是一个“把 Eth RPC API 装配起来”的 builder：
/// 给定已启动节点组件（provider/pool/evm…）和 RPC 上下文，产出 `EthApi` 实例并注册到 RPC server。
#[derive(Debug)]
pub struct EthereumEthApiBuilder<NetworkT = Ethereum>(PhantomData<NetworkT>);

impl<NetworkT> Default for EthereumEthApiBuilder<NetworkT> {
    fn default() -> Self {
        Self(Default::default())
    }
}

impl<N, NetworkT> EthApiBuilder<N> for EthereumEthApiBuilder<NetworkT>
where
    N: FullNodeComponents<
        Types: NodeTypes<ChainSpec: Hardforks + EthereumHardforks>,
        Evm: ConfigureEvm<NextBlockEnvCtx: BuildPendingEnv<HeaderTy<N::Types>>>,
    >,
    NetworkT: RpcTypes<TransactionRequest: SignableTxRequest<TxTy<N::Types>>>,
    EthRpcConverterFor<N, NetworkT>: RpcConvert<
        Primitives = PrimitivesTy<N::Types>,
        Error = EthApiError,
        Network = NetworkT,
        Evm = N::Evm,
    >,
    EthApiError: FromEvmError<N::Evm>,
{
    type EthApi = EthApiFor<N, NetworkT>;

    async fn build_eth_api(self, ctx: EthApiCtx<'_, N>) -> eyre::Result<Self::EthApi> {
        // `ctx.eth_api_builder()` 会基于节点组件准备好一个 EthApi builder，这里额外把
        // converter 配上 Network 类型（用于交易请求/返回类型的网络差异）。
        Ok(ctx.eth_api_builder().map_converter(|r| r.with_network()).build())
    }
}

/// Add-ons w.r.t. l1 ethereum.
///
/// 以太坊节点的“附加功能”集合，主要围绕 RPC/Engine API：
/// - `RpcAddOns` 内部负责启动 HTTP/WS/IPC 等 RPC 服务，并装配各模块（eth, net, web3, admin…）
/// - 这里用泛型把 Eth API builder、payload 校验器、Engine API builder、middleware 等都参数化，
///   方便复用默认实现，也允许上层替换/定制。
#[derive(Debug)]
pub struct EthereumAddOns<
    N: FullNodeComponents,
    EthB: EthApiBuilder<N>,
    PVB,
    EB = BasicEngineApiBuilder<PVB>,
    EVB = BasicEngineValidatorBuilder<PVB>,
    RpcMiddleware = Identity,
> {
    inner: RpcAddOns<N, EthB, PVB, EB, EVB, RpcMiddleware>,
}

impl<N, EthB, PVB, EB, EVB, RpcMiddleware> EthereumAddOns<N, EthB, PVB, EB, EVB, RpcMiddleware>
where
    N: FullNodeComponents,
    EthB: EthApiBuilder<N>,
{
    /// Creates a new instance from the inner `RpcAddOns`.
    pub const fn new(inner: RpcAddOns<N, EthB, PVB, EB, EVB, RpcMiddleware>) -> Self {
        Self { inner }
    }
}

impl<N> Default for EthereumAddOns<N, EthereumEthApiBuilder, EthereumEngineValidatorBuilder>
where
    N: FullNodeComponents<
        Types: NodeTypes<
            ChainSpec: EthereumHardforks + Clone + 'static,
            Payload: EngineTypes<ExecutionData = ExecutionData>
                         + PayloadTypes<PayloadAttributes = EthPayloadAttributes>,
            Primitives = EthPrimitives,
        >,
    >,
    EthereumEthApiBuilder: EthApiBuilder<N>,
{
    fn default() -> Self {
        // 默认的以太坊 add-ons：使用
        // - `EthereumEthApiBuilder`：装配 eth 模块
        // - `EthereumEngineValidatorBuilder`：装配 payload 校验逻辑
        // - `BasicEngineApiBuilder` / `BasicEngineValidatorBuilder`：提供 Engine API 侧默认实现
        // - `Default::default()`：RPC middleware（默认 Identity）
        Self::new(RpcAddOns::new(
            EthereumEthApiBuilder::default(),
            EthereumEngineValidatorBuilder::default(),
            BasicEngineApiBuilder::default(),
            BasicEngineValidatorBuilder::default(),
            Default::default(),
        ))
    }
}

impl<N, EthB, PVB, EB, EVB, RpcMiddleware> EthereumAddOns<N, EthB, PVB, EB, EVB, RpcMiddleware>
where
    N: FullNodeComponents,
    EthB: EthApiBuilder<N>,
{
    /// Replace the engine API builder.
    ///
    /// 允许上层替换 Engine API 的构建器（例如自定义认证/路由/实现）。
    pub fn with_engine_api<T>(
        self,
        engine_api_builder: T,
    ) -> EthereumAddOns<N, EthB, PVB, T, EVB, RpcMiddleware>
    where
        T: Send,
    {
        let Self { inner } = self;
        EthereumAddOns::new(inner.with_engine_api(engine_api_builder))
    }

    /// Replace the payload validator builder.
    ///
    /// 允许上层替换 payload 校验器构建器（例如自定义验证规则）。
    pub fn with_payload_validator<V, T>(
        self,
        payload_validator_builder: T,
    ) -> EthereumAddOns<N, EthB, T, EB, EVB, RpcMiddleware> {
        let Self { inner } = self;
        EthereumAddOns::new(inner.with_payload_validator(payload_validator_builder))
    }

    /// Sets rpc middleware
    ///
    /// 为 RPC 服务设置 middleware（例如鉴权、限流、日志等）。
    pub fn with_rpc_middleware<T>(
        self,
        rpc_middleware: T,
    ) -> EthereumAddOns<N, EthB, PVB, EB, EVB, T>
    where
        T: Send,
    {
        let Self { inner } = self;
        EthereumAddOns::new(inner.with_rpc_middleware(rpc_middleware))
    }

    /// Sets the tokio runtime for the RPC servers.
    ///
    /// Caution: This runtime must not be created from within asynchronous context.
    pub fn with_tokio_runtime(self, tokio_runtime: Option<tokio::runtime::Handle>) -> Self {
        // 允许指定 RPC server 使用的 tokio runtime（例如把 RPC 放到单独 runtime）。
        let Self { inner } = self;
        Self { inner: inner.with_tokio_runtime(tokio_runtime) }
    }
}

impl<N, EthB, PVB, EB, EVB, RpcMiddleware> NodeAddOns<N>
    for EthereumAddOns<N, EthB, PVB, EB, EVB, RpcMiddleware>
where
    N: FullNodeComponents<
        Types: NodeTypes<
            ChainSpec: Hardforks + EthereumHardforks,
            Primitives = EthPrimitives,
            Payload: EngineTypes<ExecutionData = ExecutionData>,
        >,
        Evm: ConfigureEvm<NextBlockEnvCtx = NextBlockEnvAttributes>,
    >,
    EthB: EthApiBuilder<N>,
    PVB: Send,
    EB: EngineApiBuilder<N>,
    EVB: EngineValidatorBuilder<N>,
    EthApiError: FromEvmError<N::Evm>,
    EvmFactoryFor<N::Evm>: EvmFactory<Tx = TxEnv>,
    RpcMiddleware: RethRpcMiddleware,
{
    type Handle = RpcHandle<N, EthB::EthApi>;

    async fn launch_add_ons(
        self,
        ctx: reth_node_api::AddOnsContext<'_, N>,
    ) -> eyre::Result<Self::Handle> {
        // 这里是 add-ons 的“启动入口”：
        // - 根据节点组件与配置构造各 RPC 模块实例（ValidationApi/EthConfig/TestingApi…）
        // - 调用 inner 的 `launch_add_ons_with` 真正启动 RPC server，并把模块注册进 server
        let validation_api = ValidationApi::<_, _, <N::Types as NodeTypes>::Payload>::new(
            ctx.node.provider().clone(),
            Arc::new(ctx.node.consensus().clone()),
            ctx.node.evm_config().clone(),
            ctx.config.rpc.flashbots_config(),
            Box::new(ctx.node.task_executor().clone()),
            Arc::new(EthereumEngineValidator::new(ctx.config.chain.clone())),
        );

        let eth_config =
            EthConfigHandler::new(ctx.node.provider().clone(), ctx.node.evm_config().clone());

        let testing_skip_invalid_transactions = ctx.config.rpc.testing_skip_invalid_transactions;

        self.inner
            .launch_add_ons_with(ctx, move |container| {
                // `container` 提供模块 registry 与 module 合并器：
                // - `merge_if_module_configured` 会检查用户是否在 `--http.api/--ws.api/...` 中启用了模块，
                //   若启用则把对应 RPC server 注册进去；未启用则不会暴露该模块。
                container.modules.merge_if_module_configured(
                    RethRpcModule::Flashbots,
                    validation_api.into_rpc(),
                )?;

                container
                    .modules
                    .merge_if_module_configured(RethRpcModule::Eth, eth_config.into_rpc())?;

                // testing_buildBlockV1: only wire when the hidden testing module is explicitly
                // requested on any transport. Default stays disabled to honor security guidance.
                let mut testing_api = TestingApi::new(
                    container.registry.eth_api().clone(),
                    container.registry.evm_config().clone(),
                );
                if testing_skip_invalid_transactions {
                    testing_api = testing_api.with_skip_invalid_transactions();
                }
                container
                    .modules
                    .merge_if_module_configured(RethRpcModule::Testing, testing_api.into_rpc())?;

                Ok(())
            })
            .await
    }
}

impl<N, EthB, PVB, EB, EVB, RpcMiddleware> RethRpcAddOns<N>
    for EthereumAddOns<N, EthB, PVB, EB, EVB, RpcMiddleware>
where
    N: FullNodeComponents<
        Types: NodeTypes<
            ChainSpec: Hardforks + EthereumHardforks,
            Primitives = EthPrimitives,
            Payload: EngineTypes<ExecutionData = ExecutionData>,
        >,
        Evm: ConfigureEvm<NextBlockEnvCtx = NextBlockEnvAttributes>,
    >,
    EthB: EthApiBuilder<N>,
    PVB: PayloadValidatorBuilder<N>,
    EB: EngineApiBuilder<N>,
    EVB: EngineValidatorBuilder<N>,
    EthApiError: FromEvmError<N::Evm>,
    EvmFactoryFor<N::Evm>: EvmFactory<Tx = TxEnv>,
    RpcMiddleware: RethRpcMiddleware,
{
    // 这一段实现（省略了部分上下文）主要是把“RPC/Engine API 相关的辅助能力”绑定到节点类型上，
    // 例如：
    // - 指定 Eth RPC 的 transaction request / block 类型如何从内部类型转换为 RPC 类型
    // - 指定本地 payload attributes builder（`LocalPayloadAttributesBuilder`）如何构造
    // 这样上层在启动/测试时可以用统一接口操作（而无需关心具体类型细节）。
    type EthApi = EthB::EthApi;

    fn hooks_mut(&mut self) -> &mut reth_node_builder::rpc::RpcHooks<N, Self::EthApi> {
        self.inner.hooks_mut()
    }
}

impl<N, EthB, PVB, EB, EVB, RpcMiddleware> EngineValidatorAddOn<N>
    for EthereumAddOns<N, EthB, PVB, EB, EVB, RpcMiddleware>
where
    N: FullNodeComponents<
        Types: NodeTypes<
            ChainSpec: EthChainSpec + EthereumHardforks,
            Primitives = EthPrimitives,
            Payload: EngineTypes<ExecutionData = ExecutionData>,
        >,
        Evm: ConfigureEvm<NextBlockEnvCtx = NextBlockEnvAttributes>,
    >,
    EthB: EthApiBuilder<N>,
    PVB: Send,
    EB: EngineApiBuilder<N>,
    EVB: EngineValidatorBuilder<N>,
    EthApiError: FromEvmError<N::Evm>,
    EvmFactoryFor<N::Evm>: EvmFactory<Tx = TxEnv>,
    RpcMiddleware: Send,
{
    type ValidatorBuilder = EVB;

    fn engine_validator_builder(&self) -> Self::ValidatorBuilder {
        self.inner.engine_validator_builder()
    }
}

impl<N> Node<N> for EthereumNode
where
    N: FullNodeTypes<Types = Self>,
{
    type ComponentsBuilder = ComponentsBuilder<
        N,
        EthereumPoolBuilder,
        BasicPayloadServiceBuilder<EthereumPayloadBuilder>,
        EthereumNetworkBuilder,
        EthereumExecutorBuilder,
        EthereumConsensusBuilder,
    >;

    type AddOns =
        EthereumAddOns<NodeAdapter<N>, EthereumEthApiBuilder, EthereumEngineValidatorBuilder>;

    fn components_builder(&self) -> Self::ComponentsBuilder {
        Self::components()
    }

    fn add_ons(&self) -> Self::AddOns {
        EthereumAddOns::default()
    }
}

impl<N: FullNodeComponents<Types = Self>> DebugNode<N> for EthereumNode {
    type RpcBlock = alloy_rpc_types_eth::Block;

    fn rpc_to_primitive_block(rpc_block: Self::RpcBlock) -> reth_ethereum_primitives::Block {
        rpc_block.into_consensus().convert_transactions()
    }

    fn local_payload_attributes_builder(
        chain_spec: &Self::ChainSpec,
    ) -> impl PayloadAttributesBuilder<<Self::Payload as PayloadTypes>::PayloadAttributes> {
        // 本地 payload attributes builder：用于在本地构造新块所需的 payload attributes。
        LocalPayloadAttributesBuilder::new(Arc::new(chain_spec.clone()))
    }
}

/// A regular ethereum evm and executor builder.
///
/// 负责创建 EVM 配置（`EthEvmConfig`）。它会读取 chain spec（分叉规则等）来配置 EVM 行为。
#[derive(Debug, Default, Clone, Copy)]
#[non_exhaustive]
pub struct EthereumExecutorBuilder;

impl<Types, Node> ExecutorBuilder<Node> for EthereumExecutorBuilder
where
    Types: NodeTypes<
        ChainSpec: Hardforks + EthExecutorSpec + EthereumHardforks,
        Primitives = EthPrimitives,
    >,
    Node: FullNodeTypes<Types = Types>,
{
    type EVM = EthEvmConfig<Types::ChainSpec>;

    async fn build_evm(self, ctx: &BuilderContext<Node>) -> eyre::Result<Self::EVM> {
        // 从节点上下文拿到 chain spec，并据此构造 EVM 配置。
        Ok(EthEvmConfig::new(ctx.chain_spec()))
    }
}

/// A basic ethereum transaction pool.
///
/// This contains various settings that can be configured and take precedence over the node's
/// config.
#[derive(Debug, Default, Clone, Copy)]
#[non_exhaustive]
pub struct EthereumPoolBuilder {
    // TODO add options for txpool args
}

impl<Types, Node> PoolBuilder<Node> for EthereumPoolBuilder
where
    Types: NodeTypes<
        ChainSpec: EthereumHardforks,
        Primitives: NodePrimitives<SignedTx = TransactionSigned>,
    >,
    Node: FullNodeTypes<Types = Types>,
{
    type Pool = EthTransactionPool<Node::Provider, DiskFileBlobStore>;

    async fn build_pool(self, ctx: &BuilderContext<Node>) -> eyre::Result<Self::Pool> {
        // 交易池构建流程大致是：
        // 1) 读取 txpool 配置（以及是否禁用 blobs）
        // 2) 计算/选择 blob cache size（如果未配置则按当前时间戳推导当前分叉的 blob params）
        // 3) 创建 blob store（磁盘+缓存）
        // 4) 构造交易验证器（含 EIP-4844/KZG settings、费率上限、gas 限制等）
        // 5) 构建交易池并启动维护任务（清理/重组等）
        let pool_config = ctx.pool_config();

        let blobs_disabled = ctx.config().txpool.disable_blobs_support ||
            ctx.config().txpool.blobpool_max_count == 0;

        let blob_cache_size = if let Some(blob_cache_size) = pool_config.blob_cache_size {
            Some(blob_cache_size)
        } else {
            // get the current blob params for the current timestamp, fallback to default Cancun
            // params
            let current_timestamp =
                SystemTime::now().duration_since(SystemTime::UNIX_EPOCH)?.as_secs();
            let blob_params = ctx
                .chain_spec()
                .blob_params_at_timestamp(current_timestamp)
                .unwrap_or_else(BlobParams::cancun);

            // Derive the blob cache size from the target blob count, to auto scale it by
            // multiplying it with the slot count for 2 epochs: 384 for pectra
            Some((blob_params.target_blob_count * EPOCH_SLOTS * 2) as u32)
        };

        let blob_store =
            reth_node_builder::components::create_blob_store_with_cache(ctx, blob_cache_size)?;

        let validator = TransactionValidationTaskExecutor::eth_builder(ctx.provider().clone())
            .with_head_timestamp(ctx.head().timestamp)
            .set_eip4844(!blobs_disabled)
            .kzg_settings(ctx.kzg_settings()?)
            .with_max_tx_input_bytes(ctx.config().txpool.max_tx_input_bytes)
            .with_local_transactions_config(pool_config.local_transactions_config.clone())
            .set_tx_fee_cap(ctx.config().rpc.rpc_tx_fee_cap)
            .with_max_tx_gas_limit(ctx.config().txpool.max_tx_gas_limit)
            .with_minimum_priority_fee(ctx.config().txpool.minimum_priority_fee)
            .with_additional_tasks(ctx.config().txpool.additional_validation_tasks)
            .build_with_tasks(ctx.task_executor().clone(), blob_store.clone());

        if validator.validator().eip4844() {
            // initializing the KZG settings can be expensive, this should be done upfront so that
            // it doesn't impact the first block or the first gossiped blob transaction, so we
            // initialize this in the background
            let kzg_settings = validator.validator().kzg_settings().clone();
            ctx.task_executor().spawn_blocking(async move {
                let _ = kzg_settings.get();
                debug!(target: "reth::cli", "Initialized KZG settings");
            });
        }

        let transaction_pool = TxPoolBuilder::new(ctx)
            .with_validator(validator)
            .build_and_spawn_maintenance_task(blob_store, pool_config)?;

        info!(target: "reth::cli", "Transaction pool initialized");
        debug!(target: "reth::cli", "Spawned txpool maintenance task");

        Ok(transaction_pool)
    }
}

/// A basic ethereum payload service.
///
/// 负责启动并返回网络句柄（`NetworkHandle`）。网络构建器会：
/// - 生成网络配置（enode/key/协议等）
/// - 启动网络任务，并把交易池接入网络以便 gossip/同步
#[derive(Debug, Default, Clone, Copy)]
pub struct EthereumNetworkBuilder {
    // TODO add closure to modify network
}

impl<Node, Pool> NetworkBuilder<Node, Pool> for EthereumNetworkBuilder
where
    Node: FullNodeTypes<Types: NodeTypes<ChainSpec: Hardforks>>,
    Pool: TransactionPool<Transaction: PoolTransaction<Consensus = TxTy<Node::Types>>>
        + Unpin
        + 'static,
{
    type Network =
        NetworkHandle<BasicNetworkPrimitives<PrimitivesTy<Node::Types>, PoolPooledTx<Pool>>>;

    async fn build_network(
        self,
        ctx: &BuilderContext<Node>,
        pool: Pool,
    ) -> eyre::Result<Self::Network> {
        // ctx.network_builder() 负责根据配置创建网络实例；ctx.start_network() 负责真正启动。
        let network = ctx.network_builder().await?;
        let handle = ctx.start_network(network, pool);
        info!(target: "reth::cli", enode=%handle.local_node_record(), "P2P networking initialized");
        Ok(handle)
    }
}

/// A basic ethereum consensus builder.
///
/// 构建以太坊共识实现（执行层侧使用）：这里是 `EthBeaconConsensus`（PoS/Beacon 链相关规则）。
#[derive(Debug, Default, Clone, Copy)]
pub struct EthereumConsensusBuilder {
    // TODO add closure to modify consensus
}

impl<Node> ConsensusBuilder<Node> for EthereumConsensusBuilder
where
    Node: FullNodeTypes<
        Types: NodeTypes<ChainSpec: EthChainSpec + EthereumHardforks, Primitives = EthPrimitives>,
    >,
{
    type Consensus = Arc<EthBeaconConsensus<<Node::Types as NodeTypes>::ChainSpec>>;

    async fn build_consensus(self, ctx: &BuilderContext<Node>) -> eyre::Result<Self::Consensus> {
        // 共识对象通常是纯逻辑（基于 chain spec 的规则），用 Arc 共享给各组件。
        Ok(Arc::new(EthBeaconConsensus::new(ctx.chain_spec())))
    }
}

/// Builder for [`EthereumEngineValidator`].
///
/// payload 校验器 builder：在 Engine API 收到新 payload 时，对 payload 合法性/兼容性做验证。
#[derive(Debug, Default, Clone)]
#[non_exhaustive]
pub struct EthereumEngineValidatorBuilder;

impl<Node, Types> PayloadValidatorBuilder<Node> for EthereumEngineValidatorBuilder
where
    Types: NodeTypes<
        ChainSpec: Hardforks + EthereumHardforks + Clone + 'static,
        Payload: EngineTypes<ExecutionData = ExecutionData>
                     + PayloadTypes<PayloadAttributes = EthPayloadAttributes>,
        Primitives = EthPrimitives,
    >,
    Node: FullNodeComponents<Types = Types>,
{
    type Validator = EthereumEngineValidator<Types::ChainSpec>;

    async fn build(self, ctx: &AddOnsContext<'_, Node>) -> eyre::Result<Self::Validator> {
        // validator 依赖 chain spec（分叉规则等），这里从 ctx.config.chain 克隆出 Arc。
        Ok(EthereumEngineValidator::new(ctx.config.chain.clone()))
    }
}
