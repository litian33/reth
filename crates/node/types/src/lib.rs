//! Reth 节点“类型配置”相关的独立 crate。
//!
//! 这个 crate 的核心目的是把“一个节点需要哪些类型（types）”这件事抽象出来，供 builder 层复用：
//! - `NodeTypes`：描述一个以太坊类节点的基本类型族（Primitives / ChainSpec / Storage / Payload）
//! - `NodeTypesWithDB`：在 `NodeTypes` 基础上再加上 DB 类型（内部组装时使用）
//! - `AnyNodeTypes` / `AnyNodeTypesWithEngine`：用于在类型层面“拼装”一套 NodeTypes（类似 type builder）
//!
//! 重要点：
//! - 这些 trait/类型大多是“类型级配置”，运行时几乎不携带数据（大量使用 `PhantomData`）。
//! - 它们用于让 `NodeBuilder` 能在泛型层面推导出：区块/交易/收据/链配置/engine payload 等具体类型。

#![doc(
    html_logo_url = "https://raw.githubusercontent.com/paradigmxyz/reth/main/assets/reth-docs.png",
    html_favicon_url = "https://avatars0.githubusercontent.com/u/97369466?s=256",
    issue_tracker_base_url = "https://github.com/paradigmxyz/reth/issues/"
)]
// 非测试构建时，如果某个依赖只在 Cargo.toml 里声明但代码里没用到，这里会发出告警。
#![cfg_attr(not(test), warn(unused_crate_dependencies))]
// 在 docs.rs 构建时启用 doc_cfg，用于在文档里标注 `#[cfg(...)]` 的可用性。
#![cfg_attr(docsrs, feature(doc_cfg))]
// 如果未启用 `std` feature，则在 no_std 环境下编译（便于嵌入式/更小依赖面）。
#![cfg_attr(not(feature = "std"), no_std)]

use core::{fmt::Debug, marker::PhantomData};
// 重新导出 primitives_traits 中常用的原语与 trait，让外部引用更方便。
pub use reth_primitives_traits::{
    Block, BlockBody, FullBlock, FullReceipt, FullSignedTx, NodePrimitives,
};

use reth_chainspec::EthChainSpec;
use reth_db_api::{database_metrics::DatabaseMetrics, Database};
use reth_engine_primitives::EngineTypes;
use reth_payload_primitives::{BuiltPayload, PayloadTypes};

/// The type that configures the essential types of an Ethereum-like node.
///
/// This includes the primitive types of a node and chain specification.
///
/// This trait is intended to be stateless and only define the types of the node.
///
/// `NodeTypes` 可以理解为“节点的类型清单”：
/// - `Primitives`：区块/交易/收据等基础类型（由 `NodePrimitives` 约束）
/// - `ChainSpec`：链配置（分叉规则、genesis 等），要求 header 类型与 primitives 对齐
/// - `Storage`：写入链原语到存储的类型（具体实现由 node 内部决定）
/// - `Payload`：Engine API payload 类型（执行层与共识层交互的数据模型）
pub trait NodeTypes: Clone + Debug + Send + Sync + Unpin + 'static {
    /// The node's primitive types, defining basic operations and structures.
    type Primitives: NodePrimitives;
    /// The type used for configuration of the EVM.
    type ChainSpec: EthChainSpec<Header = <Self::Primitives as NodePrimitives>::BlockHeader>;
    /// The type responsible for writing chain primitives to storage.
    type Storage: Default + Send + Sync + Unpin + Debug + 'static;
    /// The node's engine types, defining the interaction with the consensus engine.
    type Payload: PayloadTypes<BuiltPayload: BuiltPayload<Primitives = Self::Primitives>>;
}

/// A helper trait that is downstream of the [`NodeTypes`] trait and adds database to the
/// node.
///
/// Its types are configured by node internally and are not intended to be user configurable.
///
/// 这个 trait 通常不会暴露给“最终用户”去实现，它更多是 builder 在内部把“DB 类型”也纳入类型族时用。
pub trait NodeTypesWithDB: NodeTypes {
    /// Underlying database type used by the node to store and retrieve data.
    type DB: Database + DatabaseMetrics + Clone + Unpin + 'static;
}

/// An adapter type combining [`NodeTypes`] and db into [`NodeTypesWithDB`].
///
/// 适配器（adapter）的典型用法：你已经有一套 `Types: NodeTypes`，再把 `DB` 类型“拼”进去，
/// 得到一个实现了 `NodeTypesWithDB` 的新类型。
///
/// 由于它只承载类型信息（PhantomData），所以是零运行时开销。
#[derive(Clone, Debug, Default)]
pub struct NodeTypesWithDBAdapter<Types, DB> {
    types: PhantomData<Types>,
    db: PhantomData<DB>,
}

impl<Types, DB> NodeTypesWithDBAdapter<Types, DB> {
    /// Create a new adapter with the configured types.
    ///
    /// 只创建一个“类型占位符”实例，不会真正创建 Types/DB 的值。
    pub fn new() -> Self {
        Self { types: Default::default(), db: Default::default() }
    }
}

impl<Types, DB> NodeTypes for NodeTypesWithDBAdapter<Types, DB>
where
    Types: NodeTypes,
    DB: Clone + Debug + Send + Sync + Unpin + 'static,
{
    // 转发实现：沿用 `Types` 的 primitives/chainspec/storage/payload。
    type Primitives = Types::Primitives;
    type ChainSpec = Types::ChainSpec;
    type Storage = Types::Storage;
    type Payload = Types::Payload;
}

impl<Types, DB> NodeTypesWithDB for NodeTypesWithDBAdapter<Types, DB>
where
    Types: NodeTypes,
    DB: Database + DatabaseMetrics + Clone + Unpin + 'static,
{
    type DB = DB;
}

/// A [`NodeTypes`] type builder.
///
/// `AnyNodeTypes` 是一个“类型构建器”（type builder）：
/// 通过链式调用 `.primitives() / .chain_spec() / .storage() / .payload()`
/// 在类型层面组装出一套 `NodeTypes`。
///
/// 它的字段全是 `PhantomData`，所以没有运行时状态。
#[derive(Clone, Debug, Default)]
pub struct AnyNodeTypes<P = (), C = (), S = (), PL = ()>(
    PhantomData<P>,
    PhantomData<C>,
    PhantomData<S>,
    PhantomData<PL>,
);

impl<P, C, S, PL> AnyNodeTypes<P, C, S, PL> {
    /// Creates a new instance of [`AnyNodeTypes`].
    pub const fn new() -> Self {
        Self(PhantomData, PhantomData, PhantomData, PhantomData)
    }

    /// Sets the `Primitives` associated type.
    ///
    /// 注意：返回的是一个新类型（类型参数变了），运行时不会保存 `T` 的值。
    pub const fn primitives<T>(self) -> AnyNodeTypes<T, C, S, PL> {
        AnyNodeTypes::new()
    }

    /// Sets the `ChainSpec` associated type.
    pub const fn chain_spec<T>(self) -> AnyNodeTypes<P, T, S, PL> {
        AnyNodeTypes::new()
    }

    /// Sets the `Storage` associated type.
    pub const fn storage<T>(self) -> AnyNodeTypes<P, C, T, PL> {
        AnyNodeTypes::new()
    }

    /// Sets the `Payload` associated type.
    pub const fn payload<T>(self) -> AnyNodeTypes<P, C, S, T> {
        AnyNodeTypes::new()
    }
}

impl<P, C, S, PL> NodeTypes for AnyNodeTypes<P, C, S, PL>
where
    P: NodePrimitives + Send + Sync + Unpin + 'static,
    C: EthChainSpec<Header = P::BlockHeader> + Clone + 'static,
    S: Default + Clone + Send + Sync + Unpin + Debug + 'static,
    PL: PayloadTypes<BuiltPayload: BuiltPayload<Primitives = P>> + Send + Sync + Unpin + 'static,
{
    // 这里把 4 个类型参数映射回 `NodeTypes` 的 4 个关联类型。
    type Primitives = P;
    type ChainSpec = C;
    type Storage = S;
    type Payload = PL;
}

/// A [`NodeTypes`] type builder.
///
/// 和 `AnyNodeTypes` 类似，但额外带一个 `Engine` 类型参数 `E`。
/// 这在一些需要显式区分/约束 EngineTypes 的场景下更方便。
#[derive(Clone, Debug, Default)]
pub struct AnyNodeTypesWithEngine<P = (), E = (), C = (), S = (), PL = ()> {
    /// Embedding the basic node types.
    _base: AnyNodeTypes<P, C, S, PL>,
    /// Phantom data for the engine.
    _engine: PhantomData<E>,
}

impl<P, E, C, S, PL> AnyNodeTypesWithEngine<P, E, C, S, PL> {
    /// Creates a new instance of [`AnyNodeTypesWithEngine`].
    pub const fn new() -> Self {
        Self { _base: AnyNodeTypes::new(), _engine: PhantomData }
    }

    /// Sets the `Primitives` associated type.
    pub const fn primitives<T>(self) -> AnyNodeTypesWithEngine<T, E, C, S, PL> {
        AnyNodeTypesWithEngine::new()
    }

    /// Sets the `Engine` associated type.
    ///
    /// 这里的 `Engine` 是一个单独的类型参数，主要用于额外的 trait bound（见下方 impl）。
    pub const fn engine<T>(self) -> AnyNodeTypesWithEngine<P, T, C, S, PL> {
        AnyNodeTypesWithEngine::new()
    }

    /// Sets the `ChainSpec` associated type.
    pub const fn chain_spec<T>(self) -> AnyNodeTypesWithEngine<P, E, T, S, PL> {
        AnyNodeTypesWithEngine::new()
    }

    /// Sets the `Storage` associated type.
    pub const fn storage<T>(self) -> AnyNodeTypesWithEngine<P, E, C, T, PL> {
        AnyNodeTypesWithEngine::new()
    }

    /// Sets the `Payload` associated type.
    pub const fn payload<T>(self) -> AnyNodeTypesWithEngine<P, E, C, S, T> {
        AnyNodeTypesWithEngine::new()
    }
}

impl<P, E, C, S, PL> NodeTypes for AnyNodeTypesWithEngine<P, E, C, S, PL>
where
    P: NodePrimitives + Send + Sync + Unpin + 'static,
    E: EngineTypes + Send + Sync + Unpin,
    C: EthChainSpec<Header = P::BlockHeader> + Clone + 'static,
    S: Default + Clone + Send + Sync + Unpin + Debug + 'static,
    PL: PayloadTypes<BuiltPayload: BuiltPayload<Primitives = P>> + Send + Sync + Unpin + 'static,
{
    // 同样把类型参数映射回 `NodeTypes`；这里虽然额外约束了 `E: EngineTypes`，
    // 但 `NodeTypes::Payload` 仍然由 `PL` 决定（保持通用性）。
    type Primitives = P;
    type ChainSpec = C;
    type Storage = S;
    type Payload = PL;
}

/// Helper adapter type for accessing [`NodePrimitives::Block`] on [`NodeTypes`].
///
/// 下面这些 `*Ty` 都是“类型快捷方式”：从 `NodeTypes` 一路投影到更具体的 primitives 类型，
/// 让其它模块的泛型签名更短、更可读。
pub type BlockTy<N> = <PrimitivesTy<N> as NodePrimitives>::Block;

/// Helper adapter type for accessing [`NodePrimitives::BlockHeader`] on [`NodeTypes`].
pub type HeaderTy<N> = <PrimitivesTy<N> as NodePrimitives>::BlockHeader;

/// Helper adapter type for accessing [`NodePrimitives::BlockBody`] on [`NodeTypes`].
pub type BodyTy<N> = <PrimitivesTy<N> as NodePrimitives>::BlockBody;

/// Helper adapter type for accessing [`NodePrimitives::SignedTx`] on [`NodeTypes`].
pub type TxTy<N> = <PrimitivesTy<N> as NodePrimitives>::SignedTx;

/// Helper adapter type for accessing [`NodePrimitives::Receipt`] on [`NodeTypes`].
pub type ReceiptTy<N> = <PrimitivesTy<N> as NodePrimitives>::Receipt;

/// Helper type for getting the `Primitives` associated type from a [`NodeTypes`].
pub type PrimitivesTy<N> = <N as NodeTypes>::Primitives;

/// Helper adapter type for accessing [`PayloadTypes::PayloadAttributes`] on [`NodeTypes`].
pub type PayloadAttrTy<N> = <<N as NodeTypes>::Payload as PayloadTypes>::PayloadAttributes;
