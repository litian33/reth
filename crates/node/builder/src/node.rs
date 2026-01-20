//! Node 类型与已启动节点句柄（builder 层）。
//!
//! 这个文件的目标是把“节点类型（NodeTypes/Node）”和“已启动节点（FullNode）”这两件事连接起来：
//! - `NodeTypes`：描述一组与链相关的类型（Primitives/ChainSpec/Storage/Payload）
//! - `Node<N>`：在 `NodeTypes` 的基础上，再提供“如何构建组件”和“要安装哪些 add-ons”的预设
//! - `FullNode`：节点启动后暴露给上层使用的一组句柄（pool/network/provider/rpc/engine-api 等）
//!
//! 直观理解：
//! - `EthereumNode` 这种“零字段类型”只是一个 **配置标记**，告诉 builder 应该用哪套组件组合。
//! - 真正的节点对象（网络、数据库、任务等）是在 builder 根据这些类型信息组装并启动后才产生。

use reth_db::DatabaseEnv;
// 重新导出 Node API 的核心类型，方便外部通过 `reth_node_builder::node::*` 直接引用。
pub use reth_node_api::{FullNodeTypes, NodeTypes};

use crate::{
    components::NodeComponentsBuilder, rpc::RethRpcAddOns, NodeAdapter, NodeAddOns, NodeHandle,
    RethFullAdapter,
};
use reth_node_api::{EngineTypes, FullNodeComponents, PayloadTypes};
use reth_node_core::{
    dirs::{ChainPath, DataDirPath},
    node_config::NodeConfig,
};
use reth_payload_builder::PayloadBuilderHandle;
use reth_provider::ChainSpecProvider;
use reth_rpc_api::EngineApiClient;
use reth_rpc_builder::{auth::AuthServerHandle, RpcServerHandle};
use reth_tasks::TaskExecutor;
use std::{
    fmt::Debug,
    marker::PhantomData,
    ops::{Deref, DerefMut},
    sync::Arc,
};

/// A helper type to obtain components for a given node when [`FullNodeTypes::Types`] is a [`Node`]
/// implementation.
///
/// 这是一个“类型体操”别名：给定一个 `FullNodeTypes`（通常是 `RethFullAdapter<DB, N>`），
/// 若它的 `Types` 实现了本文件的 `Node<N>` trait，则可以从 `ComponentsBuilder` 推导出最终组件类型。
/// 这让其它模块在泛型约束里能更容易写出“节点的组件集合是什么”。
pub type ComponentsFor<N> = <<<N as FullNodeTypes>::Types as Node<N>>::ComponentsBuilder as NodeComponentsBuilder<N>>::Components;

/// A [`crate::Node`] is a [`NodeTypes`] that comes with preconfigured components.
///
/// This can be used to configure the builder with a preset of components.
///
/// `Node<N>` 的关键点：
/// - 继承 `NodeTypes`：先把“类型族”确定下来（链/存储/payload 等）。
/// - 提供 `components_builder()`：告诉 builder “用哪套组件构建器来组装完整节点组件”。
/// - 提供 `add_ons()`：告诉 builder “节点启动后还要挂哪些扩展（RPC、metrics、exex…）”。
pub trait Node<N: FullNodeTypes>: NodeTypes + Clone {
    /// The type that builds the node's components.
    type ComponentsBuilder: NodeComponentsBuilder<N>;

    /// Exposes the customizable node add-on types.
    type AddOns: NodeAddOns<
        NodeAdapter<N, <Self::ComponentsBuilder as NodeComponentsBuilder<N>>::Components>,
    >;

    /// Returns a [`NodeComponentsBuilder`] for the node.
    fn components_builder(&self) -> Self::ComponentsBuilder;

    /// Returns the node add-ons.
    fn add_ons(&self) -> Self::AddOns;
}

/// A [`Node`] type builder
///
/// `AnyNode` 是一个通用的“节点类型拼装器”：
/// - `N`：FullNodeTypes（通常是某个 adapter，如 `RethFullAdapter<DB, _>`）
/// - `C`：组件构建器（实现 `NodeComponentsBuilder<N>`）
/// - `AO`：add-ons 集合（实现 `NodeAddOns<...>`）
///
/// 你可以通过链式调用 `.types(..) / .components_builder(..) / .add_ons(..)` 在类型层面把它“配齐”，
/// 最终得到一个满足 `Node<N>` 的类型，用于驱动 `NodeBuilder` 的泛型组装逻辑。
#[derive(Clone, Default, Debug)]
pub struct AnyNode<N = (), C = (), AO = ()>(PhantomData<N>, C, AO);

impl<N, C, AO> AnyNode<N, C, AO> {
    /// Configures the types of the node.
    ///
    /// 只改变类型参数 `N`（编译期），不改运行时数据。
    pub fn types<T>(self) -> AnyNode<T, C, AO> {
        AnyNode(PhantomData, self.1, self.2)
    }

    /// Sets the node components builder.
    ///
    /// 设置“组件构建器”值（运行时持有），并在类型层面替换 `C`。
    pub fn components_builder<T>(self, value: T) -> AnyNode<N, T, AO> {
        AnyNode(PhantomData, value, self.2)
    }

    /// Sets the node add-ons.
    ///
    /// 设置“add-ons”值（运行时持有），并在类型层面替换 `AO`。
    pub fn add_ons<T>(self, value: T) -> AnyNode<N, C, T> {
        AnyNode(PhantomData, self.1, value)
    }
}

impl<N, C, AO> NodeTypes for AnyNode<N, C, AO>
where
    N: FullNodeTypes,
    C: Clone + Debug + Send + Sync + Unpin + 'static,
    AO: Clone + Debug + Send + Sync + Unpin + 'static,
{
    // 这里的实现是“转发”：AnyNode 自己不定义链相关类型，而是直接沿用 `N::Types` 的类型族。
    type Primitives = <N::Types as NodeTypes>::Primitives;

    type ChainSpec = <N::Types as NodeTypes>::ChainSpec;

    type Storage = <N::Types as NodeTypes>::Storage;

    type Payload = <N::Types as NodeTypes>::Payload;
}

impl<N, C, AO> Node<N> for AnyNode<N, C, AO>
where
    N: FullNodeTypes + Clone,
    C: NodeComponentsBuilder<N> + Clone + Debug + Sync + Unpin + 'static,
    AO: NodeAddOns<NodeAdapter<N, C::Components>> + Clone + Debug + Sync + Unpin + 'static,
{
    type ComponentsBuilder = C;
    type AddOns = AO;

    fn components_builder(&self) -> Self::ComponentsBuilder {
        // builder 通常会消费/移动 components builder；这里返回 clone，避免借用问题。
        self.1.clone()
    }

    fn add_ons(&self) -> Self::AddOns {
        // 同上：add-ons 也按值返回（clone），便于 builder 在启动阶段持有。
        self.2.clone()
    }
}

/// The launched node with all components including RPC handlers.
///
/// This can be used to interact with the launched node.
///
/// `FullNode` 是“节点已启动”后的高层句柄集合：
/// - 前半部分（evm/pool/network/provider/payload_builder_handle/task_executor/config/data_dir）
///   是节点核心组件或其句柄
/// - `add_ons_handle` 则是 add-ons 安装后的句柄聚合（例如 RPC server handles、engine events 等）
#[derive(Debug)]
pub struct FullNode<Node: FullNodeComponents, AddOns: NodeAddOns<Node>> {
    /// The evm configuration.
    pub evm_config: Node::Evm,
    /// The node's transaction pool.
    pub pool: Node::Pool,
    /// Handle to the node's network.
    pub network: Node::Network,
    /// Provider to interact with the node's database
    pub provider: Node::Provider,
    /// Handle to the node's payload builder service.
    pub payload_builder_handle: PayloadBuilderHandle<<Node::Types as NodeTypes>::Payload>,
    /// Task executor for the node.
    pub task_executor: TaskExecutor,
    /// The initial node config.
    pub config: NodeConfig<<Node::Types as NodeTypes>::ChainSpec>,
    /// The data dir of the node.
    pub data_dir: ChainPath<DataDirPath>,
    /// The handle to launched add-ons
    pub add_ons_handle: AddOns::Handle,
}

impl<Node: FullNodeComponents, AddOns: NodeAddOns<Node>> Clone for FullNode<Node, AddOns> {
    fn clone(&self) -> Self {
        Self {
            evm_config: self.evm_config.clone(),
            pool: self.pool.clone(),
            network: self.network.clone(),
            provider: self.provider.clone(),
            payload_builder_handle: self.payload_builder_handle.clone(),
            task_executor: self.task_executor.clone(),
            config: self.config.clone(),
            data_dir: self.data_dir.clone(),
            add_ons_handle: self.add_ons_handle.clone(),
        }
    }
}

impl<Payload, Node, AddOns> FullNode<Node, AddOns>
where
    Payload: PayloadTypes,
    Node: FullNodeComponents<Types: NodeTypes<Payload = Payload>>,
    AddOns: NodeAddOns<Node>,
{
    /// Returns the chain spec of the node.
    ///
    /// 通过 provider 取 chain spec（通常是 Arc<ChainSpec>），方便上层判断链/分叉配置。
    pub fn chain_spec(&self) -> Arc<<Node::Types as NodeTypes>::ChainSpec> {
        self.provider.chain_spec()
    }
}

impl<Payload, Node, AddOns> FullNode<Node, AddOns>
where
    Payload: PayloadTypes,
    Node: FullNodeComponents<Types: NodeTypes<Payload = Payload>>,
    AddOns: RethRpcAddOns<Node>,
{
    /// Returns the [`RpcServerHandle`] to the started rpc server.
    ///
    /// 仅当 add-ons 包含 RPC 相关能力（`RethRpcAddOns`）时才有该接口。
    pub const fn rpc_server_handle(&self) -> &RpcServerHandle {
        &self.add_ons_handle.rpc_server_handles.rpc
    }

    /// Returns the [`AuthServerHandle`] to the started authenticated engine API server.
    ///
    /// Engine API（JWT 认证）服务的句柄。用于与共识层/engine client 交互。
    pub const fn auth_server_handle(&self) -> &AuthServerHandle {
        &self.add_ons_handle.rpc_server_handles.auth
    }
}

impl<Engine, Node, AddOns> FullNode<Node, AddOns>
where
    Engine: EngineTypes,
    Node: FullNodeComponents<Types: NodeTypes<Payload = Engine>>,
    AddOns: RethRpcAddOns<Node>,
{
    /// Returns the [`EngineApiClient`] interface for the authenticated engine API.
    ///
    /// This will send authenticated http requests to the node's auth server.
    ///
    /// 注意：这里返回的是一个“客户端适配器”，不是直接暴露底层 HTTP client。
    pub fn engine_http_client(&self) -> impl EngineApiClient<Engine> + use<Engine, Node, AddOns> {
        self.auth_server_handle().http_client()
    }

    /// Returns the [`EngineApiClient`] interface for the authenticated engine API.
    ///
    /// This will send authenticated ws requests to the node's auth server.
    pub async fn engine_ws_client(
        &self,
    ) -> impl EngineApiClient<Engine> + use<Engine, Node, AddOns> {
        self.auth_server_handle().ws_client().await
    }

    /// Returns the [`EngineApiClient`] interface for the authenticated engine API.
    ///
    /// This will send not authenticated IPC requests to the node's auth server.
    #[cfg(unix)]
    pub async fn engine_ipc_client(
        &self,
    ) -> Option<impl EngineApiClient<Engine> + use<Engine, Node, AddOns>> {
        self.auth_server_handle().ipc_client().await
    }
}

impl<Node: FullNodeComponents, AddOns: NodeAddOns<Node>> Deref for FullNode<Node, AddOns> {
    type Target = AddOns::Handle;

    fn deref(&self) -> &Self::Target {
        // 允许把 `&FullNode` 当成 `&AddOns::Handle` 用，方便调用 add-ons 暴露的各种接口。
        &self.add_ons_handle
    }
}

impl<Node: FullNodeComponents, AddOns: NodeAddOns<Node>> DerefMut for FullNode<Node, AddOns> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.add_ons_handle
    }
}

/// Helper type alias to define [`FullNode`] for a given [`Node`].
///
/// 这是最终对外最常用的 `FullNode` 形态：
/// - `RethFullAdapter<DB, N>`：把 DB 类型与 Node 类型（比如 EthereumNode）组合成 FullNodeTypes
/// - `NodeAdapter<...>`：把组件集合与 add-ons 绑定成 “可运行节点组件” 的统一接口
pub type FullNodeFor<N, DB = Arc<DatabaseEnv>> =
    FullNode<NodeAdapter<RethFullAdapter<DB, N>>, <N as Node<RethFullAdapter<DB, N>>>::AddOns>;

/// Helper type alias to define [`NodeHandle`] for a given [`Node`].
///
/// `NodeHandle` 通常包含：运行中节点句柄 + 退出 future。
pub type NodeHandleFor<N, DB = Arc<DatabaseEnv>> =
    NodeHandle<NodeAdapter<RethFullAdapter<DB, N>>, <N as Node<RethFullAdapter<DB, N>>>::AddOns>;
