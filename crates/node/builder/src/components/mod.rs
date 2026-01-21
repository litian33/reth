//! 支持配置节点组件。
//!
//! 节点的可定制组件包括：
//!  - 交易池（Transaction pool）。
//!  - 网络实现（Network implementation）。
//!  - 有效载荷构建服务（Payload builder service）。
//!
//! 组件依赖于完全类型配置的节点：[FullNodeTypes](crate::node::FullNodeTypes)。

mod builder;
mod consensus;
mod execute;
mod network;
mod payload;
mod pool;

pub use builder::*;
pub use consensus::*;
pub use execute::*;
pub use network::*;
pub use payload::*;
pub use pool::*;

use crate::{ConfigureEvm, FullNodeTypes};
use reth_consensus::FullConsensus;
use reth_network::types::NetPrimitivesFor;
use reth_network_api::FullNetwork;
use reth_node_api::{NodeTypes, PrimitivesTy, TxTy};
use reth_payload_builder::PayloadBuilderHandle;
use reth_transaction_pool::{PoolPooledTx, PoolTransaction, TransactionPool};
use std::fmt::Debug;

/// 节点组件的抽象，包含：
///  - EVM 和执行器
///  - 交易池
///  - 网络
///  - 有效载荷构建器。
pub trait NodeComponents<T: FullNodeTypes>: Clone + Debug + Unpin + Send + Sync + 'static {
    /// 节点的交易池类型。
    type Pool: TransactionPool<Transaction: PoolTransaction<Consensus = TxTy<T::Types>>> + Unpin;

    /// 节点的 EVM 配置，定义以太坊虚拟机的设置。
    type Evm: ConfigureEvm<Primitives = <T::Types as NodeTypes>::Primitives>;

    /// 节点的共识类型。
    type Consensus: FullConsensus<<T::Types as NodeTypes>::Primitives> + Clone + Unpin + 'static;

    /// 网络 API。
    type Network: FullNetwork<Primitives: NetPrimitivesFor<<T::Types as NodeTypes>::Primitives>>;

    /// 返回节点的交易池。
    fn pool(&self) -> &Self::Pool;

    /// 返回节点的 EVM 配置。
    fn evm_config(&self) -> &Self::Evm;

    /// 返回节点的共识类型。
    fn consensus(&self) -> &Self::Consensus;

    /// 返回网络句柄。
    fn network(&self) -> &Self::Network;

    /// 返回有效载荷构建服务的句柄，负责处理来自引擎的有效载荷构建请求。
    fn payload_builder_handle(&self) -> &PayloadBuilderHandle<<T::Types as NodeTypes>::Payload>;
}

/// 节点的所有组件。
///
/// 提供对节点所有组件的访问。
#[derive(Debug)]
pub struct Components<Node: FullNodeTypes, Network, Pool, EVM, Consensus> {
    /// 节点的交易池。
    pub transaction_pool: Pool,
    /// 节点的 EVM 配置，定义以太坊虚拟机的设置。
    pub evm_config: EVM,
    /// 节点的共识实现。
    pub consensus: Consensus,
    /// 节点的网络实现。
    pub network: Network,
    /// 有效载荷构建服务的句柄。
    pub payload_builder_handle: PayloadBuilderHandle<<Node::Types as NodeTypes>::Payload>,
}


impl<Node, Pool, EVM, Cons, Network> NodeComponents<Node>
    for Components<Node, Network, Pool, EVM, Cons>
where
    Node: FullNodeTypes,
    Network: FullNetwork<
        Primitives: NetPrimitivesFor<
            PrimitivesTy<Node::Types>,
            PooledTransaction = PoolPooledTx<Pool>,
        >,
    >,
    Pool: TransactionPool<Transaction: PoolTransaction<Consensus = TxTy<Node::Types>>>
        + Unpin
        + 'static,
    EVM: ConfigureEvm<Primitives = PrimitivesTy<Node::Types>> + 'static,
    Cons: FullConsensus<PrimitivesTy<Node::Types>> + Clone + Unpin + 'static,
{
    type Pool = Pool;
    type Evm = EVM;
    type Consensus = Cons;
    type Network = Network;

    fn pool(&self) -> &Self::Pool {
        &self.transaction_pool
    }

    fn evm_config(&self) -> &Self::Evm {
        &self.evm_config
    }

    fn consensus(&self) -> &Self::Consensus {
        &self.consensus
    }

    fn network(&self) -> &Self::Network {
        &self.network
    }

    fn payload_builder_handle(&self) -> &PayloadBuilderHandle<<Node::Types as NodeTypes>::Payload> {
        &self.payload_builder_handle
    }
}

impl<Node, N, Pool, EVM, Cons> Clone for Components<Node, N, Pool, EVM, Cons>
where
    N: Clone,
    Node: FullNodeTypes,
    Pool: TransactionPool,
    EVM: ConfigureEvm,
    Cons: Clone,
{
    fn clone(&self) -> Self {
        Self {
            transaction_pool: self.transaction_pool.clone(),
            evm_config: self.evm_config.clone(),
            consensus: self.consensus.clone(),
            network: self.network.clone(),
            payload_builder_handle: self.payload_builder_handle.clone(),
        }
    }
}
