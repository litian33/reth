//! 实现以太坊有线协议版本 66 到 70。
//! 定义了消息、请求-响应对以及广播的结构体和枚举。
//! 处理与 [`EthVersion`] 的兼容性。
//!
//! 示例包括创建、编码和解码协议消息。
//!
//! 参考: [以太坊有线协议](https://github.com/ethereum/devp2p/blob/master/caps/eth.md)。

use super::{
    broadcast::NewBlockHashes, BlockBodies, BlockHeaders, GetBlockBodies, GetBlockHeaders,
    GetNodeData, GetPooledTransactions, GetReceipts, GetReceipts70, NewPooledTransactionHashes66,
    NewPooledTransactionHashes68, NodeData, PooledTransactions, Receipts, Status, StatusEth69,
    Transactions,
};
use crate::{
    status::StatusMessage, BlockRangeUpdate, EthNetworkPrimitives, EthVersion, NetworkPrimitives,
    RawCapabilityMessage, Receipts69, Receipts70, SharedTransactions,
};
use alloc::{boxed::Box, string::String, sync::Arc};
use alloy_primitives::{
    bytes::{Buf, BufMut},
    Bytes,
};
use alloy_rlp::{length_of_length, Decodable, Encodable, Header};
use core::fmt::Debug;

/// [`MAX_MESSAGE_SIZE`] 是协议消息大小的最大上限（10MB）。
// 参考 Geth 实现，防止大消息攻击。
// https://github.com/ethereum/go-ethereum/blob/30602163d5d8321fbc68afdcbbaf2362b2641bde/eth/protocols/eth/protocol.go#L50
pub const MAX_MESSAGE_SIZE: usize = 10 * 1024 * 1024;

/// 发送/接收消息时可能出现的错误
#[derive(thiserror::Error, Debug)]
pub enum MessageError {
    /// 标记给定协议版本不支持的消息 ID。
    #[error("message id {1:?} is invalid for version {0:?}")]
    Invalid(EthVersion, EthMessageID),
    /// RLP 解码消息失败。
    #[error("RLP error: {0}")]
    RlpError(#[from] alloy_rlp::Error),
    /// 带有自定义消息的其他错误。
    #[error("{0}")]
    Other(String),
}

/// 一个 `eth` 协议消息，包含消息 ID 和负载（Payload）。
/// 这是在线缆上实际传输的结构。
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct ProtocolMessage<N: NetworkPrimitives = EthNetworkPrimitives> {
    /// 唯一标识符，代表以太坊消息的类型。
    pub message_type: EthMessageID,
    /// 消息的具体内容，包括基于消息类型的特定数据。
    #[cfg_attr(
        feature = "serde",
        serde(bound = "EthMessage<N>: serde::Serialize + serde::de::DeserializeOwned")
    )]
    pub message: EthMessage<N>,
}

impl<N: NetworkPrimitives> ProtocolMessage<N> {
    /// 根据消息类型和消息 RLP 字节创建一个新的 `ProtocolMessage`。
    ///
    /// **核心逻辑**：此函数根据连接的 [`EthVersion`] 强制执行特定的解码规则，
    /// 因为不同版本的以太坊协议对同一 ID 的消息可能有不同的结构定义。
    pub fn decode_message(version: EthVersion, buf: &mut &[u8]) -> Result<Self, MessageError> {
        let message_type = EthMessageID::decode(buf)?;

        // 对于 EIP-7642:
        // 合并前（遗留）的状态消息包含总难度，而 eth/69 则省略了它。
        let message = match message_type {
            // 握手状态消息：eth/69 之后删除了总难度 (total difficulty)
            EthMessageID::Status => EthMessage::Status(if version < EthVersion::Eth69 {
                StatusMessage::Legacy(Status::decode(buf)?)
            } else {
                StatusMessage::Eth69(StatusEth69::decode(buf)?)
            }),
            EthMessageID::NewBlockHashes => {
                EthMessage::NewBlockHashes(NewBlockHashes::decode(buf)?)
            }
            EthMessageID::NewBlock => {
                EthMessage::NewBlock(Box::new(N::NewBlockPayload::decode(buf)?))
            }
            EthMessageID::Transactions => EthMessage::Transactions(Transactions::decode(buf)?),
            // 交易池哈希广播：eth/68 引入了类型和大小信息
            EthMessageID::NewPooledTransactionHashes => {
                if version >= EthVersion::Eth68 {
                    EthMessage::NewPooledTransactionHashes68(NewPooledTransactionHashes68::decode(
                        buf,
                    )?)
                } else {
                    EthMessage::NewPooledTransactionHashes66(NewPooledTransactionHashes66::decode(
                        buf,
                    )?)
                }
            }
            // 以下是请求-响应对消息，由 RequestPair 封装（包含 request_id）
            EthMessageID::GetBlockHeaders => EthMessage::GetBlockHeaders(RequestPair::decode(buf)?),
            EthMessageID::BlockHeaders => EthMessage::BlockHeaders(RequestPair::decode(buf)?),
            EthMessageID::GetBlockBodies => EthMessage::GetBlockBodies(RequestPair::decode(buf)?),
            EthMessageID::BlockBodies => EthMessage::BlockBodies(RequestPair::decode(buf)?),
            EthMessageID::GetPooledTransactions => {
                EthMessage::GetPooledTransactions(RequestPair::decode(buf)?)
            }
            EthMessageID::PooledTransactions => {
                EthMessage::PooledTransactions(RequestPair::decode(buf)?)
            }
            // eth/67 删除了 GetNodeData 和 NodeData 消息
            EthMessageID::GetNodeData => {
                if version >= EthVersion::Eth67 {
                    return Err(MessageError::Invalid(version, EthMessageID::GetNodeData))
                }
                EthMessage::GetNodeData(RequestPair::decode(buf)?)
            }
            EthMessageID::NodeData => {
                if version >= EthVersion::Eth67 {
                    return Err(MessageError::Invalid(version, EthMessageID::GetNodeData))
                }
                EthMessage::NodeData(RequestPair::decode(buf)?)
            }
            // 收据请求：eth/70 改变了 GetReceipts 的编码方式
            EthMessageID::GetReceipts => {
                if version >= EthVersion::Eth70 {
                    EthMessage::GetReceipts70(RequestPair::decode(buf)?)
                } else {
                    EthMessage::GetReceipts(RequestPair::decode(buf)?)
                }
            }
            // 收据响应：
            // eth/69 删除了布隆过滤器 (Bloom Filter)
            // eth/70 (EIP-7975) 引入了部分收据和 lastBlockIncomplete 标志
            EthMessageID::Receipts => {
                match version {
                    v if v >= EthVersion::Eth70 => {
                        // eth/70 继续省略布隆过滤器，并添加 `lastBlockIncomplete` 标志，
                        // 编码为 `[request-id, lastBlockIncomplete, [[receipt₁, receipt₂], ...]]`。
                        EthMessage::Receipts70(RequestPair::decode(buf)?)
                    }
                    EthVersion::Eth69 => {
                        // 在 eth69 中，收据不再包含布隆过滤器
                        EthMessage::Receipts69(RequestPair::decode(buf)?)
                    }
                    _ => {
                        // 在 eth69 之前，我们也需要解码布隆过滤器
                        EthMessage::Receipts(RequestPair::decode(buf)?)
                    }
                }
            }
            // eth/69 引入的消息，用于告知节点服务的历史区块范围
            EthMessageID::BlockRangeUpdate => {
                if version < EthVersion::Eth69 {
                    return Err(MessageError::Invalid(version, EthMessageID::BlockRangeUpdate))
                }
                EthMessage::BlockRangeUpdate(BlockRangeUpdate::decode(buf)?)
            }
            // 处理未知或自定义扩展消息
            EthMessageID::Other(_) => {
                let raw_payload = Bytes::copy_from_slice(buf);
                buf.advance(raw_payload.len());
                EthMessage::Other(RawCapabilityMessage::new(
                    message_type.to_u8() as usize,
                    raw_payload.into(),
                ))
            }
        };
        Ok(Self { message_type, message })
    }
}

impl<N: NetworkPrimitives> Encodable for ProtocolMessage<N> {
    /// 将协议消息编码为字节。消息类型被编码为单个字节并放在消息前面。
    fn encode(&self, out: &mut dyn BufMut) {
        self.message_type.encode(out);
        self.message.encode(out);
    }
    fn length(&self) -> usize {
        self.message_type.length() + self.message.length()
    }
}

impl<N: NetworkPrimitives> From<EthMessage<N>> for ProtocolMessage<N> {
    fn from(message: EthMessage<N>) -> Self {
        Self { message_type: message.message_id(), message }
    }
}

/// 表示可以发送给多个对等节点的广播消息。
#[derive(Clone, Debug)]
pub struct ProtocolBroadcastMessage<N: NetworkPrimitives = EthNetworkPrimitives> {
    /// 唯一标识符，代表以太坊消息的类型。
    pub message_type: EthMessageID,
    /// 要广播的消息内容，包括基于消息类型的特定数据。
    pub message: EthBroadcastMessage<N>,
}

impl<N: NetworkPrimitives> Encodable for ProtocolBroadcastMessage<N> {
    /// 将协议消息编码为字节。消息类型被编码为单个字节并放在消息前面。
    fn encode(&self, out: &mut dyn BufMut) {
        self.message_type.encode(out);
        self.message.encode(out);
    }
    fn length(&self) -> usize {
        self.message_type.length() + self.message.length()
    }
}

impl<N: NetworkPrimitives> From<EthBroadcastMessage<N>> for ProtocolBroadcastMessage<N> {
    fn from(message: EthBroadcastMessage<N>) -> Self {
        Self { message_type: message.message_id(), message }
    }
}

/// 表示以太坊有线协议版本 66、67、68、69 和 70 中的消息。
///
/// 以太坊有线协议是一组广播到网络的消息，主要有两种风格：
///  * 请求-响应对：由一方发起请求（如 [`GetPooledTransactions`]），另一方回复（如 [`PooledTransactions`]）。
///    从 eth/66 开始，这些消息包含 `request_id` 以支持多路复用。
///  * 广播消息：直接发送到网络，不需要对应的请求。
///
/// 较新的 `eth/66` 是在 `eth/65` 基础上的效率升级，引入了请求 ID 以关联请求-响应消息对。这允许请求多路复用。
///
/// `eth/67` 基于 `eth/66`，但仅删除了两条消息：[`GetNodeData`] 和 [`NodeData`]。
///
/// `eth/68` 仅更改了 `NewPooledTransactionHashes` 以包含 `types` 和 `sizes`。为此，
/// `NewPooledTransactionHashes` 被重命名为 [`NewPooledTransactionHashes66`]，
/// 并定义了 [`NewPooledTransactionHashes68`]。
///
/// `eth/69` 宣告了节点服务的历史区块范围。删除了总难度信息。并从协议传输的收据中删除了布隆过滤器字段。
///
/// `eth/70` (EIP-7975) 保持了 eth/69 的状态格式，并引入了部分收据的请求/响应。
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum EthMessage<N: NetworkPrimitives = EthNetworkPrimitives> {
    /// 握手消息，建立连接后的第一个操作。
    Status(StatusMessage),
    /// 广播新区块的哈希。
    NewBlockHashes(NewBlockHashes),
    /// 广播完整的新区块。
    #[cfg_attr(
        feature = "serde",
        serde(bound = "N::NewBlockPayload: serde::Serialize + serde::de::DeserializeOwned")
    )]
    NewBlock(Box<N::NewBlockPayload>),
    /// 广播新交易。
    #[cfg_attr(
        feature = "serde",
        serde(bound = "N::BroadcastedTransaction: serde::Serialize + serde::de::DeserializeOwned")
    )]
    Transactions(Transactions<N::BroadcastedTransaction>),
    /// eth/66 版本的交易池哈希广播。
    NewPooledTransactionHashes66(NewPooledTransactionHashes66),
    /// eth/68 版本的交易池哈希广播（增加了类型和大小）。
    NewPooledTransactionHashes68(NewPooledTransactionHashes68),
    // 以下消息是请求-响应消息对
    /// 表示 `GetBlockHeaders` 请求-响应对。
    GetBlockHeaders(RequestPair<GetBlockHeaders>),
    /// 表示 `BlockHeaders` 请求-响应对。
    #[cfg_attr(
        feature = "serde",
        serde(bound = "N::BlockHeader: serde::Serialize + serde::de::DeserializeOwned")
    )]
    BlockHeaders(RequestPair<BlockHeaders<N::BlockHeader>>),
    /// 表示 `GetBlockBodies` 请求-响应对。
    GetBlockBodies(RequestPair<GetBlockBodies>),
    /// 表示 `BlockBodies` 请求-响应对。
    #[cfg_attr(
        feature = "serde",
        serde(bound = "N::BlockBody: serde::Serialize + serde::de::DeserializeOwned")
    )]
    BlockBodies(RequestPair<BlockBodies<N::BlockBody>>),
    /// 表示 `GetPooledTransactions` 请求-响应对。
    GetPooledTransactions(RequestPair<GetPooledTransactions>),
    /// 表示 `PooledTransactions` 请求-响应对。
    #[cfg_attr(
        feature = "serde",
        serde(bound = "N::PooledTransaction: serde::Serialize + serde::de::DeserializeOwned")
    )]
    PooledTransactions(RequestPair<PooledTransactions<N::PooledTransaction>>),
    /// 表示 `GetNodeData` 请求-响应对（eth/67 废弃）。
    GetNodeData(RequestPair<GetNodeData>),
    /// 表示 `NodeData` 请求-响应对（eth/67 废弃）。
    NodeData(RequestPair<NodeData>),
    /// 表示 `GetReceipts` 请求-响应对。
    GetReceipts(RequestPair<GetReceipts>),
    /// 表示 eth/70 的 `GetReceipts` 请求。
    ///
    /// 注意：与早期协议版本不同，EIP-7975 中 eth/70 编码的 `GetReceipts` 内联了请求 ID。
    /// 该类型仍然包装了一个 [`RequestPair`]，但使用了自定义的内联编码。
    GetReceipts70(RequestPair<GetReceipts70>),
    /// 表示 `Receipts` 请求-响应对。
    #[cfg_attr(
        feature = "serde",
        serde(bound = "N::Receipt: serde::Serialize + serde::de::DeserializeOwned")
    )]
    Receipts(RequestPair<Receipts<N::Receipt>>),
    /// 表示 eth/69 的 `Receipts` 请求-响应对（不含布隆过滤器）。
    #[cfg_attr(
        feature = "serde",
        serde(bound = "N::Receipt: serde::Serialize + serde::de::DeserializeOwned")
    )]
    Receipts69(RequestPair<Receipts69<N::Receipt>>),
    /// 表示 eth/70 的 `Receipts` 请求-响应对（包含 lastBlockIncomplete 标志）。
    #[cfg_attr(
        feature = "serde",
        serde(bound = "N::Receipt: serde::Serialize + serde::de::DeserializeOwned")
    )]
    ///
    /// 注意：EIP-7975 中 eth/70 编码的 `Receipts` 内联了请求 ID。
    /// 该类型仍然包装了一个 [`RequestPair`]，但使用了自定义的内联编码。
    Receipts70(RequestPair<Receipts70<N::Receipt>>),
    /// 表示广播到网络的 `BlockRangeUpdate` 消息。
    #[cfg_attr(
        feature = "serde",
        serde(bound = "N::BroadcastedTransaction: serde::Serialize + serde::de::DeserializeOwned")
    )]
    BlockRangeUpdate(BlockRangeUpdate),
    /// 表示不匹配任何其他变体的编码消息。
    Other(RawCapabilityMessage),
}

impl<N: NetworkPrimitives> EthMessage<N> {
    /// 返回消息的 ID。
    pub const fn message_id(&self) -> EthMessageID {
        match self {
            Self::Status(_) => EthMessageID::Status,
            Self::NewBlockHashes(_) => EthMessageID::NewBlockHashes,
            Self::NewBlock(_) => EthMessageID::NewBlock,
            Self::Transactions(_) => EthMessageID::Transactions,
            Self::NewPooledTransactionHashes66(_) | Self::NewPooledTransactionHashes68(_) => {
                EthMessageID::NewPooledTransactionHashes
            }
            Self::GetBlockHeaders(_) => EthMessageID::GetBlockHeaders,
            Self::BlockHeaders(_) => EthMessageID::BlockHeaders,
            Self::GetBlockBodies(_) => EthMessageID::GetBlockBodies,
            Self::BlockBodies(_) => EthMessageID::BlockBodies,
            Self::GetPooledTransactions(_) => EthMessageID::GetPooledTransactions,
            Self::PooledTransactions(_) => EthMessageID::PooledTransactions,
            Self::GetNodeData(_) => EthMessageID::GetNodeData,
            Self::NodeData(_) => EthMessageID::NodeData,
            Self::GetReceipts(_) | Self::GetReceipts70(_) => EthMessageID::GetReceipts,
            Self::Receipts(_) | Self::Receipts69(_) | Self::Receipts70(_) => EthMessageID::Receipts,
            Self::BlockRangeUpdate(_) => EthMessageID::BlockRangeUpdate,
            Self::Other(msg) => EthMessageID::Other(msg.id as u8),
        }
    }

    /// 如果消息变体是请求，则返回 true。
    pub const fn is_request(&self) -> bool {
        matches!(
            self,
            Self::GetBlockBodies(_) |
                Self::GetBlockHeaders(_) |
                Self::GetReceipts(_) |
                Self::GetReceipts70(_) |
                Self::GetPooledTransactions(_) |
                Self::GetNodeData(_)
        )
    }

    /// 如果消息变体是对请求的响应，则返回 true。
    pub const fn is_response(&self) -> bool {
        matches!(
            self,
            Self::PooledTransactions(_) |
                Self::Receipts(_) |
                Self::Receipts69(_) |
                Self::Receipts70(_) |
                Self::BlockHeaders(_) |
                Self::BlockBodies(_) |
                Self::NodeData(_)
        )
    }

    /// 在适用时转换消息类型。
    ///
    /// 处理向上/向下转型，例如对于不同的收据请求类型。
    pub fn map_versioned(self, version: EthVersion) -> Self {
        // 对于 eth/70 对等节点，我们使用新的 eth/70 编码发送 `GetReceipts`，
        // 并设置 `firstBlockReceiptIndex = 0`，同时保持面向用户的 `PeerRequest` API 不变。
        if version >= EthVersion::Eth70 {
            return match self {
                Self::GetReceipts(pair) => {
                    let RequestPair { request_id, message } = pair;
                    let req = RequestPair {
                        request_id,
                        message: GetReceipts70 {
                            first_block_receipt_index: 0,
                            block_hashes: message.0,
                        },
                    };
                    Self::GetReceipts70(req)
                }
                other => other,
            }
        }

        self
    }
}

impl<N: NetworkPrimitives> Encodable for EthMessage<N> {
    fn encode(&self, out: &mut dyn BufMut) {
        match self {
            Self::Status(status) => status.encode(out),
            Self::NewBlockHashes(new_block_hashes) => new_block_hashes.encode(out),
            Self::NewBlock(new_block) => new_block.encode(out),
            Self::Transactions(transactions) => transactions.encode(out),
            Self::NewPooledTransactionHashes66(hashes) => hashes.encode(out),
            Self::NewPooledTransactionHashes68(hashes) => hashes.encode(out),
            Self::GetBlockHeaders(request) => request.encode(out),
            Self::BlockHeaders(headers) => headers.encode(out),
            Self::GetBlockBodies(request) => request.encode(out),
            Self::BlockBodies(bodies) => bodies.encode(out),
            Self::GetPooledTransactions(request) => request.encode(out),
            Self::PooledTransactions(transactions) => transactions.encode(out),
            Self::GetNodeData(request) => request.encode(out),
            Self::NodeData(data) => data.encode(out),
            Self::GetReceipts(request) => request.encode(out),
            Self::GetReceipts70(request) => request.encode(out),
            Self::Receipts(receipts) => receipts.encode(out),
            Self::Receipts69(receipt69) => receipt69.encode(out),
            Self::Receipts70(receipt70) => receipt70.encode(out),
            Self::BlockRangeUpdate(block_range_update) => block_range_update.encode(out),
            Self::Other(unknown) => out.put_slice(&unknown.payload),
        }
    }
    fn length(&self) -> usize {
        match self {
            Self::Status(status) => status.length(),
            Self::NewBlockHashes(new_block_hashes) => new_block_hashes.length(),
            Self::NewBlock(new_block) => new_block.length(),
            Self::Transactions(transactions) => transactions.length(),
            Self::NewPooledTransactionHashes66(hashes) => hashes.length(),
            Self::NewPooledTransactionHashes68(hashes) => hashes.length(),
            Self::GetBlockHeaders(request) => request.length(),
            Self::BlockHeaders(headers) => headers.length(),
            Self::GetBlockBodies(request) => request.length(),
            Self::BlockBodies(bodies) => bodies.length(),
            Self::GetPooledTransactions(request) => request.length(),
            Self::PooledTransactions(transactions) => transactions.length(),
            Self::GetNodeData(request) => request.length(),
            Self::NodeData(data) => data.length(),
            Self::GetReceipts(request) => request.length(),
            Self::GetReceipts70(request) => request.length(),
            Self::Receipts(receipts) => receipts.length(),
            Self::Receipts69(receipt69) => receipt69.length(),
            Self::Receipts70(receipt70) => receipt70.length(),
            Self::BlockRangeUpdate(block_range_update) => block_range_update.length(),
            Self::Other(unknown) => unknown.length(),
        }
    }
}

/// 表示 [`EthMessage`] 的广播消息，使用同一个对象可以发送给多个对等节点。
///
/// 包含哈希列表的消息取决于消息发送给哪个对等节点。对等节点永远不应接收它已经见过的对象（区块、交易）的哈希。
///
/// 注意：这仅对传出消息有用。
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum EthBroadcastMessage<N: NetworkPrimitives = EthNetworkPrimitives> {
    /// 表示新区块广播消息。
    NewBlock(Arc<N::NewBlockPayload>),
    /// 表示交易广播消息。
    Transactions(SharedTransactions<N::BroadcastedTransaction>),
}

// === impl EthBroadcastMessage ===

impl<N: NetworkPrimitives> EthBroadcastMessage<N> {
    /// 返回消息的 ID。
    pub const fn message_id(&self) -> EthMessageID {
        match self {
            Self::NewBlock(_) => EthMessageID::NewBlock,
            Self::Transactions(_) => EthMessageID::Transactions,
        }
    }
}

impl<N: NetworkPrimitives> Encodable for EthBroadcastMessage<N> {
    fn encode(&self, out: &mut dyn BufMut) {
        match self {
            Self::NewBlock(new_block) => new_block.encode(out),
            Self::Transactions(transactions) => transactions.encode(out),
        }
    }

    fn length(&self) -> usize {
        match self {
            Self::NewBlock(new_block) => new_block.length(),
            Self::Transactions(transactions) => transactions.length(),
        }
    }
}

/// 表示以太坊协议消息的消息 ID。
#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum EthMessageID {
    /// 状态消息。
    Status = 0x00,
    /// 新区块哈希消息。
    NewBlockHashes = 0x01,
    /// 交易消息。
    Transactions = 0x02,
    /// 获取区块头消息。
    GetBlockHeaders = 0x03,
    /// 区块头消息。
    BlockHeaders = 0x04,
    /// 获取区块体消息。
    GetBlockBodies = 0x05,
    /// 区块体消息。
    BlockBodies = 0x06,
    /// 新区块消息。
    NewBlock = 0x07,
    /// 新池交易哈希消息。
    NewPooledTransactionHashes = 0x08,
    /// 请求池交易。
    GetPooledTransactions = 0x09,
    /// 表示池交易。
    PooledTransactions = 0x0a,
    /// 请求节点数据（eth/67 废弃）。
    GetNodeData = 0x0d,
    /// 表示节点数据（eth/67 废弃）。
    NodeData = 0x0e,
    /// 请求收据。
    GetReceipts = 0x0f,
    /// 表示收据。
    Receipts = 0x10,
    /// 区块范围更新。
    ///
    /// 在 Eth69 中引入。
    BlockRangeUpdate = 0x11,
    /// 表示未知的消息类型。
    Other(u8),
}

impl EthMessageID {
    /// 返回 `EthMessageID` 对应的 `u8` 值。
    pub const fn to_u8(&self) -> u8 {
        match self {
            Self::Status => 0x00,
            Self::NewBlockHashes => 0x01,
            Self::Transactions => 0x02,
            Self::GetBlockHeaders => 0x03,
            Self::BlockHeaders => 0x04,
            Self::GetBlockBodies => 0x05,
            Self::BlockBodies => 0x06,
            Self::NewBlock => 0x07,
            Self::NewPooledTransactionHashes => 0x08,
            Self::GetPooledTransactions => 0x09,
            Self::PooledTransactions => 0x0a,
            Self::GetNodeData => 0x0d,
            Self::NodeData => 0x0e,
            Self::GetReceipts => 0x0f,
            Self::Receipts => 0x10,
            Self::BlockRangeUpdate => 0x11,
            Self::Other(value) => *value,
        }
    }

    /// 返回给定协议版本的最大 ID 值。
    pub const fn max(version: EthVersion) -> u8 {
        if version.is_eth69() {
            Self::BlockRangeUpdate.to_u8()
        } else {
            Self::Receipts.to_u8()
        }
    }

    /// 返回给定协议版本的消息类型总数。
    ///
    /// 这用于消息 ID 多路复用。
    ///
    /// <https://github.com/ethereum/go-ethereum/blob/85077be58edea572f29c3b1a6a055077f1a56a8b/eth/protocols/eth/protocol.go#L45-L47>
    pub const fn message_count(version: EthVersion) -> u8 {
        Self::max(version) + 1
    }
}

impl Encodable for EthMessageID {
    fn encode(&self, out: &mut dyn BufMut) {
        out.put_u8(self.to_u8());
    }
    fn length(&self) -> usize {
        1
    }
}

impl Decodable for EthMessageID {
    fn decode(buf: &mut &[u8]) -> alloy_rlp::Result<Self> {
        let id = match buf.first().ok_or(alloy_rlp::Error::InputTooShort)? {
            0x00 => Self::Status,
            0x01 => Self::NewBlockHashes,
            0x02 => Self::Transactions,
            0x03 => Self::GetBlockHeaders,
            0x04 => Self::BlockHeaders,
            0x05 => Self::GetBlockBodies,
            0x06 => Self::BlockBodies,
            0x07 => Self::NewBlock,
            0x08 => Self::NewPooledTransactionHashes,
            0x09 => Self::GetPooledTransactions,
            0x0a => Self::PooledTransactions,
            0x0d => Self::GetNodeData,
            0x0e => Self::NodeData,
            0x0f => Self::GetReceipts,
            0x10 => Self::Receipts,
            0x11 => Self::BlockRangeUpdate,
            unknown => Self::Other(*unknown),
        };
        buf.advance(1);
        Ok(id)
    }
}

impl TryFrom<usize> for EthMessageID {
    type Error = &'static str;

    fn try_from(value: usize) -> Result<Self, Self::Error> {
        match value {
            0x00 => Ok(Self::Status),
            0x01 => Ok(Self::NewBlockHashes),
            0x02 => Ok(Self::Transactions),
            0x03 => Ok(Self::GetBlockHeaders),
            0x04 => Ok(Self::BlockHeaders),
            0x05 => Ok(Self::GetBlockBodies),
            0x06 => Ok(Self::BlockBodies),
            0x07 => Ok(Self::NewBlock),
            0x08 => Ok(Self::NewPooledTransactionHashes),
            0x09 => Ok(Self::GetPooledTransactions),
            0x0a => Ok(Self::PooledTransactions),
            0x0d => Ok(Self::GetNodeData),
            0x0e => Ok(Self::NodeData),
            0x0f => Ok(Self::GetReceipts),
            0x10 => Ok(Self::Receipts),
            0x11 => Ok(Self::BlockRangeUpdate),
            _ => Err("Invalid message ID"),
        }
    }
}

/// 这用于所有请求-响应风格的 `eth` 协议消息。
/// 这可以表示请求或响应，因为两者都包含消息负载和请求 ID。
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(any(test, feature = "arbitrary"), derive(arbitrary::Arbitrary))]
pub struct RequestPair<T> {
    /// 包含的请求或响应消息的 ID。
    pub request_id: u64,

    /// 请求或响应消息的负载。
    pub message: T,
}

impl<T> RequestPair<T> {
    /// 使用给定的闭包转换消息类型。
    pub fn map<F, R>(self, f: F) -> RequestPair<R>
    where
        F: FnOnce(T) -> R,
    {
        let Self { request_id, message } = self;
        RequestPair { request_id, message: f(message) }
    }
}

/// 允许带有请求 ID 的消息序列化为 RLP 字节。
impl<T> Encodable for RequestPair<T>
where
    T: Encodable,
{
    fn encode(&self, out: &mut dyn alloy_rlp::BufMut) {
        let header =
            Header { list: true, payload_length: self.request_id.length() + self.message.length() };

        header.encode(out);
        self.request_id.encode(out);
        self.message.encode(out);
    }

    fn length(&self) -> usize {
        let mut length = 0;
        length += self.request_id.length();
        length += self.message.length();
        length += length_of_length(length);
        length
    }
}

/// 允许带有请求 ID 的消息反序列化为 RLP 字节。
impl<T> Decodable for RequestPair<T>
where
    T: Decodable,
{
    fn decode(buf: &mut &[u8]) -> alloy_rlp::Result<Self> {
        let header = Header::decode(buf)?;

        let initial_length = buf.len();
        let request_id = u64::decode(buf)?;
        let message = T::decode(buf)?;

        // 检查解码 RequestPair 后缓冲区是否正好消耗了 payload_length 字节
        let consumed_len = initial_length - buf.len();
        if consumed_len != header.payload_length {
            return Err(alloy_rlp::Error::UnexpectedLength)
        }

        Ok(Self { request_id, message })
    }
}

#[cfg(test)]
mod tests {
    use super::MessageError;
    use crate::{
        message::RequestPair, EthMessage, EthMessageID, EthNetworkPrimitives, EthVersion,
        GetNodeData, NodeData, ProtocolMessage, RawCapabilityMessage,
    };
    use alloy_primitives::hex;
    use alloy_rlp::{Decodable, Encodable, Error};
    use reth_ethereum_primitives::BlockBody;

    fn encode<T: Encodable>(value: T) -> Vec<u8> {
        let mut buf = vec![];
        value.encode(&mut buf);
        buf
    }

    #[test]
    fn test_removed_message_at_eth67() {
        let get_node_data = EthMessage::<EthNetworkPrimitives>::GetNodeData(RequestPair {
            request_id: 1337,
            message: GetNodeData(vec![]),
        });
        let buf = encode(ProtocolMessage {
            message_type: EthMessageID::GetNodeData,
            message: get_node_data,
        });
        let msg = ProtocolMessage::<EthNetworkPrimitives>::decode_message(
            crate::EthVersion::Eth67,
            &mut &buf[..],
        );
        assert!(matches!(msg, Err(MessageError::Invalid(..))));

        let node_data = EthMessage::<EthNetworkPrimitives>::NodeData(RequestPair {
            request_id: 1337,
            message: NodeData(vec![]),
        });
        let buf =
            encode(ProtocolMessage { message_type: EthMessageID::NodeData, message: node_data });
        let msg = ProtocolMessage::<EthNetworkPrimitives>::decode_message(
            crate::EthVersion::Eth67,
            &mut &buf[..],
        );
        assert!(matches!(msg, Err(MessageError::Invalid(..))));
    }

    #[test]
    fn request_pair_encode() {
        let request_pair = RequestPair { request_id: 1337, message: vec![5u8] };

        // c5: 列表开始 (c0) + len(full_list) (长度 <55 字节)
        // 82: 0x80 + len(1337)
        // 05 39: 1337 (request_id)
        // === full_list ===
        // c1: 列表开始 (c0) + len(list) (长度 <55 字节)
        // 05: 5 (message)
        let expected = hex!("c5820539c105");
        let got = encode(request_pair);
        assert_eq!(expected[..], got, "expected: {expected:X?}, got: {got:X?}",);
    }

    #[test]
    fn request_pair_decode() {
        let raw_pair = &hex!("c5820539c105")[..];

        let expected = RequestPair { request_id: 1337, message: vec![5u8] };

        let got = RequestPair::<Vec<u8>>::decode(&mut &*raw_pair).unwrap();
        assert_eq!(expected.length(), raw_pair.len());
        assert_eq!(expected, got);
    }

    #[test]
    fn malicious_request_pair_decode() {
        // 一个恶意编码的请求对，其中 len(full_list) 为 5，但解码时实际消耗了 6 个字节
        let raw_pair = &hex!("c5820539c20505")[..];

        let result = RequestPair::<Vec<u8>>::decode(&mut &*raw_pair);
        assert!(matches!(result, Err(Error::UnexpectedLength)));
    }

    #[test]
    fn empty_block_bodies_protocol() {
        let empty_block_bodies =
            ProtocolMessage::from(EthMessage::<EthNetworkPrimitives>::BlockBodies(RequestPair {
                request_id: 0,
                message: Default::default(),
            }));
        let mut buf = Vec::new();
        empty_block_bodies.encode(&mut buf);
        let decoded =
            ProtocolMessage::decode_message(EthVersion::Eth68, &mut buf.as_slice()).unwrap();
        assert_eq!(empty_block_bodies, decoded);
    }

    #[test]
    fn empty_block_body_protocol() {
        let empty_block_bodies =
            ProtocolMessage::from(EthMessage::<EthNetworkPrimitives>::BlockBodies(RequestPair {
                request_id: 0,
                message: vec![BlockBody {
                    transactions: vec![],
                    ommers: vec![],
                    withdrawals: Some(Default::default()),
                }]
                .into(),
            }));
        let mut buf = Vec::new();
        empty_block_bodies.encode(&mut buf);
        let decoded =
            ProtocolMessage::decode_message(EthVersion::Eth68, &mut buf.as_slice()).unwrap();
        assert_eq!(empty_block_bodies, decoded);
    }

    #[test]
    fn decode_block_bodies_message() {
        let buf = hex!("06c48199c1c0");
        let msg = ProtocolMessage::<EthNetworkPrimitives>::decode_message(
            EthVersion::Eth68,
            &mut &buf[..],
        )
        .unwrap_err();
        assert!(matches!(msg, MessageError::RlpError(alloy_rlp::Error::InputTooShort)));
    }

    #[test]
    fn custom_message_roundtrip() {
        let custom_payload = vec![1, 2, 3, 4, 5];
        let custom_message = RawCapabilityMessage::new(0x20, custom_payload.into());
        let protocol_message = ProtocolMessage::<EthNetworkPrimitives> {
            message_type: EthMessageID::Other(0x20),
            message: EthMessage::Other(custom_message),
        };

        let encoded = encode(protocol_message.clone());
        let decoded = ProtocolMessage::<EthNetworkPrimitives>::decode_message(
            EthVersion::Eth68,
            &mut &encoded[..],
        )
        .unwrap();

        assert_eq!(protocol_message, decoded);
    }

    #[test]
    fn custom_message_empty_payload_roundtrip() {
        let custom_message = RawCapabilityMessage::new(0x30, vec![].into());
        let protocol_message = ProtocolMessage::<EthNetworkPrimitives> {
            message_type: EthMessageID::Other(0x30),
            message: EthMessage::Other(custom_message),
        };

        let encoded = encode(protocol_message.clone());
        let decoded = ProtocolMessage::<EthNetworkPrimitives>::decode_message(
            EthVersion::Eth68,
            &mut &encoded[..],
        )
        .unwrap();

        assert_eq!(protocol_message, decoded);
    }
}