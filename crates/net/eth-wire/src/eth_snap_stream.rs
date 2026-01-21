//! 以太坊 (eth) 与快照 (snap) 混合协议流实现。
//!
//! 提供一种在单个 `RLPx` 连接上同时处理 eth 和 snap 协议消息的流类型。
//! 包含消息编解码、消息 ID 多路复用 (ID multiplexing) 以及协议消息处理。

use super::message::MAX_MESSAGE_SIZE;
use crate::{
    message::{EthBroadcastMessage, ProtocolBroadcastMessage},
    EthMessage, EthMessageID, EthNetworkPrimitives, EthVersion, NetworkPrimitives, ProtocolMessage,
    RawCapabilityMessage, SnapMessageId, SnapProtocolMessage,
};
use alloy_rlp::{Bytes, BytesMut, Encodable};
use core::fmt::Debug;
use futures::{Sink, SinkExt};
use pin_project::pin_project;
use std::{
    marker::PhantomData,
    pin::Pin,
    task::{ready, Context, Poll},
};
use tokio_stream::Stream;

/// eth 和 snap 流的错误类型
#[derive(thiserror::Error, Debug)]
pub enum EthSnapStreamError {
    /// 协议版本对应的无效消息
    #[error("invalid message for version {0:?}: {1}")]
    InvalidMessage(EthVersion, String),

    /// 未知的消息 ID
    #[error("unknown message id: {0}")]
    UnknownMessageId(u8),

    /// 消息过大
    #[error("message too large: {0} > {1}")]
    MessageTooLarge(usize, usize),

    /// RLP 解码错误
    #[error("rlp error: {0}")]
    Rlp(#[from] alloy_rlp::Error),

    /// 在握手之外收到了 Status 消息
    #[error("status message received outside handshake")]
    StatusNotInHandshake,
}

/// 混合消息类型，包含 eth 或 snap 协议消息
#[derive(Debug)]
pub enum EthSnapMessage<N: NetworkPrimitives = EthNetworkPrimitives> {
    /// 以太坊 (eth) 协议消息
    Eth(EthMessage<N>),
    /// 快照 (snap) 协议消息
    Snap(SnapProtocolMessage),
}

/// 在单个连接上同时处理 eth 和 snap 协议消息的流实现。
#[pin_project]
#[derive(Debug, Clone)]
pub struct EthSnapStream<S, N = EthNetworkPrimitives> {
    /// 协议逻辑处理
    eth_snap: EthSnapStreamInner<N>,
    /// 内部字节流
    #[pin]
    inner: S,
}

impl<S, N> EthSnapStream<S, N>
where
    N: NetworkPrimitives,
{
    /// 创建一个新的 eth/snap 协议流
    pub const fn new(stream: S, eth_version: EthVersion) -> Self {
        Self { eth_snap: EthSnapStreamInner::new(eth_version), inner: stream }
    }

    /// 返回 eth 版本
    #[inline]
    pub const fn eth_version(&self) -> EthVersion {
        self.eth_snap.eth_version()
    }

    /// 返回底层流的引用
    #[inline]
    pub const fn inner(&self) -> &S {
        &self.inner
    }

    /// 返回底层流的可变引用
    #[inline]
    pub const fn inner_mut(&mut self) -> &mut S {
        &mut self.inner
    }

    /// 消费此类型并返回包装的流
    #[inline]
    pub fn into_inner(self) -> S {
        self.inner
    }
}

impl<S, E, N> EthSnapStream<S, N>
where
    S: Sink<Bytes, Error = E> + Unpin,
    EthSnapStreamError: From<E>,
    N: NetworkPrimitives,
{
    /// 与 [`Sink::start_send`] 类似，但接受的是 [`EthBroadcastMessage`]。
    pub fn start_send_broadcast(
        &mut self,
        item: EthBroadcastMessage<N>,
    ) -> Result<(), EthSnapStreamError> {
        self.inner.start_send_unpin(Bytes::from(alloy_rlp::encode(
            ProtocolBroadcastMessage::from(item),
        )))?;

        Ok(())
    }

    /// 直接在流上发送原始功能消息
    pub fn start_send_raw(&mut self, msg: RawCapabilityMessage) -> Result<(), EthSnapStreamError> {
        let mut bytes = Vec::with_capacity(msg.payload.len() + 1);
        msg.id.encode(&mut bytes);
        bytes.extend_from_slice(&msg.payload);

        self.inner.start_send_unpin(bytes.into())?;
        Ok(())
    }
}

impl<S, E, N> Stream for EthSnapStream<S, N>
where
    S: Stream<Item = Result<BytesMut, E>> + Unpin,
    EthSnapStreamError: From<E>,
    N: NetworkPrimitives,
{
    type Item = Result<EthSnapMessage<N>, EthSnapStreamError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.project();
        let res = ready!(this.inner.poll_next(cx));

        match res {
            Some(Ok(bytes)) => Poll::Ready(Some(this.eth_snap.decode_message(bytes))),
            Some(Err(err)) => Poll::Ready(Some(Err(err.into()))),
            None => Poll::Ready(None),
        }
    }
}

impl<S, E, N> Sink<EthSnapMessage<N>> for EthSnapStream<S, N>
where
    S: Sink<Bytes, Error = E> + Unpin,
    EthSnapStreamError: From<E>,
    N: NetworkPrimitives,
{
    type Error = EthSnapStreamError;

    fn poll_ready(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.project().inner.poll_ready(cx).map_err(Into::into)
    }

    fn start_send(mut self: Pin<&mut Self>, item: EthSnapMessage<N>) -> Result<(), Self::Error> {
        let mut this = self.as_mut().project();

        let bytes = match item {
            EthSnapMessage::Eth(eth_msg) => this.eth_snap.encode_eth_message(eth_msg)?,
            EthSnapMessage::Snap(snap_msg) => this.eth_snap.encode_snap_message(snap_msg),
        };

        this.inner.start_send_unpin(bytes)?;
        Ok(())
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.project().inner.poll_flush(cx).map_err(Into::into)
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.project().inner.poll_close(cx).map_err(Into::into)
    }
}

/// 处理 eth 和 snap 混合协议逻辑的内部结构
#[derive(Debug, Clone)]
struct EthSnapStreamInner<N> {
    /// eth 协议版本
    eth_version: EthVersion,
    /// 类型标记
    _pd: PhantomData<N>,
}

impl<N> EthSnapStreamInner<N>
where
    N: NetworkPrimitives,
{
    const fn new(eth_version: EthVersion) -> Self {
        Self { eth_version, _pd: PhantomData }
    }

    #[inline]
    const fn eth_version(&self) -> EthVersion {
        self.eth_version
    }

    /// 从流中解码消息
    fn decode_message(&self, bytes: BytesMut) -> Result<EthSnapMessage<N>, EthSnapStreamError> {
        if bytes.len() > MAX_MESSAGE_SIZE {
            return Err(EthSnapStreamError::MessageTooLarge(bytes.len(), MAX_MESSAGE_SIZE));
        }

        if bytes.is_empty() {
            return Err(EthSnapStreamError::Rlp(alloy_rlp::Error::InputTooShort));
        }

        let message_id = bytes[0];

        // 核心多路复用逻辑：
        // 能力 (capabilities) 按字典序排序。如果 "eth" 在 "snap" 之前，
        // 则 eth 消息 ID 较低（0x10 开始分配），snap 消息 ID 紧随其后。
        //
        // 1. 如果 ID <= eth 消息的最大 ID，则是 eth 消息。
        if message_id <= EthMessageID::max(self.eth_version) {
            let mut buf = bytes.as_ref();
            match ProtocolMessage::decode_message(self.eth_version, &mut buf) {
                Ok(protocol_msg) => {
                    if matches!(protocol_msg.message, EthMessage::Status(_)) {
                        return Err(EthSnapStreamError::StatusNotInHandshake);
                    }
                    Ok(EthSnapMessage::Eth(protocol_msg.message))
                }
                Err(err) => {
                    Err(EthSnapStreamError::InvalidMessage(self.eth_version, err.to_string()))
                }
            }
        // 2. 如果 ID 在 snap 消息的范围内，则是 snap 消息。
        } else if message_id > EthMessageID::max(self.eth_version) &&
            message_id <=
                EthMessageID::message_count(self.eth_version) + SnapMessageId::TrieNodes as u8
        {
            // 对于多路复用的 snap 消息 ID：
            // 真实的 snap_id = 多路复用 ID - eth 消息的总数
            let adjusted_message_id = message_id - EthMessageID::message_count(self.eth_version);
            let mut buf = &bytes[1..];

            match SnapProtocolMessage::decode(adjusted_message_id, &mut buf) {
                Ok(snap_msg) => Ok(EthSnapMessage::Snap(snap_msg)),
                Err(err) => Err(EthSnapStreamError::Rlp(err)),
            }
        } else {
            Err(EthSnapStreamError::UnknownMessageId(message_id))
        }
    }

    /// 编码 eth 消息
    fn encode_eth_message(&self, item: EthMessage<N>) -> Result<Bytes, EthSnapStreamError> {
        if matches!(item, EthMessage::Status(_)) {
            return Err(EthSnapStreamError::StatusNotInHandshake);
        }

        let protocol_msg = ProtocolMessage::from(item);
        let mut buf = Vec::new();
        protocol_msg.encode(&mut buf);
        Ok(Bytes::from(buf))
    }

    /// 编码 snap 协议消息，并根据 eth 消息数量调整其 ID，以实现正确的多路复用。
    fn encode_snap_message(&self, message: SnapProtocolMessage) -> Bytes {
        let encoded = message.encode();

        let message_id = encoded[0];
        // 调整 ID：snap_id + eth_message_count
        let adjusted_id = message_id + EthMessageID::message_count(self.eth_version);

        let mut adjusted = Vec::with_capacity(encoded.len());
        adjusted.push(adjusted_id);
        adjusted.extend_from_slice(&encoded[1..]);

        Bytes::from(adjusted)
    }
}

#[cfg(test)]
mod tests {
    // ... (测试部分保持不变)
}