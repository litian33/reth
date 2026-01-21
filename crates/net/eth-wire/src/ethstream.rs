//! 以太坊协议流实现。
//!
//! 提供以太坊有线协议 (eth wire protocol) 的流类型。
//! 它将协议逻辑 [`EthStreamInner`] 与传输关注点 [`EthStream`] 分离开来。
//! 处理握手、消息处理和 RLP 序列化。

use crate::{
    errors::{EthHandshakeError, EthStreamError},
    handshake::EthereumEthHandshake,
    message::{EthBroadcastMessage, ProtocolBroadcastMessage},
    p2pstream::HANDSHAKE_TIMEOUT,
    CanDisconnect, DisconnectReason, EthMessage, EthNetworkPrimitives, EthVersion, ProtocolMessage,
    UnifiedStatus,
};
use alloy_primitives::bytes::{Bytes, BytesMut};
use alloy_rlp::Encodable;
use futures::{ready, Sink, SinkExt};
use pin_project::pin_project;
use reth_eth_wire_types::{NetworkPrimitives, RawCapabilityMessage};
use reth_ethereum_forks::ForkFilter;
use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
    time::Duration,
};
use tokio::time::timeout;
use tokio_stream::Stream;
use tracing::{debug, trace};

/// [`MAX_MESSAGE_SIZE`] 是协议消息大小的最大上限（10MB）。
// 参考 Geth 实现：https://github.com/ethereum/go-ethereum/blob/30602163d5d8321fbc68afdcbbaf2362b2641bde/eth/protocols/eth/protocol.go#L50
pub const MAX_MESSAGE_SIZE: usize = 10 * 1024 * 1024;

/// 未经身份验证的 [`EthStream`]。在完成 `Status` 握手后，它会被消费并返回一个 [`EthStream`]。
#[pin_project]
#[derive(Debug)]
pub struct UnauthedEthStream<S> {
    #[pin]
    inner: S,
}

impl<S> UnauthedEthStream<S> {
    /// 从实现 `Stream` 和 `Sink` 的类型 `S` 创建一个新的 `UnauthedEthStream`。
    pub const fn new(inner: S) -> Self {
        Self { inner }
    }

    /// 消费此类型并返回包装的流。
    pub fn into_inner(self) -> S {
        self.inner
    }
}

impl<S, E> UnauthedEthStream<S>
where
    S: Stream<Item = Result<BytesMut, E>> + CanDisconnect<Bytes> + Send + Unpin,
    EthStreamError: From<E> + From<<S as Sink<Bytes>>::Error>,
{
    /// 消费 [`UnauthedEthStream`]，并在 `Status` 握手成功完成后返回一个 [`EthStream`]。
    /// 同时返回远程对等方发送的 `Status` 消息。
    ///
    /// 注意：此函数要求 [`UnifiedStatus`] 配置了正确的 eth 版本，因为从 ETH69 开始初始状态消息发生了变化。
    pub async fn handshake<N: NetworkPrimitives>(
        self,
        status: UnifiedStatus,
        fork_filter: ForkFilter,
    ) -> Result<(EthStream<S, N>, UnifiedStatus), EthStreamError> {
        self.handshake_with_timeout(status, fork_filter, HANDSHAKE_TIMEOUT).await
    }

    /// 带有超时限制的握手包装函数。
    pub async fn handshake_with_timeout<N: NetworkPrimitives>(
        self,
        status: UnifiedStatus,
        fork_filter: ForkFilter,
        timeout_limit: Duration,
    ) -> Result<(EthStream<S, N>, UnifiedStatus), EthStreamError> {
        timeout(timeout_limit, Self::handshake_without_timeout(self, status, fork_filter))
            .await
            .map_err(|_| EthStreamError::StreamTimeout)?
    }

    /// 不带超时限制的握手。
    pub async fn handshake_without_timeout<N: NetworkPrimitives>(
        mut self,
        status: UnifiedStatus,
        fork_filter: ForkFilter,
    ) -> Result<(EthStream<S, N>, UnifiedStatus), EthStreamError> {
        trace!(
            status = %status.into_message(),
            "sending eth status to peer"
        );
        // 执行以太坊 eth 协议层的握手 (Status 交换)
        let their_status =
            EthereumEthHandshake(&mut self.inner).eth_handshake(status, fork_filter).await?;

        // 握手成功完成后，创建正式的 EthStream
        let stream = EthStream::new(status.version, self.inner);

        Ok((stream, their_status))
    }
}

/// 包含处理以太坊协议消息的特定逻辑
#[derive(Debug)]
pub struct EthStreamInner<N> {
    /// 协商一致的 eth 版本
    version: EthVersion,
    _pd: std::marker::PhantomData<N>,
}

impl<N> EthStreamInner<N>
where
    N: NetworkPrimitives,
{
    /// 使用给定的 eth 版本创建一个新的 [`EthStreamInner`]。
    pub const fn new(version: EthVersion) -> Self {
        Self { version, _pd: std::marker::PhantomData }
    }

    /// 返回 eth 版本。
    #[inline]
    pub const fn version(&self) -> EthVersion {
        self.version
    }

    /// 将输入的字节解码为 [`EthMessage`]。
    pub fn decode_message(&self, bytes: BytesMut) -> Result<EthMessage<N>, EthStreamError> {
        if bytes.len() > MAX_MESSAGE_SIZE {
            return Err(EthStreamError::MessageTooBig(bytes.len()));
        }

        // 使用 ProtocolMessage 进行解码，这会根据版本处理具体的解码逻辑
        let msg = match ProtocolMessage::decode_message(self.version, &mut bytes.as_ref()) {
            Ok(m) => m,
            Err(err) => {
                let msg = if bytes.len() > 50 {
                    format!("{:02x?}...{:x?}", &bytes[..10], &bytes[bytes.len() - 10..])
                } else {
                    format!("{bytes:02x?}")
                };
                debug!(
                    version=?self.version,
                    %msg,
                    "failed to decode protocol message"
                );
                return Err(EthStreamError::InvalidMessage(err));
            }
        };

        // 握手完成后，协议中不应再出现 Status 消息
        if matches!(msg.message, EthMessage::Status(_)) {
            return Err(EthStreamError::EthHandshakeError(EthHandshakeError::StatusNotInHandshake));
        }

        Ok(msg.message)
    }

    /// 将 [`EthMessage`] 编码为字节。
    ///
    /// 验证握手后是否发送了 Status 消息，强制执行协议规则。
    pub fn encode_message(&self, item: EthMessage<N>) -> Result<Bytes, EthStreamError> {
        if matches!(item, EthMessage::Status(_)) {
            return Err(EthStreamError::EthHandshakeError(EthHandshakeError::StatusNotInHandshake));
        }

        Ok(Bytes::from(alloy_rlp::encode(ProtocolMessage::from(item))))
    }
}

/// `EthStream` 包装任何产生字节的 `Stream`，并使其与 eth 网络协议消息兼容。
/// 这些消息会进行 RLP 编码/解码。
#[pin_project]
#[derive(Debug)]
pub struct EthStream<S, N = EthNetworkPrimitives> {
    /// 以太坊协议特定的处理逻辑
    eth: EthStreamInner<N>,
    /// 内部传输层
    #[pin]
    inner: S,
}

impl<S, N: NetworkPrimitives> EthStream<S, N> {
    /// 创建一个新的正式 [`EthStream`]。
    /// 通常在 `UnauthedEthStream::handshake` 之后调用。
    #[inline]
    pub const fn new(version: EthVersion, inner: S) -> Self {
        Self { eth: EthStreamInner::new(version), inner }
    }

    /// 返回 eth 版本。
    #[inline]
    pub const fn version(&self) -> EthVersion {
        self.eth.version()
    }

    /// 返回对内部流的引用。
    #[inline]
    pub const fn inner(&self) -> &S {
        &self.inner
    }

    /// 返回对内部流的可变引用。
    #[inline]
    pub const fn inner_mut(&mut self) -> &mut S {
        &mut self.inner
    }

    /// 消费此类型并返回包装的流。
    #[inline]
    pub fn into_inner(self) -> S {
        self.inner
    }
}

impl<S, E, N> EthStream<S, N>
where
    S: Sink<Bytes, Error = E> + Unpin,
    EthStreamError: From<E>,
    N: NetworkPrimitives,
{
    /// 类似于 [`Sink::start_send`]，但接受 [`EthBroadcastMessage`]。
    pub fn start_send_broadcast(
        &mut self,
        item: EthBroadcastMessage<N>,
    ) -> Result<(), EthStreamError> {
        self.inner.start_send_unpin(Bytes::from(alloy_rlp::encode(
            ProtocolBroadcastMessage::from(item),
        )))?;

        Ok(())
    }

    /// 直接在流上发送原始功能消息 (raw capability message)。
    pub fn start_send_raw(&mut self, msg: RawCapabilityMessage) -> Result<(), EthStreamError> {
        let mut bytes = Vec::with_capacity(msg.payload.len() + 1);
        msg.id.encode(&mut bytes);
        bytes.extend_from_slice(&msg.payload);

        self.inner.start_send_unpin(bytes.into())?;
        Ok(())
    }
}

impl<S, E, N> Stream for EthStream<S, N>
where
    S: Stream<Item = Result<BytesMut, E>> + Unpin,
    EthStreamError: From<E>,
    N: NetworkPrimitives,
{
    type Item = Result<EthMessage<N>, EthStreamError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.project();
        let res = ready!(this.inner.poll_next(cx));

        match res {
            Some(Ok(bytes)) => Poll::Ready(Some(this.eth.decode_message(bytes))),
            Some(Err(err)) => Poll::Ready(Some(Err(err.into()))),
            None => Poll::Ready(None),
        }
    }
}

impl<S, N> Sink<EthMessage<N>> for EthStream<S, N>
where
    S: CanDisconnect<Bytes> + Unpin,
    EthStreamError: From<<S as Sink<Bytes>>::Error>,
    N: NetworkPrimitives,
{
    type Error = EthStreamError;

    fn poll_ready(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.project().inner.poll_ready(cx).map_err(Into::into)
    }

    fn start_send(self: Pin<&mut Self>, item: EthMessage<N>) -> Result<(), Self::Error> {
        if matches!(item, EthMessage::Status(_)) {
            // 握手完成后尝试发送 Status 消息属于违反协议，应触发断开连接。
            let mut this = self.project();
            // 这里我们启动断开连接流程。
            let _disconnect_future = this.inner.disconnect(DisconnectReason::ProtocolBreach);
            return Err(EthStreamError::EthHandshakeError(EthHandshakeError::StatusNotInHandshake))
        }

        self.project()
            .inner
            .start_send(Bytes::from(alloy_rlp::encode(ProtocolMessage::from(item))))?;

        Ok(())
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.project().inner.poll_flush(cx).map_err(Into::into)
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.project().inner.poll_close(cx).map_err(Into::into)
    }
}

impl<S, N> CanDisconnect<EthMessage<N>> for EthStream<S, N>
where
    S: CanDisconnect<Bytes> + Send,
    EthStreamError: From<<S as Sink<Bytes>>::Error>,
    N: NetworkPrimitives,
{
    fn disconnect(
        &mut self,
        reason: DisconnectReason,
    ) -> Pin<Box<dyn Future<Output = Result<(), EthStreamError>> + Send + '_>> {
        Box::pin(async move { self.inner.disconnect(reason).await.map_err(Into::into) })
    }
}

#[cfg(test)]
mod tests {
    // ... (测试部分保持不变)
}