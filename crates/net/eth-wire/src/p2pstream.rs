use crate::{
    capability::SharedCapabilities,
    disconnect::CanDisconnect,
    errors::{P2PHandshakeError, P2PStreamError},
    pinger::{Pinger, PingerEvent},
    DisconnectReason, HelloMessage, HelloMessageWithProtocols,
};
use alloy_primitives::{
    bytes::{Buf, BufMut, Bytes, BytesMut},
    hex,
};
use alloy_rlp::{Decodable, Encodable, Error as RlpError, EMPTY_LIST_CODE};
use futures::{Sink, SinkExt, StreamExt};
use pin_project::pin_project;
use reth_codecs::add_arbitrary_tests;
use reth_metrics::metrics::counter;
use reth_primitives_traits::GotExpected;
use std::{
    collections::VecDeque,
    future::Future,
    io,
    pin::Pin,
    task::{ready, Context, Poll},
    time::Duration,
};
use tokio_stream::Stream;
use tracing::{debug, trace};

#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

/// [`MAX_PAYLOAD_SIZE`] 是未压缩消息负载的最大大小（16MB）。
/// 这是在 [EIP-706](https://eips.ethereum.org/EIPS/eip-706) 中定义的。
const MAX_PAYLOAD_SIZE: usize = 16 * 1024 * 1024;

/// [`MAX_RESERVED_MESSAGE_ID`] 是为 `p2p` 子协议保留的最大消息 ID。
/// 任何 ID 大于此值的入站消息均为子协议消息。
pub const MAX_RESERVED_MESSAGE_ID: u8 = 0x0f;

/// [`MAX_P2P_MESSAGE_ID`] 是 `p2p` 子协议正在使用的最大消息 ID。
const MAX_P2P_MESSAGE_ID: u8 = P2PMessageID::Pong as u8;

/// [`HANDSHAKE_TIMEOUT`] 确定在认定 `p2p` 握手超时之前等待的时间。
pub const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);

/// [`PING_TIMEOUT`] 确定在认定 `p2p` ping 超时之前等待的时间。
const PING_TIMEOUT: Duration = Duration::from_secs(15);

/// [`PING_INTERVAL`] 确定在对等方响应时发送 `p2p` ping 消息之间等待的时间。
const PING_INTERVAL: Duration = Duration::from_secs(60);

/// [`MAX_P2P_CAPACITY`] 是 `p2p` 流中可以缓冲发送的消息的最大数量。
///
/// 注意：这个默认值相当低，因为预期 [`P2PStream`] 会包装一个
/// [`ECIESStream`](reth_ecies::stream::ECIESStream)，后者内部已经缓冲了几 MB 的编码数据。
const MAX_P2P_CAPACITY: usize = 2;

/// 未经过身份验证的 [`P2PStream`]。在完成 `Hello` 握手后，它会被消费并返回一个 [`P2PStream`]。
#[pin_project]
#[derive(Debug)]
pub struct UnauthedP2PStream<S> {
    #[pin]
    inner: S,
}

impl<S> UnauthedP2PStream<S> {
    /// 从实现 `Stream` 和 `Sink` 的类型 `S` 创建一个新的 `UnauthedP2PStream`。
    pub const fn new(inner: S) -> Self {
        Self { inner }
    }

    /// 返回对内部流的引用。
    pub const fn inner(&self) -> &S {
        &self.inner
    }
}

impl<S> UnauthedP2PStream<S>
where
    S: Stream<Item = io::Result<BytesMut>> + Sink<Bytes, Error = io::Error> + Unpin,
{
    /// 消费 `UnauthedP2PStream`，并在 `Hello` 握手成功完成后返回 `P2PStream`。
    /// 同时返回远程对等方发送的 `Hello` 消息。
    pub async fn handshake(
        mut self,
        hello: HelloMessageWithProtocols,
    ) -> Result<(P2PStream<S>, HelloMessage), P2PStreamError> {
        trace!(?hello, "sending p2p hello to peer");

        // 使用 Sink 发送我们的 hello 消息
        self.inner.send(alloy_rlp::encode(P2PMessage::Hello(hello.message())).into()).await?;

        // 等待对方的第一个消息，带有超时机制
        let first_message_bytes = tokio::time::timeout(HANDSHAKE_TIMEOUT, self.inner.next())
            .await
            .or(Err(P2PStreamError::HandshakeError(P2PHandshakeError::Timeout)))?
            .ok_or(P2PStreamError::HandshakeError(P2PHandshakeError::NoResponse))??;

        // 检查未压缩消息长度是否超过最大负载大小。
        // 注意：第一个消息 (Hello/Disconnect) 没有经过 snappy 压缩。
        // 握手后的后续消息将再次检查解压缩后的长度。
        if first_message_bytes.len() > MAX_PAYLOAD_SIZE {
            return Err(P2PStreamError::MessageTooBig {
                message_size: first_message_bytes.len(),
                max_size: MAX_PAYLOAD_SIZE,
            })
        }

        // 发送的第一个消息必须是 hello 或 disconnect 消息
        //
        // 如果第一个消息是 disconnect，我们不应该使用 Decodable::decode 解码，
        // 因为第一个消息（无论 Disconnect 还是 Hello）都没有经过 snappy 压缩，
        // 而 Decodable 实现假设非 hello 消息都经过了 snappy 压缩。
        let their_hello = match P2PMessage::decode(&mut &first_message_bytes[..]) {
            Ok(P2PMessage::Hello(hello)) => Ok(hello),
            Ok(P2PMessage::Disconnect(reason)) => {
                if matches!(reason, DisconnectReason::TooManyPeers) {
                    // TooManyPeers 是非常常见的断开连接原因，避免在 DEBUG 日志中产生太多干扰
                    trace!(%reason, "Disconnected by peer during handshake");
                } else {
                    debug!(%reason, "Disconnected by peer during handshake");
                };
                counter!("p2pstream.disconnected_errors").increment(1);
                Err(P2PStreamError::HandshakeError(P2PHandshakeError::Disconnected(reason)))
            }
            Err(err) => {
                debug!(%err, msg=%hex::encode(&first_message_bytes), "Failed to decode first message from peer");
                Err(P2PStreamError::HandshakeError(err.into()))
            }
            Ok(msg) => {
                debug!(?msg, "expected hello message but received another message");
                Err(P2PStreamError::HandshakeError(P2PHandshakeError::NonHelloMessageInHandshake))
            }
        }?;

        trace!(
            hello=?their_hello,
            "validating incoming p2p hello from peer"
        );

        // 验证协议版本是否一致
        if (hello.protocol_version as u8) != their_hello.protocol_version as u8 {
            // 发送断开连接消息通知对方协议版本不匹配
            self.send_disconnect(DisconnectReason::IncompatibleP2PProtocolVersion).await?;
            return Err(P2PStreamError::MismatchedProtocolVersion(GotExpected {
                got: their_hello.protocol_version,
                expected: hello.protocol_version,
            }))
        }

        // 确定共享能力 (目前仅返回一个共享能力)
        let capability_res =
            SharedCapabilities::try_new(hello.protocols, their_hello.capabilities.clone());

        let shared_capability = match capability_res {
            Err(err) => {
                // 如果没有共享能力，发送断开连接消息
                self.send_disconnect(DisconnectReason::UselessPeer).await?;
                Err(err)
            }
            Ok(cap) => Ok(cap),
        }?;

        let stream = P2PStream::new(self.inner, shared_capability);

        Ok((stream, their_hello))
    }
}

impl<S> UnauthedP2PStream<S>
where
    S: Sink<Bytes, Error = io::Error> + Unpin,
{
    /// 在握手期间发送断开连接消息。此消息发送时不使用 snappy 压缩。
    pub async fn send_disconnect(
        &mut self,
        reason: DisconnectReason,
    ) -> Result<(), P2PStreamError> {
        trace!(
            %reason,
            "Sending disconnect message during the handshake",
        );
        self.inner
            .send(Bytes::from(alloy_rlp::encode(P2PMessage::Disconnect(reason))))
            .await
            .map_err(P2PStreamError::Io)
    }
}

impl<S> CanDisconnect<Bytes> for P2PStream<S>
where
    S: Sink<Bytes, Error = io::Error> + Unpin + Send + Sync,
{
    fn disconnect(
        &mut self,
        reason: DisconnectReason,
    ) -> Pin<Box<dyn Future<Output = Result<(), P2PStreamError>> + Send + '_>> {
        Box::pin(async move { self.disconnect(reason).await })
    }
}

/// `P2PStream` 包装任何产生字节的 `Stream`，并使其与 `p2p` 协议消息兼容。
///
/// 该流支持在握手期间协商的多个共享能力。
///
/// ### 基于消息 ID 的多路复用 (Message-ID based multiplexing)
///
/// > 每个功能（capability）根据其需要被分配一定范围的消息 ID 空间。所有此类功能必须静态指定它们需要的消息 ID 数量。
/// > 在连接和接收到 Hello 消息时，双方都拥有关于它们共享哪些功能（包括版本）的等效信息，
/// > 并能够对消息 ID 空间的构成达成共识。
///
/// > 消息 ID 被假定为从 ID 0x10 开始紧凑分配（0x00-0x0f 保留给 "p2p" 功能），
/// > 并按字母顺序分配给每个共享（版本相同、名称相同）的功能。功能名称区分大小写。
/// > 未共享的功能将被忽略。如果同一个（名称相同）功能共享多个版本，则数值最高的版本获胜，其他版本被忽略。
///
/// 详见 <https://github.com/ethereum/devp2p/blob/master/rlpx.md#message-id-based-multiplexing>
///
/// 此流发出 _非空_ 的 Bytes，这些字节以标准化的消息 ID 开始，因此每个消息的第一字节从 0 开始。
/// 如果此流仅支持单个功能（例如 `eth`），则每个消息的第一字节将与
/// [EthMessageID](reth_eth_wire_types::message::EthMessageID) 匹配。
#[pin_project]
#[derive(Debug)]
pub struct P2PStream<S> {
    /// 内部传输流
    #[pin]
    inner: S,

    /// 用于压缩传出消息的 snappy 编码器
    encoder: snap::raw::Encoder,

    /// 用于解压传入消息的 snappy 解码器
    decoder: snap::raw::Decoder,

    /// 用于跟踪对等方 ping 状态的状态机
    pinger: Pinger,

    /// 此流支持的共享能力
    shared_capabilities: SharedCapabilities,

    /// 缓冲的待发送传出消息
    outgoing_messages: VecDeque<Bytes>,

    /// 在 [Sink] 实现返回 [`Poll::Pending`] 之前可以缓冲的消息最大数量
    outgoing_message_buffer_capacity: usize,

    /// 当前流是否正处于通过发送断开连接消息来断开连接的过程中
    disconnecting: bool,
}

impl<S> P2PStream<S> {
    /// 从提供的流创建新的 [`P2PStream`]。
    /// 假定新的 [`P2PStream`] 已成功完成 `p2p` 握手，并准备好发送和接收子协议消息。
    pub fn new(inner: S, shared_capabilities: SharedCapabilities) -> Self {
        Self {
            inner,
            encoder: snap::raw::Encoder::new(),
            decoder: snap::raw::Decoder::new(),
            pinger: Pinger::new(PING_INTERVAL, PING_TIMEOUT),
            shared_capabilities,
            outgoing_messages: VecDeque::new(),
            outgoing_message_buffer_capacity: MAX_P2P_CAPACITY,
            disconnecting: false,
        }
    }

    /// 返回对内部流的引用。
    pub const fn inner(&self) -> &S {
        &self.inner
    }

    /// 设置自定义的传出消息缓冲区容量。
    ///
    /// # Panics
    ///
    /// 如果提供的容量为 `0`。
    pub const fn set_outgoing_message_buffer_capacity(&mut self, capacity: usize) {
        self.outgoing_message_buffer_capacity = capacity;
    }

    /// 返回此流的共享能力。
    ///
    /// 这包括握手期间协商的所有共享能力及其基于每个功能消息数量的偏移量。
    pub const fn shared_capabilities(&self) -> &SharedCapabilities {
        &self.shared_capabilities
    }

    /// 如果流仍有传出容量，则返回 `true`。
    fn has_outgoing_capacity(&self) -> bool {
        self.outgoing_messages.len() < self.outgoing_message_buffer_capacity
    }

    /// 将 _snappy_ 编码的 [`P2PMessage::Pong`] 消息放入队列。
    fn send_pong(&mut self) {
        self.outgoing_messages.push_back(Bytes::from(alloy_rlp::encode(P2PMessage::Pong)));
    }

    /// 将 _snappy_ 编码 of [`P2PMessage::Ping`] 消息放入队列。
    pub fn send_ping(&mut self) {
        self.outgoing_messages.push_back(Bytes::from(alloy_rlp::encode(P2PMessage::Ping)));
    }
}

/// 通过发送断开连接消息并停止读取新消息来优雅地断开连接。
pub trait DisconnectP2P {
    /// 开始优雅地断开连接。
    fn start_disconnect(&mut self, reason: DisconnectReason) -> Result<(), P2PStreamError>;

    /// 如果连接即将断开，则返回 `true`。
    fn is_disconnecting(&self) -> bool;
}

impl<S> DisconnectP2P for P2PStream<S> {
    /// 开始通过发送断开连接消息并停止读取新消息来优雅地断开连接。
    ///
    /// 一旦断开连接过程开始，[`Stream`] 将立即终止。
    ///
    /// # Errors
    ///
    /// 仅当消息压缩失败时返回错误。
    fn start_disconnect(&mut self, reason: DisconnectReason) -> Result<(), P2PStreamError> {
        // 清除任何缓冲的消息并放入队列
        self.outgoing_messages.clear();
        let disconnect = P2PMessage::Disconnect(reason);
        let mut buf = Vec::with_capacity(disconnect.length());
        disconnect.encode(&mut buf);

        let mut compressed = vec![0u8; 1 + snap::raw::max_compress_len(buf.len() - 1)];
        let compressed_size =
            self.encoder.compress(&buf[1..], &mut compressed[1..]).map_err(|err| {
                debug!(
                    %err,
                    msg=%hex::encode(&buf[1..]),
                    "error compressing disconnect"
                );
                err
            })?;

        // 将压缩缓冲区截断为实际压缩大小 (加上消息 ID 的一个字节)
        compressed.truncate(compressed_size + 1);

        // 我们不添加功能偏移量，因为断开连接消息是 `p2p` 保留消息
        compressed[0] = buf[0];

        self.outgoing_messages.push_back(compressed.into());
        self.disconnecting = true;
        Ok(())
    }

    fn is_disconnecting(&self) -> bool {
        self.disconnecting
    }
}

impl<S> P2PStream<S>
where
    S: Sink<Bytes, Error = io::Error> + Unpin + Send,
{
    /// 通过发送断开连接消息来断开连接。
    ///
    /// 此 future 在发送完断开连接消息且流关闭后解析。
    pub async fn disconnect(&mut self, reason: DisconnectReason) -> Result<(), P2PStreamError> {
        self.start_disconnect(reason)?;
        self.close().await
    }
}

// S 也必须是 `Sink`，因为我们需要能够回复 ping 消息以遵循协议
impl<S> Stream for P2PStream<S>
where
    S: Stream<Item = io::Result<BytesMut>> + Sink<Bytes, Error = io::Error> + Unpin,
{
    type Item = Result<BytesMut, P2PStreamError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();

        if this.disconnecting {
            // 如果正在断开连接，停止读取消息
            return Poll::Ready(None)
        }

        // 我们应该在这里循环，以确保如果我们要返回的消息之后还有任何需要响应的 ping，我们不会返回 Poll::Pending
        while let Poll::Ready(res) = this.inner.poll_next_unpin(cx) {
            let bytes = match res {
                Some(Ok(bytes)) => bytes,
                Some(Err(err)) => return Poll::Ready(Some(Err(err.into()))),
                None => return Poll::Ready(None),
            };

            if bytes.is_empty() {
                // 不允许空消息
                return Poll::Ready(Some(Err(P2PStreamError::EmptyProtocolMessage)))
            }

            // 首先解码断开连接原因，因为它们在线缆上可能以多种形式编码，
            // 包括经过 snappy 压缩和未压缩的形式。
            let id = bytes[0];
            if id == P2PMessageID::Disconnect as u8 {
                // 我们不能在这里处理错误，因为断开连接原因在网络上编码为：
                // * snappy 压缩，且
                // * 未压缩
                //
                // 如果解码成功，我们已经检查了 ID 并知道这是一个断开连接消息，所以我们可以返回原因。
                // 如果解码失败，我们继续，如果消息经过 snappy 压缩，我们将尝试再次解码。
                if let Ok(reason) = DisconnectReason::decode(&mut &bytes[1..]) {
                    return Poll::Ready(Some(Err(P2PStreamError::Disconnected(reason))))
                }
            }

            // 首先检查压缩消息长度是否超过最大负载大小
            let decompressed_len = snap::raw::decompress_len(&bytes[1..])?;
            if decompressed_len > MAX_PAYLOAD_SIZE {
                return Poll::Ready(Some(Err(P2PStreamError::MessageTooBig {
                    message_size: decompressed_len,
                    max_size: MAX_PAYLOAD_SIZE,
                })))
            }

            // 创建一个缓冲区来保存解压缩后的消息，长度增加一个字节用于存放消息 ID
            let mut decompress_buf = BytesMut::zeroed(decompressed_len + 1);

            // 成功握手后的每条消息都经过 snappy 压缩，因此我们需要在解码之前解压消息
            this.decoder.decompress(&bytes[1..], &mut decompress_buf[1..]).map_err(|err| {
                debug!(
                    %err,
                    msg=%hex::encode(&bytes[1..]),
                    "error decompressing p2p message"
                );
                err
            })?;

            match id {
                _ if id == P2PMessageID::Ping as u8 => {
                    trace!("Received Ping, Sending Pong");
                    this.send_pong();
                    // 这是必需的，因为 Sink 可能不会在外部被轮询，如果发生这种情况，pong 将永远不会被发送。
                    cx.waker().wake_by_ref();
                }
                _ if id == P2PMessageID::Hello as u8 => {
                    // 我们在握手之外收到了 hello 消息，因此返回错误
                    return Poll::Ready(Some(Err(P2PStreamError::HandshakeError(
                        P2PHandshakeError::HelloNotInHandshake,
                    ))))
                }
                _ if id == P2PMessageID::Pong as u8 => {
                    // 如果我们正在等待 pong，这将重置 pinger 状态
                    this.pinger.on_pong()?
                }
                _ if id == P2PMessageID::Disconnect as u8 => {
                    // 此时 decompress_buf 包含经过 snappy 解压的断开连接消息。
                    // 有可能我们之前尝试过 RLP 解码，但它是经过 snappy 压缩的，所以我们需要再次进行 RLP 解码。
                    let reason = DisconnectReason::decode(&mut &decompress_buf[1..]).inspect_err(|err| {
                        debug!(
                            %err, msg=%hex::encode(&decompress_buf[1..]), "Failed to decode disconnect message from peer"
                        );
                    })?;
                    return Poll::Ready(Some(Err(P2PStreamError::Disconnected(reason))))
                }
                _ if id > MAX_P2P_MESSAGE_ID && id <= MAX_RESERVED_MESSAGE_ID => {
                    // 我们收到了一条未知的保留消息
                    return Poll::Ready(Some(Err(P2PStreamError::UnknownReservedMessageId(id))))
                }
                _ => {
                    // 我们收到了一条处于 `p2p` 保留消息空间之外的消息，因此它是子协议消息。

                    // 对等方必须能够使用单个消息 ID 字节识别针对不同子协议的消息，
                    // 并且这些消息必须与底层的 `p2p` 消息区分开来。
                    //
                    // 为确保子协议的消息与针对 `p2p` 功能的消息区分开来，
                    // 消息 ID 0x00 - 0x0f 被保留给 `p2p` 消息，
                    // 因此子协议消息的 ID 必须为 0x10 或更高。
                    //
                    // 为确保两个不同功能的消息彼此区分开来，所有共享功能首先按字典序排序。
                    // 然后按此顺序从 0x10 开始保留消息 ID，为功能支持的每个消息保留一个消息 ID。
                    //
                    // 例如，如果共享功能是 `eth/67` (包含 10 条消息) 和 "qrs/65" (包含 8 条消息):
                    //
                    //  * `p2p` 的特殊情况：`p2p` 保留消息 ID 0x00 - 0x0f。
                    //  * `eth/67` 保留消息 ID 0x10 - 0x19。
                    //  * `qrs/65` 保留消息 ID 0x1a - 0x21。
                    //
                    // 这里我们将 ID 重新映射到从 0 开始，通过减去保留范围的长度。
                    decompress_buf[0] = bytes[0] - MAX_RESERVED_MESSAGE_ID - 1;

                    return Poll::Ready(Some(Ok(decompress_buf)))
                }
            }
        }

        Poll::Pending
    }
}

impl<S> Sink<Bytes> for P2PStream<S>
where
    S: Sink<Bytes, Error = io::Error> + Unpin,
{
    type Error = P2PStreamError;

    fn poll_ready(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let mut this = self.as_mut();

        // 轮询 pinger 以确定是否应发送 ping
        match this.pinger.poll_ping(cx) {
            Poll::Pending => {}
            Poll::Ready(Ok(PingerEvent::Ping)) => {
                this.send_ping();
            }
            _ => {
                // 编码断开连接消息
                this.start_disconnect(DisconnectReason::PingTimeout)?;

                // ping 相关错误后结束流
                return Poll::Ready(Ok(()))
            }
        }

        match this.inner.poll_ready_unpin(cx) {
            Poll::Pending => {}
            Poll::Ready(Err(err)) => return Poll::Ready(Err(P2PStreamError::Io(err))),
            Poll::Ready(Ok(())) => {
                let flushed = this.poll_flush(cx);
                if flushed.is_ready() {
                    return flushed
                }
            }
        }

        if self.has_outgoing_capacity() {
            // 仍有容量
            Poll::Ready(Ok(()))
        } else {
            Poll::Pending
        }
    }

    fn start_send(self: Pin<&mut Self>, item: Bytes) -> Result<(), Self::Error> {
        if item.len() > MAX_PAYLOAD_SIZE {
            return Err(P2PStreamError::MessageTooBig {
                message_size: item.len(),
                max_size: MAX_PAYLOAD_SIZE,
            })
        }

        if item.is_empty() {
            // 不允许空消息
            return Err(P2PStreamError::EmptyProtocolMessage)
        }

        // 确保有空闲容量
        if !self.has_outgoing_capacity() {
            return Err(P2PStreamError::SendBufferFull)
        }

        let this = self.project();

        let mut compressed = BytesMut::zeroed(1 + snap::raw::max_compress_len(item.len() - 1));
        let compressed_size =
            this.encoder.compress(&item[1..], &mut compressed[1..]).map_err(|err| {
                debug!(
                    %err,
                    msg=%hex::encode(&item[1..]),
                    "error compressing p2p message"
                );
                err
            })?;

        // 将压缩缓冲区截断为实际压缩大小 (加上消息 ID 的一个字节)
        compressed.truncate(compressed_size + 1);

        // 此流中发送的所有消息都是子协议消息，因此我们需要根据偏移量切换消息 ID
        compressed[0] = item[0] + MAX_RESERVED_MESSAGE_ID + 1;
        this.outgoing_messages.push_back(compressed.freeze());

        Ok(())
    }

    /// 当没有保留的缓冲项时返回 `Poll::Ready(Ok(()))`。
    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let mut this = self.project();
        let poll_res = loop {
            match this.inner.as_mut().poll_ready(cx) {
                Poll::Pending => break Poll::Pending,
                Poll::Ready(Err(err)) => break Poll::Ready(Err(err.into())),
                Poll::Ready(Ok(())) => {
                    let Some(message) = this.outgoing_messages.pop_front() else {
                        break Poll::Ready(Ok(()))
                    };
                    if let Err(err) = this.inner.as_mut().start_send(message) {
                        break Poll::Ready(Err(err.into()))
                    }
                }
            }
        };

        ready!(this.inner.as_mut().poll_flush(cx))?;

        poll_res
    }

    fn poll_close(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        ready!(self.as_mut().poll_flush(cx))?;
        ready!(self.project().inner.poll_close(cx))?;

        Poll::Ready(Ok(()))
    }
}

/// 仅表示保留的 `p2p` 子协议消息。
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(any(test, feature = "arbitrary"), derive(arbitrary::Arbitrary))]
#[add_arbitrary_tests(rlp)]
pub enum P2PMessage {
    /// 连接上发送的第一个数据包，双方各发送一次。
    Hello(HelloMessage),

    /// 通知对等方即将断开连接；如果收到，对等方应立即断开连接。
    Disconnect(DisconnectReason),

    /// 请求对等方立即回复 [`P2PMessage::Pong`]。
    Ping,

    /// 回复对等方的 [`P2PMessage::Ping`] 数据包。
    Pong,
}

impl P2PMessage {
    /// 获取给定消息的 [`P2PMessageID`]。
    pub const fn message_id(&self) -> P2PMessageID {
        match self {
            Self::Hello(_) => P2PMessageID::Hello,
            Self::Disconnect(_) => P2PMessageID::Disconnect,
            Self::Ping => P2PMessageID::Ping,
            Self::Pong => P2PMessageID::Pong,
        }
    }
}

impl Encodable for P2PMessage {
    /// [`P2PMessage::Ping`] 和 [`P2PMessage::Pong`] 的 [`Encodable`] 实现将消息编码为 RLP，
    /// 并在 RLP 字节前添加 snappy 报头。除 [`P2PMessage::Hello`] 外的所有变体均如此，
    /// 因为 hello 消息在 `p2p` 子协议中从不压缩。
    fn encode(&self, out: &mut dyn BufMut) {
        (self.message_id() as u8).encode(out);
        match self {
            Self::Hello(msg) => msg.encode(out),
            Self::Disconnect(msg) => msg.encode(out),
            Self::Ping => {
                // Ping 负载总是 snappy 编码的
                out.put_u8(0x01);
                out.put_u8(0x00);
                out.put_u8(EMPTY_LIST_CODE);
            }
            Self::Pong => {
                // Pong 负载总是 snappy 编码的
                out.put_u8(0x01);
                out.put_u8(0x00);
                out.put_u8(EMPTY_LIST_CODE);
            }
        }
    }

    fn length(&self) -> usize {
        let payload_len = match self {
            Self::Hello(msg) => msg.length(),
            Self::Disconnect(msg) => msg.length(),
            // id + snappy 编码的负载
            Self::Ping | Self::Pong => 3, // len([0x01, 0x00, 0xc0]) = 3
        };
        payload_len + 1 // (1 用于 p2p 消息 ID 的长度)
    }
}

impl Decodable for P2PMessage {
    /// [`P2PMessage`] 的 [`Decodable`] 实现假定每个消息变体都经过了 snappy 压缩，
    /// 但 [`P2PMessage::Hello`] 变体除外，因为 hello 消息在 `p2p` 子协议中从不压缩。
    ///
    /// [`P2PMessage::Ping`] 和 [`P2PMessage::Pong`] 的 [`Decodable`] 实现预期有一个 snappy 编码的负载，
    /// 参见 [`Encodable`] 实现。
    fn decode(buf: &mut &[u8]) -> alloy_rlp::Result<Self> {
        /// 从 Ping/Pong 缓冲区移除 snappy 前缀
        fn advance_snappy_ping_pong_payload(buf: &mut &[u8]) -> alloy_rlp::Result<()> {
            if buf.len() < 3 {
                return Err(RlpError::InputTooShort)
            }
            if buf[..3] != [0x01, 0x00, EMPTY_LIST_CODE] {
                return Err(RlpError::Custom("expected snappy payload"))
            }
            buf.advance(3);
            Ok(())
        }

        let message_id = u8::decode(&mut &buf[..])?;
        let id = P2PMessageID::try_from(message_id)
            .or(Err(RlpError::Custom("unknown p2p message id")))?;
        buf.advance(1);
        match id {
            P2PMessageID::Hello => Ok(Self::Hello(HelloMessage::decode(buf)?)),
            P2PMessageID::Disconnect => Ok(Self::Disconnect(DisconnectReason::decode(buf)?)),
            P2PMessageID::Ping => {
                advance_snappy_ping_pong_payload(buf)?;
                Ok(Self::Ping)
            }
            P2PMessageID::Pong => {
                advance_snappy_ping_pong_payload(buf)?;
                Ok(Self::Pong)
            }
        }
    }
}

/// `p2p` 子协议消息的消息 ID。
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum P2PMessageID {
    /// [`P2PMessage::Hello`] 消息的消息 ID。
    Hello = 0x00,

    /// [`P2PMessage::Disconnect`] 消息的消息 ID。
    Disconnect = 0x01,

    /// [`P2PMessage::Ping`] 消息的消息 ID。
    Ping = 0x02,

    /// [`P2PMessage::Pong`] 消息的消息 ID。
    Pong = 0x03,
}

impl From<P2PMessage> for P2PMessageID {
    fn from(msg: P2PMessage) -> Self {
        match msg {
            P2PMessage::Hello(_) => Self::Hello,
            P2PMessage::Disconnect(_) => Self::Disconnect,
            P2PMessage::Ping => Self::Ping,
            P2PMessage::Pong => Self::Pong,
        }
    }
}

impl TryFrom<u8> for P2PMessageID {
    type Error = P2PStreamError;

    fn try_from(id: u8) -> Result<Self, Self::Error> {
        match id {
            0x00 => Ok(Self::Hello),
            0x01 => Ok(Self::Disconnect),
            0x02 => Ok(Self::Ping),
            0x03 => Ok(Self::Pong),
            _ => Err(P2PStreamError::UnknownReservedMessageId(id)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{capability::SharedCapability, test_utils::eth_hello, EthVersion, ProtocolVersion};
    use tokio::net::{TcpListener, TcpStream};
    use tokio_util::codec::Decoder;

    #[tokio::test]
    async fn test_can_disconnect() {
        reth_tracing::init_test_tracing();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let local_addr = listener.local_addr().unwrap();

        let expected_disconnect = DisconnectReason::UselessPeer;

        let handle = tokio::spawn(async move {
            // roughly based off of the design of tokio::net::TcpListener
            let (incoming, _) = listener.accept().await.unwrap();
            let stream = crate::PassthroughCodec::default().framed(incoming);

            let (server_hello, _) = eth_hello();

            let (mut p2p_stream, _) =
                UnauthedP2PStream::new(stream).handshake(server_hello).await.unwrap();

            p2p_stream.disconnect(expected_disconnect).await.unwrap();
        });

        let outgoing = TcpStream::connect(local_addr).await.unwrap();
        let sink = crate::PassthroughCodec::default().framed(outgoing);

        let (client_hello, _) = eth_hello();

        let (mut p2p_stream, _) =
            UnauthedP2PStream::new(sink).handshake(client_hello).await.unwrap();

        let err = p2p_stream.next().await.unwrap().unwrap_err();
        match err {
            P2PStreamError::Disconnected(reason) => assert_eq!(reason, expected_disconnect),
            e => panic!("unexpected err: {e}"),
        }

        handle.await.unwrap();
    }

    #[tokio::test]
    async fn test_can_disconnect_weird_disconnect_encoding() {
        reth_tracing::init_test_tracing();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let local_addr = listener.local_addr().unwrap();

        let expected_disconnect = DisconnectReason::SubprotocolSpecific;

        let handle = tokio::spawn(async move {
            // roughly based off of the design of tokio::net::TcpListener
            let (incoming, _) = listener.accept().await.unwrap();
            let stream = crate::PassthroughCodec::default().framed(incoming);

            let (server_hello, _) = eth_hello();

            let (mut p2p_stream, _) =
                UnauthedP2PStream::new(stream).handshake(server_hello).await.unwrap();

            // Unrolled `disconnect` method, without compression
            p2p_stream.outgoing_messages.clear();

            p2p_stream.outgoing_messages.push_back(Bytes::from(alloy_rlp::encode(
                P2PMessage::Disconnect(DisconnectReason::SubprotocolSpecific),
            )));
            p2p_stream.disconnecting = true;
            p2p_stream.close().await.unwrap();
        });

        let outgoing = TcpStream::connect(local_addr).await.unwrap();
        let sink = crate::PassthroughCodec::default().framed(outgoing);

        let (client_hello, _) = eth_hello();

        let (mut p2p_stream, _) =
            UnauthedP2PStream::new(sink).handshake(client_hello).await.unwrap();

        let err = p2p_stream.next().await.unwrap().unwrap_err();
        match err {
            P2PStreamError::Disconnected(reason) => assert_eq!(reason, expected_disconnect),
            e => panic!("unexpected err: {e}"),
        }

        handle.await.unwrap();
    }

    #[tokio::test]
    async fn test_handshake_passthrough() {
        // create a p2p stream and server, then confirm that the two are authed
        // create tcpstream
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let local_addr = listener.local_addr().unwrap();

        let handle = tokio::spawn(async move {
            // roughly based off of the design of tokio::net::TcpListener
            let (incoming, _) = listener.accept().await.unwrap();
            let stream = crate::PassthroughCodec::default().framed(incoming);

            let (server_hello, _) = eth_hello();

            let unauthed_stream = UnauthedP2PStream::new(stream);
            let (p2p_stream, _) = unauthed_stream.handshake(server_hello).await.unwrap();

            // ensure that the two share a single capability, eth67
            assert_eq!(
                *p2p_stream.shared_capabilities.iter_caps().next().unwrap(),
                SharedCapability::Eth {
                    version: EthVersion::Eth67,
                    offset: MAX_RESERVED_MESSAGE_ID + 1
                }
            );
        });

        let outgoing = TcpStream::connect(local_addr).await.unwrap();
        let sink = crate::PassthroughCodec::default().framed(outgoing);

        let (client_hello, _) = eth_hello();

        let unauthed_stream = UnauthedP2PStream::new(sink);
        let (p2p_stream, _) = unauthed_stream.handshake(client_hello).await.unwrap();

        // ensure that the two share a single capability, eth67
        assert_eq!(
            *p2p_stream.shared_capabilities.iter_caps().next().unwrap(),
            SharedCapability::Eth {
                version: EthVersion::Eth67,
                offset: MAX_RESERVED_MESSAGE_ID + 1
            }
        );

        // make sure the server receives the message and asserts before ending the test
        handle.await.unwrap();
    }

    #[tokio::test]
    async fn test_handshake_disconnect() {
        // create a p2p stream and server, then confirm that the two are authed
        // create tcpstream
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let local_addr = listener.local_addr().unwrap();

        let handle = tokio::spawn(Box::pin(async move {
            // roughly based off of the design of tokio::net::TcpListener
            let (incoming, _) = listener.accept().await.unwrap();
            let stream = crate::PassthroughCodec::default().framed(incoming);

            let (server_hello, _) = eth_hello();

            let unauthed_stream = UnauthedP2PStream::new(stream);
            match unauthed_stream.handshake(server_hello.clone()).await {
                Ok((_, hello)) => {
                    panic!("expected handshake to fail, instead got a successful Hello: {hello:?}")
                }
                Err(P2PStreamError::MismatchedProtocolVersion(GotExpected { got, expected })) => {
                    assert_ne!(expected, got);
                    assert_eq!(expected, server_hello.protocol_version);
                }
                Err(other_err) => {
                    panic!("expected mismatched protocol version error, got {other_err:?}")
                }
            }
        }));

        let outgoing = TcpStream::connect(local_addr).await.unwrap();
        let sink = crate::PassthroughCodec::default().framed(outgoing);

        let (mut client_hello, _) = eth_hello();

        // modify the hello to include an incompatible p2p protocol version
        client_hello.protocol_version = ProtocolVersion::V4;

        let unauthed_stream = UnauthedP2PStream::new(sink);
        match unauthed_stream.handshake(client_hello.clone()).await {
            Ok((_, hello)) => {
                panic!("expected handshake to fail, instead got a successful Hello: {hello:?}")
            }
            Err(P2PStreamError::MismatchedProtocolVersion(GotExpected { got, expected })) => {
                assert_ne!(expected, got);
                assert_eq!(expected, client_hello.protocol_version);
            }
            Err(other_err) => {
                panic!("expected mismatched protocol version error, got {other_err:?}")
            }
        }

        // make sure the server receives the message and asserts before ending the test
        handle.await.unwrap();
    }

    #[test]
    fn snappy_decode_encode_ping() {
        let snappy_ping = b"\x02\x01\0\xc0";
        let ping = P2PMessage::decode(&mut &snappy_ping[..]).unwrap();
        assert!(matches!(ping, P2PMessage::Ping));
        assert_eq!(alloy_rlp::encode(ping), &snappy_ping[..]);
    }

    #[test]
    fn snappy_decode_encode_pong() {
        let snappy_pong = b"\x03\x01\0\xc0";
        let pong = P2PMessage::decode(&mut &snappy_pong[..]).unwrap();
        assert!(matches!(pong, P2PMessage::Pong));
        assert_eq!(alloy_rlp::encode(pong), &snappy_pong[..]);
    }
}