# eth-wire

`eth-wire` crate 为 [`RLPx`](https://github.com/ethereum/devp2p/blob/master/rlpx.md) 与
[Eth wire](https://github.com/ethereum/devp2p/blob/master/caps/eth.md) 协议提供了抽象。

可以把这个 crate 理解为包含两个部分：

1. 把以太坊协议消息序列化/反序列化成 Rust 可用类型的数据结构。
2. 基于 Tokio Streams 的流式抽象，让这些类型可以在网络流上收发。

（注意：ECIES 的实现位于单独的 `reth-ecies` crate 中。）
此外，这个 crate 重点关注流实现（P2P 与 Eth）、握手（handshake）与复用（multiplexing）。协议消息类型以及 RLP 编解码逻辑位于独立的 `eth-wire-types` crate 中；为了使用方便，`eth-wire` 会将其重新导出（re-export）。

## 类型（Types）

最基础的 Eth-wire 类型是 `ProtocolMessage`。它描述了 reth 在 eth 子协议中可以发送/接收的所有消息。

[文件：crates/net/eth-wire-types/src/message.rs](../../crates/net/eth-wire-types/src/message.rs)

```rust, ignore
/// An `eth` protocol message, containing a message ID and payload.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProtocolMessage<N: NetworkPrimitives = EthNetworkPrimitives> {
    pub message_type: EthMessageID,
    pub message: EthMessage<N>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum EthMessage<N: NetworkPrimitives = EthNetworkPrimitives> {
    Status(StatusMessage),
    NewBlockHashes(NewBlockHashes),
    NewBlock(Box<N::NewBlockPayload>),
    Transactions(Transactions<N::BroadcastedTransaction>),
    NewPooledTransactionHashes66(NewPooledTransactionHashes66),
    NewPooledTransactionHashes68(NewPooledTransactionHashes68),
    GetBlockHeaders(RequestPair<GetBlockHeaders>),
    BlockHeaders(RequestPair<BlockHeaders<N::BlockHeader>>),
    GetBlockBodies(RequestPair<GetBlockBodies>),
    BlockBodies(RequestPair<BlockBodies<N::BlockBody>>),
    GetPooledTransactions(RequestPair<GetPooledTransactions>),
    PooledTransactions(RequestPair<PooledTransactions<N::PooledTransaction>>),
    GetNodeData(RequestPair<GetNodeData>),
    NodeData(RequestPair<NodeData>),
    GetReceipts(RequestPair<GetReceipts>),
    GetReceipts70(RequestPair<GetReceipts70>),
    Receipts(RequestPair<Receipts<N::Receipt>>),
    Receipts69(RequestPair<Receipts69<N::Receipt>>),
    Receipts70(RequestPair<Receipts70<N::Receipt>>),
    BlockRangeUpdate(BlockRangeUpdate),
    Other(RawCapabilityMessage),
}

/// Represents message IDs for eth protocol messages.
#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EthMessageID {
    Status = 0x00,
    NewBlockHashes = 0x01,
    Transactions = 0x02,
    GetBlockHeaders = 0x03,
    BlockHeaders = 0x04,
    GetBlockBodies = 0x05,
    BlockBodies = 0x06,
    NewBlock = 0x07,
    NewPooledTransactionHashes = 0x08,
    GetPooledTransactions = 0x09,
    PooledTransactions = 0x0a,
    GetNodeData = 0x0d,
    NodeData = 0x0e,
    GetReceipts = 0x0f,
    Receipts = 0x10,
    BlockRangeUpdate = 0x11,
    Other(u8),
}

```

消息既可以被广播到网络，也可以作为“请求/响应”消息与单个 peer 交互。后一种消息使用 `RequestPair` 来描述：它本质上就是“消息 + request id”的组合。

[文件：crates/net/eth-wire-types/src/message.rs](../../crates/net/eth-wire-types/src/message.rs)

```rust, ignore
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RequestPair<T> {
    pub request_id: u64,
    pub message: T,
}

```

每一种 `EthMessage` 都有一个对应的 Rust 结构体，并实现了 `alloy_rlp::Encodable` 与 `alloy_rlp::Decodable`（通常通过 `RlpEncodable`/`RlpDecodable` 等 derive 宏实现）。这些 trait 定义在 `alloy_rlp` 中：

```rust, ignore
pub trait Decodable: Sized {
    fn decode(buf: &mut &[u8]) -> alloy_rlp::Result<Self>;
}
pub trait Encodable {
    fn encode(&self, out: &mut dyn BufMut);
    fn length(&self) -> usize;
}
```

这些 trait 描述了如何使用 RLP 格式把 `EthMessage` 序列化/反序列化成原始字节。在 reth 中，所有 [RLP](https://ethereum.org/en/developers/docs/data-structures-and-encoding/rlp/) 编解码操作都由 `alloy_rlp` 以及 `eth-wire-types` 中使用的 derive 宏来完成。

注意：`ProtocolMessage` 实现了 `Encodable`；而解码则通过 `ProtocolMessage::decode_message(version, &mut bytes)` 完成，因为解码必须遵循握手协商得到的 `EthVersion`。

### 示例：Transactions 消息

为了理解 `EthMessage` 是如何被实现的，我们看一下 `Transactions` 消息。eth 规范将 Transaction 消息描述为一个 RLP 编码交易列表：

[文件：ethereum/devp2p/caps/eth.md](https://github.com/ethereum/devp2p/blob/master/caps/eth.md#transactions-0x02)

```
Transactions (0x02)
[tx₁, tx₂, ...]

Specify transactions that the peer should make sure are included in its transaction queue.
The items in the list are transactions in the format described in the main Ethereum specification.
...

```

在 reth 中，它被表示为：

[文件：crates/net/eth-wire-types/src/broadcast.rs](../../crates/net/eth-wire-types/src/broadcast.rs)

```rust,ignore
pub struct Transactions<T = TransactionSigned>(
    /// New transactions for the peer to include in its mempool.
    pub Vec<T>,
);
```

对应的交易类型定义在这里：

[文件：crates/ethereum/primitives/src/transaction.rs](../../crates/ethereum/primitives/src/transaction.rs)

```rust, ignore
#[reth_codec]
#[derive(Debug, Clone, PartialEq, Eq, Hash, AsRef, Deref, Default, Serialize, Deserialize)]
pub struct TransactionSigned {
    pub hash: TxHash,
    pub signature: Signature,
    #[deref]
    #[as_ref]
    pub transaction: Transaction,
}

impl Encodable for TransactionSigned {
    fn encode(&self, out: &mut dyn bytes::BufMut) {
        self.encode_inner(out, true);
    }

    fn length(&self) -> usize {
        let len = self.payload_len();
        len + length_of_length(len)
    }
}

impl Decodable for TransactionSigned {
    fn decode(buf: &mut &[u8]) -> alloy_rlp::Result<Self> {
        // Implementation omitted for brevity
        //...
    }
}
```

现在我们已经了解了这些类型如何定义，接下来看看它们在网络中如何被使用。

## P2PStream

与其他 peer 通信的最底层流是 P2P stream。它包装一个底层 Tokio stream，并承担如下职责：

- 跟踪并管理 Ping/Pong 消息，并在需要时发送。
- 维护 reth 节点与 peer 之间协商得到的 SharedCapabilities。
- 从 peer 接收字节，解压后转发给上层 stream。
- 从上层 stream 接收字节，压缩后发给 peer。

字节的压缩/解压使用 snappy 算法（[EIP 706](https://eips.ethereum.org/EIPS/eip-706)），并依赖外部 `snap` crate。

[文件：crates/net/eth-wire/src/p2pstream.rs](../../crates/net/eth-wire/src/p2pstream.rs)

```rust,ignore
#[pin_project]
pub struct P2PStream<S> {
    #[pin]
    inner: S,
    encoder: snap::raw::Encoder,
    decoder: snap::raw::Decoder,
    pinger: Pinger,
    /// Negotiated shared capabilities
    shared_capabilities: SharedCapabilities,
    /// Outgoing messages buffered for sending to the underlying stream.
    outgoing_messages: VecDeque<Bytes>,
    /// Maximum number of messages that can be buffered before yielding backpressure.
    outgoing_message_buffer_capacity: usize,
    /// Whether this stream is currently in the process of gracefully disconnecting.
    disconnecting: bool,
}
```

### Pinger

为了管理 ping 行为，会使用一个 `Pinger` 结构体实例。它是一个状态机，用来跟踪我们已发送/接收的 ping 以及对应的超时信息。

[文件：crates/net/eth-wire/src/pinger.rs](../../crates/net/eth-wire/src/pinger.rs)

```rust,ignore
#[derive(Debug)]
pub(crate) struct Pinger {
    /// The timer used for the next ping.
    ping_interval: Interval,
    /// The timer used to detect a ping timeout.
    timeout_timer: Pin<Box<Sleep>>,
    /// The timeout duration for each ping.
    timeout: Duration,
    state: PingState,
}

/// This represents the possible states of the pinger.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PingState {
    /// There are no pings in flight, or all pings have been responded to.
    Ready,
    /// We have sent a ping and are waiting for a pong, but the peer has missed n pongs.
    WaitingForPong,
    /// The peer has failed to respond to a ping.
    TimedOut,
}
```

状态转换以类似 future 的方式实现：`poll_ping` 会推进 pinger 的状态机。

[文件：crates/net/eth-wire/src/pinger.rs](https://github.com/paradigmxyz/reth/blob/1563506aea09049a85e5cc72c2894f3f7a371581/crates/net/eth-wire/src/pinger.rs)

```rust, ignore
pub(crate) fn poll_ping(
    &mut self,
    cx: &mut Context<'_>,
) -> Poll<Result<PingerEvent, PingerError>> {
    match self.state() {
        PingState::Ready => {
            if self.ping_interval.poll_tick(cx).is_ready() {
                self.timeout_timer.as_mut().reset(Instant::now() + self.timeout);
                self.state = PingState::WaitingForPong;
                return Poll::Ready(Ok(PingerEvent::Ping))
            }
        }
        PingState::WaitingForPong => {
            if self.timeout_timer.as_mut().poll(cx).is_ready() {
                self.state = PingState::TimedOut;
                return Poll::Ready(Ok(PingerEvent::Timeout))
            }
        }
        PingState::TimedOut => {
            return Poll::Pending
        }
    };
    Poll::Pending
```

### 发送与接收数据

为了收发数据，`P2PStream` 本身实现了 `futures` crate 的 `Stream` 与 `Sink` trait（它本身就是一个 future）。

对于 `Stream`，会轮询（poll）`inner` stream，解压后返回。下面示例省略了大量错误处理代码以便聚焦核心逻辑：

[文件：crates/net/eth-wire/src/p2pstream.rs](../../crates/net/eth-wire/src/p2pstream.rs)

```rust,ignore

impl<S> Stream for P2PStream<S> {
    type Item = Result<BytesMut, P2PStreamError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        while let Poll::Ready(res) = this.inner.poll_next_unpin(cx) {
            let bytes = match res {
                Some(Ok(bytes)) => bytes,
                Some(Err(err)) => return Poll::Ready(Some(Err(err.into()))),
                None => return Poll::Ready(None),
            };
            let decompressed_len = snap::raw::decompress_len(&bytes[1..])?;
            let mut decompress_buf = BytesMut::zeroed(decompressed_len + 1);
            this.decoder.decompress(&bytes[1..], &mut decompress_buf[1..])?;
            // ... Omitted Error handling
            // Normalize IDs: reserved p2p range is 0x00..=0x0f; subprotocols start at 0x10
            decompress_buf[0] = bytes[0] - MAX_RESERVED_MESSAGE_ID - 1;
            return Poll::Ready(Some(Ok(decompress_buf)))
        }
    }
}
```

对于 `Sink`，逻辑正好相反：压缩后发送到 `inner` stream。下面展示其中最关键的函数：

[文件：crates/net/eth-wire/src/p2pstream.rs](../../crates/net/eth-wire/src/p2pstream.rs)

```rust, ignore
impl<S> Sink<Bytes> for P2PStream<S> {
    fn start_send(self: Pin<&mut Self>, item: Bytes) -> Result<(), Self::Error> {
        let this = self.project();
        let mut compressed = BytesMut::zeroed(1 + snap::raw::max_compress_len(item.len() - 1));
        let compressed_size = this.encoder.compress(&item[1..], &mut compressed[1..])?;
        compressed.truncate(compressed_size + 1);
        // Mask subprotocol IDs into global space above reserved p2p IDs
        compressed[0] = item[0] + MAX_RESERVED_MESSAGE_ID + 1;
        this.outgoing_messages.push_back(compressed.freeze());
        Ok(())
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let mut this = self.project();
        loop {
            match ready!(this.inner.as_mut().poll_flush(cx)) {
                Err(err) => return Poll::Ready(Err(err.into())),
                Ok(()) => {
                    if let Some(message) = this.outgoing_messages.pop_front() {
                        if let Err(err) = this.inner.as_mut().start_send(message) {
                            return Poll::Ready(Err(err.into()))
                        }
                    } else {
                        return Poll::Ready(Ok(()))
                    }
                }
            }
        }
    }
}
```

## EthStream

`EthStream` 包装一个 stream，并根据协商得到的 `EthVersion` 来处理 eth 消息的（RLP）编码/解码。

[文件：crates/net/eth-wire/src/ethstream.rs](../../crates/net/eth-wire/src/ethstream.rs)

```rust,ignore
#[pin_project]
pub struct EthStream<S, N = EthNetworkPrimitives> {
    /// Eth-specific logic
    eth: EthStreamInner<N>,
    #[pin]
    inner: S,
}
```

`EthStream` 通过 `ProtocolMessage::decode_message(version, &mut bytes)` 与 `ProtocolMessage::encode()` 来进行 RLP 解码/编码，并强制执行协议规则（例如握手完成后禁止再发送/接收 `Status`）。

[文件：crates/net/eth-wire/src/ethstream.rs](../../crates/net/eth-wire/src/ethstream.rs)

```rust,ignore
impl<S, E> Stream for EthStream<S> {
    // ...
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.project();
        let bytes = ready!(this.inner.poll_next(cx)).unwrap();
        // ...
        let msg = match ProtocolMessage::decode_message(self.version(), &mut bytes.as_ref()) {
            Ok(m) => m,
            Err(err) => {
                return Poll::Ready(Some(Err(err.into())))
            }
        };
        Poll::Ready(Some(Ok(msg.message)))
    }
}

impl<S, E> Sink<EthMessage> for EthStream<S> {
    // ...
    fn start_send(self: Pin<&mut Self>, item: EthMessage) -> Result<(), Self::Error> {
        if matches!(item, EthMessage::Status(_)) {
            let _ = self.project().inner.disconnect(DisconnectReason::ProtocolBreach);
            return Err(EthStreamError::EthHandshakeError(EthHandshakeError::StatusNotInHandshake))
        }
        let mut bytes = BytesMut::new();
        ProtocolMessage::from(item).encode(&mut bytes);
        let bytes = bytes.freeze();
        self.project().inner.start_send(bytes)?;
        Ok(())
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.project().inner.poll_flush(cx).map_err(Into::into)
    }
}
```

## 未认证流（Unauthed streams）

要建立一个会话（session），以太坊网络中的 peer 必须先在 RLPx 层交换 `Hello` 消息，然后在 eth-wire 层交换 `Status` 消息。

为此，reth 提供了上面各类 stream 的特殊 “Unauthed” 版本。

`UnauthedP2PStream` 负责执行 `Hello` 握手，并返回一个 `P2PStream`。

[文件：crates/net/eth-wire/src/p2pstream.rs](../../crates/net/eth-wire/src/p2pstream.rs)

```rust, ignore
#[pin_project]
pub struct UnauthedP2PStream<S> {
    #[pin]
    inner: S,
}

impl<S> UnauthedP2PStream<S> {
    // ...
    pub async fn handshake(mut self, hello: HelloMessageWithProtocols) -> Result<(P2PStream<S>, HelloMessage), P2PStreamError> {
        self.inner.send(alloy_rlp::encode(P2PMessage::Hello(hello.message())).into()).await?;
        let first_message_bytes = tokio::time::timeout(HANDSHAKE_TIMEOUT, self.inner.next()).await;

        let their_hello = match P2PMessage::decode(&mut &first_message_bytes[..]) {
            Ok(P2PMessage::Hello(hello)) => Ok(hello),
            // ...
            }
        }?;
        let stream = P2PStream::new(self.inner, shared_capabilities);

        Ok((stream, their_hello))
    }
}

```

类似地，`UnauthedEthStream` 负责执行 `Status` 握手，并返回一个 `EthStream`。它接收一个 `UnifiedStatus` 与一个 `ForkFilter`，并提供超时包装。相关代码见 [这里](../../crates/net/eth-wire/src/ethstream.rs)。

### 复用与“卫星协议”（Multiplexing and satellites）

`eth-wire` 还提供了 `RlpxProtocolMultiplexer`/`RlpxSatelliteStream`，用于在协商得到的 `SharedCapabilities` 下，让主 `eth` 协议与额外的“卫星协议”（例如 `snap`）并行运行。

## 消息变体与版本

- `NewPooledTransactionHashes` 在 ETH66（`NewPooledTransactionHashes66`）与 ETH68（`NewPooledTransactionHashes68`）之间不同。
- 从 ETH67 开始，`GetNodeData` 与 `NodeData` 被移除（对 >=67 的版本解码它们会返回错误）。
- 从 ETH69 开始：
  - `BlockRangeUpdate (0x11)` 用于公告可提供的历史区块范围。
  - Receipts 省略 bloom：编码为 `Receipts69` 而不是 `Receipts`。
- 从 ETH70（EIP-7975）开始：
  - Status 复用 ETH69 的格式（不增加额外的 block range 字段）。
  - Receipts 仍然省略 bloom；`GetReceipts`/`Receipts` 增加 eth/70 的变体以支持“部分 receipt 范围”（`firstBlockReceiptIndex` 与 `lastBlockIncomplete`）。

