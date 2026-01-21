use crate::{
    errors::{EthHandshakeError, EthStreamError, P2PStreamError},
    ethstream::MAX_MESSAGE_SIZE,
    CanDisconnect,
};
use bytes::{Bytes, BytesMut};
use futures::{Sink, SinkExt, Stream};
use reth_eth_wire_types::{
    DisconnectReason, EthMessage, EthNetworkPrimitives, ProtocolMessage, StatusMessage,
    UnifiedStatus,
};
use reth_ethereum_forks::ForkFilter;
use reth_primitives_traits::GotExpected;
use std::{fmt::Debug, future::Future, pin::Pin, time::Duration};
use tokio::time::timeout;
use tokio_stream::StreamExt;
use tracing::{debug, trace};

/// 负责执行 P2P 握手的 Trait。
pub trait EthRlpxHandshake: Debug + Send + Sync + 'static {
    /// 执行 `eth` 协议的 P2P 握手。
    fn handshake<'a>(
        &'a self,
        unauth: &'a mut dyn UnauthEth,
        status: UnifiedStatus,
        fork_filter: ForkFilter,
        timeout_limit: Duration,
    ) -> Pin<Box<dyn Future<Output = Result<UnifiedStatus, EthStreamError>> + 'a + Send>>;
}

/// 一个未经身份验证的、可以发送和接收消息的流。
pub trait UnauthEth:
    Stream<Item = Result<BytesMut, P2PStreamError>>
    + Sink<Bytes, Error = P2PStreamError>
    + CanDisconnect<Bytes>
    + Unpin
    + Send
{
}

impl<T> UnauthEth for T where
    T: Stream<Item = Result<BytesMut, P2PStreamError>>
        + Sink<Bytes, Error = P2PStreamError>
        + CanDisconnect<Bytes>
        + Unpin
        + Send
{
}

/// 以太坊 P2P 握手逻辑。
///
/// 这执行标准的以太坊 `eth` RLPx 握手。
#[derive(Debug, Default, Clone)]
#[non_exhaustive]
pub struct EthHandshake;

impl EthRlpxHandshake for EthHandshake {
    fn handshake<'a>(
        &'a self,
        unauth: &'a mut dyn UnauthEth,
        status: UnifiedStatus,
        fork_filter: ForkFilter,
        timeout_limit: Duration,
    ) -> Pin<Box<dyn Future<Output = Result<UnifiedStatus, EthStreamError>> + 'a + Send>> {
        Box::pin(async move {
            // 对握手过程应用超时限制
            timeout(timeout_limit, EthereumEthHandshake(unauth).eth_handshake(status, fork_filter))
                .await
                .map_err(|_| EthStreamError::StreamTimeout)?
        })
    }
}

/// 负责执行以太坊特定的 `eth` 协议握手的类型。
#[derive(Debug)]
pub struct EthereumEthHandshake<'a, S: ?Sized>(pub &'a mut S);

impl<S: ?Sized, E> EthereumEthHandshake<'_, S>
where
    S: Stream<Item = Result<BytesMut, E>> + CanDisconnect<Bytes> + Send + Unpin,
    EthStreamError: From<E> + From<<S as Sink<Bytes>>::Error>,
{
    /// 使用给定的输入流执行 `eth` RLPx 协议握手（交换 Status 消息）。
    pub async fn eth_handshake(
        self,
        unified_status: UnifiedStatus,
        fork_filter: ForkFilter,
    ) -> Result<UnifiedStatus, EthStreamError> {
        let unauth = self.0;

        let status = unified_status.into_message();

        // 1. 发送我们的 Status 消息
        let status_msg = alloy_rlp::encode(ProtocolMessage::<EthNetworkPrimitives>::from(
            EthMessage::Status(status),
        ))
        .into();
        unauth.send(status_msg).await.map_err(EthStreamError::from)?;

        // 2. 接收对等方的响应
        let their_msg_res = unauth.next().await;
        let their_msg = match their_msg_res {
            Some(Ok(msg)) => msg,
            Some(Err(e)) => return Err(EthStreamError::from(e)),
            None => {
                unauth
                    .disconnect(DisconnectReason::DisconnectRequested)
                    .await
                    .map_err(EthStreamError::from)?;
                return Err(EthStreamError::EthHandshakeError(EthHandshakeError::NoResponse));
            }
        };

        // 3. 校验消息大小
        if their_msg.len() > MAX_MESSAGE_SIZE {
            unauth
                .disconnect(DisconnectReason::ProtocolBreach)
                .await
                .map_err(EthStreamError::from)?;
            return Err(EthStreamError::MessageTooBig(their_msg.len()));
        }

        // 4. 解码对等方的消息
        let version = status.version();
        let msg = match ProtocolMessage::<EthNetworkPrimitives>::decode_message(
            version,
            &mut their_msg.as_ref(),
        ) {
            Ok(m) => m,
            Err(err) => {
                debug!("decode error in eth handshake: msg={their_msg:x}");
                unauth
                    .disconnect(DisconnectReason::DisconnectRequested)
                    .await
                    .map_err(EthStreamError::from)?;
                return Err(EthStreamError::InvalidMessage(err));
            }
        };

        // 5. 验证对等方的响应是否为 Status 消息，并核对关键信息
        match msg.message {
            EthMessage::Status(their_status_message) => {
                trace!("Validating incoming ETH status from peer");

                // 核对 Genesis 哈希
                if status.genesis() != their_status_message.genesis() {
                    unauth
                        .disconnect(DisconnectReason::ProtocolBreach)
                        .await
                        .map_err(EthStreamError::from)?;
                    return Err(EthHandshakeError::MismatchedGenesis(
                        GotExpected {
                            expected: status.genesis(),
                            got: their_status_message.genesis(),
                        }
                        .into(),
                    )
                    .into());
                }

                // 核对协议版本
                if status.version() != their_status_message.version() {
                    unauth
                        .disconnect(DisconnectReason::ProtocolBreach)
                        .await
                        .map_err(EthStreamError::from)?;
                    return Err(EthHandshakeError::MismatchedProtocolVersion(GotExpected {
                        got: their_status_message.version(),
                        expected: status.version(),
                    })
                    .into());
                }

                // 核对链 ID
                if *status.chain() != *their_status_message.chain() {
                    unauth
                        .disconnect(DisconnectReason::ProtocolBreach)
                        .await
                        .map_err(EthStreamError::from)?;
                    return Err(EthHandshakeError::MismatchedChain(GotExpected {
                        got: *their_status_message.chain(),
                        expected: *status.chain(),
                    })
                    .into());
                }

                // 确保对等方的总难度 (Total Difficulty) 在合理范围内（Legacy 模式）
                if let StatusMessage::Legacy(s) = their_status_message &&
                    s.total_difficulty.bit_len() > 160
                {
                    unauth
                        .disconnect(DisconnectReason::ProtocolBreach)
                        .await
                        .map_err(EthStreamError::from)?;
                    return Err(EthHandshakeError::TotalDifficultyBitLenTooLarge {
                        got: s.total_difficulty.bit_len(),
                        maximum: 160,
                    }
                    .into());
                }

                // 分叉验证 (Fork validation)
                if let Err(err) = fork_filter
                    .validate(their_status_message.forkid())
                    .map_err(EthHandshakeError::InvalidFork)
                {
                    unauth
                        .disconnect(DisconnectReason::ProtocolBreach)
                        .await
                        .map_err(EthStreamError::from)?;
                    return Err(err.into());
                }

                // 针对 eth/69 的额外校验
                if let StatusMessage::Eth69(s) = their_status_message {
                    if s.earliest > s.latest {
                        return Err(EthHandshakeError::EarliestBlockGreaterThanLatestBlock {
                            got: s.earliest,
                            latest: s.latest,
                        }
                        .into());
                    }

                    if s.blockhash.is_zero() {
                        return Err(EthHandshakeError::BlockhashZero.into());
                    }
                }

                Ok(UnifiedStatus::from_message(their_status_message))
            }
            _ => {
                // 如果握手期间收到的不是 Status 消息，视为违反协议
                unauth
                    .disconnect(DisconnectReason::ProtocolBreach)
                    .await
                    .map_err(EthStreamError::from)?;
                Err(EthStreamError::EthHandshakeError(
                    EthHandshakeError::NonStatusMessageInHandshake,
                ))
            }
        }
    }
}