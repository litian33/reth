use crate::{Capability, EthVersion, ProtocolVersion};
use alloy_rlp::{RlpDecodable, RlpEncodable};
use reth_codecs::add_arbitrary_tests;
use reth_network_peers::PeerId;
use reth_primitives_traits::constants::RETH_CLIENT_VERSION;

/// P2P 网络默认的 TCP 端口。
///
/// 注意：这与节点发现端口 `DEFAULT_DISCOVERY_PORT` 相同。
pub(crate) const DEFAULT_TCP_PORT: u16 = 30303;

use crate::protocol::Protocol;
#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

/// 这是 [`HelloMessage`] 的超集，它提供了关于每个能力 (capability) 所使用的消息数量的
/// 额外 [Protocol] 信息，以便进行正确的消息 ID 多路复用 (multiplexing)。
///
/// 这个类型在 `p2p` 握手过程中是必需的，因为原生的 [`HelloMessage`] 并不共享
/// 每个能力使用的消息数量。
///
/// 若要获取不含额外协议信息、可直接进行 RLP 编码的 [`HelloMessage`]，
/// 请使用 [`HelloMessageWithProtocols::message`]。
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct HelloMessageWithProtocols {
    /// `p2p` 协议版本。
    pub protocol_version: ProtocolVersion,
    /// 客户端软件身份，作为人类可读的字符串（例如 "Ethereum(++)/1.0.0"）。
    pub client_version: String,
    /// 支持的能力及其版本的列表。
    pub protocols: Vec<Protocol>,
    /// 客户端监听的端口，零表示客户端未在监听。
    ///
    /// 默认值为 `30303`，与默认发现端口相同。
    pub port: u16,
    /// 与节点私钥对应的 secp256k1 公钥。
    pub id: PeerId,
}

impl HelloMessageWithProtocols {
    /// 启动一个新的 `HelloMessageProtocolsBuilder`
    pub const fn builder(id: PeerId) -> HelloMessageBuilder {
        HelloMessageBuilder::new(id)
    }

    /// 返回原始的 [`HelloMessage`]，不包含额外的协议信息。
    #[inline]
    pub fn message(&self) -> HelloMessage {
        HelloMessage {
            protocol_version: self.protocol_version,
            client_version: self.client_version.clone(),
            capabilities: self.protocols.iter().map(|p| p.cap.clone()).collect(),
            port: self.port,
            id: self.id,
        }
    }

    /// 将此类型转换为不含额外协议信息的 [`HelloMessage`]。
    pub fn into_message(self) -> HelloMessage {
        HelloMessage {
            protocol_version: self.protocol_version,
            client_version: self.client_version,
            capabilities: self.protocols.into_iter().map(|p| p.cap).collect(),
            port: self.port,
            id: self.id,
        }
    }

    /// 如果协议集中包含给定的协议，则返回 true。
    #[inline]
    pub fn contains_protocol(&self, protocol: &Protocol) -> bool {
        self.protocols.iter().any(|p| p.cap == protocol.cap)
    }

    /// 向集合中添加一个新协议。
    ///
    /// 如果协议已存在，则返回错误。
    #[inline]
    pub fn try_add_protocol(&mut self, protocol: Protocol) -> Result<(), Protocol> {
        if self.contains_protocol(&protocol) {
            Err(protocol)
        } else {
            self.protocols.push(protocol);
            Ok(())
        }
    }
}

/// RLPx 协议握手阶段使用的原始消息，包含支持的 RLPx 协议版本和能力信息。
///
/// 参见 <https://github.com/ethereum/devp2p/blob/master/rlpx.md#hello-0x00>
#[derive(Clone, Debug, PartialEq, Eq, RlpEncodable, RlpDecodable)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(any(test, feature = "arbitrary"), derive(arbitrary::Arbitrary))]
#[add_arbitrary_tests(rlp)]
pub struct HelloMessage {
    /// `p2p` 协议版本。
    pub protocol_version: ProtocolVersion,
    /// 客户端软件身份。
    pub client_version: String,
    /// 支持的能力及其版本的列表。
    pub capabilities: Vec<Capability>,
    /// 客户端监听的端口。
    pub port: u16,
    /// 与节点私钥对应的 secp256k1 公钥。
    pub id: PeerId,
}

// === impl HelloMessage ===

impl HelloMessage {
    /// 启动一个新的 `HelloMessageBuilder`
    pub const fn builder(id: PeerId) -> HelloMessageBuilder {
        HelloMessageBuilder::new(id)
    }
}

/// [`HelloMessageWithProtocols`] 的构建器
#[derive(Debug)]
pub struct HelloMessageBuilder {
    /// `p2p` 协议版本。
    pub protocol_version: Option<ProtocolVersion>,
    /// 客户端软件身份。
    pub client_version: Option<String>,
    /// 支持的协议列表。
    pub protocols: Option<Vec<Protocol>>,
    /// 客户端监听的端口。
    pub port: Option<u16>,
    /// 节点的公钥 ID。
    pub id: PeerId,
}

// === impl HelloMessageBuilder ===

impl HelloMessageBuilder {
    /// 创建一个新的构建器以配置 [`HelloMessage`]
    pub const fn new(id: PeerId) -> Self {
        Self { protocol_version: None, client_version: None, protocols: None, port: None, id }
    }

    /// 设置客户端监听的端口
    pub const fn port(mut self, port: u16) -> Self {
        self.port = Some(port);
        self
    }

    /// 添加一个要使用的新协议。
    pub fn protocol(mut self, protocols: impl Into<Protocol>) -> Self {
        self.protocols.get_or_insert_with(Vec::new).push(protocols.into());
        self
    }

    /// 设置要使用的协议列表。
    pub fn protocols(mut self, protocols: impl IntoIterator<Item = Protocol>) -> Self {
        self.protocols.get_or_insert_with(Vec::new).extend(protocols);
        self
    }

    /// 设置客户端版本。
    pub fn client_version(mut self, client_version: impl Into<String>) -> Self {
        self.client_version = Some(client_version.into());
        self
    }

    /// 设置协议版本。
    pub const fn protocol_version(mut self, protocol_version: ProtocolVersion) -> Self {
        self.protocol_version = Some(protocol_version);
        self
    }

    /// 消费此类型并返回配置好的 [`HelloMessage`]
    ///
    /// 未设置的字段将使用默认值：
    /// - `protocol_version`: [`ProtocolVersion::V5`]
    /// - `client_version`: [`RETH_CLIENT_VERSION`]
    /// - `capabilities`: 所有的 [`EthVersion`]
    pub fn build(self) -> HelloMessageWithProtocols {
        let Self { protocol_version, client_version, protocols, port, id } = self;
        HelloMessageWithProtocols {
            protocol_version: protocol_version.unwrap_or_default(),
            client_version: client_version.unwrap_or_else(|| RETH_CLIENT_VERSION.to_string()),
            protocols: protocols.unwrap_or_else(|| {
                EthVersion::ALL_VERSIONS.iter().copied().map(Into::into).collect()
            }),
            port: port.unwrap_or(DEFAULT_TCP_PORT),
            id,
        }
    }
}

#[cfg(test)]
mod tests {
    // ... (测试部分保持不变)
}