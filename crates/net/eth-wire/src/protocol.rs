//! Protocol 定义了 `RLPx` 连接中的 P2P 子协议。

use crate::{Capability, EthMessageID, EthVersion};

/// 表示 [Capability] 以及该协议使用的消息数量的类型。
///
/// 只有 [Capability] 会与远程对等方共享。假设双方都已知协议使用的消息数量，
/// 这用于计算消息 ID 的多路复用 (multiplexing) 偏移量。
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct Protocol {
    /// 子协议的能力定义（名称和版本）
    pub cap: Capability,
    /// 该协议使用/保留的消息数量。
    ///
    /// 这用于消息 ID 的多路复用。
    messages: u8,
}

impl Protocol {
    /// 创建一个新的 Protocol，指定名称和消息数量
    pub const fn new(cap: Capability, messages: u8) -> Self {
        Self { cap, messages }
    }

    /// 返回给定版本对应的 eth 能力。
    pub const fn eth(version: EthVersion) -> Self {
        let cap = Capability::eth(version);
        // 获取该 eth 版本支持的消息总数，用于多路复用
        let messages = EthMessageID::message_count(version);
        Self::new(cap, messages)
    }

    /// 返回 [`EthVersion::Eth66`] 能力。
    pub const fn eth_66() -> Self {
        Self::eth(EthVersion::Eth66)
    }

    /// 返回 [`EthVersion::Eth67`] 能力。
    pub const fn eth_67() -> Self {
        Self::eth(EthVersion::Eth67)
    }

    /// 返回 [`EthVersion::Eth68`] 能力。
    pub const fn eth_68() -> Self {
        Self::eth(EthVersion::Eth68)
    }

    /// 消费此类型并返回 [Capability] 和消息数量组成的元组。
    #[inline]
    pub(crate) fn split(self) -> (Capability, u8) {
        (self.cap, self.messages)
    }

    /// 返回代表该能力所有消息 ID 所需的值数量。
    pub const fn messages(&self) -> u8 {
        self.messages
    }
}

impl From<EthVersion> for Protocol {
    fn from(version: EthVersion) -> Self {
        Self::eth(version)
    }
}

/// 一个辅助类型，用于跟踪协议版本和协议使用的消息数量。
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) struct ProtoVersion {
    /// 协议的消息数量
    pub(crate) messages: u8,
    /// 协议的版本
    pub(crate) version: usize,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_protocol_eth_message_count() {
        // 测试 Protocol::eth() 为每个版本返回正确的消息计数
        // 这确保了 EthMessageID::message_count() 产生预期的结果
        assert_eq!(Protocol::eth(EthVersion::Eth66).messages(), 17);
        assert_eq!(Protocol::eth(EthVersion::Eth67).messages(), 17);
        assert_eq!(Protocol::eth(EthVersion::Eth68).messages(), 17);
        assert_eq!(Protocol::eth(EthVersion::Eth69).messages(), 18);
    }
}