use crate::errors::PingerError;
use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
    time::Duration,
};
use tokio::time::{Instant, Interval, Sleep};
use tokio_stream::Stream;

/// Pinger 是一个简单的状态机，负责发送 ping（探测消息），等待 pong（响应消息），
/// 如果在超时时间内未收到 pong，则转换到超时状态。
#[derive(Debug)]
pub(crate) struct Pinger {
    /// 用于触发下一次 ping 的定时器。
    ping_interval: Interval,
    /// 用于检测 ping 超时的定时器。
    timeout_timer: Pin<Box<Sleep>>,
    /// 每次 ping 的超时时长。
    timeout: Duration,
    /// 跟踪当前状态。
    state: PingState,
}

// === impl Pinger ===

impl Pinger {
    /// 创建一个新的 [`Pinger`]，指定 ping 的时间间隔和超时时长。
    pub(crate) fn new(ping_interval: Duration, timeout_duration: Duration) -> Self {
        let now = Instant::now();
        let timeout_timer = tokio::time::sleep(timeout_duration);
        Self {
            state: PingState::Ready,
            // 设置在指定间隔后触发第一次心跳探测
            ping_interval: tokio::time::interval_at(now + ping_interval, ping_interval),
            timeout_timer: Box::pin(timeout_timer),
            timeout: timeout_duration,
        }
    }

    /// 标记已收到 pong 响应。
    /// 如果当前处于 `WaitingForPong`（等待响应）状态，则转换回 `Ready` 状态。
    /// 该操作会重置 ping 间隔定时器，重新开始计时。
    pub(crate) fn on_pong(&mut self) -> Result<(), PingerError> {
        match self.state {
            PingState::Ready => Err(PingerError::UnexpectedPong),
            PingState::WaitingForPong => {
                self.state = PingState::Ready;
                self.ping_interval.reset();
                Ok(())
            }
            PingState::TimedOut => {
                // 如果在超时后收到了 pong，我们也会重置状态，
                // 假设连接在超时后仍然保持活跃（可能由于网络延迟导致响应延迟到达）。
                self.state = PingState::Ready;
                self.ping_interval.reset();
                Ok(())
            }
        }
    }

    /// 返回 pinger 的当前状态。
    pub(crate) const fn state(&self) -> PingState {
        self.state
    }

    /// 轮询 pinger 的状态，决定是否需要发送新的 ping 或判定之前的 ping 已超时。
    pub(crate) fn poll_ping(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Result<PingerEvent, PingerError>> {
        match self.state() {
            PingState::Ready => {
                // 检查是否到了发送下一个 ping 的时间点
                if self.ping_interval.poll_tick(cx).is_ready() {
                    // 重置超时定时器，并进入等待响应状态
                    self.timeout_timer.as_mut().reset(Instant::now() + self.timeout);
                    self.state = PingState::WaitingForPong;
                    return Poll::Ready(Ok(PingerEvent::Ping))
                }
            }
            PingState::WaitingForPong => {
                // 检查超时定时器是否已到期
                if self.timeout_timer.as_mut().poll(cx).is_ready() {
                    self.state = PingState::TimedOut;
                    return Poll::Ready(Ok(PingerEvent::Timeout))
                }
            }
            PingState::TimedOut => {
                // 如果已经处于超时状态，且连接尚未终止，持续调用将返回 Pending
                return Poll::Pending
            }
        };
        Poll::Pending
    }
}

impl Stream for Pinger {
    type Item = Result<PingerEvent, PingerError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.get_mut().poll_ping(cx).map(Some)
    }
}

/// 表示 pinger 可能的状态。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PingState {
    /// 准备就绪状态：当前没有正在进行的 ping，或者所有的 ping 都已收到响应。
    Ready,
    /// 等待响应状态：已发送 ping，正在等待对方的 pong。
    WaitingForPong,
    /// 超时状态：对等方未能在规定时间内响应 ping。
    TimedOut,
}

/// [`Pinger`] 产生的事件类型。
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum PingerEvent {
    /// 应该发送一个新的 [`Ping`](super::P2PMessage::Ping) 消息。
    Ping,

    /// 判定对等方已超时。
    Timeout,
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::StreamExt;

    #[tokio::test]
    async fn test_ping_timeout() {
        let interval = Duration::from_millis(300);
        // 我们应该等待间隔到期，并在超时之前收到 pong
        let mut pinger = Pinger::new(interval, Duration::from_millis(20));
        assert_eq!(pinger.next().await.unwrap().unwrap(), PingerEvent::Ping);
        pinger.on_pong().unwrap();
        assert_eq!(pinger.next().await.unwrap().unwrap(), PingerEvent::Ping);

        tokio::time::sleep(interval).await;
        assert_eq!(pinger.next().await.unwrap().unwrap(), PingerEvent::Timeout);
        pinger.on_pong().unwrap();

        assert_eq!(pinger.next().await.unwrap().unwrap(), PingerEvent::Ping);
    }
}