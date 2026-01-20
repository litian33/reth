# Network（网络）

`network` crate 负责管理节点与以太坊点对点（P2P）网络的连接，使节点能够通过 [各种 P2P 子协议](https://github.com/ethereum/devp2p) 与其他节点通信。

Reth 的 P2P 网络主要由 4 个持续运行的任务（task）构成：
- **Discovery（发现）**：发现网络中的新 peers
- **Transactions（交易）**：接收、请求、广播 mempool 交易
- **ETH Requests（ETH 请求）**：响应来自 peers 的 headers 与 bodies 等请求
- **Network Management（网络管理）**：处理入站/出站连接，并在 peers 与其他任务之间路由请求

下面我们以主 Reth CLI（即默认配置的 full node）为例，看看它如何使用 P2P 层，从而了解 `network` crate 的主要接口与入口点。

---

## Network Management Task（网络管理任务）

网络管理任务是 pipeline 与 P2P 网络交互时最常用的任务。它除了管理与 peers 的连通性外，还提供了若干接口用于发送**出站（outbound）请求**。

我们先看看它提供了哪些接口、这些接口如何在 pipeline 中使用，并简要看一下其内部实现，突出一些关键的结构体与 trait。

### 节点如何使用网络层

用于运行节点本身的 `"node"` CLI 命令，在高层次上会执行以下步骤：
1. 初始化 DB
2. 初始化共识 API
3. 将 genesis block 写入 DB
4. 初始化网络
5. 创建一个从网络获取数据的 client
6. 配置 pipeline（向其中添加各 stages）
7. 运行 pipeline

第 5-6 步会消费 `network` crate 中的类型/接口，是我们关心的重点：

[File: bin/reth/src/node/mod.rs](https://github.com/paradigmxyz/reth/blob/1563506aea09049a85e5cc72c2894f3f7a371581/bin/reth/src/node/mod.rs)
```rust,ignore
let network = start_network(network_config(db.clone(), chain_id, genesis_hash)).await?;

let fetch_client = Arc::new(network.fetch_client().await?);
let mut pipeline = reth_stages::Pipeline::new()
    .push(HeaderStage {
        downloader: headers::reverse_headers::ReverseHeadersDownloaderBuilder::default()
            .batch_size(config.stages.headers.downloader_batch_size)
            .retries(config.stages.headers.downloader_retries)
            .build(consensus.clone(), fetch_client.clone()),
        consensus: consensus.clone(),
        client: fetch_client.clone(),
        network_handle: network.clone(),
        commit_threshold: config.stages.headers.commit_threshold,
        metrics: HeaderMetrics::default(),
    })
    .push(BodyStage {
        downloader: Arc::new(
            bodies::bodies::BodiesDownloader::new(
                fetch_client.clone(),
                consensus.clone(),
            )
            .with_batch_size(config.stages.bodies.downloader_batch_size)
            .with_retries(config.stages.bodies.downloader_retries)
            .with_concurrency(config.stages.bodies.downloader_concurrency),
        ),
        consensus: consensus.clone(),
        commit_threshold: config.stages.bodies.commit_threshold,
    })
    .push(SenderRecoveryStage {
        commit_threshold: config.stages.sender_recovery.commit_threshold,
    })
    .push(ExecutionStage { config: ExecutorConfig::new_ethereum() });

if let Some(tip) = self.tip {
    debug!("Tip manually set: {}", tip);
    consensus.notify_fork_choice_state(ForkchoiceState {
        head_block_hash: tip,
        safe_block_hash: tip,
        finalized_block_hash: tip,
    })?;
}

// Run pipeline
info!("Starting pipeline");
pipeline.run(db.clone()).await?;
```

我们从“网络启动”的那一行开始：调用 `start_network`。听起来很关键，对吧？

[File: bin/reth/src/node/mod.rs](https://github.com/paradigmxyz/reth/blob/1563506aea09049a85e5cc72c2894f3f7a371581/bin/reth/src/node/mod.rs)
```rust,ignore
// Method on NetworkConfig for starting the network with request handler
pub async fn start_network(self) -> Result<NetworkHandle<N>, NetworkError>
where
    C: BlockReader<Block = N::Block, Receipt = N::Receipt, Header = N::BlockHeader>
        + HeaderProvider
        + Clone
        + Unpin
        + 'static,
{
    let client = self.client.clone();
    let (handle, network, _txpool, eth) = NetworkManager::builder::<C>(self)
        .await?
        .request_handler::<C>(client)
        .split_with_handle();

    tokio::task::spawn(network);
    // TODO: tokio::task::spawn(txpool); 
    tokio::task::spawn(eth);
    Ok(handle)
}
```

从高层次看，这个函数负责启动本章开头列出的各个任务。

它通过 `NetworkManager::builder` 下游获得 network management、transactions、ETH requests 等任务的句柄（handle），然后把它们 spawn 出来运行。

`NetworkManager::builder` 构造函数需要一个 `NetworkConfig` 作为参数，这可以视为网络层的主要配置入口：

[File: crates/net/network/src/config.rs](https://github.com/paradigmxyz/reth/blob/1563506aea09049a85e5cc72c2894f3f7a371581/crates/net/network/src/config.rs)
```rust,ignore
pub struct NetworkConfig<C, N: NetworkPrimitives = EthNetworkPrimitives> {
    /// The client type that can interact with the chain.
    ///
    /// This type is used to fetch the block number after we established a session and received the
    /// [`UnifiedStatus`] block hash.
    pub client: C,
    /// The node's secret key, from which the node's identity is derived.
    pub secret_key: SecretKey,
    /// All boot nodes to start network discovery with.
    pub boot_nodes: HashSet<TrustedPeer>,
    /// How to set up discovery over DNS.
    pub dns_discovery_config: Option<DnsDiscoveryConfig>,
    /// Address to use for discovery v4.
    pub discovery_v4_addr: SocketAddr,
    /// How to set up discovery.
    pub discovery_v4_config: Option<Discv4Config>,
    /// How to set up discovery version 5.
    pub discovery_v5_config: Option<reth_discv5::Config>,
    /// Address to listen for incoming connections
    pub listener_addr: SocketAddr,
    /// How to instantiate peer manager.
    pub peers_config: PeersConfig,
    /// How to configure the [`SessionManager`](crate::session::SessionManager).
    pub sessions_config: SessionsConfig,
    /// The chain id
    pub chain_id: u64,
    /// The [`ForkFilter`] to use at launch for authenticating sessions.
    ///
    /// See also <https://github.com/ethereum/EIPs/blob/master/EIPS/eip-2124.md#stale-software-examples>
    ///
    /// For sync from block `0`, this should be the default chain [`ForkFilter`] beginning at the
    /// first hardfork, `Frontier` for mainnet.
    pub fork_filter: ForkFilter,
    /// The block importer type.
    pub block_import: Box<dyn BlockImport<N::NewBlockPayload>>,
    /// The default mode of the network.
    pub network_mode: NetworkMode,
    /// The executor to use for spawning tasks.
    pub executor: Box<dyn TaskSpawner>,
    /// The `Status` message to send to peers at the beginning.
    pub status: UnifiedStatus,
    /// Sets the hello message for the p2p handshake in `RLPx`
    pub hello_message: HelloMessageWithProtocols,
    /// Additional protocols to announce and handle in `RLPx`
    pub extra_protocols: RlpxSubProtocols,
    /// Whether to disable transaction gossip
    pub tx_gossip_disabled: bool,
    /// How to instantiate transactions manager.
    pub transactions_manager_config: TransactionsManagerConfig,
    /// The NAT resolver for external IP
    pub nat: Option<NatResolver>,
    /// The Ethereum P2P handshake, see also:
    /// <https://github.com/ethereum/devp2p/blob/master/rlpx.md#initial-handshake>.
    /// This can be overridden to support custom handshake logic via the
    /// [`NetworkConfigBuilder`].
    pub handshake: Arc<dyn EthRlpxHandshake>,
    /// List of block number-hash pairs to check for required blocks.
    /// If non-empty, peers that don't have these blocks will be filtered out.
    pub required_block_hashes: Vec<BlockNumHash>,
}
```

`NetworkConfig` 有两个泛型参数：
- `C`：提供区块链数据（headers、blocks 等）访问能力的 client 类型
- `N`：网络原语类型（network primitives），定义网络层使用的区块/交易类型。默认是标准以太坊网络的 `EthNetworkPrimitives`，但也可为其他链自定义（例如 Optimism）。

Discovery（发现）任务会随着 network management task 被 poll 而推进：它会处理 peer 管理相关事件，这些事件通过 `NetworkManager` 的字段 `Swarm` 来驱动：

[File: crates/net/network/src/swarm.rs](https://github.com/paradigmxyz/reth/blob/1563506aea09049a85e5cc72c2894f3f7a371581/crates/net/network/src/swarm.rs)
```rust,ignore
pub(crate) struct Swarm<N: NetworkPrimitives = EthNetworkPrimitives> {
    /// Listens for new incoming connections.
    incoming: ConnectionListener,
    /// All sessions.
    sessions: SessionManager<N>,
    /// Tracks the entire state of the network and handles events received from the sessions.
    state: NetworkState<N>,
}
```

`Swarm` 把 peers 的入站连接、与 peers 的会话管理（sessions），以及网络整体状态记录（例如活跃 peer 数、网络的 genesis hash 等）粘合在一起。它把这些变化作为 `SwarmEvent` 发给 `NetworkManager`，并在其持有的 `SessionManager` 与 `NetworkState` 之间路由命令与事件。

我们后面会更多提到 `NetworkManager`——它大概是这个 crate 中最重要的结构体。

ETH requests 任务与 transactions 任务会在后面的章节单独讲解。

从 `start_network` 返回的 `network` 变量，以及从 `network.fetch_client` 返回的 `fetch_client` 变量，其类型分别是 `NetworkHandle` 和 `FetchClient`。这两个是与 P2P 网络交互的主要接口，并且目前在 `HeaderStage` 与 `BodyStage` 中使用。

接下来我们先理解这两个接口的实现方式，然后再结合 pipeline 看它们如何被使用。通过这个过程，我们也会更深入地了解 network management task 的内部结构。

### 使用 `NetworkHandle` 与 Network Management Task 交互

`NetworkHandle` 是一个可跨线程共享的 network management task 客户端。它内部用 `Arc` 包装了 `NetworkInner`，其定义如下：

[File: crates/net/network/src/network.rs](https://github.com/paradigmxyz/reth/blob/1563506aea09049a85e5cc72c2894f3f7a371581/crates/net/network/src/network.rs)
```rust,ignore
struct NetworkInner<N: NetworkPrimitives = EthNetworkPrimitives> {
    /// Number of active peer sessions the node's currently handling.
    num_active_peers: Arc<AtomicUsize>,
    /// Sender half of the message channel to the [`NetworkManager`].
    to_manager_tx: UnboundedSender<NetworkHandleMessage<N>>,
    /// The local address that accepts incoming connections.
    listener_address: Arc<Mutex<SocketAddr>>,
    /// The secret key used for authenticating sessions.
    secret_key: SecretKey,
    /// The identifier used by this node.
    local_peer_id: PeerId,
    /// Access to all the nodes
    peers: PeersHandle,
    /// The mode of the network
    network_mode: NetworkMode,
}
```

这里值得关注的是 `to_manager_tx`：它是一个 channel 的发送端，用于向 `NetworkManager` 实例发送消息。

[File: crates/net/network/src/manager.rs](https://github.com/paradigmxyz/reth/blob/1563506aea09049a85e5cc72c2894f3f7a371581/crates/net/network/src/manager.rs)
```rust,ignore
pub struct NetworkManager<N: NetworkPrimitives = EthNetworkPrimitives> {
    /// The type that manages the actual network part, which includes connections.
    swarm: Swarm<N>,
    /// Underlying network handle that can be shared.
    handle: NetworkHandle<N>,
    /// Receiver half of the command channel set up between this type and the [`NetworkHandle`]
    from_handle_rx: UnboundedReceiverStream<NetworkHandleMessage<N>>,
    /// Handles block imports according to the `eth` protocol.
    block_import: Box<dyn BlockImport<N::NewBlockPayload>>,
    /// Sender for high level network events.
    event_sender: EventSender<NetworkEvent<PeerRequest<N>>>,
    /// Sender half to send events to the
    /// [`TransactionsManager`](crate::transactions::TransactionsManager) task, if configured.
    to_transactions_manager: Option<UnboundedMeteredSender<NetworkTransactionEvent<N>>>,
    /// Sender half to send events to the
    /// [`EthRequestHandler`](crate::eth_requests::EthRequestHandler) task, if configured.
    to_eth_request_handler: Option<mpsc::Sender<IncomingEthRequest<N>>>,
    /// Tracks the number of active sessions (connected peers).
    ///
    /// This is updated via internal events and shared via `Arc` with the [`NetworkHandle`]
    /// Updated by the `NetworkWorker` and loaded by the `NetworkService`.
    num_active_peers: Arc<AtomicUsize>,
    /// Metrics for the Network
    metrics: NetworkMetrics,
    /// Disconnect metrics for the Network
    disconnect_metrics: DisconnectMetrics,
}
```

这就进入 `network` crate 的核心部分了。`NetworkManager` 结构体代表本章开头提到的 “Network Management” 任务。它被实现为一个无限 [`Future`](https://doc.rust-lang.org/std/future/trait.Future.html)：你可以把它理解为一个“枢纽进程（hub）”，它监听来自 `NetworkHandle` 或 peers 的消息，维护网络状态，并把消息派发给其他任务。

`NetworkManager` 设计为一个独立的 [`tokio::task`](https://docs.rs/tokio/0.2.4/tokio/task/index.html) 来运行；而 `NetworkHandle` 则可以被四处传递与共享：通过发送请求/命令到相应的 channel，就能从任何地方访问 `NetworkManager`。

#### `NetworkHandle` 在 pipeline 中的用法

在 pipeline 中，`NetworkHandle` 会用来创建 `FetchClient`（下一节会讲），并且会在 `HeaderStage` 中用于更新节点的 ["status"](https://github.com/ethereum/devp2p/blob/master/caps/eth.md#status-0x00)（记录已处理区块的 total difficulty、hash 与 height）：

[File: crates/stages/src/stages/headers.rs](https://github.com/paradigmxyz/reth/blob/1563506aea09049a85e5cc72c2894f3f7a371581/crates/stages/src/stages/headers.rs)
```rust,ignore
async fn update_head<DB: Database>(
    &self,
    tx: &Transaction<'_, DB>,
    height: BlockNumber,
) -> Result<(), StageError> {
    // --snip--
    self.network_handle.update_status(height, block_key.hash(), td);
    // --snip--

}
```

理解了 network management task 的内部结构后，我们再看一个更高层的抽象：用来从其他 peers 获取数据的 `FetchClient`。

### 在 pipeline 中使用 `FetchClient` 从网络获取数据

`FetchClient` 与 `NetworkHandle` 类似，也是一个可跨线程共享的 client，用于从网络获取数据。它是一个相对轻量的结构体：

[File: crates/net/network/src/fetch/client.rs](https://github.com/paradigmxyz/reth/blob/1563506aea09049a85e5cc72c2894f3f7a371581/crates/net/network/src/fetch/client.rs)
```rust,ignore
pub struct FetchClient<N: NetworkPrimitives = EthNetworkPrimitives> {
    /// Sender half of the request channel.
    pub(crate) request_tx: UnboundedSender<DownloadRequest<N>>,
    /// The handle to the peers
    pub(crate) peers_handle: PeersHandle,
    /// Number of active peer sessions the node's currently handling.
    pub(crate) num_active_peers: Arc<AtomicUsize>,
}
```

`request_tx` 是一个 channel 的发送端，用于发送“下载数据”的请求；`peers_handle` 则包装了另一个 channel 的句柄，用于对 peer 集合进行一些手动变更。

#### 创建 `FetchClient`

在创建 `FetchClient` 时，其字段 `request_tx` 与 `peers_handle` 会从 `StateFetcher` 结构体中 clone 出来。`StateFetcher` 是更底层的结构体，负责管理网络上的数据获取操作：

[File: crates/net/network/src/fetch/mod.rs](https://github.com/paradigmxyz/reth/blob/1563506aea09049a85e5cc72c2894f3f7a371581/crates/net/network/src/fetch/mod.rs)
```rust,ignore
pub struct StateFetcher<N: NetworkPrimitives = EthNetworkPrimitives> {
    /// Currently active [`GetBlockHeaders`] requests
    inflight_headers_requests: HashMap<PeerId, InflightHeadersRequest<N::BlockHeader>>,
    /// Currently active [`GetBlockBodies`] requests
    inflight_bodies_requests: HashMap<PeerId, InflightBodiesRequest<N::BlockBody>>,
    /// The list of _available_ peers for requests.
    peers: HashMap<PeerId, Peer>,
    /// The handle to the peers manager
    peers_handle: PeersHandle,
    /// Number of active peer sessions the node's currently handling.
    num_active_peers: Arc<AtomicUsize>,
    /// Requests queued for processing
    queued_requests: VecDeque<DownloadRequest<N>>,
    /// Receiver for new incoming download requests
    download_requests_rx: UnboundedReceiverStream<DownloadRequest<N>>,
    /// Sender for download requests, used to detach a [`FetchClient`]
    download_requests_tx: UnboundedSender<DownloadRequest<N>>,
}
```

`StateFetcher` 本身深度嵌在 `NetworkManager` 内部：前面展示过的 `Swarm` 中包含 `NetworkState`，而 `NetworkState` 把 `StateFetcher` 作为字段：

[File: crates/net/network/src/state.rs](https://github.com/paradigmxyz/reth/blob/1563506aea09049a85e5cc72c2894f3f7a371581/crates/net/network/src/state.rs)
```rust,ignore
pub struct NetworkState<N: NetworkPrimitives = EthNetworkPrimitives> {
    /// All active peers and their state.
    active_peers: HashMap<PeerId, ActivePeer<N>>,
    /// Manages connections to peers.
    peers_manager: PeersManager,
    /// Buffered messages until polled.
    queued_messages: VecDeque<StateAction<N>>,
    /// The client type that can interact with the chain.
    ///
    /// This type is used to fetch the block number after we established a session and received the
    /// [`UnifiedStatus`] block hash.
    client: BlockNumReader,
    /// Network discovery.
    discovery: Discovery,
    /// The type that handles requests.
    ///
    /// The fetcher streams `RLPx` related requests on a per-peer basis to this type. This type
    /// will then queue in the request and notify the fetcher once the result has been
    /// received.
    state_fetcher: StateFetcher<N>,
}
```

#### `FetchClient` 在 pipeline 中的用法

`FetchClient` 实现了 `HeadersClient` 与 `BodiesClient` trait，用于从可用 peers 获取 headers 与 block bodies。

[File: crates/net/network/src/fetch/client.rs](https://github.com/paradigmxyz/reth/blob/1563506aea09049a85e5cc72c2894f3f7a371581/crates/net/network/src/fetch/client.rs)
```rust,ignore
impl HeadersClient for FetchClient {
    /// Sends a `GetBlockHeaders` request to an available peer.
    async fn get_headers(&self, request: HeadersRequest) -> PeerRequestResult<BlockHeaders> {
        let (response, rx) = oneshot::channel();
        self.request_tx.send(DownloadRequest::GetBlockHeaders { request, response })?;
        rx.await?.map(WithPeerId::transform)
    }
}

impl BodiesClient for FetchClient {
    async fn get_block_bodies(&self, request: Vec<B256>) -> PeerRequestResult<Vec<BlockBody>> {
        let (response, rx) = oneshot::channel();
        self.request_tx.send(DownloadRequest::GetBlockBodies { request, response })?;
        rx.await?
    }
}
```

这些能力分别在 `HeaderStage` 与 `BodyStage` 中使用。

在主 reth 二进制默认配置的 pipeline 中，`HeaderStage` 使用 `ReverseHeadersDownloader` 从网络以 stream 的方式获取 headers：

[File: crates/net/downloaders/src/headers/reverse_headers.rs](https://github.com/paradigmxyz/reth/blob/1563506aea09049a85e5cc72c2894f3f7a371581/crates/net/downloaders/src/headers/reverse_headers.rs)
```rust,ignore
pub struct ReverseHeadersDownloader<C, H> {
    /// The consensus client
    consensus: Arc<C>,
    /// The headers client
    client: Arc<H>,
    /// The batch size per one request
    pub batch_size: u64,
    /// The number of retries for downloading
    pub request_retries: usize,
}
```

`FetchClient` 会被传入 `client` 字段；并且在 `HeaderStage` 的 `execute` 方法中 poll 这个 downloader stream 时，会使用 `FetchClient.get_headers`：

[File: crates/net/downloaders/src/headers/reverse_headers.rs](https://github.com/paradigmxyz/reth/blob/1563506aea09049a85e5cc72c2894f3f7a371581/crates/net/downloaders/src/headers/reverse_headers.rs)
```rust,ignore
fn get_or_init_fut(&mut self) -> HeadersRequestFuture {
    match self.request.take() {
        None => {
            // queue in the first request
            let client = Arc::clone(&self.client);
            let req = self.headers_request();
            tracing::trace!(
                target: "downloaders::headers",
                "requesting headers {req:?}"
            );
            HeadersRequestFuture {
                request: req.clone(),
                fut: Box::pin(async move { client.get_headers(req).await }),
                retries: 0,
                max_retries: self.request_retries,
            }
        }
        Some(fut) => fut,
    }
}
```

在主二进制配置的 `BodyStage` 中，会使用 `BodiesDownloader`：

[File: crates/net/downloaders/src/bodies/bodies.rs](https://github.com/paradigmxyz/reth/blob/1563506aea09049a85e5cc72c2894f3f7a371581/crates/net/downloaders/src/bodies/bodies.rs)
```rust,ignore
pub struct BodiesDownloader<Client, Consensus> {
    /// The bodies client
    client: Arc<Client>,
    /// The consensus client
    consensus: Arc<Consensus>,
    /// The number of retries for each request.
    retries: usize,
    /// The batch size per one request
    batch_size: usize,
    /// The maximum number of requests to send concurrently.
    concurrency: usize,
}
```

同样，`FetchClient` 会被传入 `client` 字段；并且在 `BodyStage` 的 `execute` 方法创建的 stream 中，会调用 `FetchClient.get_block_bodies`：

[File: crates/net/downloaders/src/bodies/bodies.rs](https://github.com/paradigmxyz/reth/blob/1563506aea09049a85e5cc72c2894f3f7a371581/crates/net/downloaders/src/bodies/bodies.rs)
```rust,ignore
async fn fetch_bodies(
    &self,
    headers: Vec<&SealedHeader>,
) -> DownloadResult<Vec<BlockResponse>> {
    // --snip--
    let (peer_id, bodies) =
        self.client.get_block_bodies(headers_with_txs_and_ommers).await?.split();
    // --snip--
}
```

值得注意的是：当节点开始从某个 peer 下载 headers 或 bodies 时，它并不一定会一直固定使用该 peer；否则就无法有效支持并发请求。

当调用 `FetchClient.get_headers` 或 `FetchClient.get_block_bodies` 时，会把对应的 `DownloadRequest` 发送到 `StateFetcher.download_requests_tx` channel；这些请求会在 `StateFetcher` 被 poll 时被处理。

每次 `StateFetcher` 被 poll 时，它会寻找一个“空闲（idle）”的 peer 来处理当前请求（header 或 body）。这里 “idle” 指该 peer 当前没有在处理来自本节点的请求：

[File: crates/net/network/src/fetch/mod.rs](https://github.com/paradigmxyz/reth/blob/1563506aea09049a85e5cc72c2894f3f7a371581/crates/net/network/src/fetch/mod.rs)
```rust,ignore
/// Returns the next action to return
fn poll_action(&mut self) -> PollAction {
    // we only check and not pop here since we don't know yet whether a peer is available.
    if self.queued_requests.is_empty() {
        return PollAction::NoRequests
    }

    let peer_id = if let Some(peer_id) = self.next_peer() {
        peer_id
    } else {
        return PollAction::NoPeersAvailable
    };

    let request = self.queued_requests.pop_front().expect("not empty");
    let request = self.prepare_block_request(peer_id, request);

    PollAction::Ready(FetchAction::BlockRequest { peer_id, request })
}
```

---

## ETH Requests Task（ETH 请求任务）

ETH requests 任务负责为其他 peers **处理入站（incoming）**的、与区块相关的请求（属于 [`eth` P2P 子协议](https://github.com/ethereum/devp2p/blob/master/caps/eth.md#protocol-messages)）。

与 network management task 类似，它也是一个无限 future，但它被设计为后台任务（独立 `tokio::task`）运行，pipeline 并不会直接与它交互。该任务由 `EthRequestHandler` 表示：

[File: crates/net/network/src/eth_requests.rs](https://github.com/paradigmxyz/reth/blob/1563506aea09049a85e5cc72c2894f3f7a371581/crates/net/network/src/eth_requests.rs)
```rust,ignore
pub struct EthRequestHandler<C, N: NetworkPrimitives = EthNetworkPrimitives> {
    /// The client type that can interact with the chain.
    client: C,
    /// Used for reporting peers.
    #[expect(dead_code)]
    peers: PeersHandle,
    /// Incoming request from the [`NetworkManager`](crate::NetworkManager).
    incoming_requests: ReceiverStream<IncomingEthRequest<N>>,
    /// Metrics for the eth request handler.
    metrics: EthRequestHandlerMetrics,
}
```

这里的 `client` 是用于从数据库取数据的 client。不要把它与前面 downloader（例如 `ReverseHeadersDownloader`）中的 `client` 混淆：后者通常是 `FetchClient`。

### ETH Requests Task 的输入流

`incoming_requests` 字段是一个 channel 的接收端，用于接收入站 ETH 请求。该 channel 的发送端保存在 `NetworkManager` 的 `to_eth_request_handler` 字段里。

当 `NetworkManager` 被 poll 并通过其 `Swarm` 字段监听来自 peers 的事件时，它会把收到的 ETH 请求发送到这个 channel。

### ETH Requests Task 的运行方式

作为一个无限 future，ETH requests 任务的核心逻辑在其 `poll` 方法中。`EthRequestHandler` 被 poll 时，会从 channel 中读取 ETH 请求并进行处理。撰写本文时，ETH requests 任务能处理 [`GetBlockHeaders`](https://github.com/ethereum/devp2p/blob/master/caps/eth.md#getblockheaders-0x03) 与 [`GetBlockBodies`](https://github.com/ethereum/devp2p/blob/master/caps/eth.md#getblockbodies-0x05) 请求。

[File: crates/net/network/src/eth_requests.rs](https://github.com/paradigmxyz/reth/blob/1563506aea09049a85e5cc72c2894f3f7a371581/crates/net/network/src/eth_requests.rs)
```rust,ignore
fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
    let this = self.get_mut();

    loop {
        match this.incoming_requests.poll_next_unpin(cx) {
            Poll::Pending => return Poll::Pending,
            Poll::Ready(None) => return Poll::Ready(()),
            Poll::Ready(Some(incoming)) => match incoming {
                IncomingEthRequest::GetBlockHeaders { peer_id, request, response } => {
                    this.on_headers_request(peer_id, request, response)
                }
                IncomingEthRequest::GetBlockBodies { peer_id, request, response } => {
                    this.on_bodies_request(peer_id, request, response)
                }
                IncomingEthRequest::GetNodeData { .. } => {}
                IncomingEthRequest::GetReceipts { .. } => {}
                IncomingEthRequest::GetReceipts69 { .. } => {}
            },
        }
    }
}
```

这些请求的处理方式比较直接。`GetBlockHeaders` 的 payload 如下：

[File: crates/net/eth-wire/src/types/blocks.rs](https://github.com/paradigmxyz/reth/blob/1563506aea09049a85e5cc72c2894f3f7a371581/crates/net/eth-wire/src/types/blocks.rs)
```rust,ignore
pub struct GetBlockHeaders {
    /// The block number or hash that the peer should start returning headers from.
    pub start_block: BlockHashOrNumber,

    /// The maximum number of headers to return.
    pub limit: u64,

    /// The number of blocks that the node should skip while traversing and returning headers.
    /// A skip value of zero denotes that the peer should return contiguous headers, starting from
    /// [`start_block`](#structfield.start_block) and returning at most
    /// [`limit`](#structfield.limit) headers.
    pub skip: u32,

    /// The direction in which the headers should be returned in.
    pub direction: HeadersDirection,
}
```

处理该请求时，ETH requests 任务会从 `start_block` 开始尝试从数据库获取对应 header，然后根据 `direction`（升序/降序）按 `skip` 递增/递减要获取的区块号（同时检查溢出/下溢），并确保不超过“最大 headers 数/最大字节数”等边界限制。

[File: crates/net/network/src/eth_requests.rs](https://github.com/paradigmxyz/reth/blob/1563506aea09049a85e5cc72c2894f3f7a371581/crates/net/network/src/eth_requests.rs)
```rust,ignore
fn get_headers_response(&self, request: GetBlockHeaders) -> Vec<Header> {
    let GetBlockHeaders { start_block, limit, skip, direction } = request;

    let mut headers = Vec::new();

    let mut block: BlockHashOrNumber = match start_block {
        BlockHashOrNumber::Hash(start) => start.into(),
        BlockHashOrNumber::Number(num) => {
            if let Some(hash) = self.client.block_hash(num.into()).unwrap_or_default() {
                hash.into()
            } else {
                return headers
            }
        }
    };

    let skip = skip as u64;
    let mut total_bytes = APPROX_HEADER_SIZE;

    for _ in 0..limit {
        if let Some(header) = self.client.header_by_hash_or_number(block).unwrap_or_default() {
            match direction {
                HeadersDirection::Rising => {
                    if let Some(next) = (header.number + 1).checked_add(skip) {
                        block = next.into()
                    } else {
                        break
                    }
                }
                HeadersDirection::Falling => {
                    if skip > 0 {
                        // prevent under flows for block.number == 0 and `block.number - skip <
                        // 0`
                        if let Some(next) =
                            header.number.checked_sub(1).and_then(|num| num.checked_sub(skip))
                        {
                            block = next.into()
                        } else {
                            break
                        }
                    } else {
                        block = header.parent_hash.into()
                    }
                }
            }

            headers.push(header);

            if headers.len() >= MAX_HEADERS_SERVE {
                break
            }

            total_bytes += APPROX_HEADER_SIZE;

            if total_bytes > SOFT_RESPONSE_LIMIT {
                break
            }
        } else {
            break
        }
    }

    headers
}
```

`GetBlockBodies` 的 payload 更简单：它只包含一个请求的区块哈希列表：

[File: crates/net/eth-wire/src/types/blocks.rs](https://github.com/paradigmxyz/reth/blob/1563506aea09049a85e5cc72c2894f3f7a371581/crates/net/eth-wire/src/types/blocks.rs)
```rust,ignore
pub struct GetBlockBodies(
    /// The block hashes to request bodies for.
    pub Vec<B256>,
);
```

处理该请求时，ETH requests 任务会按请求顺序，依次尝试从数据库获取每个哈希对应的区块体（transactions 与 ommers），同样会检查“最大 bodies 数/最大字节数”等边界限制：

[File: crates/net/network/src/eth_requests.rs](https://github.com/paradigmxyz/reth/blob/1563506aea09049a85e5cc72c2894f3f7a371581/crates/net/network/src/eth_requests.rs)
```rust,ignore
fn on_bodies_request(
    &mut self,
    _peer_id: PeerId,
    request: GetBlockBodies,
    response: oneshot::Sender<RequestResult<BlockBodies>>,
) {
    let mut bodies = Vec::new();

    let mut total_bytes = APPROX_BODY_SIZE;

    for hash in request.0 {
        if let Some(block) = self.client.block(hash.into()).unwrap_or_default() {
            let body = BlockBody { transactions: block.body, ommers: block.ommers };

            bodies.push(body);

            total_bytes += APPROX_BODY_SIZE;

            if total_bytes > SOFT_RESPONSE_LIMIT {
                break
            }

            if bodies.len() >= MAX_BODIES_SERVE {
                break
            }
        } else {
            break
        }
    }

    let _ = response.send(Ok(BlockBodies(bodies)));
}
```

---

## Transactions Task（交易任务）

交易任务负责监听、请求并传播交易：既包括来自 peers 的交易，也包括本地新增的交易（例如通过 RPC 提交）。注意该任务只关注“交易相关的网络通信”；交易池本身的结构会在 [transaction-pool](https://reth.rs/docs/reth_transaction_pool/index.html) 章节讲解。

与 network management 和 ETH requests 任务类似，transactions 任务也是一个无限 future，在独立的 `tokio::task` 上作为后台任务运行。它由 `TransactionsManager` 表示：

[File: crates/net/network/src/transactions.rs](https://github.com/paradigmxyz/reth/blob/1563506aea09049a85e5cc72c2894f3f7a371581/crates/net/network/src/transactions.rs)
```rust,ignore
pub struct TransactionsManager<Pool, N: NetworkPrimitives = EthNetworkPrimitives> {
    /// Access to the transaction pool.
    pool: Pool,
    /// Network access.
    network: NetworkHandle<N>,
    /// Subscriptions to all network related events.
    ///
    /// From which we get all new incoming transaction related messages.
    network_events: EventStream<NetworkEvent<PeerRequest<N>>>,
    /// Transaction fetcher to handle inflight and missing transaction requests.
    transaction_fetcher: TransactionFetcher<N>,
    /// All currently pending transactions grouped by peers.
    ///
    /// This way we can track incoming transactions and prevent multiple pool imports for the same
    /// transaction
    transactions_by_peers: HashMap<TxHash, HashSet<PeerId>>,
    /// Transactions that are currently imported into the `Pool`.
    pool_imports: FuturesUnordered<PoolImportFuture>,
    /// Stats on pending pool imports that help the node self-monitor.
    pending_pool_imports_info: PendingPoolImportsInfo,
    /// Bad imports.
    bad_imports: LruCache<TxHash>,
    /// All the connected peers.
    peers: HashMap<PeerId, PeerMetadata<N>>,
    /// Send half for the command channel.
    command_tx: mpsc::UnboundedSender<TransactionsCommand<N>>,
    /// Incoming commands from [`TransactionsHandle`].
    command_rx: UnboundedReceiverStream<TransactionsCommand<N>>,
    /// A stream that yields new __pending__ transactions.
    pending_transactions: mpsc::Receiver<TxHash>,
    /// Incoming events from the [`NetworkManager`](crate::NetworkManager).
    transaction_events: UnboundedMeteredReceiver<NetworkTransactionEvent<N>>,
    /// How the `TransactionsManager` is configured.
    config: TransactionsManagerConfig,
    /// Network Policies
    policies: NetworkPolicies<N>,
    /// `TransactionsManager` metrics
    metrics: TransactionsManagerMetrics,
    /// `AnnouncedTxTypes` metrics
    announced_tx_types_metrics: AnnouncedTxTypesMetrics,
}
```

与 ETH requests 任务不同、但与 network management 的 `NetworkHandle` 类似，transactions 任务也可以通过一个可共享的“handle”结构体访问：`TransactionsHandle`。

[File: crates/net/network/src/transactions.rs](https://github.com/paradigmxyz/reth/blob/1563506aea09049a85e5cc72c2894f3f7a371581/crates/net/network/src/transactions.rs)
```rust,ignore
pub struct TransactionsHandle {
    /// Command channel to the [`TransactionsManager`]
    manager_tx: mpsc::UnboundedSender<TransactionsCommand>,
}
```

### Transactions Task 的输入流

后续我们会逐步解释 `TransactionsManager` 的大多数字段，但现在先关注 4 个输入来源（streams）：
- `transaction_events`：监听来自 `NetworkManager` 的 `NetworkTransactionEvent`（纯交易相关事件）
- `network_events`：监听来自 `NetworkManager` 的 `NetworkEvent`（更“元”的事件，例如 peer 会话建立/关闭）
- `command_rx`：监听通过 `TransactionsHandle` 发来的 `TransactionsCommand`
- `pending`：监听 `TransactionPool` 中新进入“pending”的交易

下面我们通过走读 `TransactionManager::poll` 方法来理解交易任务的运行流程。

### Transactions Task 的运行方式

`poll` 方法规定了 transactions 任务的操作顺序。它会按以下顺序 drain（排空/处理）输入流：
1) `TransactionsManager.network_events`
2) `TransactionsManager.command_rx`
3) `TransactionsManager.transaction_events`

然后检查所有 `TransactionsManager.inflight_requests`（节点向 peers 发出的、请求完整交易对象的请求），接着检查已完成的 `TransactionsManager.pool_imports`（正在导入交易池的异步验证结果），最后 drain `TransactionsManager.pending_transactions`（交易池中新进入 pending 的交易）。

[File: crates/net/network/src/transactions.rs](https://github.com/paradigmxyz/reth/blob/1563506aea09049a85e5cc72c2894f3f7a371581/crates/net/network/src/transactions.rs)
```rust,ignore
fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
    let this = self.get_mut();

    // drain network/peer related events
    while let Poll::Ready(Some(event)) = this.network_events.poll_next_unpin(cx) {
        this.on_network_event(event);
    }

    // drain commands
    while let Poll::Ready(Some(cmd)) = this.command_rx.poll_next_unpin(cx) {
        this.on_command(cmd);
    }

    // drain incoming transaction events
    while let Poll::Ready(Some(event)) = this.transaction_events.poll_next_unpin(cx) {
        this.on_network_tx_event(event);
    }

    // Advance all requests.
    // We remove each request one by one and add them back.
    for idx in (0..this.inflight_requests.len()).rev() {
        let mut req = this.inflight_requests.swap_remove(idx);
        match req.response.poll_unpin(cx) {
            Poll::Pending => {
                this.inflight_requests.push(req);
            }
            Poll::Ready(Ok(Ok(txs))) => {
                this.import_transactions(req.peer_id, txs.0);
            }
            Poll::Ready(Ok(Err(_))) => {
                this.report_bad_message(req.peer_id);
            }
            Poll::Ready(Err(_)) => {
                this.report_bad_message(req.peer_id);
            }
        }
    }

    // Advance all imports
    while let Poll::Ready(Some(import_res)) = this.pool_imports.poll_next_unpin(cx) {
        match import_res {
            Ok(hash) => {
                this.on_good_import(hash);
            }
            Err(err) => {
                this.on_bad_import(*err.hash());
            }
        }
    }

    // handle and propagate new transactions
    let mut new_txs = Vec::new();
    while let Poll::Ready(Some(hash)) = this.pending_transactions.poll_next_unpin(cx) {
        new_txs.push(hash);
    }
    if !new_txs.is_empty() {
        this.on_new_transactions(new_txs);
    }

    // all channels are fully drained and import futures pending

    Poll::Pending
}
```

接下来我们按顺序讲解每一步发生了什么：从 drain `TransactionsManager.network_events` 开始。

#### 处理 `NetworkEvent`

之所以优先处理 `TransactionsManager.network_events`，是因为它包含 peer 会话开启/关闭等事件。这样可以确保：在处理某个 peer 发来的交易相关事件之前，`TransactionsManager` 已经把该 peer 正确登记/移除。

这个 channel 里收到的事件类型为 `NetworkEvent`：

[File: crates/net/network/src/manager.rs](https://github.com/paradigmxyz/reth/blob/1563506aea09049a85e5cc72c2894f3f7a371581/crates/net/network/src/manager.rs)

```rust,ignore
pub enum NetworkEvent<R = PeerRequest> {
    /// Basic peer lifecycle event.
    Peer(PeerEvent),
    /// Session established with requests.
    ActivePeerSession {
        /// Session information
        info: SessionInfo,
        /// A request channel to the session task.
        messages: PeerRequestSender<R>,
    },
}
```

以及：
```rust,ignore
pub enum PeerEvent {
    /// Closed the peer session.
    SessionClosed {
        /// The identifier of the peer to which a session was closed.
        peer_id: PeerId,
        /// Why the disconnect was triggered
        reason: Option<DisconnectReason>,
    },
    /// Established a new session with the given peer.
    SessionEstablished(SessionInfo),
    /// Event emitted when a new peer is added
    PeerAdded(PeerId),
    /// Event emitted when a new peer is removed
    PeerRemoved(PeerId),
}
```
[File: crates/net/network-api/src/events.rs](https://github.com/paradigmxyz/reth/blob/c46b5fc1157d12184d1dceb4dc45e26cf74b2bc6/crates/net/network-api/src/events.rs)

它们会在 `on_network_event` 中处理：主要通过 `NetworkEvent::Peer(PeerEvent::SessionClosed)`、`NetworkEvent::Peer(PeerEvent::SessionEstablished)`、`NetworkEvent::ActivePeerSession` 这几类事件来初始化 peer 连接与交易广播。

`PeerEvent` 的各个变体大致含义如下：

**`PeerEvent::PeerAdded`**  
通过 network handle 将 peer 加入到节点的网络管理中。

**`PeerEvent::PeerRemoved`**  
从 `TransactionsManager.peers` map 中移除 `NetworkEvent::SessionClosed.peer_id` 指定的 peer。

**`PeerEvent::SessionClosed`**  
在断开连接后关闭 peer 会话。

**`PeerEvent::SessionEstablished`**  
首先把一个 `PeerMetadata` 按 `peer_id` 插入到 `TransactionsManager.peers`。`PeerMetadata` 的结构如下：

[File: crates/net/network/src/transactions.rs](https://github.com/paradigmxyz/reth/blob/1563506aea09049a85e5cc72c2894f3f7a371581/crates/net/network/src/transactions.rs)
```rust,ignore
pub struct PeerMetadata<N: NetworkPrimitives = EthNetworkPrimitives> {
    /// Optimistically keeps track of transactions that we know the peer has seen.
    seen_transactions: LruCache<TxHash>,
    /// A communication channel directly to the peer's session task.
    request_tx: PeerRequestSender<PeerRequest<N>>,
    /// Negotiated version of the session.
    version: EthVersion,
    /// The peer's client version.
    client_version: Arc<str>,
    /// The kind of peer.
    peer_kind: PeerKind,
}
```

注意 `PeerMetadata` 包含 `seen_transactions` 字段：它是一个该 peer 已知交易的 [LRU cache](https://en.wikipedia.org/wiki/Cache_replacement_policies#Least_recently_used_(LRU))。

`PeerMetadata.request_tx` 字段是一个 channel 的发送端，用于向该 peer 的 session task 发送请求。

当把 `PeerMetadata` 插入到 `TransactionsManager.peers` 后，会把交易池中所有交易的哈希通过 [`NewPooledTransactionHashes` 消息](https://github.com/ethereum/devp2p/blob/master/caps/eth.md#newpooledtransactionhashes-0x08) 发送给该 peer。

[File: crates/net/network/src/transactions.rs](https://github.com/paradigmxyz/reth/blob/1563506aea09049a85e5cc72c2894f3f7a371581/crates/net/network/src/transactions.rs)
```rust,ignore
fn on_network_event(&mut self, event_result: NetworkEvent) {
    match event_result {
        NetworkEvent::Peer(PeerEvent::SessionClosed { peer_id, .. }) => {
            // remove the peer
            self.peers.remove(&peer_id);
            self.transaction_fetcher.remove_peer(&peer_id);
        }
        NetworkEvent::ActivePeerSession { info, messages } => {
            // process active peer session and broadcast available transaction from the pool
            self.handle_peer_session(info, messages);
        }
        NetworkEvent::Peer(PeerEvent::SessionEstablished(info)) => {
            let peer_id = info.peer_id;
            // get messages from existing peer
             let messages = match self.peers.get(&peer_id) {
                Some(p) => p.request_tx.clone(),
                None => {
                    debug!(target: "net::tx", ?peer_id, "No peer request sender found");
                    return;
                }
            };
            self.handle_peer_session(info, messages);
        }
         _ => {}
    }
}
```

#### 处理 `TransactionsCommand`

`poll` 方法下一步会处理从 `TransactionsManager.command_rx` 流进入的 `TransactionsCommand`。它们会优先于从网络收集到的交易请求被处理，因为它们是通过 `TransactionsHandle` “手工”发送的命令。`TransactionsCommand` 枚举在撰写本文时的形式如下：

[File: crates/net/network/src/transactions.rs](https://github.com/paradigmxyz/reth/blob/1563506aea09049a85e5cc72c2894f3f7a371581/crates/net/network/src/transactions.rs)
```rust,ignore
enum TransactionsCommand {
    PropagateHash(B256),
}
```

`TransactionsCommand` 由 `on_command` 处理：它会对目前唯一的变体 `TransactionsCommand::PropagateHash` 调用 `on_new_transactions`，并传入一个只包含该 hash 的迭代器（虽然该方法也支持传入多个交易哈希）。

`on_new_transactions` 会通过 `propagate_transactions` 把完整的交易对象（附带 signer）传播给一小部分随机 peers；然后再通知其他 peers 新交易的 hash，使得它们如果还没有该交易对象，就可以请求获取。

[File: crates/net/network/src/transactions.rs](https://github.com/paradigmxyz/reth/blob/1563506aea09049a85e5cc72c2894f3f7a371581/crates/net/network/src/transactions.rs)
```rust,ignore
fn on_new_transactions(&mut self, hashes: impl IntoIterator<Item = TxHash>) {
    trace!(target: "net::tx", "Start propagating transactions");

    let propagated = self.propagate_transactions(
        self.pool
            .get_all(hashes)
            .into_iter()
            .map(|tx| {
                (*tx.hash(), Arc::new(tx.transaction.to_recovered_transaction().into_tx()))
            })
            .collect(),
    );

    // notify pool so events get fired
    self.pool.on_propagated(propagated);
}

fn propagate_transactions(
    &mut self,
    txs: Vec<(TxHash, Arc<TransactionSigned>)>,
) -> PropagatedTransactions {
    let mut propagated = PropagatedTransactions::default();

    // send full transactions to a fraction of the connected peers (square root of the total
    // number of connected peers)
    let max_num_full = (self.peers.len() as f64).sqrt() as usize + 1;

    // Note: Assuming ~random~ order due to random state of the peers map hasher
    for (idx, (peer_id, peer)) in self.peers.iter_mut().enumerate() {
        let (hashes, full): (Vec<_>, Vec<_>) =
            txs.iter().filter(|(hash, _)| peer.transactions.insert(*hash)).cloned().unzip();

        if !full.is_empty() {
            if idx > max_num_full {
                for hash in &hashes {
                    propagated.0.entry(*hash).or_default().push(PropagateKind::Hash(*peer_id));
                }
                // send hashes of transactions
                self.network.send_transactions_hashes(*peer_id, hashes);
            } else {
                // send full transactions
                self.network.send_transactions(*peer_id, full);

                for hash in hashes {
                    propagated.0.entry(hash).or_default().push(PropagateKind::Full(*peer_id));
                }
            }
        }
    }

    propagated
}
```

#### 处理 `NetworkTransactionEvent`

处理完 `TransactionsCommand` 后，就轮到处理来自网络的交易相关事件：`poll` 会处理 `TransactionsManager.transaction_events` 流中的 `NetworkTransactionEvent`。其形式如下：

[File: crates/net/network/src/transactions.rs](https://github.com/paradigmxyz/reth/blob/1563506aea09049a85e5cc72c2894f3f7a371581/crates/net/network/src/transactions.rs)
```rust,ignore
pub enum NetworkTransactionEvent<N: NetworkPrimitives = EthNetworkPrimitives> {
    /// Received list of transactions from the given peer.
    IncomingTransactions { peer_id: PeerId, msg: Transactions<N::BroadcastedTransaction> },
    /// Received list of transactions hashes to the given peer.
    IncomingPooledTransactionHashes { peer_id: PeerId, msg: NewPooledTransactionHashes },
    /// Incoming `GetPooledTransactions` request from a peer.
    GetPooledTransactions {
        peer_id: PeerId,
        request: GetPooledTransactions,
        response: oneshot::Sender<RequestResult<PooledTransactions<N::PooledTransaction>>>,
    },
}
```

这些事件由 `on_network_tx_event` 处理，对各个变体的响应方式如下：

**`NetworkTransactionEvent::IncomingTransactions`**

该事件由 [`Transactions` 协议消息](https://github.com/ethereum/devp2p/blob/master/caps/eth.md#transactions-0x02) 触发，并由 `import_transactions` 处理。

对变体的 `msg` 中每笔交易，系统会尝试恢复 signer，并把该交易哈希写入由 `peer_id` 对应的 `PeerMetadata` 的 LRU cache；同时把 `peer_id` 加入到 `TransactionsManager.transactions_by_peers` 中以交易哈希为 key 的 peer ID 集合里。如果该交易哈希此前没有记录，则开始异步将交易对象导入到交易池：向 `TransactionsManager.pool_imports` 加入一个 `PoolImportFuture`。如果 signer 恢复失败，会对该 `peer_id` 调用 `report_bad_message`，降低该 peer 的信誉分。

为了更好理解，我们回过头看看 `TransactionsManager.transactions_by_peers` 与 `TransactionsManager.pool_imports` 的用途。

`TransactionsManager.transactions_by_peers` 是一个 `HashMap<TxHash, Vec<PeerId>>`，用于跟踪“哪些 peers 给我们发送了某个 hash 的交易”。它有两个作用：第一，避免对同一笔交易重复发起导入流程（这在 `import_transactions` 中检查）；第二，如果某笔交易导入交易池时发现它是畸形/不合法的，可以降低所有传播该交易的 peers 的信誉分（发生在 `on_bad_import`，稍后会提到）。

`TransactionsManager.pool_imports` 是一组 futures，代表“正在导入交易池”的交易。由于导入过程包含异步的交易验证，因此需要保存 future 以便后续 poll 并处理结果。

[File: crates/net/network/src/transactions.rs](https://github.com/paradigmxyz/reth/blob/1563506aea09049a85e5cc72c2894f3f7a371581/crates/net/network/src/transactions.rs)
```rust,ignore
fn import_transactions(&mut self, peer_id: PeerId, transactions: Vec<TransactionSigned>) {
    let mut has_bad_transactions = false;
    if let Some(peer) = self.peers.get_mut(&peer_id) {
        for tx in transactions {
            // recover transaction
            let tx = if let Some(tx) = tx.into_ecrecovered() {
                tx
            } else {
                has_bad_transactions = true;
                continue
            };

            // track that the peer knows this transaction
            peer.transactions.insert(tx.hash());

            match self.transactions_by_peers.entry(tx.hash()) {
                Entry::Occupied(mut entry) => {
                    // transaction was already inserted
                    entry.get_mut().push(peer_id);
                }
                Entry::Vacant(entry) => {
                    // this is a new transaction that should be imported into the pool
                    let pool_transaction = <Pool::Transaction as FromRecoveredTransaction>::from_recovered_transaction(tx);

                    let pool = self.pool.clone();
                    let import = Box::pin(async move {
                        pool.add_external_transaction(pool_transaction).await
                    });

                    self.pool_imports.push(import);
                    entry.insert(vec![peer_id]);
                }
            }
        }
    }

    if has_bad_transactions {
        self.report_bad_message(peer_id);
    }
}
```

**`NetworkTransactionEvent::IncomingPooledTransactionHashes`**

该事件由 [`NewPooledTransactionHashes` 协议消息](https://github.com/ethereum/devp2p/blob/master/caps/eth.md#newpooledtransactionhashes-0x08) 触发，由 `on_new_pooled_transactions` 处理。

它会先把 `NewPooledTransactionHashes` 中的交易哈希添加到 `TransactionsManager.peers` 中 `peer_id` 对应 `PeerMetadata` 的 LRU cache。然后过滤出交易池中还不存在的那些哈希，并通过 `PeerMetadata.request_tx` 向该 peer 发送 [`GetPooledTransactions` 协议消息](https://github.com/ethereum/devp2p/blob/master/caps/eth.md#getpooledtransactions-0x09) 请求完整交易对象。若请求发送成功，则将一个 `GetPooledTxRequest` 加入 `TransactionsManager.inflight_requests` 向量：

[File: crates/net/network/src/transactions.rs](https://github.com/paradigmxyz/reth/blob/1563506aea09049a85e5cc72c2894f3f7a371581/crates/net/network/src/transactions.rs)
```rust,ignore
struct GetPooledTxRequest {
    peer_id: PeerId,
    response: oneshot::Receiver<RequestResult<PooledTransactions>>,
}
```

该结构体也包含 `response` channel，用于之后 poll peer 的响应。

[File: crates/net/network/src/transactions.rs](https://github.com/paradigmxyz/reth/blob/1563506aea09049a85e5cc72c2894f3f7a371581/crates/net/network/src/transactions.rs)
```rust,ignore
fn on_new_pooled_transactions(&mut self, peer_id: PeerId, msg: NewPooledTransactionHashes) {
    if let Some(peer) = self.peers.get_mut(&peer_id) {
        let mut transactions = msg.0;

        // keep track of the transactions the peer knows
        peer.transactions.extend(transactions.clone());

        self.pool.retain_unknown(&mut transactions);

        if transactions.is_empty() {
            // nothing to request
            return
        }

        // request the missing transactions
        let (response, rx) = oneshot::channel();
        let req = PeerRequest::GetPooledTransactions {
            request: GetPooledTransactions(transactions),
            response,
        };

        if peer.request_tx.try_send(req).is_ok() {
            self.inflight_requests.push(GetPooledTxRequest { peer_id, response: rx })
        }
    }
}
```

**`NetworkTransactionEvent::GetPooledTransactions`**

该事件由 [`GetPooledTransactions` 协议消息](https://github.com/ethereum/devp2p/blob/master/caps/eth.md#getpooledtransactions-0x09) 触发，由 `on_get_pooled_transactions` 处理。

它会从交易池收集所请求的全部交易，恢复其 signer，把交易哈希加入请求方 peer 的 LRU cache，并通过 [`PooledTransactions` 协议消息](https://github.com/ethereum/devp2p/blob/master/caps/eth.md#pooledtransactions-0x0a) 把交易发送回去。发送使用的是 `NetworkTransaction::GetPooledTransactions` 变体中携带的 `response` channel。

[File: crates/net/network/src/transactions.rs](https://github.com/paradigmxyz/reth/blob/1563506aea09049a85e5cc72c2894f3f7a371581/crates/net/network/src/transactions.rs)
```rust,ignore
fn on_get_pooled_transactions(
    &mut self,
    peer_id: PeerId,
    request: GetPooledTransactions,
    response: oneshot::Sender<RequestResult<PooledTransactions>>,
) {
    if let Some(peer) = self.peers.get_mut(&peer_id) {
        let transactions = self
            .pool
            .get_all(request.0)
            .into_iter()
            .map(|tx| tx.transaction.to_recovered_transaction().into_tx())
            .collect::<Vec<_>>();

        // we sent a response at which point we assume that the peer is aware of the transaction
        peer.transactions.extend(transactions.iter().map(|tx| tx.hash()));

        let resp = PooledTransactions(transactions);
        let _ = response.send(Ok(resp));
    }
}
```

#### 检查 `inflight_requests`

当 `TransactionsManager.network_events`、`TransactionsManager.command_rx` 与 `TransactionsManager.transaction_events` 都被 drain 后，`poll` 方法会检查所有 `inflight_requests` 的状态。

对每个 in-flight request，会 poll `GetPooledTxRequest.response`。如果仍未就绪，则把它留在 `TransactionsManager.inflight_requests` 中；如果成功收到 peer 的 `PooledTransactions` 响应，则交给 `import_transactions` 处理；否则如果在 poll 响应时发生错误，则对该 peer 调用 `report_bad_message`（降低其信誉分）。

#### 检查 `pool_imports`

在处理完已完成的 `inflight_requests` 并把新一轮 `PoolImportFuture` 加入 `TransactionsManager.pool_imports` 后，`poll` 会继续检查 `pool_imports` 的状态。

它会遍历并 poll `TransactionsManager.pool_imports`：如果某个 future 已就绪（resolve），则根据导入结果分别调用 `on_good_import`（成功）或 `on_bad_import`（失败）。

`on_good_import` 在交易成功导入交易池时被调用：它会从 `TransactionsManager.transactions_by_peers` 中移除该交易哈希对应的条目。

`on_bad_import` 同样会移除对应条目，但还会对条目里记录的所有 peers 调用 `report_bad_message`，因为这些 peers 传播了一笔无法被验证的交易。

#### 检查 `pending_transactions`

最后，`poll` 会 drain `TransactionsManager.pending_transactions` 流。这些交易要么来自 peer 传播（前面已经讲了处理流程），要么来自本地 RPC 提交；并且它们已经通过验证、进入交易池。

它会把收到的所有 hash 收集到一个向量，并调用 `on_new_transactions`。该方法的逻辑在前面处理 `TransactionsCommand::PropagateHash` 时已经解释过。

