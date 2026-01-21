//! Engine node related functionality.

use crate::{
    common::{Attached, LaunchContextWith, WithConfigs},
    hooks::NodeHooks,
    rpc::{EngineShutdown, EngineValidatorAddOn, EngineValidatorBuilder, RethRpcAddOns, RpcHandle},
    setup::build_networked_pipeline,
    AddOns, AddOnsContext, FullNode, LaunchContext, LaunchNode, NodeAdapter,
    NodeBuilderWithComponents, NodeComponents, NodeComponentsBuilder, NodeHandle, NodeTypesAdapter,
};
use alloy_consensus::BlockHeader;
use futures::{stream_select, FutureExt, StreamExt};
use reth_chainspec::{EthChainSpec, EthereumHardforks};
use reth_engine_service::service::{ChainEvent, EngineService};
use reth_engine_tree::{
    chain::FromOrchestrator,
    engine::{EngineApiRequest, EngineRequestHandler},
    tree::TreeConfig,
};
use reth_engine_util::EngineMessageStreamExt;
use reth_exex::ExExManagerHandle;
use reth_network::{types::BlockRangeUpdate, NetworkSyncUpdater, SyncState};
use reth_network_api::BlockDownloaderProvider;
use reth_node_api::{
    BuiltPayload, ConsensusEngineHandle, FullNodeTypes, NodeTypes, NodeTypesWithDBAdapter,
};
use reth_node_core::{
    dirs::{ChainPath, DataDirPath},
    exit::NodeExitFuture,
    primitives::Head,
};
use reth_node_events::node;
use reth_provider::{
    providers::{BlockchainProvider, NodeTypesForProvider},
    BlockNumReader, MetadataProvider,
};
use reth_tasks::TaskExecutor;
use reth_tokio_util::EventSender;
use reth_tracing::tracing::{debug, error, info};
use reth_trie_db::ChangesetCache;
use std::{future::Future, pin::Pin, sync::Arc};
use tokio::sync::{mpsc::unbounded_channel, oneshot};
use tokio_stream::wrappers::UnboundedReceiverStream;
use tracing::warn;

/// 引擎节点启动器。
#[derive(Debug)]
pub struct EngineNodeLauncher {
    /// 节点的任务执行上下文。
    pub ctx: LaunchContext,

    /// 引擎树（engine tree）的临时配置。
    /// 待引擎稳定后，应通过节点构建器进行配置。
    pub engine_tree_config: TreeConfig,
}

impl EngineNodeLauncher {
    /// 创建一个新的以太坊节点启动器实例。
    pub const fn new(
        task_executor: TaskExecutor,
        data_dir: ChainPath<DataDirPath>,
        engine_tree_config: TreeConfig,
    ) -> Self {
        Self { ctx: LaunchContext::new(task_executor, data_dir), engine_tree_config }
    }

    /// 启动节点的核心逻辑。
    /// run step 011
    async fn launch_node<T, CB, AO>(
        self,
        target: NodeBuilderWithComponents<T, CB, AO>,
    ) -> eyre::Result<NodeHandle<NodeAdapter<T, CB::Components>, AO>>
    where
        T: FullNodeTypes<
            Types: NodeTypesForProvider,
            Provider = BlockchainProvider<
                NodeTypesWithDBAdapter<<T as FullNodeTypes>::Types, <T as FullNodeTypes>::DB>,
            >,
        >,
        CB: NodeComponentsBuilder<T>,
        AO: RethRpcAddOns<NodeAdapter<T, CB::Components>>
            + EngineValidatorAddOn<NodeAdapter<T, CB::Components>>,
    {
        let Self { ctx, engine_tree_config } = self;
        let NodeBuilderWithComponents {
            adapter: NodeTypesAdapter { database },
            components_builder,
            add_ons: AddOns { hooks, exexs: installed_exex, add_ons },
            config,
        } = target;
        let NodeHooks { on_component_initialized, on_node_started, .. } = hooks;

        // 创建在整个引擎中共享的变更集（changeset）缓存
        let changeset_cache = ChangesetCache::new();

        // 设置启动上下文
        let ctx = ctx
            .with_configured_globals(engine_tree_config.reserved_cpu_cores())
            // 加载 toml 配置
            .with_loaded_toml_config(config)?
            // 添加解析后的对等节点
            .with_resolved_peers()?
            // 挂载数据库
            .attach(database.clone())
            // 确保某些设置生效
            .with_adjusted_configs()
            // 创建带有变更集缓存的提供者工厂
            .with_provider_factory::<_, <CB::Components as NodeComponents<T>>::Evm>(changeset_cache.clone()).await?
            .inspect(|ctx| {
                info!(target: "reth::cli", "数据库已打开");
                match ctx.provider_factory().storage_settings() {
                    Ok(settings) => {
                        info!(
                            target: "reth::cli",
                            ?settings,
                            "存储设置"
                        );
                    },
                    Err(err) => {
                        warn!(
                            target: "reth::cli",
                            ?err,
                            "获取存储设置失败"
                        );
                    },
                }
            })
            .with_prometheus_server().await?
            .inspect(|this| {
                debug!(target: "reth::cli", chain=%this.chain_id(), genesis=?this.genesis_hash(), "正在初始化创世状态");
            })
            .with_genesis()?
            .inspect(|this: &LaunchContextWith<Attached<WithConfigs<<T::Types as NodeTypes>::ChainSpec>, _>>| {
                info!(target: "reth::cli", "\n{}", this.chain_spec().display_hardforks());
            })
            .with_metrics_task()
            // 在此处传递 FullNodeTypes 作为类型参数，以便后续构建组件
            .with_blockchain_db::<T, _>(move |provider_factory| {
                Ok(BlockchainProvider::new(provider_factory)?)
            })?
            // 构建所有组件
            .with_components(components_builder, on_component_initialized).await?;

        // 如果安装了 ExEx（Execution Extensions），则启动它们
        let maybe_exex_manager_handle = ctx.launch_exex(installed_exex).await?;

        // 创建网络流水线
        let network_handle = ctx.components().network().clone();
        let network_client = network_handle.fetch_client().await?;
        let (consensus_engine_tx, consensus_engine_rx) = unbounded_channel();

        let node_config = ctx.node_config();

        // 默认重启后节点处于同步状态
        network_handle.update_sync_state(SyncState::Syncing);

        let max_block = ctx.max_block(network_client.clone()).await?;

        let static_file_producer = ctx.static_file_producer();
        let static_file_producer_events = static_file_producer.lock().events();
        info!(target: "reth::cli", "StaticFileProducer 已初始化");

        let consensus = Arc::new(ctx.components().consensus().clone());

        // 构建带有网络能力的流水线
        let pipeline = build_networked_pipeline(
            &ctx.toml_config().stages,
            network_client.clone(),
            consensus.clone(),
            ctx.provider_factory().clone(),
            ctx.task_executor(),
            ctx.sync_metrics_tx(),
            ctx.prune_config(),
            max_block,
            static_file_producer,
            ctx.components().evm_config().clone(),
            maybe_exex_manager_handle.clone().unwrap_or_else(ExExManagerHandle::empty),
            ctx.era_import_source(),
        )?;

        // 新引擎直接写入静态文件，确保静态文件更新到最新高度
        pipeline.move_to_static_files()?;

        let pipeline_events = pipeline.events();

        let mut pruner_builder = ctx.pruner_builder();
        if let Some(exex_manager_handle) = &maybe_exex_manager_handle {
            pruner_builder =
                pruner_builder.finished_exex_height(exex_manager_handle.finished_height());
        }
        let pruner = pruner_builder.build_with_provider_factory(ctx.provider_factory().clone());
        let pruner_events = pruner.events();
        info!(target: "reth::cli", prune_config=?ctx.prune_config(), "修剪器（Pruner）已初始化");

        let event_sender = EventSender::default();

        let beacon_engine_handle = ConsensusEngineHandle::new(consensus_engine_tx.clone());

        // 如果可能，从参数中提取 JWT 密钥
        let jwt_secret = ctx.auth_jwt_secret()?;

        let add_ons_ctx = AddOnsContext {
            node: ctx.node_adapter().clone(),
            config: ctx.node_config(),
            beacon_engine_handle: beacon_engine_handle.clone(),
            jwt_secret,
            engine_events: event_sender.clone(),
        };
        let validator_builder = add_ons.engine_validator_builder();

        // 使用所有必需组件构建引擎验证器
        let engine_validator = validator_builder
            .clone()
            .build_tree_validator(&add_ons_ctx, engine_tree_config.clone(), changeset_cache.clone())
            .await?;

        // 创建带有可选重组（reorg）支持的共识引擎消息流
        let consensus_engine_stream = UnboundedReceiverStream::from(consensus_engine_rx)
            .maybe_skip_fcu(node_config.debug.skip_fcu)
            .maybe_skip_new_payload(node_config.debug.skip_new_payload)
            .maybe_reorg(
                ctx.blockchain_db().clone(),
                ctx.components().evm_config().clone(),
                || async {
                    // 为重组验证器创建单独的缓存（不与主引擎共享）
                    let reorg_cache = ChangesetCache::new();
                    validator_builder
                        .build_tree_validator(&add_ons_ctx, engine_tree_config.clone(), reorg_cache)
                        .await
                },
                node_config.debug.reorg_frequency,
                node_config.debug.reorg_depth,
            )
            .await?
            // 在跳过处理之后存储消息，以便 `replay-engine` 命令仅回放引擎实际观察到的消息
            .maybe_store_messages(node_config.debug.engine_api_store.clone());

        let mut engine_service = EngineService::new(
            consensus.clone(),
            ctx.chain_spec(),
            network_client.clone(),
            Box::pin(consensus_engine_stream),
            pipeline,
            Box::new(ctx.task_executor().clone()),
            ctx.provider_factory().clone(),
            ctx.blockchain_db().clone(),
            pruner,
            ctx.components().payload_builder_handle().clone(),
            engine_validator,
            engine_tree_config,
            ctx.sync_metrics_tx(),
            ctx.components().evm_config().clone(),
            changeset_cache,
        );

        info!(target: "reth::cli", "共识引擎已初始化");

        #[allow(clippy::needless_continue)]
        let events = stream_select!(
            event_sender.new_listener().map(Into::into),
            pipeline_events.map(Into::into),
            ctx.consensus_layer_events(),
            pruner_events.map(Into::into),
            static_file_producer_events.map(Into::into),
        );

        // 启动关键事件处理任务
        ctx.task_executor().spawn_critical(
            "events task",
            Box::pin(node::handle_events(
                Some(Box::new(ctx.components().network().clone())),
                Some(ctx.head().number),
                events,
            )),
        );

        // 启动附加组件（如 RPC 服务）
        let RpcHandle {
            rpc_server_handles,
            rpc_registry,
            engine_events,
            beacon_engine_handle,
            engine_shutdown: _,
        } = add_ons.launch_add_ons(add_ons_ctx).await?;

        // 创建引擎关闭句柄
        let (engine_shutdown, shutdown_rx) = EngineShutdown::new();

        // 运行共识引擎直至完成
        let initial_target = ctx.initial_backfill_target()?;
        let mut built_payloads = ctx
            .components()
            .payload_builder_handle()
            .subscribe()
            .await
            .map_err(|e| eyre::eyre!("订阅 Payload 构建器事件失败: {:?}", e))?
            .into_built_payload_stream()
            .fuse();

        let chainspec = ctx.chain_spec();
        let provider = ctx.blockchain_db().clone();
        let (exit, rx) = oneshot::channel();
        let terminate_after_backfill = ctx.terminate_after_initial_backfill();
        let startup_sync_state_idle = ctx.node_config().debug.startup_sync_state_idle;

        info!(target: "reth::cli", "正在启动共识引擎");
        let consensus_engine = async move {
            // 如果有初始回填目标，则开始回填同步
            if let Some(initial_target) = initial_target {
                debug!(target: "reth::cli", %initial_target,  "开始回填同步");
                // network_handle 的同步状态在 Syncing 时已经初始化
                engine_service.orchestrator_mut().start_backfill_sync(initial_target);
            } else if startup_sync_state_idle {
                network_handle.update_sync_state(SyncState::Idle);
            }

            let mut res = Ok(());
            let mut shutdown_rx = shutdown_rx.fuse();

            // 推进区块链并等待本地构建的 payload，以便将其加入引擎 API 树处理器，
            // 从而防止从 CL 接收到该区块作为 payload 时进行重复执行。
            loop {
                tokio::select! {
                    shutdown_req = &mut shutdown_rx => {
                        if let Ok(req) = shutdown_req {
                            debug!(target: "reth::cli", "收到引擎关闭请求");
                            engine_service.orchestrator_mut().handler_mut().handler_mut().on_event(
                                FromOrchestrator::Terminate { tx: req.done_tx }.into()
                            );
                        }
                    }
                    payload = built_payloads.select_next_some() => {
                        if let Some(executed_block) = payload.executed_block() {
                            debug!(target: "reth::cli", block=?executed_block.recovered_block.num_hash(),  "正在插入构建的 payload");
                            engine_service.orchestrator_mut().handler_mut().handler_mut().on_event(EngineApiRequest::InsertExecutedBlock(executed_block.into_executed_payload()).into());
                        }
                    }
                    event = engine_service.next() => {
                        let Some(event) = event else { break };
                        debug!(target: "reth::cli", "事件: {event}");
                        match event {
                            ChainEvent::BackfillSyncFinished => {
                                if terminate_after_backfill {
                                    debug!(target: "reth::cli", "初始回填后终止");
                                    break
                                }
                                if startup_sync_state_idle {
                                    network_handle.update_sync_state(SyncState::Idle);
                                }
                            }
                            ChainEvent::BackfillSyncStarted => {
                                network_handle.update_sync_state(SyncState::Syncing);
                            }
                            ChainEvent::FatalError => {
                                error!(target: "reth::cli", "共识引擎发生致命错误");
                                res = Err(eyre::eyre!("共识引擎发生致命错误"));
                                break
                            }
                            ChainEvent::Handler(ev) => {
                                if let Some(head) = ev.canonical_header() {
                                    // 一旦通过 live sync 进行推进，我们可以认为节点不再处于同步状态
                                    network_handle.update_sync_state(SyncState::Idle);
                                    let head_block = Head {
                                        number: head.number(),
                                        hash: head.hash(),
                                        difficulty: head.difficulty(),
                                        timestamp: head.timestamp(),
                                        total_difficulty: chainspec.final_paris_total_difficulty()
                                            .filter(|_| chainspec.is_paris_active_at_block(head.number()))
                                            .unwrap_or_default(),
                                    };
                                    network_handle.update_status(head_block);

                                    let updated = BlockRangeUpdate {
                                        earliest: provider.earliest_block_number().unwrap_or_default(),
                                        latest: head.number(),
                                        latest_hash: head.hash(),
                                    };
                                    network_handle.update_block_range(updated);
                                }
                                event_sender.notify(ev);
                            }
                        }
                    }
                }
            }

            let _ = exit.send(res);
        };
        ctx.task_executor().spawn_critical("共识引擎", Box::pin(consensus_engine));

        let engine_events_for_ethstats = engine_events.new_listener();

        let full_node = FullNode {
            evm_config: ctx.components().evm_config().clone(),
            pool: ctx.components().pool().clone(),
            network: ctx.components().network().clone(),
            provider: ctx.node_adapter().provider.clone(),
            payload_builder_handle: ctx.components().payload_builder_handle().clone(),
            task_executor: ctx.task_executor().clone(),
            config: ctx.node_config().clone(),
            data_dir: ctx.data_dir().clone(),
            add_ons_handle: RpcHandle {
                rpc_server_handles,
                rpc_registry,
                engine_events,
                beacon_engine_handle,
                engine_shutdown,
            },
        };
        // 节点启动后的通知
        on_node_started.on_event(FullNode::clone(&full_node))?;

        ctx.spawn_ethstats(engine_events_for_ethstats).await?;

        let handle = NodeHandle {
            node_exit_future: NodeExitFuture::new(
                async { rx.await? },
                full_node.config.debug.terminate,
            ),
            node: full_node,
        };

        Ok(handle)
    }
}

impl<T, CB, AO> LaunchNode<NodeBuilderWithComponents<T, CB, AO>> for EngineNodeLauncher
where
    T: FullNodeTypes<
        Types: NodeTypesForProvider,
        Provider = BlockchainProvider<
            NodeTypesWithDBAdapter<<T as FullNodeTypes>::Types, <T as FullNodeTypes>::DB>,
        >,
    >,
    CB: NodeComponentsBuilder<T> + 'static,
    AO: RethRpcAddOns<NodeAdapter<T, CB::Components>>
        + EngineValidatorAddOn<NodeAdapter<T, CB::Components>>
        + 'static,
{
    type Node = NodeHandle<NodeAdapter<T, CB::Components>, AO>;
    type Future = Pin<Box<dyn Future<Output = eyre::Result<Self::Node>> + Send>>;

    fn launch_node(self, target: NodeBuilderWithComponents<T, CB, AO>) -> Self::Future {
        Box::pin(self.launch_node(target))
    }
}
