#![allow(missing_docs)]

// 为整个进程设置全局内存分配器（Allocator）。
// 这会影响所有堆分配（Vec/String/Box 等），用于统一内存策略与可观测性。
#[global_allocator]
static ALLOC: reth_cli_util::allocator::Allocator = reth_cli_util::allocator::new_allocator();

// 在 Unix 且启用 `jemalloc-prof` feature 时，导出 jemalloc 的配置：
// - prof:true/prof_active:true：启用并激活 heap profiling
// - lg_prof_sample:19：采样率（2^19 字节一次采样）
#[cfg(all(feature = "jemalloc-prof", unix))]
#[unsafe(export_name = "_rjem_malloc_conf")]
static MALLOC_CONF: &[u8] = b"prof:true,prof_active:true,lg_prof_sample:19\0";

use clap::Parser;
use reth::{args::RessArgs, cli::Cli, ress::install_ress_subprotocol};
use reth_ethereum_cli::chainspec::EthereumChainSpecParser;
use reth_node_builder::NodeHandle;
use reth_node_ethereum::EthereumNode;
use tracing::info;

fn main() {
    // 安装 SIGSEGV（段错误）处理器：在崩溃时尽可能给出更好的诊断信息。
    reth_cli_util::sigsegv_handler::install();

    // 如果用户没显式设置 RUST_BACKTRACE，则默认启用 backtrace，便于排查 panic。
    if std::env::var_os("RUST_BACKTRACE").is_none() {
        unsafe { std::env::set_var("RUST_BACKTRACE", "1") };
    }

    // 解析 CLI 参数并运行对应子命令；若发生错误则打印并以非 0 退出码退出。
    if let Err(err) =
        // run step 000
        Cli::<EthereumChainSpecParser, RessArgs>::parse().run(async move |builder, ress_args| {
            // 进入 node 启动闭包：builder 已根据 CLI 参数（chain/rpc/network/datadir 等）配置好。
            info!(target: "reth::cli", "Launching node");
            let NodeHandle { node, node_exit_future } =
                builder.node(EthereumNode::default()).launch_with_debug_capabilities().await?;

            // 如果启用 ress，则在网络层安装 ress 子协议（需要用到 node 的若干组件句柄）。
            if ress_args.enabled {
                install_ress_subprotocol(
                    ress_args,
                    node.provider,
                    node.evm_config,
                    node.network,        
                    node.task_executor,
                    node.add_ons_handle.engine_events.new_listener(),
                )?;
            }

            // 等待节点退出；进程生命周期由 node_exit_future 决定。
            node_exit_future.await
        })
    {
        // CLI 运行失败：打印错误并退出。
        eprintln!("Error: {err:?}");
        std::process::exit(1);
    }
}
