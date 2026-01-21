//! Reth CLI 应用程序实现，负责协调命令执行、追踪初始化和节点启动。

use crate::{
    interface::{Commands, NoSubCmd},
    Cli,
};
use clap::Subcommand;
use eyre::{eyre, Result};
use reth_chainspec::{ChainSpec, EthChainSpec, Hardforks};
use reth_cli::chainspec::ChainSpecParser;
use reth_cli_commands::{
    common::{CliComponentsBuilder, CliNodeTypes, HeaderMut},
    launcher::{FnLauncher, Launcher},
};
use reth_cli_runner::CliRunner;
use reth_db::DatabaseEnv;
use reth_node_api::NodePrimitives;
use reth_node_builder::{NodeBuilder, WithLaunchContext};
use reth_node_ethereum::{consensus::EthBeaconConsensus, EthEvmConfig, EthereumNode};
use reth_node_metrics::recorder::install_prometheus_recorder;
use reth_rpc_server_types::RpcModuleValidator;
use reth_tracing::{FileWorkerGuard, Layers};
use std::{fmt, sync::Arc};

/// 一个包裹了解析后的 CLI 的包装器，负责处理命令执行。
#[derive(Debug)]
pub struct CliApp<
    Spec: ChainSpecParser,
    Ext: clap::Args + fmt::Debug,
    Rpc: RpcModuleValidator,
    SubCmd: Subcommand + fmt::Debug = NoSubCmd,
> {
    /// 解析后的 CLI 参数。
    cli: Cli<Spec, Ext, Rpc, SubCmd>,
    /// 负责运行异步任务和阻塞命令的运行器。
    runner: Option<CliRunner>,
    /// 用于配置日志记录和追踪的层。
    layers: Option<Layers>,
    /// 文件日志记录器的守护进程。
    guard: Option<FileWorkerGuard>,
}

impl<C, Ext, Rpc, SubCmd> CliApp<C, Ext, Rpc, SubCmd>
where
    C: ChainSpecParser,
    Ext: clap::Args + fmt::Debug,
    Rpc: RpcModuleValidator,
    SubCmd: ExtendedCommand + Subcommand + fmt::Debug,
{
    /// 创建一个新的 `CliApp` 实例。
    pub(crate) fn new(cli: Cli<C, Ext, Rpc, SubCmd>) -> Self {
        Self { cli, runner: None, layers: Some(Layers::new()), guard: None }
    }

    /// 为 CLI 设置运行器。
    ///
    /// 这将替换掉任何现有的运行器。
    pub fn set_runner(&mut self, runner: CliRunner) {
        self.runner = Some(runner);
    }

    /// 访问追踪层。
    ///
    /// 返回追踪层的可变引用，如果追踪已初始化且层已分离，则返回错误。
    pub fn access_tracing_layers(&mut self) -> Result<&mut Layers> {
        self.layers.as_mut().ok_or_else(|| eyre!("Tracing already initialized"))
    }

    /// 执行配置好的 CLI 命令。
    ///
    /// 接收一个闭包，用于通过 [`NodeCommand`](reth_cli_commands::node::NodeCommand) 启动节点。
    pub fn run(self, launcher: impl Launcher<C, Ext>) -> Result<()>
    where
        C: ChainSpecParser<ChainSpec = ChainSpec>,
    {
        let components = |spec: Arc<ChainSpec>| {
            (EthEvmConfig::ethereum(spec.clone()), Arc::new(EthBeaconConsensus::new(spec)))
        };
        // run step 003
        self.run_with_components::<EthereumNode>(components, |builder, ext| async move {
            launcher.entrypoint(builder, ext).await
        })
    }

    /// 使用提供的 [`CliComponentsBuilder`] 执行配置好的 CLI 命令。
    ///
    /// 接收一个闭包，用于通过 [`NodeCommand`](reth_cli_commands::node::NodeCommand) 启动节点，并允许提供自定义组件。
    pub fn run_with_components<N>(
        mut self,
        components: impl CliComponentsBuilder<N>,
        launcher: impl AsyncFnOnce(
            WithLaunchContext<NodeBuilder<Arc<DatabaseEnv>, C::ChainSpec>>,
            Ext,
        ) -> Result<()>,
    ) -> Result<()>
    where
        N: CliNodeTypes<Primitives: NodePrimitives<BlockHeader: HeaderMut>, ChainSpec: Hardforks>,
        C: ChainSpecParser<ChainSpec = N::ChainSpec>,
    {
        let runner = match self.runner.take() {
            Some(runner) => runner,
            None => CliRunner::try_default_runtime()?,
        };

        // 如果可用，将网络名称添加到日志目录
        if let Some(chain_spec) = self.cli.command.chain_spec() {
            self.cli.logs.log_file_directory =
                self.cli.logs.log_file_directory.join(chain_spec.chain().to_string());
        }

        self.init_tracing(&runner)?;

        // 安装 prometheus 记录器以确保记录所有指标
        install_prometheus_recorder();
        // run step 004
        run_commands_with::<C, Ext, Rpc, N, SubCmd>(self.cli, runner, components, launcher)
    }

    /// 使用配置的选项初始化追踪。
    ///
    /// 更多信息请参阅 [`Cli::init_tracing`]。
    pub fn init_tracing(&mut self, runner: &CliRunner) -> Result<()> {
        if self.guard.is_none() {
            self.guard = self.cli.init_tracing(runner, self.layers.take().unwrap_or_default())?;
        }

        Ok(())
    }
}

/// 使用提供的运行器、组件和启动器运行 CLI 命令。
/// 这是 `CliApp` 和 Cli 方法共同使用的共享实现。
pub(crate) fn run_commands_with<C, Ext, Rpc, N, SubCmd>(
    cli: Cli<C, Ext, Rpc, SubCmd>,
    runner: CliRunner,
    components: impl CliComponentsBuilder<N>,
    launcher: impl AsyncFnOnce(
        WithLaunchContext<NodeBuilder<Arc<DatabaseEnv>, C::ChainSpec>>,
        Ext,
    ) -> Result<()>,
) -> Result<()>
where
    C: ChainSpecParser<ChainSpec = N::ChainSpec>,
    Ext: clap::Args + fmt::Debug,
    Rpc: RpcModuleValidator,
    N: CliNodeTypes<Primitives: NodePrimitives<BlockHeader: HeaderMut>, ChainSpec: Hardforks>,
    SubCmd: ExtendedCommand + Subcommand + fmt::Debug,
{
    // run step 005
    match cli.command {
        Commands::Node(command) => {
            // 使用配置的验证器验证 RPC 模块
            if let Some(http_api) = &command.rpc.http_api {
                Rpc::validate_selection(http_api, "http.api").map_err(|e| eyre!("{e}"))?;
            }
            if let Some(ws_api) = &command.rpc.ws_api {
                Rpc::validate_selection(ws_api, "ws.api").map_err(|e| eyre!("{e}"))?;
            }

            runner.run_command_until_exit(|ctx| {
                // run step 006
                command.execute(ctx, FnLauncher::new::<C, Ext>(launcher))
            })
        }
        Commands::Init(command) => runner.run_blocking_until_ctrl_c(command.execute::<N>()),
        Commands::InitState(command) => runner.run_blocking_until_ctrl_c(command.execute::<N>()),
        Commands::Import(command) => {
            runner.run_blocking_until_ctrl_c(command.execute::<N, _>(components))
        }
        Commands::ImportEra(command) => runner.run_blocking_until_ctrl_c(command.execute::<N>()),
        Commands::ExportEra(command) => runner.run_blocking_until_ctrl_c(command.execute::<N>()),
        Commands::DumpGenesis(command) => runner.run_blocking_until_ctrl_c(command.execute()),
        Commands::Db(command) => {
            runner.run_blocking_command_until_exit(|ctx| command.execute::<N>(ctx))
        }
        Commands::Download(command) => runner.run_blocking_until_ctrl_c(command.execute::<N>()),
        Commands::Stage(command) => {
            runner.run_command_until_exit(|ctx| command.execute::<N, _>(ctx, components))
        }
        Commands::P2P(command) => runner.run_until_ctrl_c(command.execute::<N>()),
        Commands::Config(command) => runner.run_until_ctrl_c(command.execute()),
        Commands::Prune(command) => runner.run_until_ctrl_c(command.execute::<N>()),
        #[cfg(feature = "dev")]
        Commands::TestVectors(command) => runner.run_until_ctrl_c(command.execute()),
        Commands::ReExecute(command) => runner.run_until_ctrl_c(command.execute::<N>(components)),
        Commands::Ext(command) => command.execute(runner),
    }
}

/// 可以添加到 CLI 的扩展子命令的 trait。
///
/// 使用者为他们的自定义子命令实现此 trait，以定义它们应该如何执行。
pub trait ExtendedCommand {
    /// 使用提供的 CLI 运行器执行扩展命令。
    fn execute(self, runner: CliRunner) -> Result<()>;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::chainspec::EthereumChainSpecParser;
    use clap::Parser;
    use reth_cli_commands::node::NoArgs;

    #[test]
    fn test_cli_app_creation() {
        let args = vec!["reth", "config"];
        let cli = Cli::<EthereumChainSpecParser, NoArgs>::try_parse_from(args).unwrap();
        let app = cli.configure();

        // Verify app is created correctly
        assert!(app.runner.is_none());
        assert!(app.layers.is_some());
        assert!(app.guard.is_none());
    }

    #[test]
    fn test_set_runner() {
        let args = vec!["reth", "config"];
        let cli = Cli::<EthereumChainSpecParser, NoArgs>::try_parse_from(args).unwrap();
        let mut app = cli.configure();

        // Create and set a runner
        if let Ok(runner) = CliRunner::try_default_runtime() {
            app.set_runner(runner);
            assert!(app.runner.is_some());
        }
    }

    #[test]
    fn test_access_tracing_layers() {
        let args = vec!["reth", "config"];
        let cli = Cli::<EthereumChainSpecParser, NoArgs>::try_parse_from(args).unwrap();
        let mut app = cli.configure();

        // Should be able to access layers before initialization
        assert!(app.access_tracing_layers().is_ok());

        // After taking layers (simulating initialization), access should error
        app.layers = None;
        assert!(app.access_tracing_layers().is_err());
    }
}
