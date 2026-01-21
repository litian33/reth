//! 一个基于 tokio 的 CLI 运行器。

#![doc(
    html_logo_url = "https://raw.githubusercontent.com/paradigmxyz/reth/main/assets/reth-docs.png",
    html_favicon_url = "https://avatars0.githubusercontent.com/u/97369466?s=256",
    issue_tracker_base_url = "https://github.com/paradigmxyz/reth/issues/"
)]
#![cfg_attr(not(test), warn(unused_crate_dependencies))]
#![cfg_attr(docsrs, feature(doc_cfg))]

//! 运行命令的入口点。

use reth_tasks::{TaskExecutor, TaskManager};
use std::{future::Future, pin::pin, sync::mpsc, time::Duration};
use tracing::{debug, error, trace};

/// 执行 CLI 命令。
///
/// 提供运行 CLI 命令直至完成的工具。
#[derive(Debug)]
pub struct CliRunner {
    /// 运行器的配置选项。
    config: CliRunnerConfig,
    /// 用于执行异步任务的 tokio 运行时。
    tokio_runtime: tokio::runtime::Runtime,
}

impl CliRunner {
    /// 尝试使用默认的 tokio [`Runtime`](tokio::runtime::Runtime) 创建一个新的 [`CliRunner`]。
    ///
    /// 默认的 tokio 运行时是多线程的，并启用了 I/O 和时间驱动。
    pub fn try_default_runtime() -> Result<Self, std::io::Error> {
        Ok(Self { config: CliRunnerConfig::default(), tokio_runtime: tokio_runtime()? })
    }

    /// 从提供的 tokio [`Runtime`](tokio::runtime::Runtime) 创建一个新的 [`CliRunner`]。
    pub const fn from_runtime(tokio_runtime: tokio::runtime::Runtime) -> Self {
        Self { config: CliRunnerConfig::new(), tokio_runtime }
    }

    /// 为此运行器设置 [`CliRunnerConfig`]。
    pub const fn with_config(mut self, config: CliRunnerConfig) -> Self {
        self.config = config;
        self
    }

    /// 在运行时执行一个异步代码块并阻塞直到完成。
    pub fn block_on<F, T>(&self, fut: F) -> T
    where
        F: Future<Output = T>,
    {
        self.tokio_runtime.block_on(fut)
    }

    /// 在 tokio 运行时执行给定的 _异步_ 命令，直到命令 future 解析，
    /// 或直到进程收到 `SIGINT` (Ctrl+C) 或 `SIGTERM` 信号。
    ///
    /// 命令通过 [`TaskExecutor`] 派生的任务将被关闭，
    /// 并在命令结束后尝试驱动它们完成关闭。
    pub fn run_command_until_exit<F, E>(
        self,
        command: impl FnOnce(CliContext) -> F,
    ) -> Result<(), E>
    where
        F: Future<Output = Result<(), E>>,
        E: Send + Sync + From<std::io::Error> + From<reth_tasks::PanickedTaskError> + 'static,
    {
        let AsyncCliRunner { context, mut task_manager, tokio_runtime } =
            AsyncCliRunner::new(self.tokio_runtime);

        // 执行命令直到完成或触发 Ctrl-C
        let command_res = tokio_runtime.block_on(run_to_completion_or_panic(
            &mut task_manager,
            run_until_ctrl_c(command(context)),
        ));

        if command_res.is_err() {
            error!(target: "reth::cli", "由于错误正在关闭");
        } else {
            debug!(target: "reth::cli", "正在优雅关闭");
            // 命令完成后或收到退出信号后，我们关闭任务管理器，
            // 它会向所有通过任务执行器派生的任务发出关闭信号，并等待带有优雅关闭的任务。
            task_manager.graceful_shutdown_with_timeout(self.config.graceful_shutdown_timeout);
        }

        // `drop(tokio_runtime)` 会阻塞当前线程，直到其线程池（包括阻塞池）关闭。
        // 由于我们希望尽快退出，因此在单独的线程上将其 drop，并等待最多 5 秒以完成此操作。
        let (tx, rx) = mpsc::channel();
        std::thread::Builder::new()
            .name("tokio-runtime-shutdown".to_string())
            .spawn(move || {
                drop(tokio_runtime);
                let _ = tx.send(());
            })
            .unwrap();

        let _ = rx.recv_timeout(Duration::from_secs(5)).inspect_err(|err| {
            debug!(target: "reth::cli", %err, "tokio 运行时关闭超时");
        });

        command_res
    }

    /// 在阻塞上下文中执行命令，并允许访问 `CliContext`。
    ///
    /// 参见 [`Runtime::spawn_blocking`](tokio::runtime::Runtime::spawn_blocking)。
    pub fn run_blocking_command_until_exit<F, E>(
        self,
        command: impl FnOnce(CliContext) -> F + Send + 'static,
    ) -> Result<(), E>
    where
        F: Future<Output = Result<(), E>> + Send + 'static,
        E: Send + Sync + From<std::io::Error> + From<reth_tasks::PanickedTaskError> + 'static,
    {
        let AsyncCliRunner { context, mut task_manager, tokio_runtime } =
            AsyncCliRunner::new(self.tokio_runtime);

        // 在阻塞线程池上启动命令
        let handle = tokio_runtime.handle().clone();
        let command_handle =
            tokio_runtime.handle().spawn_blocking(move || handle.block_on(command(context)));

        // 等待命令完成或收到 Ctrl-C
        let command_res = tokio_runtime.block_on(run_to_completion_or_panic(
            &mut task_manager,
            run_until_ctrl_c(
                async move { command_handle.await.expect("无法加入阻塞任务") },
            ),
        ));

        if command_res.is_err() {
            error!(target: "reth::cli", "由于错误正在关闭");
        } else {
            debug!(target: "reth::cli", "正在优雅关闭");
            task_manager.graceful_shutdown_with_timeout(self.config.graceful_shutdown_timeout);
        }

        // 在单独的线程上关闭运行时
        let (tx, rx) = mpsc::channel();
        std::thread::Builder::new()
            .name("tokio-runtime-shutdown".to_string())
            .spawn(move || {
                drop(tokio_runtime);
                let _ = tx.send(());
            })
            .unwrap();

        let _ = rx.recv_timeout(Duration::from_secs(5)).inspect_err(|err| {
            debug!(target: "reth::cli", %err, "tokio 运行时关闭超时");
        });

        command_res
    }

    /// 执行一个常规 future，直到完成或收到外部信号。
    pub fn run_until_ctrl_c<F, E>(self, fut: F) -> Result<(), E>
    where
        F: Future<Output = Result<(), E>>,
        E: Send + Sync + From<std::io::Error> + 'static,
    {
        self.tokio_runtime.block_on(run_until_ctrl_c(fut))?;
        Ok(())
    }

    /// 在派生的阻塞任务中执行常规 future，直到完成或收到外部信号。
    ///
    /// 参见 [`Runtime::spawn_blocking`](tokio::runtime::Runtime::spawn_blocking)。
    pub fn run_blocking_until_ctrl_c<F, E>(self, fut: F) -> Result<(), E>
    where
        F: Future<Output = Result<(), E>> + Send + 'static,
        E: Send + Sync + From<std::io::Error> + 'static,
    {
        let tokio_runtime = self.tokio_runtime;
        let handle = tokio_runtime.handle().clone();
        let fut = tokio_runtime.handle().spawn_blocking(move || handle.block_on(fut));
        tokio_runtime
            .block_on(run_until_ctrl_c(async move { fut.await.expect("无法加入任务") }))?;

        // 在单独的线程上 drop tokio 运行时，因为 drop 会阻塞直到其线程池（包括阻塞池）关闭。
        // 换句话说，`drop(tokio_runtime)` 会阻塞当前线程，但我们希望立即退出。
        std::thread::Builder::new()
            .name("tokio-runtime-shutdown".to_string())
            .spawn(move || drop(tokio_runtime))
            .unwrap();

        Ok(())
    }
}

/// 异步执行命令时的 [`CliRunner`] 配置。
struct AsyncCliRunner {
    /// 提供给命令的 CLI 上下文。
    context: CliContext,
    /// 负责管理派生任务生命周期的管理器。
    task_manager: TaskManager,
    /// 用于执行任务的 tokio 运行时。
    tokio_runtime: tokio::runtime::Runtime,
}

// === impl AsyncCliRunner ===

impl AsyncCliRunner {
    /// 给定一个 tokio [`Runtime`](tokio::runtime::Runtime)，创建异步执行命令所需的额外上下文。
    fn new(tokio_runtime: tokio::runtime::Runtime) -> Self {
        let task_manager = TaskManager::new(tokio_runtime.handle().clone());
        let task_executor = task_manager.executor();
        Self { context: CliContext { task_executor }, task_manager, tokio_runtime }
    }
}

/// 执行命令时由 [`CliRunner`] 提供的额外上下文。
#[derive(Debug)]
pub struct CliContext {
    /// 用于执行/派生任务的任务执行器。
    pub task_executor: TaskExecutor,
}

/// 任务优雅关闭的默认超时时间。
const DEFAULT_GRACEFUL_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(5);

/// [`CliRunner`] 的配置。
#[derive(Debug, Clone)]
pub struct CliRunnerConfig {
    /// 任务优雅关闭的超时时间。
    ///
    /// 命令完成后，这是在强制终止之前等待派生任务完成的最大时间。
    pub graceful_shutdown_timeout: Duration,
}

impl Default for CliRunnerConfig {
    fn default() -> Self {
        Self::new()
    }
}

impl CliRunnerConfig {
    /// 使用默认值创建一个新配置。
    pub const fn new() -> Self {
        Self { graceful_shutdown_timeout: DEFAULT_GRACEFUL_SHUTDOWN_TIMEOUT }
    }

    /// 设置优雅关闭超时时间。
    pub const fn with_graceful_shutdown_timeout(mut self, timeout: Duration) -> Self {
        self.graceful_shutdown_timeout = timeout;
        self
    }
}

/// 创建一个新的默认 tokio 多线程 [Runtime](tokio::runtime::Runtime)，并启用所有功能。
pub fn tokio_runtime() -> Result<tokio::runtime::Runtime, std::io::Error> {
    tokio::runtime::Builder::new_multi_thread().enable_all().build()
}

/// 运行给定的 future 直至完成，或者直到关键任务发生 panic。
///
/// 如果任务发生 panic，或者给定的 future 返回错误，则返回错误。
async fn run_to_completion_or_panic<F, E>(tasks: &mut TaskManager, fut: F) -> Result<(), E>
where
    F: Future<Output = Result<(), E>>,
    E: Send + Sync + From<reth_tasks::PanickedTaskError> + 'static,
{
    {
        let fut = pin!(fut);
        tokio::select! {
            task_manager_result = tasks => {
                if let Err(panicked_error) = task_manager_result {
                    return Err(panicked_error.into());
                }
            },
            res = fut => res?,
        }
    }
    Ok(())
}

/// 运行 future 直至完成，或者直到：
/// - 收到 `ctrl-c`。
/// - 收到 `SIGTERM`（仅限 unix）。
async fn run_until_ctrl_c<F, E>(fut: F) -> Result<(), E>
where
    F: Future<Output = Result<(), E>>,
    E: Send + Sync + 'static + From<std::io::Error>,
{
    let ctrl_c = tokio::signal::ctrl_c();

    #[cfg(unix)]
    {
        let mut stream = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
        let sigterm = stream.recv();
        let sigterm = pin!(sigterm);
        let ctrl_c = pin!(ctrl_c);
        let fut = pin!(fut);

        tokio::select! {
            _ = ctrl_c => {
                trace!(target: "reth::cli", "收到 Ctrl-C");
            },
            _ = sigterm => {
                trace!(target: "reth::cli", "收到 SIGTERM");
            },
            res = fut => res?,
        }
    }

    #[cfg(not(unix))]
    {
        let ctrl_c = pin!(ctrl_c);
        let fut = pin!(fut);

        tokio::select! {
            _ = ctrl_c => {
                trace!(target: "reth::cli", "收到 Ctrl-C");
            },
            res = fut => res?,
        }
    }

    Ok(())
}
