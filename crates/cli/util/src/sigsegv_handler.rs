//! Signal handler to extract a backtrace from stack overflow.
//!
//! Implementation modified from [`rustc`](https://github.com/rust-lang/rust/blob/3dee9775a8c94e701a08f7b2df2c444f353d8699/compiler/rustc_driver_impl/src/signal_handler.rs).

use std::{
    alloc::{alloc, Layout},
    fmt, mem, ptr,
};

unsafe extern "C" {
    fn backtrace_symbols_fd(buffer: *const *mut libc::c_void, size: libc::c_int, fd: libc::c_int);
}

fn backtrace_stderr(buffer: &[*mut libc::c_void]) {
    let size = buffer.len().try_into().unwrap_or_default();
    unsafe { backtrace_symbols_fd(buffer.as_ptr(), size, libc::STDERR_FILENO) };
}

/// Unbuffered, unsynchronized writer to stderr.
///
/// Only acceptable because everything will end soon anyways.
struct RawStderr(());

impl fmt::Write for RawStderr {
    fn write_str(&mut self, s: &str) -> Result<(), fmt::Error> {
        let ret = unsafe { libc::write(libc::STDERR_FILENO, s.as_ptr().cast(), s.len()) };
        if ret == -1 {
            Err(fmt::Error)
        } else {
            Ok(())
        }
    }
}

/// We don't really care how many bytes we actually get out. SIGSEGV comes for our head.
/// Splash stderr with letters of our own blood to warn our friends about the monster.
macro_rules! raw_errln {
    ($tokens:tt) => {
        let _ = ::core::fmt::Write::write_fmt(&mut RawStderr(()), format_args!($tokens));
        let _ = ::core::fmt::Write::write_char(&mut RawStderr(()), '\n');
    };
}

/// Signal handler installed for SIGSEGV
extern "C" fn print_stack_trace(_: libc::c_int) {
    const MAX_FRAMES: usize = 256;
    let mut stack_trace: [*mut libc::c_void; MAX_FRAMES] = [ptr::null_mut(); MAX_FRAMES];
    let stack = unsafe {
        // Collect return addresses
        let depth = libc::backtrace(stack_trace.as_mut_ptr(), MAX_FRAMES as i32);
        if depth == 0 {
            return
        }
        &stack_trace[0..depth as usize]
    };

    // Just a stack trace is cryptic. Explain what we're doing.
    raw_errln!("error: reth interrupted by SIGSEGV, printing backtrace\n");
    let mut written = 1;
    let mut consumed = 0;
    // Begin elaborating return addrs into symbols and writing them directly to stderr
    // Most backtraces are stack overflow, most stack overflows are from recursion
    // Check for cycles before writing 250 lines of the same ~5 symbols
    let cycled = |(runner, walker)| runner == walker;
    let mut cyclic = false;
    if let Some(period) = stack.iter().skip(1).step_by(2).zip(stack).position(cycled) {
        let period = period.saturating_add(1); // avoid "what if wrapped?" branches
        let Some(offset) = stack.iter().skip(period).zip(stack).position(cycled) else {
            // impossible.
            return
        };

        // Count matching trace slices, else we could miscount "biphasic cycles"
        // with the same period + loop entry but a different inner loop
        let next_cycle = stack[offset..].chunks_exact(period).skip(1);
        let cycles = 1 + next_cycle
            .zip(stack[offset..].chunks_exact(period))
            .filter(|(next, prev)| next == prev)
            .count();
        backtrace_stderr(&stack[..offset]);
        written += offset;
        consumed += offset;
        if cycles > 1 {
            raw_errln!("\n### cycle encountered after {offset} frames with period {period}");
            backtrace_stderr(&stack[consumed..consumed + period]);
            raw_errln!("### recursed {cycles} times\n");
            written += period + 4;
            consumed += period * cycles;
            cyclic = true;
        };
    }
    let rem = &stack[consumed..];
    backtrace_stderr(rem);
    raw_errln!("");
    written += rem.len() + 1;

    let random_depth = || 8 * 16; // chosen by random diceroll (2d20)
    if cyclic || stack.len() > random_depth() {
        // technically speculation, but assert it with confidence anyway.
        // We only arrived in this signal handler because bad things happened
        // and this message is for explaining it's not the programmer's fault
        raw_errln!("note: reth unexpectedly overflowed its stack! this is a bug");
        written += 1;
    }
    if stack.len() == MAX_FRAMES {
        raw_errln!("note: maximum backtrace depth reached, frames may have been lost");
        written += 1;
    }
    raw_errln!("note: we would appreciate a report at https://github.com/paradigmxyz/reth");
    written += 1;
    if written > 24 {
        // We probably just scrolled the earlier "we got SIGSEGV" message off the terminal
        raw_errln!("note: backtrace dumped due to SIGSEGV! resuming signal");
    }
}

/// Installs a SIGSEGV handler.
///
/// When SIGSEGV is delivered to the process, print a stack trace and then exit.
pub fn install() {
    unsafe {
        // 为信号处理函数准备“备用信号栈”(alternate signal stack)：
        // - 典型场景是 stack overflow 导致 SIGSEGV，此时当前线程的正常栈可能已经不可用
        // - 如果处理函数继续在“坏掉的栈”上执行，很可能二次崩溃，拿不到回溯信息
        //
        // 所以我们提前分配一段内存作为信号处理时的栈，并通过 `sigaltstack` 注册。
        let alt_stack_size: usize = min_sigstack_size() + 64 * 1024;
        let mut alt_stack: libc::stack_t = mem::zeroed();
        // 分配备用栈内存。对 signal stack 来说只要可用地址即可，这里用 1 字节对齐。
        alt_stack.ss_sp = alloc(Layout::from_size_align(alt_stack_size, 1).unwrap()).cast();
        alt_stack.ss_size = alt_stack_size;
        // 注册备用信号栈（第二个参数用于读取旧的备用栈，这里不需要）。
        libc::sigaltstack(&raw const alt_stack, ptr::null_mut());

        // 安装 SIGSEGV 的 sigaction 处理器。
        // 这里用的是 sa_sigaction（而不是 sa_handler），以便使用更通用的 handler 形式。
        let mut sa: libc::sigaction = mem::zeroed();
        sa.sa_sigaction =
            print_stack_trace as unsafe extern "C" fn(libc::c_int) as libc::sighandler_t;
        // SA_ONSTACK：在备用信号栈上运行 handler（对应上面的 sigaltstack）。
        // SA_RESETHAND：handler 触发一次后恢复默认处理，避免重复进入。
        // SA_NODEFER：进入 handler 时不自动屏蔽同类信号（不把 SIGSEGV 加到 mask 里）。
        sa.sa_flags = libc::SA_NODEFER | libc::SA_RESETHAND | libc::SA_ONSTACK;
        // 清空信号屏蔽集：handler 运行时不额外屏蔽其它信号。
        libc::sigemptyset(&raw mut sa.sa_mask);
        // 将 SIGSEGV 的处理动作设置为我们定义的 handler（第三个参数用于保存旧 action，这里不需要）。
        libc::sigaction(libc::SIGSEGV, &raw const sa, ptr::null_mut());
    }
}

/// Modern kernels on modern hardware can have dynamic signal stack sizes.
#[cfg(any(target_os = "linux", target_os = "android"))]
fn min_sigstack_size() -> usize {
    // Linux/Android 上可以通过 `getauxval(AT_MINSIGSTKSZ)` 获取内核建议的最小 signal stack 大小。
    // 兼容：老内核可能不提供该值（返回 0），因此取 `MINSIGSTKSZ` 与 auxval 的最大值。
    const AT_MINSIGSTKSZ: core::ffi::c_ulong = 51;
    let dynamic_sigstksz = unsafe { libc::getauxval(AT_MINSIGSTKSZ) };
    // If getauxval couldn't find the entry, it returns 0,
    // so take the higher of the "constant" and auxval.
    // This transparently supports older kernels which don't provide AT_MINSIGSTKSZ
    libc::MINSIGSTKSZ.max(dynamic_sigstksz as _)
}

/// Not all OS support hardware where this is needed.
#[cfg(not(any(target_os = "linux", target_os = "android")))]
const fn min_sigstack_size() -> usize {
    // 其它平台没有动态最小值，直接使用 libc 提供的常量。
    libc::MINSIGSTKSZ
}
