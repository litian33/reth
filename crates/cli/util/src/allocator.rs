//! Custom allocator implementation.
//!
//! We provide support for jemalloc and snmalloc on unix systems, and prefer jemalloc if both are
//! enabled.

// 这里通过“编译期条件选择”来决定最终使用哪个全局分配器实现：
// - Unix + feature `jemalloc`：使用 tikv_jemallocator::Jemalloc
// - 否则 Unix + feature `snmalloc`：使用 snmalloc_rs::SnMalloc
// - 否则：回退到 Rust 标准库的系统分配器 std::alloc::System（通常是 libc malloc）
//
// `cfg_if::cfg_if!` 是 cfg-if crate 提供的宏，用来把一串 `#[cfg(...)]` 的 if/else 写得更直观：
// 它会在编译期只展开满足条件的那个分支，未命中的分支完全不会参与编译。
cfg_if::cfg_if! {
    if #[cfg(all(feature = "jemalloc", unix))] {
        type AllocatorInner = tikv_jemallocator::Jemalloc;
    } else if #[cfg(all(feature = "snmalloc", unix))] {
        type AllocatorInner = snmalloc_rs::SnMalloc;
    } else {
        type AllocatorInner = std::alloc::System;
    }
}

// 这个块的唯一目的：当你用 `--all-features` 同时打开 `jemalloc` + `snmalloc` 时，
// 上面的选择逻辑会优先走 jemalloc 分支，导致 snmalloc 相关依赖“引入但未使用”，clippy 可能告警。
// 这里用 `use snmalloc_rs as _;` 显式“使用一次”来消除 unused 警告（`as _` 表示不引入名字）。
cfg_if::cfg_if! {
    if #[cfg(all(feature = "snmalloc", feature = "jemalloc", unix))] {
        use snmalloc_rs as _;
    }
}

// tracy allocator profiling 支持：
// - 如果启用 feature `tracy-allocator`，则用 tracy_client::ProfiledAllocator 包一层，
//   让分配/释放事件可以被 Tracy 采集。
//
// `#[cfg(feature = "...")]` 是 Rust 的条件编译属性（由 rustc 实现）：
// - `feature = "tracy-allocator"` 表示“当 Cargo features 中启用了 tracy-allocator 时”此分支才会编译。
// - 未启用时会走 else 分支，完全不引入 tracy_client 相关类型，避免额外依赖和开销。
cfg_if::cfg_if! {
    if #[cfg(feature = "tracy-allocator")] {
        type AllocatorWrapper = tracy_client::ProfiledAllocator<AllocatorInner>;
        const fn new_allocator_wrapper() -> AllocatorWrapper {
            AllocatorWrapper::new(AllocatorInner {}, 100)
        }
    } else {
        type AllocatorWrapper = AllocatorInner;
        const fn new_allocator_wrapper() -> AllocatorWrapper {
            AllocatorInner {}
        }
    }
}

/// Custom allocator.
pub type Allocator = AllocatorWrapper;

/// Creates a new [custom allocator][Allocator].
pub const fn new_allocator() -> Allocator {
    new_allocator_wrapper()
}
