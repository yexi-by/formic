//! 规模观测：静态原子量 + FORMIC_METRICS=1 时的每秒汇总（stderr，机器可 grep）。
//! 附属证据，不参与任何业务状态与准入判断（AGENTS.md §9）；未设置环境变量时
//! 不启动汇总任务、不产生输出，原子量更新本身零分配。

use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::time::Duration;

/// 在途 LLM 调用数。
pub static LLM_IN_FLIGHT: AtomicI64 = AtomicI64::new(0);
/// 调度器已收未复的工具调用数（队列深度）。
pub static TOOL_INFLIGHT: AtomicI64 = AtomicI64::new(0);
/// 全部在途 worker 的对话历史字节总和。
pub static HISTORY_BYTES: AtomicI64 = AtomicI64::new(0);
/// search 执行次数与耗时累计（算均值）及峰值。
pub static SEARCH_COUNT: AtomicU64 = AtomicU64::new(0);
pub static SEARCH_TOTAL_MS: AtomicU64 = AtomicU64::new(0);
pub static SEARCH_MAX_MS: AtomicU64 = AtomicU64::new(0);
/// 单元结局计数。
pub static UNITS_COMPLETED: AtomicU64 = AtomicU64::new(0);
pub static UNITS_FAILED: AtomicU64 = AtomicU64::new(0);
pub static UNITS_CANCELLED: AtomicU64 = AtomicU64::new(0);

pub fn gauge_add(gauge: &'static AtomicI64, delta: i64) {
    gauge.fetch_add(delta, Ordering::Relaxed);
}

pub fn counter_inc(counter: &'static AtomicU64) {
    counter.fetch_add(1, Ordering::Relaxed);
}

pub fn observe_search_ms(ms: u64) {
    counter_inc(&SEARCH_COUNT);
    SEARCH_TOTAL_MS.fetch_add(ms, Ordering::Relaxed);
    SEARCH_MAX_MS.fetch_max(ms, Ordering::Relaxed);
}

fn load(gauge: &AtomicI64) -> i64 {
    gauge.load(Ordering::Relaxed)
}

fn load_u(counter: &AtomicU64) -> u64 {
    counter.load(Ordering::Relaxed)
}

/// 输出一行汇总到 stderr（RSS 以 MB、历史以 KB 计）。
pub fn report_once() {
    let count = load_u(&SEARCH_COUNT);
    let avg = load_u(&SEARCH_TOTAL_MS).checked_div(count).unwrap_or(0);
    eprintln!(
        "metrics rss_mb={} llm_in_flight={} tool_inflight={} history_kb={} \
         search_avg_ms={} search_max_ms={} done={} failed={} cancelled={}",
        current_rss_bytes() / (1024 * 1024),
        load(&LLM_IN_FLIGHT),
        load(&TOOL_INFLIGHT),
        load(&HISTORY_BYTES) / 1024,
        avg,
        load_u(&SEARCH_MAX_MS),
        load_u(&UNITS_COMPLETED),
        load_u(&UNITS_FAILED),
        load_u(&UNITS_CANCELLED),
    );
}

/// 每 250ms 一行汇总到 stderr。调用方负责仅在 FORMIC_METRICS=1 时启动。
pub fn spawn_reporter() {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_millis(250));
        loop {
            interval.tick().await;
            report_once();
        }
    });
}

/// 当前进程的工作集字节数（RSS）。
#[cfg(windows)]
pub fn current_rss_bytes() -> u64 {
    #[repr(C)]
    struct ProcessMemoryCounters {
        cb: u32,
        _page_fault_count: u32,
        _peak_working_set_size: usize,
        working_set_size: usize,
        _quota_peak_paged_pool_usage: usize,
        _quota_paged_pool_usage: usize,
        _quota_peak_non_paged_pool_usage: usize,
        _quota_non_paged_pool_usage: usize,
        _pagefile_usage: usize,
        _peak_pagefile_usage: usize,
        _private_usage: usize,
    }
    #[link(name = "kernel32")]
    unsafe extern "system" {
        fn GetCurrentProcess() -> *mut core::ffi::c_void;
        fn K32GetProcessMemoryInfo(
            process: *mut core::ffi::c_void,
            counters: *mut ProcessMemoryCounters,
            cb: u32,
        ) -> i32;
    }
    let mut counters: ProcessMemoryCounters = unsafe { std::mem::zeroed() };
    counters.cb = std::mem::size_of::<ProcessMemoryCounters>() as u32;
    let ok = unsafe { K32GetProcessMemoryInfo(GetCurrentProcess(), &mut counters, counters.cb) };
    if ok != 0 {
        counters.working_set_size as u64
    } else {
        0
    }
}

#[cfg(not(windows))]
pub fn current_rss_bytes() -> u64 {
    std::fs::read_to_string("/proc/self/statm")
        .ok()
        .and_then(|s| s.split_whitespace().nth(1)?.parse::<u64>().ok())
        .map(|pages| pages * 4096)
        .unwrap_or(0)
}
