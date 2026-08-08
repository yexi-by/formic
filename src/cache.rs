//! 作业内存工具缓存：只保存调用方标记为可缓存的完整成功结果，并合并相同在途调用。

use std::collections::HashMap;
use std::sync::Arc;

use tokio::sync::{Mutex, watch};

use crate::tools::ToolOutput;

#[derive(Clone)]
pub struct ToolCache {
    inner: Arc<Mutex<State>>,
    max_bytes: usize,
}

struct State {
    entries: HashMap<String, CacheEntry>,
    in_flight: HashMap<String, watch::Receiver<FlightState>>,
    bytes: usize,
    clock: u64,
}

struct CacheEntry {
    output: Arc<ToolOutput>,
    bytes: usize,
    last_used: u64,
}

#[derive(Clone)]
pub enum FlightState {
    Pending,
    Complete(Arc<ToolOutput>),
    Aborted,
}

pub enum CacheLookup {
    Hit(Arc<ToolOutput>),
    Join(watch::Receiver<FlightState>),
    Owner(watch::Sender<FlightState>),
}

impl ToolCache {
    pub fn new(max_bytes: usize) -> Self {
        Self {
            inner: Arc::new(Mutex::new(State {
                entries: HashMap::new(),
                in_flight: HashMap::new(),
                bytes: 0,
                clock: 0,
            })),
            max_bytes,
        }
    }

    pub async fn lookup(&self, key: &str) -> CacheLookup {
        let mut state = self.inner.lock().await;
        state.clock = state.clock.wrapping_add(1);
        let now = state.clock;
        if let Some(entry) = state.entries.get_mut(key) {
            entry.last_used = now;
            return CacheLookup::Hit(Arc::clone(&entry.output));
        }
        if let Some(receiver) = state.in_flight.get(key) {
            return CacheLookup::Join(receiver.clone());
        }
        let (sender, receiver) = watch::channel(FlightState::Pending);
        state.in_flight.insert(key.to_string(), receiver);
        CacheLookup::Owner(sender)
    }

    /// 完成 owner 调用；返回本次产生的 LRU 淘汰数量。
    pub async fn complete(
        &self,
        key: &str,
        sender: watch::Sender<FlightState>,
        output: Arc<ToolOutput>,
    ) -> u64 {
        let mut state = self.inner.lock().await;
        state.in_flight.remove(key);
        let mut evictions = 0;
        if output.cacheable {
            let bytes = key.len().saturating_add(output.content.len());
            if bytes <= self.max_bytes {
                state.clock = state.clock.wrapping_add(1);
                let last_used = state.clock;
                if let Some(previous) = state.entries.remove(key) {
                    state.bytes = state.bytes.saturating_sub(previous.bytes);
                }
                state.bytes = state.bytes.saturating_add(bytes);
                state.entries.insert(
                    key.to_string(),
                    CacheEntry {
                        output: Arc::clone(&output),
                        bytes,
                        last_used,
                    },
                );
                while state.bytes > self.max_bytes {
                    let Some(oldest) = state
                        .entries
                        .iter()
                        .min_by_key(|(_, entry)| entry.last_used)
                        .map(|(key, _)| key.clone())
                    else {
                        break;
                    };
                    if let Some(removed) = state.entries.remove(&oldest) {
                        state.bytes = state.bytes.saturating_sub(removed.bytes);
                        evictions += 1;
                    }
                }
            }
        }
        let _ = sender.send(FlightState::Complete(output));
        evictions
    }

    pub async fn abort(&self, key: &str, sender: watch::Sender<FlightState>) {
        let mut state = self.inner.lock().await;
        state.in_flight.remove(key);
        let _ = sender.send(FlightState::Aborted);
    }
}

pub async fn wait_for_flight(mut receiver: watch::Receiver<FlightState>) -> FlightState {
    loop {
        let state = receiver.borrow().clone();
        if !matches!(state, FlightState::Pending) {
            return state;
        }
        if receiver.changed().await.is_err() {
            return FlightState::Aborted;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn output(content: &str) -> Arc<ToolOutput> {
        Arc::new(ToolOutput {
            content: content.into(),
            cacheable: true,
        })
    }

    #[tokio::test]
    async fn joins_in_flight_then_hits_completed_value() {
        let cache = ToolCache::new(1024);
        let CacheLookup::Owner(sender) = cache.lookup("key").await else {
            panic!("首次查询应成为 owner")
        };
        let CacheLookup::Join(receiver) = cache.lookup("key").await else {
            panic!("第二次查询应合并")
        };
        cache.complete("key", sender, output("value")).await;
        let FlightState::Complete(joined) = wait_for_flight(receiver).await else {
            panic!("应收到结果")
        };
        assert_eq!(joined.content, "value");
        let CacheLookup::Hit(hit) = cache.lookup("key").await else {
            panic!("完成后应命中")
        };
        assert_eq!(hit.content, "value");
    }

    #[tokio::test]
    async fn lru_evicts_oldest_entry() {
        let cache = ToolCache::new(8);
        for (key, value) in [("a", "111"), ("b", "222"), ("c", "333")] {
            let CacheLookup::Owner(sender) = cache.lookup(key).await else {
                panic!()
            };
            cache.complete(key, sender, output(value)).await;
        }
        assert!(matches!(cache.lookup("a").await, CacheLookup::Owner(_)));
        assert!(matches!(cache.lookup("c").await, CacheLookup::Hit(_)));
    }
}
