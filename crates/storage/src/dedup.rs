//! Deduplication filter for ClickHouse writes, port of
//! `src/hypeedge/storage/dedup.py`.
//!
//! Prevents duplicate rows from being written to ClickHouse by tracking seen
//! keys in an in-memory ordered map with configurable capacity. When capacity
//! is reached, the oldest entries are evicted (FIFO).

use std::collections::{HashMap, VecDeque};

/// Thread-safe deduplication filter using an ordered set with FIFO eviction.
pub struct DedupFilter {
    max_keys: usize,
    /// `table:key` → `true`.
    seen: HashMap<String, bool>,
    /// Insertion order for FIFO eviction.
    order: VecDeque<String>,
    dedup_count: u64,
}

impl DedupFilter {
    pub fn new(max_keys: usize) -> Self {
        Self {
            max_keys,
            seen: HashMap::new(),
            order: VecDeque::new(),
            dedup_count: 0,
        }
    }

    /// Whether a key has been seen before for the table.
    pub fn is_duplicate(&self, table: &str, key: &str) -> bool {
        self.seen.contains_key(&composite(table, key))
    }

    /// Mark a key as seen, evicting the oldest entries past capacity.
    pub fn mark_seen(&mut self, table: &str, key: &str) {
        let composite = composite(table, key);
        if self.seen.contains_key(&composite) {
            return;
        }
        self.seen.insert(composite.clone(), true);
        self.order.push_back(composite);
        while self.order.len() > self.max_keys {
            if let Some(oldest) = self.order.pop_front() {
                self.seen.remove(&oldest);
            }
        }
    }

    /// Check-and-mark atomically: returns `true` when the key was a duplicate.
    pub fn check_and_mark(&mut self, table: &str, key: &str) -> bool {
        if self.is_duplicate(table, key) {
            self.dedup_count += 1;
            return true;
        }
        self.mark_seen(table, key);
        false
    }

    /// Reset the filter; optionally only for one table's entries.
    pub fn reset(&mut self, table: Option<&str>) {
        match table {
            None => {
                self.seen.clear();
                self.order.clear();
                self.dedup_count = 0;
            }
            Some(table) => {
                let prefix = format!("{table}:");
                let keep: Vec<String> = self
                    .order
                    .iter()
                    .filter(|k| !k.starts_with(&prefix))
                    .cloned()
                    .collect();
                self.order = keep.clone().into();
                self.seen = keep
                    .into_iter()
                    .map(|k| (k, true))
                    .collect();
            }
        }
    }

    pub fn seen_keys(&self) -> usize {
        self.seen.len()
    }

    pub fn max_keys(&self) -> usize {
        self.max_keys
    }

    pub fn dedup_count(&self) -> u64 {
        self.dedup_count
    }
}

fn composite(table: &str, key: &str) -> String {
    format!("{table}:{key}")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn detects_duplicates() {
        let mut filter = DedupFilter::new(100);
        assert!(!filter.check_and_mark("orders", "k1"));
        assert!(filter.check_and_mark("orders", "k1"));
        assert!(!filter.check_and_mark("orders", "k2"));
        assert_eq!(filter.dedup_count(), 1);
        // Different table, same key — not a duplicate.
        assert!(!filter.check_and_mark("fills", "k1"));
        assert_eq!(filter.seen_keys(), 3);
    }

    #[test]
    fn evicts_oldest_past_capacity() {
        let mut filter = DedupFilter::new(3);
        for i in 0..5 {
            filter.check_and_mark("t", &format!("k{i}"));
        }
        assert_eq!(filter.seen_keys(), 3);
        // Oldest (k0, k1) evicted, so re-marking them is not a duplicate.
        assert!(!filter.is_duplicate("t", "k0"));
        assert!(filter.is_duplicate("t", "k4"));
    }

    #[test]
    fn reset_all_and_by_table() {
        let mut filter = DedupFilter::new(100);
        filter.mark_seen("a", "k1");
        filter.mark_seen("b", "k1");
        filter.reset(Some("a"));
        assert!(!filter.is_duplicate("a", "k1"));
        assert!(filter.is_duplicate("b", "k1"));
        filter.reset(None);
        assert!(!filter.is_duplicate("b", "k1"));
        assert_eq!(filter.seen_keys(), 0);
    }

    #[test]
    fn mark_seen_is_idempotent() {
        let mut filter = DedupFilter::new(100);
        filter.mark_seen("t", "k");
        filter.mark_seen("t", "k");
        assert_eq!(filter.seen_keys(), 1);
    }

    #[test]
    fn stats_reflect_usage() {
        let mut filter = DedupFilter::new(10);
        filter.mark_seen("t", "a");
        filter.check_and_mark("t", "a");
        assert_eq!(filter.seen_keys(), 1);
        assert_eq!(filter.dedup_count(), 1);
        assert_eq!(filter.max_keys(), 10);
    }
}
