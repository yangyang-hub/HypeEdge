//! Backfill checkpoint store, port of
//! `src/hypeedge/market_data/checkpoint.py`.
//!
//! Tracks the last successfully fetched millisecond timestamp per
//! `(endpoint, coin, interval)` so backfill can resume after restarts. Uses
//! atomic writes (temp file + rename) to prevent corruption.

use std::collections::BTreeMap;
use std::path::PathBuf;

/// JSON-file-backed store for backfill progress.
pub struct BackfillCheckpointStore {
    path: PathBuf,
    data: BTreeMap<String, i64>,
}

impl BackfillCheckpointStore {
    pub fn new(state_dir: impl Into<PathBuf>) -> Self {
        let path = state_dir.into().join("backfill_checkpoints.json");
        Self {
            path,
            data: BTreeMap::new(),
        }
    }

    pub fn path(&self) -> &PathBuf {
        &self.path
    }

    /// Load checkpoints from disk; an absent or corrupt file yields empty data.
    pub fn load(&mut self) {
        match std::fs::read_to_string(&self.path) {
            Ok(text) => match serde_json::from_str::<BTreeMap<String, i64>>(&text) {
                Ok(data) => {
                    self.data = data;
                    tracing::info!(
                        path = %self.path.display(),
                        entries = self.data.len(),
                        "checkpoints_loaded"
                    );
                }
                Err(e) => {
                    tracing::warn!(error = %e, path = %self.path.display(), "checkpoints_load_failed");
                    self.data = BTreeMap::new();
                }
            },
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                self.data = BTreeMap::new();
                tracing::info!(path = %self.path.display(), "checkpoints_no_file");
            }
            Err(e) => {
                tracing::warn!(error = %e, path = %self.path.display(), "checkpoints_load_failed");
                self.data = BTreeMap::new();
            }
        }
    }

    /// Last successful timestamp for a backfill key, or `None`.
    pub fn get(&self, endpoint: &str, coin: &str, interval: &str) -> Option<i64> {
        self.data.get(&make_key(endpoint, coin, interval)).copied()
    }

    /// Update the checkpoint for a backfill key and flush to disk.
    pub fn save(
        &mut self,
        endpoint: &str,
        coin: &str,
        interval: &str,
        last_ts: i64,
    ) -> Result<(), String> {
        let key = make_key(endpoint, coin, interval);
        self.data.insert(key, last_ts);
        self.flush()
    }

    /// All checkpoint entries (for inspection/debugging).
    pub fn all_entries(&self) -> BTreeMap<String, i64> {
        self.data.clone()
    }

    /// Write checkpoints to disk atomically (temp file + rename).
    pub fn flush(&self) -> Result<(), String> {
        let dir = self
            .path
            .parent()
            .ok_or_else(|| "checkpoint state dir missing".to_string())?;
        std::fs::create_dir_all(dir).map_err(|e| e.to_string())?;

        let tmp_path = dir.join(format!(
            ".backfill_checkpoints_{}.tmp",
            uuid::Uuid::new_v4()
        ));
        let payload = serde_json::to_string_pretty(&self.data).map_err(|e| e.to_string())?;
        std::fs::write(&tmp_path, payload).map_err(|e| e.to_string())?;
        std::fs::rename(&tmp_path, &self.path).map_err(|e| e.to_string())?;
        tracing::debug!(entries = self.data.len(), "checkpoints_flushed");
        Ok(())
    }
}

fn make_key(endpoint: &str, coin: &str, interval: &str) -> String {
    format!("{endpoint}:{coin}:{interval}")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_dir(tag: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "hypeedge_checkpoint_{tag}_{}",
            uuid::Uuid::new_v4()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    #[test]
    fn save_load_roundtrip() {
        let dir = temp_dir("roundtrip");
        let mut store = BackfillCheckpointStore::new(&dir);
        store.load();
        assert!(store.get("candleSnapshot", "BTC", "1m").is_none());
        store
            .save("candleSnapshot", "BTC", "1m", 1_717_200_000_000)
            .unwrap();
        store
            .save("fundingHistory", "ETH", "1h", 1_717_196_400_000)
            .unwrap();

        let mut reloaded = BackfillCheckpointStore::new(&dir);
        reloaded.load();
        assert_eq!(
            reloaded.get("candleSnapshot", "BTC", "1m"),
            Some(1_717_200_000_000)
        );
        assert_eq!(
            reloaded.get("fundingHistory", "ETH", "1h"),
            Some(1_717_196_400_000)
        );
        assert_eq!(reloaded.all_entries().len(), 2);
    }

    #[test]
    fn missing_file_loads_empty() {
        let dir = temp_dir("missing");
        let mut store = BackfillCheckpointStore::new(&dir);
        store.load();
        assert!(store.all_entries().is_empty());
    }

    #[test]
    fn corrupt_file_loads_empty() {
        let dir = temp_dir("corrupt");
        let store = BackfillCheckpointStore::new(&dir);
        std::fs::write(store.path(), "{ not valid json").unwrap();
        let mut reloaded = BackfillCheckpointStore::new(&dir);
        reloaded.load();
        assert!(reloaded.all_entries().is_empty());
    }

    #[test]
    fn keys_are_per_endpoint_coin_interval() {
        let dir = temp_dir("keys");
        let mut store = BackfillCheckpointStore::new(&dir);
        store.save("a", "BTC", "1m", 1).unwrap();
        store.save("b", "BTC", "1m", 2).unwrap();
        store.save("a", "BTC", "5m", 3).unwrap();
        assert_eq!(store.get("a", "BTC", "1m"), Some(1));
        assert_eq!(store.get("b", "BTC", "1m"), Some(2));
        assert_eq!(store.get("a", "BTC", "5m"), Some(3));
    }

    #[test]
    fn save_overwrites_same_key() {
        let dir = temp_dir("overwrite");
        let mut store = BackfillCheckpointStore::new(&dir);
        store.save("a", "BTC", "1m", 10).unwrap();
        store.save("a", "BTC", "1m", 20).unwrap();
        assert_eq!(store.get("a", "BTC", "1m"), Some(20));
        assert_eq!(store.all_entries().len(), 1);
    }
}
