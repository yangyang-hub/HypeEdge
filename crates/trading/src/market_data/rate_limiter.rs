//! Hyperliquid rate limiter for IP weight and address action quota, port of
//! `src/hypeedge/market_data/rate_limiter.py`.

use std::sync::Mutex;
use std::time::Instant;

use tokio::time::Duration;

/// Hyperliquid rate-limit constants (design doc §3.1, §3.2).
pub const IP_WEIGHT_LIMIT_PER_MIN: u64 = 1200;
pub const DEFAULT_INFO_WEIGHT: u64 = 20;
pub const LIGHTWEIGHT_INFO_WEIGHT: u64 = 2;
pub const EXCHANGE_WEIGHT_BASE: u64 = 1;
pub const ACTION_CREDITS_INITIAL: i64 = 10_000;
pub const PER_ITEM_BATCH_SIZE: u64 = 20;
pub const CANDLE_PER_ITEM_BATCH_SIZE: u64 = 60;

/// Endpoint → base IP weight.
pub const ENDPOINT_WEIGHTS: &[(&str, u64)] = &[
    ("l2Book", 2),
    ("allMids", 2),
    ("clearinghouseState", 2),
    ("orderStatus", 2),
    ("spotClearinghouseState", 2),
    ("exchangeStatus", 2),
    ("userRole", 60),
    ("explorer", 40),
];

/// Endpoints with a per-item weight surcharge (batch size per +1 weight).
pub const PER_ITEM_ENDPOINTS: &[(&str, u64)] = &[
    ("fundingHistory", 20),
    ("candleSnapshot", 60),
    ("userFills", 20),
    ("userFillsByTime", 20),
    ("recentTrades", 20),
    ("historicalOrders", 20),
];

/// Mutable state tracking rate limits.
struct RateLimitState {
    weight_timestamps: Vec<(Instant, u64)>,
    action_credits_remaining: i64,
    action_credits_last_query: Option<Instant>,
}

/// Dual-dimension rate limiter: IP weight (1200/min sliding window) + address
/// action credits.
pub struct RateLimiter {
    ip_weight_limit: u64,
    action_credits_low_watermark: i64,
    state: Mutex<RateLimitState>,
}

impl Default for RateLimiter {
    fn default() -> Self {
        Self::new(IP_WEIGHT_LIMIT_PER_MIN, 1000)
    }
}

impl RateLimiter {
    pub fn new(ip_weight_limit: u64, action_credits_low_watermark: i64) -> Self {
        Self {
            ip_weight_limit,
            action_credits_low_watermark,
            state: Mutex::new(RateLimitState {
                weight_timestamps: Vec::new(),
                action_credits_remaining: ACTION_CREDITS_INITIAL,
                action_credits_last_query: None,
            }),
        }
    }

    /// Side-effect-free request weight estimator (public `estimate_weight`).
    pub fn estimate_weight(&self, endpoint: &str, batch_length: u64, item_count: u64) -> u64 {
        Self::calculate_weight(endpoint, batch_length, item_count)
    }

    /// Acquire IP-weight capacity, waiting (up to 60s) if the window is full.
    pub async fn acquire(
        &self,
        endpoint: &str,
        batch_length: u64,
        item_count: u64,
    ) -> Result<(), String> {
        let weight = Self::calculate_weight(endpoint, batch_length, item_count);
        let mut attempts = 0u64;
        loop {
            let current = {
                let mut st = self.state.lock().unwrap();
                let current = Self::current_weight(&mut st);
                if current + weight <= self.ip_weight_limit {
                    st.weight_timestamps.push((Instant::now(), weight));
                    return Ok(());
                }
                current
            };
            attempts += 1;
            if attempts > 60 {
                return Err(format!(
                    "IP weight limit exceeded after 60 retries: need {weight}, have {}",
                    self.ip_weight_limit.saturating_sub(current)
                ));
            }
            // Guard dropped above; sleep outside the lock.
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    }

    /// Pre-check that action credits are available (does not consume them).
    pub async fn acquire_action_credits(&self, count: i64) -> bool {
        let st = self.state.lock().unwrap();
        if st.action_credits_remaining < count {
            tracing::warn!(
                remaining = st.action_credits_remaining,
                requested = count,
                "action_credits_low"
            );
            false
        } else {
            true
        }
    }

    pub fn update_action_credits(&self, remaining: i64) {
        let mut st = self.state.lock().unwrap();
        st.action_credits_remaining = remaining;
        st.action_credits_last_query = Some(Instant::now());
        if remaining < self.action_credits_low_watermark {
            tracing::warn!(
                remaining,
                watermark = self.action_credits_low_watermark,
                "action_credits_low_watermark"
            );
        }
    }

    /// Synchronous check: credits fresh (≤120s) and above the low watermark.
    pub fn check_action_credits(&self) -> bool {
        self.action_credits_are_fresh(120.0)
            && self.state.lock().unwrap().action_credits_remaining
                >= self.action_credits_low_watermark
    }

    pub fn action_credits_are_fresh(&self, max_age_seconds: f64) -> bool {
        let st = self.state.lock().unwrap();
        match st.action_credits_last_query {
            Some(ts) => ts.elapsed().as_secs_f64() <= max_age_seconds,
            None => false,
        }
    }

    pub fn action_credits_remaining(&self) -> i64 {
        self.state.lock().unwrap().action_credits_remaining
    }

    pub fn ip_weight_remaining(&self) -> u64 {
        let mut st = self.state.lock().unwrap();
        self.ip_weight_limit
            .saturating_sub(Self::current_weight(&mut st))
    }

    fn calculate_weight(endpoint: &str, batch_length: u64, item_count: u64) -> u64 {
        if endpoint == "exchange" {
            return EXCHANGE_WEIGHT_BASE + (batch_length / 40);
        }
        let base = ENDPOINT_WEIGHTS
            .iter()
            .find(|(e, _)| *e == endpoint)
            .map(|(_, w)| *w)
            .unwrap_or(DEFAULT_INFO_WEIGHT);
        let per_item = PER_ITEM_ENDPOINTS
            .iter()
            .find(|(e, _)| *e == endpoint)
            .map(|(_, b)| *b);
        let item_weight = match per_item {
            Some(batch) if item_count > 0 => item_count.div_ceil(batch),
            _ => 0,
        };
        base + item_weight
    }

    fn current_weight(st: &mut RateLimitState) -> u64 {
        let cutoff = Instant::now() - Duration::from_secs(60);
        st.weight_timestamps.retain(|(ts, _)| *ts > cutoff);
        st.weight_timestamps.iter().map(|(_, w)| *w).sum()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn weight_calculation_matches_python() {
        assert_eq!(RateLimiter::calculate_weight("l2Book", 0, 0), 2);
        assert_eq!(RateLimiter::calculate_weight("clearinghouseState", 0, 0), 2);
        assert_eq!(RateLimiter::calculate_weight("meta", 0, 0), 20);
        assert_eq!(RateLimiter::calculate_weight("exchange", 80, 0), 3); // 1 + floor(80/40)
        assert_eq!(RateLimiter::calculate_weight("fundingHistory", 0, 40), 22); // 20 + ceil(40/20)
        assert_eq!(RateLimiter::calculate_weight("candleSnapshot", 0, 120), 22); // 20 + ceil(120/60)
        assert_eq!(RateLimiter::calculate_weight("userFills", 0, 0), 20); // no items -> base
    }

    #[test]
    fn credits_freshness_gate() {
        let limiter = RateLimiter::default();
        assert!(!limiter.check_action_credits(), "no query yet -> not fresh");
        limiter.update_action_credits(5000);
        assert!(limiter.check_action_credits());
        limiter.update_action_credits(50); // below watermark
        assert!(!limiter.check_action_credits());
    }
}
