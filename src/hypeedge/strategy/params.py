"""Strategy parameter loading and hot-reload (design doc §15.2).

Parameters are defined as frozen dataclasses and loaded from YAML.
A file watcher detects changes and triggers callback for hot-reload.
Each parameter change is logged for audit.
"""

from __future__ import annotations

import os
import time
from collections.abc import Callable
from dataclasses import dataclass

import structlog

logger = structlog.get_logger(__name__)


@dataclass(frozen=True)
class TrendParams:
    """Trend following strategy parameters.

    All values have sensible defaults for a medium-frequency
    trend-following strategy on Hyperliquid.
    """

    # Target symbol
    symbol: str = "BTC"

    # EMA / MACD signal parameters
    fast_ema_period: int = 12
    slow_ema_period: int = 26
    signal_ema_period: int = 9

    # Momentum
    momentum_period: int = 10
    momentum_threshold: float = 0.0  # |momentum| > threshold confirms trend

    # ATR (volatility)
    atr_period: int = 14

    # Position sizing
    atr_position_multiplier: float = 0.5  # size = (equity * risk_pct) / (ATR * mult)
    max_position_pct: float = 0.15  # max position as % of equity
    risk_per_trade_pct: float = 0.01  # risk 1% of equity per trade

    # Stop-loss
    atr_stop_multiplier: float = 2.0  # stop = entry ± ATR * multiplier

    # MACD cross thresholds
    macd_cross_threshold: float = 0.0  # MACD cross above this → buy signal

    def __post_init__(self) -> None:
        """Validate parameter constraints."""
        errors: list[str] = []

        if self.fast_ema_period < 1:
            errors.append(f"fast_ema_period must be >= 1, got {self.fast_ema_period}")
        if self.slow_ema_period < 1:
            errors.append(f"slow_ema_period must be >= 1, got {self.slow_ema_period}")
        if self.fast_ema_period >= self.slow_ema_period:
            errors.append(
                f"fast_ema_period ({self.fast_ema_period}) must be < slow_ema_period ({self.slow_ema_period})"
            )
        if self.signal_ema_period < 1:
            errors.append(f"signal_ema_period must be >= 1, got {self.signal_ema_period}")
        if self.momentum_period < 1:
            errors.append(f"momentum_period must be >= 1, got {self.momentum_period}")
        if self.atr_period < 1:
            errors.append(f"atr_period must be >= 1, got {self.atr_period}")
        if self.atr_position_multiplier <= 0:
            errors.append(f"atr_position_multiplier must be > 0, got {self.atr_position_multiplier}")
        if not (0 < self.max_position_pct <= 1.0):
            errors.append(f"max_position_pct must be in (0, 1.0], got {self.max_position_pct}")
        if not (0 < self.risk_per_trade_pct <= 1.0):
            errors.append(f"risk_per_trade_pct must be in (0, 1.0], got {self.risk_per_trade_pct}")
        if self.atr_stop_multiplier <= 0:
            errors.append(f"atr_stop_multiplier must be > 0, got {self.atr_stop_multiplier}")

        if errors:
            raise ValueError("Invalid TrendParams: " + "; ".join(errors))


def load_params(path: str) -> TrendParams:
    """Load trend strategy parameters from a YAML file.

    Falls back to defaults for any missing keys.
    """
    try:
        import yaml

        with open(path) as f:
            data = yaml.safe_load(f) or {}

        # Filter to only known fields
        known_fields = {f.name for f in TrendParams.__dataclass_fields__.values()}
        filtered = {k: v for k, v in data.items() if k in known_fields}

        params = TrendParams(**filtered)
        logger.info("trend_params_loaded", path=path, params=filtered)
        return params
    except FileNotFoundError:
        logger.warning("trend_params_file_not_found", path=path, using="defaults")
        return TrendParams()
    except Exception:
        logger.exception("trend_params_load_error", path=path, using="defaults")
        return TrendParams()


class ParamWatcher:
    """Watches a YAML file for changes and triggers parameter reload.

    Design doc §15.2: "配置文件支持热更新：monitor 文件变更 → 通知策略
    重新加载参数（不重启进程）。每次参数变更记录日志（旧值 → 新值、
    变更时间、触发者），便于事后审计。"
    """

    def __init__(
        self,
        path: str,
        on_change: Callable[[TrendParams, TrendParams], None],
        check_interval: float = 5.0,
    ) -> None:
        self._path = path
        self._on_change = on_change
        self._check_interval = check_interval
        self._last_mtime: float = 0.0
        self._last_params: TrendParams | None = None
        self._running = False

    async def run(self) -> None:
        """Start watching the param file for changes."""
        self._running = True
        # Initial load
        self._last_params = load_params(self._path)
        try:
            self._last_mtime = os.path.getmtime(self._path)
        except OSError:
            self._last_mtime = 0.0

        logger.info("param_watcher_started", path=self._path)

        try:
            while self._running:
                import asyncio

                await asyncio.sleep(self._check_interval)
                if not self._running:
                    return

                try:
                    current_mtime = os.path.getmtime(self._path)
                except OSError:
                    continue

                if current_mtime > self._last_mtime:
                    logger.info(
                        "param_file_changed",
                        path=self._path,
                        old_mtime=self._last_mtime,
                        new_mtime=current_mtime,
                    )
                    new_params = load_params(self._path)
                    old_params = self._last_params

                    # Log each changed field for audit (§15.2)
                    if old_params:
                        self._log_changes(old_params, new_params)

                    self._last_params = new_params
                    self._last_mtime = current_mtime
                    self._on_change(old_params or TrendParams(), new_params)
        except asyncio.CancelledError:
            logger.debug("param_watcher_cancelled")
        finally:
            self._running = False
            logger.info("param_watcher_stopped")

    async def stop(self) -> None:
        self._running = False

    @staticmethod
    def _log_changes(old: TrendParams, new: TrendParams) -> None:
        """Log each changed parameter for audit trail."""
        old_dict = old.__dict__
        new_dict = new.__dict__
        for key in old_dict:
            old_val = old_dict[key]
            new_val = new_dict.get(key)
            if old_val != new_val:
                logger.info(
                    "param_changed",
                    field=key,
                    old_value=old_val,
                    new_value=new_val,
                    timestamp=time.time(),
                )
