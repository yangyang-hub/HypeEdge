"""Account state tracker — positions, equity, drawdown, PnL attribution.

Design doc §4: "account — 余额/持仓/PnL"
Design doc §5.2: Postgres tables for orders, positions, fills, pnl.
Design doc §8.1: Risk limits based on account equity and peak equity.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

import structlog

from hypeedge.core.enums import Side
from hypeedge.core.models import AccountState, Fill, Position
from hypeedge.core.types import Cloid, Price, Size, Symbol, Usd

logger = structlog.get_logger(__name__)


class AccountTracker:
    """Tracks account balance, positions, and PnL in real time.

    Updated from two sources:
    1. Exchange `clearinghouseState` polling (authoritative)
    2. Local fill processing (for immediate position updates between polls)

    Design doc §8.1 risk limits depend on:
    - equity, peak_equity → max drawdown
    - per-coin position size → max position %
    - per-strategy PnL → max strategy loss %
    """

    def __init__(self) -> None:
        self._positions: dict[Symbol, Position] = {}
        self._account_state: AccountState | None = None
        self._peak_equity: Usd = Usd(0.0)
        self._total_fees: Usd = Usd(0.0)
        self._total_funding: Usd = Usd(0.0)
        self._fill_count: int = 0
        self._last_update_ts: datetime | None = None
        self._authoritative_fill_ids: set[str] = set()
        self._provisional_fill_fees: dict[Cloid, Usd] = {}

    # --- Position management from fills ---

    def update_fill(self, fill: Fill, *, provisional: bool = False) -> None:
        """Update position tracking after a fill.

        Maintains per-symbol position with VWAP entry price.
        Called by ExecutionEngine after each fill event.
        """
        symbol = fill.symbol
        pos = self._positions.get(symbol)

        if pos is None:
            # New position
            signed_size = fill.size if fill.side == Side.BUY else -fill.size
            self._positions[symbol] = Position(
                symbol=symbol,
                size=Size(signed_size),
                entry_price=fill.price,
                mark_price=fill.price,
            )
        else:
            # Update existing position
            is_buy = fill.side == Side.BUY
            old_size = pos.size
            new_size = Size(old_size + fill.size if is_buy else old_size - fill.size)

            if new_size == Size(0.0):
                # Position fully closed
                del self._positions[symbol]
                logger.info("position_closed", symbol=str(symbol), fill_cloid=str(fill.cloid))
            elif (old_size > 0 and new_size < 0) or (old_size < 0 and new_size > 0):
                # Position flipped (e.g. long → short)
                pos.size = new_size
                pos.entry_price = fill.price
                logger.info(
                    "position_flipped",
                    symbol=str(symbol),
                    old_size=float(old_size),
                    new_size=float(new_size),
                )
            elif (old_size > 0 and is_buy) or (old_size < 0 and not is_buy):
                # Adding in the same direction — update VWAP entry price.
                old_notional = abs(old_size) * (pos.entry_price or fill.price)
                new_notional = fill.size * fill.price
                total_size = abs(new_size)
                new_entry = Price((old_notional + new_notional) / total_size) if total_size > 0 else fill.price

                pos.size = new_size
                pos.entry_price = new_entry
            else:
                # Partial reduction keeps the original entry price. Realized PnL
                # belongs in the ledger; re-weighting here corrupts cost basis.
                pos.size = new_size

            # Update mark price
            if symbol in self._positions:
                self._positions[symbol].mark_price = fill.price

        # Track fees
        self._total_fees = Usd(self._total_fees + abs(float(fill.fee)))
        self._fill_count += 1
        if provisional:
            self._provisional_fill_fees[fill.cloid] = Usd(abs(float(fill.fee)))
        self._last_update_ts = datetime.now(UTC)

        logger.debug(
            "tracker_fill_processed",
            symbol=str(symbol),
            side=str(fill.side),
            size=float(fill.size),
            price=float(fill.price),
            positions=len(self._positions),
        )

    def apply_authoritative_fill(self, external_event_id: str, fill: Fill, position: Position) -> bool:
        """Apply a committed exchange fill exactly once to the live projection."""
        if external_event_id in self._authoritative_fill_ids:
            return False
        self._authoritative_fill_ids.add(external_event_id)

        if position.is_flat:
            self._positions.pop(position.symbol, None)
        else:
            self._positions[position.symbol] = position

        authoritative_fee = abs(float(fill.fee))
        provisional_fee = self._provisional_fill_fees.pop(fill.cloid, None)
        if provisional_fee is None:
            self._total_fees = Usd(self._total_fees + authoritative_fee)
            self._fill_count += 1
        else:
            self._total_fees = Usd(self._total_fees + authoritative_fee - float(provisional_fee))
        self._last_update_ts = datetime.fromtimestamp(int(fill.timestamp) / 1000, tz=UTC)
        logger.debug(
            "tracker_authoritative_fill_applied",
            external_event_id=external_event_id,
            cloid=str(fill.cloid),
            symbol=str(fill.symbol),
            position_size=float(position.size),
        )
        return True

    # --- Account state from exchange polling ---

    def update_account_state(self, state: AccountState) -> None:
        """Update from exchange clearinghouse state (authoritative).

        Called periodically by the reconciler or account polling task.
        Updates peak equity for drawdown tracking.
        """
        self._account_state = state
        if state.equity > self._peak_equity:
            self._peak_equity = state.equity
        self._last_update_ts = datetime.now(UTC)

        logger.debug(
            "tracker_account_updated",
            equity=float(state.equity),
            peak_equity=float(self._peak_equity),
            drawdown_pct=state.drawdown_pct,
        )

    def update_position_from_exchange(self, symbol: Symbol, position: Position) -> None:
        """Replace local position with exchange-truth (used by reconciler)."""
        self._positions[symbol] = position

    def remove_position(self, symbol: Symbol) -> None:
        """Remove a position (used when reconciler finds position closed on exchange)."""
        self._positions.pop(symbol, None)

    # --- Funding tracking ---

    def apply_funding(self, amount: Usd) -> None:
        """Record a funding payment (positive = received, negative = paid)."""
        self._total_funding = Usd(self._total_funding + amount)

    # --- Query methods ---

    def get_position(self, symbol: Symbol) -> Position | None:
        """Get current position for a symbol."""
        return self._positions.get(symbol)

    def get_all_positions(self) -> dict[Symbol, Position]:
        """Get all current positions."""
        return dict(self._positions)

    def get_account_state(self) -> AccountState | None:
        """Get current account state."""
        return self._account_state

    @property
    def peak_equity(self) -> Usd:
        return self._peak_equity

    @property
    def current_equity(self) -> Usd:
        if self._account_state:
            return self._account_state.equity
        return Usd(0.0)

    @property
    def drawdown_pct(self) -> float:
        """Current drawdown from peak equity as a fraction."""
        if self._account_state:
            return self._account_state.drawdown_pct
        return 0.0

    @property
    def total_fees(self) -> Usd:
        return self._total_fees

    @property
    def total_funding(self) -> Usd:
        return self._total_funding

    @property
    def fill_count(self) -> int:
        return self._fill_count

    @property
    def last_update_ts(self) -> datetime | None:
        return self._last_update_ts

    def get_position_value(self, symbol: Symbol) -> Usd:
        """Get the notional value of a position."""
        pos = self._positions.get(symbol)
        if pos is None or pos.mark_price is None:
            return Usd(0.0)
        return Usd(abs(pos.size) * pos.mark_price)

    def get_total_position_value(self) -> Usd:
        """Get total notional value of all positions."""
        total = Usd(0)
        for pos in self._positions.values():
            if pos.mark_price is not None:
                total = Usd(total + abs(pos.size) * pos.mark_price)
        return Usd(total)

    def get_leverage(self) -> float:
        """Current effective leverage = total position value / equity."""
        equity = self.current_equity
        if equity <= 0:
            return 0.0
        return float(self.get_total_position_value() / equity)

    # --- Serialization ---

    def get_status(self) -> dict[str, Any]:
        """Return full tracker status for monitoring."""
        return {
            "equity": float(self.current_equity),
            "peak_equity": float(self._peak_equity),
            "drawdown_pct": round(self.drawdown_pct, 4),
            "total_fees": float(self._total_fees),
            "total_funding": float(self._total_funding),
            "fill_count": self._fill_count,
            "position_count": len(self._positions),
            "leverage": round(self.get_leverage(), 2),
            "positions": {
                str(sym): {
                    "size": float(pos.size),
                    "entry_price": float(pos.entry_price) if pos.entry_price else None,
                    "mark_price": float(pos.mark_price) if pos.mark_price else None,
                }
                for sym, pos in self._positions.items()
            },
            "last_update": self._last_update_ts.isoformat() if self._last_update_ts else None,
        }
