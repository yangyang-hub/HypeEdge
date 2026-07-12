"""Pure market-making policy that produces candidate desired quotes."""

from __future__ import annotations

from datetime import timedelta
from decimal import ROUND_CEILING, ROUND_FLOOR, Decimal

from hypeedge.core.enums import ActionBudgetMode, QuoteDecision, Side
from hypeedge.core.types import Price, Size, StrategyId, Usd
from hypeedge.strategy.market_maker.fair_value import FairValueModel
from hypeedge.strategy.market_maker.inventory import InventoryController
from hypeedge.strategy.market_maker.models import (
    ActionBudgetSnapshot,
    InventorySnapshot,
    MarketFeatures,
    MarketMakerConfig,
)
from hypeedge.trading.quotes import DesiredQuote, DesiredQuoteSet, QuoteSlotKey


class MarketMakerPolicy:
    """Generate explainable one-level ALO quote candidates without I/O."""

    def __init__(
        self,
        fair_value_model: FairValueModel | None = None,
        inventory_controller: InventoryController | None = None,
    ) -> None:
        self._fair_value = fair_value_model or FairValueModel()
        self._inventory = inventory_controller or InventoryController()

    def quote(
        self,
        *,
        strategy_id: StrategyId,
        session_id: str,
        revision: int,
        current_slot_revision: int,
        features: MarketFeatures,
        inventory: InventorySnapshot,
        budget: ActionBudgetSnapshot,
        config: MarketMakerConfig,
    ) -> DesiredQuoteSet:
        fair = self._fair_value.calculate(features, config)
        inventory_decision = self._inventory.calculate(fair, inventory, features, config)
        now = features.received_at

        no_quote_reason: str | None = None
        if not features.healthy:
            no_quote_reason = "market_unhealthy"
        elif not budget.healthy:
            no_quote_reason = "action_budget_stale"
        elif budget.mode in {ActionBudgetMode.CANCEL_ONLY, ActionBudgetMode.EXHAUSTED}:
            no_quote_reason = f"budget_{budget.mode.value}"
        elif inventory_decision.emergency:
            no_quote_reason = "inventory_emergency"

        half_spread_bps = max(
            config.min_half_spread_bps,
            features.expected_adverse_markout_bps
            + features.latency_buffer_bps
            + config.toxicity_spread_bps * features.toxicity,
        )
        half_spread = Decimal(inventory_decision.reservation_price) * half_spread_bps / Decimal("10000")
        raw_bid = Decimal(inventory_decision.reservation_price) - half_spread
        raw_ask = Decimal(inventory_decision.reservation_price) + half_spread
        bid_price = Price(self._to_step(raw_bid, config.tick_size, ROUND_FLOOR))
        ask_price = Price(self._to_step(raw_ask, config.tick_size, ROUND_CEILING))

        # ALO candidates must stay strictly outside the opposite best price.
        bid_price = Price(min(Decimal(bid_price), Decimal(features.best_ask) - config.tick_size))
        ask_price = Price(max(Decimal(ask_price), Decimal(features.best_bid) + config.tick_size))

        quote_size = self._quote_size(fair, inventory_decision.inventory_notional, features, config)
        allow_bid = inventory_decision.allow_bid
        allow_ask = inventory_decision.allow_ask
        if budget.mode == ActionBudgetMode.CRITICAL:
            allow_bid = allow_bid and inventory_decision.inventory_notional < 0
            allow_ask = allow_ask and inventory_decision.inventory_notional > 0

        bid_edge = self._gross_edge(Side.BUY, fair, bid_price, quote_size, features, config)
        ask_edge = self._gross_edge(Side.SELL, fair, ask_price, quote_size, features, config)
        bid = self._desired(
            strategy_id,
            features,
            Side.BUY,
            bid_price,
            quote_size,
            bid_edge,
            allow_bid and no_quote_reason is None,
            no_quote_reason,
            config,
        )
        ask = self._desired(
            strategy_id,
            features,
            Side.SELL,
            ask_price,
            quote_size,
            ask_edge,
            allow_ask and no_quote_reason is None,
            no_quote_reason,
            config,
        )
        expected_utility = Usd(bid.gross_edge_usdc + ask.gross_edge_usdc)

        return DesiredQuoteSet(
            strategy_id=strategy_id,
            symbol=features.symbol,
            session_id=session_id,
            config_version=config.version,
            model_version=config.model_version,
            market_version=features.market_version,
            connection_generation=features.connection_generation,
            current_slot_revision=current_slot_revision,
            revision=revision,
            fair_price=fair,
            reservation_price=inventory_decision.reservation_price,
            inventory_notional=inventory_decision.inventory_notional,
            expected_utility_usdc=expected_utility,
            budget_mode=budget.mode,
            bid=bid,
            ask=ask,
            created_at=now,
            valid_until=now + timedelta(seconds=float(config.max_quote_lifetime_seconds)),
            feature_values=(
                ("toxicity", features.toxicity),
                ("half_spread_bps", half_spread_bps),
                ("inventory_shift_bps", inventory_decision.shift_bps),
            ),
        )

    @staticmethod
    def _quote_size(
        fair: Price,
        inventory_notional: Usd,
        features: MarketFeatures,
        config: MarketMakerConfig,
    ) -> Size:
        inventory_headroom = max(Decimal("0"), Decimal(config.hard_inventory_notional) - abs(inventory_notional))
        inventory_size = inventory_headroom / Decimal(fair)
        visible_depth = min(Decimal(features.best_bid_size), Decimal(features.best_ask_size))
        depth_size = visible_depth * config.max_depth_participation
        raw_size = min(Decimal(config.quote_size), inventory_size, depth_size)
        stepped = MarketMakerPolicy._to_step(raw_size, config.lot_size, ROUND_FLOOR)
        return Size(stepped)

    @staticmethod
    def _gross_edge(
        side: Side,
        fair: Price,
        quote_price: Price,
        size: Size,
        features: MarketFeatures,
        config: MarketMakerConfig,
    ) -> Usd:
        if size <= 0:
            return Usd(0)
        if side == Side.BUY:
            capture_rate = (Decimal(fair) - Decimal(quote_price)) / Decimal(fair)
        else:
            capture_rate = (Decimal(quote_price) - Decimal(fair)) / Decimal(fair)
        adverse_rate = features.expected_adverse_markout_bps / Decimal("10000")
        funding_rate = abs(features.funding_rate) * config.horizon_seconds / Decimal("3600")
        edge_rate = capture_rate - config.signed_maker_fee_rate - adverse_rate - funding_rate
        expected = Decimal(size) * Decimal(quote_price) * edge_rate * config.expected_fill_probability
        return Usd(max(Decimal("0"), expected))

    @staticmethod
    def _desired(
        strategy_id: StrategyId,
        features: MarketFeatures,
        side: Side,
        price: Price,
        size: Size,
        gross_edge: Usd,
        allowed: bool,
        global_reason: str | None,
        config: MarketMakerConfig,
    ) -> DesiredQuote:
        slot = QuoteSlotKey(strategy_id=strategy_id, symbol=features.symbol, side=side)
        if not allowed:
            return DesiredQuote(
                slot=slot,
                decision=QuoteDecision.NO_QUOTE,
                price=None,
                size=None,
                gross_edge_usdc=Usd(0),
                reason=global_reason or "inventory_side_blocked",
            )
        if size < config.min_size:
            return DesiredQuote(
                slot=slot,
                decision=QuoteDecision.NO_QUOTE,
                price=None,
                size=None,
                gross_edge_usdc=Usd(0),
                reason="size_below_minimum",
            )
        if gross_edge <= config.min_expected_pnl_usdc:
            return DesiredQuote(
                slot=slot,
                decision=QuoteDecision.NO_QUOTE,
                price=None,
                size=None,
                gross_edge_usdc=Usd(0),
                reason="expected_edge_below_threshold",
            )
        return DesiredQuote(
            slot=slot,
            decision=QuoteDecision.QUOTE,
            price=price,
            size=size,
            gross_edge_usdc=gross_edge,
            reason="positive_expected_edge",
        )

    @staticmethod
    def _to_step(value: Decimal, step: Decimal, rounding: str) -> Decimal:
        units = (value / step).to_integral_value(rounding=rounding)
        return units * step
