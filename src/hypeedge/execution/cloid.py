"""Client order ID (cloid) generator for idempotent order submission."""

from __future__ import annotations

import hashlib
import time
import uuid

import structlog

from hypeedge.core.types import Cloid, StrategyId

logger = structlog.get_logger(__name__)

# HL SDK Cloid format: 0x + 32 hex chars (16 bytes)
_HL_CLOID_LEN = 34  # "0x" + 32 hex


class CloidGenerator:
    """Generates unique client order IDs for idempotent order submission.

    Internal format: {strategy_id}_{timestamp_ms}_{short_uuid} (human-readable)
    HL exchange format: 0x + 32 hex chars (converted via to_hl_cloid())

    Max 64 chars (Hyperliquid cloid limit).
    """

    @staticmethod
    def generate(strategy_id: StrategyId | None = None) -> Cloid:
        """Generate a new unique cloid."""
        ts = int(time.time() * 1000)
        short_id = uuid.uuid4().hex[:8]
        prefix = str(strategy_id)[:20] if strategy_id else "sys"
        cloid_str = f"{prefix}_{ts}_{short_id}"

        if len(cloid_str) > 64:
            cloid_str = cloid_str[:64]

        return Cloid(cloid_str)

    @staticmethod
    def generate_for_strategy(strategy_id: StrategyId, seq: int = 0) -> Cloid:
        """Generate a deterministic cloid for a specific strategy sequence number."""
        ts = int(time.time() * 1000)
        cloid_str = f"{strategy_id}_{ts}_{seq:04d}"
        if len(cloid_str) > 64:
            cloid_str = cloid_str[:64]
        return Cloid(cloid_str)

    @staticmethod
    def to_hl_cloid(cloid: Cloid) -> str:
        """Convert our internal Cloid to the HL SDK's 0x-prefixed hex format.

        The HL exchange requires cloid as a 0x + 32 hex char string (16 bytes).
        We deterministically hash our human-readable cloid to produce this format.
        """
        raw = str(cloid)
        if len(raw) == _HL_CLOID_LEN and raw.startswith("0x"):
            try:
                int(raw[2:], 16)
            except ValueError:
                pass
            else:
                return raw.lower()
        digest = hashlib.sha256(raw.encode()).hexdigest()[:32]
        return f"0x{digest}"

    @staticmethod
    def validate(cloid_str: str) -> bool:
        """Validate that a cloid string is non-empty and within length limits."""
        if not cloid_str or not cloid_str.strip():
            return False
        return len(cloid_str) <= 64
