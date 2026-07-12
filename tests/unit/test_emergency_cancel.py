"""Tests for the fsync-backed, cancel-only emergency execution path."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from hypeedge.core.exceptions import ExecutionError
from hypeedge.execution.emergency_cancel import (
    EmergencyCancelJournal,
    EmergencyCancelTarget,
    SdkAuthoritativeOpenOrderProvider,
    WalEmergencyCancelExecutor,
)


class MutableOpenOrders:
    def __init__(self, targets: list[EmergencyCancelTarget]) -> None:
        self.targets = list(targets)

    async def get_open_orders(self) -> list[EmergencyCancelTarget]:
        return list(self.targets)

    def remove(self, *, symbol: str, cloid: str | None = None, oid: int | str | None = None) -> None:
        self.targets = [
            target
            for target in self.targets
            if not (
                target.symbol == symbol
                and ((cloid is not None and target.cloid == cloid) or (oid is not None and target.oid == oid))
            )
        ]


class FakeExchange:
    def __init__(self, orders: MutableOpenOrders, journal_path: Path, *, remove_on_cancel: bool = True) -> None:
        self._orders = orders
        self._journal_path = journal_path
        self._remove_on_cancel = remove_on_cancel
        self.calls: list[tuple[str, str]] = []

    def _assert_intent_was_fsynced(self) -> None:
        records = [json.loads(line) for line in self._journal_path.read_text().splitlines()]
        assert records[-1]["event"] == "dispatch_intent"

    def cancel_by_cloid(self, symbol: str, cloid: object) -> dict[str, object]:
        self._assert_intent_was_fsynced()
        canonical = str(cloid)
        self.calls.append((symbol, canonical))
        if self._remove_on_cancel:
            self._orders.remove(symbol=symbol, cloid=canonical)
        return {"status": "ok", "response": {"data": {"statuses": ["success"]}}}

    def cancel(self, symbol: str, oid: int | str) -> dict[str, object]:
        self._assert_intent_was_fsynced()
        self.calls.append((symbol, str(oid)))
        if self._remove_on_cancel:
            self._orders.remove(symbol=symbol, oid=oid)
        return {"status": "ok", "response": {"data": {"statuses": ["success"]}}}


class FakeSerialSignedActions:
    def __init__(self, exchange: FakeExchange | None) -> None:
        self._exchange = exchange
        self.submissions = 0

    @property
    def exchange(self) -> FakeExchange | None:
        return self._exchange

    async def submit(
        self,
        action_fn: Any,
        *args: Any,
        cloid_hint: str | None = None,
        **kwargs: Any,
    ) -> Any:
        del cloid_hint
        self.submissions += 1
        return action_fn(*args, **kwargs)


def _build_executor(
    tmp_path: Path,
    targets: list[EmergencyCancelTarget],
) -> tuple[WalEmergencyCancelExecutor, MutableOpenOrders, FakeSerialSignedActions, EmergencyCancelJournal]:
    orders = MutableOpenOrders(targets)
    journal = EmergencyCancelJournal(tmp_path / "emergency-cancel.jsonl")
    signed_actions = FakeSerialSignedActions(FakeExchange(orders, journal.path))
    return WalEmergencyCancelExecutor(signed_actions, orders, journal), orders, signed_actions, journal


class TestWalEmergencyCancelExecutor:
    async def test_cancel_writes_intent_before_using_single_signing_outlet(self, tmp_path: Path) -> None:
        target = EmergencyCancelTarget("BTC", cloid="0x" + "a" * 32)
        executor, orders, signed_actions, journal = _build_executor(tmp_path, [target])

        result = await executor.cancel(target)

        assert result.success is True
        assert result.outcome == "cancelled"
        assert orders.targets == []
        assert signed_actions.submissions == 1
        records = await journal.read_records()
        assert [record.event for record in records] == ["dispatch_intent", "transport_result", "verified_absent"]
        assert journal.path.stat().st_mode & 0o777 == 0o600

    async def test_cancel_absent_order_does_not_sign(self, tmp_path: Path) -> None:
        target = EmergencyCancelTarget("BTC", cloid="0x" + "b" * 32)
        executor, _, signed_actions, journal = _build_executor(tmp_path, [])

        result = await executor.cancel(target)

        assert result.success is True
        assert result.outcome == "already_absent"
        assert signed_actions.submissions == 0
        assert (await journal.read_records())[0].event == "already_absent"

    async def test_cancel_all_uses_authoritative_targets_and_symbol_filter(self, tmp_path: Path) -> None:
        btc_cloid = EmergencyCancelTarget("BTC", cloid="0x" + "c" * 32)
        btc_oid = EmergencyCancelTarget("BTC", oid=42)
        eth = EmergencyCancelTarget("ETH", oid=99)
        executor, orders, signed_actions, _ = _build_executor(tmp_path, [btc_cloid, btc_oid, eth])

        result = await executor.cancel_all("BTC")

        assert result.success is True
        assert result.requested == 2
        assert result.cancelled == 2
        assert orders.targets == [eth]
        assert signed_actions.submissions == 2

    async def test_order_still_open_remains_pending_for_recovery(self, tmp_path: Path) -> None:
        target = EmergencyCancelTarget("BTC", oid=88)
        orders = MutableOpenOrders([target])
        journal = EmergencyCancelJournal(tmp_path / "emergency-cancel.jsonl")
        signed_actions = FakeSerialSignedActions(FakeExchange(orders, journal.path, remove_on_cancel=False))
        executor = WalEmergencyCancelExecutor(signed_actions, orders, journal)

        result = await executor.cancel(target)

        assert result.success is False
        assert result.outcome == "still_open"
        assert result.error == "authoritative_order_still_open"
        assert len(await journal.pending_attempts()) == 1

    async def test_missing_signed_exchange_is_journaled_and_rejected(self, tmp_path: Path) -> None:
        target = EmergencyCancelTarget("BTC", oid=91)
        orders = MutableOpenOrders([target])
        journal = EmergencyCancelJournal(tmp_path / "emergency-cancel.jsonl")
        executor = WalEmergencyCancelExecutor(FakeSerialSignedActions(None), orders, journal)

        with pytest.raises(ExecutionError, match="signed_action_exchange_not_configured"):
            await executor.cancel(target)

        assert [record.event for record in await journal.read_records()] == ["dispatch_intent", "transport_error"]

    async def test_wal_recovery_replays_only_still_open_target(self, tmp_path: Path) -> None:
        open_target = EmergencyCancelTarget("BTC", cloid="0x" + "d" * 32)
        absent_target = EmergencyCancelTarget("ETH", oid=7)
        journal = EmergencyCancelJournal(tmp_path / "emergency-cancel.jsonl")
        await journal.append(attempt_id="crashed-open", event="dispatch_intent", target=open_target)
        await journal.append(attempt_id="crashed-absent", event="dispatch_intent", target=absent_target)

        orders = MutableOpenOrders([open_target])
        signed_actions = FakeSerialSignedActions(FakeExchange(orders, journal.path))
        executor = WalEmergencyCancelExecutor(signed_actions, orders, journal)

        result = await executor.recover_pending()

        assert result.success is True
        assert result.requested == 2
        assert result.cancelled == 2
        assert signed_actions.submissions == 1
        assert await journal.pending_attempts() == ()


class TestEmergencyCancelJournal:
    async def test_missing_journal_has_no_pending_attempts(self, tmp_path: Path) -> None:
        journal = EmergencyCancelJournal(tmp_path / "missing.jsonl")

        assert await journal.read_records() == ()
        assert await journal.pending_attempts() == ()

    async def test_reader_ignores_torn_final_record_after_crash(self, tmp_path: Path) -> None:
        target = EmergencyCancelTarget("BTC", oid=123)
        journal = EmergencyCancelJournal(tmp_path / "emergency-cancel.jsonl")
        await journal.append(attempt_id="valid", event="dispatch_intent", target=target)
        with journal.path.open("ab") as stream:
            stream.write(b'{"attempt_id":"torn"')

        records = await journal.read_records()

        assert len(records) == 1
        assert records[0].attempt_id == "valid"

    async def test_malformed_complete_record_is_not_silently_ignored(self, tmp_path: Path) -> None:
        journal = EmergencyCancelJournal(tmp_path / "emergency-cancel.jsonl")
        journal.path.write_text("not-json\n")

        with pytest.raises(ExecutionError, match="Malformed emergency cancel journal"):
            await journal.read_records()

    def test_target_requires_symbol_and_identifier(self) -> None:
        with pytest.raises(ValueError, match="requires a symbol"):
            EmergencyCancelTarget("", oid=1)
        with pytest.raises(ValueError, match="requires cloid or oid"):
            EmergencyCancelTarget("BTC")


class StaticSdkInfo:
    def __init__(self, response: object) -> None:
        self.response = response

    def open_orders(self, account_address: str) -> object:
        assert account_address == "0xaccount"
        return self.response


class TestSdkAuthoritativeOpenOrderProvider:
    async def test_parses_cloid_and_oid_targets(self) -> None:
        provider = SdkAuthoritativeOpenOrderProvider(
            StaticSdkInfo(
                [
                    {"coin": "BTC", "cloid": "0x" + "e" * 32, "oid": 1},
                    {"coin": "ETH", "cloid": None, "oid": 2},
                ]
            ),
            "0xaccount",
        )

        targets = await provider.get_open_orders()

        assert targets == [
            EmergencyCancelTarget("BTC", cloid="0x" + "e" * 32, oid=1),
            EmergencyCancelTarget("ETH", oid=2),
        ]

    async def test_invalid_authoritative_response_raises(self) -> None:
        provider = SdkAuthoritativeOpenOrderProvider(StaticSdkInfo({"not": "a list"}), "0xaccount")

        with pytest.raises(ExecutionError, match="Invalid authoritative open-orders response"):
            await provider.get_open_orders()
