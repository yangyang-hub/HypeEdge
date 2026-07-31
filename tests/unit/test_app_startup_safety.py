from __future__ import annotations

import asyncio
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

from hypeedge.app import HypeEdgeApp
from hypeedge.config.settings import AppSettings
from hypeedge.core.enums import SafetyMode
from hypeedge.market_data.instrument_cache import InstrumentMetaCache


async def test_credentials_do_not_initialize_trading_when_v2_flags_are_incomplete(tmp_path: Path) -> None:
    settings = AppSettings(
        environment="dev",
        exchange={
            "api_url": "https://api.hyperliquid-testnet.xyz",
            "ws_url": "wss://api.hyperliquid-testnet.xyz/ws",
            "account_address": "0x1234",
            "agent_private_key": "0xdeadbeef",
        },
        backfill={"state_dir": str(tmp_path)},
        clickhouse={"spool_path": str(tmp_path / "spool.sqlite3")},
        features={},
    )
    app = HypeEdgeApp(settings)

    await app._initialize_components()

    assert app._pg_engine is None
    assert app.execution_engine is None
    assert app._trading_prerequisites_ok is False
    assert app._safety_controller.mode == SafetyMode.CANCEL_ONLY
    assert app._safety_controller.reason == "v2_feature_set_incomplete"


def test_sdk_connection_uses_configured_testnet_url_not_environment_inference() -> None:
    settings = AppSettings(
        environment="dev",
        exchange={"api_url": "https://api.hyperliquid-testnet.xyz"},
    )
    app = HypeEdgeApp(settings)
    assert app.settings.exchange.api_url == "https://api.hyperliquid-testnet.xyz"


async def test_v2_trading_fails_before_database_or_signing_when_metadata_is_unavailable(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    ensure_loaded = AsyncMock(side_effect=RuntimeError("metadata unavailable"))
    monkeypatch.setattr(InstrumentMetaCache, "ensure_loaded", ensure_loaded)
    settings = AppSettings(
        environment="dev",
        exchange={
            "account_address": "0x1234",
            "agent_private_key": "0xdeadbeef",
        },
        backfill={"state_dir": str(tmp_path)},
        clickhouse={"spool_path": str(tmp_path / "spool.sqlite3")},
        features={
            "durable_ledger_v2": True,
            "execution_v2": True,
            "user_stream_v2": True,
            "reconciliation_v2": True,
            "api_v1": True,
            "strategy_runner_v2": True,
        },
    )

    with pytest.raises(RuntimeError, match="metadata unavailable"):
        await HypeEdgeApp(settings)._initialize_components()

    ensure_loaded.assert_awaited_once()


async def test_graceful_shutdown_disconnects_sdk_websocket_thread() -> None:
    app = HypeEdgeApp(AppSettings())
    info = MagicMock()
    info.disconnect_websocket = MagicMock()
    app._nonce_manager = SimpleNamespace(info=info, stop=AsyncMock())  # type: ignore[assignment]

    await app._graceful_shutdown()

    info.disconnect_websocket.assert_called_once_with()


async def test_graceful_shutdown_exits_uvicorn_via_should_exit_not_cancellation() -> None:
    """The api_server task must unwind through uvicorn's should_exit flag.

    Cancelling serve() directly skips Server.shutdown(), so the ASGI
    lifespan.shutdown event is never sent and starlette's lifespan parks on
    receive() — surfacing an asyncio.CancelledError traceback on teardown.
    """

    class _FakeUvicornServer:
        def __init__(self) -> None:
            self.should_exit = False
            self.lifespan_shutdown_sent = False

        async def serve(self) -> None:
            # Mirrors uvicorn main_loop(): exits on should_exit, then runs the
            # shutdown path (which emits the lifespan.shutdown event).
            while not self.should_exit:
                await asyncio.sleep(0.01)
            self.lifespan_shutdown_sent = True

    app = HypeEdgeApp(AppSettings())
    server = _FakeUvicornServer()
    app._api_server = server  # type: ignore[assignment]
    app._tasks = [asyncio.create_task(server.serve(), name="api_server")]

    await app._graceful_shutdown()

    # should_exit was requested and the serve() coroutine observed it and ran
    # its lifespan shutdown (rather than being mid-flight cancelled).
    assert server.should_exit is True
    assert server.lifespan_shutdown_sent is True
    assert app._tasks[0].cancelled() is False


@pytest.mark.parametrize(
    "placement, should_stay_alive",
    [
        ("background_tasks", True),  # one-shot tasks must not gate lifetime (the fix)
        ("tasks", False),  # a finite task in the watched set tears the app down (the bug)
    ],
)
async def test_one_shot_task_placement_gates_app_lifetime(
    monkeypatch: pytest.MonkeyPatch, placement: str, should_stay_alive: bool
) -> None:
    """A finite task returning must not tear the process down.

    Regression guard for ``market_making_restore``: it is a one-shot task that
    polls until prerequisites are fresh, restores instances, then RETURNS. It
    used to live in ``self._tasks``, which ``run()`` watches with
    ``asyncio.wait(..., FIRST_COMPLETED)`` — so the moment it finished (or found
    nothing to restore) the whole app exited ~0.25s after ``hypeedge_running``
    with no signal and no error. One-shot tasks belong in ``self._background_tasks``.
    """
    app = HypeEdgeApp(AppSettings())
    started = asyncio.Event()
    release_long_lived = asyncio.Event()

    async def fake_initialize(self: HypeEdgeApp) -> None:
        return None

    async def fake_start(self: HypeEdgeApp) -> list[asyncio.Task[Any]]:
        async def long_lived() -> None:
            started.set()
            await release_long_lived.wait()

        async def one_shot() -> None:
            return  # returns immediately

        self._tasks = [asyncio.create_task(long_lived(), name="long_lived")]
        one_shot_task = asyncio.create_task(one_shot(), name="one_shot")
        if placement == "tasks":
            self._tasks.append(one_shot_task)
        else:
            self._background_tasks.append(one_shot_task)
        return self._tasks

    async def fake_graceful(self: HypeEdgeApp) -> None:
        for task in self._tasks + self._background_tasks:
            task.cancel()
        await asyncio.gather(*self._tasks, *self._background_tasks, return_exceptions=True)

    monkeypatch.setattr(HypeEdgeApp, "_initialize_components", fake_initialize)
    monkeypatch.setattr(HypeEdgeApp, "_start_components", fake_start)
    monkeypatch.setattr(HypeEdgeApp, "_graceful_shutdown", fake_graceful)

    run_task = asyncio.create_task(app.run())
    await asyncio.wait_for(started.wait(), timeout=2.0)
    await asyncio.sleep(0.1)  # let the one-shot return

    if should_stay_alive:
        assert not run_task.done(), "one-shot background task shut the app down"
    else:
        assert run_task.done(), "watched one-shot task failed to trigger shutdown"

    # Whatever state, a real shutdown signal must stop the app cleanly.
    app._shutdown_event.set()
    release_long_lived.set()
    await asyncio.wait_for(run_task, timeout=2.0)



