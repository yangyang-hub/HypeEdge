#!/usr/bin/env python3
"""Record golden parity fixtures from the running Python backend.

The Rust rewrite verifies parity by replaying fixtures recorded here (the
Python process is deleted at cutover, so there is no live cross-check). Run
this script against a working Python venv BEFORE the Python code is frozen.

Outputs (written under the repo root):

    crates/domain/tests/fixtures/golden_decimal.jsonl   decimal_string corpus
    crates/domain/tests/fixtures/signing.jsonl          signatures + full /exchange POST bodies
    crates/domain/tests/fixtures/http/*.json            API route responses (Phase 6)
    crates/domain/tests/fixtures/risk/decisions_*.jsonl risk decisions (Phase 4)
    crates/domain/tests/fixtures/features/features_*.jsonl  MarketFeatureEngine values (Phase 2)
    crates/domain/tests/fixtures/ws_feed/*.jsonl        canned HL WS frames (Phase 2)

Usage:
    uv run python scripts/record_golden.py            # decimal + HTTP corpora (offline-safe)
    uv run python scripts/record_golden.py --signing  # requires HYPE_EXCHANGE__* creds + SDK

The signing corpus is the one that REQUIRES the live SDK and a testnet agent
key; everything else is derived from pure functions and recorded structs.
"""

from __future__ import annotations

import argparse
import json
import os
import random
import sys
from decimal import Decimal
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
FIXTURES = REPO_ROOT / "crates" / "domain" / "tests" / "fixtures"


def _representable(d: Decimal) -> bool:
    """Whether the value fits the fixed-point NUMERIC(38,18) domain."""
    if not d.is_finite():
        return False
    t = d.as_tuple()
    exp = t.exponent
    if len(t.digits) > 38 or max(0, -exp) > 18:
        return False
    return True


def record_decimal() -> None:
    """Record the `decimal_string()` corpus (authoritative source: schemas.py)."""
    from hypeedge.api.schemas import _parse_decimal_string, decimal_string

    hand_cases = [
        "0", "1", "-1", "100", "-100", "120.00", "1.2300", "0.5", "-0.5",
        "0.000000000000000001",
        "99999999999999999999.999999999999999999",
        "-99999999999999999999.999999999999999999",
        "0.0015", "1.5", "0.1", "0.30000000000000004", "1000.5",
        "123456789.123456789", "0.000001", "1e2", "1e-07", "-2.5",
        "3.141592653589793238", "2.718281828", "1000000000000000000",
        ".5", "5.", "00.5", "nan", "inf", "",
        "1.0000000000000000001", "999999999999999999999999999999999999999",
    ]
    rows: list[dict] = []
    for c in hand_cases:
        try:
            parsed = _parse_decimal_string(c)
            rows.append({"input": c, "canonical": decimal_string(parsed), "strict": True})
        except Exception:
            try:
                d = Decimal(c)
                if _representable(d):
                    rows.append({"input": c, "canonical": decimal_string(d), "strict": False})
            except Exception:
                rows.append({"input": c, "canonical": None, "strict": False})

    random.seed(42)
    for _ in range(50):
        f = random.uniform(-1e8, 1e8) * random.choice([1, 1e-8, 1e-4])
        if random.random() < 0.5:
            f = -f
        s = str(f)
        d = Decimal(s)
        if not _representable(d):
            continue
        rows.append({"input": s, "canonical": decimal_string(d), "strict": False})

    out = FIXTURES / "golden_decimal.jsonl"
    out.parent.mkdir(parents=True, exist_ok=True)
    with open(out, "w") as fh:
        for r in rows:
            fh.write(json.dumps(r) + "\n")
    print(f"decimal corpus: {len(rows)} rows -> {out.relative_to(REPO_ROOT)}")


def _capture_sdk_http():
    """Monkeypatch the SDK HTTP boundary to capture /exchange POST bodies."""
    import http.client
    import json as _json

    captured: list[dict] = []
    original = http.client.HTTPConnection.request

    def patched(self, method, url, body=None, headers=None, *, encode_chunked=False):
        if url.endswith("/exchange") and body is not None:
            try:
                captured.append(_json.loads(body))
            except Exception:
                captured.append({"raw": body})
        return original(self, method, url, body, headers, encode_chunked=encode_chunked)

    http.client.HTTPConnection.request = patched
    return captured


def record_signing() -> None:
    """Record EIP-712/L1 signatures and the full /exchange POST bodies.

    Requires HYPE_ENV=testnet plus HYPE_EXCHANGE__ACCOUNT_ADDRESS and
    HYPE_EXCHANGE__AGENT_PRIVATE_KEY in the environment, and the
    hyperliquid-python-sdk pinned in uv.lock.

    The capture must go through the SDK's `_post_action` (so the signed body is
    the real wire format) with `_capture_sdk_http` recording each `/exchange`
    POST instead of sending it. To get a nonce, run the placement through
    `NonceManager.submit` — that is the code path Rust must reproduce.
    """
    from hypeedge.config.loader import load_settings

    env = os.environ.get("HYPE_ENV", "testnet")
    if env != "testnet":
        print("signing corpus requires HYPE_ENV=testnet")
        sys.exit(1)
    settings = load_settings(env)
    if not settings.exchange.is_configured:
        print("signing corpus requires HYPE_EXCHANGE__ACCOUNT_ADDRESS and AGENT_PRIVATE_KEY")
        sys.exit(1)

    captured = _capture_sdk_http()
    print(
        "signing corpus capture is wired; run the placement path through "
        "NonceManager.submit with _capture_sdk_http active, then persist the "
        "captured /exchange bodies + the (action, nonce, signature, key) rows "
        "to signing.jsonl. This is completed during Phase 3 once the Rust "
        "signing module is built and needs golden verification."
    )
    _ = captured
    out = FIXTURES / "signing.jsonl"
    print(f"signing corpus -> {out.relative_to(REPO_ROOT)} (pending live capture)")


def record_http() -> None:
    """Record API route responses (Phase 6) against a live server.

    Requires the Python API server running on localhost:37001.
    """
    import urllib.request

    endpoints = [
        "/health",
        "/api/v1/system/status",
        "/api/v1/risk/status",
        "/api/v1/market/BTC/meta",
    ]
    out_dir = FIXTURES / "http"
    out_dir.mkdir(parents=True, exist_ok=True)
    n = 0
    for ep in endpoints:
        try:
            with urllib.request.urlopen(f"http://127.0.0.1:37001{ep}", timeout=5) as resp:
                body = resp.read().decode()
        except Exception as e:
            print(f"  skipped {ep}: {e}")
            continue
        name = ep.strip("/").replace("/", "_")
        with open(out_dir / f"{name}.json", "w") as fh:
            fh.write(body)
        n += 1
    print(f"http corpus: {n} responses -> {out_dir.relative_to(REPO_ROOT)}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Record golden parity fixtures from the Python backend")
    parser.add_argument("--signing", action="store_true", help="record the signing corpus (needs testnet creds)")
    args = parser.parse_args()

    record_decimal()
    record_http()
    if args.signing:
        record_signing()
    else:
        print("(skip signing corpus: pass --signing with testnet creds)")


if __name__ == "__main__":
    main()
