# Golden parity fixtures

These fixtures are the authoritative cross-check for the Rust rewrite. They
were recorded from the now-deleted Python backend; with the Python process
removed at cutover, these recorded outputs ARE the reference. They are consumed
by the Rust golden-corpus tests (`crates/domain/tests/decimal_corpus.rs`,
`crates/config/tests/config_parity.rs`, and the HTTP/WS router tests).

Every fixture is JSONL (one JSON object per line) unless noted.

## `golden_decimal.jsonl`

Rows: `{"input": str, "canonical": str|null, "strict": bool}`.

- `strict: true` — `input` passes Python's `_parse_decimal_string`; our
  `Decimal::from_str_strict` must produce `canonical` (`decimal_string`).
- `strict: false` — `input` came from float-`str()` coercion or an exponent
  form; our lenient parser must produce `canonical`.
- `canonical: null` — Python rejected the input entirely; ours must too.

## `signing.jsonl` (captured during Phase 3)

Rows: `{action, nonce, vault_address, expires_after, is_mainnet, private_key,
signature: {r,s,v}, post_body}`. `post_body` is the exact JSON sent to
`/exchange`, captured at the SDK HTTP boundary. Rust asserts byte-identical
signature and POST body.

## `http/*.json`

Canned API responses, one file per endpoint. Rust router tests assert equality
after stripping volatile fields (`timestamp`, `request_id`, generated UUIDs).

## `risk/decisions_*.jsonl` (Phase 4)

Rows: `{account: {...}, market: {...}, expected: {passed, reason,
checked_limits}}` for the risk checker.

## `features/features_*.jsonl` (Phase 2)

Rows: `{events: [...], expected: {microprice, normalized_ofi_l1, ...}}` for the
market feature engine. Decimal fields exact; f64 fields within tolerance.

## `ws_feed/*.jsonl` (Phase 2)

Raw Hyperliquid WS frames interleaved with reconnect boundaries, plus the
expected normalized `DomainEvent`s (including `connection_generation` and book
`version` stamps).
