# Hyperliquid testnet integration gate

These tests send real signed actions to Hyperliquid **testnet**. They are skipped by default and refuse mainnet,
custom endpoints, shared accounts, pre-existing target-symbol positions, and accounts with existing open orders.

Use a dedicated disposable testnet account. Never supply mainnet credentials. The suite places ALO (post-only) buy
orders below the current mid, caps mark-price exposure at USD 25, cancels only cloids it created, and closes a
test-created target-symbol position if the market moves through the resting order before cleanup.

Required explicit opt-in:

```bash
export HYPEEDGE_RUN_TESTNET_INTEGRATION=I_UNDERSTAND_THIS_PLACES_TESTNET_ORDERS
export HYPEEDGE_TESTNET_DEDICATED_ACCOUNT=I_CONFIRM_THIS_IS_A_DEDICATED_TESTNET_ACCOUNT
export HYPE_ENVIRONMENT=testnet
export HYPE_EXCHANGE__API_URL=https://api.hyperliquid-testnet.xyz
export HYPE_EXCHANGE__ACCOUNT_ADDRESS=0x...            # dedicated testnet account/vault
export HYPE_EXCHANGE__AGENT_PRIVATE_KEY=<TESTNET_AGENT_PRIVATE_KEY>  # authorized testnet agent only
export HYPE_TESTNET_POSTGRES__URL=postgresql+asyncpg://.../hypeedge_testnet
export HYPEEDGE_TESTNET_MAX_NOTIONAL_USD=15             # required, allowed range 12.50..25.00

uv run pytest tests/integration/ -m testnet -v
```

The Postgres database must be dedicated to tests, its name must contain `test`, and Alembic must already be at the
repository head (`uv run alembic upgrade head`). `HYPE_POSTGRES__URL` may be used instead of
`HYPE_TESTNET_POSTGRES__URL`. After explicit opt-in, an unreachable/outdated database, invalid exchange response,
non-empty account, insufficient balance, or insufficient action credits is a hard failure rather than a skip.

Optional controls:

- `HYPEEDGE_TESTNET_SYMBOL` defaults to `BTC`.
- `HYPEEDGE_TESTNET_RESTING_OFFSET` defaults to `0.03`; allowed range is `0.02..0.10`.

The live gates cover:

- post-only resting limit placement, exchange query by cloid, and cancellation;
- repeated canonical-cloid idempotency (at most one exchange order and the same local order result);
- Kill Switch cancel-all, placement rejection while halted, live reconciliation/action-credit refresh, and recovery.

The fixture uses the real `HypeEdgeApp` V2 startup path: schema verification, durable repositories/command worker,
SDK wiring, nonce worker, authenticated event ingestion/history recovery, startup reconciliation, and action-credit
refresh. Unrelated ClickHouse, public market-data, Prometheus, API-server, and strategy daemons are held idle so this
destructive gate cannot launch the configured strategy or depend on local observability services.
