# HypeEdge

HypeEdge 是面向 Hyperliquid 永续合约的个人量化交易系统。架构与安全约束以
[`docs/design.md`](docs/design.md) 为准。

## 本地开发

```bash
uv sync
cp .env.example .env
uv run pytest tests/unit/
uv run hypeedge
```

前端仪表盘：

```bash
cd web
pnpm install
pnpm dev
```

dev/testnet 默认可按**纯内网、无鉴权**运行（API/Dashboard 凭证留空）；mainnet 必须注入 Agent Wallet、
Postgres 与 admin API token。详见 [`docs/deployment.md`](docs/deployment.md)。

## 质量检查

```bash
uv run ruff check src/ tests/
uv run mypy src/
uv run pytest tests/
```

```bash
cd web
pnpm lint
pnpm typecheck
pnpm test
pnpm build
```
