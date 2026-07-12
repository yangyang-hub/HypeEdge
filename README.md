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

dev/testnet 保留本地数据库默认值以方便开发；mainnet 不接受这些默认凭证。实盘部署前必须阅读
[`docs/deployment.md`](docs/deployment.md)，通过环境变量注入 Agent Wallet、Postgres、后端 API token
和 Dashboard Basic Auth 凭证。

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
