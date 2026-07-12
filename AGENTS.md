# AGENTS.md — HypeEdge 项目指南

## 项目概述

HypeEdge 是一个面向 Hyperliquid 永续合约交易所的个人量化交易系统。采用 Python asyncio 单进程模块化单体架构，分三阶段实现：数据采集 → 实盘策略 → 做市/HFT。

设计文档：`docs/design.md`（所有架构决策的权威来源，修改前必读）。

## 技术栈

- **语言**：Python 3.12+（异步，类型注解）
- **包管理**：uv（`uv sync` 安装依赖，`uv run` 执行命令）
- **异步框架**：asyncio（单事件循环，禁止为业务逻辑引入线程）
- **存储**：ClickHouse（行情时序数据）、Postgres（订单/持仓事务数据，SQLAlchemy 2.0 async + asyncpg）
- **配置**：pydantic-settings + YAML 文件（`configs/{dev,testnet,mainnet}.yaml`）
- **日志**：structlog（JSON 结构化，生产用 JSONRenderer，开发用 ConsoleRenderer）
- **监控**：prometheus-client + Grafana
- **Lint**：ruff（format + lint，line-length=120）
- **类型检查**：mypy --strict
- **测试**：pytest + pytest-asyncio（asyncio_mode="auto"）

## 项目结构

```
src/hypeedge/
├── core/          # 共享基础：类型(types)、枚举(enums)、模型(models)、EventBus(events)、异常(exceptions)
├── config/        # 配置加载：settings(pydantic-settings)、loader(YAML)
├── market_data/   # WS行情(ws_feed)、REST回填(rest_client)、限速器(rate_limiter)、订单簿(book)、数据供应接口(provider)
├── storage/       # ClickHouse写入(clickhouse)、Postgres ORM + PostgresWriter(postgres)、数据质量(data_quality)、去重(dedup)
├── monitor/       # Prometheus指标(metrics)、告警(alerts, 骨架)
├── strategy/      # 策略基类(base)、趋势跟随(trend_follow)、技术指标(indicators)、参数管理(params + 热更新)
├── risk/          # 风控检查(checker)、Kill Switch(kill_switch) — 触发时自动撤单
├── execution/     # 执行引擎(engine)、Nonce管理(nonce, SDK集成)、订单状态机(order_state)、Cloid生成(cloid)
├── account/       # 账户追踪(tracker)、对账(reconciler) — 启动门控
├── backtest/      # 模拟撮合(broker)、回测引擎(engine)、绩效指标(metrics)、Walk-Forward/Monte Carlo(walk_forward)
├── app.py         # HypeEdgeApp 主类（全链路串联、启动对账门控、策略热更新、优雅停机）
└── __main__.py    # CLI 入口
configs/           # 环境配置文件 + 策略参数文件(strategy_trend.yaml)
tests/             # 单元测试(unit/)、集成测试(integration/)
```

**实现状态**：
- **Phase 1 完成**：market_data、storage(clickhouse)、backtest
- **Phase 2 完成**：execution、risk、account、strategy(trend_follow)、app.py 全链路串联
- **骨架**：monitor/alerts.py（Telegram/钉钉告警未实现）、backtest round-trip PnL 简化版

## 常用命令

```bash
uv sync                    # 安装依赖
uv run pytest tests/unit/  # 运行单元测试
uv run pytest tests/ -v    # 运行全部测试（含集成测试，需网络/服务）
uv run ruff check src/ tests/    # Lint 检查
uv run ruff format src/ tests/   # 格式化
uv run mypy src/           # 类型检查
uv run hypeedge            # 启动应用
make lint && make test     # 一键检查
```

## 编码规范

### 通用

- 遵循现有代码风格：注释密度、命名习惯、import 顺序。
- 所有公共函数和类必须带类型注解。
- 使用 `from __future__ import annotations` 在每个模块顶部。
- 日志使用 `structlog.get_logger(__name__)`，关键操作绑定 contextvars（cloid、strategy_id）。
- 错误使用自定义异常层级（`core/exceptions.py`），不抛裸 Exception。
- 私有属性用 `_` 前缀，类型注解用 `ClassVar` 或实例属性。

### 异步

- 所有 I/O 操作使用 async/await，禁止在业务逻辑中使用 `threading`。
- 长时间运行的任务用 `asyncio.create_task()` 在事件循环中并发。
- 阻塞操作（如 ClickHouse 写入）使用 `run_in_executor()`。

### 数据模型

- 使用 `core/types.py` 的语义类型（`Symbol`, `Price`, `Size`, `Cloid` 等），不直接用裸 str/float。
- 领域模型定义在 `core/models.py`，用 `@dataclass` 或 pydantic。
- 枚举定义在 `core/enums.py`。
- 模块间通信通过 EventBus 事件（`core/events.py` 的常量），不直接调用其他模块方法。

### 配置

- 新增配置项在 `config/settings.py` 对应的 Settings 类中添加，带 Field 约束（ge/le）和默认值。
- 三个环境配置文件 `configs/*.yaml` 须同步更新。
- 密钥/私钥只通过环境变量传入，不写进代码或 YAML。

## 模块间通信模式

```
market_data ──publish──▶ EventBus ──queue──▶ strategy
                                              │
                                         OrderIntent
                                              │
                                              ▼
                              risk(同步内联, 500ms超时, fail-safe)
                                              │
                                              ▼
                              execution(串行nonce队列) ──▶ Hyperliquid
                                              │
                                              ▼
                              account/reconciler ◀── 交易所对账
```

- **EventBus** 是唯一的模块间通信通道（发布/订阅，asyncio.Queue per subscriber）。
- 策略通过注入的 `ExecutionClient` 提交订单意图，不直接访问 ExecutionEngine。
- 风控在执行路径中同步内联，超时 = 拒绝（fail-safe）。
- 所有签名操作汇聚到 NonceManager 的单队列串行处理。

## Hyperliquid 平台关键约束（必须遵守）

- **IP 权重**：1200 weight/min，轻量端点(l2Book等)权重2，普通info权重20，exchange = 1 + floor(batch/40)。
- **地址动作额度**：初始10,000，1动作/USDC成交量，额度耗尽系统停摆。做市前必须估算额度消耗。
- **按条目权重**：fundingHistory 等端点每返回20条+1权重，candleSnapshot每60条+1。回填需分页限速。
- **expiresAfter 过期撤单**：5x 动作额度惩罚，尽量避免。
- **挂单≥1000** 时：reduce-only 和止损单被拒，网格/做市须留额度。
- **WS 限制**：10连接/IP，1000订阅，2000 msg/min。
- **funding 每小时结算**，非币安 8h。

## 测试要求

- 每个 bug fix 和新功能必须有对应测试。
- 单元测试放 `tests/unit/`，集成测试放 `tests/integration/`。
- 测试异步代码用 `@pytest.mark.asyncio`（已在 pyproject.toml 开启 auto 模式）。
- 测试风控逻辑用已知输入/输出覆盖边界条件。
- 测试订单状态机验证每个状态转换的合法性。
- 核心逻辑覆盖率目标 ≥ 90%。

## 安全红线

- 主钱包私钥永不进交易进程。
- agent wallet 私钥只通过环境变量传入，不硬编码。
- `configs/mainnet.yaml` 在 `.gitignore` 中，不入版本控制。
- `.env` 不入版本控制。
- 下单前必须通过风控检查，无例外。

## 修改时的注意事项

- 修改 `core/enums.py` 的枚举值时，同步更新 `ORDER_TRANSITIONS` 字典。
  - 注意 `WsChannel` 中的 `USER_FILLS` 和 `ORDER_UPDATES` 为 Phase 2 预留（需认证），Phase 1 不使用。
- 新增 EventBus 事件类型时，在 `core/events.py` 的 `ALL_EVENT_TYPES` 集合中注册。
- 修改 ClickHouse 表结构时，同步更新 `storage/clickhouse.py` 的 DDL_STATEMENTS 和 `docs/design.md` §5.2。
- 新增模块接口时，先定义 Protocol/ABC，在骨架文件中占位，再实现。
- 修改配置结构后，运行 `uv run pytest tests/unit/test_config.py` 验证。

---

## 编码实现规范

详细的编码规范存放在 `rules/` 目录下，按前后端分离：

- **后端规范**：[`rules/backend.md`](rules/backend.md) — Python 后端的架构约束、类型系统、异步模式、EventBus、风控、执行引擎、存储层、配置、日志、错误处理、测试规范。
- **前端规范**：[`rules/frontend.md`](rules/frontend.md) — Next.js + React + shadcn/ui 的组件设计、类型系统、数据获取、样式、性能、API 契约、测试规范。

### 前后端共享约束

#### 通用原则

- **先设计文档，后写代码**：新功能先更新 `docs/design.md`，再实现。
- **先接口，后实现**：Protocol/ABC/interface 先定义，测试可 mock。
- **先测试，后上线**：单元测试通过 → 集成测试通过 → testnet 验证 → mainnet。
- **最小变更原则**：每次 commit 只做一件事，方便回滚和 review。

#### 数据一致性

- 前端显示的价格/数量精度与后端一致：
  - 价格：2 位小数（BTC）或 4 位小数（山寨币），由后端 `meta` 接口提供精度信息。
  - 数量：按币种精度显示，不自行截断。
  - 百分比：2 位小数 + `%`（如 `12.34%`）。
- 时间显示统一为 UTC + 本地时区转换，格式 `YYYY-MM-DD HH:mm:ss`。
- PnL 颜色：全球统一（正=绿、负=红），不使用地区性红绿反转。

#### 错误处理

- 后端错误通过 API 响应码 + 结构化错误信息传递。
- 前端对每个 API 调用做错误处理，显示 toast 通知用户。
- 网络断连时前端显示连接状态指示器 + 最后更新时间。
- 后端 kill switch 触发时，前端全屏红色告警横幅。

#### 版本同步

- 后端新增/修改 API 时，同步更新前端 `lib/types.ts`。
- 后端新增配置项时，前端设置页面同步添加对应控件。
- 后端新增事件类型时，评估是否需要前端实时展示。
