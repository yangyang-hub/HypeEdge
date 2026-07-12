# HypeEdge 前端 UI 重设计 — 设计文档 v1

- 版本：v1.0
- 日期：2026-07-12
- 状态：已落地（Phase A–D）
- 前置文档：[`docs/frontend_design.md`](./frontend_design.md)（页面职责 / 数据源 / API 契约仍以该文档为准）
- 参考对象：Hyperliquid 官网 + `app.hyperliquid.xyz` 交易界面的视觉与信息密度

---

## 0. 一句话定位

HypeEdge 前端不是零售 DEX 下单页，而是面向个人量化运营者的 **暗色高密度操作台（Operator Console）**：在 Hyperliquid 同款冷静、克制的暗色交易美学之上，突出策略、风控、额度与系统健康，而不是营销落地页或通用 SaaS 仪表盘。

---

## 1. 设计目标与原则

### 1.1 目标

| 目标 | 说明 |
|------|------|
| 专业可信 | 一眼像交易终端，不像后台管理模板 |
| 高信息密度 | 桌面端优先，数字可扫读，少装饰 |
| 操作可预期 | 按钮层级清晰，危险操作有明确确认路径 |
| 状态可读 | 连接、stale、Kill Switch、环境标签永远可见 |
| 与 HL 同语境 | 颜色、排版、控件气质贴近 Hyperliquid，降低认知切换成本 |

### 1.2 非目标

- 不复制 Hyperliquid 完整「交易画布」（左盘口 / 中图 / 右下单）作为全局布局——HypeEdge 的核心工作流是监控与风控，不是手动开仓。
- 不做营销向 hero、渐变噪点、玻璃拟态堆叠、emoji 导航。
- 不引入亮色主题（v1 仅暗色；亮色可后续评估）。
- 不在本阶段改后端 API 契约（除非现有字段不足以支撑 UI）。

### 1.3 设计原则（硬约束）

1. **单一强调色**：青绿 accent 只用于主操作、选中态、Live 指示；PnL 专用绿/红，不混用。
2. **数字优先**：价格、仓位、PnL、额度用等宽数字字体；标签用小 caps / 弱对比。
3. **平面克制**：无多层阴影、无大圆角卡片堆、无紫色光晕；面板靠细边框 + 微弱表面色差分层。
4. **危险有仪式**：Kill Switch / 全部平仓 / mainnet 配置激活 = 二阶段确认，永不与普通按钮同级。
5. **密度分级**：行情/持仓/订单 = 交易终端密度；策略工作台 = 分区面板密度；设置 = 表单密度。

---

## 2. 现状问题（为何重设计）

基于当前 `web/` 实现：

| 问题 | 表现 | 影响 |
|------|------|------|
| 视觉无品牌 | `zinc-950` + system-ui + emoji 侧栏 | 像临时脚手架，不像交易终端 |
| 组件无体系 | 无 `components/ui`，按钮/输入/表格各自手写 | 交互不一致，难扩展 |
| 层级扁平 | 页面标题 + `rounded-xl` 卡片重复堆叠 | 信息分区弱，难扫读 |
| 强调色缺失 | 仅有 profit/loss/warning，无品牌 accent | 主操作与次操作难区分 |
| 导航偏「后台」 | 侧栏图标+文字，emoji | 与 HL 顶栏导航气质不符 |
| 状态分散 | 连接点在底栏，环境标签在侧栏 | 关键状态不够「一眼可见」 |
| 行情布局偏报表 | 上指标卡 + 下图/簿 | 缺少 HL 式交易终端的紧凑工具条 |

---

## 3. 视觉方向：Hyperliquid → HypeEdge

### 3.1 从 HL 借鉴什么

| HL 特征 | HypeEdge 用法 |
|---------|---------------|
| 深黑底 + 青绿强调 | 全局主题；主按钮 / 选中 / Live pulse |
| 克制暗色、少装饰 | 全站禁止营销渐变与噪点背景 |
| 高密度表格与盘口 | 持仓、订单、订单簿、做市 quotes |
| 冷静的信息层级 | 小标签 + 大数字；弱辅助文案 |
| 明确买卖语义色 | 买/多 = profit 绿；卖/空 = loss 红 |
| 顶栏状态（Online 等） | 顶栏：环境、WS/SSE、safety mode、Kill |

### 3.2 HypeEdge 自身差异（必须保留）

- **运营视角**：策略生命周期、Kill Switch、动作额度 runway、对账门禁优先于「一键开仓」。
- **中文为主**：标签、按钮、空态、错误文案中文；币种与 API 字段保持英文符号（BTC、Funding、Mark）。
- **双通道数据**：后端 SSE/REST 权威 +（行情页）标准化流；UI 必须显式展示 stale / 降级。

### 3.3 Signature（记忆点）

**「青绿 Live 脉搏 + 等宽数字网格」**：顶栏一枚细长 Live 指示（呼吸脉冲），主内容区以等宽数字表格/指标为主视觉，无插画、无 emoji 导航。

---

## 4. Design Tokens

实现落在 `web/styles/globals.css` 的 CSS 变量 + Tailwind `@theme`。下列 hex 为 v1 建议值，实现时可微调但语义名稳定。

### 4.1 色彩

```
背景层级
--bg-base:        #07080A    /* 页面最底层 */
--bg-elevated:    #0C0E12    /* 主内容区 */
--bg-panel:       #11141A    /* 面板 / 表头 */
--bg-hover:       #171B22    /* 行 hover */
--bg-active:      #1C222C    /* 选中行 / 按下 */

边框
--border-subtle:  #1A1F28
--border-default: #252B36
--border-strong:  #343B49

文字
--text-primary:   #F2F4F7
--text-secondary: #9AA3B2
--text-tertiary:  #6B7380
--text-disabled:  #4A5160

品牌强调（HL 同语境青绿）
--accent:         #50E3C2
--accent-muted:   rgba(80, 227, 194, 0.14)
--accent-hover:   #6EEAD0

语义色（仅语义用途）
--profit:         #2DD4A0    /* 略偏青，与 accent 协调；正 PnL / 多 */
--loss:           #F07178    /* 负 PnL / 空 / 拒绝 */
--warning:        #E6B84D
--critical:       #E5484D    /* Kill Switch / 不可恢复错误 */
--info:           #5B9FD4

环境标签
--env-dev:        #5B9FD4
--env-testnet:    #E6B84D
--env-mainnet:    #E5484D
```

**规则：**

- Accent **不**用于 PnL（正收益仍用 `--profit`）。
- Critical **只**用于 Kill Switch、不可逆破坏操作、mainnet 强警告。
- 图表蜡烛：涨=`--profit`，跌=`--loss`；网格线=`--border-subtle`。

### 4.2 字体

| 角色 | 字体 | 用途 |
|------|------|------|
| UI Sans | `IBM Plex Sans`（fallback: system-ui） | 导航、按钮、标签、正文 |
| Data Mono | `IBM Plex Mono`（fallback: ui-monospace） | 价格、数量、PnL、时间、地址截断 |
| Display（可选） | 同 UI Sans，字重 600–700 | 页面标题；不用装饰性衬线 |

**选型理由**：IBM Plex 具备金融/终端气质，数字 tabular 对齐好；避免 Inter/Roboto 的「通用 AI 后台」感，也避免 HL 营销站的超大展示字抢戏。

**字号阶梯（桌面）：**

| Token | Size / Line | 用途 |
|-------|-------------|------|
| `text-2xs` | 10 / 14 | 表头辅助、盘口单位 |
| `text-xs` | 12 / 16 | 标签、底栏、helper |
| `text-sm` | 13 / 18 | 表格正文、按钮 |
| `text-base` | 14 / 20 | 表单、段落 |
| `text-lg` | 16 / 24 | 面板标题 |
| `text-xl` | 20 / 28 | 页面标题 |
| `text-metric` | 24 / 32 | 关键指标数字（mono） |
| `text-price` | 28 / 32 | 行情最新价（mono，仅行情顶栏） |

数字一律 `font-variant-numeric: tabular-nums`。

### 4.3 间距与圆角

```
空间：4 / 8 / 12 / 16 / 24 / 32（禁止随意 10、14、18）
圆角：
  --radius-sm: 4px   /* 输入、小按钮、tag */
  --radius-md: 6px   /* 默认按钮、面板 */
  --radius-lg: 8px   /* 模态、大型容器 */
禁止 rounded-xl / rounded-2xl / rounded-full（pill 仅 Live 指示与环境 tag 允许）
```

### 4.4 边框与分割

- 面板：`1px solid var(--border-default)`，背景 `--bg-panel`
- 表头底边：`--border-subtle`
- 不使用 card shadow；模态可用极轻 `0 8px 32px rgba(0,0,0,0.45)`

### 4.5 动效

| 场景 | 时长 | 说明 |
|------|------|------|
| Hover / 按下 | 120ms | 颜色与边框 |
| 面板展开 | 180ms | ease-out |
| Toast 入场 | 200ms | |
| Live pulse | 1.8s loop | 仅连接指示 |
| 数字闪烁（成交更新） | 220ms | 背景色淡入淡出 |

尊重 `prefers-reduced-motion`（已有全局规则，保留）。

---

## 5. 布局骨架重设计

### 5.1 全局结构（对齐 HL 顶栏气质 + 运营侧栏）

```
┌──────────────────────────────────────────────────────────────────────────┐
│ TOPBAR  48px                                                              │
│  HypeEdge │ ENV │ Live● │ Safety │ Equity $… │ [Kill]        [账户截断]   │
├────────┬─────────────────────────────────────────────────────────────────┤
│ NAV    │  PAGE HEADER（标题 + 上下文操作）                                  │
│ 56/200 │─────────────────────────────────────────────────────────────────│
│        │                                                                  │
│ 总览   │                     MAIN CANVAS                                  │
│ 行情   │                                                                  │
│ 持仓   │                                                                  │
│ 订单   │                                                                  │
│ 策略   │                                                                  │
│ 风控   │                                                                  │
│ 设置   │                                                                  │
│        │                                                                  │
├────────┴─────────────────────────────────────────────────────────────────┤
│ STATUSBAR  28px — SSE · 行情源 · 动作额度 · 版本                            │
└──────────────────────────────────────────────────────────────────────────┘
```

**相对现状的关键变化：**

1. **新增顶栏 Topbar**：环境、连接、安全门禁、权益摘要、Kill 入口常驻（现状分散在侧栏/底栏）。
2. **侧栏去 emoji**：改为线性图标（Lucide）+ 文字；折叠态仅图标。
3. **底栏降级为系统遥测**：WS/SSE 延迟、动作额度、版本；不再承担主状态展示。
4. **Kill Switch**：顶栏右侧红色 ghost 按钮；触发后顶栏下方全宽 critical banner（不可关闭直至重置）。

### 5.2 顶栏（Topbar）规格

| 区域 | 内容 | 行为 |
|------|------|------|
| Brand | `HypeEdge` 字标（无 slogan） | 点击回总览 |
| EnvBadge | `DEV` / `TESTNET` / `MAINNET` | mainnet 红底白字，不可忽略 |
| LiveIndicator | `LIVE` / `DEGRADED` / `OFFLINE` | 脉冲点；点击展开连接详情 |
| SafetyChip | `safety_mode` 文案 | 只读 |
| EquityStrip | 权益 · 未实现 PnL（带色） | 点击跳转总览 |
| KillButton | `Kill Switch` | 打开确认抽屉（非直接触发） |
| Account | `0x1234…abcd` | 复制地址 |

### 5.3 侧栏（Sidebar）规格

- 宽：展开 200px / 折叠 56px（`md+` 可钉住）
- 项高：36px；选中：左侧 2px accent 条 + `--bg-active` + 文字 primary
- 图标：16px stroke，当前色跟随文字
- 不做二级折叠菜单（策略详情走路由 `/strategy/[id]/...`）

### 5.4 页面头（Page Header）

统一模式，避免每页自制：

```
[Title]                    [Filters / Segmented]  [Primary Action]
[Subtitle · last updated]
```

- Title：`text-xl`，无装饰线
- Subtitle：`text-xs text-tertiary`，含数据新鲜度
- Primary Action：仅一页一个主按钮（如「全部平仓」「启动策略」）

---

## 6. 组件与按钮体系

v1 引入精简 shadcn/ui 基座（Radix + CVA），按需生成，不一次装全库。

### 6.1 组件清单（优先级）

| 优先级 | 组件 | 用途 |
|--------|------|------|
| P0 | `Button` `Input` `Select` `Tabs` `Badge` `Dialog` `AlertDialog` `Tooltip` `Table` | 全站 |
| P0 | `Topbar` `Sidebar` `StatusBar` `PageHeader` `EnvBadge` `LiveIndicator` | 布局 |
| P0 | `Metric` `DataTable` `PnLText` `SideTag` `ProgressBar` `EmptyState` `StaleBanner` | 数据展示 |
| P1 | `SegmentedControl` `Sheet` `Toast` `ConfirmPhraseDialog` | 交互增强 |
| P1 | `OrderBook` `CandleToolbar` `FundingPill` | 行情 |
| P1 | `StrategyStatusChip` `KillBanner` | 风控/策略 |
| P2 | `Sparkline` `DiffView` `JsonViewer` | 做市/高级 |

### 6.2 Button 变体与功能语义

| Variant | 视觉 | 何时用 | 示例 |
|---------|------|--------|------|
| `primary` | accent 实心，文字深色 | 页内唯一主操作 | 保存参数、启动策略 |
| `secondary` | 边框 `--border-strong`，透明底 | 次要操作 | 刷新、导出、参数详情 |
| `ghost` | 无边框，hover 浅底 | 工具条、表内操作 | 撤单、查看 |
| `buy` | profit 实心/浅底 | 明确「买/开多」语义 | （若有手动单） |
| `sell` | loss 实心/浅底 | 明确「卖/开空/平仓」 | 平仓 |
| `danger` | critical 边框或实心 | 破坏性/紧急 | 触发 Kill、全部平仓 |
| `danger-soft` | critical 浅底 | 重置类但仍需谨慎 | 重置 Kill（需二次确认） |

**尺寸：** `sm` 28px（表内） / `md` 32px（默认） / `lg` 36px（顶栏主操作）。

**规则：**

- 同一视觉区域 **最多一个** `primary`。
- 表内操作默认 `ghost` + `sm`；不可用 `primary`。
- `danger` 必须配合 `AlertDialog` 或 `ConfirmPhraseDialog`（输入 `CONFIRM`）。
- Loading：按钮内 spinner，禁用重复提交；文案变为进行时（「保存中…」）。
- Disabled：opacity 0.4 + `cursor-not-allowed`，Tooltip 说明原因。

### 6.3 确认与危险流

| 操作 | 确认级别 | UI |
|------|----------|-----|
| 单笔撤单 | L1 轻确认 | AlertDialog：取消 / 确认撤单 |
| 单仓平仓 | L1 | AlertDialog：展示预估影响 |
| 全部平仓 | L2 | ConfirmPhrase：输入 `CLOSE ALL` |
| 触发 Kill Switch | L2 | ConfirmPhrase：输入 `CONFIRM` + 原因可选 |
| 重置 Kill Switch | L1 + 说明 | AlertDialog：说明重置后策略不会自动恢复 |
| mainnet 配置激活 | L2 | ConfirmPhrase + EnvBadge 强调 |

### 6.4 表单控件

- 输入框：高 32px，`--bg-elevated`，focus ring = accent 2px（替换现状 warning 黄）
- Select：与 Input 同高；行情币种/周期改用 `SegmentedControl`（更像 HL 工具条）
- 数字输入：右对齐 mono；支持 `%` 快捷（25/50/75/100）在需要处出现

### 6.5 数据展示组件

**`Metric`**

```
LABEL（2xs / tertiary / tracking-wide）
VALUE（metric / mono / primary）
DELTA（xs / profit|loss）可选
```

去掉厚卡片感：可用底部分割线或极轻 panel，避免大 padding 的「仪表盘五宫格」。

**`PnLText`**：自动着色 + 符号（`+$105.00` / `-$20.00`）；零值 tertiary。

**`SideTag`**：`多` / `空` 小徽章（绿/红浅底），替代 emoji 🟢🔴。

**`StrategyStatusChip`**：STOPPED / WARMING / SHADOW / RUNNING / PAUSED / DRAINING / FAULTED —— 固定色板，全文大写或中英对照短标签。

**`StaleBanner`**：数据超时显示在面板顶：「行情延迟 12s · 已降级 REST」，warning 色，不阻断阅读。

**`EmptyState`**：单行说明 + 一个可选操作（「去行情」），无插画。

### 6.6 表格

- 行高 32–36px；表头 `text-2xs uppercase tracking-wider text-tertiary`
- 右对齐所有数字列；左对齐符号/状态
- Hover：`--bg-hover`；无斑马纹（或极弱，二选一，v1 推荐无斑马）
- 粘性表头；横向滚动时左侧币种列可 sticky
- 操作列右固定，ghost 按钮

---

## 7. 分页面 UI 重设计

页面职责与数据源仍见 `frontend_design.md`；本节只定 **布局与控件表现**。

### 7.1 总览 `/`

**布局：**

```
┌─ Metrics strip（单行 5 项，底部分割，非大卡片）──────────────────────┐
│ 权益 | 可用 | 保证金 | 未实现PnL | 今日PnL                              │
├─ 主区 8 ─┬─ 侧区 4 ──────────────────────────────────────────────────┤
│ 权益曲线  │ 风控限额迷你条                                             │
│          │ 最近告警列表                                                │
├─ 全宽 ──────────────────────────────────────────────────────────────┤
│ 活跃持仓 DataTable（与持仓页列对齐，操作列仅「查看」）                    │
└──────────────────────────────────────────────────────────────────────┘
```

**按钮：** 无页级主按钮；告警项可点进风控/策略。

### 7.2 行情 `/market`

向 HL Trade 工具条靠拢，但保留「监控」定位（无下单面板）。

```
┌─ Symbol · Price · Mark · Funding · OI · [1m 5m 15m 1h 4h] · Live ─┐
├─ Chart（flex）──────────────────────────────┬─ OrderBook ──────────┤
│  Candlestick + 可选成交量                    │  聚合档位 / 累计深度   │
│                                              │  mid 高亮              │
├──────────────────────────────────────────────┴──────────────────────┤
│  Recent trades（可选折叠）                                            │
└─────────────────────────────────────────────────────────────────────┘
```

**交互：**

- 币种：下拉或可搜索 Select（币种增多时）
- 周期：SegmentedControl
- 订单簿：买卖分区背景浅染色（绿/红 6%），数量条宽度映射累计
- 断连：顶条 StaleBanner，图表冻结最后一根并标注

### 7.3 持仓 `/positions`

```
PageHeader: 活跃持仓 (n)                    [全部平仓 danger]
DataTable: 币种 | 方向 | 数量 | 入场 | 现价 | uPnL | 保证金 | 操作[平仓]
Footer 合计 uPnL
下方：持仓 PnL 曲线（可折叠）
```

**按钮：** 行内「平仓」=`sell` ghost；「全部平仓」=`danger` + L2 确认。

### 7.4 订单 `/orders`

```
Tabs: 活跃 | 历史
Filters: 状态 · 币种
DataTable + 行内 [撤单] ghost
```

状态用 `Badge` 色点，不用 emoji。

### 7.5 策略 `/strategy`

**列表页：** 每策略一行面板（非厚卡片）：

```
[Name / type]  [StatusChip]  [今日信号] [成交] [PnL]
参数摘要一行 mono
[启动 primary] [停止 secondary] [工作台 ghost→]
```

**做市工作台**（已有分区）：统一换成新 token；Tab 为 `Overview | Quotes | Inventory | PnL | Budget | Config | Events`；危险操作沿用 L2。

### 7.6 风控 `/risk`

```
Kill 状态面板（critical 边框当已触发）
限额表：进度条颜色 <60% profit / 60–80% warning / >80% loss
动作额度 + IP 权重独立强调（runway 文案）
检查统计一行 Metrics
```

顶栏已有 Kill 入口；本页保留完整控制与历史原因。

### 7.7 设置 `/settings`

```
Tabs: 连接(只读) | 风控 | 策略参数 | 告警
只读区用 description list；可编辑区 Input + [保存 primary] [重置 secondary]
mainnet 保存走 L2
```

---

## 8. 导航与信息架构微调

路由不变（兼容现有实现）：

| 路由 | 侧栏标签 | 图标建议（Lucide） |
|------|----------|-------------------|
| `/` | 总览 | `LayoutDashboard` |
| `/market` | 行情 | `CandlestickChart` |
| `/positions` | 持仓 | `Briefcase` |
| `/orders` | 订单 | `ListOrdered` |
| `/strategy` | 策略 | `Bot` |
| `/risk` | 风控 | `Shield` |
| `/settings` | 设置 | `Settings` |

做市工作台保持 `/strategy/[id]/market-making`，不进侧栏。

---

## 9. 文案与微交互

| 场景 | 文案原则 | 示例 |
|------|----------|------|
| 按钮 | 动词 + 对象，进行时态一致 | 「保存参数」→ Toast「参数已保存」 |
| 空态 | 说明现状 + 下一步 | 「暂无持仓。策略启动后将显示在这里。」 |
| 错误 | 不道歉；给原因与动作 | 「撤单失败：订单已成交。刷新订单列表。」 |
| Stale | 标明延迟与降级 | 「SSE 断开 8s · 显示缓存数据」 |
| Kill | 冷静、指令式 | 「Kill Switch 已触发。所有挂单将撤销。输入 CONFIRM 确认。」 |

禁止装饰性 emoji 作为状态主信号（可用极简色点）；Toast 可保留少量符号若有助于扫读，但不作为唯一语义。

---

## 10. 响应式

| 断点 | 行为 |
|------|------|
| ≥1280px | 顶栏 + 展开侧栏 + 完整密度 |
| 768–1279 | 侧栏折叠为图标；行情图/簿上下堆叠 |
| <768 | 底栏 Tab（总览/持仓/订单/策略/更多）；顶栏保留 Live + Env + Kill；图表简化 |

手机端不追求完整做市工作台；提示「请使用桌面」。

---

## 11. 无障碍与安全

- 对比度：正文与主数字满足 WCAG AA
- 焦点环：accent，`:focus-visible` only
- 危险对话框：焦点陷阱 + Esc 关闭（Kill 触发确认除外：Esc 取消而非提交）
- 主钱包私钥永不出现在前端；地址仅截断展示
- `mainnet` 下所有 mutation 按钮旁显示 EnvBadge

---

## 12. 与现有文档的关系

| 文档 | 关系 |
|------|------|
| `docs/frontend_design.md` | 页面用途、数据源、API、实时架构仍有效；本文覆盖其「视觉与组件」层 |
| `docs/market_making_design.md` | 做市工作台信息架构不变；换肤 + 统一控件 |
| `rules/frontend.md` | 实现时同步更新：token、组件约定、禁用 emoji 导航等 |

实现完成后，将 `frontend_design.md` §1 全局布局 ASCII 替换为本文 §5 结构，避免双源真相。

---

## 13. 实施计划（建议）

### Phase A — Foundation（1–2 天）

1. 写入 CSS tokens + 字体
2. 搭建 `Button` `Input` `Badge` `Dialog` `Tabs` `Table` 等 P0
3. 实现 `Topbar` / 新 `Sidebar` / `StatusBar` / `PageHeader`
4. 根布局接入，去掉 emoji 导航

### Phase B — Pages skin（2–3 天）

1. 总览、持仓、订单、风控、设置换新组件
2. 行情页工具条 + OrderBook 视觉升级
3. 策略列表换 StatusChip 与按钮语义

### Phase C — Market-making polish（1–2 天）

1. 做市工作台 token 对齐
2. Stale / Live / Confirm 流统一

### Phase D — QA

1. 视觉回归（桌面 + 窄屏）
2. 危险流手动走查（testnet）
3. 单元测试：ConfirmPhrase、EnvBadge、PnLText、按钮 disabled 原因

**验收标准（v1）：**

- [ ] 无 emoji 导航与状态主信号
- [ ] 全站按钮变体符合 §6.2，无「每页多个 primary」
- [ ] 顶栏常驻 Env + Live + Kill
- [ ] 数字列 tabular mono
- [ ] Kill / 全部平仓 / mainnet 激活走对应确认级别
- [ ] `prefers-reduced-motion` 下无脉冲动画

---

## 14. 开放问题（评审时决定）

1. **字体加载**：用 `next/font` 引入 IBM Plex，还是系统栈先上线？
2. **侧栏默认**：桌面默认展开还是折叠？
3. **行情是否保留「最近成交」常驻**，还是默认折叠以更接近 HL 主视图？
4. **是否在总览加入迷你权益 sparkline**（P2）？
5. **中英标签**：状态芯片用中文（运行中）还是英文枚举（RUNNING）+ Tooltip？

---

## 15. 附录：Token 速查（实现拷贝用）

```css
@theme {
  --color-bg-base: #07080A;
  --color-bg-elevated: #0C0E12;
  --color-bg-panel: #11141A;
  --color-bg-hover: #171B22;
  --color-bg-active: #1C222C;
  --color-border-subtle: #1A1F28;
  --color-border-default: #252B36;
  --color-border-strong: #343B49;
  --color-text-primary: #F2F4F7;
  --color-text-secondary: #9AA3B2;
  --color-text-tertiary: #6B7380;
  --color-accent: #50E3C2;
  --color-accent-muted: color-mix(in srgb, #50E3C2 14%, transparent);
  --color-profit: #2DD4A0;
  --color-loss: #F07178;
  --color-warning: #E6B84D;
  --color-critical: #E5484D;
  --color-info: #5B9FD4;
  --radius-sm: 4px;
  --radius-md: 6px;
  --radius-lg: 8px;
}
```

---

**下一步**：评审本文件（尤其 §14 开放问题）后，按 Phase A→D 落地实现；实现期间如需改 API，先回写 `frontend_design.md` / `design.md`。
