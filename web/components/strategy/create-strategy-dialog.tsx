"use client"

import { useEffect, useMemo, useRef, useState } from "react"
import { useRouter } from "next/navigation"
import { Button } from "@/components/ui/button"
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog"
import { Input } from "@/components/ui/input"
import { MarketMakerConfigFields } from "@/components/market-making/market-maker-config-fields"
import { TrendFollowConfigFields } from "@/components/strategy/trend-follow-config-fields"
import { useAccount } from "@/hooks/use-account"
import { createStrategy } from "@/hooks/use-strategies"
import { useInstrumentMeta } from "@/hooks/use-system-status"
import { TRADE_SYMBOLS } from "@/lib/constants"
import { ApiError, createIdempotencyKey } from "@/lib/api"
import {
  cloneDefaultMmConfig,
  inventoryBandsFromEquity,
  suggestQuoteSize,
  validateMmConfig,
  validateStrategyIdentity,
} from "@/lib/market-maker-config"
import { cloneDefaultTfConfig, validateTfConfig } from "@/lib/trend-follow-config"
import type {
  MarketMakerConfig,
  StrategyCreateRequest,
  StrategyInstance,
  TrendFollowConfig,
} from "@/lib/types"

export type CreatableStrategyType = "market_maker" | "trend_follow"

export interface CreateStrategyDialogProps {
  open: boolean
  onOpenChange: (open: boolean) => void
  existing: StrategyInstance[]
  onCreated?: (strategy: StrategyInstance) => void
}

type Step = 1 | 2

const TYPE_LABELS: Record<CreatableStrategyType, string> = {
  market_maker: "做市策略",
  trend_follow: "趋势跟随",
}

export function CreateStrategyDialog({
  open,
  onOpenChange,
  existing,
  onCreated,
}: CreateStrategyDialogProps) {
  const router = useRouter()
  const { account } = useAccount()
  const [step, setStep] = useState<Step>(1)
  const [strategyType, setStrategyType] = useState<CreatableStrategyType>("market_maker")
  const [strategyId, setStrategyId] = useState("")
  const [subAccount, setSubAccount] = useState("")
  const [symbol, setSymbol] = useState<string>(TRADE_SYMBOLS[0])
  const [metadataNote, setMetadataNote] = useState("")
  const [mmConfig, setMmConfig] = useState<MarketMakerConfig>(() => cloneDefaultMmConfig())
  const [tfConfig, setTfConfig] = useState<TrendFollowConfig>(() => cloneDefaultTfConfig())
  const [showAdvanced, setShowAdvanced] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const [submitting, setSubmitting] = useState(false)
  const idempotencyKeyRef = useRef(createIdempotencyKey())
  const needsInstrumentMeta = strategyType === "market_maker"
  const { meta, error: metaError } = useInstrumentMeta(open && needsInstrumentMeta ? symbol : undefined)

  useEffect(() => {
    if (!open) return
    setStep(1)
    setStrategyType("market_maker")
    setStrategyId("")
    setSubAccount("")
    setSymbol(TRADE_SYMBOLS[0])
    setMetadataNote("")
    setShowAdvanced(false)
    setError(null)
    setSubmitting(false)
    idempotencyKeyRef.current = createIdempotencyKey()
    const next = cloneDefaultMmConfig()
    if (account?.equity) {
      Object.assign(next, inventoryBandsFromEquity(account.equity))
    }
    setMmConfig(next)
    setTfConfig(cloneDefaultTfConfig())
  }, [open, account?.equity])

  useEffect(() => {
    if (!open || !meta || strategyType !== "market_maker") return
    setMmConfig((previous) => ({ ...previous, quote_size: suggestQuoteSize(meta) }))
  }, [meta, open, symbol, strategyType])

  const conflictHint = useMemo(() => {
    const id = strategyId.trim()
    const sub = subAccount.trim()
    const sym = symbol.trim().toUpperCase()
    if (id && existing.some((item) => item.strategy_id === id)) {
      return `strategy_id「${id}」已存在`
    }
    if (
      sub &&
      sym &&
      existing.some(
        (item) =>
          (item.strategy_type === "market_maker" || item.strategy_type === "trend_follow") &&
          item.sub_account?.toLowerCase() === sub.toLowerCase() &&
          item.symbol.toUpperCase() === sym,
      )
    ) {
      return `子账户「${sub}」已占用 ${sym}`
    }
    return null
  }, [existing, strategyId, subAccount, symbol])

  function goNext() {
    const identityError = validateStrategyIdentity({
      strategy_id: strategyId.trim(),
      sub_account: subAccount.trim(),
      symbol: symbol.trim().toUpperCase(),
    })
    if (identityError) {
      setError(identityError)
      return
    }
    if (conflictHint) {
      setError(conflictHint)
      return
    }
    if (needsInstrumentMeta && metaError) {
      setError(`无法加载 ${symbol} 合约元数据，请换品种或稍后重试`)
      return
    }
    setError(null)
    setStep(2)
  }

  async function handleSubmit() {
    if (strategyType === "market_maker") {
      const configError = validateMmConfig(mmConfig)
      if (configError) {
        setError(configError)
        return
      }
    } else {
      const configError = validateTfConfig(tfConfig)
      if (configError) {
        setError(configError)
        return
      }
    }

    setSubmitting(true)
    setError(null)
    try {
      const metadata: Record<string, string> = {}
      if (metadataNote.trim()) metadata.note = metadataNote.trim()
      const body: StrategyCreateRequest =
        strategyType === "market_maker"
          ? {
              strategy_id: strategyId.trim(),
              strategy_type: "market_maker",
              sub_account: subAccount.trim(),
              symbol: symbol.trim().toUpperCase(),
              initial_config: mmConfig,
              metadata,
            }
          : {
              strategy_id: strategyId.trim(),
              strategy_type: "trend_follow",
              sub_account: subAccount.trim(),
              symbol: symbol.trim().toUpperCase(),
              initial_config: tfConfig,
              metadata,
            }
      const created = await createStrategy(body, { idempotencyKey: idempotencyKeyRef.current })
      onCreated?.(created)
      onOpenChange(false)
      if (created.strategy_type === "market_maker") {
        router.push(`/strategy/${encodeURIComponent(created.strategy_id)}/market-making?created=1`)
      }
    } catch (err) {
      const message =
        err instanceof ApiError
          ? err.code === "STRATEGY_CREATE_CONFLICT"
            ? "创建冲突：strategy_id 或 sub_account+symbol 已占用"
            : err.message
          : err instanceof Error
            ? err.message
            : "创建失败"
      setError(message)
    } finally {
      setSubmitting(false)
    }
  }

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="max-w-3xl max-h-[90vh] overflow-y-auto">
        <DialogHeader>
          <DialogTitle>新建策略</DialogTitle>
          <DialogDescription>
            {step === 1
              ? "第一步：选择类型并定义实例身份。创建后为 stopped。"
              : strategyType === "market_maker"
                ? "第二步：设置做市初始配置。高级参数可折叠。"
                : "第二步：设置趋势跟随参数（EMA / ATR / 仓位）。"}
          </DialogDescription>
          <p className="text-2xs text-text-tertiary">步骤 {step} / 2</p>
        </DialogHeader>

        {error ? (
          <p role="alert" className="rounded-md border border-critical/30 bg-critical/10 px-3 py-2 text-sm text-critical">
            {error}
          </p>
        ) : null}

        {step === 1 ? (
          <div className="grid gap-3 sm:grid-cols-2">
            <label className="text-xs text-text-secondary sm:col-span-2" htmlFor="create-strategy-type">
              <span className="font-medium text-text-primary">策略类型</span>
              <p className="mt-0.5 text-2xs leading-snug text-text-tertiary">
                做市适合库存型双边报价；趋势跟随适合中频方向信号。
              </p>
              <select
                id="create-strategy-type"
                className="mt-1.5 flex h-9 w-full rounded-md border border-border bg-bg-primary px-3 text-sm"
                value={strategyType}
                onChange={(event) => setStrategyType(event.target.value as CreatableStrategyType)}
              >
                {(Object.keys(TYPE_LABELS) as CreatableStrategyType[]).map((type) => (
                  <option key={type} value={type}>
                    {TYPE_LABELS[type]}
                  </option>
                ))}
              </select>
            </label>
            <label className="text-xs text-text-secondary sm:col-span-2" htmlFor="create-strategy-id">
              <span className="font-medium text-text-primary">策略 ID</span>
              <p className="mt-0.5 text-2xs leading-snug text-text-tertiary">
                实例唯一标识，创建后不可改；建议用 mm-btc-1 / trend-btc-1 这类可读命名。
              </p>
              <Input
                id="create-strategy-id"
                className="mt-1.5 font-mono"
                value={strategyId}
                onChange={(event) => setStrategyId(event.target.value)}
                placeholder={strategyType === "market_maker" ? "mm-btc-1" : "trend-btc-1"}
              />
            </label>
            <label className="text-xs text-text-secondary" htmlFor="create-sub-account">
              <span className="font-medium text-text-primary">子账户</span>
              <p className="mt-0.5 text-2xs leading-snug text-text-tertiary">
                Hyperliquid 子账户名；同一子账户 + 品种同一时间只能跑一个活跃实例。
              </p>
              <Input
                id="create-sub-account"
                className="mt-1.5 font-mono"
                value={subAccount}
                onChange={(event) => setSubAccount(event.target.value)}
                placeholder={strategyType === "market_maker" ? "mm_btc" : "trend_btc"}
              />
            </label>
            <label className="text-xs text-text-secondary" htmlFor="create-symbol">
              <span className="font-medium text-text-primary">交易品种</span>
              <p className="mt-0.5 text-2xs leading-snug text-text-tertiary">永续合约标的，例如 BTC / ETH / SOL。</p>
              <select
                id="create-symbol"
                className="mt-1.5 flex h-9 w-full rounded-md border border-border bg-bg-primary px-3 font-mono text-sm"
                value={symbol}
                onChange={(event) => setSymbol(event.target.value)}
              >
                {TRADE_SYMBOLS.map((item) => (
                  <option key={item} value={item}>
                    {item}
                  </option>
                ))}
              </select>
            </label>
            <label className="text-xs text-text-secondary sm:col-span-2" htmlFor="create-note">
              <span className="font-medium text-text-primary">备注（可选）</span>
              <p className="mt-0.5 text-2xs leading-snug text-text-tertiary">仅用于列表展示，不影响交易逻辑。</p>
              <Input
                id="create-note"
                className="mt-1.5"
                value={metadataNote}
                onChange={(event) => setMetadataNote(event.target.value)}
              />
            </label>
            {conflictHint ? <p className="text-xs text-warning sm:col-span-2">{conflictHint}</p> : null}
          </div>
        ) : strategyType === "market_maker" ? (
          <div className="space-y-3">
            <MarketMakerConfigFields
              value={mmConfig}
              onChange={setMmConfig}
              mode="create"
              showAdvanced={showAdvanced}
            />
            <Button type="button" variant="ghost" size="sm" onClick={() => setShowAdvanced((value) => !value)}>
              {showAdvanced ? "隐藏高级参数" : "显示高级参数"}
            </Button>
          </div>
        ) : (
          <TrendFollowConfigFields value={tfConfig} onChange={setTfConfig} />
        )}

        <DialogFooter className="gap-2 sm:gap-0">
          {step === 2 ? (
            <Button type="button" variant="secondary" onClick={() => setStep(1)} disabled={submitting}>
              上一步
            </Button>
          ) : null}
          {step === 1 ? (
            <Button type="button" variant="primary" onClick={goNext}>
              下一步
            </Button>
          ) : (
            <Button type="button" variant="primary" onClick={() => void handleSubmit()} disabled={submitting}>
              {submitting ? "创建中…" : "创建"}
            </Button>
          )}
        </DialogFooter>
      </DialogContent>
    </Dialog>
  )
}
