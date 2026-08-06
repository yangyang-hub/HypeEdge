"use client"

import { Input } from "@/components/ui/input"
import {
  FA_DECIMAL_FIELDS,
  FA_INTEGER_FIELDS,
  type FundingArbConfigFieldKey,
  type FundingArbFieldMeta,
} from "@/lib/funding-arb-config"
import type { DecimalString, FundingArbConfig } from "@/lib/types"
import { cn } from "@/lib/utils"

export interface FundingArbConfigFieldsProps {
  value: FundingArbConfig
  onChange: (next: FundingArbConfig) => void
  className?: string
}

function ConfigField({
  field,
  value,
  onChange,
  type = "text",
}: {
  field: FundingArbFieldMeta
  value: string | number
  onChange: (raw: string) => void
  type?: "text" | "number"
}) {
  return (
    <label className="block text-xs text-text-secondary" htmlFor={`fa-${field.key}`}>
      <span className="font-medium text-text-primary">{field.label}</span>
      <p className="mt-0.5 text-2xs leading-snug text-text-tertiary">{field.description}</p>
      <Input
        id={`fa-${field.key}`}
        type={type}
        className="mt-1.5 font-mono"
        value={value}
        onChange={(event) => onChange(event.target.value)}
      />
    </label>
  )
}

export function FundingArbConfigFields({ value, onChange, className }: FundingArbConfigFieldsProps) {
  function setDecimal(key: FundingArbConfigFieldKey, raw: string) {
    onChange({ ...value, [key]: raw as DecimalString })
  }

  function setInteger(key: FundingArbConfigFieldKey, raw: string) {
    onChange({ ...value, [key]: Number.parseInt(raw, 10) || 0 })
  }

  return (
    <div className={cn("space-y-4", className)}>
      <div className="rounded-md border border-border bg-bg-secondary px-3 py-2 text-xs text-text-secondary">
        系统会自动扫描 USDC 现货与同名永续的共有市场，并按成交量、双边深度、点差、基差和净 edge
        过滤。单个 cycle 建立后会锁定实际市场，完全平仓后才会重新选择。
      </div>
      <div className="grid gap-4 sm:grid-cols-2">
        {FA_INTEGER_FIELDS.map((field) => (
          <ConfigField
            key={field.key}
            field={field}
            type="number"
            value={value[field.key]}
            onChange={(raw) => setInteger(field.key, raw)}
          />
        ))}
        {FA_DECIMAL_FIELDS.map((field) => (
          <ConfigField
            key={field.key}
            field={field}
            value={value[field.key]}
            onChange={(raw) => setDecimal(field.key, raw)}
          />
        ))}
      </div>
    </div>
  )
}
