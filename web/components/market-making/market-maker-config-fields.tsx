"use client"

import { Input } from "@/components/ui/input"
import {
  MM_CREATE_CORE_DECIMAL_KEYS,
  MM_CREATE_CORE_INTEGER_KEYS,
  MM_DECIMAL_FIELDS,
  MM_INTEGER_FIELDS,
  type MarketMakerConfigFieldKey,
  type MarketMakerFieldMeta,
} from "@/lib/market-maker-config"
import type { DecimalString, MarketMakerConfig } from "@/lib/types"
import { cn } from "@/lib/utils"

export interface MarketMakerConfigFieldsProps {
  value: MarketMakerConfig
  onChange: (next: MarketMakerConfig) => void
  /** When true, only core create fields are shown unless `showAdvanced` is also true. */
  mode?: "full" | "create"
  showAdvanced?: boolean
  className?: string
}

function ConfigField({
  field,
  value,
  inputMode,
  onChange,
}: {
  field: MarketMakerFieldMeta
  value: string
  inputMode: "decimal" | "numeric"
  onChange: (raw: string) => void
}) {
  return (
    <label className="block text-xs text-text-secondary" htmlFor={`mm-cfg-${field.key}`}>
      <span className="font-medium text-text-primary">{field.label}</span>
      <p className="mt-0.5 text-2xs leading-snug text-text-tertiary">{field.description}</p>
      <Input
        id={`mm-cfg-${field.key}`}
        value={value}
        onChange={(event) => onChange(event.target.value)}
        inputMode={inputMode}
        className="mt-1.5 font-mono"
      />
    </label>
  )
}

export function MarketMakerConfigFields({
  value,
  onChange,
  mode = "full",
  showAdvanced = false,
  className,
}: MarketMakerConfigFieldsProps) {
  function setDecimal(key: MarketMakerConfigFieldKey, raw: string) {
    onChange({ ...value, [key]: raw as DecimalString })
  }

  function setInteger(key: MarketMakerConfigFieldKey, raw: string) {
    onChange({ ...value, [key]: Number.parseInt(raw, 10) || 0 })
  }

  const showAll = mode === "full" || showAdvanced
  const decimalFields = showAll
    ? MM_DECIMAL_FIELDS
    : MM_DECIMAL_FIELDS.filter((field) => MM_CREATE_CORE_DECIMAL_KEYS.includes(field.key))
  const integerFields = showAll
    ? MM_INTEGER_FIELDS
    : MM_INTEGER_FIELDS.filter((field) => MM_CREATE_CORE_INTEGER_KEYS.includes(field.key))

  return (
    <div className={cn("grid gap-4 md:grid-cols-2 xl:grid-cols-3", className)}>
      {decimalFields.map((field) => (
        <ConfigField
          key={field.key}
          field={field}
          value={String(value[field.key])}
          inputMode="decimal"
          onChange={(raw) => setDecimal(field.key, raw)}
        />
      ))}
      {integerFields.map((field) => (
        <ConfigField
          key={field.key}
          field={field}
          value={String(value[field.key])}
          inputMode="numeric"
          onChange={(raw) => setInteger(field.key, raw)}
        />
      ))}
    </div>
  )
}
