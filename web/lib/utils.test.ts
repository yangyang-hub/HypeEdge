import { describe, expect, it } from "vitest"
import {
  addDecimals,
  asDecimalString,
  formatDateTime,
  formatPct,
  formatPctUsed,
  formatPrice,
  formatSize,
  pctUsedFraction,
} from "@/lib/utils"

describe("formatting helpers", () => {
  it("uses instrument-provided precision", () => {
    expect(formatPrice(1234.5, 2)).toBe("1,234.50")
    expect(formatSize(0.12345678, 4)).toBe("0.1235")
  })

  it("formats percentages consistently", () => {
    expect(formatPct(0.1234)).toBe("12.34%")
  })

  it("formats timestamps in Asia/Shanghai as YYYY-MM-DD HH:mm:ss", () => {
    // 12:34:56Z → 20:34:56 CST (UTC+8)
    expect(formatDateTime("2026-07-11T12:34:56Z")).toBe("2026-07-11 20:34:56")
    expect(formatDateTime(null)).toBe("—")
  })

  it("preserves decimal strings beyond JavaScript safe integer precision", () => {
    expect(formatPrice("9007199254740993.125", 3)).toBe("9,007,199,254,740,993.125")
    expect(addDecimals(["0.1", "0.2"])).toBe("0.3")
    expect(asDecimalString("1.2300")).toBe("1.23")
  })
})

describe("pct_used 展示（P4-4：后端返回 0–100，前端不再 ×100）", () => {
  it("formatPctUsed 直接按百分数值显示，不做双重放大", () => {
    // 旧实现 formatPct(33.33) → "3333.00%"；正确应为 "33.33%"。
    expect(formatPctUsed("33.33")).toBe("33.33%")
    expect(formatPctUsed("0")).toBe("0.00%")
    expect(formatPctUsed("100")).toBe("100.00%")
    expect(formatPctUsed("12.5", 0)).toBe("13%")
  })

  it("pctUsedFraction 把 0–100 转为 0–1 分数（进度条用），并饱和超限值", () => {
    expect(pctUsedFraction("33.33")).toBeCloseTo(0.3333, 4)
    expect(pctUsedFraction("0")).toBe(0)
    expect(pctUsedFraction("100")).toBe(1)
    expect(pctUsedFraction("250")).toBe(1) // 超限饱和，条不为满之外不会溢出
    expect(pctUsedFraction("33.33")).toBeLessThan(1)
  })
})
