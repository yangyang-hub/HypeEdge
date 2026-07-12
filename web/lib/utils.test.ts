import { describe, expect, it } from "vitest"
import { addDecimals, asDecimalString, formatDateTime, formatPct, formatPrice, formatSize } from "@/lib/utils"

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
