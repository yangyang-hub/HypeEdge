import { cleanup, render, screen } from "@testing-library/react"
import { afterEach, describe, expect, it, vi } from "vitest"
import { FundingArbConfigFields } from "@/components/strategy/funding-arb-config-fields"
import { cloneDefaultFaConfig } from "@/lib/funding-arb-config"

afterEach(cleanup)

describe("FundingArbConfigFields", () => {
  it("explains automatic discovery and exposes no manual pair field", () => {
    render(<FundingArbConfigFields value={cloneDefaultFaConfig()} onChange={vi.fn()} />)

    expect(screen.getByText(/自动扫描 USDC 现货与同名永续/)).toBeInTheDocument()
    expect(screen.queryByText("现货标的")).not.toBeInTheDocument()
    expect(screen.getByLabelText(/入场资金费阈值/)).toBeInTheDocument()
  })
})
