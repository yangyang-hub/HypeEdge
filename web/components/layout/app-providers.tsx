"use client"

import { SWRConfig } from "swr"
import { SSEProvider } from "@/hooks/use-sse"

export function AppProviders({ children }: { children: React.ReactNode }) {
  return (
    <SWRConfig
      value={{
        shouldRetryOnError: true,
        errorRetryCount: 3,
        revalidateOnFocus: true,
        keepPreviousData: true,
      }}
    >
      <SSEProvider>{children}</SSEProvider>
    </SWRConfig>
  )
}
