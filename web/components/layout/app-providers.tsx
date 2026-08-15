"use client"

import { SWRConfig } from "swr"
import { SSEProvider } from "@/hooks/use-sse"
import { ApiError } from "@/lib/api"

export function AppProviders({ children }: { children: React.ReactNode }) {
  return (
    <SWRConfig
      value={{
        // L-FE6: 404 是确定性错误（路由/资源不存在），重试只会徒增无效请求。
        shouldRetryOnError: (error: unknown) => !(error instanceof ApiError && error.status === 404),
        errorRetryCount: 3,
        revalidateOnFocus: true,
        keepPreviousData: true,
      }}
    >
      <SSEProvider>{children}</SSEProvider>
    </SWRConfig>
  )
}
