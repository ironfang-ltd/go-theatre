import { useQuery } from '@tanstack/react-query'
import { useRef } from 'react'
import { fetchAllClusterStatus } from '../lib/api'
import type { ClusterStatus } from '../lib/api'

export interface ClusterStatusWithRates extends ClusterStatus {
  rates: Record<string, number>
}

export function useAllClusterStatus() {
  const prevRef = useRef<{ metrics: Record<string, number>; updatedAt: number } | null>(null)
  const ratesRef = useRef<Record<string, number>>({})

  const query = useQuery({
    queryKey: ['all-cluster-status'],
    queryFn: fetchAllClusterStatus,
    refetchInterval: 2000,
  })

  const { data, dataUpdatedAt } = query

  // Only recompute rates when dataUpdatedAt changes (= new server response).
  // Using select() was broken because it runs on every re-render, resetting
  // prevRef with the same metrics but a new timestamp → rates always 0.
  if (data && prevRef.current?.updatedAt !== dataUpdatedAt) {
    if (prevRef.current) {
      const dt = (dataUpdatedAt - prevRef.current.updatedAt) / 1000
      if (dt > 0) {
        const rates: Record<string, number> = {}
        for (const key of Object.keys(data.metrics)) {
          const prev = prevRef.current.metrics[key] ?? 0
          const curr = data.metrics[key] ?? 0
          rates[key] = Math.max(0, Math.round((curr - prev) / dt))
        }
        ratesRef.current = rates
      }
    }
    prevRef.current = { metrics: { ...data.metrics }, updatedAt: dataUpdatedAt }
  }

  return {
    ...query,
    data: data ? ({ ...data, rates: ratesRef.current } as ClusterStatusWithRates) : undefined,
  }
}
