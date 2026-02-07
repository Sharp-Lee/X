/**
 * Hook for fetching analytics data with periodic refresh.
 */

import { useState, useEffect, useCallback } from 'react'
import { api } from '@/services/api'
import type { AnalyticsSummary } from '@/services/api'

export function useAnalytics(days: number = 30) {
  const [data, setData] = useState<AnalyticsSummary | null>(null)
  const [isLoading, setIsLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)

  const fetchAnalytics = useCallback(async () => {
    try {
      setIsLoading(true)
      const result = await api.getAnalyticsSummary({ days })
      setData(result)
      setError(null)
    } catch (e) {
      setError(e instanceof Error ? e.message : 'Failed to fetch analytics')
    } finally {
      setIsLoading(false)
    }
  }, [days])

  useEffect(() => {
    fetchAnalytics()
    const interval = setInterval(fetchAnalytics, 60000)
    return () => clearInterval(interval)
  }, [fetchAnalytics])

  return { data, isLoading, error, refresh: fetchAnalytics }
}
