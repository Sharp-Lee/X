import { useEffect, useState, useCallback } from 'react'
import { api, type TradingOverview } from '@/services/api'

export function useTradingAccounts() {
  const [data, setData] = useState<TradingOverview | null>(null)
  const [isLoading, setIsLoading] = useState(true)

  const fetch = useCallback(async () => {
    try {
      const overview = await api.getTradingOverview()
      setData(overview)
    } catch (error) {
      console.error('Failed to fetch trading overview:', error)
    } finally {
      setIsLoading(false)
    }
  }, [])

  useEffect(() => {
    fetch()
    const interval = setInterval(fetch, 15000)
    return () => clearInterval(interval)
  }, [fetch])

  return { data, isLoading, refresh: fetch }
}
