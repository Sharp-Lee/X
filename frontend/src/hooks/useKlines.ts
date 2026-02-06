import { useState, useEffect, useCallback } from 'react'
import { fetchKlines, type Kline } from '@/services/kline-api'

interface UseKlinesOptions {
  symbol: string
  interval: string
  limit?: number
  refreshInterval?: number
}

export function useKlines({
  symbol,
  interval,
  limit = 200,
  refreshInterval = 60000,
}: UseKlinesOptions) {
  const [klines, setKlines] = useState<Kline[]>([])
  const [isLoading, setIsLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)

  const loadKlines = useCallback(async () => {
    try {
      const data = await fetchKlines(symbol, interval, limit)
      setKlines(data)
      setError(null)
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to fetch klines')
    } finally {
      setIsLoading(false)
    }
  }, [symbol, interval, limit])

  useEffect(() => {
    setIsLoading(true)
    loadKlines()

    const timer = setInterval(loadKlines, refreshInterval)
    return () => clearInterval(timer)
  }, [loadKlines, refreshInterval])

  return { klines, isLoading, error, refresh: loadKlines }
}
