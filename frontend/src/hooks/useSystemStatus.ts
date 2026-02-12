import { useEffect, useState, useCallback } from 'react'
import { api, type SystemStatus } from '@/services/api'

const PHASE_LABELS: Record<string, string> = {
  idle: 'Idle',
  init: 'Initializing WebSocket...',
  check_state: 'Checking processing state...',
  backfill: 'Backfilling K-line gaps...',
  restore: 'Restoring buffers...',
  replay: 'Replaying missed K-lines...',
  live: '',
}

export function useSystemStatus() {
  const [status, setStatus] = useState<SystemStatus | null>(null)

  const fetch = useCallback(async () => {
    try {
      const s = await api.getStatus()
      setStatus(s)
    } catch {
      // Backend not ready yet
    }
  }, [])

  useEffect(() => {
    fetch()
    const interval = setInterval(fetch, 2000)
    return () => clearInterval(interval)
  }, [fetch])

  const isStarting = status !== null && status.startup_phase !== 'live'
  const phaseLabel = status ? (PHASE_LABELS[status.startup_phase] || status.startup_phase) : ''

  return { status, isStarting, phaseLabel }
}
