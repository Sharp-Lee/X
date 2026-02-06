/**
 * Hook for managing signals state with real-time updates.
 */

import { useState, useEffect, useCallback } from 'react'
import { api } from '@/services/api'
import type { Signal } from '@/services/api'
import { useWebSocket } from '@/hooks/useWebSocket'

export function useSignals(symbol?: string) {
  const [signals, setSignals] = useState<Signal[]>([]);
  const [activeSignals, setActiveSignals] = useState<Signal[]>([]);
  const [isLoading, setIsLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  // Fetch signals from API
  const fetchSignals = useCallback(async () => {
    try {
      setIsLoading(true);
      const [all, active] = await Promise.all([
        api.getSignals({ symbol, limit: 50 }),
        api.getActiveSignals(symbol),
      ]);
      setSignals(all);
      setActiveSignals(active);
      setError(null);
    } catch (e) {
      setError(e instanceof Error ? e.message : 'Failed to fetch signals');
    } finally {
      setIsLoading(false);
    }
  }, [symbol]);

  // Handle new signal from WebSocket
  const handleNewSignal = useCallback((data: Record<string, unknown>) => {
    const signal = data as unknown as Signal;
    // Prevent duplicates - check if signal already exists
    setSignals((prev) => {
      if (prev.some((s) => s.id === signal.id)) {
        return prev;
      }
      return [signal, ...prev.slice(0, 49)];
    });
    setActiveSignals((prev) => {
      if (prev.some((s) => s.id === signal.id)) {
        return prev;
      }
      return [signal, ...prev];
    });
  }, []);

  // Handle MAE update from WebSocket
  const handleMaeUpdate = useCallback(
    (data: { signal_id: string; mae_ratio: number; mfe_ratio: number }) => {
      setActiveSignals((prev) =>
        prev.map((s) =>
          s.id === data.signal_id
            ? { ...s, mae_ratio: data.mae_ratio, mfe_ratio: data.mfe_ratio }
            : s
        )
      );
      setSignals((prev) =>
        prev.map((s) =>
          s.id === data.signal_id
            ? { ...s, mae_ratio: data.mae_ratio, mfe_ratio: data.mfe_ratio }
            : s
        )
      );
    },
    []
  );

  // Handle outcome from WebSocket
  const handleOutcome = useCallback(
    (data: { signal_id: string; outcome: string; exit_price: number }) => {
      // Remove from active signals
      setActiveSignals((prev) => prev.filter((s) => s.id !== data.signal_id));

      // Update in all signals
      setSignals((prev) =>
        prev.map((s) =>
          s.id === data.signal_id
            ? {
                ...s,
                outcome: data.outcome as Signal['outcome'],
                outcome_price: data.exit_price,
              }
            : s
        )
      );
    },
    []
  );

  // Connect to WebSocket
  const { isConnected } = useWebSocket({
    onSignal: handleNewSignal,
    onMaeUpdate: handleMaeUpdate,
    onOutcome: handleOutcome,
  });

  // Initial fetch
  useEffect(() => {
    fetchSignals();
  }, [fetchSignals]);

  return {
    signals,
    activeSignals,
    isLoading,
    error,
    isConnected,
    refresh: fetchSignals,
  };
}
