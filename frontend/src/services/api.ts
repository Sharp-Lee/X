/**
 * API service for communicating with the backend.
 */

const API_BASE = import.meta.env.VITE_API_URL || 'http://localhost:8000/api';

export interface Signal {
  id: string;
  symbol: string;
  timeframe: string;
  signal_time: string;
  direction: 'LONG' | 'SHORT';
  entry_price: number;
  tp_price: number;
  sl_price: number;
  streak_at_signal: number;
  mae_ratio: number;
  mfe_ratio: number;
  outcome: 'active' | 'tp' | 'sl';
  outcome_time?: string;
  outcome_price?: number;
}

export interface SystemStatus {
  status: string;
  version: string;
  symbols: string[];
  timeframe: string;
  active_signals: number;
}

export interface Stats {
  total_signals: number;
  wins: number;
  losses: number;
  active: number;
  win_rate: number;
  breakeven_win_rate: number;
}

export interface OrderRequest {
  symbol: string;
  side: 'buy' | 'sell';
  quantity: number;
  price?: number;
}

export interface OrderResponse {
  success: boolean;
  order_id?: string;
  message: string;
}

async function fetchJson<T>(url: string, options?: RequestInit): Promise<T> {
  const response = await fetch(url, {
    ...options,
    headers: {
      'Content-Type': 'application/json',
      ...options?.headers,
    },
  });

  if (!response.ok) {
    throw new Error(`API error: ${response.status} ${response.statusText}`);
  }

  return response.json();
}

// --- Analytics Types ---

export interface WinRateBySymbol {
  symbol: string
  wins: number
  losses: number
  total: number
  win_rate: number
}

export interface WinRateByTimeframe {
  timeframe: string
  wins: number
  losses: number
  total: number
  win_rate: number
}

export interface WinRateByDirection {
  direction: 'LONG' | 'SHORT'
  wins: number
  losses: number
  total: number
  win_rate: number
}

export interface ExpectancyStats {
  total: number
  wins: number
  losses: number
  win_rate: number
  expectancy_r: number
  total_r: number
  profit_factor: number
}

export interface DailyPerformance {
  date: string
  wins: number
  losses: number
  total: number
  daily_r: number
  cumulative_r: number
}

export interface MaeMfeStats {
  outcome: string
  count: number
  avg_mae: number
  avg_mfe: number
  mae_p25: number
  mae_p50: number
  mae_p75: number
  mae_p90: number
  mfe_p25: number
  mfe_p50: number
  mfe_p75: number
  mfe_p90: number
}

export interface AnalyticsSummary {
  by_symbol: WinRateBySymbol[]
  by_timeframe: WinRateByTimeframe[]
  by_direction: WinRateByDirection[]
  expectancy: ExpectancyStats
  daily: DailyPerformance[]
  mae_mfe: Record<string, MaeMfeStats>
}

export const api = {
  async getStatus(): Promise<SystemStatus> {
    return fetchJson(`${API_BASE}/status`);
  },

  async getSignals(params?: {
    symbol?: string;
    limit?: number;
    outcome?: string;
  }): Promise<Signal[]> {
    const searchParams = new URLSearchParams();
    if (params?.symbol) searchParams.set('symbol', params.symbol);
    if (params?.limit) searchParams.set('limit', params.limit.toString());
    if (params?.outcome) searchParams.set('outcome', params.outcome);

    const query = searchParams.toString();
    return fetchJson(`${API_BASE}/signals${query ? `?${query}` : ''}`);
  },

  async getActiveSignals(symbol?: string): Promise<Signal[]> {
    const query = symbol ? `?symbol=${symbol}` : '';
    return fetchJson(`${API_BASE}/signals/active${query}`);
  },

  async getSignal(id: string): Promise<Signal> {
    return fetchJson(`${API_BASE}/signals/${id}`);
  },

  async getStats(params?: { symbol?: string; days?: number }): Promise<Stats> {
    const searchParams = new URLSearchParams();
    if (params?.symbol) searchParams.set('symbol', params.symbol);
    if (params?.days) searchParams.set('days', params.days.toString());

    const query = searchParams.toString();
    return fetchJson(`${API_BASE}/stats${query ? `?${query}` : ''}`);
  },

  async getAnalyticsSummary(params?: { days?: number }): Promise<AnalyticsSummary> {
    const searchParams = new URLSearchParams();
    if (params?.days) searchParams.set('days', params.days.toString());
    const query = searchParams.toString();
    return fetchJson(`${API_BASE}/analytics/summary${query ? `?${query}` : ''}`);
  },

  async placeOrder(order: OrderRequest): Promise<OrderResponse> {
    return fetchJson(`${API_BASE}/order`, {
      method: 'POST',
      body: JSON.stringify(order),
    });
  },
};
