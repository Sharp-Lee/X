const BINANCE_API = 'https://api.binance.com/api/v3'

export interface Kline {
  time: number
  open: number
  high: number
  low: number
  close: number
  volume: number
}

export async function fetchKlines(
  symbol: string,
  interval: string,
  limit: number = 200
): Promise<Kline[]> {
  const response = await fetch(
    `${BINANCE_API}/klines?symbol=${symbol}&interval=${interval}&limit=${limit}`
  )

  if (!response.ok) {
    throw new Error(`Failed to fetch klines: ${response.statusText}`)
  }

  const data = await response.json()

  return data.map((k: (string | number)[]) => ({
    time: (k[0] as number) / 1000, // Convert to seconds for lightweight-charts
    open: parseFloat(k[1] as string),
    high: parseFloat(k[2] as string),
    low: parseFloat(k[3] as string),
    close: parseFloat(k[4] as string),
    volume: parseFloat(k[5] as string),
  }))
}
