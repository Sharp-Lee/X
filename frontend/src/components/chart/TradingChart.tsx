import { useEffect, useRef } from 'react'
import {
  createChart,
  ColorType,
  CandlestickSeries,
  type IChartApi,
  type ISeriesApi,
  type CandlestickData,
  type UTCTimestamp,
} from 'lightweight-charts'
import { useTheme } from 'next-themes'
import { useKlines } from '@/hooks/useKlines'
import type { Signal } from '@/services/api'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { Skeleton } from '@/components/ui/skeleton'
import { cn } from '@/lib/utils'

interface TradingChartProps {
  symbol: string
  timeframe: string
  selectedSignal?: Signal | null
  className?: string
}

export function TradingChart({
  symbol,
  timeframe,
  selectedSignal,
  className,
}: TradingChartProps) {
  const containerRef = useRef<HTMLDivElement>(null)
  const chartRef = useRef<IChartApi | null>(null)
  const candleSeriesRef = useRef<ISeriesApi<'Candlestick'> | null>(null)
  const { theme } = useTheme()
  const { klines, isLoading } = useKlines({ symbol, interval: timeframe })

  // Create chart
  useEffect(() => {
    if (!containerRef.current) return

    const isDark = theme === 'dark'

    const chart = createChart(containerRef.current, {
      layout: {
        background: { type: ColorType.Solid, color: 'transparent' },
        textColor: isDark ? '#94a3b8' : '#64748b',
      },
      grid: {
        vertLines: { color: isDark ? '#334155' : '#e2e8f0' },
        horzLines: { color: isDark ? '#334155' : '#e2e8f0' },
      },
      timeScale: {
        timeVisible: true,
        secondsVisible: false,
        borderColor: isDark ? '#334155' : '#e2e8f0',
      },
      rightPriceScale: {
        borderColor: isDark ? '#334155' : '#e2e8f0',
      },
      crosshair: {
        mode: 1,
      },
    })

    const candleSeries = chart.addSeries(CandlestickSeries, {
      upColor: '#22c55e',
      downColor: '#ef4444',
      borderVisible: false,
      wickUpColor: '#22c55e',
      wickDownColor: '#ef4444',
    })

    chartRef.current = chart
    candleSeriesRef.current = candleSeries

    // Handle resize
    const handleResize = () => {
      if (containerRef.current) {
        chart.applyOptions({
          width: containerRef.current.clientWidth,
          height: containerRef.current.clientHeight,
        })
      }
    }

    window.addEventListener('resize', handleResize)
    handleResize()

    return () => {
      window.removeEventListener('resize', handleResize)
      chart.remove()
      chartRef.current = null
      candleSeriesRef.current = null
    }
  }, [theme])

  // Update theme
  useEffect(() => {
    if (!chartRef.current) return

    const isDark = theme === 'dark'
    chartRef.current.applyOptions({
      layout: {
        background: { type: ColorType.Solid, color: 'transparent' },
        textColor: isDark ? '#94a3b8' : '#64748b',
      },
      grid: {
        vertLines: { color: isDark ? '#334155' : '#e2e8f0' },
        horzLines: { color: isDark ? '#334155' : '#e2e8f0' },
      },
      timeScale: {
        borderColor: isDark ? '#334155' : '#e2e8f0',
      },
      rightPriceScale: {
        borderColor: isDark ? '#334155' : '#e2e8f0',
      },
    })
  }, [theme])

  // Update kline data
  useEffect(() => {
    if (!candleSeriesRef.current || klines.length === 0) return

    // Convert klines to CandlestickData format
    const candleData: CandlestickData<UTCTimestamp>[] = klines.map((k) => ({
      time: k.time as UTCTimestamp,
      open: k.open,
      high: k.high,
      low: k.low,
      close: k.close,
    }))

    candleSeriesRef.current.setData(candleData)
  }, [klines])

  // Draw signal markers (Entry, TP, SL lines)
  useEffect(() => {
    if (!candleSeriesRef.current || !selectedSignal) return

    const entryLine = candleSeriesRef.current.createPriceLine({
      price: selectedSignal.entry_price,
      color: '#3b82f6',
      lineWidth: 2,
      lineStyle: 0, // Solid
      axisLabelVisible: true,
      title: 'Entry',
    })

    const tpLine = candleSeriesRef.current.createPriceLine({
      price: selectedSignal.tp_price,
      color: '#22c55e',
      lineWidth: 1,
      lineStyle: 2, // Dashed
      axisLabelVisible: true,
      title: 'TP',
    })

    const slLine = candleSeriesRef.current.createPriceLine({
      price: selectedSignal.sl_price,
      color: '#ef4444',
      lineWidth: 1,
      lineStyle: 2, // Dashed
      axisLabelVisible: true,
      title: 'SL',
    })

    return () => {
      if (candleSeriesRef.current) {
        candleSeriesRef.current.removePriceLine(entryLine)
        candleSeriesRef.current.removePriceLine(tpLine)
        candleSeriesRef.current.removePriceLine(slLine)
      }
    }
  }, [selectedSignal])

  return (
    <Card className={cn('overflow-hidden', className)}>
      <CardHeader className="pb-2">
        <CardTitle className="text-sm font-medium text-muted-foreground">
          {symbol} - {timeframe}
        </CardTitle>
      </CardHeader>
      <CardContent className="p-0">
        {isLoading ? (
          <Skeleton className="h-[400px] w-full" />
        ) : (
          <div ref={containerRef} className="h-[400px] w-full" />
        )}
      </CardContent>
    </Card>
  )
}
