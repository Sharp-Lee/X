import { useState } from 'react'
import { Tabs, TabsContent, TabsList, TabsTrigger } from '@/components/ui/tabs'
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from '@/components/ui/select'
import { useAnalytics } from '@/hooks/useAnalytics'
import { OverviewMetrics } from './OverviewMetrics'
import { DirectionComparison } from './DirectionComparison'
import { PerformanceTable } from './PerformanceTable'
import { DailyChart } from './DailyChart'
import { MaeMfeChart } from './MaeMfeChart'

const DAYS_OPTIONS = [
  { value: '7', label: 'Last 7 days' },
  { value: '30', label: 'Last 30 days' },
  { value: '90', label: 'Last 90 days' },
  { value: '365', label: 'All time' },
]

export function AnalyticsPage() {
  const [days, setDays] = useState(30)
  const { data, isLoading } = useAnalytics(days)

  const symbolData =
    data?.by_symbol.map((s) => ({
      key: s.symbol,
      wins: s.wins,
      losses: s.losses,
      total: s.total,
      win_rate: s.win_rate,
    })) ?? []

  const timeframeData =
    data?.by_timeframe.map((t) => ({
      key: t.timeframe,
      wins: t.wins,
      losses: t.losses,
      total: t.total,
      win_rate: t.win_rate,
    })) ?? []

  return (
    <main className="flex-1 p-6 space-y-6">
      {/* Time range selector */}
      <div className="flex justify-end">
        <Select
          value={String(days)}
          onValueChange={(v) => setDays(Number(v))}
        >
          <SelectTrigger className="w-[160px]">
            <SelectValue />
          </SelectTrigger>
          <SelectContent>
            {DAYS_OPTIONS.map((opt) => (
              <SelectItem key={opt.value} value={opt.value}>
                {opt.label}
              </SelectItem>
            ))}
          </SelectContent>
        </Select>
      </div>

      {/* Section 1: Overview */}
      <OverviewMetrics data={data?.expectancy ?? null} isLoading={isLoading} />

      {/* Section 2: LONG vs SHORT */}
      <DirectionComparison
        data={data?.by_direction ?? []}
        isLoading={isLoading}
      />

      {/* Section 3: Breakdown tables */}
      <Tabs defaultValue="symbol">
        <TabsList>
          <TabsTrigger value="symbol">By Symbol</TabsTrigger>
          <TabsTrigger value="timeframe">By Timeframe</TabsTrigger>
        </TabsList>
        <TabsContent value="symbol" className="mt-4">
          <PerformanceTable
            title="Performance by Symbol"
            data={symbolData}
            isLoading={isLoading}
          />
        </TabsContent>
        <TabsContent value="timeframe" className="mt-4">
          <PerformanceTable
            title="Performance by Timeframe"
            data={timeframeData}
            isLoading={isLoading}
          />
        </TabsContent>
      </Tabs>

      {/* Section 4: Daily performance */}
      <DailyChart data={data?.daily ?? []} isLoading={isLoading} />

      {/* Section 5: MAE/MFE distribution */}
      <MaeMfeChart data={data?.mae_mfe ?? {}} isLoading={isLoading} />
    </main>
  )
}
