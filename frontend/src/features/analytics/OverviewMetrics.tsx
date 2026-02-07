import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { Progress } from '@/components/ui/progress'
import { Skeleton } from '@/components/ui/skeleton'
import { cn } from '@/lib/utils'
import type { ExpectancyStats } from '@/services/api'

const BREAKEVEN_WIN_RATE = 81.5

interface OverviewMetricsProps {
  data: ExpectancyStats | null
  isLoading: boolean
}

function MetricCard({
  label,
  value,
  color,
}: {
  label: string
  value: string
  color?: 'success' | 'destructive' | 'default'
}) {
  return (
    <div className="space-y-1">
      <p className="text-xs text-muted-foreground">{label}</p>
      <p
        className={cn(
          'text-xl font-bold font-mono',
          color === 'success' && 'text-success',
          color === 'destructive' && 'text-destructive'
        )}
      >
        {value}
      </p>
    </div>
  )
}

export function OverviewMetrics({ data, isLoading }: OverviewMetricsProps) {
  if (isLoading || !data) {
    return (
      <Card>
        <CardHeader>
          <CardTitle>Overall Performance</CardTitle>
        </CardHeader>
        <CardContent className="space-y-4">
          <div className="grid grid-cols-2 gap-4 md:grid-cols-5">
            {Array.from({ length: 5 }).map((_, i) => (
              <Skeleton key={i} className="h-12 w-full" />
            ))}
          </div>
          <Skeleton className="h-4 w-full" />
        </CardContent>
      </Card>
    )
  }

  const winRate = data.win_rate ?? 0
  const aboveBreakeven = winRate >= BREAKEVEN_WIN_RATE
  const expectancy = data.expectancy_r ?? 0
  const profitFactor = data.profit_factor ?? 0

  return (
    <Card>
      <CardHeader>
        <CardTitle>Overall Performance</CardTitle>
      </CardHeader>
      <CardContent className="space-y-6">
        {/* Key metrics */}
        <div className="grid grid-cols-2 gap-4 md:grid-cols-5">
          <MetricCard label="Total Closed" value={String(data.total)} />
          <MetricCard
            label="Win Rate"
            value={`${winRate.toFixed(1)}%`}
            color={aboveBreakeven ? 'success' : 'destructive'}
          />
          <MetricCard
            label="Expectancy"
            value={`${expectancy >= 0 ? '+' : ''}${expectancy.toFixed(2)}R`}
            color={expectancy > 0 ? 'success' : 'destructive'}
          />
          <MetricCard
            label="Profit Factor"
            value={profitFactor.toFixed(2)}
            color={profitFactor >= 1 ? 'success' : 'destructive'}
          />
          <MetricCard
            label="W / L"
            value={`${data.wins} / ${data.losses}`}
          />
        </div>

        {/* Win rate progress bar with breakeven marker */}
        <div className="space-y-2">
          <div className="flex justify-between text-xs text-muted-foreground">
            <span>Win Rate</span>
            <span>Breakeven: {BREAKEVEN_WIN_RATE}%</span>
          </div>
          <div className="relative">
            <Progress
              value={winRate}
              className="h-3"
              indicatorClassName={aboveBreakeven ? 'bg-success' : 'bg-destructive'}
            />
            {/* Breakeven marker */}
            <div
              className="absolute top-0 bottom-0 w-0.5 bg-foreground/70"
              style={{ left: `${BREAKEVEN_WIN_RATE}%` }}
            />
          </div>
        </div>

        {/* Cumulative R */}
        <div className="flex items-center justify-between text-sm">
          <span className="text-muted-foreground">Cumulative P&L</span>
          <span
            className={cn(
              'font-bold font-mono text-lg',
              (data.total_r ?? 0) >= 0 ? 'text-success' : 'text-destructive'
            )}
          >
            {(data.total_r ?? 0) >= 0 ? '+' : ''}
            {(data.total_r ?? 0).toFixed(1)}R
          </span>
        </div>
      </CardContent>
    </Card>
  )
}
