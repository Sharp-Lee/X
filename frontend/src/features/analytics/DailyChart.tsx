import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import {
  Tooltip,
  TooltipContent,
  TooltipTrigger,
} from '@/components/ui/tooltip'
import { Skeleton } from '@/components/ui/skeleton'
import { cn } from '@/lib/utils'
import type { DailyPerformance } from '@/services/api'

interface DailyChartProps {
  data: DailyPerformance[]
  isLoading: boolean
}

export function DailyChart({ data, isLoading }: DailyChartProps) {
  if (isLoading) {
    return (
      <Card>
        <CardHeader>
          <CardTitle>Daily Performance</CardTitle>
        </CardHeader>
        <CardContent>
          <Skeleton className="h-40 w-full" />
        </CardContent>
      </Card>
    )
  }

  if (data.length === 0) {
    return (
      <Card>
        <CardHeader>
          <CardTitle>Daily Performance</CardTitle>
        </CardHeader>
        <CardContent>
          <p className="text-sm text-muted-foreground">No daily data yet.</p>
        </CardContent>
      </Card>
    )
  }

  const maxSignals = Math.max(...data.map((d) => d.total), 1)
  const totalWins = data.reduce((s, d) => s + d.wins, 0)
  const totalLosses = data.reduce((s, d) => s + d.losses, 0)
  const lastCumulativeR = data[data.length - 1]?.cumulative_r ?? 0

  return (
    <Card>
      <CardHeader>
        <CardTitle>Daily Performance</CardTitle>
      </CardHeader>
      <CardContent className="space-y-4">
        {/* Summary */}
        <div className="flex gap-4 text-sm">
          <span className="text-muted-foreground">Last {data.length} days</span>
          <span className="text-success font-medium">{totalWins} wins</span>
          <span className="text-destructive font-medium">{totalLosses} losses</span>
          <span
            className={cn(
              'font-bold font-mono ml-auto',
              lastCumulativeR >= 0 ? 'text-success' : 'text-destructive'
            )}
          >
            {lastCumulativeR >= 0 ? '+' : ''}
            {lastCumulativeR.toFixed(1)}R
          </span>
        </div>

        {/* Bar chart */}
        <div className="flex items-end gap-[2px] h-32">
          {data.map((day) => (
            <Tooltip key={day.date}>
              <TooltipTrigger asChild>
                <div className="flex-1 flex flex-col justify-end min-w-[3px] cursor-pointer">
                  {day.losses > 0 && (
                    <div
                      className="bg-destructive rounded-t-sm"
                      style={{
                        height: `${(day.losses / maxSignals) * 100}%`,
                      }}
                    />
                  )}
                  {day.wins > 0 && (
                    <div
                      className={cn(
                        'bg-success',
                        day.losses === 0 && 'rounded-t-sm'
                      )}
                      style={{
                        height: `${(day.wins / maxSignals) * 100}%`,
                      }}
                    />
                  )}
                </div>
              </TooltipTrigger>
              <TooltipContent>
                <p className="font-medium">{day.date}</p>
                <p className="text-success">{day.wins}W / <span className="text-destructive">{day.losses}L</span></p>
                <p className="font-mono">
                  Day: {(day.daily_r ?? 0) >= 0 ? '+' : ''}{(day.daily_r ?? 0).toFixed(1)}R
                </p>
                <p className="font-mono">
                  Total: {(day.cumulative_r ?? 0) >= 0 ? '+' : ''}{(day.cumulative_r ?? 0).toFixed(1)}R
                </p>
              </TooltipContent>
            </Tooltip>
          ))}
        </div>

        {/* Date labels */}
        <div className="flex justify-between text-xs text-muted-foreground">
          <span>{data[0]?.date}</span>
          <span>{data[data.length - 1]?.date}</span>
        </div>
      </CardContent>
    </Card>
  )
}
