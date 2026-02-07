import { Card, CardContent } from '@/components/ui/card'
import { Badge } from '@/components/ui/badge'
import { Progress } from '@/components/ui/progress'
import { Skeleton } from '@/components/ui/skeleton'
import { cn } from '@/lib/utils'
import type { WinRateByDirection } from '@/services/api'

const BREAKEVEN_WIN_RATE = 81.5

interface DirectionComparisonProps {
  data: WinRateByDirection[]
  isLoading: boolean
}

export function DirectionComparison({ data, isLoading }: DirectionComparisonProps) {
  if (isLoading || data.length === 0) {
    return (
      <div className="grid grid-cols-1 gap-4 sm:grid-cols-2">
        {[0, 1].map((i) => (
          <Card key={i}>
            <CardContent className="p-4 space-y-3">
              <Skeleton className="h-6 w-20" />
              <Skeleton className="h-10 w-full" />
              <Skeleton className="h-3 w-full" />
            </CardContent>
          </Card>
        ))}
      </div>
    )
  }

  return (
    <div className="grid grid-cols-1 gap-4 sm:grid-cols-2">
      {data.map((d) => {
        const winRate = d.win_rate ?? 0
        const aboveBE = winRate >= BREAKEVEN_WIN_RATE
        return (
          <Card key={d.direction}>
            <CardContent className="p-4 space-y-3">
              <div className="flex items-center justify-between">
                <span className="text-lg font-semibold">{d.direction}</span>
                <Badge variant={d.direction === 'LONG' ? 'success' : 'destructive'}>
                  {d.total} trades
                </Badge>
              </div>

              <div className="grid grid-cols-2 gap-2 text-sm">
                <div>
                  <span className="text-muted-foreground">Wins</span>
                  <p className="text-success font-semibold">{d.wins}</p>
                </div>
                <div>
                  <span className="text-muted-foreground">Losses</span>
                  <p className="text-destructive font-semibold">{d.losses}</p>
                </div>
              </div>

              <div className="space-y-1">
                <div className="flex justify-between text-sm">
                  <span className="text-muted-foreground">Win Rate</span>
                  <span
                    className={cn(
                      'font-semibold font-mono',
                      aboveBE ? 'text-success' : 'text-destructive'
                    )}
                  >
                    {winRate.toFixed(1)}%
                  </span>
                </div>
                <div className="relative">
                  <Progress
                    value={winRate}
                    className="h-2"
                    indicatorClassName={aboveBE ? 'bg-success' : 'bg-destructive'}
                  />
                  <div
                    className="absolute top-0 bottom-0 w-0.5 bg-foreground/70"
                    style={{ left: `${BREAKEVEN_WIN_RATE}%` }}
                  />
                </div>
              </div>
            </CardContent>
          </Card>
        )
      })}
    </div>
  )
}
