import { Card, CardContent } from '@/components/ui/card'
import { Badge } from '@/components/ui/badge'
import type { Signal } from '@/services/api'
import { cn } from '@/lib/utils'

const TIMEFRAMES = ['1m', '3m', '5m', '15m', '30m']

interface TimeframeGridProps {
  signals: Signal[]
  selectedTimeframe?: string
  onTimeframeSelect?: (timeframe: string) => void
  className?: string
}

export function TimeframeGrid({
  signals,
  selectedTimeframe,
  onTimeframeSelect,
  className,
}: TimeframeGridProps) {
  return (
    <div className={cn('grid grid-cols-5 gap-2', className)}>
      {TIMEFRAMES.map((tf) => {
        const tfSignals = signals.filter((s) => s.timeframe === tf)
        const activeCount = tfSignals.filter((s) => s.outcome === 'active').length
        const tpCount = tfSignals.filter((s) => s.outcome === 'tp').length
        const slCount = tfSignals.filter((s) => s.outcome === 'sl').length

        return (
          <Card
            key={tf}
            className={cn(
              'cursor-pointer transition-colors hover:bg-muted/50',
              selectedTimeframe === tf && 'ring-2 ring-primary'
            )}
            onClick={() => onTimeframeSelect?.(tf)}
          >
            <CardContent className="p-3 text-center">
              <div className="text-lg font-semibold">{tf}</div>
              <div className="mt-2 flex justify-center gap-1">
                {activeCount > 0 && (
                  <Badge variant="default" className="text-xs">
                    {activeCount}
                  </Badge>
                )}
                {tpCount > 0 && (
                  <Badge variant="success" className="text-xs">
                    {tpCount}
                  </Badge>
                )}
                {slCount > 0 && (
                  <Badge variant="destructive" className="text-xs">
                    {slCount}
                  </Badge>
                )}
                {activeCount === 0 && tpCount === 0 && slCount === 0 && (
                  <span className="text-xs text-muted-foreground">-</span>
                )}
              </div>
            </CardContent>
          </Card>
        )
      })}
    </div>
  )
}
