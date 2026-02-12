import type { Signal } from '@/services/api'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { Progress } from '@/components/ui/progress'
import { DirectionBadge } from './SignalBadge'
import { formatPrice, formatDateTime } from '@/lib/utils'
import { cn } from '@/lib/utils'

interface ActivePositionsProps {
  signals: Signal[]
  onSelect?: (signal: Signal) => void
  className?: string
}

export function ActivePositions({ signals, onSelect, className }: ActivePositionsProps) {
  if (signals.length === 0) {
    return (
      <Card className={className}>
        <CardHeader>
          <CardTitle className="text-sm font-medium text-muted-foreground">
            Active Positions
          </CardTitle>
        </CardHeader>
        <CardContent>
          <p className="text-center text-sm text-muted-foreground py-8">
            No active positions
          </p>
        </CardContent>
      </Card>
    )
  }

  return (
    <Card className={className}>
      <CardHeader>
        <CardTitle className="text-sm font-medium text-muted-foreground">
          Active Positions ({signals.length})
        </CardTitle>
      </CardHeader>
      <CardContent className="space-y-4">
        {signals.map((signal) => (
          <PositionCard
            key={signal.id}
            signal={signal}
            onClick={() => onSelect?.(signal)}
          />
        ))}
      </CardContent>
    </Card>
  )
}

interface PositionCardProps {
  signal: Signal
  onClick?: () => void
}

function PositionCard({ signal, onClick }: PositionCardProps) {
  const maePercent = Math.min(signal.mae_ratio * 100, 100)
  const mfePercent = Math.min(signal.mfe_ratio * 100, 100)

  // Determine MAE color based on ratio
  const getMaeColor = (ratio: number) => {
    if (ratio < 0.25) return 'bg-success'
    if (ratio < 0.5) return 'bg-warning'
    return 'bg-destructive'
  }

  return (
    <div
      className={cn(
        'rounded-lg border bg-muted/50 p-3 space-y-3',
        onClick && 'cursor-pointer hover:bg-muted'
      )}
      onClick={onClick}
    >
      {/* Header */}
      <div className="flex items-center justify-between">
        <span className="font-semibold">{signal.symbol}</span>
        <DirectionBadge direction={signal.direction} />
      </div>

      {/* Price levels */}
      <div className="space-y-1 text-sm">
        <div className="flex justify-between">
          <span className="text-muted-foreground">Entry</span>
          <span className="font-mono">{formatPrice(signal.entry_price)}</span>
        </div>
        <div className="flex justify-between text-success">
          <span>TP</span>
          <span className="font-mono">{formatPrice(signal.tp_price)}</span>
        </div>
        <div className="flex justify-between text-destructive">
          <span>SL</span>
          <span className="font-mono">{formatPrice(signal.sl_price)}</span>
        </div>
      </div>

      {/* Metrics */}
      <div className="grid grid-cols-2 gap-4 text-sm">
        <div>
          <span className="text-muted-foreground">MAE</span>
          <div className="flex items-center gap-2">
            <Progress
              value={maePercent}
              className="h-1.5 flex-1"
              indicatorClassName={getMaeColor(signal.mae_ratio)}
            />
            <span className="font-mono text-xs">{maePercent.toFixed(0)}%</span>
          </div>
        </div>
        <div>
          <span className="text-muted-foreground">MFE</span>
          <div className="flex items-center gap-2">
            <Progress
              value={mfePercent}
              className="h-1.5 flex-1"
              indicatorClassName="bg-success"
            />
            <span className="font-mono text-xs">{mfePercent.toFixed(0)}%</span>
          </div>
        </div>
      </div>

      {/* Timeframe & Time */}
      <div className="text-xs text-muted-foreground">
        {signal.timeframe} | Streak: {signal.streak_at_signal}
      </div>
      <div className="text-xs text-muted-foreground">
        {formatDateTime(signal.signal_time)}
      </div>
    </div>
  )
}
