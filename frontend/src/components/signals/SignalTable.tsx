import type { Signal } from '@/services/api'
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from '@/components/ui/table'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { DirectionBadge, OutcomeBadge } from './SignalBadge'
import { formatPrice, formatTime, formatDateTime } from '@/lib/utils'
import { cn } from '@/lib/utils'

interface SignalTableProps {
  signals: Signal[]
  title: string
  onSignalClick?: (signal: Signal) => void
  className?: string
}

export function SignalTable({ signals, title, onSignalClick, className }: SignalTableProps) {
  if (signals.length === 0) {
    return (
      <Card className={className}>
        <CardHeader>
          <CardTitle className="text-sm font-medium text-muted-foreground">
            {title}
          </CardTitle>
        </CardHeader>
        <CardContent>
          <p className="text-center text-sm text-muted-foreground py-8">
            No signals
          </p>
        </CardContent>
      </Card>
    )
  }

  return (
    <Card className={className}>
      <CardHeader>
        <CardTitle className="text-sm font-medium text-muted-foreground">
          {title}
        </CardTitle>
      </CardHeader>
      <CardContent>
        <Table>
          <TableHeader>
            <TableRow>
              <TableHead>Time</TableHead>
              <TableHead>Symbol</TableHead>
              <TableHead>TF</TableHead>
              <TableHead>Direction</TableHead>
              <TableHead className="text-right">Entry</TableHead>
              <TableHead className="text-right">TP</TableHead>
              <TableHead className="text-right">SL</TableHead>
              <TableHead>Outcome</TableHead>
              <TableHead>Closed</TableHead>
            </TableRow>
          </TableHeader>
          <TableBody>
            {signals.map((signal) => (
              <TableRow
                key={signal.id}
                className={cn(
                  onSignalClick && 'cursor-pointer'
                )}
                onClick={() => onSignalClick?.(signal)}
              >
                <TableCell className="font-mono text-xs whitespace-nowrap">
                  {formatDateTime(signal.signal_time)}
                </TableCell>
                <TableCell className="font-medium">{signal.symbol}</TableCell>
                <TableCell>{signal.timeframe}</TableCell>
                <TableCell>
                  <DirectionBadge direction={signal.direction} />
                </TableCell>
                <TableCell className="text-right font-mono">
                  {formatPrice(signal.entry_price)}
                </TableCell>
                <TableCell className="text-right font-mono text-success">
                  {formatPrice(signal.tp_price)}
                </TableCell>
                <TableCell className="text-right font-mono text-destructive">
                  {formatPrice(signal.sl_price)}
                </TableCell>
                <TableCell>
                  <OutcomeBadge outcome={signal.outcome} />
                </TableCell>
                <TableCell className="font-mono text-xs whitespace-nowrap">
                  {signal.outcome_time ? formatDateTime(signal.outcome_time) : '-'}
                </TableCell>
              </TableRow>
            ))}
          </TableBody>
        </Table>
      </CardContent>
    </Card>
  )
}
