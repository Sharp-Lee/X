import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { Skeleton } from '@/components/ui/skeleton'
import { cn } from '@/lib/utils'
import type { MaeMfeStats } from '@/services/api'

interface MaeMfeChartProps {
  data: Record<string, MaeMfeStats>
  isLoading: boolean
}

function StatRow({ label, value }: { label: string; value: string }) {
  return (
    <div className="flex justify-between text-sm">
      <span className="text-muted-foreground">{label}</span>
      <span className="font-mono">{value}</span>
    </div>
  )
}

function DistributionBar({
  label,
  value,
  maxValue,
  color,
}: {
  label: string
  value: number
  maxValue: number
  color: string
}) {
  const pct = maxValue > 0 ? (value / maxValue) * 100 : 0
  return (
    <div className="flex items-center gap-2 text-xs">
      <span className="w-10 text-right text-muted-foreground font-mono">{label}</span>
      <div className="flex-1 bg-muted rounded-sm h-4 overflow-hidden">
        <div
          className={cn('h-full rounded-sm', color)}
          style={{ width: `${Math.max(pct, pct > 0 ? 2 : 0)}%` }}
        />
      </div>
      <span className="w-14 text-right font-mono">{(value * 100).toFixed(1)}%</span>
    </div>
  )
}

function OutcomeCard({
  title,
  stats,
  color,
  barColor,
}: {
  title: string
  stats: MaeMfeStats
  color: 'success' | 'destructive'
  barColor: string
}) {
  const percentiles = [
    { label: 'P25', mae: stats.mae_p25, mfe: stats.mfe_p25 },
    { label: 'P50', mae: stats.mae_p50, mfe: stats.mfe_p50 },
    { label: 'P75', mae: stats.mae_p75, mfe: stats.mfe_p75 },
    { label: 'P90', mae: stats.mae_p90, mfe: stats.mfe_p90 },
  ]

  const maxMae = Math.max(...percentiles.map((p) => p.mae ?? 0), 0.01)
  const maxMfe = Math.max(...percentiles.map((p) => p.mfe ?? 0), 0.01)

  return (
    <Card>
      <CardHeader className="pb-3">
        <CardTitle className="text-sm flex items-center gap-2">
          <span
            className={cn(
              'h-2 w-2 rounded-full',
              color === 'success' ? 'bg-success' : 'bg-destructive'
            )}
          />
          {title}
          <span className="text-muted-foreground font-normal">({stats.count})</span>
        </CardTitle>
      </CardHeader>
      <CardContent className="space-y-4">
        <div className="grid grid-cols-2 gap-4">
          <StatRow label="Avg MAE" value={`${((stats.avg_mae ?? 0) * 100).toFixed(1)}%`} />
          <StatRow label="Avg MFE" value={`${((stats.avg_mfe ?? 0) * 100).toFixed(1)}%`} />
        </div>

        <div className="space-y-1">
          <p className="text-xs text-muted-foreground font-medium">MAE Distribution</p>
          {percentiles.map((p) => (
            <DistributionBar
              key={`mae-${p.label}`}
              label={p.label}
              value={p.mae ?? 0}
              maxValue={maxMae}
              color={barColor}
            />
          ))}
        </div>

        <div className="space-y-1">
          <p className="text-xs text-muted-foreground font-medium">MFE Distribution</p>
          {percentiles.map((p) => (
            <DistributionBar
              key={`mfe-${p.label}`}
              label={p.label}
              value={p.mfe ?? 0}
              maxValue={maxMfe}
              color={barColor}
            />
          ))}
        </div>
      </CardContent>
    </Card>
  )
}

export function MaeMfeChart({ data, isLoading }: MaeMfeChartProps) {
  if (isLoading) {
    return (
      <div className="grid gap-6 md:grid-cols-2">
        {[0, 1].map((i) => (
          <Card key={i}>
            <CardHeader>
              <Skeleton className="h-4 w-24" />
            </CardHeader>
            <CardContent className="space-y-2">
              {Array.from({ length: 6 }).map((_, j) => (
                <Skeleton key={j} className="h-4 w-full" />
              ))}
            </CardContent>
          </Card>
        ))}
      </div>
    )
  }

  const tp = data['tp']
  const sl = data['sl']

  if (!tp && !sl) {
    return null
  }

  return (
    <div className="grid gap-6 md:grid-cols-2">
      {tp && (
        <OutcomeCard
          title="Winners (TP)"
          stats={tp}
          color="success"
          barColor="bg-success"
        />
      )}
      {sl && (
        <OutcomeCard
          title="Losers (SL)"
          stats={sl}
          color="destructive"
          barColor="bg-destructive"
        />
      )}
    </div>
  )
}
