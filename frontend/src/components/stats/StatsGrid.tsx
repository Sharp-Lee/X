import { useEffect, useState } from 'react'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { Progress } from '@/components/ui/progress'
import { api, type Stats } from '@/services/api'
import { cn } from '@/lib/utils'

interface StatsGridProps {
  symbol?: string
  className?: string
}

export function StatsGrid({ symbol, className }: StatsGridProps) {
  const [stats, setStats] = useState<Stats | null>(null)
  const [isLoading, setIsLoading] = useState(true)

  useEffect(() => {
    const fetchStats = async () => {
      try {
        const data = await api.getStats({ symbol })
        setStats(data)
      } catch (error) {
        console.error('Failed to fetch stats:', error)
      } finally {
        setIsLoading(false)
      }
    }

    fetchStats()
    const interval = setInterval(fetchStats, 30000) // Refresh every 30 seconds

    return () => clearInterval(interval)
  }, [symbol])

  if (isLoading || !stats) {
    return (
      <Card className={className}>
        <CardHeader>
          <CardTitle className="text-sm font-medium text-muted-foreground">
            Performance
          </CardTitle>
        </CardHeader>
        <CardContent>
          <div className="animate-pulse space-y-4">
            <div className="grid grid-cols-2 gap-4">
              {[1, 2, 3, 4].map((i) => (
                <div key={i} className="space-y-2">
                  <div className="h-3 w-16 bg-muted rounded" />
                  <div className="h-6 w-12 bg-muted rounded" />
                </div>
              ))}
            </div>
          </div>
        </CardContent>
      </Card>
    )
  }

  const winRate = stats.win_rate * 100
  const breakevenRate = stats.breakeven_win_rate

  return (
    <Card className={className}>
      <CardHeader>
        <CardTitle className="text-sm font-medium text-muted-foreground">
          Performance
        </CardTitle>
      </CardHeader>
      <CardContent className="space-y-4">
        {/* Stats grid */}
        <div className="grid grid-cols-2 gap-4">
          <StatItem label="Total" value={stats.total_signals} />
          <StatItem label="Active" value={stats.active} className="text-primary" />
          <StatItem label="Wins" value={stats.wins} className="text-success" />
          <StatItem label="Losses" value={stats.losses} className="text-destructive" />
        </div>

        {/* Win rate bar */}
        <div className="space-y-2">
          <div className="flex justify-between text-sm">
            <span className="text-muted-foreground">Win Rate</span>
            <span className={cn(
              'font-semibold',
              winRate >= breakevenRate ? 'text-success' : 'text-destructive'
            )}>
              {winRate.toFixed(1)}%
            </span>
          </div>
          <div className="relative">
            <Progress
              value={winRate}
              className="h-2"
              indicatorClassName={winRate >= breakevenRate ? 'bg-success' : 'bg-destructive'}
            />
            {/* Breakeven marker */}
            <div
              className="absolute top-0 bottom-0 w-0.5 bg-foreground"
              style={{ left: `${breakevenRate}%` }}
            />
          </div>
          <div className="flex justify-between text-xs text-muted-foreground">
            <span>0%</span>
            <span>Breakeven: {breakevenRate}%</span>
            <span>100%</span>
          </div>
        </div>
      </CardContent>
    </Card>
  )
}

interface StatItemProps {
  label: string
  value: number
  className?: string
}

function StatItem({ label, value, className }: StatItemProps) {
  return (
    <div className="space-y-1">
      <span className="text-xs text-muted-foreground">{label}</span>
      <p className={cn('text-2xl font-semibold', className)}>{value}</p>
    </div>
  )
}
