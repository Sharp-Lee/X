import { Tabs, TabsList, TabsTrigger, TabsContent } from '@/components/ui/tabs'
import { SignalTable } from '@/components/signals/SignalTable'
import type { Signal } from '@/services/api'
import { cn } from '@/lib/utils'

const TIMEFRAMES = ['1m', '3m', '5m', '15m', '30m']

interface TimeframeTabsProps {
  signals: Signal[]
  onSignalClick?: (signal: Signal) => void
  className?: string
}

export function TimeframeTabs({ signals, onSignalClick, className }: TimeframeTabsProps) {
  // Count signals per timeframe
  const countByTimeframe = TIMEFRAMES.reduce((acc, tf) => {
    acc[tf] = signals.filter((s) => s.timeframe === tf && s.outcome === 'active').length
    return acc
  }, {} as Record<string, number>)

  return (
    <Tabs defaultValue="all" className={cn('w-full', className)}>
      <TabsList className="w-full justify-start">
        <TabsTrigger value="all" className="gap-2">
          All
          <CountBadge count={signals.filter(s => s.outcome === 'active').length} />
        </TabsTrigger>
        {TIMEFRAMES.map((tf) => (
          <TabsTrigger key={tf} value={tf} className="gap-2">
            {tf}
            <CountBadge count={countByTimeframe[tf]} />
          </TabsTrigger>
        ))}
      </TabsList>

      <TabsContent value="all" className="mt-4">
        <SignalTable
          signals={signals}
          title="All Timeframes"
          onSignalClick={onSignalClick}
        />
      </TabsContent>

      {TIMEFRAMES.map((tf) => (
        <TabsContent key={tf} value={tf} className="mt-4">
          <SignalTable
            signals={signals.filter((s) => s.timeframe === tf)}
            title={`${tf} Signals`}
            onSignalClick={onSignalClick}
          />
        </TabsContent>
      ))}
    </Tabs>
  )
}

function CountBadge({ count }: { count: number }) {
  if (count === 0) return null

  return (
    <span className="flex h-5 w-5 items-center justify-center rounded-full bg-primary text-[10px] text-primary-foreground">
      {count}
    </span>
  )
}
