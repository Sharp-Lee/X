import { Button } from '@/components/ui/button'
import { ConnectionStatus } from '@/components/common/ConnectionStatus'
import { ThemeToggle } from '@/components/common/ThemeToggle'

const SYMBOLS = ['BTCUSDT', 'ETHUSDT', 'SOLUSDT', 'BNBUSDT', 'XRPUSDT']
const TIMEFRAMES = ['1m', '3m', '5m', '15m', '30m']

interface HeaderProps {
  selectedSymbol?: string
  onSymbolChange: (symbol: string | undefined) => void
  selectedTimeframe?: string
  onTimeframeChange: (timeframe: string | undefined) => void
  selectedStrategy?: string
  onStrategyChange: (strategy: string | undefined) => void
  strategies: string[]
  isConnected: boolean
}

export function Header({
  selectedSymbol,
  onSymbolChange,
  selectedTimeframe,
  onTimeframeChange,
  selectedStrategy,
  onStrategyChange,
  strategies,
  isConnected,
}: HeaderProps) {
  return (
    <header className="border-b bg-card px-6 py-3">
      <div className="flex items-center justify-between">
        <h1 className="text-xl font-semibold">Signal Dashboard</h1>
        <div className="flex items-center gap-4">
          <ConnectionStatus isConnected={isConnected} />
          <ThemeToggle />
        </div>
      </div>

      <div className="mt-2 flex items-center gap-6">
        {/* Strategy filters (only show when multiple strategies exist) */}
        {strategies.length > 1 && (
          <>
            <div className="flex items-center gap-1">
              <span className="text-xs text-muted-foreground mr-1">Strategy</span>
              <Button
                size="sm"
                variant={selectedStrategy === undefined ? 'secondary' : 'outline'}
                onClick={() => onStrategyChange(undefined)}
              >
                All
              </Button>
              {strategies.map((name) => (
                <Button
                  key={name}
                  size="sm"
                  variant={selectedStrategy === name ? 'secondary' : 'outline'}
                  onClick={() => onStrategyChange(selectedStrategy === name ? undefined : name)}
                >
                  {name.replace(/_/g, ' ')}
                </Button>
              ))}
            </div>
            <div className="h-6 w-px bg-border" />
          </>
        )}

        {/* Symbol filters */}
        <div className="flex items-center gap-1">
          <span className="text-xs text-muted-foreground mr-1">Symbol</span>
          <Button
            size="sm"
            variant={selectedSymbol === undefined ? 'secondary' : 'outline'}
            onClick={() => onSymbolChange(undefined)}
          >
            All
          </Button>
          {SYMBOLS.map((symbol) => (
            <Button
              key={symbol}
              size="sm"
              variant={selectedSymbol === symbol ? 'secondary' : 'outline'}
              onClick={() => onSymbolChange(selectedSymbol === symbol ? undefined : symbol)}
            >
              {symbol.replace('USDT', '')}
            </Button>
          ))}
        </div>

        <div className="h-6 w-px bg-border" />

        {/* Timeframe filters */}
        <div className="flex items-center gap-1">
          <span className="text-xs text-muted-foreground mr-1">Timeframe</span>
          <Button
            size="sm"
            variant={selectedTimeframe === undefined ? 'secondary' : 'outline'}
            onClick={() => onTimeframeChange(undefined)}
          >
            All
          </Button>
          {TIMEFRAMES.map((tf) => (
            <Button
              key={tf}
              size="sm"
              variant={selectedTimeframe === tf ? 'secondary' : 'outline'}
              onClick={() => onTimeframeChange(selectedTimeframe === tf ? undefined : tf)}
            >
              {tf}
            </Button>
          ))}
        </div>
      </div>
    </header>
  )
}
