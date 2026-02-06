import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from '@/components/ui/select'
import { ConnectionStatus } from '@/components/common/ConnectionStatus'
import { ThemeToggle } from '@/components/common/ThemeToggle'

const SYMBOLS = ['BTCUSDT', 'ETHUSDT', 'SOLUSDT', 'BNBUSDT', 'XRPUSDT']

interface HeaderProps {
  selectedSymbol?: string
  onSymbolChange: (symbol: string | undefined) => void
  isConnected: boolean
}

export function Header({ selectedSymbol, onSymbolChange, isConnected }: HeaderProps) {
  return (
    <header className="flex items-center justify-between border-b bg-card px-6 py-4">
      <div className="flex items-center gap-8">
        <h1 className="text-xl font-semibold">MSR Retest Capture</h1>
      </div>

      <div className="flex items-center gap-4">
        <Select
          value={selectedSymbol || 'all'}
          onValueChange={(value) => onSymbolChange(value === 'all' ? undefined : value)}
        >
          <SelectTrigger className="w-[140px]">
            <SelectValue placeholder="All Symbols" />
          </SelectTrigger>
          <SelectContent>
            <SelectItem value="all">All Symbols</SelectItem>
            {SYMBOLS.map((symbol) => (
              <SelectItem key={symbol} value={symbol}>
                {symbol}
              </SelectItem>
            ))}
          </SelectContent>
        </Select>

        <ConnectionStatus isConnected={isConnected} />
        <ThemeToggle />
      </div>
    </header>
  )
}
