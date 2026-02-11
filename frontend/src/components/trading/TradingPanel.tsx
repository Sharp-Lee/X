import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { Badge } from '@/components/ui/badge'
import { useTradingAccounts } from '@/hooks/useTradingAccounts'
import { formatPrice, cn } from '@/lib/utils'
import type { TradingAccount, TradingPosition } from '@/services/api'

export function TradingPanel() {
  const { data, isLoading } = useTradingAccounts()

  // Don't render if auto-trading is not enabled
  if (!isLoading && (!data || !data.enabled)) {
    return null
  }

  if (isLoading) {
    return (
      <Card>
        <CardHeader>
          <CardTitle className="text-sm font-medium text-muted-foreground">
            Trading Accounts
          </CardTitle>
        </CardHeader>
        <CardContent>
          <div className="animate-pulse space-y-3">
            <div className="h-4 w-24 bg-muted rounded" />
            <div className="h-3 w-32 bg-muted rounded" />
            <div className="h-3 w-28 bg-muted rounded" />
          </div>
        </CardContent>
      </Card>
    )
  }

  const accounts = data!.accounts

  if (accounts.length === 0) {
    return (
      <Card>
        <CardHeader>
          <CardTitle className="text-sm font-medium text-muted-foreground">
            Trading Accounts
          </CardTitle>
        </CardHeader>
        <CardContent>
          <p className="text-center text-sm text-muted-foreground py-4">
            No active accounts
          </p>
        </CardContent>
      </Card>
    )
  }

  return (
    <Card>
      <CardHeader>
        <CardTitle className="text-sm font-medium text-muted-foreground">
          Trading Accounts ({accounts.length})
        </CardTitle>
      </CardHeader>
      <CardContent className="space-y-4">
        {accounts.map((account) => (
          <AccountCard key={account.name} account={account} />
        ))}
      </CardContent>
    </Card>
  )
}

function AccountCard({ account }: { account: TradingAccount }) {
  if (account.error) {
    return (
      <div className="rounded-lg border border-destructive/50 bg-destructive/10 p-3 space-y-2">
        <div className="flex items-center justify-between">
          <span className="font-semibold text-sm">{account.name}</span>
          <Badge variant="destructive">Error</Badge>
        </div>
        <p className="text-xs text-destructive">{account.error}</p>
      </div>
    )
  }

  const totalPnl = account.positions.reduce(
    (sum, p) => sum + p.unrealized_pnl,
    0
  )

  return (
    <div className="rounded-lg border bg-muted/50 p-3 space-y-3">
      {/* Header */}
      <div className="flex items-center justify-between">
        <span className="font-semibold text-sm">{account.name}</span>
        <div className="flex items-center gap-1.5">
          <Badge variant={account.testnet ? 'warning' : 'success'}>
            {account.testnet ? 'Testnet' : 'Live'}
          </Badge>
          <Badge variant="outline">{account.leverage}x</Badge>
        </div>
      </div>

      {/* Balance */}
      <div className="space-y-1 text-sm">
        <div className="flex justify-between">
          <span className="text-muted-foreground">Balance</span>
          <span className="font-mono font-semibold">
            ${formatBalance(account.balance.total)}
          </span>
        </div>
        <div className="flex justify-between text-xs">
          <span className="text-muted-foreground">Free / Used</span>
          <span className="font-mono text-muted-foreground">
            ${formatBalance(account.balance.free)} / ${formatBalance(account.balance.used)}
          </span>
        </div>
      </div>

      {/* Positions */}
      {account.positions.length > 0 && (
        <div className="space-y-2">
          <div className="flex items-center justify-between text-xs text-muted-foreground">
            <span>Positions ({account.positions.length})</span>
            <span className={cn(
              'font-mono font-semibold text-sm',
              totalPnl >= 0 ? 'text-success' : 'text-destructive'
            )}>
              {totalPnl >= 0 ? '+' : ''}{formatBalance(totalPnl)}
            </span>
          </div>
          {account.positions.map((pos) => (
            <PositionRow key={pos.symbol} position={pos} />
          ))}
        </div>
      )}

      {account.positions.length === 0 && (
        <p className="text-xs text-muted-foreground text-center py-1">
          No open positions
        </p>
      )}
    </div>
  )
}

function PositionRow({ position }: { position: TradingPosition }) {
  const isLong = position.side === 'long'

  return (
    <div className="flex items-center justify-between rounded border bg-background/50 px-2 py-1.5 text-xs">
      <div className="flex items-center gap-2">
        <Badge variant={isLong ? 'success' : 'destructive'} className="text-[10px] px-1.5 py-0">
          {isLong ? 'LONG' : 'SHORT'}
        </Badge>
        <span className="font-semibold">{position.symbol}</span>
        <span className="text-muted-foreground font-mono">{position.contracts}</span>
      </div>
      <div className="text-right">
        <span className={cn(
          'font-mono font-semibold',
          position.unrealized_pnl >= 0 ? 'text-success' : 'text-destructive'
        )}>
          {position.unrealized_pnl >= 0 ? '+' : ''}
          ${formatBalance(position.unrealized_pnl)}
        </span>
      </div>
    </div>
  )
}

function formatBalance(value: number): string {
  return value.toLocaleString(undefined, {
    minimumFractionDigits: 2,
    maximumFractionDigits: 2,
  })
}
