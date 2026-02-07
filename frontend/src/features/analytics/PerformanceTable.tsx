import {
  Card,
  CardContent,
  CardHeader,
  CardTitle,
} from '@/components/ui/card'
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from '@/components/ui/table'
import { Badge } from '@/components/ui/badge'
import { Progress } from '@/components/ui/progress'
import { Skeleton } from '@/components/ui/skeleton'
import { cn } from '@/lib/utils'

const BREAKEVEN_WIN_RATE = 81.5

interface PerformanceRow {
  key: string
  wins: number
  losses: number
  total: number
  win_rate: number
}

interface PerformanceTableProps {
  title: string
  data: PerformanceRow[]
  isLoading: boolean
}

export function PerformanceTable({ title, data, isLoading }: PerformanceTableProps) {
  if (isLoading) {
    return (
      <Card>
        <CardHeader>
          <CardTitle>{title}</CardTitle>
        </CardHeader>
        <CardContent>
          {Array.from({ length: 5 }).map((_, i) => (
            <Skeleton key={i} className="h-8 w-full mb-2" />
          ))}
        </CardContent>
      </Card>
    )
  }

  if (data.length === 0) {
    return (
      <Card>
        <CardHeader>
          <CardTitle>{title}</CardTitle>
        </CardHeader>
        <CardContent>
          <p className="text-sm text-muted-foreground">No closed signals yet.</p>
        </CardContent>
      </Card>
    )
  }

  return (
    <Card>
      <CardHeader>
        <CardTitle>{title}</CardTitle>
      </CardHeader>
      <CardContent>
        <div className="overflow-x-auto">
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead>Name</TableHead>
                <TableHead className="text-right">Total</TableHead>
                <TableHead className="text-right">Wins</TableHead>
                <TableHead className="text-right">Losses</TableHead>
                <TableHead className="min-w-[150px]">Win Rate</TableHead>
                <TableHead>Status</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {data.map((row) => {
                const winRate = row.win_rate ?? 0
                const aboveBE = winRate >= BREAKEVEN_WIN_RATE
                return (
                  <TableRow key={row.key}>
                    <TableCell className="font-medium">{row.key}</TableCell>
                    <TableCell className="text-right font-mono">{row.total}</TableCell>
                    <TableCell className="text-right font-mono text-success">
                      {row.wins}
                    </TableCell>
                    <TableCell className="text-right font-mono text-destructive">
                      {row.losses}
                    </TableCell>
                    <TableCell>
                      <div className="flex items-center gap-2">
                        <div className="relative flex-1">
                          <Progress
                            value={winRate}
                            className="h-2"
                            indicatorClassName={aboveBE ? 'bg-success' : 'bg-destructive'}
                          />
                          <div
                            className="absolute top-0 bottom-0 w-0.5 bg-foreground/50"
                            style={{ left: `${BREAKEVEN_WIN_RATE}%` }}
                          />
                        </div>
                        <span
                          className={cn(
                            'font-mono text-xs w-14 text-right',
                            aboveBE ? 'text-success' : 'text-destructive'
                          )}
                        >
                          {winRate.toFixed(1)}%
                        </span>
                      </div>
                    </TableCell>
                    <TableCell>
                      <Badge variant={aboveBE ? 'success' : 'destructive'}>
                        {aboveBE ? 'Above BE' : 'Below BE'}
                      </Badge>
                    </TableCell>
                  </TableRow>
                )
              })}
            </TableBody>
          </Table>
        </div>
      </CardContent>
    </Card>
  )
}
