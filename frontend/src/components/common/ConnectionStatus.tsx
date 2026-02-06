import { Badge } from '@/components/ui/badge'
import { cn } from '@/lib/utils'

interface ConnectionStatusProps {
  isConnected: boolean
  className?: string
}

export function ConnectionStatus({ isConnected, className }: ConnectionStatusProps) {
  return (
    <Badge
      variant={isConnected ? 'success' : 'destructive'}
      className={cn('gap-1.5', className)}
    >
      <span
        className={cn(
          'h-2 w-2 rounded-full',
          isConnected ? 'bg-green-400 animate-pulse' : 'bg-red-400'
        )}
      />
      {isConnected ? 'Connected' : 'Disconnected'}
    </Badge>
  )
}
