import { Badge } from '@/components/ui/badge'
import { cn } from '@/lib/utils'

interface DirectionBadgeProps {
  direction: 'LONG' | 'SHORT'
  className?: string
}

export function DirectionBadge({ direction, className }: DirectionBadgeProps) {
  return (
    <Badge
      variant={direction === 'LONG' ? 'success' : 'destructive'}
      className={className}
    >
      {direction}
    </Badge>
  )
}

interface OutcomeBadgeProps {
  outcome: 'active' | 'tp' | 'sl'
  className?: string
}

export function OutcomeBadge({ outcome, className }: OutcomeBadgeProps) {
  const variant = outcome === 'tp' ? 'success' : outcome === 'sl' ? 'destructive' : 'default'

  return (
    <Badge variant={variant} className={cn('uppercase', className)}>
      {outcome}
    </Badge>
  )
}
