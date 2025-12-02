import { CheckCircle2, Loader2 } from 'lucide-react'
import { cn } from '@/utils/cn'
import type { ThinkingStep as DomainThinkingStep } from '@/types/domain'

interface ThinkingStepsProps {
  steps: DomainThinkingStep[]
  className?: string
}

export function ThinkingSteps({ steps, className }: ThinkingStepsProps) {
  if (!steps || steps.length === 0) return null

  return (
    <div className={cn('space-y-2 text-sm', className)}>
      {steps.map((step, index) => {
        const rawStatus =
          step.status === 'completed' ||
          step.status === 'in-progress' ||
          step.status === 'pending'
            ? step.status
            : 'completed'
        const status = rawStatus
        const label = step.content || step.stage || step.name || `Step ${index + 1}`
        return (
          <div key={step.stage || step.name || index} className="flex items-start gap-2">
            <div className="mt-0.5">
              {status === 'completed' ? (
                <CheckCircle2 className="h-4 w-4 text-green-500" />
              ) : status === 'in-progress' ? (
                <Loader2 className="h-4 w-4 text-blue-500 animate-spin" />
              ) : (
                <div className="h-4 w-4 rounded-full border-2 border-gray-300" />
              )}
            </div>
            <div className="flex-1">
              <p className={cn(
                'text-sm',
                status === 'completed' && 'text-gray-700',
                status === 'in-progress' && 'text-blue-600 font-medium',
                status === 'pending' && 'text-gray-400'
              )}>
                {label}
              </p>
              {step.timestamp && status === 'completed' && (
                <p className="text-xs text-gray-400 mt-0.5">
                  {new Date(step.timestamp).toLocaleTimeString()}
                </p>
              )}
            </div>
          </div>
        )
      })}
    </div>
  )
}
