import { CheckCircle2, Loader2, XCircle, Clock } from 'lucide-react'
import { cn } from '@/utils/cn'
import type { ThinkingStep as DomainThinkingStep } from '@/types/domain'

export type NodeStatus = 'pending' | 'in-progress' | 'completed' | 'failed'

export interface NodeExecution {
  name: string
  status: NodeStatus
  start_time?: string
  end_time?: string
  thinking_steps?: DomainThinkingStep[]
  error?: string
}

interface ReasoningPanelProps {
  current_node?: string
  node_history: NodeExecution[]
  className?: string
}

const NODE_DISPLAY_NAMES: Record<string, string> = {
  understand: 'Understand Intent',
  retrieve_context: 'Retrieve Context',
  generate_sql: 'Generate SQL',
  execute_query: 'Execute Query',
  analyze_results: 'Analyze Results'
}

const NODE_ORDER = ['understand', 'retrieve_context', 'generate_sql', 'execute_query', 'analyze_results']

export function ReasoningPanel({ current_node, node_history, className }: ReasoningPanelProps) {
  if (!node_history || node_history.length === 0) {
    return null
  }

  // Build status map from history
  const statusMap: Record<string, NodeExecution> = {}
  node_history.forEach(node => {
    statusMap[node.name] = node
  })

  // Determine status for each node in pipeline
  const getNodeStatus = (nodeName: string): NodeStatus => {
    if (statusMap[nodeName]) {
      return statusMap[nodeName].status
    }
    if (current_node === nodeName) {
      return 'in-progress'
    }
    // If current node comes after this node in pipeline, mark as pending
    const currentIndex = NODE_ORDER.indexOf(current_node || '')
    const nodeIndex = NODE_ORDER.indexOf(nodeName)
    if (currentIndex > nodeIndex) {
      return 'completed'  // Should have been completed
    }
    return 'pending'
  }

  const renderNodeIcon = (status: NodeStatus) => {
    switch (status) {
      case 'completed':
        return <CheckCircle2 className="h-5 w-5 text-green-500" />
      case 'in-progress':
        return <Loader2 className="h-5 w-5 text-blue-500 animate-spin" />
      case 'failed':
        return <XCircle className="h-5 w-5 text-red-500" />
      case 'pending':
      default:
        return <Clock className="h-5 w-5 text-gray-300" />
    }
  }

  return (
    <div className={cn('space-y-3', className)}>
      <div className="flex items-center gap-2 text-sm font-medium text-gray-700 dark:text-gray-200">
        <span>Reasoning Pipeline</span>
      </div>

      {/* Node Pipeline */}
      <div className="space-y-2">
        {NODE_ORDER.map((nodeName, index) => {
          const status = getNodeStatus(nodeName)
          const nodeData = statusMap[nodeName]
          const displayName = NODE_DISPLAY_NAMES[nodeName] || nodeName

          return (
            <div key={nodeName} className="relative">
              {/* Connector Line */}
              {index < NODE_ORDER.length - 1 && (
                <div className="absolute left-[10px] top-[28px] w-[2px] h-[calc(100%+8px)] bg-gray-200 dark:bg-gray-700" />
              )}

              {/* Node Row */}
              <div className="flex items-start gap-3">
                <div className="relative z-10 bg-white dark:bg-slate-950">
                  {renderNodeIcon(status)}
                </div>
                <div className="flex-1 min-w-0">
                  <div className={cn(
                    'text-sm font-medium',
                    status === 'completed' && 'text-gray-700 dark:text-gray-200',
                    status === 'in-progress' && 'text-blue-600 dark:text-blue-400',
                    status === 'failed' && 'text-red-600 dark:text-red-400',
                    status === 'pending' && 'text-gray-400 dark:text-gray-500'
                  )}>
                    {displayName}
                  </div>

                  {/* Thinking Steps for this node */}
                  {nodeData?.thinking_steps && nodeData.thinking_steps.length > 0 && (
                    <div className="mt-2 space-y-1">
                      {nodeData.thinking_steps.map((step, idx) => (
                        <div key={step.stage || idx} className="text-xs text-gray-600 dark:text-gray-400 pl-3 border-l-2 border-gray-200 dark:border-gray-700">
                          {step.content || step.name || `Step ${idx + 1}`}
                        </div>
                      ))}
                    </div>
                  )}

                  {/* Error Message */}
                  {nodeData?.error && (
                    <div className="mt-2 text-xs text-red-600 dark:text-red-400 bg-red-50 dark:bg-red-950/20 p-2 rounded border border-red-200 dark:border-red-800">
                      {nodeData.error}
                    </div>
                  )}

                  {/* Timing */}
                  {nodeData?.start_time && nodeData?.end_time && status === 'completed' && (
                    <div className="mt-1 text-xs text-gray-400">
                      {calculateDuration(nodeData.start_time, nodeData.end_time)}
                    </div>
                  )}
                </div>
              </div>
            </div>
          )
        })}
      </div>
    </div>
  )
}

// Helper to calculate duration
function calculateDuration(start: string, end: string): string {
  try {
    const startTime = new Date(start).getTime()
    const endTime = new Date(end).getTime()
    const durationMs = endTime - startTime
    if (durationMs < 1000) return `${durationMs}ms`
    return `${(durationMs / 1000).toFixed(1)}s`
  } catch {
    return ''
  }
}
