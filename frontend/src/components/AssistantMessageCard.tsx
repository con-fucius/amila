import { Card, CardContent } from './ui/card'
import { cn } from '@/utils/cn'
import { Button } from './ui/button'
import { Loader2 } from 'lucide-react'
import { ProgressIndicator } from './ProgressIndicator'
import { SQLPanel } from './SQLPanel'
import { QueryResultsTable } from './QueryResultsTable'
import QueryResults from './QueryResults'
import { SuggestedActions } from './SuggestedActions'
import { ErrorCard } from './ErrorCard'
import type { ChatMessage } from '@/stores/chatStore'
import type { ThinkingStep } from '@/types/domain'
import { Badge } from '@/components/ui/badge'
import { extractLineage } from '@/utils/sqlAnalyzer'
import { LineageView } from '@/components/LineageView'
import { ExecutionTimeline } from '@/components/ExecutionTimeline'
import { Tooltip, TooltipContent, TooltipProvider, TooltipTrigger } from '@/components/ui/tooltip'
import { usePinnedQueriesStore } from '@/stores/pinnedQueriesStore'
import { useState } from 'react'

interface AssistantMessageCardProps {
  message: ChatMessage
  isReasoningOpen: boolean
  isChartOpen: boolean
  thinkingSteps: ThinkingStep[]
  hasReasoningInfo: boolean
  onToggleReasoning: () => void
  onToggleChart: () => void
  onCopySQL: (sql: string) => void
  onRowActionPrompt: (prompt: string) => void
  onSuggestedQueryClick: (query: string) => void
  isLoading: boolean
  onRetry?: () => void
  onCancelQuery?: (queryId: string) => void
  cancelling?: boolean
}

export function AssistantMessageCard({
  message,
  isReasoningOpen,
  isChartOpen,
  thinkingSteps,
  hasReasoningInfo,
  onToggleReasoning,
  onToggleChart,
  onCopySQL,
  onRowActionPrompt,
  onSuggestedQueryClick,
  isLoading,
  onRetry,
  onCancelQuery,
  cancelling = false,
}: AssistantMessageCardProps) {
  const { addPinnedQuery, pinnedQueries, removePinnedQuery } = usePinnedQueriesStore()
  const [isPinned, setIsPinned] = useState(false)

  // Check if this query is already pinned
  const checkPinned = () => {
    if (!message.toolCall?.result) return false
    return pinnedQueries.some(pq => pq.query === message.content)
  }

  const handlePin = () => {
    if (!message.toolCall?.result) return

    const queryId = `pin-${Date.now()}`

    if (isPinned) {
      // Unpin
      const existing = pinnedQueries.find(pq => pq.query === message.content)
      if (existing) {
        removePinnedQuery(existing.id)
        setIsPinned(false)
      }
    } else {
      // Pin
      addPinnedQuery({
        id: queryId,
        query: message.content,
        sql: message.toolCall.metadata?.sql,
        timestamp: new Date(message.timestamp),
        result: {
          columns: message.toolCall.result.columns || [],
          rows: message.toolCall.result.rows || [],
          rowCount: message.toolCall.result.rowCount || 0
        }
      })
      setIsPinned(true)
    }
  }

  return (
    <div className="space-y-2">
      <Card className="bg-white/80 dark:bg-slate-950/80 border border-emerald-50/60 dark:border-slate-800/80 backdrop-blur-md shadow-sm">
        <CardContent className="pt-3 pr-3 pb-2 pl-3 relative">
          <div className="flex items-start">
            <div className="flex-1">
              <span className="chat-timestamp float-right ml-2 text-gray-400">
                {new Date(message.timestamp).toLocaleTimeString('en-US', { hour: 'numeric', minute: '2-digit' })}
              </span>

              {/* Confidence Indicator */}
              {message.toolCall?.metadata?.sql_confidence !== undefined && (
                <div className="float-right mr-3">
                  <TooltipProvider>
                    <Tooltip>
                      <TooltipTrigger>
                        <Badge variant="outline" className={
                          (message.toolCall.metadata.sql_confidence || 0) > 0.8 ? "text-emerald-600 border-emerald-200 bg-emerald-50" :
                            (message.toolCall.metadata.sql_confidence || 0) > 0.5 ? "text-amber-600 border-amber-200 bg-amber-50" :
                              "text-red-600 border-red-200 bg-red-50"
                        }>
                          {Math.round((message.toolCall.metadata.sql_confidence || 0) * 100)}% Confidence
                        </Badge>
                      </TooltipTrigger>
                      <TooltipContent>
                        <p>Model confidence in the generated SQL</p>
                      </TooltipContent>
                    </Tooltip>
                  </TooltipProvider>
                </div>
              )}

              <p className="text-sm text-gray-700 leading-relaxed whitespace-pre-wrap break-words">{message.content}</p>
            </div>
          </div>
        </CardContent>
      </Card>

      {/* Progress indicator - skip for conversational responses */}
      {message.toolCall?.status === 'pending' && message.toolCall.metadata?.currentState && !message.toolCall.metadata?.isConversational && (
        <ProgressIndicator
          currentState={message.toolCall.metadata.currentState}
          steps={[]}
          schemaData={Array.isArray(message.toolCall.metadata.schemaData) ? message.toolCall.metadata.schemaData : []}
          intermediateData={message.toolCall.metadata.intermediateData}
          thinkingSteps={thinkingSteps}
          visible={true}
          queryId={message.toolCall.metadata.queryId}
          onCancel={onCancelQuery}
          cancelling={cancelling}
        />
      )}

      {/* SQL query ribbon directly under preamble - skip for conversational responses */}
      {message.toolCall?.metadata?.sql && !message.toolCall.metadata?.isConversational && (
        <div className="mt-1">
          <SQLPanel
            sql={message.toolCall.metadata.sql}
            status={message.toolCall.status === 'completed' ? 'executed' : 'generated'}
            onCopy={() => onCopySQL(message.toolCall!.metadata!.sql)}
            compact={message.toolCall.status === 'completed'}
          />
        </div>
      )}

      {isReasoningOpen && (
        <div className="border rounded-lg p-3 bg-gray-50 dark:bg-slate-900/50 dark:border-slate-700">
          <div className="text-xs font-semibold mb-3 text-gray-700 dark:text-gray-200">Execution Details</div>

          {/* Timeline & Lineage in Reasoning Panel */}
          {message.toolCall?.metadata?.sql && (
            <div className="mb-4 space-y-4">
              <LineageView lineage={extractLineage(message.toolCall.metadata.sql)} />
              {message.toolCall.result?.executionTime && (
                <ExecutionTimeline totalTimeMs={message.toolCall.result.executionTime} />
              )}
            </div>
          )}

          {message.toolCall?.status === 'pending' && (
            <div className="space-y-2">
              <div className="text-xs text-emerald-700 dark:text-emerald-300 mb-1 flex items-center gap-1.5">
                <Loader2 className="h-3 w-3 animate-spin" />
                Processing your request...
              </div>
              {message.toolCall.metadata?.currentState && (
                <div className="text-xs text-gray-600 dark:text-gray-400 bg-white dark:bg-slate-800 rounded px-2 py-1 inline-block">
                  Stage: <span className="font-medium">{message.toolCall.metadata.currentState}</span>
                </div>
              )}
              {thinkingSteps.length > 0 && (
                <div className="space-y-1.5 mt-2 border-l-2 border-emerald-200 dark:border-emerald-700 pl-3">
                  {thinkingSteps.map((step, idx) => {
                    const stepName = step.content || step.name || step.stage || `Step ${idx + 1}`
                    const stepStatus = step.status || 'pending'

                    return (
                      <div key={idx} className="flex items-start gap-2 text-xs">
                        <span className={`flex-shrink-0 ${stepStatus === 'failed' ? 'text-red-500' : stepStatus === 'completed' ? 'text-green-500' : stepStatus === 'in-progress' ? 'text-blue-500' : 'text-gray-400'}`}>
                          {stepStatus === 'failed' ? 'x' : stepStatus === 'completed' ? '+' : stepStatus === 'in-progress' ? '*' : 'o'}
                        </span>
                        <span className="text-gray-700 dark:text-gray-300">{stepName}</span>
                        {step.error && <span className="text-red-500 text-[10px]">({step.error})</span>}
                      </div>
                    )
                  })}
                </div>
              )}
              {thinkingSteps.length === 0 && (
                <div className="text-xs text-gray-500 dark:text-gray-400 italic">
                  Waiting for execution steps...
                </div>
              )}
            </div>
          )}

          {message.toolCall?.status === 'pending' && message.toolCall?.metadata?.currentState === 'pending_approval' && (
            <div className="space-y-2">
              <div className="text-xs text-orange-600 mb-1"> Awaiting Approval</div>
              {message.toolCall.metadata?.riskLevel && (
                <div className="text-xs text-gray-600">
                  Risk Level: {message.toolCall.metadata.riskLevel}
                </div>
              )}
              {thinkingSteps.length > 0 && (
                <div className="space-y-1 mt-2">
                  {thinkingSteps.map((step, idx) => {
                    const stepName = step.name || step.stage || step.content || `Step ${idx + 1}`
                    const stepStatus = step.status || 'completed'

                    return (
                      <div key={idx} className="flex items-start gap-1.5 text-xs">
                        <span className={stepStatus === 'failed' ? 'text-red-600' : stepStatus === 'completed' ? 'text-green-600' : 'text-gray-600'}>
                          {stepStatus === 'failed' ? '' : stepStatus === 'completed' ? '' : ''}
                        </span>
                        <span className="text-gray-700">{stepName}</span>
                        {step.error && <span className="text-red-600">- {step.error}</span>}
                      </div>
                    )
                  })}
                </div>
              )}
            </div>
          )}

          {message.toolCall?.status === 'error' && (
            <div className="space-y-2">
              <div>
                <div className="text-xs font-semibold text-red-600 mb-1"> Query Failed</div>
                <div className="text-xs text-red-700">{message.toolCall.error || 'Unknown error occurred'}</div>
              </div>

              {message.toolCall.metadata?.currentState && (
                <div className="text-xs text-gray-600">
                  Failed at stage: <span className="font-semibold">{message.toolCall.metadata.currentState}</span>
                </div>
              )}

              {!message.toolCall.metadata?.sql && (
                <div className="text-xs text-gray-500 italic">
                  SQL was not generated (failed before SQL generation stage)
                </div>
              )}

              {thinkingSteps.length > 0 && (
                <div className="mt-2">
                  <div className="text-xs font-semibold text-gray-700 mb-1">Execution Steps:</div>
                  <div className="space-y-1">
                    {thinkingSteps.map((step, idx) => {
                      const stepName = step.name || step.stage || step.content || `Step ${idx + 1}`
                      const stepStatus = step.status || 'failed'

                      return (
                        <div key={idx} className="flex items-start gap-1.5 text-xs">
                          <span className={stepStatus === 'failed' ? 'text-red-600' : stepStatus === 'completed' ? 'text-green-600' : 'text-gray-600'}>
                            {stepStatus === 'failed' ? '' : stepStatus === 'completed' ? '' : ''}
                          </span>
                          <span className="text-gray-700">{stepName}</span>
                          {step.error && <span className="text-red-600">- {step.error}</span>}
                        </div>
                      )
                    })}
                  </div>
                </div>
              )}
            </div>
          )}

          {message.toolCall?.status === 'completed' && (
            <div className="space-y-2">
              {thinkingSteps.length > 0 ? (
                <div className="space-y-1.5 border-l-2 border-green-200 dark:border-green-700 pl-3">
                  {thinkingSteps.map((step, idx) => {
                    const stepName = step.content || step.name || step.stage || `Step ${idx + 1}`
                    const stepStatus = step.status || 'completed'

                    return (
                      <div key={idx} className="flex items-start gap-2 text-xs">
                        <span className={`flex-shrink-0 ${stepStatus === 'failed' ? 'text-red-500' : 'text-green-500'}`}>
                          {stepStatus === 'failed' ? 'x' : '+'}
                        </span>
                        <span className="text-gray-700 dark:text-gray-300">{stepName}</span>
                        {step.error && <span className="text-red-500 text-[10px]">({step.error})</span>}
                      </div>
                    )
                  })}
                </div>
              ) : (
                <div className="flex items-center gap-2 text-xs bg-green-50 dark:bg-green-900/20 rounded px-2 py-1.5">
                  <span className="text-green-600 dark:text-green-400">+</span>
                  <span className="text-gray-700 dark:text-gray-300">
                    Executed successfully: <span className="font-medium">{message.toolCall?.result?.rowCount || message.toolCall?.result?.row_count || 0}</span> rows in <span className="font-medium">{message.toolCall?.result?.executionTime || message.toolCall?.result?.execution_time_ms || 0}ms</span>
                  </span>
                </div>
              )}
            </div>
          )}

          {!message.toolCall?.status && (
            <div className="text-xs text-gray-500 italic">
              No execution data available - query may not have reached the backend
            </div>
          )}
        </div>
      )}

      {/* Loading skeleton - skip for conversational responses */}
      {message.toolCall?.status === 'pending' && !message.toolCall.metadata?.isConversational && (
        <Card className="border-emerald-100/70 bg-white/70 dark:border-emerald-500/30 dark:bg-slate-950/70 backdrop-blur-md shadow-sm">
          <CardContent className="p-3">
            <div className="flex items-center justify-end mb-2">
              <div className="flex items-center gap-2 text-xs text-emerald-700 dark:text-emerald-300">
                <Loader2 className="h-3.5 w-3.5 animate-spin" />
                <span>Processing your request...</span>
              </div>
            </div>
            <div className="border rounded-lg overflow-hidden">
              <div className="animate-pulse divide-y divide-gray-200 dark:divide-slate-800">
                <div className="h-8 bg-gray-100 dark:bg-slate-800" />
                {Array.from({ length: 6 }).map((_, i) => (
                  <div key={i} className="h-7 bg-white dark:bg-slate-900/70" />
                ))}
              </div>
            </div>
          </CardContent>
        </Card>
      )}

      {message.toolCall?.status === 'completed' && message.toolCall.result && (
        <>
          {!isChartOpen && (
            <QueryResultsTable
              columns={message.toolCall.result.columns || []}
              rows={message.toolCall.result.rows || []}
              executionTime={message.toolCall.result.executionTime}
              rowCount={message.toolCall.result.rowCount ?? 0}
              timestamp={message.timestamp}
              assistantText={message.content}
              isPinned={checkPinned()}
              onPin={handlePin}
              isChartOpen={isChartOpen}
              onToggleChart={onToggleChart}
              isReasoningOpen={isReasoningOpen}
              onToggleReasoning={onToggleReasoning}
              isLoading={isLoading}
              onRowAction={(action, ctx) => {
                const cols = ctx.columns || []
                const row = ctx.row
                if (!row || cols.length === 0) return

                const upper = cols.map((c: string) => c.toUpperCase())
                const priority = ['CUSTOMER_NAME', 'CUSTOMER', 'ACCOUNT_NAME', 'ACCOUNT', 'CLIENT', 'COMPANY', 'PARTNER', 'ORG_NAME']
                let colIdx = upper.findIndex((c: string) => priority.includes(c))
                if (colIdx === -1) colIdx = 0

                const colName = cols[colIdx] || cols[0]
                let rawValue: any
                if (Array.isArray(row)) {
                  rawValue = row[colIdx]
                } else if (typeof row === 'object' && row !== null) {
                  rawValue = (row as any)[colName] ?? (row as any)[colIdx]
                }

                if (rawValue === undefined || rawValue === null || rawValue === '') return
                const value = String(rawValue)

                const prompt =
                  action === 'filter'
                    ? `Filter the current results to only rows where ${colName} = "${value}".`
                    : `Drill down on ${colName} = "${value}" to show more detailed metrics and trends.`

                if (!prompt.trim() || isLoading) return
                onRowActionPrompt(prompt)
              }}
            />
          )}
          {isChartOpen && (
            <QueryResults
              data={{ columns: message.toolCall.result.columns, rows: message.toolCall.result.rows }}
              loading={false}
              error={null}
              viewMode="chart"
            />
          )}

          {(message.toolCall.metadata?.insights || message.toolCall.metadata?.suggestedQueries) && (
            <SuggestedActions
              insights={message.toolCall.metadata.insights}
              suggestedQueries={message.toolCall.metadata.suggestedQueries}
              onQueryClick={onSuggestedQueryClick}
            />
          )}
        </>
      )}

      {/* Error Display - Self-Heal UI with Retry */}
      {message.toolCall?.status === 'error' && (
        <ErrorCard
          title="Query Error"
          message={message.toolCall.error || 'An unknown error occurred'}
          severity="error"
          details={{
            code: message.toolCall.metadata?.errorDetails?.code,
            stage: message.toolCall.metadata?.errorDetails?.failed_at || message.toolCall.metadata?.currentState,
            sql: message.toolCall.metadata?.errorDetails?.sql_attempted || message.toolCall.metadata?.sql,
            correlationId: message.toolCall.metadata?.correlationId,
            timestamp: new Date().toISOString(),
          }}
          onRetry={onRetry}
          retryLabel="Retry Query"
          isRetrying={isLoading}
        />
      )}
    </div>
  )
}
