import { Card, CardContent } from './ui/card'
import { Button } from './ui/button'
import { AlertTriangle, Loader2 } from 'lucide-react'
import { ProgressIndicator } from './ProgressIndicator'
import { SQLPanel } from './SQLPanel'
import { QueryResultsTable } from './QueryResultsTable'
import QueryResults from './QueryResults'
import { SuggestedActions } from './SuggestedActions'
import { ReportGenerator } from './ReportGenerator'
import type { ChatMessage } from '@/stores/chatStore'
import type { ThinkingStep } from '@/types/domain'

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
}: AssistantMessageCardProps) {
  return (
    <div className="space-y-3">
      <Card className="bg-white/80 dark:bg-slate-950/80 border border-emerald-50/60 dark:border-slate-800/80 backdrop-blur-md shadow-sm">
        <CardContent className="pt-3 pr-3 pb-2 pl-3 relative">
          <div className="flex items-start">
            <div className="flex-1">
              <span className="text-[10px] text-gray-400 float-right ml-2">
                {new Date(message.timestamp).toLocaleTimeString('en-US', { hour: 'numeric', minute: '2-digit' })}
              </span>
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
        />
      )}

      {/* SQL query ribbon directly under preamble - skip for conversational responses */}
      {message.toolCall?.metadata?.sql && !message.toolCall.metadata?.isConversational && (
        <div className="mt-3">
          <SQLPanel
            sql={message.toolCall.metadata.sql}
            status={message.toolCall.status === 'completed' ? 'executed' : 'generated'}
            onCopy={() => onCopySQL(message.toolCall!.metadata!.sql)}
            compact={message.toolCall.status === 'completed'}
          />
        </div>
      )}

      {/* Unified controls row: reasoning (left) + chart (right) */}
      {(hasReasoningInfo || (message.toolCall?.status === 'completed' && message.toolCall.result)) && (
        <div className="flex items-center justify-between mt-3 mb-2">
          <div>
            {hasReasoningInfo && (
              <Button
                variant="ghost"
                size="sm"
                onClick={onToggleReasoning}
                className="text-xs"
              >
                {isReasoningOpen ? 'Hide' : 'Show'} reasoning
              </Button>
            )}
          </div>
          <div className="flex items-center gap-3">
            {message.toolCall?.status === 'completed' && message.toolCall.result && (
              <>
                <button
                  className="text-xs underline text-emerald-700 hover:text-emerald-900"
                  onClick={onToggleChart}
                >
                  {isChartOpen ? 'Hide chart' : 'Show chart'}
                </button>
                <ReportGenerator
                  queryResults={[{
                    columns: message.toolCall.result.columns || [],
                    rows: message.toolCall.result.rows || [],
                    row_count: message.toolCall.result.rowCount || message.toolCall.result.row_count,
                  }]}
                  userQueries={[message.content]}
                />
              </>
            )}
          </div>
        </div>
      )}

      {isReasoningOpen && (
        <div className="border rounded-lg p-3 bg-gray-50 dark:bg-slate-900/50 dark:border-slate-700">
          <div className="text-xs font-semibold mb-3 text-gray-700 dark:text-gray-200">Execution Details</div>

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
              onRowAction={(action, ctx) => {
                const cols = ctx.columns || []
                const row = ctx.row
                if (!row || cols.length === 0) return

                const upper = cols.map((c) => c.toUpperCase())
                const priority = ['CUSTOMER_NAME', 'CUSTOMER', 'ACCOUNT_NAME', 'ACCOUNT', 'CLIENT', 'COMPANY', 'PARTNER', 'ORG_NAME']
                let colIdx = upper.findIndex((c) => priority.includes(c))
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

      {/* Error Display */}
      {message.toolCall?.status === 'error' && (
        <Card className="border-red-300 bg-red-50 dark:border-red-500 dark:bg-red-950/70">
          <CardContent className="p-4">
            <div className="flex items-start gap-3">
              <AlertTriangle className="h-5 w-5 text-red-600 dark:text-red-400 flex-shrink-0 mt-0.5" />
              <div>
                <div className="font-semibold text-red-900 dark:text-red-100 mb-1">Query Error</div>
                <div className="text-sm text-red-700 dark:text-red-100 whitespace-pre-wrap break-words">{message.toolCall.error}</div>
                {message.toolCall.metadata?.errorDetails && (
                  <div className="mt-2 text-xs text-red-800 dark:text-red-100 space-y-1">
                    {message.toolCall.metadata.errorDetails.message && (
                      <div>
                        <span className="font-semibold">Cause:</span> {message.toolCall.metadata.errorDetails.message}
                      </div>
                    )}
                    {message.toolCall.metadata.errorDetails.failed_at && (
                      <div>
                        <span className="font-semibold">Stage:</span> {message.toolCall.metadata.errorDetails.failed_at}
                      </div>
                    )}
                    {message.toolCall.metadata.errorDetails.sql_attempted && (
                      <div className="mt-1 text-[11px] text-red-900/80 dark:text-red-100 font-mono bg-red-50/80 dark:bg-red-900/40 border border-red-200 dark:border-red-500 rounded p-2 max-h-32 overflow-auto whitespace-pre-wrap">
                        {message.toolCall.metadata.errorDetails.sql_attempted}
                      </div>
                    )}
                  </div>
                )}
              </div>
            </div>
          </CardContent>
        </Card>
      )}
    </div>
  )
}
