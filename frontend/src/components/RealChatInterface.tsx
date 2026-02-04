import { useRef, useEffect, useState } from 'react'
import { Button } from './ui/button'

import { QuerySuggestionsSimple } from './QuerySuggestionsSimple'
import { QueryValidationIndicator } from './QueryValidationIndicator'
import { HITLApprovalDialog } from './HITLApprovalDialog'
import { ChatHistoryDrawer } from './ChatHistoryDrawer'
import { SessionCostTicker } from './SessionCostTicker'
import { DrillDownBreadcrumbs } from './DrillDownBreadcrumbs'
import { AssistantMessageCard } from './AssistantMessageCard'
import { UserMessageBubble } from './UserMessageBubble'
import { ChatTopBar } from './ChatTopBar'
import { ClarificationDialog } from './ClarificationDialog'
import { ErrorCard } from './ErrorCard'
import { useQuerySubmission } from '@/hooks/useQuerySubmission'
import { useMessages, useIsLoading, useChats, useCurrentChatId, useChatActions } from '@/stores/chatStore'
import { cn } from '@/utils/cn'
import { useQueryHistory } from '@/hooks/useQueryHistory'
import type { NormalizedHistoryItem } from '@/utils/history'
import { extractThinkingSteps } from '@/utils/thinking'
import { classifyInitialQueryResponse } from '@/utils/queryContract'
import { useChatUIState } from '@/hooks/useChatUIState'
import { useApprovalFlow } from '@/hooks/useApprovalFlow'
import { useClarificationFlow } from '@/hooks/useClarificationFlow'
import { UI_STRINGS } from '@/constants/strings'
import { Input } from './ui/input'
import { Send, Sparkles } from 'lucide-react'
import { apiService } from '@/services/apiService'

export function RealChatInterface() {
  // Global Store State
  const messages = useMessages()
  const storeLoading = useIsLoading()
  const {
    addMessage,
    updateMessage,
    mergeMessage,
    setLoading,
    createChat,
    switchChat,
    autoNameChatFromQuery
  } = useChatActions()
  const chats = useChats()
  const currentChatId = useCurrentChatId()
  const currentChat = chats.find(c => c.id === currentChatId) || null

  // Local UI State
  const {
    input, setInput,
    showHistory, setShowHistory,
    reasoningOpen,
    chartOpen,
    showSuggestions, setShowSuggestions,
    databaseType, setDatabaseType,
    toggleReasoning, toggleChart
  } = useChatUIState()

  const { items: historyItems, setItems: setHistoryItems, loadHistory } = useQueryHistory(currentChat?.sessionId ?? null, 3)
  const messagesEndRef = useRef<HTMLDivElement>(null)
  const lastUserQueryRef = useRef<string>("")

  // Backend Hooks
  const { submitQuery, isLoading, error, currentState, response, retryConnection, cancelQuery } = useQuerySubmission()

  // Cancellation state
  const [cancelling, setCancelling] = useState(false)
  const [enhancing, setEnhancing] = useState(false)
  const [enhanceEnabled, setEnhanceEnabled] = useState(
    localStorage.getItem('enableQueryEnhancement') !== 'false'
  )

  // Handle query cancellation
  const handleCancelQuery = async () => {
    setCancelling(true)
    try {
      await cancelQuery()
      // Update the last message to show cancellation
      const lastMessage = messages[messages.length - 1]
      if (lastMessage && lastMessage.type === 'assistant') {
        mergeMessage(lastMessage.id, (prev) => ({
          ...prev,
          content: 'Query cancelled by user',
          toolCall: {
            ...(prev.toolCall || { name: 'query_processor', params: { query: lastUserQueryRef.current || '' } }),
            status: 'error',
            error: 'Query cancelled by user',
          },
        }))
      }
    } catch (err: any) {
      console.error('Failed to cancel query:', err)
    } finally {
      setCancelling(false)
    }
  }

  // Dialog Flows
  const {
    approvalDialog,
    setApprovalDialog,
    handleApproveQuery,
    handleRejectQuery
  } = useApprovalFlow()

  const {
    clarificationDialog,
    setClarificationDialog,
    handleClarificationSubmit
  } = useClarificationFlow(setApprovalDialog, messages)

  // Handlers
  const handleRerunQuery = (query: string) => {
    setInput(query)
    setShowHistory(false)
    setTimeout(() => {
      if (query.trim()) {
        setInput('')
        handleSendQuery(query)
      }
    }, 100)
  }

  const handleEditAndRun = (query: string) => {
    setInput(query)
    setShowHistory(false)
    setTimeout(() => {
      const inputElement = document.querySelector('input.chat-input') as HTMLInputElement
      if (inputElement) {
        inputElement.focus()
        inputElement.setSelectionRange(query.length, query.length)
      }
    }, 100)
  }

  const handleSendQuery = async (queryText: string) => {
    if (!queryText.trim() || storeLoading) return

    const userQuery = queryText.trim()
    lastUserQueryRef.current = userQuery

    const newHistoryItem: NormalizedHistoryItem = {
      id: Date.now().toString(),
      query: userQuery,
      status: 'pending' as const,
      timestamp: new Date()
    }
    setHistoryItems((prev: NormalizedHistoryItem[]) => [newHistoryItem, ...prev.slice(0, 49)])

    if (currentChat && (currentChat.promptCount >= 20 || currentChat.promptCount >= 15)) {
      const newId = createChat('New chat')
      switchChat(newId)
    }

    autoNameChatFromQuery(userQuery)

    addMessage({
      type: 'user',
      content: userQuery,
    })

    addMessage({
      type: 'assistant',
      content: 'Processing your request...',
      toolCall: {
        name: 'query_processor',
        params: { query: userQuery },
        status: 'pending',
      },
    })

    setLoading(true)

    try {
      await submitQuery(userQuery, currentChat?.sessionId, databaseType)
      setHistoryItems((prev: NormalizedHistoryItem[]) =>
        prev.map((item: NormalizedHistoryItem) =>
          item.id === newHistoryItem.id
            ? { ...item, status: 'success' as const }
            : item
        )
      )
    } catch (err: any) {
      setHistoryItems((prev: NormalizedHistoryItem[]) =>
        prev.map((item: NormalizedHistoryItem) =>
          item.id === newHistoryItem.id
            ? { ...item, status: 'error' as const }
            : item
        )
      )
      updateMessage(messages[messages.length - 1].id, {
        content: `Error: ${err.message || UI_STRINGS.ERROR_PROCESSING}`,
        toolCall: {
          name: 'query_processor',
          params: { query: userQuery },
          status: 'error',
        },
      })
    } finally {
      setLoading(false)
    }
  }

  const handleSend = async () => {
    if (!input.trim() || isLoading) return
    const query = input.trim()
    setInput('')
    await handleSendQuery(query)
  }

  const handleEnhance = async () => {
    if (!input.trim() || isLoading || enhancing) return
    setEnhancing(true)
    try {
      const history = messages
        .slice(-6)
        .map((m) => ({ role: m.type === 'assistant' ? 'assistant' : 'user', content: m.content }))
      const resp = await apiService.enhanceQuery({
        query: input.trim(),
        conversation_history: history,
        database_type: databaseType as any,
      })
      if (resp?.enhanced_query) {
        setInput(resp.enhanced_query)
      }
    } catch (err) {
      console.error('Enhancement failed:', err)
    } finally {
      setEnhancing(false)
    }
  }

  const handleCopySQL = (sql: string) => {
    navigator.clipboard.writeText(sql)
  }

  // Effects

  // Auto-scroll
  useEffect(() => {
    messagesEndRef.current?.scrollIntoView({ behavior: 'smooth' })
  }, [messages, currentState])

  // Handle query response outcomes
  useEffect(() => {
    if (!response) return

    const outcome = classifyInitialQueryResponse(response)

    if (outcome.kind === 'needs_approval') {
      const resp = outcome.response
      const lastMessage = messages[messages.length - 1]
      if (lastMessage) {
        setApprovalDialog({
          open: true,
          messageId: lastMessage.id,
          queryId: resp.query_id,
          query: lastUserQueryRef.current || input,
          sql: resp.sql_query || '',
          riskLevel: (resp as any)?.approval_context?.risk_level || 'MEDIUM',
          originalSQL: (resp as any)?.original_sql,
          riskReasons: (resp as any)?.risk_reasons,
          sqlExplanation: (resp as any)?.sql_explanation,
          queryPlan: (resp as any)?.query_plan,
          rlsExplanation: (resp as any)?.rls_explanation,
          approvalContext: (resp as any)?.approval_context,
        })
        mergeMessage(lastMessage.id, (prev) => ({
          ...prev,
          toolCall: {
            ...(prev.toolCall || { name: 'query_processor', params: { query: lastUserQueryRef.current || '' }, status: 'pending' }),
            metadata: {
              ...(prev.toolCall?.metadata || {}),
              sql: resp.sql_query || prev.toolCall?.metadata?.sql,
              thinkingSteps: (resp as any)?.llm_metadata?.thinking_steps || prev.toolCall?.metadata?.thinkingSteps,
            },
          }
        }))
      }
    } else if (outcome.kind === 'clarification_needed') {
      const resp = outcome.response
      setClarificationDialog({
        open: true,
        queryId: resp.query_id,
        originalQuery: lastUserQueryRef.current || input,
        message: (resp as any).clarification_message || UI_STRINGS.CLARIFICATION_DEFAULT_MSG,
        details: (resp as any).clarification_details,
        databaseType: databaseType as any,
      })
    } else if (outcome.kind === 'conversational') {
      // Handle conversational responses (greetings, help, meta questions)
      const conversationalMessage = outcome.message
      const lastMessage = messages[messages.length - 1]

      if (lastMessage && lastMessage.type === 'assistant') {
        mergeMessage(lastMessage.id, (prev) => ({
          ...prev,
          content: conversationalMessage,
          toolCall: {
            ...(prev.toolCall || { name: 'query_processor', params: { query: lastUserQueryRef.current || '' } }),
            status: 'completed',
            metadata: {
              ...(prev.toolCall?.metadata || {}),
              isConversational: true,
              intent: (response as any)?.intent || (response as any)?.llm_metadata?.intent,
              structured_intent: (response as any).structured_intent ?? prev.toolCall?.metadata?.structured_intent,
              originalQuery: lastUserQueryRef.current || prev.toolCall?.metadata?.originalQuery,
            },
          },
        }))
      }
      setApprovalDialog(null)
      setClarificationDialog(null)
    } else if (outcome.kind === 'success') {
      const resp = outcome.response
      const normalized = outcome.normalizedResult
      const lastMessage = messages[messages.length - 1]

      if (lastMessage && lastMessage.type === 'assistant') {
        mergeMessage(lastMessage.id, (prev) => ({
          ...prev,
          toolCall: {
            ...(prev.toolCall || { name: 'query_processor', params: { query: lastUserQueryRef.current || '' }, status: 'pending' }),
            status: 'completed',
            result: normalized,
            metadata: {
              ...(prev.toolCall?.metadata || {}),
              resultRef: resp.result_ref ? {
                queryId: resp.result_ref.query_id,
                rowCount: resp.result_ref.row_count,
                columns: resp.result_ref.columns,
                cacheStatus: resp.result_ref.cache_status,
              } : prev.toolCall?.metadata?.resultRef,
              resultsTruncated: resp.results_truncated ?? prev.toolCall?.metadata?.resultsTruncated,
              sql: resp.sql_query || prev.toolCall?.metadata?.sql,
              insights: resp.insights ?? prev.toolCall?.metadata?.insights,
              suggestedQueries: resp.suggested_queries ?? prev.toolCall?.metadata?.suggestedQueries,
              sqlExplanation: resp.sql_explanation ?? prev.toolCall?.metadata?.sqlExplanation,
              queryPlan: (resp as any).query_plan ?? prev.toolCall?.metadata?.queryPlan,
              rlsExplanation: (resp as any).rls_explanation ?? prev.toolCall?.metadata?.rlsExplanation,
              resultAnalysis: (resp as any).result_analysis ?? prev.toolCall?.metadata?.resultAnalysis,
              thinkingSteps: (resp as any)?.llm_metadata?.thinking_steps ?? prev.toolCall?.metadata?.thinkingSteps,
              structured_intent: (resp as any).structured_intent ?? prev.toolCall?.metadata?.structured_intent,
              originalQuery: lastUserQueryRef.current || prev.toolCall?.metadata?.originalQuery,
            },
          },
        }))
      }
      setApprovalDialog(null)
      setClarificationDialog(null)
    } else if (outcome.kind === 'error') {
      const resp = outcome.response
      const errorMessage = outcome.errorMessage
      const lastMessage = messages[messages.length - 1]
      if (lastMessage && lastMessage.type === 'assistant') {
        mergeMessage(lastMessage.id, (prev) => ({
          ...prev,
          toolCall: {
            ...(prev.toolCall || { name: 'query_processor', params: { query: lastUserQueryRef.current || '' } }),
            status: 'error',
            error: errorMessage,
            metadata: {
              ...(prev.toolCall?.metadata || {}),
              sql: resp.sql_query || prev.toolCall?.metadata?.sql,
              currentState: (resp as any)?.llm_metadata?.failed_stage || prev.toolCall?.metadata?.currentState,
              thinkingSteps: (resp as any)?.llm_metadata?.thinking_steps || prev.toolCall?.metadata?.thinkingSteps,
              errorDetails: (resp as any)?.llm_metadata?.error_details,
              error_taxonomy: (resp as any)?.llm_metadata?.error_details?.error_taxonomy,
            },
          },
        }))
      }
      setApprovalDialog(null)
      setClarificationDialog(null)
    }
    setLoading(false)
  }, [response])

  // Backend error reflection
  useEffect(() => {
    if (error) {
      const lastMessage = messages[messages.length - 1]
      if (lastMessage && lastMessage.type === 'assistant') {
        mergeMessage(lastMessage.id, (prev) => ({
          ...prev,
          toolCall: {
            ...(prev.toolCall || { name: 'query_processor', params: { query: lastUserQueryRef.current || '' } }),
            status: 'error',
            error,
          },
        }))
        setLoading(false)
      }
    }
  }, [error])

  // SSE state updates
  useEffect(() => {
    if (currentState) {
      const lastMessage = messages[messages.length - 1]
      if (lastMessage && lastMessage.type === 'assistant') {
        mergeMessage(lastMessage.id, (prev) => ({
          ...prev,
          toolCall: {
            ...(prev.toolCall || { name: 'query_processor', params: { query: lastUserQueryRef.current || '' } }),
            status: (prev.toolCall?.status as any) || 'pending',
            metadata: {
              ...(prev.toolCall?.metadata || {}),
              currentState: currentState.state,
              sql: currentState.sql ?? prev.toolCall?.metadata?.sql,
              thinkingSteps: currentState.thinking_steps ?? prev.toolCall?.metadata?.thinkingSteps,
              schemaData: currentState.schema_data ?? prev.toolCall?.metadata?.schemaData,
              intermediateData: currentState.intermediate_data ?? prev.toolCall?.metadata?.intermediateData,
              queryId: response?.query_id ?? prev.toolCall?.metadata?.queryId,
              resultRef: (currentState as any).result_ref ?? prev.toolCall?.metadata?.resultRef,
              resultsTruncated: (currentState as any).results_truncated ?? prev.toolCall?.metadata?.resultsTruncated,
              sqlExplanation: (currentState as any).sql_explanation ?? prev.toolCall?.metadata?.sqlExplanation,
              queryPlan: (currentState as any).query_plan ?? prev.toolCall?.metadata?.queryPlan,
              rlsExplanation: (currentState as any).rls_explanation ?? prev.toolCall?.metadata?.rlsExplanation,
              structured_intent: (currentState as any).structured_intent ?? prev.toolCall?.metadata?.structured_intent,
              originalQuery: lastUserQueryRef.current || prev.toolCall?.metadata?.originalQuery,
            },
          },
        }))
      }
    }
  }, [currentState, response?.query_id])

  // SSE Pending Approval Trigger
  useEffect(() => {
    if (currentState?.state && (currentState.state === 'pending_approval' || currentState.state === 'PENDING_APPROVAL') && (response?.needs_approval ?? true)) {
      const lastMessage = messages[messages.length - 1]
      if (lastMessage && !approvalDialog) {
        setApprovalDialog({
          open: true,
          messageId: lastMessage.id,
          queryId: (response?.query_id || ''),
          query: lastUserQueryRef.current || input,
          sql: response?.sql_query || '',
          riskLevel: (response as any)?.approval_context?.risk_level || 'MEDIUM',
          originalSQL: (response as any)?.original_sql,
          riskReasons: (response as any)?.risk_reasons,
          sqlExplanation: (response as any)?.sql_explanation,
          queryPlan: (response as any)?.query_plan,
          rlsExplanation: (response as any)?.rls_explanation,
          approvalContext: (response as any)?.approval_context,
        })
      }
    }
  }, [currentState?.state])

  useEffect(() => {
    const syncEnhanceSetting = () => {
      setEnhanceEnabled(localStorage.getItem('enableQueryEnhancement') !== 'false')
    }
    window.addEventListener('storage', syncEnhanceSetting)
    window.addEventListener('settings-changed', syncEnhanceSetting)
    return () => {
      window.removeEventListener('storage', syncEnhanceSetting)
      window.removeEventListener('settings-changed', syncEnhanceSetting)
    }
  }, [])

  useEffect(() => {
    const focusInput = () => {
      const inputElement = document.querySelector('input.chat-input') as HTMLInputElement | null
      if (inputElement) inputElement.focus()
    }
    const cancel = () => {
      if (isLoading) handleCancelQuery()
    }
    window.addEventListener('focus-chat-input', focusInput as EventListener)
    window.addEventListener('query:cancel', cancel as EventListener)
    return () => {
      window.removeEventListener('focus-chat-input', focusInput as EventListener)
      window.removeEventListener('query:cancel', cancel as EventListener)
    }
  }, [isLoading])
  return (
    <div className="h-screen flex flex-col overflow-hidden bg-gradient-to-b from-emerald-50 via-slate-50 to-slate-100 dark:from-slate-950 dark:via-slate-950 dark:to-emerald-950">
      {/* Top Bar */}
      <ChatTopBar
        isLoading={isLoading}
        storeLoading={storeLoading}
        databaseType={databaseType}
        onDatabaseTypeChange={setDatabaseType}
        onOpenHistory={async () => {
          await loadHistory()
          setShowHistory(true)
        }}
      />

      {/* Drill-Down Breadcrumbs */}
      <DrillDownBreadcrumbs />

      {/* Messages Area */}
      <div className="flex-1 overflow-y-auto overflow-x-hidden px-3 sm:px-6 py-3 sm:py-4 space-y-4">

          {messages.map((message, index) => {
            const isReasoningOpen = !!reasoningOpen[message.id]
            const isChartOpen = !!chartOpen[message.id]
            const thinkingStepsArray = extractThinkingSteps((message as any).toolCall?.metadata || (message as any).toolCall)
            const hasReasoningInfo = !!(message as any).toolCall?.metadata?.sql || thinkingStepsArray.length > 0
            
            const isNewChat = index > 0 && messages[index - 1].type === 'assistant' && message.type === 'user'

            return (
              <div
                key={message.id}
                className={cn(
                  message.type === 'user' ? 'flex justify-end' : '',
                  isNewChat && 'mt-6'
                )}
              >
                {message.type === 'user' ? (
                  <UserMessageBubble message={message} />
                ) : (
                  <AssistantMessageCard
                    message={message as any}
                    isReasoningOpen={isReasoningOpen}
                    isChartOpen={isChartOpen}
                    thinkingSteps={thinkingStepsArray as any}
                    hasReasoningInfo={hasReasoningInfo}
                    onToggleReasoning={() => toggleReasoning(message.id)}
                    onToggleChart={() => toggleChart(message.id)}
                    onCopySQL={handleCopySQL}
                    onRowActionPrompt={handleSendQuery}
                    onSuggestedQueryClick={(query) => {
                      setInput(query)
                      handleSend()
                    }}
                    isLoading={isLoading}
                    onRetry={() => {
                      const originalQuery = message.toolCall?.params?.query
                      if (originalQuery && typeof originalQuery === 'string') {
                        handleSendQuery(originalQuery)
                      }
                    }}
                    onCancelQuery={handleCancelQuery}
                    cancelling={cancelling}
                  />
                )}
              </div>
            )
          })}

          {/* Connection Error with Retry */}
          {error && error.includes('Connection to server lost') && (
            <ErrorCard
              title={UI_STRINGS.CONNECTION_LOST_TITLE}
              message={error}
              severity="warning"
              onRetry={retryConnection}
              retryLabel={UI_STRINGS.RETRY_CONNECTION}
            />
          )}

          <div ref={messagesEndRef} />
        </div>

      {/* Input Area */}
      <div className="p-3 sm:p-4 border-t border-emerald-100/60 dark:border-emerald-500/30 bg-white/80 dark:bg-slate-950/70 backdrop-blur-xl shadow-md flex-shrink-0">
        <div className="max-w-5xl mx-auto relative">
          <QuerySuggestionsSimple
            show={showSuggestions}
            onSuggestionClick={(suggestion) => {
              setInput(suggestion)
              setShowSuggestions(false)
            }}
            currentInput={input}
          />
          <div className="flex gap-3">
            <Input
              value={input}
              onChange={(e) => setInput(e.target.value)}
              onKeyDown={(e) => e.key === 'Enter' && !e.shiftKey && handleSend()}
              onFocus={() => setShowSuggestions(true)}
              onBlur={() => setTimeout(() => setShowSuggestions(false), 200)}
              placeholder={UI_STRINGS.INPUT_PLACEHOLDER}
              className="flex-1 chat-input bg-white/80 dark:bg-slate-900/70 border border-emerald-100/60 dark:border-slate-700/80 backdrop-blur-md"
              disabled={isLoading}
            />
            {enhanceEnabled && (
              <Button
                onClick={handleEnhance}
                disabled={isLoading || enhancing || !input.trim()}
                variant="outline"
                className="border-emerald-200/70 text-emerald-700 hover:text-emerald-800 hover:border-emerald-300 bg-white/80 dark:bg-slate-900/70 dark:text-emerald-200 dark:border-emerald-500/30"
                title="Enhance query"
              >
                <Sparkles className="h-4 w-4 mr-2" />
                {enhancing ? 'Enhancing' : 'Enhance'}
              </Button>
            )}
            <Button
              onClick={handleSend}
              disabled={isLoading || !input.trim()}
              className="bg-gradient-to-r from-emerald-500 via-emerald-500 to-emerald-600 hover:from-emerald-600 hover:to-emerald-700 shadow-lg shadow-emerald-500/40"
            >
              <Send className="h-4 w-4 mr-2" />
              {UI_STRINGS.SEND_BUTTON}
            </Button>
          </div>
          {input.trim() && (
            <QueryValidationIndicator
              query={input}
              className="mt-2 px-1"
            />
          )}
        </div>
      </div>

      {/* HITL Approval Dialog */}
      {approvalDialog && (
        <HITLApprovalDialog
          open={approvalDialog.open}
          onOpenChange={(open) => !open && setApprovalDialog(null)}
          query={approvalDialog.query}
          sql={approvalDialog.sql}
          riskLevel={approvalDialog.riskLevel}
          originalSQL={(approvalDialog as any).originalSQL}
          riskReasons={(approvalDialog as any).riskReasons}
          sqlExplanation={(approvalDialog as any).sqlExplanation}
          queryPlan={(approvalDialog as any).queryPlan}
          rlsExplanation={(approvalDialog as any).rlsExplanation}
          onApprove={handleApproveQuery}
          onReject={handleRejectQuery}
          approvalContext={approvalDialog.approvalContext}
        />
      )}

      {/* Clarification Dialog */}
      {clarificationDialog && (
        <ClarificationDialog
          open={clarificationDialog.open}
          onOpenChange={(open) => !open && setClarificationDialog(null)}
          originalQuery={clarificationDialog.originalQuery}
          message={clarificationDialog.message}
          details={clarificationDialog.details}
          databaseType={clarificationDialog.databaseType}
          onSubmit={handleClarificationSubmit}
        />
      )}

      <ChatHistoryDrawer
        open={showHistory}
        history={historyItems}
        onRerun={handleRerunQuery}
        onEditAndRun={handleEditAndRun}
        onClose={() => setShowHistory(false)}
      />

      {/* Session Cost Ticker */}
      <SessionCostTicker />
    </div>
  )
}
