import { useRef, useEffect, useState } from 'react'
import { Button } from './ui/button'
import { Card, CardContent } from './ui/card'
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
import { Send } from 'lucide-react'

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
        databaseType,
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
              sql: resp.sql_query || prev.toolCall?.metadata?.sql,
              insights: resp.insights ?? prev.toolCall?.metadata?.insights,
              suggestedQueries: resp.suggested_queries ?? prev.toolCall?.metadata?.suggestedQueries,
              sqlExplanation: resp.sql_explanation ?? prev.toolCall?.metadata?.sqlExplanation,
              resultAnalysis: (resp as any).result_analysis ?? prev.toolCall?.metadata?.resultAnalysis,
              thinkingSteps: (resp as any)?.llm_metadata?.thinking_steps ?? prev.toolCall?.metadata?.thinkingSteps,
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
          approvalContext: (response as any)?.approval_context,
        })
      }
    }
  }, [currentState?.state])

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
      <div className="flex-1 overflow-y-auto overflow-x-hidden px-6 py-4">
        <div className="space-y-4">
          {messages.length === 0 && (
            <Card className="border-emerald-100/70 bg-white/70 dark:border-emerald-500/40 dark:bg-slate-950/70 backdrop-blur-xl shadow-sm">
              <CardContent className="p-6 text-center">
                <div className="text-emerald-900 dark:text-emerald-100 font-semibold mb-2">{UI_STRINGS.WELCOME_TITLE}</div>
                <div className="text-slate-700 dark:text-slate-200 text-sm">
                  {UI_STRINGS.WELCOME_SUBTITLE}
                </div>
              </CardContent>
            </Card>
          )}

          {messages.map((message) => {
            const isReasoningOpen = !!reasoningOpen[message.id]
            const isChartOpen = !!chartOpen[message.id]
            const thinkingStepsArray = extractThinkingSteps((message as any).toolCall?.metadata || (message as any).toolCall)
            const hasReasoningInfo = !!(message as any).toolCall?.metadata?.sql || thinkingStepsArray.length > 0

            return (
              <div
                key={message.id}
                className={cn(
                  'space-y-3',
                  message.type === 'user' ? 'flex justify-end' : ''
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
                      // Retry the original query from this message's context
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
      </div>

      {/* Input Area */}
      <div className="p-4 border-t border-emerald-100/60 dark:border-emerald-500/30 bg-white/80 dark:bg-slate-950/70 backdrop-blur-xl shadow-md flex-shrink-0">
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
          onApprove={handleApproveQuery}
          onReject={handleRejectQuery}
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
