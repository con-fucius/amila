import { useState, useCallback, useRef, useEffect } from 'react'
import { apiService, QueryResponse } from '@/services/apiService'
import type { ThinkingStep, DatabaseType } from '@/types/domain'
import { classifyInitialQueryResponse } from '@/utils/queryContract'

export interface QueryState {
  state: string
  progress?: number
  message?: string
  sql?: string
  thinking_steps?: ThinkingStep[]
  schema_data?: any[]
  intermediate_data?: any
}

export interface UseQuerySubmissionReturn {
  submitQuery: (query: string, sessionId?: string, databaseType?: DatabaseType) => Promise<void>
  isLoading: boolean
  error: string | null
  currentState: QueryState | null
  response: QueryResponse | null
  cancelQuery: () => void
  retryConnection: () => void
}

export function useQuerySubmission(): UseQuerySubmissionReturn {
  const [isLoading, setIsLoading] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const [currentState, setCurrentState] = useState<QueryState | null>(null)
  const [response, setResponse] = useState<QueryResponse | null>(null)
  const abortControllerRef = useRef<AbortController | null>(null)
  const retryTimeoutRef = useRef<ReturnType<typeof setTimeout> | null>(null)
  const retryCountRef = useRef(0)
  const maxRetries = 3
  const currentQueryIdRef = useRef<string | null>(null)

  useEffect(() => {
    return () => {
      if (abortControllerRef.current) {
        abortControllerRef.current.abort()
      }
      if (retryTimeoutRef.current) {
        clearTimeout(retryTimeoutRef.current)
      }
    }
  }, [])

  const cancelQuery = useCallback(async () => {
    const queryId = currentQueryIdRef.current

    // Abort SSE stream
    if (abortControllerRef.current) {
      abortControllerRef.current.abort()
      abortControllerRef.current = null
    }
    if (retryTimeoutRef.current) {
      clearTimeout(retryTimeoutRef.current)
      retryTimeoutRef.current = null
    }
    retryCountRef.current = 0

    // Call backend cancel endpoint if we have a query ID
    if (queryId) {
      try {
        await apiService.cancelQuery(queryId)
        console.log(`Query ${queryId} cancellation requested`)
      } catch (err: any) {
        console.error('Failed to cancel query:', err)
        // Don't throw - we still want to clean up local state
      }
    }

    currentQueryIdRef.current = null
    setIsLoading(false)
    setError('Query cancelled by user')
  }, [])

  const setupSSEStream = useCallback(async (queryId: string, initialResult: QueryResponse) => {
    if (abortControllerRef.current) {
      abortControllerRef.current.abort()
    }
    const abortController = new AbortController()
    abortControllerRef.current = abortController
    currentQueryIdRef.current = queryId

    try {
      for await (const data of apiService.streamQueryState(queryId)) {
        if (abortController.signal.aborted) break

        retryCountRef.current = 0 // Reset retries on success
        const metadata = data.metadata || {}

        // Standardize results extraction
        const extractedResult = data.results || data.result || metadata.results || metadata.result
        const resultRef = data.result_ref || metadata.result_ref
        const resultsTruncated = data.results_truncated ?? metadata.results_truncated

        // Standardize state extraction
        const rawState = data.state || data.status || metadata.state || metadata.status || 'processing'

        setCurrentState({
          state: rawState,
          progress: data.progress ?? metadata.progress,
          message: data.message || metadata.message,
          sql: data.sql || data.sql_query || metadata.sql,
          thinking_steps: data.thinking_steps || metadata.thinking_steps || [],
          schema_data: data.schema_data || metadata.schema_data || [],
          intermediate_data: data.intermediate_data ?? metadata.intermediate_data,
        })

        const isFinished = ['finished', 'success', 'completed'].includes(rawState.toLowerCase())
        const isError = ['error', 'rejected'].includes(rawState.toLowerCase())

        if (isFinished) {
          const finalResult = extractedResult || initialResult.results
          setResponse({
            ...initialResult,
            status: 'success',
            completion_state: 'completed',
            needs_approval: false,
            results: finalResult,
            result_ref: resultRef,
            results_truncated: resultsTruncated,
            sql_query: data.sql || initialResult.sql_query,
            insights: data.insights || initialResult.insights,
            suggested_queries: data.suggested_queries || initialResult.suggested_queries,
            sql_explanation: data.sql_explanation || initialResult.sql_explanation,
          })
          setIsLoading(false)
          currentQueryIdRef.current = null
          break
        }

        if (isError) {
          const errorMessage = data.error || data.message || metadata.error || 'Query processing failed'
          const errorDetails = data.errorDetails || data.error_details || metadata.errorDetails || metadata.error_details

          setError(errorMessage)
          setResponse({
            ...initialResult,
            status: 'error',
            completion_state: 'error',
            needs_approval: false,
            error: errorMessage,
            sql_query: data.sql || data.sql_query || initialResult.sql_query,
            llm_metadata: {
              ...(initialResult as any).llm_metadata,
              error_details: errorDetails,
            },
          } as QueryResponse)

          setIsLoading(false)
          currentQueryIdRef.current = null
          break
        }
      }
    } catch (err: any) {
      if (abortController.signal.aborted) return
      console.error('Stream error:', err)

      // Enhanced SSE error detection and reporting
      const isNetworkError = err.message?.includes('network') ||
        err.message?.includes('fetch') ||
        err.name === 'TypeError'
      const isTimeoutError = err.message?.includes('timeout')

      // Report error to backend for monitoring
      try {
        apiService.reportError({
          message: `SSE stream error: ${err.message}`,
          component: 'useQuerySubmission',
          additional_context: {
            queryId: queryId,
            retryCount: retryCountRef.current,
            isNetworkError,
            isTimeoutError,
          },
        }).catch(() => { })
      } catch { }

      // Retry logic with enhanced error handling
      if (retryCountRef.current < maxRetries) {
        retryCountRef.current++
        const delay = Math.min(1000 * Math.pow(2, retryCountRef.current - 1), 10000)

        setCurrentState(prev => ({
          ...prev,
          state: 'reconnecting',
          message: `Connection lost, retrying in ${delay / 1000}s (attempt ${retryCountRef.current}/${maxRetries})...`,
        } as QueryState))

        retryTimeoutRef.current = setTimeout(() => {
          if (currentQueryIdRef.current) {
            setupSSEStream(currentQueryIdRef.current, initialResult)
          }
        }, delay)
      } else {
        const errorMsg = isNetworkError
          ? 'Connection to server lost. Please check your network and try again.'
          : isTimeoutError
            ? 'Request timed out. The query may still be processing.'
            : 'Connection lost after multiple retries'
        setError(errorMsg)
        setIsLoading(false)
      }
    }
  }, [])

  const retryConnection = useCallback(() => {
    if (currentQueryIdRef.current && response) {
      retryCountRef.current = 0
      setError(null)
      setupSSEStream(currentQueryIdRef.current, response)
    }
  }, [response, setupSSEStream])

  const submitQuery = useCallback(async (query: string, sessionId?: string, databaseType?: DatabaseType) => {
    try {
      setIsLoading(true)
      setError(null)
      setCurrentState(null)
      setResponse(null)
      retryCountRef.current = 0

      const autoApprove = localStorage.getItem('disableSqlApproval') === 'true'

      const initial = await apiService.submitQuery({
        query,
        user_id: 'default_user',
        session_id: sessionId,
        database_type: databaseType,
        auto_approve: autoApprove,
      })

      const outcome = classifyInitialQueryResponse(initial)

      if (outcome.kind === 'error') {
        // Preserve existing behavior: surface structured error via response, not generic error channel
        setResponse(initial)
        setIsLoading(false)
        return
      }

      if (outcome.kind === 'needs_approval') {
        setResponse(initial)
        setIsLoading(false)
        return
      }

      if (outcome.kind === 'clarification_needed') {
        setResponse(initial)
        setIsLoading(false)
        return
      }

      if (outcome.kind === 'conversational') {
        // Conversational responses (greetings, help, meta questions)
        // These don't need SSE streaming - response is complete
        setResponse(initial)
        setIsLoading(false)
        return
      }

      if (outcome.kind === 'success') {
        setResponse(initial)
        setIsLoading(false)
        return
      }

      // Streaming path: rely on SSE updates, don't setResponse yet to avoid
      // prematurely toggling loading state in RealChatInterface.
      if (outcome.kind === 'streaming') {
        setupSSEStream(initial.query_id, initial)
        return
      }

    } catch (err: any) {
      console.error('Query submission error:', err)
      setError(err.message || 'Failed to submit query')
      setIsLoading(false)
      currentQueryIdRef.current = null
    }
  }, [setupSSEStream])

  return {
    submitQuery,
    isLoading,
    error,
    currentState,
    response,
    cancelQuery,
    retryConnection,
  }
}
