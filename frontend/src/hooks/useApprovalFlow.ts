import { useState, useRef } from 'react';
import { useChatStore } from '@/stores/chatStore';
import { apiService } from '@/services/apiService';
import { coerceToCanonicalQueryResponse, classifyInitialQueryResponse } from '@/utils/queryContract';
import { UI_STRINGS } from '@/constants/strings';

export function useApprovalFlow() {
  const { updateMessage, setLoading } = useChatStore();

  const [approvalDialog, setApprovalDialog] = useState<{
    open: boolean
    messageId: string
    queryId: string
    query: string
    sql: string
    riskLevel: 'SAFE' | 'LOW' | 'MEDIUM' | 'HIGH' | 'CRITICAL'
    approvalContext?: any
  } | null>(null);

  const approvingRef = useRef(false);

  const handleApproveQuery = async (modifiedSQL?: string) => {
    if (import.meta.env.DEV) {
      console.log('handleApproveQuery called, queryId:', approvalDialog?.queryId, 'modifiedSQL:', !!modifiedSQL)
    }
    if (!approvalDialog) {
      console.error('No approval dialog state!')
      return
    }
    if (approvingRef.current) {
      console.warn('Approval already in progress, ignoring duplicate click')
      return
    }
    approvingRef.current = true
    const messageId = approvalDialog.messageId
    const queryId = approvalDialog.queryId

    // Close dialog immediately
    setApprovalDialog(null)

    // Set loading state on message
    updateMessage(messageId, {
      toolCall: {
        name: 'query_processor',
        params: {},
        status: 'pending',
      },
    })
    setLoading(true)

    try {
      if (import.meta.env.DEV) {
        console.log('Submitting approval to backend...')
      }
      const approvalResponse = await apiService.submitApproval({
        query_id: queryId,
        approved: true,
        modified_sql: modifiedSQL,
      })

      const canonical = coerceToCanonicalQueryResponse(approvalResponse)
      const outcome = classifyInitialQueryResponse(canonical)

      // Prefer direct approval response when it contains a terminal outcome
      if (outcome.kind === 'success') {
        const resp = outcome.response
        const normalized = outcome.normalizedResult

        updateMessage(messageId, {
          toolCall: {
            name: 'query_processor',
            params: {},
            status: 'completed',
            result: normalized,
            metadata: {
              sql: resp.sql_query,
              insights: (resp as any).insights,
              suggestedQueries: (resp as any).suggested_queries,
              thinkingSteps: (resp as any)?.llm_metadata?.thinking_steps,
            },
          },
        })
        setLoading(false)
        approvingRef.current = false
        return
      }

      if (outcome.kind === 'error') {
        const errorMessage = outcome.errorMessage
        updateMessage(messageId, {
          toolCall: {
            name: 'query_processor',
            params: {},
            status: 'error',
            error: errorMessage,
          },
        })
        setLoading(false)
        approvingRef.current = false
        return
      }

      // If result not in response, wait for SSE using async generator
      if (import.meta.env.DEV) {
        console.log('Approval response incomplete, waiting for SSE...')
      }

      // Use async generator for SSE streaming
      ; (async () => {
        try {
          for await (const data of apiService.streamQueryState(queryId)) {
            if (data.state === 'finished' || data.state === 'FINISHED' || data.status === 'success') {
              const res = data.results || data.result

              const normalizedColumns = Array.isArray(res?.columns)
                ? res.columns.map((c: any) =>
                  typeof c === 'string' ? c : (c?.name != null ? String(c.name) : String(c))
                )
                : []

              updateMessage(messageId, {
                toolCall: {
                  name: 'query_processor',
                  params: {},
                  status: 'completed',
                  result: {
                    columns: normalizedColumns,
                    rows: res?.rows || [],
                    executionTime: (res?.execution_time_ms || 0) / 1000,
                    rowCount: res?.row_count || 0,
                  },
                  metadata: {
                    sql: data.sql || data.sql_query,
                    insights: data.insights,
                    suggestedQueries: data.suggested_queries,
                    thinkingSteps: data.thinking_steps,
                  },
                },
              })
              setLoading(false)
              approvingRef.current = false
              break
            } else if (data.state === 'error' || data.status === 'error') {
              console.error('Query failed:', data.error)
              updateMessage(messageId, {
                toolCall: {
                  name: 'query_processor',
                  params: {},
                  status: 'error',
                  error: data.error || 'Execution failed after approval',
                },
              })
              setLoading(false)
              approvingRef.current = false
              break
            }
          }
        } catch (err) {
          console.error('SSE stream error after approval:', err)
          setLoading(false)
          approvingRef.current = false
        }
      })()

    } catch (err: any) {
      console.error('Approval failed:', err)
      updateMessage(messageId, {
        toolCall: {
          name: 'query_processor',
          params: {},
          status: 'error',
          error: err.message || UI_STRINGS.APPROVAL_ERROR,
        },
      })
      setLoading(false)
    } finally {
      approvingRef.current = false
    }
  }

  const handleRejectQuery = async () => {
    if (!approvalDialog) return

    const messageId = approvalDialog.messageId
    const queryId = approvalDialog.queryId

    // Optimistically update UI
    updateMessage(messageId, {
      content: UI_STRINGS.QUERY_REJECTED_MSG,
      toolCall: {
        name: 'query_processor',
        params: {},
        status: 'rejected',
      },
    })

    setApprovalDialog(null)
    setLoading(true)

    try {
      await apiService.submitApproval({
        query_id: queryId,
        approved: false,
        rejection_reason: UI_STRINGS.REJECTION_REASON_DEFAULT,
      })
    } catch (err: any) {
      console.error('Reject approval call failed:', err)
      // Surface backend failure but keep rejected status
      updateMessage(messageId, {
        toolCall: {
          name: 'query_processor',
          params: {},
          status: 'rejected',
          error: err?.message || UI_STRINGS.REJECTION_ERROR,
        },
      })
    } finally {
      setLoading(false)
    }
  }

  return {
    approvalDialog,
    setApprovalDialog,
    handleApproveQuery,
    handleRejectQuery,
  }
}
