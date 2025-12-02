import { useState } from 'react';
import { useChatStore } from '@/stores/chatStore';
import { apiService } from '@/services/apiService';
import { coerceToCanonicalQueryResponse, classifyInitialQueryResponse } from '@/utils/queryContract';
import { UI_STRINGS } from '@/constants/strings';

export function useClarificationFlow(
  setApprovalDialog: (val: any) => void,
  messages: any[],
) {
  const { updateMessage, mergeMessage } = useChatStore();
  const [clarificationDialog, setClarificationDialog] = useState<{
    open: boolean
    queryId: string
    originalQuery: string
    message: string
    details?: any
    databaseType: 'oracle' | 'doris'
  } | null>(null);

  const handleClarificationSubmit = async (clarification: string) => {
    if (!clarificationDialog) return;

    try {
      const clarifyResp = await apiService.clarifyQuery({
        query_id: clarificationDialog.queryId,
        clarification,
        original_query: clarificationDialog.originalQuery,
        database_type: clarificationDialog.databaseType,
      })
      const canonical = coerceToCanonicalQueryResponse(clarifyResp)
      const outcome = classifyInitialQueryResponse(canonical)

      // Handle clarify response similar to initial processing
      setClarificationDialog(null)
      const lastMessage = messages[messages.length - 1]
      if (!lastMessage) return

      if (outcome.kind === 'needs_approval') {
        const resp = outcome.response
        // Open approval dialog with new query_id and sql
        setApprovalDialog({
          open: true,
          messageId: lastMessage.id,
          queryId: resp.query_id,
          query: clarificationDialog.originalQuery,
          sql: resp.sql_query || '',
          riskLevel: (resp as any)?.approval_context?.risk_level || 'MEDIUM',
          approvalContext: (resp as any)?.approval_context,
        })
        mergeMessage(lastMessage.id, (prev: any) => ({
          ...prev,
          toolCall: {
            ...(prev.toolCall || { name: 'query_processor', params: { query: clarificationDialog.originalQuery }, status: 'pending' }),
            metadata: {
              ...(prev.toolCall?.metadata || {}),
              sql: resp.sql_query || prev.toolCall?.metadata?.sql,
              thinkingSteps: (resp as any)?.llm_metadata?.thinking_steps || prev.toolCall?.metadata?.thinkingSteps,
            },
          },
        }))
        return
      }

      if (outcome.kind === 'clarification_needed') {
        const resp = outcome.response
        // Reopen clarification dialog with new message
        setClarificationDialog({
          open: true,
          queryId: clarificationDialog.queryId,
          originalQuery: clarificationDialog.originalQuery,
          message:
            (resp as any).clarification_message ||
            UI_STRINGS.CLARIFICATION_DEFAULT_MSG,
          details: (resp as any).clarification_details,
          databaseType: clarificationDialog.databaseType,
        })
        return
      }

      if (outcome.kind === 'success') {
        const resp = outcome.response
        const normalized = outcome.normalizedResult

        updateMessage(lastMessage.id, {
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
        return
      }

      if (outcome.kind === 'error') {
        const errorMessage = outcome.errorMessage
        updateMessage(lastMessage.id, {
          toolCall: {
            name: 'query_processor',
            params: {},
            status: 'error',
            error: errorMessage,
          },
        })
        return
      }
    } catch (e) {
      console.error(e)
    }
  }

  return {
    clarificationDialog,
    setClarificationDialog,
    handleClarificationSubmit,
  }
}
