import type { QueryResponse } from '@/services/apiService'
import type { QueryResult } from '@/types/domain'
import { normalizeBackendResult } from './results'

export type QueryOutcomeKind =
  | 'needs_approval'
  | 'clarification_needed'
  | 'success'
  | 'error'
  | 'streaming'

interface BaseOutcome {
  kind: QueryOutcomeKind
  response: QueryResponse
}

export interface SuccessOutcome extends BaseOutcome {
  kind: 'success'
  normalizedResult: QueryResult
}

export interface ErrorOutcome extends BaseOutcome {
  kind: 'error'
  errorMessage: string
}

export interface NeedsApprovalOutcome extends BaseOutcome {
  kind: 'needs_approval'
}

export interface ClarificationOutcome extends BaseOutcome {
  kind: 'clarification_needed'
}

export interface StreamingOutcome extends BaseOutcome {
  kind: 'streaming'
}

export type QueryOutcome =
  | SuccessOutcome
  | ErrorOutcome
  | NeedsApprovalOutcome
  | ClarificationOutcome
  | StreamingOutcome

export function coerceToCanonicalQueryResponse(raw: QueryResponse | any): QueryResponse {
  const base = raw as QueryResponse
  const existingResults = base.results

  let candidate: any =
    existingResults ??
    (raw as any).result ??
    (raw as any).results

  if (!candidate) {
    return base
  }

  const nested = (candidate as any).results ?? candidate

  const columns = nested.columns ?? existingResults?.columns ?? []
  const rows = nested.rows ?? existingResults?.rows ?? []
  const rowCount =
    typeof nested.row_count === 'number'
      ? nested.row_count
      : typeof existingResults?.row_count === 'number'
        ? existingResults.row_count
        : Array.isArray(rows)
          ? rows.length
          : 0

  const executionTimeMs =
    typeof nested.execution_time_ms === 'number'
      ? nested.execution_time_ms
      : typeof existingResults?.execution_time_ms === 'number'
        ? existingResults.execution_time_ms
        : 0

  const merged = {
    ...(existingResults || {}),
    ...nested,
    columns,
    rows,
    row_count: rowCount,
    execution_time_ms: executionTimeMs,
  }

  return {
    ...base,
    results: merged,
  }
}

/**
 * Central classification of orchestrator responses into high-level outcomes.
 * This keeps status / error / clarification mapping consistent across surfaces.
 */
export function classifyInitialQueryResponse(response: QueryResponse): QueryOutcome {
  const rawStatus = (response.status || '').toLowerCase()
  const hasError = !!response.error
  const needsApproval = !!response.needs_approval
  const hasClarificationStatus = (response as any).status === 'clarification_needed'
  const hasClarificationMessage = !!(response as any).clarification_message

  if (needsApproval) {
    return { kind: 'needs_approval', response }
  }

  if (hasClarificationStatus || hasClarificationMessage) {
    return { kind: 'clarification_needed', response }
  }

  if (rawStatus === 'error' || hasError) {
    const errorMessage =
      response.error ||
      (response as any).message ||
      'Query execution failed'

    return { kind: 'error', response, errorMessage }
  }

  if (rawStatus === 'success' && response.results) {
    const normalizedResult = normalizeBackendResult(response.results as any)
    return { kind: 'success', response, normalizedResult }
  }

  // Fallback: assume results will arrive via SSE stream
  return { kind: 'streaming', response }
}
