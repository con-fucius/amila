/**
 * API Service for Backend Communication
 * Handles all HTTP requests to the FastAPI backend
 * 
 * When running in Docker, uses relative paths (empty string) so requests go through nginx proxy.
 * nginx.conf proxies /api/* to backend:8000 and /health to backend:8000/health
 */

const API_BASE_URL = import.meta.env.VITE_API_URL || ''

export class APIError extends Error {
  status: number;
  detail: any;

  constructor(message: string, status: number, detail?: any) {
    super(message);
    this.name = 'APIError';
    this.status = status;
    this.detail = detail;
  }
}

function getCsrfToken(): string | null {
  try {
    const cookies = document.cookie?.split(';') || []
    for (const c of cookies) {
      const [k, ...rest] = c.trim().split('=')
      if (k === 'csrf_token') return decodeURIComponent(rest.join('='))
    }
  } catch (e) {
    console.warn('[apiService] Failed to parse CSRF token:', e)
  }
  return null
}

function getAuthHeaders(): Record<string, string> {
  const headers: Record<string, string> = {}
  try {
    const token = localStorage.getItem('access_token')
    if (token) {
      headers['Authorization'] = `Bearer ${token}`
    }
    // Note: removed temp-dev-token fallback for security
  } catch (e) {
    console.warn('[apiService] Failed to get auth token:', e)
  }
  const csrf = getCsrfToken()
  if (csrf) headers['X-CSRF-Token'] = csrf
  return headers
}

export interface SchemaResponse {
  status: string
  source: string
  schema_data: { tables: Record<string, Array<{ name: string; type: string; nullable: boolean }>> }
}

export interface ConnectionsResponse {
  status: string
  connections: Array<{ name: string; type: string; status: string }>
}

export interface QueryRequest {
  query: string
  user_id?: string
  session_id?: string
  database_type?: 'oracle' | 'doris'
}
/** Mirrors backend OrchestratorQueryResponse and tolerates extra/optional fields from the backend. */
export interface QueryResponse {
  query_id: string
  status: string
  // Frontend-only helper to track terminal state from SSE
  completion_state?: 'completed' | 'error' | string
  sql_query?: string
  validation?: any
  // Primary tabular results payload (orchestrator "results")
  results?: {
    columns: string[]
    rows: any[]
    row_count: number
    execution_time_ms: number
    // Allow backend to add extra metadata without breaking the client
    [key: string]: any
  }
  // Optional alternate payload some backends may use
  result?: any
  visualization?: any
  needs_approval?: boolean
  error?: string
  insights?: string[]
  suggested_queries?: string[]
  sql_explanation?: string
  llm_metadata?: any
  approval_context?: any
  clarification_message?: string
  clarification_details?: any
  sql_confidence?: number
  optimization_suggestions?: any[]
}

export interface HistoryAPIResponse {
  history?: any[]
  items?: any[]
  // Allow backend to add extra metadata fields without breaking the client
  [key: string]: any
}

export interface DirectSQLResults {
  columns?: string[]
  rows?: any[]
  row_count?: number
  execution_time_ms?: number
}

export interface DirectSQLResponse {
  query_id?: string
  status: string
  message?: string
  sql?: string
  results?: DirectSQLResults
  error?: string
  execution_time_ms?: number
}

export interface ApprovalRequest {
  query_id: string
  approved: boolean
  modified_sql?: string
  rejection_reason?: string
}

class APIService {
  private baseURL: string

  constructor() {
    this.baseURL = API_BASE_URL
    if (import.meta.env.DEV) {
      try {
        console.log('[apiService] Base URL =', this.baseURL)
      } catch { }
    }
  }

  private async request<T>(endpoint: string, options: RequestInit = {}): Promise<T> {
    const url = endpoint.startsWith('http') ? endpoint : `${this.baseURL}${endpoint}`

    const defaultHeaders: Record<string, string> = {
      ...getAuthHeaders(),
    };

    // Default to JSON content type if body is present
    if (options.body && typeof options.body === 'string') {
      defaultHeaders['Content-Type'] = 'application/json';
    }

    const config: RequestInit = {
      ...options,
      headers: {
        ...defaultHeaders,
        ...options.headers,
      },
      credentials: 'include',
    }

    if (import.meta.env.DEV) {
      try {
        console.log(`[apiService] ${options.method || 'GET'} ${url}`)
      } catch { }
    }

    try {
      const response = await fetch(url, config)

      if (!response.ok) {
        let errorData: any = {};
        try {
          errorData = await response.json();
        } catch {
          errorData = { detail: response.statusText };
        }

        // Standardized error handling
        const message = errorData.detail || errorData.message || errorData.error || 'An unexpected error occurred';

        // You might trigger a global toast/notification here if you had a store/event bus
        if (response.status === 401) {
          console.warn('Unauthorized access - redirect to login?');
        } else if (response.status === 403) {
          console.warn('Access forbidden');
        }

        throw new APIError(message, response.status, errorData);
      }

      // Some endpoints might return empty body (204 No Content)
      if (response.status === 204) {
        return {} as T;
      }

      return response.json();
    } catch (error) {
      if (error instanceof APIError) {
        throw error;
      }
      // Network errors (fetch failed)
      throw new APIError(error instanceof Error ? error.message : 'Network error', 0);
    }
  }

  async submitQuery(request: QueryRequest): Promise<QueryResponse> {
    return this.request<QueryResponse>('/api/v1/queries/process', {
      method: 'POST',
      body: JSON.stringify({
        query: request.query,
        user_id: request.user_id || 'default_user',
        session_id: request.session_id || this.generateSessionId(),
        database_type: request.database_type || 'doris',
      }),
    })
  }

  async clarifyQuery(params: { query_id: string; clarification: string; original_query?: string; database_type?: 'oracle' | 'doris' }): Promise<QueryResponse> {
    return this.request<QueryResponse>('/api/v1/queries/clarify', {
      method: 'POST',
      body: JSON.stringify({
        query_id: params.query_id,
        clarification: params.clarification,
        original_query: params.original_query,
        database_type: params.database_type,
      }),
    })
  }

  async *streamQueryState(queryId: string): AsyncGenerator<any, void, unknown> {
    const url = `${this.baseURL}/api/v1/queries/${queryId}/stream`

    if (import.meta.env.DEV) {
      try {
        console.log('[apiService] GET (Stream)', url)
      } catch { }
    }

    const response = await fetch(url, {
      headers: {
        ...getAuthHeaders(),
        'Accept': 'text/event-stream',
      },
    })

    if (!response.ok) {
      throw new APIError('Failed to start event stream', response.status)
    }

    if (!response.body) {
      throw new Error('ReadableStream not supported in this browser.')
    }

    const reader = response.body.getReader()
    const decoder = new TextDecoder()
    let buffer = ''

    try {
      while (true) {
        const { done, value } = await reader.read()
        if (done) break

        buffer += decoder.decode(value, { stream: true })
        const lines = buffer.split('\n\n') // SSE double newline delimiter
        buffer = lines.pop() || '' // Keep incomplete chunk

        for (const line of lines) {
          const trimmed = line.trim()
          if (!trimmed) continue

          // Standard SSE format: "data: ..."
          if (trimmed.startsWith('data: ')) {
            const dataStr = trimmed.slice(6)
            try {
              if (dataStr === '[DONE]') return
              const data = JSON.parse(dataStr)
              yield data
            } catch (e) {
              console.warn('Failed to parse SSE JSON:', e)
            }
          }
        }
      }
    } finally {
      reader.releaseLock()
    }
  }

  async submitApproval(request: ApprovalRequest): Promise<QueryResponse> {
    return this.request<QueryResponse>(`/api/v1/queries/${request.query_id}/approve`, {
      method: 'POST',
      body: JSON.stringify({
        approved: request.approved,
        modified_sql: request.modified_sql,
        rejection_reason: request.rejection_reason,
      }),
    })
  }

  async listConnections(): Promise<ConnectionsResponse> {
    return this.request<ConnectionsResponse>('/api/v1/queries/connections')
  }

  async getQueryHistory(sessionId: string, limit: number = 50): Promise<HistoryAPIResponse> {
    return this.request<HistoryAPIResponse>('/api/v1/history/get', {
      method: 'POST',
      body: JSON.stringify({ session_id: sessionId, limit }),
    })
  }

  async getSchema(options?: { connection_name?: string; use_cache?: boolean; database_type?: 'oracle' | 'doris' }): Promise<SchemaResponse> {
    // Build query params manually to avoid URL constructor issues with relative paths
    const params = new URLSearchParams()
    if (options?.connection_name) params.set('connection_name', options.connection_name)
    if (options?.use_cache !== undefined) params.set('use_cache', String(options.use_cache))
    if (options?.database_type) params.set('database_type', options.database_type)
    
    const queryString = params.toString()
    const endpoint = `/api/v1/schema/${queryString ? `?${queryString}` : ''}`

    return this.request<SchemaResponse>(endpoint)
  }

  async checkHealth(): Promise<{ status: string; components?: any; latency_ms?: number }> {
    const start = performance.now()
    const data = await this.request<any>('/health')
    data.latency_ms = Math.round(performance.now() - start)
    return data
  }

  async testConnection(): Promise<boolean> {
    try {
      const health = await this.checkHealth()
      return health.status === 'healthy'
    } catch (err) {
      console.error('Backend connection test failed:', err)
      return false
    }
  }

  private generateSessionId(): string {
    return `session_${Date.now()}_${Math.random().toString(36).substring(2, 9)}`
  }

  getBaseURL(): string {
    return this.baseURL
  }

  async submitSQL(sql: string, connection_name?: string, database_type?: 'oracle' | 'doris'): Promise<DirectSQLResponse> {
    return this.request<DirectSQLResponse>('/api/v1/queries/submit', {
      method: 'POST',
      body: JSON.stringify({
        query: sql,
        connection_name: connection_name || 'TestUserCSV',
        database_type: database_type || 'doris',
      }),
    })
  }

  /**
   * Login via OAuth2PasswordRequestForm-compatible endpoint
   */
  async login(username: string, password: string): Promise<{ access_token: string; refresh_token: string; token_type: string }> {
    const body = new URLSearchParams()
    body.set('username', username)
    body.set('password', password)

    return this.request<{ access_token: string; refresh_token: string; token_type: string }>('/api/v1/auth/login', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/x-www-form-urlencoded',
      },
      body: body.toString(),
    })
  }

  /**
   * Generate Python-based visualization using Plotly
   */
  async generateVisualization(params: {
    columns: string[]
    rows: any[][]
    chart_type?: string
    title?: string
  }): Promise<{
    status: string
    chart_type?: string
    plotly_json?: any
    message?: string
    fallback?: string
  }> {
    return this.request('/api/v1/queries/visualize', {
      method: 'POST',
      body: JSON.stringify(params),
    })
  }



  /**
   * Get column statistics for a table
   */
  async getTableStats(tableName: string, databaseType: string = 'oracle'): Promise<{
    status: string
    table_name: string
    stats: Array<{
      column: string
      type: string
      distinct_count?: number
      null_count?: number
      min?: any
      max?: any
      error?: string
    }>
  }> {
    return this.request(`/api/v1/schema/table/${encodeURIComponent(tableName)}/stats?database_type=${databaseType}`)
  }

  /**
   * Generate executive report from query results
   */
  async generateReport(params: {
    query_results: Array<{ columns: string[]; rows: any[][]; sql_query?: string; row_count?: number }>
    format: 'html' | 'pdf' | 'docx'
    title?: string
    user_queries?: string[]
  }): Promise<{
    status: string
    format: string
    content_type: string
    content: string
    encoding?: string
    title: string
    generated_at: string
  }> {
    return this.request('/api/v1/queries/report', {
      method: 'POST',
      body: JSON.stringify(params),
    })
  }

  /**
   * Report frontend errors to backend for centralized logging
   */
  async reportError(params: {
    message: string
    stack?: string
    url?: string
    component?: string
    additional_context?: Record<string, any>
  }): Promise<{ status: string; error_id: string; message: string; timestamp: string }> {
    return this.request('/api/v1/errors/report', {
      method: 'POST',
      body: JSON.stringify({
        message: params.message,
        stack: params.stack,
        url: params.url || window.location.href,
        component: params.component,
        user_agent: navigator.userAgent,
        additional_context: params.additional_context,
      }),
    })
  }

  /**
   * Check database health before allowing selection
   */
  async checkDatabaseHealth(databaseType: 'oracle' | 'doris'): Promise<{
    status: string
    healthy: boolean
    latency_ms?: number
    error?: string
  }> {
    try {
      const start = performance.now()
      const response = await this.request<any>(`/api/v1/health/database?type=${databaseType}`)
      const latency = Math.round(performance.now() - start)
      return {
        status: 'success',
        healthy: response.status === 'healthy',
        latency_ms: latency,
      }
    } catch (err: any) {
      return {
        status: 'error',
        healthy: false,
        error: err.message || 'Health check failed',
      }
    }
  }
}

// Global error handler to report uncaught errors
if (typeof window !== 'undefined') {
  window.addEventListener('error', (event) => {
    apiService.reportError({
      message: event.message,
      stack: event.error?.stack,
      url: event.filename,
      component: 'global',
      additional_context: {
        lineno: event.lineno,
        colno: event.colno,
      },
    }).catch(() => {
      // Silently fail if error reporting fails
    })
  })

  window.addEventListener('unhandledrejection', (event) => {
    apiService.reportError({
      message: `Unhandled Promise Rejection: ${event.reason}`,
      stack: event.reason?.stack,
      component: 'global',
      additional_context: {
        type: 'unhandledrejection',
      },
    }).catch(() => {
      // Silently fail if error reporting fails
    })
  })
}

export const apiService = new APIService()
