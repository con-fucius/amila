/**
 * API Service for Backend Communication
 * Handles all HTTP requests to the FastAPI backend
 * 
 * When running in Docker, uses relative paths (empty string) so requests go through nginx proxy.
 * nginx.conf proxies /api/* to backend:8000 and /health to backend:8000/health
 */

const API_BASE_URL = import.meta.env.VITE_API_URL || ''
import type { DatabaseType } from '@/types/domain'

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

/**
 * Check if JWT token is expired or about to expire (within 5 minutes)
 */
function isTokenExpired(token: string, bufferSeconds: number = 300): boolean {
  try {
    const parts = token.split('.')
    if (parts.length !== 3) return true

    const payload = JSON.parse(atob(parts[1]))
    const exp = payload.exp
    if (!exp) return true

    const now = Math.floor(Date.now() / 1000)
    return exp < (now + bufferSeconds)
  } catch {
    return true
  }
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
  database_type?: DatabaseType
  auto_approve?: boolean
}

export interface WebhookItem {
  webhook_id: string
  user_id?: string
  url: string
  events: string[]
  active: boolean
  secret?: string | null
  created_at: string
  updated_at: string
  last_delivery_at?: string | null
  last_status_code?: number | null
  last_error?: string | null
  consecutive_failures?: number
}

export interface EnhanceQueryResponse {
  original_query: string
  enhanced_query: string
  method: string
  context_used?: boolean
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
  result_ref?: {
    query_id: string
    row_count: number
    columns: string[]
    cache_status?: string
  }
  results_truncated?: boolean
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
  // Conversational response fields
  message?: string
  is_conversational?: boolean
  intent?: string
  structured_intent?: {
    query_type: string
    complexity: string
    domain: string
    temporal: string
    expected_cardinality: string
    tables: string[]
    entities: Array<{
      name: string
      type: string
      confidence: number
    }>
    time_period?: string
    aggregations: Array<{
      function: string
      column: string
      alias?: string
    }>
    filters: Array<{
      column: string
      operator: string
      value?: string
    }>
    joins_count: number
    source: string
    confidence: number
    measures: string[]
    dimensions: string[]
  }
}

export interface RepairTraceEntry {
  type: string
  error: string
  action: string
  before_sql?: string
  after_sql?: string
  diff?: string
  timestamp: string
}

export interface SystemDiagnosticsSummary {
  timestamp: string
  overall_status: string // HEALTHY, DEGRADED, CRITICAL
  mcp_tools: any[]
  connection_pools: any[]
  degraded_components: any[]
  active_queries: number
  recent_failures: any[]
  performance_metrics: {
    avg_query_latency_ms: number
    queries_per_minute: number
    error_rate: number
  }
  business_kpis: {
    total_query_cost_24h: number
    global_rejection_rate: number
    high_risk_alerts_count: number
  }
  alerts: {
    total_active: number
    critical: number
    warning: number
    latest_alerts: any[]
  }
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
  result_ref?: {
    query_id: string
    row_count: number
    columns: string[]
    cache_status?: string
  }
  results_truncated?: boolean
  error?: string
  execution_time_ms?: number
}

export interface ApprovalRequest {
  query_id: string
  approved: boolean
  modified_sql?: string
  rejection_reason?: string
  decision_reason?: string
  constraints?: {
    max_rows?: number
  }
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

  private async request<T>(endpoint: string, options: RequestInit = {}, retryOnAuth: boolean = true): Promise<T> {
    const url = endpoint.startsWith('http') ? endpoint : `${this.baseURL}${endpoint}`

    // Check if token needs refresh before making request
    await this.ensureValidToken()

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

        // Handle 401 with token refresh retry
        if (response.status === 401 && retryOnAuth) {
          const refreshed = await this.tryRefreshToken()
          if (refreshed) {
            // Retry the request with new token (no further retry)
            return this.request<T>(endpoint, options, false)
          }
          // Refresh failed - clear tokens and notify
          this.clearAuthTokens()
          window.dispatchEvent(new Event('auth-logout'))
          console.warn('Session expired - please log in again')
        }

        // Standardized error handling
        const message = errorData.detail || errorData.message || errorData.error || 'An unexpected error occurred';

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

  /**
   * Ensure we have a valid (non-expired) access token before making requests
   */
  private async ensureValidToken(): Promise<void> {
    const token = localStorage.getItem('access_token')
    if (token && isTokenExpired(token)) {
      await this.tryRefreshToken()
    }
  }

  /**
   * Attempt to refresh the access token using the refresh token
   */
  private async tryRefreshToken(): Promise<boolean> {
    const refreshToken = localStorage.getItem('refresh_token')
    if (!refreshToken) return false

    try {
      const response = await fetch(`${this.baseURL}/api/v1/auth/refresh`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ refresh_token: refreshToken }),
      })

      if (response.ok) {
        const data = await response.json()
        localStorage.setItem('access_token', data.access_token)
        if (data.refresh_token) {
          localStorage.setItem('refresh_token', data.refresh_token)
        }
        console.log('[apiService] Token refreshed successfully')
        return true
      }
    } catch (e) {
      console.warn('[apiService] Token refresh failed:', e)
    }

    return false
  }

  /**
   * Clear stored auth tokens
   */
  private clearAuthTokens(): void {
    localStorage.removeItem('access_token')
    localStorage.removeItem('refresh_token')
  }

  async submitQuery(request: QueryRequest): Promise<QueryResponse> {
    // Get authenticated user from token if available
    const authenticatedUserId = this.getAuthenticatedUserId()

    return this.request<QueryResponse>('/api/v1/queries/process', {
      method: 'POST',
      body: JSON.stringify({
        query: request.query,
        // Use authenticated user ID, fall back to request.user_id, then default
        user_id: authenticatedUserId || request.user_id || 'default_user',
        session_id: request.session_id || this.generateSessionId(),
        database_type: request.database_type || 'doris',
        auto_approve: request.auto_approve || false,
      }),
    })
  }

  /**
   * Extract user ID from stored JWT token
   */
  private getAuthenticatedUserId(): string | null {
    try {
      const token = localStorage.getItem('access_token')
      if (!token) return null

      // Decode JWT payload (base64)
      const parts = token.split('.')
      if (parts.length !== 3) return null

      const payload = JSON.parse(atob(parts[1]))
      return payload.sub || null
    } catch (e) {
      console.warn('[apiService] Failed to extract user from token:', e)
      return null
    }
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

  async enhanceQuery(params: {
    query: string
    conversation_history?: Array<{ role: string; content: string }>
    database_type?: 'oracle' | 'doris' | 'postgres'
  }): Promise<EnhanceQueryResponse> {
    return this.request<EnhanceQueryResponse>('/api/v1/queries/enhance', {
      method: 'POST',
      body: JSON.stringify({
        query: params.query,
        conversation_history: params.conversation_history || [],
        database_type: params.database_type,
        use_llm: true,
      }),
    })
  }

  async *streamQueryState(queryId: string): AsyncGenerator<any, void, unknown> {
    // Include auth token as query parameter for SSE (EventSource doesn't support headers)
    const token = localStorage.getItem('access_token')
    const tokenParam = token ? `?token=${encodeURIComponent(token)}` : ''
    const url = `${this.baseURL}/api/v1/queries/${queryId}/stream${tokenParam}`

    if (import.meta.env.DEV) {
      try {
        console.log('[apiService] GET (Stream)', url.replace(/token=[^&]+/, 'token=***'))
      } catch { }
    }

    const response = await fetch(url, {
      headers: {
        ...getAuthHeaders(),
        'Accept': 'text/event-stream',
      },
      credentials: 'include',
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
        constraints: request.constraints,
      }),
    })
  }

  async cancelQuery(queryId: string): Promise<{
    query_id: string
    status: string
    message: string
    cancelled: boolean
  }> {
    return this.request(`/api/v1/queries/${queryId}/cancel`, {
      method: 'POST',
    })
  }

  async getRateLimitStatus(endpoints?: string[]): Promise<{ status: string; user: string; tier: string; endpoints: Record<string, any> }> {
    const params = new URLSearchParams()
    if (endpoints && endpoints.length) {
      for (const ep of endpoints) params.append('endpoints', ep)
    }
    const qs = params.toString()
    return this.request(`/api/v1/ratelimits/status${qs ? `?${qs}` : ''}`)
  }

  async listWebhooks(): Promise<{ status: string; webhooks: WebhookItem[] }> {
    return this.request('/api/v1/webhooks')
  }

  async createWebhook(body: { url: string; events: string[]; active?: boolean; secret?: string }): Promise<{ status: string; webhook: WebhookItem }> {
    return this.request('/api/v1/webhooks', {
      method: 'POST',
      body: JSON.stringify(body),
    })
  }

  async updateWebhook(webhookId: string, body: { url?: string; events?: string[]; active?: boolean; secret?: string }): Promise<{ status: string; webhook: WebhookItem }> {
    return this.request(`/api/v1/webhooks/${encodeURIComponent(webhookId)}`, {
      method: 'PUT',
      body: JSON.stringify(body),
    })
  }

  async deleteWebhook(webhookId: string): Promise<{ status: string; deleted: string }> {
    return this.request(`/api/v1/webhooks/${encodeURIComponent(webhookId)}`, {
      method: 'DELETE',
    })
  }

  async testWebhook(webhookId: string): Promise<{ status: string }> {
    return this.request(`/api/v1/webhooks/${encodeURIComponent(webhookId)}/test`, {
      method: 'POST',
      body: JSON.stringify({}),
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

  async getQueryResultsPage(queryId: string, page: number, pageSize: number): Promise<{
    status: string
    query_id: string
    results: { columns: string[]; rows: any[]; row_count: number; execution_time_ms: number }
    page: number
    page_size: number
    total_pages: number
  }> {
    const params = new URLSearchParams()
    params.set('page', String(page))
    params.set('page_size', String(pageSize))
    return this.request(`/api/v1/queries/${encodeURIComponent(queryId)}/results?${params.toString()}`)
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
   * Create a scheduled report (cron-based)
   */
  async createReportSchedule(params: {
    name: string
    cron: string
    sql_query: string
    database_type: string
    connection_name?: string
    format: 'html' | 'pdf' | 'docx'
    recipients: string[]
  }): Promise<{ status: string; schedule?: any; message?: string }> {
    return this.request('/api/v1/reports/schedule', {
      method: 'POST',
      body: JSON.stringify(params),
    })
  }

  /**
   * List report schedules for current user
   */
  async listReportSchedules(limit: number = 50): Promise<{ status: string; schedules: any[] }> {
    return this.request(`/api/v1/reports/schedules?limit=${limit}`)
  }

  /**
   * Delete a report schedule by ID
   */
  async deleteReportSchedule(scheduleId: string): Promise<{ status: string; deleted: string }> {
    return this.request(`/api/v1/reports/schedule/${encodeURIComponent(scheduleId)}`, {
      method: 'DELETE',
    })
  }

  /**
   * Save dashboard from query results
   */
  async createDashboardFromQuery(params: {
    sql_query: string
    query_results: { columns: string[]; rows: any[][]; row_count?: number }
    title?: string
    description?: string
  }): Promise<{ status: string; dashboard?: any; message?: string }> {
    return this.request('/api/v1/dashboards/generate', {
      method: 'POST',
      body: JSON.stringify(params),
    })
  }

  /**
   * List dashboards for current user
   */
  async listDashboards(limit: number = 20): Promise<{ status: string; dashboards: any[]; count: number }> {
    return this.request(`/api/v1/dashboards?limit=${limit}`)
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
  async checkDatabaseHealth(databaseType: 'oracle' | 'doris' | 'postgres'): Promise<{
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

  /**
   * Get MCP tools status from both Oracle and Doris MCP servers
   */
  async getMCPToolsStatus(): Promise<any> {
    try {
      const response = await this.request<any>('/health/mcp-tools')
      return response
    } catch (err: any) {
      console.error('[apiService] Failed to get MCP tools status:', err)
      return {
        timestamp: new Date().toISOString(),
        servers: {
          oracle: {
            server_name: 'Oracle SQLcl MCP',
            server_status: 'error',
            tools: []
          },
          doris: {
            server_name: 'Apache Doris MCP',
            server_status: 'error',
            tools: []
          }
        }
      }
    }
  }

  // ============================================================================
  // Budget Forecasting API Methods
  // ============================================================================

  /**
   * Get budget forecast for current user
   */
  async getBudgetForecast(budgetLimit?: number): Promise<{
    current_period: string
    current_usage: number
    forecasted_usage: number
    budget_limit: number
    projected_overrun: number | null
    confidence_interval_low: number
    confidence_interval_high: number
    days_remaining: number
    trend_direction: string
    daily_average: number
    recommended_daily_budget: number
  }> {
    const params = budgetLimit ? `?budget_limit=${budgetLimit}` : ''
    return this.request(`/api/v1/cost/forecast${params}`)
  }

  /**
   * Get cost anomalies for current user
   */
  async getCostAnomalies(days: number = 30): Promise<Array<{
    date: string
    cost: number
    expected_cost: number
    deviation_percentage: number
    severity: string
    description: string
  }>> {
    return this.request(`/api/v1/cost/anomalies?days=${days}`)
  }

  /**
   * Get budget alerts for current user
   */
  async getBudgetAlerts(budgetLimit?: number): Promise<Array<{
    alert_level: string
    message: string
    current_usage: number
    budget_limit: number
    percentage_used: number
    recommended_action: string
    triggered_at: string
  }>> {
    const params = budgetLimit ? `?budget_limit=${budgetLimit}` : ''
    return this.request(`/api/v1/cost/budget-alerts${params}`)
  }

  /**
   * Get cost optimization recommendations
   */
  async getCostOptimizationRecommendations(): Promise<Array<{
    type: string
    priority: string
    message: string
    details: string
    potential_savings: string
  }>> {
    return this.request('/api/v1/cost/optimization')
  }

  // ============================================================================
  // Skill Generator API Methods
  // ============================================================================

  /**
   * List auto-generated skills
   */
  async listSkills(params?: {
    skill_type?: string
    min_confidence?: number
    limit?: number
    offset?: number
  }): Promise<{
    skills: Array<{
      skill_id: string
      skill_type: string
      name: string
      description: string
      confidence: number
      generated_at: string
      effectiveness_score: number
      usage_count: number
      yaml_preview?: string
    }>
    total: number
    filters_applied: Record<string, any>
  }> {
    const queryParams = new URLSearchParams()
    if (params?.skill_type) queryParams.set('skill_type', params.skill_type)
    if (params?.min_confidence) queryParams.set('min_confidence', String(params.min_confidence))
    if (params?.limit) queryParams.set('limit', String(params.limit))
    if (params?.offset) queryParams.set('offset', String(params.offset))

    const query = queryParams.toString()
    return this.request(`/api/v1/skills/list${query ? `?${query}` : ''}`)
  }

  /**
   * Get skill details
   */
  async getSkillDetail(skillId: string): Promise<{
    skill_id: string
    skill_type: string
    name: string
    description: string
    yaml_content: string
    source_queries: string[]
    confidence: number
    generated_at: string
    effectiveness_score: number
    usage_count: number
  }> {
    return this.request(`/api/v1/skills/${skillId}`)
  }

  /**
   * Generate skills from patterns
   */
  async generateSkills(params?: {
    min_frequency?: number
    min_confidence?: number
    skill_type?: string
  }): Promise<{
    success: boolean
    generated_count: number
    skills: Array<{
      skill_id: string
      skill_type: string
      name: string
      confidence: number
      generated_at: string
    }>
    message: string
  }> {
    return this.request('/api/v1/skills/generate', {
      method: 'POST',
      body: JSON.stringify(params || {}),
    })
  }

  /**
   * Approve or reject a skill
   */
  async approveSkill(skillId: string, approved: boolean, reason?: string): Promise<{
    success: boolean
    skill_id: string
    approved: boolean
    message: string
  }> {
    return this.request(`/api/v1/skills/${skillId}/approve`, {
      method: 'POST',
      body: JSON.stringify({ approved, reason }),
    })
  }

  /**
   * Delete a skill
   */
  async deleteSkill(skillId: string): Promise<{
    success: boolean
    skill_id: string
    message: string
  }> {
    return this.request(`/api/v1/skills/${skillId}`, {
      method: 'DELETE',
    })
  }

  /**
   * Get skill statistics
   */
  async getSkillStats(): Promise<{
    total_skills: number
    by_type: Record<string, number>
    avg_confidence: number
    avg_effectiveness: number
    total_usage: number
    recently_generated: number
  }> {
    return this.request('/api/v1/skills/stats/summary')
  }

  /**
   * Export skills to YAML
   */
  async exportSkills(skillIds?: string[]): Promise<{
    success: boolean
    export_format: string
    content: string
    filename: string
    skill_count: number
  }> {
    return this.request('/api/v1/skills/export', {
      method: 'POST',
      body: JSON.stringify({ skill_ids: skillIds, format: 'yaml' }),
    })
  }

  /**
   * Get business metrics glossary
   */
  async getMetricsGlossary(): Promise<{
    status: string
    glossary: Array<{
      metric_id: string
      name: string
      description: string
      owner: string
      tags: string[]
      business_definition: string
      calculation_logic: string
      usage_count: number
    }>
  }> {
    return this.request('/api/v1/analytics/metrics/glossary')
  }

  /**
   * Get comprehensive system diagnostics and business KPIs
   */
  async getSystemDiagnostics(): Promise<SystemDiagnosticsSummary> {
    return this.request<SystemDiagnosticsSummary>('/api/v1/diagnostics/status')
  }

  /**
   * Get SQL repair trajectory for a specific query
   */
  async getRepairTrace(queryId: string): Promise<RepairTraceEntry[]> {
    return this.request<RepairTraceEntry[]>(`/api/v1/diagnostics/repair-trace/${queryId}`)
  }

  // ============================================================================
  // Governance API Methods
  // ============================================================================

  /**
   * Get audit summary statistics
   */
  async getGovernanceAuditSummary(): Promise<any> {
    return this.request('/api/v1/governance/audit/summary')
  }

  /**
   * Get audit activity logs
   */
  async getGovernanceAuditActivity(limit: number = 50, userFilter?: string, actionType?: string): Promise<any> {
    const params = new URLSearchParams()
    params.set('limit', limit.toString())
    if (userFilter) params.set('user_filter', userFilter)
    if (actionType) params.set('action_type', actionType)
    return this.request(`/api/v1/governance/audit/activity?${params.toString()}`)
  }

  /**
   * Get agent capabilities
   */
  async getAgentCapabilities(): Promise<any[]> {
    return this.request('/api/v1/governance/capabilities/agents')
  }

  /**
   * Get system capabilities
   */
  async getSystemCapabilities(): Promise<any[]> {
    return this.request('/api/v1/governance/capabilities/systems')
  }

  /**
   * Get misconfigurations and warnings
   */
  async getMisconfigurations(): Promise<any> {
    return this.request('/api/v1/governance/capabilities/misconfigurations')
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
