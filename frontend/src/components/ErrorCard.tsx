import { useState, useEffect, useMemo } from 'react'
import { useNavigate } from 'react-router-dom'
import { Card, CardContent } from './ui/card'
import { Button } from './ui/button'
import { AlertTriangle, RefreshCw, ChevronDown, ChevronUp, Copy, Check, XCircle, Lightbulb } from 'lucide-react'
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuTrigger,
} from './ui/dropdown-menu'
import { Badge } from './ui/badge'
import { cn } from '@/utils/cn'

export type ErrorSeverity = 'error' | 'warning' | 'info'

export interface ErrorCardProps {
  title?: string
  message: string
  severity?: ErrorSeverity
  details?: {
    code?: string
    stage?: string
    sql?: string
    timestamp?: string
    correlationId?: string
  }
  onRetry?: () => void
  onDismiss?: () => void
  retryLabel?: string
  isRetrying?: boolean
  className?: string
  schemaColumns?: string[] // Available columns for fuzzy matching
  errorTaxonomy?: {
    category: string
    title: string
    hint: string
    steps: string[]
  }
}

interface SelfHealSuggestion {
  type: 'column_typo' | 'missing_table' | 'syntax_error' | 'permission_error'
  message: string
  suggestion: string
  fixedSql?: string
}

const severityStyles: Record<ErrorSeverity, { border: string; bg: string; icon: string; title: string }> = {
  error: {
    border: 'border-red-300 dark:border-red-500',
    bg: 'bg-red-50 dark:bg-red-950/70',
    icon: 'text-red-600 dark:text-red-400',
    title: 'text-red-900 dark:text-red-100',
  },
  warning: {
    border: 'border-orange-300 dark:border-orange-500',
    bg: 'bg-orange-50 dark:bg-orange-950/70',
    icon: 'text-orange-600 dark:text-orange-400',
    title: 'text-orange-900 dark:text-orange-100',
  },
  info: {
    border: 'border-blue-300 dark:border-blue-500',
    bg: 'bg-blue-50 dark:bg-blue-950/70',
    icon: 'text-blue-600 dark:text-blue-400',
    title: 'text-blue-900 dark:text-blue-100',
  },
}

// Levenshtein distance for fuzzy matching
function levenshteinDistance(a: string, b: string): number {
  const matrix: number[][] = []

  for (let i = 0; i <= b.length; i++) {
    matrix[i] = [i]
  }

  for (let j = 0; j <= a.length; j++) {
    matrix[0][j] = j
  }

  for (let i = 1; i <= b.length; i++) {
    for (let j = 1; j <= a.length; j++) {
      if (b.charAt(i - 1) === a.charAt(j - 1)) {
        matrix[i][j] = matrix[i - 1][j - 1]
      } else {
        matrix[i][j] = Math.min(
          matrix[i - 1][j - 1] + 1,
          matrix[i][j - 1] + 1,
          matrix[i - 1][j] + 1
        )
      }
    }
  }

  return matrix[b.length][a.length]
}

function analyzeSQLError(message: string, sql?: string, schemaColumns?: string[]): SelfHealSuggestion | null {
  const msgLower = message.toLowerCase()

  // Missing column error
  if (msgLower.includes('column') && (msgLower.includes('not found') || msgLower.includes('unknown') || msgLower.includes('invalid'))) {
    const columnMatch = message.match(/['"`]?(\w+)['"`]?/i)
    if (columnMatch && schemaColumns && schemaColumns.length > 0) {
      const missingColumn = columnMatch[1]

      // Find closest match using Levenshtein distance
      let closestMatch = ''
      let minDistance = Infinity

      schemaColumns.forEach(col => {
        const distance = levenshteinDistance(missingColumn.toLowerCase(), col.toLowerCase())
        if (distance < minDistance && distance <= 3) { // Max 3 character difference
          minDistance = distance
          closestMatch = col
        }
      })

      if (closestMatch && sql) {
        return {
          type: 'column_typo',
          message: `Column '${missingColumn}' not found`,
          suggestion: `Did you mean '${closestMatch}'?`,
          fixedSql: sql.replace(new RegExp(`\\b${missingColumn}\\b`, 'gi'), closestMatch)
        }
      }
    }
  }

  // Missing table error
  if (msgLower.includes('table') && (msgLower.includes('not found') || msgLower.includes('does not exist'))) {
    return {
      type: 'missing_table',
      message: 'Table not found in database',
      suggestion: 'Check the Schema Browser for available tables'
    }
  }

  // Syntax error
  if (msgLower.includes('syntax error') || msgLower.includes('near')) {
    return {
      type: 'syntax_error',
      message: 'SQL syntax error detected',
      suggestion: 'Review the SQL syntax or try rephrasing your question'
    }
  }

  // Permission error
  if (msgLower.includes('permission') || msgLower.includes('access denied') || msgLower.includes('insufficient privileges')) {
    return {
      type: 'permission_error',
      message: 'Insufficient permissions',
      suggestion: 'Contact your administrator for access to this resource'
    }
  }

  return null
}

export function ErrorCard({
  title = 'Error',
  message,
  severity = 'error',
  details,
  onRetry,
  onDismiss,
  retryLabel = 'Retry',
  isRetrying = false,
  className,
  schemaColumns = [],
  errorTaxonomy,
}: ErrorCardProps) {
  const [showDetails, setShowDetails] = useState(false)
  const [copied, setCopied] = useState(false)
  const [suggestion, setSuggestion] = useState<SelfHealSuggestion | null>(null)
  const styles = severityStyles[severity]
  const navigate = useNavigate()

  // Analyze error for self-heal suggestions
  useEffect(() => {
    const analyzed = analyzeSQLError(message, details?.sql, schemaColumns)
    setSuggestion(analyzed)
  }, [message, details?.sql, schemaColumns])

  const handleCopyError = async () => {
    const errorInfo = {
      title,
      message,
      severity,
      ...details,
      timestamp: details?.timestamp || new Date().toISOString(),
    }
    try {
      await navigator.clipboard.writeText(JSON.stringify(errorInfo, null, 2))
      setCopied(true)
      setTimeout(() => setCopied(false), 2000)
    } catch (err) {
      console.error('Failed to copy error:', err)
    }
  }

  const handleRetryWithFix = () => {
    if (suggestion?.fixedSql && onRetry) {
      onRetry()
    }
  }

  const hasDetails = details && (details.code || details.stage || details.sql || details.correlationId)

  const recoveryActions = useMemo(() => {
    const actions: Array<{ label: string; onClick: () => void }> = []
    const category = errorTaxonomy?.category

    if (category === 'invalid_identifier') {
      actions.push({
        label: 'Open Schema Browser',
        onClick: () => navigate('/schema-browser'),
      })
    }
    if (category === 'syntax_error') {
      const sql = details?.sql
      actions.push({
        label: 'Open Query Builder',
        onClick: () => navigate(sql ? `/query-builder?sql=${encodeURIComponent(sql)}` : '/query-builder'),
      })
    }
    if (category === 'connection_error' || category === 'network_error') {
      if (onRetry) {
        actions.push({ label: 'Retry', onClick: onRetry })
      }
    }
    if (category === 'permission_denied') {
      actions.push({
        label: 'Open Settings',
        onClick: () => navigate('/settings'),
      })
    }
    return actions
  }, [errorTaxonomy?.category, details?.sql, onRetry, navigate])

  return (
    <Card className={cn(styles.border, styles.bg, className)}>
      <CardContent className="p-3">
        <div className="flex items-start gap-2.5">
          {severity === 'error' ? (
            <XCircle className={cn('h-4 w-4 flex-shrink-0 mt-0.5', styles.icon)} />
          ) : (
            <AlertTriangle className={cn('h-4 w-4 flex-shrink-0 mt-0.5', styles.icon)} />
          )}
          <div className="flex-1 min-w-0">
            <div className="flex items-start justify-between gap-2">
              <div className={cn('text-xs font-semibold mb-0.5', styles.title)}>{title}</div>
              {onDismiss && (
                <Button
                  variant="ghost"
                  size="icon"
                  className="h-5 w-5 -mt-0.5 -mr-1"
                  onClick={onDismiss}
                >
                  <XCircle className="h-3.5 w-3.5" />
                </Button>
              )}
            </div>
            <div className="text-[13px] text-gray-700 dark:text-gray-200 whitespace-pre-wrap break-words leading-tight">
              {message}
            </div>

            {/* Error Taxonomy & Troubleshooting */}
            {errorTaxonomy && (
              <div className="mt-3 space-y-3">
                <div className="flex items-center gap-2">
                  <Badge variant="outline" className="text-[10px] uppercase tracking-wider bg-slate-100 dark:bg-slate-800 text-slate-500">
                    {errorTaxonomy.category}
                  </Badge>
                  <span className="text-xs font-semibold text-slate-700 dark:text-slate-300">
                    {errorTaxonomy.title}
                  </span>
                </div>

                <div className="p-3 bg-blue-500/5 border border-blue-500/20 rounded-md">
                  <div className="flex items-start gap-2">
                    <Lightbulb className="h-4 w-4 text-blue-500 flex-shrink-0 mt-0.5" />
                    <div>
                      <div className="text-xs font-semibold text-blue-600 dark:text-blue-400 mb-1">Actionable Hint</div>
                      <div className="text-xs text-slate-600 dark:text-slate-400">{errorTaxonomy.hint}</div>
                    </div>
                  </div>
                </div>

                {errorTaxonomy.steps && errorTaxonomy.steps.length > 0 && (
                  <div className="space-y-1.5 px-1">
                    <div className="text-[11px] font-bold text-slate-500 uppercase tracking-tight">Troubleshooting Steps</div>
                    <ul className="space-y-1">
                      {errorTaxonomy.steps.map((step, idx) => (
                        <li key={idx} className="text-xs text-slate-600 dark:text-slate-400 flex items-start gap-2">
                          <span className="w-4 h-4 rounded-full bg-slate-200 dark:bg-slate-800 flex items-center justify-center text-[10px] flex-shrink-0 mt-0.5">
                            {idx + 1}
                          </span>
                          {step}
                        </li>
                      ))}
                    </ul>
                  </div>
                )}
              </div>
            )}

            {/* Self-Heal Suggestion */}
            {suggestion && (
              <div className="mt-3 p-3 bg-amber-50 dark:bg-amber-950/30 border border-amber-200 dark:border-amber-800 rounded-md">
                <div className="flex items-start gap-2">
                  <Lightbulb className="h-4 w-4 text-amber-600 dark:text-amber-400 flex-shrink-0 mt-0.5" />
                  <div className="flex-1 min-w-0">
                    <div className="text-xs font-semibold text-amber-900 dark:text-amber-100 mb-1">
                      Suggestion
                    </div>
                    <div className="text-xs text-amber-800 dark:text-amber-200">
                      {suggestion.suggestion}
                    </div>
                    {suggestion.fixedSql && (
                      <Button
                        onClick={handleRetryWithFix}
                        size="sm"
                        variant="outline"
                        disabled={isRetrying}
                        className="mt-2 text-xs border-amber-300 dark:border-amber-700 hover:bg-amber-100 dark:hover:bg-amber-900/50"
                      >
                        <RefreshCw className={cn('h-3 w-3 mr-1.5', isRetrying && 'animate-spin')} />
                        Retry with Fix
                      </Button>
                    )}
                  </div>
                </div>
              </div>
            )}

            {/* Action buttons */}
            <div className="flex items-center gap-1.5 mt-2">
              {onRetry && (
                <Button
                  onClick={onRetry}
                  size="sm"
                  variant="outline"
                  disabled={isRetrying}
                  className={cn('h-7 text-[11px] px-2.5', styles.border, 'hover:bg-white/50 dark:hover:bg-slate-800/50')}
                >
                  <RefreshCw className={cn('h-3 w-3 mr-1.5', isRetrying && 'animate-spin')} />
                  {isRetrying ? 'Retrying...' : retryLabel}
                </Button>
              )}
              {recoveryActions.map((action) => (
                <Button
                  key={action.label}
                  onClick={action.onClick}
                  size="sm"
                  variant="ghost"
                  className="h-7 text-[11px] px-2.5"
                >
                  {action.label}
                </Button>
              ))}

              <DropdownMenu>
                <DropdownMenuTrigger asChild>
                  <Button variant="ghost" size="sm" className="h-7 px-1.5 text-gray-500 hover:text-gray-900 gap-1">
                    <span className="text-[11px]">Actions</span>
                    <ChevronDown className="h-3 w-3 opacity-50" />
                  </Button>
                </DropdownMenuTrigger>
                <DropdownMenuContent align="start" className="w-40 border-gray-200 dark:border-slate-800">
                  {onRetry && (
                    <DropdownMenuItem onClick={onRetry} disabled={isRetrying} className="text-xs gap-2 py-2 cursor-pointer">
                      <RefreshCw className={cn('h-3.5 w-3.5 text-gray-500', isRetrying && 'animate-spin')} />
                      <span>{isRetrying ? 'Retrying...' : 'Retry query'}</span>
                    </DropdownMenuItem>
                  )}
                  {hasDetails && (
                    <DropdownMenuItem onClick={() => setShowDetails(!showDetails)} className="text-xs gap-2 py-2 cursor-pointer">
                      {showDetails ? <ChevronUp className="h-3.5 w-3.5 text-gray-500" /> : <ChevronDown className="h-3.5 w-3.5 text-gray-500" />}
                      <span>{showDetails ? 'Hide details' : 'Show details'}</span>
                    </DropdownMenuItem>
                  )}
                  <DropdownMenuItem onClick={handleCopyError} className="text-xs gap-2 py-2 cursor-pointer">
                    {copied ? <Check className="h-3.5 w-3.5 text-green-600" /> : <Copy className="h-3.5 w-3.5 text-gray-500" />}
                    <span>{copied ? 'Copied' : 'Copy error'}</span>
                  </DropdownMenuItem>
                </DropdownMenuContent>
              </DropdownMenu>
            </div>

            {/* Expandable details */}
            {showDetails && hasDetails && (
              <div className="mt-3 p-3 bg-white/50 dark:bg-slate-900/50 rounded-md text-xs space-y-2">
                {details.code && (
                  <div>
                    <span className="font-semibold text-gray-600 dark:text-gray-400">Error Code:</span>{' '}
                    <span className="font-mono">{details.code}</span>
                  </div>
                )}
                {details.stage && (
                  <div>
                    <span className="font-semibold text-gray-600 dark:text-gray-400">Failed at:</span>{' '}
                    <span className="font-mono">{details.stage}</span>
                  </div>
                )}
                {details.correlationId && (
                  <div>
                    <span className="font-semibold text-gray-600 dark:text-gray-400">Correlation ID:</span>{' '}
                    <span className="font-mono text-[10px]">{details.correlationId}</span>
                  </div>
                )}
                {details.sql && (
                  <div>
                    <span className="font-semibold text-gray-600 dark:text-gray-400 block mb-1">SQL Attempted:</span>
                    <pre className="font-mono text-[10px] bg-gray-100 dark:bg-slate-800 p-2 rounded overflow-x-auto max-h-24 whitespace-pre-wrap">
                      {details.sql}
                    </pre>
                  </div>
                )}
                {details.timestamp && (
                  <div className="text-[10px] text-gray-500 dark:text-gray-500">
                    {new Date(details.timestamp).toLocaleString()}
                  </div>
                )}
              </div>
            )}
          </div>
        </div>
      </CardContent>
    </Card>
  )
}
