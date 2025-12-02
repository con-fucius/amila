import { useState, useEffect } from 'react'

import { Badge } from './ui/badge'
import { CheckCircle, AlertTriangle, Info, Database } from 'lucide-react'

interface QueryValidationIndicatorProps {
  query: string
  className?: string
}

interface ValidationResult {
  hasTableReference: boolean
  hasValidKeywords: boolean
  isComplete: boolean
  suggestions: string[]
  detectedTables: string[]
  detectedConcepts: string[]
}

const BUSINESS_KEYWORDS = [
  'revenue', 'sales', 'customer', 'sector', 'quarter', 'growth', 'total', 'count',
  'average', 'sum', 'max', 'min', 'by', 'show', 'calculate', 'compare', 'trend',
  'top', 'bottom', 'highest', 'lowest', 'each', 'per', 'group', 'filter', 'where'
]

export function QueryValidationIndicator({ query, className = '' }: QueryValidationIndicatorProps) {
  const [validation, setValidation] = useState<ValidationResult>({
    hasTableReference: false,
    hasValidKeywords: false,
    isComplete: false,
    suggestions: [],
    detectedTables: [],
    detectedConcepts: []
  })

  useEffect(() => {
    const validateQuery = () => {
      if (!query.trim()) {
        setValidation({
          hasTableReference: false,
          hasValidKeywords: false,
          isComplete: false,
          suggestions: ['Start typing to see suggestions...'],
          detectedTables: [],
          detectedConcepts: []
        })
        return
      }

      const queryLower = query.toLowerCase()

      // Detect business concepts (more flexible - no hardcoded tables)
      const detectedConcepts = BUSINESS_KEYWORDS.filter(keyword =>
        queryLower.includes(keyword)
      )

      // Detect potential table references (words in UPPERCASE or with underscores)
      const words = query.split(/\s+/)
      const detectedTables = words.filter(word => {
        const cleaned = word.replace(/[^A-Z_]/g, '')
        return cleaned.length > 2 && cleaned === cleaned.toUpperCase() && cleaned.includes('_')
      })

      // Validation logic - more lenient
      const hasTableReference = detectedTables.length > 0 || query.length > 15
      const hasValidKeywords = detectedConcepts.length > 0
      const isComplete = hasValidKeywords && query.length > 10

      // Generate suggestions
      const suggestions: string[] = []
      if (!hasValidKeywords) {
        suggestions.push('Add business terms like "total", "count", "by", or "top 10"')
      }
      if (query.length < 10) {
        suggestions.push('Add more details to your query')
      }
      if (isComplete) {
        suggestions.push('Query looks good! Press Enter to submit.')
      }

      setValidation({
        hasTableReference,
        hasValidKeywords,
        isComplete,
        suggestions,
        detectedTables,
        detectedConcepts
      })
    }

    // Debounce validation
    const timeoutId = setTimeout(validateQuery, 300)
    return () => clearTimeout(timeoutId)
  }, [query])

  if (!query.trim()) return null

  return (
    <div className={`flex items-center gap-2 text-xs ${className}`}>
      {/* Table Detection */}
      {validation.detectedTables.length > 0 && (
        <Badge variant="secondary" className="bg-blue-50 text-blue-700 border-blue-200">
          <Database size={10} className="mr-1" />
          {validation.detectedTables.length} table{validation.detectedTables.length > 1 ? 's' : ''}
        </Badge>
      )}

      {/* Concept Detection */}
      {validation.detectedConcepts.length > 0 && (
        <Badge variant="secondary" className="bg-green-50 text-green-700 border-green-200">
          <Info size={10} className="mr-1" />
          {validation.detectedConcepts.slice(0, 2).join(', ')}
        </Badge>
      )}

      {/* Validation Status */}
      {validation.isComplete ? (
        <Badge variant="secondary" className="bg-emerald-50 text-emerald-700 border-emerald-200">
          <CheckCircle size={10} className="mr-1" />
          Ready
        </Badge>
      ) : (
        <Badge variant="secondary" className="bg-amber-50 text-amber-700 border-amber-200">
          <AlertTriangle size={10} className="mr-1" />
          Add details
        </Badge>
      )}

      {/* Suggestions */}
      {validation.suggestions.length > 0 && (
        <span className="text-gray-500 ml-2">
          {validation.suggestions[0]}
        </span>
      )}
    </div>
  )
}