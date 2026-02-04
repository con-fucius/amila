import { useState, useEffect } from 'react'

import { Card, CardContent } from './ui/card'
import { Badge } from './ui/badge'
import {
  TrendingUp,
  Users,
  DollarSign,
  Calendar,
  ChevronDown,
  ChevronUp
} from 'lucide-react'

interface QuerySuggestionsSimpleProps {
  show: boolean
  onSuggestionClick: (suggestion: string) => void
  currentInput: string
}

const QUERY_TEMPLATES = [
  {
    category: 'Aggregation',
    icon: DollarSign,
    color: 'bg-green-50 text-green-700 border-green-100 dark:bg-green-900/30 dark:text-green-300 dark:border-green-800',
    suggestions: [
      'Total {metric} by {dimension}',
      'Top 10 {dimension} by {metric}'
    ]
  },
  {
    category: 'Comparison',
    icon: Users,
    color: 'bg-blue-50 text-blue-700 border-blue-100 dark:bg-blue-900/30 dark:text-blue-300 dark:border-blue-800',
    suggestions: [
      'Compare {metric} by {dimension}',
      'Highest {metric} by {dimension}'
    ]
  },
  {
    category: 'Time Series',
    icon: Calendar,
    color: 'bg-purple-50 text-purple-700 border-purple-100 dark:bg-purple-900/30 dark:text-purple-300 dark:border-purple-800',
    suggestions: [
      '{metric} trends last year',
      'Monthly {metric} patterns'
    ]
  },
  {
    category: 'Filtering',
    icon: TrendingUp,
    color: 'bg-orange-50 text-orange-700 border-orange-100 dark:bg-orange-900/30 dark:text-orange-300 dark:border-orange-800',
    suggestions: [
      '{dimension} where {metric} > {value}',
      'Bottom 5 by {metric}'
    ]
  }
]

export function QuerySuggestionsSimple({ show, onSuggestionClick, currentInput }: QuerySuggestionsSimpleProps) {
  const [filteredTemplates, setFilteredTemplates] = useState(QUERY_TEMPLATES)
  const [isExpanded, setIsExpanded] = useState(false)

  useEffect(() => {
    if (!currentInput.trim()) {
      setFilteredTemplates(QUERY_TEMPLATES)
      return
    }

    const searchTerm = currentInput.toLowerCase()
    const filtered = QUERY_TEMPLATES.map(category => ({
      ...category,
      suggestions: category.suggestions.filter(suggestion =>
        suggestion.toLowerCase().includes(searchTerm) ||
        category.category.toLowerCase().includes(searchTerm)
      )
    })).filter(category => category.suggestions.length > 0)

    setFilteredTemplates(filtered)
  }, [currentInput])

  if (!show || filteredTemplates.length === 0) return null

  return (
    <Card className="absolute bottom-full left-0 right-0 mb-2 shadow-md border border-gray-100 dark:border-slate-700 bg-white/95 dark:bg-slate-900/95 backdrop-blur-sm z-50">
      <CardContent className="p-1.5">
        <button
          onClick={() => setIsExpanded(!isExpanded)}
          className="w-full flex items-center justify-between mb-1.5 hover:bg-gray-50 dark:hover:bg-slate-800 rounded px-1 py-0.5 transition-colors"
        >
          <div className="flex items-center gap-1">
            {isExpanded ? <ChevronUp size={10} className="text-gray-400" /> : <ChevronDown size={10} className="text-gray-400" />}
            <span className="text-[9px] font-medium text-gray-400 dark:text-gray-500 tracking-wide">
              {currentInput.trim() ? 'Suggestions' : 'Quickstart'}
            </span>
          </div>
          {!currentInput.trim() && (
            <span className="text-[8px] text-gray-400 dark:text-gray-500">Click to use</span>
          )}
        </button>

        {isExpanded && (
          <div className="space-y-1">
            {filteredTemplates.slice(0, 2).map((category) => (
              <div key={category.category}>
                <div className="flex items-center gap-1 mb-0.5">
                  <category.icon size={9} className="text-gray-400" />
                  <span className="text-[9px] font-medium text-gray-500 dark:text-gray-400">
                    {category.category}
                  </span>
                </div>

                <div className="flex flex-wrap gap-0.5">
                  {category.suggestions.slice(0, 2).map((suggestion, idx) => (
                    <Badge
                      key={idx}
                      variant="secondary"
                      className={`cursor-pointer hover:scale-[1.02] transition-all duration-150 text-[9px] py-0 px-1 ${category.color}`}
                      onClick={() => onSuggestionClick(suggestion)}
                    >
                      {suggestion}
                    </Badge>
                  ))}
                </div>
              </div>
            ))}
          </div>
        )}
      </CardContent>
    </Card>
  )
}