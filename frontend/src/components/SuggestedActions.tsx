import { Lightbulb, TrendingUp, Download, BarChart3, ArrowRight } from 'lucide-react'
import { Card, CardContent } from './ui/card'
import { Button } from './ui/button'

interface SuggestedActionsProps {
  insights?: string[]
  suggestedQueries?: string[]
  onQueryClick?: (query: string) => void
}

export function SuggestedActions({
  insights = [],
  suggestedQueries = [],
  onQueryClick,
}: SuggestedActionsProps) {
  if (insights.length === 0 && suggestedQueries.length === 0) {
    return null
  }

  return (
    <Card className="border-blue-200 bg-blue-50">
      <CardContent className="p-4">
        {/* Insights Section */}
        {insights.length > 0 && (
          <div className="mb-4">
            <div className="flex items-center gap-2 mb-3">
              <Lightbulb className="h-4 w-4 text-blue-600" />
              <span className="font-semibold text-blue-900 text-sm">Key Insights</span>
            </div>
            <ul className="space-y-2">
              {insights.map((insight, idx) => (
                <li key={idx} className="flex items-start gap-2 text-sm text-blue-800">
                  <span className="text-blue-600 mt-0.5">- </span>
                  <span>{insight}</span>
                </li>
              ))}
            </ul>
          </div>
        )}

        {/* Suggested Actions */}
        {suggestedQueries.length > 0 && (
          <div>
            <div className="flex items-center gap-2 mb-3">
              <TrendingUp className="h-4 w-4 text-green-600" />
              <span className="font-semibold text-gray-900 text-sm">What would you like to do next?</span>
            </div>
            <div className="flex flex-wrap gap-2">
              {suggestedQueries.map((query, idx) => (
                <Button
                  key={idx}
                  variant="outline"
                  size="sm"
                  onClick={() => onQueryClick?.(query)}
                  className="border-green-300 hover:bg-green-50 hover:border-green-400 text-xs"
                >
                  {getActionIcon(query)}
                  <span className="ml-1">{query}</span>
                  <ArrowRight className="h-3 w-3 ml-1 opacity-60" />
                </Button>
              ))}
            </div>
          </div>
        )}
      </CardContent>
    </Card>
  )
}

function getActionIcon(query: string) {
  const lowerQuery = query.toLowerCase()
  
  if (lowerQuery.includes('export') || lowerQuery.includes('download') || lowerQuery.includes('csv')) {
    return <Download className="h-3 w-3" />
  }
  if (lowerQuery.includes('chart') || lowerQuery.includes('visualiz') || lowerQuery.includes('graph')) {
    return <BarChart3 className="h-3 w-3" />
  }
  if (lowerQuery.includes('trend') || lowerQuery.includes('compar')) {
    return <TrendingUp className="h-3 w-3" />
  }
  
  return null
}
