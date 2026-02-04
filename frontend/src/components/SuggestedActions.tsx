import { Lightbulb, TrendingUp, Download, BarChart3, ArrowRight, AlertTriangle } from 'lucide-react'
import { Card, CardContent } from './ui/card'
import { Button } from './ui/button'

interface SuggestedActionsProps {
  insights?: string[]
  anomalies?: any[]
  metrics?: any[]
  suggestedQueries?: string[]
  onQueryClick?: (query: string) => void
}

export function SuggestedActions({
  insights = [],
  anomalies = [],

  suggestedQueries = [],
  onQueryClick,
}: SuggestedActionsProps) {
  if (insights.length === 0 && suggestedQueries.length === 0 && anomalies.length === 0) {
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

        {/* Anomalies Section */}
        {anomalies.length > 0 && (
          <div className="mb-4">
            <div className="flex items-center gap-2 mb-3">
              <AlertTriangle className="h-4 w-4 text-orange-600" />
              <span className="font-semibold text-orange-900 text-sm">Detected Anomalies</span>
            </div>
            <div className="space-y-3">
              {anomalies.map((anomaly, idx) => (
                <div key={idx} className="bg-orange-100/50 border border-orange-200 rounded-lg p-3">
                  <div className="flex items-start gap-2 text-sm text-orange-900">
                    <span className="font-medium">{anomaly.message}</span>
                  </div>
                  {anomaly.evidence_row && (
                    <div className="mt-2 bg-white/80 rounded p-2 border border-orange-100 overflow-x-auto">
                      <div className="text-[10px] text-orange-600 font-bold uppercase mb-1 tracking-wider">Data Evidence:</div>
                      <table className="min-w-full text-[11px] font-mono text-gray-700">
                        <thead>
                          <tr>
                            {Object.keys(anomaly.evidence_row).map(key => (
                              <th key={key} className="text-left border-b border-orange-100 pb-1 pr-4">{key}</th>
                            ))}
                          </tr>
                        </thead>
                        <tbody>
                          <tr>
                            {Object.values(anomaly.evidence_row).map((val, i) => (
                              <td key={i} className="pt-1 pr-4 truncate max-w-[150px]">{String(val)}</td>
                            ))}
                          </tr>
                        </tbody>
                      </table>
                    </div>
                  )}
                </div>
              ))}
            </div>
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
