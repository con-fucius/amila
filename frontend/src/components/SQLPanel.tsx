import { useState } from 'react'
import { Copy, ChevronDown, ChevronUp, Edit, Activity } from 'lucide-react'
import { Card, CardContent, CardHeader } from './ui/card'
import { Button } from './ui/button'
import { Badge } from './ui/badge'
import { Collapsible, CollapsibleContent, CollapsibleTrigger } from './ui/collapsible'
import { cn } from '@/utils/cn'
import { estimateCost, type CostEstimate } from '@/utils/sqlAnalyzer'

interface SQLPanelProps {
  sql: string
  status: 'generated' | 'executing' | 'executed' | 'error'
  confidence?: number
  onCopy?: () => void
  onEdit?: () => void
  compact?: boolean
}

const statusConfig = {
  generated: { label: 'Generated', color: 'bg-blue-100 text-blue-700 border-blue-300 dark:bg-blue-900/30 dark:text-blue-300 dark:border-blue-700' },
  executing: { label: 'Executing', color: 'bg-yellow-100 text-yellow-700 border-yellow-300 dark:bg-yellow-900/30 dark:text-yellow-300 dark:border-yellow-700' },
  executed: { label: 'Executed', color: 'bg-green-100 text-green-700 border-green-300 dark:bg-green-900/30 dark:text-green-300 dark:border-green-700' },
  error: { label: 'Error', color: 'bg-red-100 text-red-700 border-red-300 dark:bg-red-900/30 dark:text-red-300 dark:border-red-700' },
}

export function SQLPanel({ 
  sql, 
  status, 
  confidence, 
  onCopy, 
  onEdit,
  compact = false 
}: SQLPanelProps) {
  const [isOpen, setIsOpen] = useState(!compact)
  const [showCost, setShowCost] = useState(false)
  const config = statusConfig[status]
  
  // Estimate query cost
  const costEstimate: CostEstimate = estimateCost(sql)
  
  const getCostColor = (complexity: string) => {
    switch (complexity) {
      case 'High': return 'text-red-600 dark:text-red-400'
      case 'Medium': return 'text-yellow-600 dark:text-yellow-400'
      default: return 'text-green-600 dark:text-green-400'
    }
  }

  return (
    <Card className="bg-white/80 dark:bg-slate-950/80 border border-gray-200 dark:border-slate-800 backdrop-blur-md shadow-sm">
      <Collapsible open={isOpen} onOpenChange={setIsOpen}>
        <CardHeader className="py-2 pb-2">
          <div className="flex items-center justify-between">
            <div className="flex items-center gap-2">
              <div className="text-sm font-semibold">SQL Query</div>
              <Badge variant="outline" className={cn("text-xs", config.color)}>
                {config.label}
              </Badge>
              {confidence !== undefined && (
                <Badge variant="outline" className="text-xs">
                  {confidence}% confidence
                </Badge>
              )}
              {showCost && (
                <Badge variant="outline" className={cn("text-xs", getCostColor(costEstimate.complexity))}>
                  {costEstimate.complexity} Complexity
                </Badge>
              )}
            </div>
            <div className="flex items-center gap-2">
              {status === 'generated' && (
                <Button
                  variant="ghost"
                  size="sm"
                  onClick={() => setShowCost(!showCost)}
                  className="h-8 px-2"
                  title="Preview query cost estimate"
                >
                  <Activity className="h-4 w-4 mr-1" />
                  {showCost ? 'Hide' : 'Preview'} Cost
                </Button>
              )}
              {onEdit && (
                <Button
                  variant="ghost"
                  size="sm"
                  onClick={onEdit}
                  className="h-8 px-2"
                >
                  <Edit className="h-4 w-4 mr-1" />
                  Edit
                </Button>
              )}
              {onCopy && (
                <Button
                  variant="ghost"
                  size="sm"
                  onClick={onCopy}
                  className="h-8 px-2"
                >
                  <Copy className="h-4 w-4 mr-1" />
                  Copy
                </Button>
              )}
              <CollapsibleTrigger asChild>
                <Button variant="ghost" size="sm" className="h-8 w-8 p-0">
                  {isOpen ? (
                    <ChevronUp className="h-4 w-4" />
                  ) : (
                    <ChevronDown className="h-4 w-4" />
                  )}
                </Button>
              </CollapsibleTrigger>
            </div>
          </div>
        </CardHeader>
        <CollapsibleContent>
          <CardContent className="pt-0 pb-2 space-y-2">
            {showCost && (
              <div className="bg-gray-50 dark:bg-slate-900/50 border border-gray-200 dark:border-slate-700 rounded-lg p-3 text-xs space-y-2">
                <div className="font-semibold text-gray-700 dark:text-gray-200">Cost Estimate (Frontend Analysis)</div>
                <div className="grid grid-cols-3 gap-2">
                  <div>
                    <div className="text-gray-500 dark:text-gray-400">Complexity</div>
                    <div className={cn("font-semibold", getCostColor(costEstimate.complexity))}>
                      {costEstimate.complexity}
                    </div>
                  </div>
                  <div>
                    <div className="text-gray-500 dark:text-gray-400">Cost Score</div>
                    <div className="font-semibold text-gray-700 dark:text-gray-200">{costEstimate.cost}/100</div>
                  </div>
                  <div>
                    <div className="text-gray-500 dark:text-gray-400">Est. Scan</div>
                    <div className="font-semibold text-gray-700 dark:text-gray-200">{costEstimate.bytesScannedEstimate}</div>
                  </div>
                </div>
                <div className="text-gray-600 dark:text-gray-300 pt-1 border-t border-gray-200 dark:border-slate-700">
                  {costEstimate.reason}
                </div>
                <div className="text-[10px] text-gray-400 dark:text-gray-500 italic">
                  Note: This is a frontend estimate. For accurate costs, run EXPLAIN PLAN on the database.
                </div>
              </div>
            )}
            <pre className="bg-gray-900 text-emerald-200 px-3 py-2 rounded-lg text-xs overflow-x-auto font-mono">
              {sql}
            </pre>
          </CardContent>
        </CollapsibleContent>
      </Collapsible>
    </Card>
  )
}
