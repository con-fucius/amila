import { useState } from 'react'
import { ChevronDown, ChevronUp, Loader2 } from 'lucide-react'
import { Card, CardContent, CardHeader } from './ui/card'
import { Button } from './ui/button'
import { Collapsible, CollapsibleContent, CollapsibleTrigger } from './ui/collapsible'
import { SchemaPreview } from './SchemaPreview'
import { cn } from '@/utils/cn'
import type { ThinkingStep as DomainThinkingStep } from '@/types/domain'

interface Step {
  id: string
  label: string
  status: 'pending' | 'in-progress' | 'completed' | 'error'
}

interface TableSchema {
  tableName: string
  columns: Array<{
    columnName: string
    dataType: string
    nullable: boolean
  }>
  sampleRows?: Array<Record<string, any>>
  rowCount?: number
}

interface ProgressIndicatorProps {
  currentState: string
  steps: Step[]
  thinkingSteps?: DomainThinkingStep[]
  schemaData?: TableSchema[]
  intermediateData?: {
    mean?: number
    stddev?: number
    nullRatio?: number
    distinctValues?: number
  }
  visible?: boolean
}

export function ProgressIndicator({
  currentState,
  steps,
  thinkingSteps,
  schemaData,
  intermediateData,
  visible = true
}: ProgressIndicatorProps) {
  const [isExpanded, setIsExpanded] = useState(false) // Collapsed by default

  if (!visible) return null

  const safeSteps = Array.isArray(steps) ? steps : []
  const safeThinkingSteps = Array.isArray(thinkingSteps) ? thinkingSteps : []
  const safeSchemaData = Array.isArray(schemaData) ? schemaData : []

  const completedSteps = safeSteps.filter(s => s.status === 'completed').length
  const totalSteps = safeSteps.length
  const progress = totalSteps > 0 ? (completedSteps / totalSteps) * 100 : 0

  return (
    <Card className="border-blue-200 bg-blue-50">
      <Collapsible open={isExpanded} onOpenChange={setIsExpanded}>
        <CardHeader className="pb-3">
          <div className="flex items-center justify-between">
            <div className="flex items-center gap-3">
              <div className="relative w-8 h-8">
                <svg className="w-8 h-8 -rotate-90">
                  <circle
                    cx="16"
                    cy="16"
                    r="14"
                    stroke="currentColor"
                    strokeWidth="3"
                    fill="none"
                    className="text-gray-300"
                  />
                  <circle
                    cx="16"
                    cy="16"
                    r="14"
                    stroke="currentColor"
                    strokeWidth="3"
                    fill="none"
                    strokeDasharray={`${2 * Math.PI * 14}`}
                    strokeDashoffset={`${2 * Math.PI * 14 * (1 - progress / 100)}`}
                    className="text-blue-600 transition-all duration-500"
                  />
                </svg>
                <div className="absolute inset-0 flex items-center justify-center">
                  <Loader2 className="h-4 w-4 text-blue-600 animate-spin" />
                </div>
              </div>
              <div>
                <div className="text-sm font-semibold text-gray-800">
                  {currentState}
                </div>
                <div className="text-xs text-gray-600">
                  {completedSteps} of {totalSteps} steps completed
                </div>
              </div>
            </div>
            <CollapsibleTrigger asChild>
              <Button variant="ghost" size="sm">
                {isExpanded ? (
                  <>
                    Hide Details <ChevronUp className="ml-1 h-4 w-4" />
                  </>
                ) : (
                  <>
                    Show Details <ChevronDown className="ml-1 h-4 w-4" />
                  </>
                )}
              </Button>
            </CollapsibleTrigger>
          </div>
        </CardHeader>

        <CollapsibleContent>
          <CardContent className="pt-0 space-y-4">
            {/* Process Steps */}
            <div>
              <div className="text-xs font-semibold text-gray-600 mb-2">Process Steps</div>
              <div className="space-y-2">
                {safeSteps.map((step) => (
                  <div key={step.id} className="flex items-center gap-2">
                    <div className={cn(
                      "w-5 h-5 rounded-full flex items-center justify-center text-xs",
                      step.status === 'completed' && "bg-green-500 text-white",
                      step.status === 'in-progress' && "bg-blue-500 text-white",
                      step.status === 'pending' && "bg-gray-300 text-gray-600",
                      step.status === 'error' && "bg-red-500 text-white"
                    )}>
                      {step.status === 'completed' && ''}
                      {step.status === 'in-progress' && ''}
                      {step.status === 'pending' && ''}
                      {step.status === 'error' && ''}
                    </div>
                    <span className={cn(
                      "text-sm",
                      step.status === 'completed' && "text-gray-700",
                      step.status === 'in-progress' && "text-blue-700 font-medium",
                      step.status === 'pending' && "text-gray-500",
                      step.status === 'error' && "text-red-700"
                    )}>
                      {step.label}
                    </span>
                  </div>
                ))}
              </div>
            </div>

            {/* Schema Preview */}
            {safeSchemaData.length > 0 && (
              <div>
                <div className="text-xs font-semibold text-gray-600 mb-2">Schema Preview</div>
                <SchemaPreview schemas={safeSchemaData} />
              </div>
            )}

            {/* Thinking Steps (Chain of Thought) */}
            {safeThinkingSteps.length > 0 && (
              <div>
                <div className="text-xs font-semibold text-gray-600 mb-2">Chain of Thought</div>
                <div className="space-y-2 max-h-48 overflow-y-auto">
                  {safeThinkingSteps.map((thinking, idx) => {
                    const label = thinking.content || thinking.stage || thinking.name || `Step ${idx + 1}`
                    return (
                      <div key={idx} className="bg-white rounded p-2 border border-gray-200">
                        <div className="text-xs text-gray-700">{label}</div>
                        {thinking.timestamp && (
                          <div className="text-xs text-gray-400 mt-1">{thinking.timestamp}</div>
                        )}
                      </div>
                    )
                  })}
                </div>
              </div>
            )}

            {/* Intermediate Data Statistics */}
            {intermediateData && (
              <div>
                <div className="text-xs font-semibold text-gray-600 mb-2">Statistical Analysis</div>
                <div className="grid grid-cols-2 gap-2">
                  {intermediateData.mean !== undefined && (
                    <div className="bg-white rounded p-2 border border-gray-200">
                      <div className="text-xs text-gray-500">Mean</div>
                      <div className="text-sm font-semibold text-gray-800">{intermediateData.mean.toFixed(2)}</div>
                    </div>
                  )}
                  {intermediateData.stddev !== undefined && (
                    <div className="bg-white rounded p-2 border border-gray-200">
                      <div className="text-xs text-gray-500">Std Dev</div>
                      <div className="text-sm font-semibold text-gray-800">{intermediateData.stddev.toFixed(2)}</div>
                    </div>
                  )}
                  {intermediateData.nullRatio !== undefined && (
                    <div className="bg-white rounded p-2 border border-gray-200">
                      <div className="text-xs text-gray-500">Null Ratio</div>
                      <div className="text-sm font-semibold text-gray-800">{(intermediateData.nullRatio * 100).toFixed(1)}%</div>
                    </div>
                  )}
                  {intermediateData.distinctValues !== undefined && (
                    <div className="bg-white rounded p-2 border border-gray-200">
                      <div className="text-xs text-gray-500">Distinct Values</div>
                      <div className="text-sm font-semibold text-gray-800">{intermediateData.distinctValues}</div>
                    </div>
                  )}
                </div>
              </div>
            )}

          </CardContent>
        </CollapsibleContent>
      </Collapsible>
    </Card>
  )
}
