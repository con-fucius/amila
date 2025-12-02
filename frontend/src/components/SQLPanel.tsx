import { useState } from 'react'
import { Copy, ChevronDown, ChevronUp, Edit } from 'lucide-react'
import { Card, CardContent, CardHeader } from './ui/card'
import { Button } from './ui/button'
import { Badge } from './ui/badge'
import { Collapsible, CollapsibleContent, CollapsibleTrigger } from './ui/collapsible'
import { cn } from '@/utils/cn'

interface SQLPanelProps {
  sql: string
  status: 'generated' | 'executing' | 'executed' | 'error'
  confidence?: number
  onCopy?: () => void
  onEdit?: () => void
  compact?: boolean
}

const statusConfig = {
  generated: { label: 'Generated', color: 'bg-blue-100 text-blue-700 border-blue-300' },
  executing: { label: 'Executing', color: 'bg-yellow-100 text-yellow-700 border-yellow-300' },
  executed: { label: 'Executed', color: 'bg-green-100 text-green-700 border-green-300' },
  error: { label: 'Error', color: 'bg-red-100 text-red-700 border-red-300' },
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
  const config = statusConfig[status]

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
            </div>
            <div className="flex items-center gap-2">
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
          <CardContent className="pt-0 pb-2">
            <pre className="bg-gray-900 text-emerald-200 px-3 py-2 rounded-lg text-xs overflow-x-auto font-mono">
              {sql}
            </pre>
          </CardContent>
        </CollapsibleContent>
      </Collapsible>
    </Card>
  )
}
