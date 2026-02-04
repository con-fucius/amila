import { useState } from 'react'
import { Dialog, DialogContent, DialogHeader, DialogTitle, DialogDescription, DialogFooter } from './ui/dialog'
import { Button } from './ui/button'
import { Alert, AlertDescription, AlertTitle } from './ui/alert'
import { Badge } from './ui/badge'
import { Textarea } from './ui/textarea'
import { Info, Plus } from 'lucide-react'

import type { DatabaseType } from '@/types/domain'

interface ClarificationDialogProps {
  open: boolean
  onOpenChange: (open: boolean) => void
  originalQuery: string
  message: string
  details?: {
    unmapped_concepts?: Array<{ concept: string, note?: string }>
    referenced_tables?: Array<{ name: string, columns: string[] }>
    reason?: string
    previous_clarifications?: string[]
    attempt_number?: number
    [key: string]: any
  }
  databaseType?: DatabaseType
  onSubmit: (clarification: string) => Promise<void> | void
}

export function ClarificationDialog({ open, onOpenChange, originalQuery, message, details, onSubmit }: ClarificationDialogProps) {
  const [clarification, setClarification] = useState('')
  const [submitting, setSubmitting] = useState(false)

  const handleSubmit = async () => {
    if (!clarification.trim()) return
    setSubmitting(true)
    try {
      await onSubmit(clarification.trim())
      setClarification('')
      onOpenChange(false)
    } finally {
      setSubmitting(false)
    }
  }

  const handleColumnClick = (columnName: string, concept?: string) => {
    const currentText = clarification
    let newText = currentText

    if (concept) {
      // Smart insertion: "Use COLUMN_NAME for concept"
      const insertion = `Use ${columnName} for ${concept}`
      newText = currentText ? `${currentText}. ${insertion}` : insertion
    } else {
      // Simple column name insertion
      newText = currentText ? `${currentText} ${columnName}` : columnName
    }

    setClarification(newText)
  }

  const getAvailableColumns = () => {
    if (!details?.referenced_tables) return []
    return details.referenced_tables.flatMap(table =>
      table.columns.map(col => ({ name: col, table: table.name }))
    )
  }

  const getUnmappedConcepts = () => {
    if (!details?.unmapped_concepts) return []
    return Array.isArray(details.unmapped_concepts)
      ? details.unmapped_concepts.map(c => typeof c === 'string' ? { concept: c } : c)
      : []
  }

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="max-w-2xl">
        <DialogHeader>
          <DialogTitle>We need a quick clarification</DialogTitle>
          <DialogDescription>
            To generate correct SQL, please clarify the following.
          </DialogDescription>
        </DialogHeader>

        <div className="space-y-4">
          <div>
            <div className="text-sm font-semibold text-gray-700 dark:text-gray-100 mb-2">Your original query</div>
            <div className="p-3 bg-gray-50 dark:bg-slate-900/70 border border-gray-200 dark:border-slate-700 rounded-lg text-xs text-gray-800 dark:text-gray-100 whitespace-pre-wrap">
              {originalQuery}
            </div>
          </div>

          <Alert className="bg-blue-50 border-blue-200 dark:bg-slate-900/80 dark:border-slate-700">
            <Info className="h-4 w-4 text-blue-700 dark:text-blue-300" />
            <AlertTitle className="text-blue-900 dark:text-blue-100">
              {details?.attempt_number && details.attempt_number > 1
                ? `Additional clarification needed (attempt ${details.attempt_number})`
                : 'What we need from you'}
            </AlertTitle>
            <AlertDescription className="text-blue-800 dark:text-blue-100 text-sm">
              {message}
            </AlertDescription>
            {details?.reason && (
              <div className="mt-2 text-xs text-blue-900 dark:text-blue-100">
                {details.reason}
              </div>
            )}
          </Alert>

          {details?.previous_clarifications && details.previous_clarifications.length > 0 && (
            <div className="text-sm">
              <div className="font-medium text-gray-700 dark:text-gray-100 mb-2">Previous clarifications provided:</div>
              <div className="space-y-1 bg-gray-100 dark:bg-slate-800 rounded-lg p-2">
                {details.previous_clarifications.map((prev, i) => (
                  <div key={i} className="text-xs text-gray-600 dark:text-gray-300 flex items-start gap-2">
                    <span className="text-green-500">[OK]</span>
                    <span>{prev}</span>
                  </div>
                ))}
              </div>
              <div className="text-xs text-gray-500 dark:text-gray-400 mt-1">
                The system still needs more information. Please provide additional details below.
              </div>
            </div>
          )}

          {getUnmappedConcepts().length > 0 && (
            <div className="text-sm">
              <div className="font-medium text-gray-700 dark:text-gray-100 mb-2">Unmapped concepts:</div>
              <div className="flex flex-wrap gap-2">
                {getUnmappedConcepts().map((concept, i) => (
                  <Badge key={i} variant="destructive" className="text-xs">
                    {concept.concept}
                  </Badge>
                ))}
              </div>
            </div>
          )}

          {details?.referenced_tables && details.referenced_tables.length > 0 && (
            <div className="text-sm">
              <div className="font-medium text-gray-700 dark:text-gray-100 mb-2">Tables &amp; columns considered (click to use):</div>
              <div className="space-y-2 max-h-40 overflow-y-auto pr-1">
                {details.referenced_tables.map((table, i) => {
                  const cols = Array.isArray(table.columns) ? table.columns : []
                  if (!cols.length) return null
                  return (
                    <div
                      key={i}
                      className="border border-gray-200 dark:border-slate-700 rounded-md bg-white/70 dark:bg-slate-900/70 px-2 py-1.5"
                    >
                      <div className="flex items-center justify-between mb-1">
                        <span className="text-xs font-semibold text-gray-800 dark:text-gray-100">{table.name}</span>
                        <span className="text-[10px] text-gray-500 dark:text-gray-400">{cols.length} columns</span>
                      </div>
                      <div className="flex flex-wrap gap-1">
                        {cols.map((colName: string, j: number) => (
                          <Badge
                            key={j}
                            variant="secondary"
                            className="cursor-pointer hover:bg-blue-100 text-[10px]"
                            onClick={() => handleColumnClick(colName)}
                          >
                            <Plus className="h-3 w-3 mr-1" />
                            {colName}
                          </Badge>
                        ))}
                      </div>
                    </div>
                  )
                })}
              </div>
              <div className="text-xs text-gray-500 dark:text-gray-400 mt-1">
                Click any column to add it to your clarification
              </div>
            </div>
          )}

          {getUnmappedConcepts().length > 0 && getAvailableColumns().length > 0 && (
            <div className="text-sm">
              <div className="font-medium text-gray-700 dark:text-gray-100 mb-2">Quick mappings:</div>
              <div className="space-y-1">
                {getUnmappedConcepts().slice(0, 3).map((concept, i) => (
                  <div key={i} className="flex items-center gap-2 text-xs">
                    <span className="text-gray-600 dark:text-gray-300">Map "{concept.concept}" to:</span>
                    <div className="flex gap-1">
                      {getAvailableColumns().slice(0, 4).map((col, j) => (
                        <Badge
                          key={j}
                          variant="outline"
                          className="cursor-pointer hover:bg-green-50 text-xs"
                          onClick={() => handleColumnClick(col.name, concept.concept)}
                        >
                          {col.name}
                        </Badge>
                      ))}
                    </div>
                  </div>
                ))}
              </div>
            </div>
          )}

          <div>
            <div className="text-sm font-medium text-gray-700 dark:text-gray-100 mb-1">Your clarification</div>
            <Textarea
              className="min-h-[100px] focus:ring-2 focus:ring-emerald-500"
              placeholder="Examples: Use REVENUE column for 'total sales'. 'customer_count' means COUNT(DISTINCT CUSTOMER_ID). Use DATE column in DD/MM/YYYY."
              value={clarification}
              onChange={(e) => setClarification(e.target.value)}
            />
            <div className="text-xs text-gray-500 mt-1">
              Tip: Click column names above to add them, or type your clarification manually.
            </div>
          </div>
        </div>

        <DialogFooter>
          <Button variant="outline" onClick={() => onOpenChange(false)}>Cancel</Button>
          <Button onClick={handleSubmit} disabled={submitting || !clarification.trim()} className="bg-gradient-to-r from-emerald-500 to-green-600 hover:from-emerald-600 hover:to-green-700">
            {submitting ? 'Submitting...' : 'Submit Clarification'}
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  )
}
