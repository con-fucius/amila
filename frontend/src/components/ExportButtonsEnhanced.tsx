import { useState } from 'react'
import { cn } from '@/utils/cn'

import { Button } from './ui/button'
import { Card, CardContent, CardHeader, CardTitle } from './ui/card'
import { Badge } from './ui/badge'
import {
  Download,
  FileText,
  Table,
  Code,
  Eye,
  ChevronDown,
  CheckCircle,
  Copy,
  Check
} from 'lucide-react'
import {
  Tooltip,
  TooltipContent,
  TooltipProvider,
  TooltipTrigger,
} from './ui/tooltip'

interface ExportData {
  columns: string[]
  rows: any[][]
  metadata?: {
    query_id?: string
    timestamp?: string
    row_count?: number
  }
}

interface ExportButtonsEnhancedProps {
  data: ExportData
  filename?: string
  className?: string
}

type ExportFormat = 'csv' | 'excel' | 'json'

export function ExportButtonsEnhanced({
  data,
  filename = 'query_results',
  className = ''
}: ExportButtonsEnhancedProps) {
  const [showOptions, setShowOptions] = useState(false)
  const [showPreview, setShowPreview] = useState(false)
  const [selectedFormat, setSelectedFormat] = useState<ExportFormat>('csv')
  const [exporting, setExporting] = useState(false)
  const [copySuccess, setCopySuccess] = useState(false)

  // Helper to escape CSV values properly
  const escapeCSVValue = (val: any): string => {
    if (val === null || val === undefined) return ''
    const str = String(val)
    // If contains comma, quote, or newline, wrap in quotes and escape internal quotes
    if (str.includes(',') || str.includes('"') || str.includes('\n') || str.includes('\r')) {
      return `"${str.replace(/"/g, '""')}"`
    }
    return str
  }

  // Generate CSV content with proper escaping
  const generateCSVContent = (): string => {
    const header = data.columns.map(escapeCSVValue).join(',')
    const rows = data.rows.map(row =>
      row.map(escapeCSVValue).join(',')
    ).join('\n')
    return `${header}\n${rows}`
  }

  const exportFormats = [
    {
      id: 'csv' as ExportFormat,
      name: 'CSV',
      description: 'Comma-separated values',
      icon: FileText,
      color: 'bg-green-100 text-green-800 border-green-200'
    },
    {
      id: 'excel' as ExportFormat,
      name: 'Excel',
      description: 'Microsoft Excel format',
      icon: Table,
      color: 'bg-blue-100 text-blue-800 border-blue-200'
    },
    {
      id: 'json' as ExportFormat,
      name: 'JSON',
      description: 'JavaScript Object Notation',
      icon: Code,
      color: 'bg-purple-100 text-purple-800 border-purple-200'
    }
  ]

  const generatePreview = (format: ExportFormat) => {
    const previewRows = data.rows.slice(0, 3)

    switch (format) {
      case 'csv':
        const csvHeader = data.columns.join(',')
        const csvRows = previewRows.map(row => row.join(',')).join('\n')
        return `${csvHeader}\n${csvRows}\n...`

      case 'json':
        const jsonData = previewRows.map(row => {
          const obj: any = {}
          data.columns.forEach((col, idx) => {
            obj[col] = row[idx]
          })
          return obj
        })
        return JSON.stringify(jsonData, null, 2).slice(0, 200) + '...'

      case 'excel':
        return `Excel file with ${data.rows.length} rows and ${data.columns.length} columns\nColumns: ${data.columns.join(', ')}`

      default:
        return 'Preview not available'
    }
  }

  const handleExport = async (format: ExportFormat) => {
    setExporting(true)

    try {
      const timestamp = new Date().toISOString().slice(0, 19).replace(/:/g, '-')
      const fullFilename = `${filename}_${timestamp}`

      switch (format) {
        case 'csv':
          const csvContent = [
            data.columns.join(','),
            ...data.rows.map(row => row.join(','))
          ].join('\n')

          const csvBlob = new Blob([csvContent], { type: 'text/csv;charset=utf-8;' })
          const csvUrl = URL.createObjectURL(csvBlob)
          const csvLink = document.createElement('a')
          csvLink.href = csvUrl
          csvLink.download = `${fullFilename}.csv`
          csvLink.click()
          URL.revokeObjectURL(csvUrl)
          break

        case 'excel':
          // Simple Excel export (would need xlsx library for full implementation)
          const excelData = [data.columns, ...data.rows]
          const worksheet = excelData.map(row => row.join('\t')).join('\n')
          const excelBlob = new Blob([worksheet], { type: 'application/vnd.ms-excel' })
          const excelUrl = URL.createObjectURL(excelBlob)
          const excelLink = document.createElement('a')
          excelLink.href = excelUrl
          excelLink.download = `${fullFilename}.xls`
          excelLink.click()
          URL.revokeObjectURL(excelUrl)
          break

        case 'json':
          const jsonData = data.rows.map(row => {
            const obj: any = {}
            data.columns.forEach((col, idx) => {
              obj[col] = row[idx]
            })
            return obj
          })

          const jsonContent = JSON.stringify({
            metadata: data.metadata,
            columns: data.columns,
            data: jsonData
          }, null, 2)

          const jsonBlob = new Blob([jsonContent], { type: 'application/json' })
          const jsonUrl = URL.createObjectURL(jsonBlob)
          const jsonLink = document.createElement('a')
          jsonLink.href = jsonUrl
          jsonLink.download = `${fullFilename}.json`
          jsonLink.click()
          URL.revokeObjectURL(jsonUrl)
          break
      }

      // Show success feedback
      setTimeout(() => setExporting(false), 1000)

    } catch (error) {
      console.error('Export failed:', error)
      setExporting(false)
    }
  }

  const handleCopy = async () => {
    try {
      // Use proper CSV format for clipboard (Excel/Sheets compatible)
      const csvContent = generateCSVContent()
      await navigator.clipboard.writeText(csvContent)
      setCopySuccess(true)
      setTimeout(() => setCopySuccess(false), 2000)
    } catch (err) {
      console.error('Failed to copy: ', err)
    }
  }

  if (!data.rows.length) return null

  return (
    <div className={`relative ${className}`}>
      {/* Quick Export Button */}
      <div className="flex items-center gap-2">
        <TooltipProvider>
          <Tooltip>
            <TooltipTrigger asChild>
              <Button
                variant="outline"
                size="sm"
                onClick={handleCopy}
                title="Smart Copy (CSV format)"
                className="mr-1"
              >
                {copySuccess ? (
                  <Check size={16} className="text-green-600" />
                ) : (
                  <Copy size={16} />
                )}
              </Button>
            </TooltipTrigger>
            <TooltipContent>
              <p>Smart Copy to clipboard (CSV format)</p>
            </TooltipContent>
          </Tooltip>
        </TooltipProvider>
        <Button
          onClick={() => handleExport(selectedFormat)}
          disabled={exporting}
          className="bg-gradient-to-r from-blue-500 to-blue-600 hover:from-blue-600 hover:to-blue-700"
        >
          {exporting ? (
            <div className="animate-spin rounded-full h-4 w-4 border-2 border-white border-t-transparent mr-2" />
          ) : (
            <Download size={16} className="mr-2" />
          )}
          Export {selectedFormat.toUpperCase()}
        </Button>

        <Button
          variant="outline"
          onClick={() => setShowOptions(!showOptions)}
          className="px-3"
        >
          <ChevronDown size={16} />
        </Button>
      </div>

      {/* Export Options */}
      {showOptions && (
        <Card className="absolute top-full right-0 mt-2 w-80 shadow-lg z-50">
          <CardHeader className="pb-3">
            <CardTitle className="text-sm">Export Options</CardTitle>
          </CardHeader>
          <CardContent className="pt-0">
            <div className="space-y-3">
              {exportFormats.map((format) => (
                <div
                  key={format.id}
                  className={`p-3 border rounded-lg cursor-pointer transition-colors ${selectedFormat === format.id
                    ? 'border-blue-500 bg-blue-50 dark:bg-slate-900/70'
                    : 'border-gray-200 dark:border-slate-700 hover:border-gray-300 dark:hover:border-slate-500'
                    }`}
                  onClick={() => setSelectedFormat(format.id)}
                >
                  <div className="flex items-center justify-between">
                    <div className="flex items-center gap-3">
                      <format.icon size={20} />
                      <div>
                        <div className="font-medium text-sm">{format.name}</div>
                        <div className="text-xs text-gray-500 dark:text-gray-400">{format.description}</div>
                      </div>
                    </div>
                    {selectedFormat === format.id && (
                      <CheckCircle size={16} className="text-blue-600" />
                    )}
                  </div>
                </div>
              ))}
            </div>

            <div className="flex gap-2 mt-4">
              <Button
                variant="outline"
                size="sm"
                onClick={() => setShowPreview(!showPreview)}
                className="flex-1"
              >
                <Eye size={14} className="mr-1" />
                Preview
              </Button>
              <Button
                size="sm"
                onClick={() => handleExport(selectedFormat)}
                disabled={exporting}
                className="flex-1"
              >
                {exporting ? (
                  <div className="animate-spin rounded-full h-3 w-3 border-2 border-white border-t-transparent mr-1" />
                ) : (
                  <Download size={14} className="mr-1" />
                )}
                Export
              </Button>
            </div>

            {/* Preview */}
            {showPreview && (
              <div className="mt-3 p-3 bg-gray-50 dark:bg-slate-900/80 rounded-lg">
                <div className="flex items-center justify-between mb-2">
                  <span className="text-xs font-medium text-gray-700 dark:text-gray-100">Preview</span>
                  <Badge variant="secondary" className="text-xs">
                    {data.rows.length} rows
                  </Badge>
                </div>
                <pre className="text-xs text-gray-600 dark:text-gray-300 overflow-x-auto whitespace-pre-wrap">
                  {generatePreview(selectedFormat)}
                </pre>
              </div>
            )}
          </CardContent>
        </Card>
      )}

      {/* Click outside to close */}
      {showOptions && (
        <div
          className="fixed inset-0 z-40"
          onClick={() => setShowOptions(false)}
        />
      )}
    </div>
  )
}