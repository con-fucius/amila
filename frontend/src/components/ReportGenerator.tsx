import { useState } from 'react'
import { FileText, Download, Loader2 } from 'lucide-react'
import { Button } from './ui/button'
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuTrigger,
} from './ui/dropdown-menu'
import { apiService } from '@/services/apiService'

interface QueryResult {
  columns: string[]
  rows: any[][]
  sql_query?: string
  row_count?: number
}

interface ReportGeneratorProps {
  queryResults: QueryResult[]
  userQueries?: string[]
  title?: string
  className?: string
}

export function ReportGenerator({ queryResults, userQueries, title, className = '' }: ReportGeneratorProps) {
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState<string | null>(null)

  const handleGenerate = async (format: 'html' | 'pdf' | 'docx') => {
    if (!queryResults.length) {
      setError('No query results to generate report from')
      return
    }

    setLoading(true)
    setError(null)

    try {
      const response = await apiService.generateReport({
        query_results: queryResults,
        format,
        title,
        user_queries: userQueries,
      })

      if (response.status !== 'success') {
        throw new Error('Report generation failed')
      }

      // Decode and download
      let blob: Blob
      if (response.encoding === 'base64') {
        const binaryString = atob(response.content)
        const bytes = new Uint8Array(binaryString.length)
        for (let i = 0; i < binaryString.length; i++) {
          bytes[i] = binaryString.charCodeAt(i)
        }
        blob = new Blob([bytes], { type: response.content_type })
      } else {
        blob = new Blob([response.content], { type: response.content_type })
      }

      // Create download link
      const url = URL.createObjectURL(blob)
      const a = document.createElement('a')
      a.href = url
      a.download = `report_${new Date().toISOString().slice(0, 10)}.${format}`
      document.body.appendChild(a)
      a.click()
      document.body.removeChild(a)
      URL.revokeObjectURL(url)

    } catch (err: any) {
      setError(err.message || 'Failed to generate report')
    } finally {
      setLoading(false)
    }
  }

  return (
    <div className={className}>
      <DropdownMenu>
        <DropdownMenuTrigger asChild>
          <Button
            variant="outline"
            size="sm"
            disabled={loading || !queryResults.length}
            className="text-xs"
          >
            {loading ? (
              <Loader2 className="h-3 w-3 mr-1 animate-spin" />
            ) : (
              <FileText className="h-3 w-3 mr-1" />
            )}
            Generate Report
          </Button>
        </DropdownMenuTrigger>
        <DropdownMenuContent align="end" className="w-40">
          <DropdownMenuItem onClick={() => handleGenerate('html')}>
            <Download className="h-3 w-3 mr-2" />
            HTML Report
          </DropdownMenuItem>
          <DropdownMenuItem onClick={() => handleGenerate('pdf')}>
            <Download className="h-3 w-3 mr-2" />
            PDF Report
          </DropdownMenuItem>
          <DropdownMenuItem onClick={() => handleGenerate('docx')}>
            <Download className="h-3 w-3 mr-2" />
            Word Document
          </DropdownMenuItem>
        </DropdownMenuContent>
      </DropdownMenu>
      {error && (
        <p className="text-xs text-red-500 mt-1">{error}</p>
      )}
    </div>
  )
}
