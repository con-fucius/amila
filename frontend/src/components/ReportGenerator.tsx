import { useState } from 'react'
import { FileText, Download, Loader2, Eye, X } from 'lucide-react'
import { Button } from './ui/button'
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuSeparator,
  DropdownMenuTrigger,
} from './ui/dropdown-menu'
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogHeader,
  DialogTitle,
} from './ui/dialog'
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
  const [showPreview, setShowPreview] = useState(false)
  const [previewContent, setPreviewContent] = useState<string>('')
  const [previewLoading, setPreviewLoading] = useState(false)

  const generatePreviewContent = () => {
    if (!queryResults.length) return 'No data available for preview'

    const result = queryResults[0]
    const previewRows = result.rows.slice(0, 5)

    let content = `# ${title || 'Query Results Report'}\n\n`
    content += `**Generated:** ${new Date().toLocaleString()}\n\n`

    if (userQueries?.length) {
      content += `## Query\n\`\`\`\n${userQueries[0]}\n\`\`\`\n\n`
    }

    if (result.sql_query) {
      content += `## SQL\n\`\`\`sql\n${result.sql_query}\n\`\`\`\n\n`
    }

    content += `## Results (${result.row_count || result.rows.length} rows)\n\n`
    content += `| ${result.columns.join(' | ')} |\n`
    content += `| ${result.columns.map(() => '---').join(' | ')} |\n`

    previewRows.forEach(row => {
      content += `| ${row.map(cell => String(cell ?? '')).join(' | ')} |\n`
    })

    if (result.rows.length > 5) {
      content += `\n*... and ${result.rows.length - 5} more rows*\n`
    }

    return content
  }

  const handlePreview = async () => {
    setPreviewLoading(true)
    setPreviewContent('')
    setShowPreview(true)

    try {
      // Generate a quick HTML preview
      const response = await apiService.generateReport({
        query_results: queryResults,
        format: 'html',
        title,
        user_queries: userQueries,
      })

      if (response.status === 'success' && response.content) {
        setPreviewContent(response.content)
      } else {
        // Fall back to markdown preview
        setPreviewContent(generatePreviewContent())
      }
    } catch {
      // Fall back to markdown preview on error
      setPreviewContent(generatePreviewContent())
    } finally {
      setPreviewLoading(false)
    }
  }

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
          <DropdownMenuItem onClick={handlePreview}>
            <Eye className="h-3 w-3 mr-2" />
            Preview Report
          </DropdownMenuItem>
          <DropdownMenuSeparator />
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

      {/* Preview Dialog */}
      <Dialog open={showPreview} onOpenChange={setShowPreview}>
        <DialogContent className="max-w-4xl max-h-[80vh] overflow-hidden flex flex-col">
          <DialogHeader>
            <div className="flex items-center justify-between">
              <div>
                <DialogTitle className="text-lg">Report Preview</DialogTitle>
                <DialogDescription className="text-sm">
                  Preview of your report before downloading
                </DialogDescription>
              </div>
              <Button
                variant="ghost"
                size="sm"
                onClick={() => setShowPreview(false)}
                className="h-8 w-8 p-0"
              >
                <X className="h-4 w-4" />
              </Button>
            </div>
          </DialogHeader>

          <div className="flex-1 overflow-auto border rounded-lg bg-white dark:bg-slate-900">
            {previewLoading ? (
              <div className="flex items-center justify-center h-64">
                <Loader2 className="h-6 w-6 animate-spin text-emerald-500" />
                <span className="ml-2 text-gray-500">Generating preview...</span>
              </div>
            ) : previewContent.startsWith('<!DOCTYPE') || previewContent.startsWith('<html') ? (
              <iframe
                srcDoc={previewContent}
                className="w-full h-[60vh] border-0"
                title="Report Preview"
              />
            ) : (
              <pre className="p-4 text-sm whitespace-pre-wrap font-mono text-gray-700 dark:text-gray-300">
                {previewContent}
              </pre>
            )}
          </div>

          <div className="flex justify-end gap-2 mt-4">
            <Button variant="outline" onClick={() => setShowPreview(false)}>
              Close
            </Button>
            <Button onClick={() => handleGenerate('html')} disabled={loading}>
              <Download className="h-4 w-4 mr-2" />
              Download HTML
            </Button>
          </div>
        </DialogContent>
      </Dialog>
    </div>
  )
}

