import { FileText, Printer } from 'lucide-react'
import { Button } from './ui/button'
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuTrigger,
} from './ui/dropdown-menu'
import { downloadHTMLBrief, printHTMLBrief, type BriefingData } from '@/utils/briefingExport'

interface BriefingExportProps {
  queries: Array<{
    question: string
    answer: string
    sql?: string
    result?: {
      columns: string[]
      rows: any[][]
      rowCount: number
    }
  }>
  title?: string
}

export function BriefingExport({ queries, title = 'Executive Brief' }: BriefingExportProps) {
  const handleExport = (format: 'html' | 'print') => {
    const briefingData: BriefingData = {
      title,
      queries,
      timestamp: new Date()
    }
    
    if (format === 'html') {
      downloadHTMLBrief(briefingData)
    } else if (format === 'print') {
      printHTMLBrief(briefingData)
    }
  }

  return (
    <DropdownMenu>
      <DropdownMenuTrigger asChild>
        <Button variant="outline" size="sm" className="h-8">
          <FileText className="h-4 w-4 mr-1" />
          Export Brief
        </Button>
      </DropdownMenuTrigger>
      <DropdownMenuContent align="end">
        <DropdownMenuItem onClick={() => handleExport('html')}>
          <FileText className="h-4 w-4 mr-2" />
          Download HTML
        </DropdownMenuItem>
        <DropdownMenuItem onClick={() => handleExport('print')}>
          <Printer className="h-4 w-4 mr-2" />
          Print / PDF
        </DropdownMenuItem>
      </DropdownMenuContent>
    </DropdownMenu>
  )
}
