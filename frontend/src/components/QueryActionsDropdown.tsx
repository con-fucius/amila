import { ChevronDown, BarChart2, Table, Pin, FileText, Download, Copy, Check, Eye } from 'lucide-react'
import { cn } from '@/utils/cn'
import {
    DropdownMenu,
    DropdownMenuContent,
    DropdownMenuItem,
    DropdownMenuSeparator,
    DropdownMenuTrigger,
    DropdownMenuSub,
    DropdownMenuSubTrigger,
    DropdownMenuSubContent,
} from './ui/dropdown-menu'
import { Button } from './ui/button'
import { useState } from 'react'

interface QueryActionsDropdownProps {
    isPinned: boolean
    onPin: () => void
    isChartOpen: boolean
    onToggleChart: () => void
    onExport: (format: 'csv' | 'excel' | 'json') => void
    onGenerateReport: (format: 'pdf' | 'docx' | 'html') => void
    onCopyCSV: () => void
    onToggleReasoning?: () => void
    isReasoningOpen?: boolean
    disabled?: boolean
}

export function QueryActionsDropdown({
    isPinned,
    onPin,
    isChartOpen,
    onToggleChart,
    onExport,
    onGenerateReport,
    onCopyCSV,
    onToggleReasoning,
    isReasoningOpen,
    disabled
}: QueryActionsDropdownProps) {
    const [copied, setCopied] = useState(false)

    const handleCopy = () => {
        onCopyCSV()
        setCopied(true)
        setTimeout(() => setCopied(false), 2000)
    }

    return (
        <DropdownMenu>
            <DropdownMenuTrigger asChild>
                <Button variant="outline" size="sm" className="h-8 gap-1 pr-1.5" disabled={disabled}>
                    Actions
                    <ChevronDown className="h-3.5 w-3.5 opacity-50" />
                </Button>
            </DropdownMenuTrigger>
            <DropdownMenuContent align="end" className="w-56 overflow-hidden rounded-xl border-gray-200 dark:border-slate-800 bg-white/95 dark:bg-slate-950/95 backdrop-blur-md shadow-xl">
                <DropdownMenuItem onClick={onPin} className="gap-2 py-2.5 cursor-pointer text-xs">
                    <Pin className={`h-4 w-4 ${isPinned ? 'fill-yellow-500 text-yellow-500' : 'text-gray-500'}`} />
                    <span>{isPinned ? 'Unpin from morning' : 'Pin to morning'}</span>
                </DropdownMenuItem>

                {onToggleReasoning && (
                    <DropdownMenuItem onClick={onToggleReasoning} className="gap-2 py-2.5 cursor-pointer text-xs">
                        <FileText className="h-4 w-4 text-gray-500" />
                        <span>{isReasoningOpen ? 'Hide Execution Logic' : 'View Execution Logic'}</span>
                    </DropdownMenuItem>
                )}

                <DropdownMenuItem onClick={onToggleChart} className="gap-2 py-2.5 cursor-pointer text-xs">
                    {isChartOpen ? (
                        <>
                            <Table className="h-4 w-4 text-gray-500" />
                            <span>Show Results Table</span>
                        </>
                    ) : (
                        <>
                            <BarChart2 className="h-4 w-4 text-gray-500" />
                            <span>Show Visual Chart</span>
                        </>
                    )}
                </DropdownMenuItem>

                <DropdownMenuSeparator className="bg-gray-100 dark:bg-slate-800" />

                <DropdownMenuSub>
                    <DropdownMenuSubTrigger className="gap-2 py-2.5 cursor-pointer">
                        <Download className="h-4 w-4 text-gray-500" />
                        <span>Export Data</span>
                    </DropdownMenuSubTrigger>
                    <DropdownMenuSubContent className="bg-white/95 dark:bg-slate-950/95 backdrop-blur-md border-gray-200 dark:border-slate-800">
                        <DropdownMenuItem onClick={() => onExport('csv')} className="cursor-pointer">CSV</DropdownMenuItem>
                        <DropdownMenuItem onClick={() => onExport('excel')} className="cursor-pointer">Excel</DropdownMenuItem>
                        <DropdownMenuItem onClick={() => onExport('json')} className="cursor-pointer">JSON</DropdownMenuItem>
                    </DropdownMenuSubContent>
                </DropdownMenuSub>

                <DropdownMenuSub>
                    <DropdownMenuSubTrigger className="gap-2 py-2.5 cursor-pointer">
                        <FileText className="h-4 w-4 text-gray-500" />
                        <span>Generate Report</span>
                    </DropdownMenuSubTrigger>
                    <DropdownMenuSubContent className="bg-white/95 dark:bg-slate-950/95 backdrop-blur-md border-gray-200 dark:border-slate-800">
                        <DropdownMenuItem onClick={() => onGenerateReport('html')} className="gap-2 cursor-pointer">
                            <Eye className="h-4 w-4 text-gray-400" />
                            Preview Report
                        </DropdownMenuItem>
                        <DropdownMenuSeparator className="bg-gray-100 dark:bg-slate-800" />
                        <DropdownMenuItem onClick={() => onGenerateReport('pdf')} className="cursor-pointer">PDF Document</DropdownMenuItem>
                        <DropdownMenuItem onClick={() => onGenerateReport('docx')} className="cursor-pointer">Word Document</DropdownMenuItem>
                    </DropdownMenuSubContent>
                </DropdownMenuSub>

                <DropdownMenuSeparator className="bg-gray-100 dark:bg-slate-800" />

                <DropdownMenuItem onClick={handleCopy} className="gap-2 py-2.5 cursor-pointer">
                    {copied ? <Check className="h-4 w-4 text-green-500" /> : <Copy className="h-4 w-4 text-gray-500" />}
                    <span>Copy to Clipboard</span>
                </DropdownMenuItem>
            </DropdownMenuContent>
        </DropdownMenu>
    )
}
