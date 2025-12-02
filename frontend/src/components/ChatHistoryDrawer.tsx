import { Button } from './ui/button'
import { QueryHistoryEnhanced } from './QueryHistoryEnhanced'
import { X } from 'lucide-react'
import type { NormalizedHistoryItem } from '@/utils/history'

interface ChatHistoryDrawerProps {
  open: boolean
  history: NormalizedHistoryItem[]
  onRerun: (query: string) => void
  onEditAndRun: (query: string) => void
  onClose: () => void
}

export function ChatHistoryDrawer({
  open,
  history,
  onRerun,
  onEditAndRun,
  onClose,
}: ChatHistoryDrawerProps) {
  if (!open) return null

  return (
    <div className="fixed inset-0 bg-black/30 z-50" onClick={onClose}>
      <div
        className="absolute right-0 top-0 bottom-0 w-[420px] bg-white dark:bg-slate-900 shadow-xl"
        onClick={(e) => e.stopPropagation()}
      >
        <div className="h-full flex flex-col">
          <div className="p-4 border-b">
            <div className="flex items-center justify-between">
              <h3 className="font-semibold">Query History</h3>
              <Button
                variant="ghost"
                size="sm"
                onClick={onClose}
              >
                <X size={16} />
              </Button>
            </div>
          </div>
          <div className="flex-1 overflow-hidden">
            <QueryHistoryEnhanced
              history={history}
              onRerun={onRerun}
              onEditAndRun={onEditAndRun}
              className="h-full border-0 rounded-none"
            />
          </div>
        </div>
      </div>
    </div>
  )
}
