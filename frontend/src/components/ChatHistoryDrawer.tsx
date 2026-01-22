import { QueryHistoryEnhanced } from './QueryHistoryEnhanced'
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
    <div className="fixed inset-0 bg-black/40 backdrop-blur-sm z-50" onClick={onClose}>
      <div
        className="absolute right-0 top-0 bottom-0 w-full max-w-[480px] bg-white dark:bg-slate-900 shadow-2xl animate-in slide-in-from-right duration-200"
        onClick={(e) => e.stopPropagation()}
      >
        <div className="h-full flex flex-col">
          <QueryHistoryEnhanced
            history={history}
            onRerun={(q) => { onRerun(q); onClose(); }}
            onEditAndRun={(q) => { onEditAndRun(q); onClose(); }}
            onClose={onClose}
            className="h-full border-0 rounded-none shadow-none"
          />
        </div>
      </div>
    </div>
  )
}
