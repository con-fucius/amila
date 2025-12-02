import { Button } from './ui/button'
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuTrigger,
} from './ui/dropdown-menu'
import { Bell, Database, Loader2 } from 'lucide-react'
import type { DatabaseType } from '@/types/domain'

interface ChatTopBarProps {
  isLoading: boolean
  storeLoading: boolean
  databaseType: DatabaseType
  onDatabaseTypeChange: (db: DatabaseType) => void
  onOpenHistory: () => void | Promise<void>
}

export function ChatTopBar({
  isLoading,
  storeLoading,
  databaseType,
  onDatabaseTypeChange,
  onOpenHistory,
}: ChatTopBarProps) {
  return (
    <header className="h-14 bg-white/80 dark:bg-slate-950/70 backdrop-blur-xl shadow-md flex items-center justify-between px-6 border-b border-emerald-100/60 dark:border-emerald-500/30 flex-shrink-0">
      <div className="flex items-center gap-4">
        <h1 className="chat-header-subtitle font-semibold text-gray-700 dark:text-gray-200">Chat Interface</h1>
        {(isLoading || storeLoading) && (
          <div className="flex items-center gap-2 text-[10px] text-emerald-600">
            <Loader2 className="h-3 w-3 animate-spin" />
            <span>Thinking...</span>
          </div>
        )}
      </div>
      <div className="flex items-center gap-4">
        <Button
          variant="outline"
          onClick={onOpenHistory}
        >
          History
        </Button>
        <Button variant="ghost" size="icon" className="relative">
          <Bell className="h-5 w-5" />
        </Button>
        <DropdownMenu>
          <DropdownMenuTrigger asChild>
            <Button variant="outline" size="sm" className="gap-2 border-blue-200 dark:border-slate-700">
              <Database className="h-4 w-4 text-blue-600 dark:text-blue-400" />
              <span className="hidden sm:inline">{databaseType === 'oracle' ? 'Oracle SQLcl' : 'Apache Doris'}</span>
            </Button>
          </DropdownMenuTrigger>
          <DropdownMenuContent align="end">
            <DropdownMenuItem
              onClick={() => onDatabaseTypeChange('oracle')}
              className="cursor-pointer"
            >
              <div className="flex items-center gap-2">
                <div className={`w-2 h-2 rounded-full ${databaseType === 'oracle' ? 'bg-green-500' : 'bg-gray-300'}`} />
                Oracle SQLcl
              </div>
            </DropdownMenuItem>
            <DropdownMenuItem
              onClick={() => onDatabaseTypeChange('doris')}
              className="cursor-pointer"
            >
              <div className="flex items-center gap-2">
                <div className={`w-2 h-2 rounded-full ${databaseType === 'doris' ? 'bg-green-500' : 'bg-gray-300'}`} />
                Apache Doris
              </div>
            </DropdownMenuItem>
          </DropdownMenuContent>
        </DropdownMenu>
      </div>
    </header>
  )
}
