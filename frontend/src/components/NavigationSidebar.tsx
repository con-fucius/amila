import { MessageSquare, Database, Code2, Menu, Plus, User, Settings, AlertTriangle, MoreHorizontal } from 'lucide-react'
import { useState, useEffect, useCallback } from 'react'
import type { MouseEvent as ReactMouseEvent } from 'react'
import { Link, useLocation, useNavigate } from 'react-router-dom'
import { cn } from '@/utils/cn'
import { useChatActions, useChats, useCurrentChatId, useDatabaseType, useMessages, type DatabaseType } from '@/stores/chatStore'
import { useBackendHealth } from '@/hooks/useBackendHealth'
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuTrigger,
} from './ui/dropdown-menu'
import {
  AlertDialog,
  AlertDialogAction,
  AlertDialogCancel,
  AlertDialogContent,
  AlertDialogDescription,
  AlertDialogFooter,
  AlertDialogHeader,
  AlertDialogTitle,
} from './ui/alert-dialog'
import { SystemHealthMonitor } from './SystemHealthMonitor'
import { DatabaseSelector } from './DatabaseSelector'

interface NavigationSidebarProps {
  isCollapsed: boolean
  onToggle: () => void
  width?: number
  onResizeMouseDown?: (event: ReactMouseEvent<HTMLDivElement>) => void
  isProcessing?: boolean
}

export function NavigationSidebar({ isCollapsed, onToggle, width, onResizeMouseDown, isProcessing = false }: NavigationSidebarProps) {
  const location = useLocation()
  const navigate = useNavigate()
  const { createChat, switchChat, renameChat, deleteChat, setDatabaseType } = useChatActions()
  const chats = useChats()
  const currentChatId = useCurrentChatId()
  const databaseType = useDatabaseType()
  const messages = useMessages()
  const { components, recheckHealth } = useBackendHealth(5000)
  const [userProfile, setUserProfile] = useState<{ username: string; role: string } | null>(null)
  const [showSwitchWarning, setShowSwitchWarning] = useState(false)
  const [pendingDbSwitch, setPendingDbSwitch] = useState<DatabaseType | null>(null)

  // Check if the target database is healthy before allowing switch
  const isDatabaseHealthy = useCallback((targetDb: DatabaseType): boolean => {
    if (!components) return false
    if (targetDb === 'oracle') {
      const oracleStatus = components.sqlcl_pool || components.mcp_client
      return oracleStatus === 'active' || oracleStatus === 'connected' || oracleStatus === 'ready'
    }
    if (targetDb === 'doris') {
      const dorisStatus = components.doris_mcp
      return dorisStatus === 'connected' || dorisStatus === 'active' || dorisStatus === 'ready'
    }
    return false
  }, [components])

  // Check if current conversation has messages (to warn about context loss)
  const hasConversationHistory = messages.length > 0

  const handleDatabaseChange = useCallback((type: DatabaseType) => {
    if (type === databaseType) return

    // Check health first
    if (!isDatabaseHealthy(type)) {
      recheckHealth()
      console.warn(`[NavigationSidebar] ${type} database not healthy, switch blocked`)
      return
    }

    // Warn if there's conversation history
    if (hasConversationHistory) {
      setPendingDbSwitch(type)
      setShowSwitchWarning(true)
      return
    }

    setDatabaseType(type)
  }, [databaseType, isDatabaseHealthy, hasConversationHistory, setDatabaseType, recheckHealth])

  const confirmDatabaseSwitch = useCallback(() => {
    if (pendingDbSwitch) {
      setDatabaseType(pendingDbSwitch)
    }
    setShowSwitchWarning(false)
    setPendingDbSwitch(null)
  }, [pendingDbSwitch, setDatabaseType])

  const cancelDatabaseSwitch = useCallback(() => {
    setShowSwitchWarning(false)
    setPendingDbSwitch(null)
  }, [])

  const navigation = [
    { name: 'Chat', href: '/', icon: MessageSquare },
    { name: 'Query Builder', href: '/query-builder', icon: Code2 },
    { name: 'Schema Browser', href: '/schema-browser', icon: Database },
  ]

  useEffect(() => {
    try {
      const token = localStorage.getItem('access_token')
      if (!token || token === 'temp-dev-token') {
        setUserProfile(null)
        return
      }
      const parts = token.split('.')
      if (parts.length < 2) {
        setUserProfile(null)
        return
      }
      const payload = JSON.parse(atob(parts[1].replace(/-/g, '+').replace(/_/g, '/')))
      const username = payload.sub || payload.username || 'user'
      const rawRole = payload.role || payload.roles?.[0] || 'analyst'
      const role = typeof rawRole === 'string' ? rawRole.charAt(0).toUpperCase() + rawRole.slice(1) : 'Analyst'
      setUserProfile({ username, role })
    } catch {
      setUserProfile(null)
    }
  }, [])



  // Derive status for the four main system health pills
  // Database: check doris_mcp first (for Doris), then sqlcl_pool (for Oracle), then generic database field








  return (
    <aside
      style={{ width: isCollapsed ? 64 : width ?? 256 }}
      className={cn(
        'h-screen bg-black border-r border-gray-800/50 text-white transition-all duration-300 z-40 flex-shrink-0 relative',
        isCollapsed && 'w-16'
      )}
    >
      {/* Header */}
      <div className="flex items-center justify-between p-4 border-b border-gray-700">
        {!isCollapsed && (
          <div>
            <div className="text-[26px] font-bold bg-gradient-to-r from-emerald-400 to-green-500 bg-clip-text text-transparent">Amila</div>
            <div className="text-[10px] text-gray-300 mt-0.5">Ask. Understand. Act</div>
          </div>
        )}
        <button
          onClick={onToggle}
          className="p-2 hover:bg-gray-700 rounded-lg transition-colors"
        >
          <Menu className="h-5 w-5" />
        </button>
      </div>

      {/* Navigation Items */}
      <nav className="p-2 space-y-0.5">
        {navigation.map((item) => {
          const Icon = item.icon
          const isActive = location.pathname === item.href

          return (
            <Link
              key={item.name}
              to={item.href}
              className={cn(
                'flex items-center transition-all relative group overflow-hidden',
                isCollapsed ? 'justify-center w-10 h-10 mx-auto rounded-lg' : 'gap-3 px-3 py-2 rounded-lg w-full',
                isActive
                  ? 'bg-emerald-500/10 text-emerald-400 border border-emerald-500/20 shadow-[0_0_15px_-3px_rgba(16,185,129,0.1)]'
                  : 'hover:bg-gray-800/80 text-gray-400 hover:text-gray-200'
              )}
            >

              <Icon className="h-5 w-5 flex-shrink-0" />
              {!isCollapsed && (
                <div className="flex items-center justify-between w-full min-w-0">
                  <span className="text-sm font-medium truncate">{item.name}</span>
                  {item.name === 'Chat' && (
                    <button
                      type="button"
                      onClick={(e) => { e.preventDefault(); const id = createChat('New chat'); switchChat(id); navigate('/') }}
                      className="ml-2 p-1 rounded hover:bg-gray-600 flex-shrink-0"
                      title="New Chat"
                    >
                      <Plus className="h-4 w-4" />
                    </button>
                  )}
                </div>
              )}
            </Link>
          )
        })}
      </nav>


      {/* Chats List */}
      {!isCollapsed && (
        <div className="px-3 mt-2">
          <div className="flex items-center justify-between text-[11px] text-gray-400 mb-1">
            <span className="font-semibold tracking-wide">Chats</span>
            <button className="p-1 hover:bg-gray-700 rounded" onClick={() => { const id = createChat('New chat'); switchChat(id) }} title="New chat">
              <Plus className="w-3 h-3" />
            </button>
          </div>
          <div className="space-y-1 max-h-56 overflow-y-auto pr-1">
            {chats.map((c) => (
              <div
                key={c.id}
                className={cn(
                  'flex items-center justify-between gap-2 px-2 py-1 rounded',
                  c.id === currentChatId
                    ? 'bg-emerald-600/20 text-emerald-200'
                    : 'hover:bg-gray-700/60 text-gray-300'
                )}
              >
                <button
                  className="truncate text-xs text-left flex-1"
                  onClick={() => switchChat(c.id)}
                >
                  {c.name}
                </button>
                <DropdownMenu>
                  <DropdownMenuTrigger asChild>
                    <button
                      type="button"
                      className="px-1 py-1 rounded hover:bg-gray-700 focus:outline-none focus:ring-1 focus:ring-emerald-400 text-gray-400 hover:text-white transition-colors"
                      aria-label="Chat actions"
                    >
                      <MoreHorizontal className="w-4 h-4" />
                    </button>
                  </DropdownMenuTrigger>
                  <DropdownMenuContent
                    align="end"
                    className="w-32 bg-gray-900 text-gray-100 border border-gray-700"
                  >
                    <DropdownMenuItem
                      className="text-xs cursor-pointer hover:bg-gray-800"
                      onClick={() => {
                        const name = prompt('Rename chat', c.name) || c.name
                        renameChat(c.id, name)
                      }}
                    >
                      Rename
                    </DropdownMenuItem>
                    <DropdownMenuItem
                      className="text-xs cursor-pointer text-red-300 focus:text-red-100 hover:bg-red-900/40"
                      onClick={() => deleteChat(c.id)}
                    >
                      Delete
                    </DropdownMenuItem>
                  </DropdownMenuContent>
                </DropdownMenu>
              </div>
            ))}
          </div>
        </div>
      )}

      {/* Footer */}
      {!isCollapsed && (
        <div className="absolute bottom-0 left-0 right-0 border-t border-gray-800/50 pt-5 pb-6 px-4 space-y-5 bg-black">
          <div className="mt-4">
            <SystemHealthMonitor components={components} />
          </div>

          {/* Database Selector (at bottom) */}
          <div className="mt-4 pt-4 border-t border-gray-800/50">
            <div className="mb-2 font-semibold text-[11px] tracking-wide text-gray-400 flex items-center gap-1">
              <span>Database</span>
              {isProcessing && <span className="text-yellow-400 text-[9px]">(locked)</span>}
            </div>

            <DatabaseSelector
              variant="sidebar"
              disabled={isProcessing}
            />
          </div>
          {/* Settings link (at very bottom) */}
          <div className="mt-2 pt-2 border-t border-gray-800/30">
            <button
              onClick={() => navigate('/settings')}
              className={cn(
                "flex items-center gap-3 px-3 py-2.5 rounded-lg w-full transition-all group",
                location.pathname === '/settings'
                  ? 'bg-emerald-500/10 text-emerald-400 border border-emerald-500/20'
                  : 'hover:bg-gray-800/60 text-gray-400 hover:text-gray-200'
              )}
            >
              <Settings className="h-4 w-4 group-hover:text-emerald-400 transition-colors" />
              <span className="text-xs font-medium">Settings</span>
            </button>
          </div>
        </div>
      )}

      {/* Minimized Footer - shows icons when sidebar is collapsed */}
      {isCollapsed && (
        <div className="absolute bottom-0 left-0 right-0 border-t border-gray-700 py-3 px-2 space-y-2 bg-gradient-to-b from-gray-900/95 to-gray-950">
          {/* System health indicator */}
          <div className="flex justify-center flex-col items-center gap-2">
            <SystemHealthMonitor components={components} collapsed={true} />
          </div>

          {/* Database indicator */}
          <div className="flex justify-center">
            <button
              className={cn(
                'p-2 rounded-lg transition-all relative',
                databaseType === 'oracle' || databaseType === 'doris'
                  ? 'bg-emerald-600/30 text-emerald-200'
                  : 'bg-gray-800/70 text-gray-400 hover:bg-gray-700'
              )}
              title={`${databaseType === 'oracle' ? 'Oracle' : 'Doris'} - ${isDatabaseHealthy(databaseType) ? 'Connected' : 'Disconnected'}`}
            >
              <Database className="w-4 h-4" />
              <span className={cn(
                'absolute top-0.5 right-0.5 w-1.5 h-1.5 rounded-full',
                isDatabaseHealthy(databaseType) ? 'bg-green-500' : 'bg-red-500'
              )} />
            </button>
          </div>

          {/* Settings link */}
          <div className="flex justify-center">
            <button
              onClick={() => navigate('/settings')}
              className="p-2 rounded-lg bg-gray-800/70 text-gray-400 hover:bg-gray-700 hover:text-white transition-all"
              title="Settings"
            >
              <Settings className="w-4 h-4" />
            </button>
          </div>
        </div>
      )}

      {/* Resize handle (only when expanded) */}
      {!isCollapsed && onResizeMouseDown && (
        <div
          className="absolute top-0 right-0 h-full w-[2px] cursor-col-resize bg-gray-800/70 hover:bg-emerald-400 transition-colors"
          onMouseDown={onResizeMouseDown}
        />
      )}

      {/* Database Switch Warning Dialog */}
      <AlertDialog open={showSwitchWarning} onOpenChange={setShowSwitchWarning}>
        <AlertDialogContent className="bg-gray-900 border-gray-700 text-white">
          <AlertDialogHeader>
            <AlertDialogTitle className="flex items-center gap-2 text-amber-400">
              <AlertTriangle className="h-5 w-5" />
              Switch Database?
            </AlertDialogTitle>
            <AlertDialogDescription className="text-gray-300">
              You have an active conversation. Switching to {pendingDbSwitch === 'oracle' ? 'Oracle' : 'Doris'} may affect query context and results.
              Previous queries were executed against {databaseType === 'oracle' ? 'Oracle' : 'Doris'}.
            </AlertDialogDescription>
          </AlertDialogHeader>
          <AlertDialogFooter>
            <AlertDialogCancel
              onClick={cancelDatabaseSwitch}
              className="bg-gray-800 border-gray-600 text-gray-300 hover:bg-gray-700"
            >
              Cancel
            </AlertDialogCancel>
            <AlertDialogAction
              onClick={confirmDatabaseSwitch}
              className="bg-emerald-600 hover:bg-emerald-700 text-white"
            >
              Switch Database
            </AlertDialogAction>
          </AlertDialogFooter>
        </AlertDialogContent>
      </AlertDialog>
    </aside>
  )
}
