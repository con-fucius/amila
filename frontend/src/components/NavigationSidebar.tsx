import { MessageSquare, Database, Code2, Menu, Plus, User, Settings, AlertTriangle } from 'lucide-react'
import { useState, useContext, useEffect, useCallback } from 'react'
import type { MouseEvent as ReactMouseEvent } from 'react'
import { Link, useLocation, useNavigate } from 'react-router-dom'
import { cn } from '@/utils/cn'
import { useChatActions, useChats, useCurrentChatId, useDatabaseType, useMessages, type DatabaseType } from '@/stores/chatStore'
import { useBackendHealth } from '@/hooks/useBackendHealth'
import { ColorModeContext } from '@/App'
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
  const { status: healthStatus, components, recheckHealth } = useBackendHealth(5000)
  const colorMode = useContext(ColorModeContext)
  const isDark = colorMode.mode === 'dark'
  const [userProfile, setUserProfile] = useState<{ username: string; role: string } | null>(null)
  const [showSwitchWarning, setShowSwitchWarning] = useState(false)
  const [pendingDbSwitch, setPendingDbSwitch] = useState<DatabaseType | null>(null)
  
  const toggleDark = () => {
    colorMode.toggleColorMode()
    document.documentElement.classList.toggle('dark', colorMode.mode !== 'dark')
  }

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

  // Normalize backend component status values to a lowercase string
  const normalizeStatus = (value: any): string => {
    if (!value) return 'unknown'
    return String(value).toLowerCase()
  }

  // Derive status for the four main system health pills
  // Database: check doris_mcp first (for Doris), then sqlcl_pool (for Oracle), then generic database field
  const databaseStatus = normalizeStatus(
    components?.database ??
    (components?.doris_mcp === 'connected' ? 'available' : null) ??
    (components?.sqlcl_pool === 'active' ? 'available' : null) ??
    components?.doris_mcp ??
    components?.sqlcl_pool ??
    components?.mcp_client
  )

  const cacheStatus = normalizeStatus(components?.redis)

  // Backend is healthy if either Doris or SQLcl is connected
  const backendComposite = components
    ? ((components.doris_mcp === 'connected' || components.sqlcl_pool === 'active' || components.mcp_client === 'connected')
      ? 'healthy'
      : (components.doris_mcp || components.sqlcl_pool || components.mcp_client || healthStatus))
    : healthStatus

  const backendStatusNorm = normalizeStatus(backendComposite)

  const graphStatus = normalizeStatus(components?.graphiti)

  const makePillClass = (status: string) => {
    const healthy = ['available', 'connected', 'active', 'healthy', 'ok', 'ready', 'reachable']
    const caution = ['unknown', 'initializing', 'checking', 'booting', 'mock', 'fallback', 'not_initialized', 'unavailable']
    const s = status

    if (healthy.includes(s)) {
      return 'border-green-200 text-green-300 bg-green-900/20'
    }
    if (caution.includes(s)) {
      return 'border-yellow-200 text-yellow-200 bg-yellow-900/30'
    }
    // Treat everything else as a failure (red)
    return 'border-red-200 text-red-300 bg-red-900/20'
  }

  return (
    <aside
      style={{ width: isCollapsed ? 64 : width ?? 256 }}
      className={cn(
        'h-screen bg-gradient-to-b from-gray-900 to-gray-800 text-white transition-all duration-300 z-40 flex-shrink-0 relative',
        isCollapsed && 'w-16'
      )}
    >
      {/* Header */}
      <div className="flex items-center justify-between p-4 border-b border-gray-700">
        {!isCollapsed && (
          <div>
            <div className="text-2xl font-bold bg-gradient-to-r from-emerald-400 to-green-500 bg-clip-text text-transparent">AMILA</div>
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
      <nav className="p-2 space-y-1">
        {navigation.map((item) => {
          const Icon = item.icon
          const isActive = location.pathname === item.href

          return (
            <Link
              key={item.name}
              to={item.href}
              className={cn(
                'flex items-center gap-3 px-3 py-2.5 rounded-lg transition-all',
                isActive
                  ? 'bg-gradient-to-r from-emerald-500 to-green-600 text-white shadow-lg'
                  : 'hover:bg-gray-700 text-gray-300'
              )}
            >
              <Icon className="h-5 w-5 flex-shrink-0" />
              {!isCollapsed && (
                <div className="flex items-center justify-between w-full">
                  <span className="text-sm font-medium">{item.name}</span>
                  {item.name === 'Chat' && (
                    <button
                      type="button"
                      onClick={(e) => { e.preventDefault(); const id = createChat('New chat'); switchChat(id); navigate('/') }}
                      className="ml-2 p-1 rounded hover:bg-gray-600"
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
          <div className="flex items-center justify-between text-[11px] text-gray-300 mb-1">
            <span>Chats</span>
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
                      className="px-1 rounded hover:bg-gray-700 focus:outline-none focus:ring-1 focus:ring-emerald-400"
                      aria-label="Chat actions"
                    >
                      - - -
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
        <div className="absolute bottom-0 left-0 right-0 border-t border-gray-700 pt-5 pb-6 px-4 space-y-6 bg-gradient-to-b from-gray-900/95 to-gray-950">
          <div className="text-[11px] text-gray-300 space-y-5">
            {/* Database Selector */}
            <div>
              <div className="mb-2 font-semibold text-[11px] tracking-wide text-gray-400 uppercase flex items-center gap-1">
                <Database className="w-3 h-3" />
                <span>Database</span>
                {isProcessing && <span className="text-yellow-400 text-[9px]">(locked)</span>}
              </div>
              <div className="flex gap-2">
                <button
                  onClick={() => !isProcessing && handleDatabaseChange('oracle')}
                  disabled={isProcessing || !isDatabaseHealthy('oracle')}
                  title={
                    isProcessing 
                      ? 'Cannot switch database while processing' 
                      : !isDatabaseHealthy('oracle')
                        ? 'Oracle database unavailable'
                        : 'Switch to Oracle'
                  }
                  className={cn(
                    'flex-1 px-3 py-2 rounded-lg text-xs font-medium transition-all border relative',
                    databaseType === 'oracle'
                      ? 'bg-emerald-600/30 border-emerald-500 text-emerald-200'
                      : 'bg-gray-800/70 border-gray-600 text-gray-300 hover:bg-gray-700',
                    (isProcessing || !isDatabaseHealthy('oracle')) && 'opacity-50 cursor-not-allowed'
                  )}
                >
                  Oracle
                  <span className={cn(
                    'absolute top-1 right-1 w-1.5 h-1.5 rounded-full',
                    isDatabaseHealthy('oracle') ? 'bg-green-400' : 'bg-red-400'
                  )} />
                </button>
                <button
                  onClick={() => !isProcessing && handleDatabaseChange('doris')}
                  disabled={isProcessing || !isDatabaseHealthy('doris')}
                  title={
                    isProcessing 
                      ? 'Cannot switch database while processing' 
                      : !isDatabaseHealthy('doris')
                        ? 'Doris database unavailable'
                        : 'Switch to Doris'
                  }
                  className={cn(
                    'flex-1 px-3 py-2 rounded-lg text-xs font-medium transition-all border relative',
                    databaseType === 'doris'
                      ? 'bg-emerald-600/30 border-emerald-500 text-emerald-200'
                      : 'bg-gray-800/70 border-gray-600 text-gray-300 hover:bg-gray-700',
                    (isProcessing || !isDatabaseHealthy('doris')) && 'opacity-50 cursor-not-allowed'
                  )}
                >
                  Doris
                  <span className={cn(
                    'absolute top-1 right-1 w-1.5 h-1.5 rounded-full',
                    isDatabaseHealthy('doris') ? 'bg-green-400' : 'bg-red-400'
                  )} />
                </button>
              </div>
            </div>

            <div className="flex items-center justify-between">
              <div className="flex items-center gap-2">
                <Settings className="w-3 h-3" />
                <span>Settings</span>
              </div>
            </div>
            <button
              onClick={toggleDark}
              className="sidebar-theme-toggle w-full text-left text-sm px-3 py-2.5 rounded-lg bg-gray-800/70 hover:bg-gray-700 transition-colors border border-gray-600 shadow-sm"
            >
              {isDark ? 'Dark' : 'Light'}
            </button>
            <div className="mt-3">
              <div className="mb-2 font-semibold text-[11px] tracking-wide text-gray-400 uppercase">
                System health
              </div>
              <div className="grid grid-cols-2 gap-3">
                <span
                  className={cn('px-3 py-1.5 rounded-md border text-center text-[11px] font-medium truncate', makePillClass(databaseStatus))}
                  title={`Database: ${databaseStatus}`}
                >
                  Database
                </span>
                <span
                  className={cn('px-3 py-1.5 rounded-md border text-center text-[11px] font-medium truncate', makePillClass(cacheStatus))}
                  title={`Cache: ${cacheStatus}`}
                >
                  Cache
                </span>
                <span
                  className={cn('px-3 py-1.5 rounded-md border text-center text-[11px] font-medium truncate', makePillClass(backendStatusNorm))}
                  title={`Backend: ${backendStatusNorm}`}
                >
                  Backend
                </span>
                <span
                  className={cn('px-3 py-1.5 rounded-md border text-center text-[11px] font-medium truncate', makePillClass(graphStatus))}
                  title={`Graph: ${graphStatus}`}
                >
                  Graph
                </span>
              </div>
            </div>
          </div>
          <button
            onClick={() => navigate('/account')}
            className="flex items-center justify-between pt-2 text-xs text-gray-300 w-full hover:bg-gray-700/50 rounded-lg p-2 -m-2 transition-colors"
          >
            <div className="flex items-center gap-2">
              <div className="w-8 h-8 rounded-full bg-gradient-to-r from-emerald-500 to-green-600 flex items-center justify-center text-white">
                <User className="w-4 h-4" />
              </div>
              <div className="leading-tight text-left">
                <div className="font-medium text-[12px]">{userProfile?.username || 'admin'}</div>
                <div className="text-[11px] text-gray-400">{userProfile?.role || 'Analyst'}</div>
              </div>
            </div>
          </button>
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
