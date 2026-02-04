import React, { ReactNode, useEffect, useState } from 'react'
import { NavigationSidebar } from './NavigationSidebar'
import { CommandPalette } from './CommandPalette'
import { useIsLoading, useChatActions } from '@/stores/chatStore'
import { Menu } from 'lucide-react'
import { useNavigate } from 'react-router-dom'
import { KeyboardShortcutsHelp } from './KeyboardShortcutsHelp'

interface MainLayoutProps {
  children: ReactNode
}

export function MainLayout({ children }: MainLayoutProps) {
  const [isSidebarCollapsed, setIsSidebarCollapsed] = useState(false)
  const [expandedSidebarWidth, setExpandedSidebarWidth] = useState(256)
  const isProcessing = useIsLoading()
  const { createChat, switchChat } = useChatActions()
  const navigate = useNavigate()
  const [isMobile, setIsMobile] = useState(false)
  const [isMobileSidebarOpen, setIsMobileSidebarOpen] = useState(false)

  const effectiveSidebarWidth = isSidebarCollapsed ? 64 : expandedSidebarWidth

  const handleToggleSidebar = () => {
    setIsSidebarCollapsed((prev) => !prev)
  }

  useEffect(() => {
    const mql = window.matchMedia('(max-width: 1024px)')
    const onChange = (e: MediaQueryListEvent | MediaQueryList) => {
      setIsMobile(e.matches)
      if (!e.matches) setIsMobileSidebarOpen(false)
    }
    onChange(mql)
    mql.addEventListener('change', onChange)
    return () => mql.removeEventListener('change', onChange)
  }, [])

  useEffect(() => {
    const toggle = () => setIsMobileSidebarOpen((prev) => !prev)
    window.addEventListener('sidebar:toggle', toggle as EventListener)
    return () => window.removeEventListener('sidebar:toggle', toggle as EventListener)
  }, [])

  useEffect(() => {
    const handler = (e: KeyboardEvent) => {
      const target = e.target as HTMLElement | null
      const tag = target?.tagName?.toLowerCase()
      const isTyping =
        tag === 'input' ||
        tag === 'textarea' ||
        (target && (target as HTMLElement).isContentEditable)

      if (isTyping && !(e.ctrlKey || e.metaKey)) return

      const ctrl = e.ctrlKey || e.metaKey
      if (ctrl && e.shiftKey && e.key.toLowerCase() === 'c') {
        e.preventDefault()
        const id = createChat('New chat')
        switchChat(id)
        navigate('/')
        return
      }
      if (ctrl && e.shiftKey && e.key.toLowerCase() === 'b') {
        e.preventDefault()
        navigate('/schema-browser')
        return
      }
      if (ctrl && e.shiftKey && e.key.toLowerCase() === 'q') {
        e.preventDefault()
        navigate('/query-builder')
        return
      }
      if (ctrl && e.shiftKey && e.key.toLowerCase() === 's') {
        e.preventDefault()
        navigate('/settings')
        return
      }
      if (ctrl && e.key.toLowerCase() === 'l') {
        e.preventDefault()
        window.dispatchEvent(new CustomEvent('focus-chat-input'))
        return
      }
      if (ctrl && e.key === '.') {
        e.preventDefault()
        window.dispatchEvent(new CustomEvent('query:cancel'))
        return
      }
      if (e.key === '?' || (e.key === '/' && e.shiftKey)) {
        e.preventDefault()
        window.dispatchEvent(new CustomEvent('shortcuts:toggle'))
        return
      }
    }
    window.addEventListener('keydown', handler)
    return () => window.removeEventListener('keydown', handler)
  }, [createChat, switchChat, navigate])

  const handleResizeMouseDown = (event: React.MouseEvent<HTMLDivElement>) => {
    if (isSidebarCollapsed) {
      return
    }

    event.preventDefault()
    const startX = event.clientX
    const startWidth = expandedSidebarWidth
    const minWidth = 200
    const maxWidth = 400

    const handleMouseMove = (moveEvent: MouseEvent) => {
      const delta = moveEvent.clientX - startX
      let newWidth = startWidth + delta
      if (newWidth < minWidth) newWidth = minWidth
      if (newWidth > maxWidth) newWidth = maxWidth
      setExpandedSidebarWidth(newWidth)
    }

    const handleMouseUp = () => {
      window.removeEventListener('mousemove', handleMouseMove)
      window.removeEventListener('mouseup', handleMouseUp)
    }

    window.addEventListener('mousemove', handleMouseMove)
    window.addEventListener('mouseup', handleMouseUp)
  }

  return (
    <div className="flex h-screen overflow-hidden">
      <NavigationSidebar
        isCollapsed={isSidebarCollapsed}
        onToggle={handleToggleSidebar}
        width={effectiveSidebarWidth}
        onResizeMouseDown={handleResizeMouseDown}
        isProcessing={isProcessing}
        isMobile={isMobile}
        isMobileOpen={isMobileSidebarOpen}
        onCloseMobile={() => setIsMobileSidebarOpen(false)}
      />
      {isMobile && isMobileSidebarOpen && (
        <div
          className="fixed inset-0 bg-black/40 z-30"
          onClick={() => setIsMobileSidebarOpen(false)}
        />
      )}
      <CommandPalette />
      <KeyboardShortcutsHelp />
      <main className="flex-1 overflow-hidden relative">
        {isMobile && !isMobileSidebarOpen && (
          <button
            className="fixed top-3 left-3 z-30 bg-slate-900/80 text-white rounded-lg p-2 shadow-lg"
            onClick={() => setIsMobileSidebarOpen(true)}
            aria-label="Open navigation"
          >
            <Menu className="h-5 w-5" />
          </button>
        )}
        {children}
      </main>
    </div>
  )
}
