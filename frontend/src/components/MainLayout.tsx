import React, { ReactNode, useState } from 'react'
import { NavigationSidebar } from './NavigationSidebar'
import { CommandPalette } from './CommandPalette'
import { useIsLoading } from '@/stores/chatStore'

interface MainLayoutProps {
  children: ReactNode
}

export function MainLayout({ children }: MainLayoutProps) {
  const [isSidebarCollapsed, setIsSidebarCollapsed] = useState(false)
  const [expandedSidebarWidth, setExpandedSidebarWidth] = useState(256)
  const isProcessing = useIsLoading()

  const effectiveSidebarWidth = isSidebarCollapsed ? 64 : expandedSidebarWidth

  const handleToggleSidebar = () => {
    setIsSidebarCollapsed((prev) => !prev)
  }

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
      />
      <CommandPalette />
      <main className="flex-1 overflow-hidden">
        {children}
      </main>
    </div>
  )
}
