
import { useEffect, useState, useCallback } from 'react'
import { useNavigate } from 'react-router-dom'
import {
    Search,
    Table,
    FileCode,
    History,
    Command,
    Database,
    Moon,
    MessageSquare,
    Trash2,
    HelpCircle
} from 'lucide-react'
import { Dialog, DialogContent } from '@/components/ui/dialog'
import { Input } from '@/components/ui/input'
import { cn } from '@/utils/cn'
import { useChatActions, useDatabaseType } from '@/stores/chatStore'

interface CommandItem {
    id: string;
    label: string;
    description?: string;
    icon: React.ElementType;
    action: () => void;
    shortcut?: string;
    group: string;
    keywords?: string[];
}

export function CommandPalette() {
    const [open, setOpen] = useState(false)
    const [search, setSearch] = useState('')
    const [selectedIndex, setSelectedIndex] = useState(0)
    const navigate = useNavigate()
    const { setDatabaseType, clearMessages, createChat } = useChatActions()
    const currentDb = useDatabaseType()

    useEffect(() => {
        const down = (e: KeyboardEvent) => {
            if (e.key === 'k' && (e.metaKey || e.ctrlKey)) {
                e.preventDefault()
                setOpen((open) => !open)
            }
        }
        document.addEventListener('keydown', down)
        return () => document.removeEventListener('keydown', down)
    }, [])

    useEffect(() => {
        const openPalette = () => setOpen(true)
        window.addEventListener('command-palette:open', openPalette as EventListener)
        return () => window.removeEventListener('command-palette:open', openPalette as EventListener)
    }, [])

    const toggleTheme = useCallback(() => {
        const html = document.documentElement
        const isDark = html.classList.contains('dark')
        if (isDark) {
            html.classList.remove('dark')
            localStorage.setItem('theme', 'light')
        } else {
            html.classList.add('dark')
            localStorage.setItem('theme', 'dark')
        }
    }, [])

    const actions: CommandItem[] = [
        // Navigation
        {
            id: 'new-chat',
            label: 'New Chat',
            description: 'Start a fresh conversation',
            icon: MessageSquare,
            action: () => {
                createChat('New chat')
                navigate('/')
            },
            shortcut: '⌘N',
            group: 'Navigation',
            keywords: ['conversation', 'start', 'fresh']
        },
        {
            id: 'query-builder',
            label: 'Query Builder',
            description: 'Write and execute SQL directly',
            icon: FileCode,
            action: () => navigate('/query-builder'),
            group: 'Navigation',
            keywords: ['sql', 'editor', 'code']
        },
        {
            id: 'schema',
            label: 'Schema Browser',
            description: 'Explore database tables and columns',
            icon: Table,
            action: () => navigate('/schema-browser'),
            group: 'Navigation',
            keywords: ['tables', 'columns', 'structure', 'metadata']
        },
        {
            id: 'history',
            label: 'Query History',
            description: 'View past queries and results',
            icon: History,
            action: () => navigate('/query-builder?tab=history'),
            group: 'Navigation',
            keywords: ['past', 'previous', 'log']
        },
        // Database
        {
            id: 'switch-doris',
            label: 'Switch to Doris',
            description: currentDb === 'doris' ? 'Currently active' : 'Switch database connection',
            icon: Database,
            action: () => setDatabaseType('doris'),
            group: 'Database',
            keywords: ['database', 'connection', 'analytics']
        },
        {
            id: 'switch-oracle',
            label: 'Switch to Oracle',
            description: currentDb === 'oracle' ? 'Currently active' : 'Switch database connection',
            icon: Database,
            action: () => setDatabaseType('oracle'),
            group: 'Database',
            keywords: ['database', 'connection', 'enterprise']
        },
        // Actions
        {
            id: 'clear-chat',
            label: 'Clear Current Chat',
            description: 'Remove all messages from current conversation',
            icon: Trash2,
            action: () => clearMessages(),
            group: 'Actions',
            keywords: ['delete', 'remove', 'reset']
        },
        {
            id: 'toggle-theme',
            label: 'Toggle Dark Mode',
            description: 'Switch between light and dark theme',
            icon: Moon,
            action: toggleTheme,
            shortcut: '⌘D',
            group: 'Actions',
            keywords: ['theme', 'light', 'dark', 'appearance']
        },
        // Help
        {
            id: 'help',
            label: 'Help & Documentation',
            description: 'View keyboard shortcuts and tips',
            icon: HelpCircle,
            action: () => {
                // Could open a help modal or navigate to docs
                window.open('https://github.com/your-repo/docs', '_blank')
            },
            shortcut: '?',
            group: 'Help',
            keywords: ['docs', 'guide', 'shortcuts']
        }
    ]

    const filteredItems = actions.filter(item => {
        const searchLower = search.toLowerCase()
        return (
            item.label.toLowerCase().includes(searchLower) ||
            item.description?.toLowerCase().includes(searchLower) ||
            item.keywords?.some(k => k.includes(searchLower)) ||
            item.group.toLowerCase().includes(searchLower)
        )
    })

    // Group items by category
    const groupedItems = filteredItems.reduce((acc, item) => {
        if (!acc[item.group]) acc[item.group] = []
        acc[item.group].push(item)
        return acc
    }, {} as Record<string, CommandItem[]>)

    const handleSelect = (item: CommandItem) => {
        item.action()
        setOpen(false)
        setSearch('')
    }

    // Handle keyboard navigation
    useEffect(() => {
        const handleKeyDown = (e: KeyboardEvent) => {
            if (!open) return

            if (e.key === 'ArrowDown') {
                e.preventDefault()
                setSelectedIndex(prev => (prev + 1) % filteredItems.length)
            } else if (e.key === 'ArrowUp') {
                e.preventDefault()
                setSelectedIndex(prev => (prev - 1 + filteredItems.length) % filteredItems.length)
            } else if (e.key === 'Enter') {
                e.preventDefault()
                if (filteredItems[selectedIndex]) {
                    handleSelect(filteredItems[selectedIndex])
                }
            }
        }

        document.addEventListener('keydown', handleKeyDown)
        return () => document.removeEventListener('keydown', handleKeyDown)
    }, [open, filteredItems, selectedIndex])

    // Reset selection when search changes
    useEffect(() => {
        setSelectedIndex(0)
    }, [search])

    let flatIndex = -1

    return (
        <Dialog open={open} onOpenChange={setOpen}>
            <DialogContent className="p-0 gap-0 max-w-2xl overflow-hidden shadow-2xl bg-white dark:bg-slate-900 border border-gray-200 dark:border-slate-800">
                <div className="flex items-center px-4 border-b border-gray-100 dark:border-slate-800">
                    <Search className="w-5 h-5 text-gray-400 mr-2" />
                    <Input
                        className="border-0 focus-visible:ring-0 px-0 py-4 h-14 text-lg bg-transparent shadow-none rounded-none placeholder:text-gray-400"
                        placeholder="Type a command or search..."
                        value={search}
                        onChange={(e) => setSearch(e.target.value)}
                        autoFocus
                    />
                    <div className="text-xs text-gray-400 bg-gray-100 dark:bg-slate-800 px-2 py-1 rounded">ESC</div>
                </div>

                <div className="max-h-[350px] overflow-y-auto py-2">
                    {filteredItems.length === 0 ? (
                        <div className="py-12 text-center text-sm text-gray-500">
                            No results found for "{search}"
                        </div>
                    ) : (
                        <div className="px-2">
                            {Object.entries(groupedItems).map(([group, items]) => (
                                <div key={group} className="mb-2">
                                    <div className="px-3 py-1.5 text-[10px] font-semibold text-gray-400 uppercase tracking-wider">
                                        {group}
                                    </div>
                                    <div className="space-y-0.5">
                                        {items.map((item) => {
                                            flatIndex++
                                            const currentIndex = flatIndex
                                            const Icon = item.icon
                                            const isActive = item.id.includes('switch') && 
                                                ((item.id === 'switch-doris' && currentDb === 'doris') ||
                                                 (item.id === 'switch-oracle' && currentDb === 'oracle'))
                                            
                                            return (
                                                <button
                                                    key={item.id}
                                                    onClick={() => handleSelect(item)}
                                                    className={cn(
                                                        "w-full flex items-center gap-3 px-3 py-2 rounded-lg text-sm transition-colors text-left",
                                                        currentIndex === selectedIndex
                                                            ? "bg-emerald-50 dark:bg-emerald-950/30 text-emerald-900 dark:text-emerald-100"
                                                            : "text-gray-700 dark:text-gray-300 hover:bg-gray-100 dark:hover:bg-slate-800"
                                                    )}
                                                    onMouseEnter={() => setSelectedIndex(currentIndex)}
                                                >
                                                    <div className={cn(
                                                        "p-1.5 rounded-md",
                                                        currentIndex === selectedIndex
                                                            ? "bg-emerald-100 dark:bg-emerald-900/50 text-emerald-600 dark:text-emerald-400"
                                                            : isActive
                                                            ? "bg-green-100 dark:bg-green-900/50 text-green-600 dark:text-green-400"
                                                            : "bg-gray-100 dark:bg-slate-800 text-gray-500"
                                                    )}>
                                                        <Icon className="w-4 h-4" />
                                                    </div>
                                                    <div className="flex-1 min-w-0">
                                                        <div className="font-medium flex items-center gap-2">
                                                            {item.label}
                                                            {isActive && (
                                                                <span className="text-[10px] px-1.5 py-0.5 bg-green-100 dark:bg-green-900/50 text-green-700 dark:text-green-300 rounded">
                                                                    Active
                                                                </span>
                                                            )}
                                                        </div>
                                                        {item.description && (
                                                            <div className="text-xs text-gray-500 dark:text-gray-400 truncate">
                                                                {item.description}
                                                            </div>
                                                        )}
                                                    </div>
                                                    {item.shortcut && (
                                                        <span className="text-xs text-gray-400 bg-gray-100 dark:bg-slate-800 px-1.5 py-0.5 rounded">
                                                            {item.shortcut}
                                                        </span>
                                                    )}
                                                </button>
                                            )
                                        })}
                                    </div>
                                </div>
                            ))}
                        </div>
                    )}
                </div>

                <div className="px-4 py-2 border-t border-gray-100 dark:border-slate-800 bg-gray-50 dark:bg-slate-900/50 flex items-center justify-between text-[11px] text-gray-500">
                    <div className="flex items-center gap-3">
                        <span className="flex items-center gap-1">
                            <Command className="w-3 h-3" />
                            <span>Command Palette</span>
                        </span>
                    </div>
                    <div className="flex items-center gap-3">
                        <span>Use <kbd className="font-sans px-1 bg-white dark:bg-slate-800 rounded border border-gray-200 dark:border-slate-700">↑</kbd><kbd className="font-sans px-1 bg-white dark:bg-slate-800 rounded border border-gray-200 dark:border-slate-700">↓</kbd> to navigate</span>
                        <span><kbd className="font-sans px-1 bg-white dark:bg-slate-800 rounded border border-gray-200 dark:border-slate-700">↵</kbd> to select</span>
                    </div>
                </div>
            </DialogContent>
        </Dialog>
    )
}
