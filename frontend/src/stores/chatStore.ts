import { create } from 'zustand'
import { devtools, persist } from 'zustand/middleware'

// Database type for global selection
export type DatabaseType = 'oracle' | 'doris' | 'postgres'

// Chat message types
export interface ToolCallSummary {
  rowCount?: number
  expectedRowCount?: number
  executionTimeMs?: number
  sql?: string
  timestamp?: string
  status?: string
  message?: string
  error?: string
  warnings?: string[]
  [key: string]: any
}

export interface ChatMessage {
  id: string
  type: 'user' | 'assistant' | 'tool' | 'system'
  content: string
  timestamp: Date
  toolCall?: {
    name: string
    params: Record<string, any>
    result?: any
    status: 'pending' | 'approved' | 'rejected' | 'completed' | 'error'
    error?: string
    metadata?: Record<string, any>
    summary?: ToolCallSummary
  }
}

export interface ChatMeta {
  id: string
  name: string
  createdAt: string
  sessionId: string
  promptCount: number
  messages: ChatMessage[]
}

// Helper to filter last 24 hours - cached threshold for performance
const TWENTY_FOUR_HOURS_MS = 24 * 60 * 60 * 1000
const within24h = (iso: string): boolean => {
  try {
    const date = new Date(iso)
    if (isNaN(date.getTime())) return false
    return date.getTime() > (Date.now() - TWENTY_FOUR_HOURS_MS)
  } catch {
    return false
  }
}

// Generate IDs using crypto API when available for better uniqueness
const genId = (prefix: string): string => {
  const timestamp = Date.now()
  let random: string
  if (typeof crypto !== 'undefined' && crypto.randomUUID) {
    random = crypto.randomUUID().slice(0, 8)
  } else {
    random = Math.random().toString(36).substring(2, 10)
  }
  return `${prefix}_${timestamp}_${random}`
}

// Maximum chats to persist to avoid localStorage quota issues
const MAX_PERSISTED_CHATS = 50

// Interface modes
export type InterfaceMode = 'chat' | 'playground' | 'schema'

// Chat store state
interface ChatState {
  // Current mode
  currentMode: InterfaceMode

  // Global database selection (persisted across pages)
  databaseType: DatabaseType

  // Chats and current selection
  chats: ChatMeta[]
  currentChatId: string | null

  // UI state
  isLoading: boolean
  pendingApproval: string | null // Message ID awaiting approval

  // Input state
  currentInput: string

  // Database actions
  setDatabaseType: (type: DatabaseType) => void

  // Chat actions
  createChat: (name?: string) => string
  renameChat: (id: string, name: string) => void
  deleteChat: (id: string) => void
  switchChat: (id: string) => void
  autoNameChatFromQuery: (query: string) => void

  // Message actions (operate on current chat)
  setMode: (mode: InterfaceMode) => void
  addMessage: (message: Omit<ChatMessage, 'id' | 'timestamp'>) => string
  updateMessage: (id: string, updates: Partial<ChatMessage>) => void
  mergeMessage: (id: string, updater: (prev: ChatMessage) => ChatMessage) => void
  setLoading: (loading: boolean) => void
  setPendingApproval: (messageId: string | null) => void
  setCurrentInput: (input: string) => void
  clearMessages: () => void
  
  // Computed selector for messages (eliminates duplication)
  getMessages: () => ChatMessage[]
}

// Create the store with persistence
export const useChatStore = create<ChatState>()(
  devtools(
    persist(
      (set, get) => ({
        // Initial state
        currentMode: 'chat',
        databaseType: 'doris' as DatabaseType,
        chats: [],
        currentChatId: null,
        isLoading: false,
        pendingApproval: null,
        currentInput: '',
        
        // Computed selector - always derives from current chat (eliminates duplication)
        getMessages: () => {
          const state = get()
          const currentChat = state.chats.find(c => c.id === state.currentChatId)
          return currentChat?.messages || []
        },

        // Database selection with URL sync and cache invalidation
        setDatabaseType: (type: DatabaseType) => {
          const currentType = get().databaseType
          if (currentType !== type) {
            // Update URL query param for bookmark-friendly state (browser-only)
            if (typeof window !== 'undefined' && window.location) {
              try {
                const url = new URL(window.location.href)
                url.searchParams.set('db', type)
                window.history.replaceState({}, '', url.toString())
              } catch (e) {
                console.warn('[chatStore] Failed to update URL:', e)
              }
            }
            
            // Clear schema cache in localStorage when switching databases
            if (typeof localStorage !== 'undefined') {
              try {
                const keysToRemove: string[] = []
                for (let i = 0; i < localStorage.length; i++) {
                  const key = localStorage.key(i)
                  if (key && (key.includes('schema') || key.includes('Schema'))) {
                    keysToRemove.push(key)
                  }
                }
                keysToRemove.forEach(key => {
                  try {
                    localStorage.removeItem(key)
                  } catch {
                    // Ignore individual removal failures
                  }
                })
              } catch (e) {
                console.warn('[chatStore] Failed to clear schema cache:', e)
              }
            }
          }
          set({ databaseType: type }, false, 'setDatabaseType')
        },

        // Helpers (removed unused _ensureChat)

        // Chat management
        createChat: (name) => {
          const id = genId('chat')
          const sessionId = `session_${id}`
          const chat: ChatMeta = {
            id,
            name: name || 'New chat',
            createdAt: new Date().toISOString(),
            sessionId,
            promptCount: 0,
            messages: [],
          }
          set((state) => ({
            chats: [...state.chats.filter(c => within24h(c.createdAt)), chat],
            currentChatId: id,
          }), false, 'createChat')
          return id
        },
        renameChat: (id, name) => set((state) => ({
          chats: state.chats.map(c => c.id === id ? { ...c, name } : c)
        }), false, 'renameChat'),
        deleteChat: (id) => set((state) => {
          const remaining = state.chats.filter(c => c.id !== id)
          const nextId = remaining[0]?.id || get().createChat('New chat')
          return { chats: remaining, currentChatId: nextId }
        }, false, 'deleteChat'),
        switchChat: (id) => set({ currentChatId: id }, false, 'switchChat'),
        autoNameChatFromQuery: (query: string) => set((state) => {
          const chat = state.chats.find(c => c.id === state.currentChatId)
          if (!chat) return {}
          if (chat.name && chat.name !== 'New chat') return {}
          const cleaned = query.replace(/\s+/g, ' ').trim()
          const placeholder = cleaned.slice(0, 60)
          const name = placeholder.length < cleaned.length ? `${placeholder}...` : placeholder
          return { chats: state.chats.map(c => c.id === chat.id ? { ...c, name: name || 'New chat' } : c) }
        }, false, 'autoNameChatFromQuery'),

        // Mode management
        setMode: (mode) => set({ currentMode: mode }, false, 'setMode'),

        // Message management (scoped to current chat)
        addMessage: (message) => {
          const st = get()
          if (!st.currentChatId || !st.chats.find(c => c.id === st.currentChatId)) {
            st.createChat('New chat')
          }
          const id = genId('msg')
          const newMessage: ChatMessage = { ...message, id, timestamp: new Date() }
          set((state) => {
            const chats = state.chats.map(c => {
              if (c.id !== state.currentChatId) return c
              const isUser = newMessage.type === 'user'
              const currentCount = c.promptCount || 0
              return {
                ...c,
                promptCount: isUser && currentCount < 20 ? currentCount + 1 : currentCount,
                messages: [...c.messages.filter(m => within24h(m.timestamp.toISOString())), newMessage],
              }
            })
            return { chats }
          }, false, 'addMessage')
          return id
        },
        updateMessage: (id, updates) => set((state) => ({
          chats: state.chats.map(c => 
            c.id === state.currentChatId 
              ? { ...c, messages: c.messages.map(m => m.id === id ? { ...m, ...updates } : m) } 
              : c
          )
        }), false, 'updateMessage'),
        mergeMessage: (id, updater) => set((state) => ({
          chats: state.chats.map(c => {
            if (c.id !== state.currentChatId) return c
            return { ...c, messages: c.messages.map(m => m.id === id ? updater(m) : m) }
          })
        }), false, 'mergeMessage'),

        // UI state management
        setLoading: (loading) => set({ isLoading: loading }, false, 'setLoading'),
        setPendingApproval: (messageId) => set({ pendingApproval: messageId }, false, 'setPendingApproval'),
        setCurrentInput: (input) => set({ currentInput: input }, false, 'setCurrentInput'),
        clearMessages: () => set((state) => ({
          chats: state.chats.map(c => c.id === state.currentChatId ? { ...c, messages: [] } : c)
        }), false, 'clearMessages'),
      }),
      {
        name: 'amil-chat-storage',
        partialize: (state) => ({
          // Limit persisted chats to prevent localStorage quota issues
          chats: state.chats
            .filter(c => within24h(c.createdAt))
            .slice(0, MAX_PERSISTED_CHATS)
            .map(c => ({
              ...c,
              messages: c.messages.filter(m => {
                try {
                  return within24h(m.timestamp.toISOString())
                } catch {
                  return false
                }
              }),
            })),
          currentChatId: state.currentChatId,
          currentInput: state.currentInput,
          currentMode: state.currentMode,
          databaseType: state.databaseType,
        }),
        // Custom serialization to handle Date objects inside messages
        serialize: (state) => {
          return JSON.stringify({
            state: {
              ...state.state,
              chats: state.state.chats.map((c: ChatMeta) => ({
                ...c,
                messages: c.messages.map((msg: ChatMessage) => ({
                  ...msg,
                  timestamp: msg.timestamp.toISOString(),
                })),
              })),
            },
            version: state.version,
          })
        },
        deserialize: (str) => {
          const data = JSON.parse(str)
          const rawChats = data.state?.chats || []

          const chats = rawChats
            .filter((c: any) => c && c.createdAt && within24h(c.createdAt))
            .slice(0, MAX_PERSISTED_CHATS)
            .map((c: any) => ({
              ...c,
              messages: (c.messages || [])
                .map((msg: any) => {
                  // Validate message structure before casting
                  if (!msg || typeof msg !== 'object') return null
                  const ts = new Date(msg.timestamp)
                  // Validate Date is valid
                  if (isNaN(ts.getTime())) return null
                  return {
                    id: msg.id || genId('msg'),
                    type: msg.type || 'user',
                    content: msg.content || '',
                    timestamp: ts,
                    toolCall: msg.toolCall,
                  }
                })
                .filter((msg: any) => msg !== null && within24h(msg.timestamp.toISOString())),
            }))

          const current = chats.find((c: any) => c.id === data.state?.currentChatId) || chats[0] || null

          // Check URL for database type override (bookmark-friendly) - SSR safe
          let databaseType = data.state?.databaseType || 'doris'
          if (typeof window !== 'undefined' && window.location) {
            try {
              const urlParams = new URLSearchParams(window.location.search)
              const urlDb = urlParams.get('db')
              if (urlDb === 'oracle' || urlDb === 'doris') {
                databaseType = urlDb
              }
            } catch {
              // Ignore URL parsing errors
            }
          }

          return {
            state: {
              ...data.state,
              chats,
              currentChatId: current ? current.id : null,
              databaseType,
            },
            version: data.version,
          }
        },
      }
    ),
    { name: 'chat-store' }
  )
)

// Selectors for optimized re-renders
export const useCurrentMode = () => useChatStore((state) => state.currentMode)
export const useMessages = () => useChatStore((state) => state.getMessages())
export const useIsLoading = () => useChatStore((state) => state.isLoading)
export const usePendingApproval = () => useChatStore((state) => state.pendingApproval)
export const useCurrentInput = () => useChatStore((state) => state.currentInput)
export const useChats = () => useChatStore((state) => state.chats)
export const useCurrentChatId = () => useChatStore((state) => state.currentChatId)
export const useDatabaseType = () => useChatStore((state) => state.databaseType)

// Action selectors
export const useChatActions = () => useChatStore((state) => ({
  setMode: state.setMode,
  setDatabaseType: state.setDatabaseType,
  addMessage: state.addMessage,
  updateMessage: state.updateMessage,
  mergeMessage: state.mergeMessage,
  setLoading: state.setLoading,
  setPendingApproval: state.setPendingApproval,
  setCurrentInput: state.setCurrentInput,
  clearMessages: state.clearMessages,
  createChat: state.createChat,
  renameChat: state.renameChat,
  deleteChat: state.deleteChat,
  switchChat: state.switchChat,
  autoNameChatFromQuery: state.autoNameChatFromQuery,
}))
