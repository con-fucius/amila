export interface ShortcutDefinition {
  id: string
  label: string
  keys: string
  description: string
}

export const SHORTCUTS: ShortcutDefinition[] = [
  { id: 'command_palette', label: 'Command Palette', keys: 'Ctrl/Cmd + K', description: 'Open the command palette' },
  { id: 'new_chat', label: 'New Chat', keys: 'Ctrl/Cmd + Shift + C', description: 'Start a new chat' },
  { id: 'schema_browser', label: 'Schema Browser', keys: 'Ctrl/Cmd + Shift + B', description: 'Open the schema browser' },
  { id: 'query_builder', label: 'Query Builder', keys: 'Ctrl/Cmd + Shift + Q', description: 'Open the query builder' },
  { id: 'settings', label: 'Settings', keys: 'Ctrl/Cmd + Shift + S', description: 'Open settings' },
  { id: 'focus_input', label: 'Focus Input', keys: 'Ctrl/Cmd + L', description: 'Focus the chat input' },
  { id: 'cancel_query', label: 'Cancel Query', keys: 'Ctrl/Cmd + .', description: 'Cancel the running query' },
  { id: 'shortcuts_help', label: 'Shortcuts Help', keys: '?', description: 'Show keyboard shortcuts' },
]
