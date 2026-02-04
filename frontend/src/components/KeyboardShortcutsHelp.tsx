import { useEffect, useState } from 'react'
import { Dialog, DialogContent, DialogHeader, DialogTitle } from '@/components/ui/dialog'
import { SHORTCUTS } from '@/utils/shortcuts'

export function KeyboardShortcutsHelp() {
  const [open, setOpen] = useState(false)

  useEffect(() => {
    const toggle = () => setOpen((prev) => !prev)
    window.addEventListener('shortcuts:toggle', toggle as EventListener)
    return () => window.removeEventListener('shortcuts:toggle', toggle as EventListener)
  }, [])

  return (
    <Dialog open={open} onOpenChange={setOpen}>
      <DialogContent className="max-w-lg">
        <DialogHeader>
          <DialogTitle>Keyboard Shortcuts</DialogTitle>
        </DialogHeader>
        <div className="space-y-3">
          {SHORTCUTS.map((s) => (
            <div key={s.id} className="flex items-center justify-between border-b border-gray-200 pb-2 text-sm">
              <div className="text-gray-700">{s.label}</div>
              <div className="font-mono text-xs text-gray-500">{s.keys}</div>
            </div>
          ))}
        </div>
      </DialogContent>
    </Dialog>
  )
}
