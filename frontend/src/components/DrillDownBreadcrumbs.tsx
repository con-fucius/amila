import { ChevronRight, Home } from 'lucide-react'
import { cn } from '@/utils/cn'
import { useMessages } from '@/stores/chatStore'

interface BreadcrumbItem {
  label: string
  level: number
}

export function DrillDownBreadcrumbs() {
  const messages = useMessages()
  
  // Extract drill-down context from message history
  const breadcrumbs: BreadcrumbItem[] = []
  
  messages.forEach((msg, idx) => {
    if (msg.type === 'user') {
      const content = msg.content.toLowerCase()
      
      // Detect drill-down patterns
      if (content.includes('drill down') || content.includes('break down') || content.includes('show me')) {
        // Extract entity being drilled into
        const match = content.match(/(?:drill down|break down|show me).*?(?:on|for|by)\s+([^.?!]+)/i)
        if (match) {
          breadcrumbs.push({ label: match[1].trim(), level: idx })
        }
      }
      
      // Detect filter patterns
      if (content.includes('filter') || content.includes('where') || content.includes('only')) {
        const match = content.match(/(?:filter|where|only).*?([A-Z_]+\s*=\s*['""]?[^'"".?!]+['""]?)/i)
        if (match) {
          breadcrumbs.push({ label: match[1].trim(), level: idx })
        }
      }
      
      // Detect "by region/category" patterns
      if (content.match(/by\s+(region|category|country|product|customer|department|quarter|month|year)/i)) {
        const match = content.match(/by\s+(\w+)/i)
        if (match) {
          breadcrumbs.push({ label: `By ${match[1]}`, level: idx })
        }
      }
    }
  })
  
  // Only show if we have drill-down context
  if (breadcrumbs.length === 0) return null
  
  return (
    <div className="px-6 py-2 bg-white/50 dark:bg-slate-900/50 border-b border-gray-200 dark:border-slate-800">
      <div className="flex items-center gap-2 text-sm overflow-x-auto">
        <Home className="w-4 h-4 text-gray-400 flex-shrink-0" />
        <ChevronRight className="w-4 h-4 text-gray-300 flex-shrink-0" />
        
        {breadcrumbs.map((crumb, idx) => (
          <div key={idx} className="flex items-center gap-2 flex-shrink-0">
            <span className={cn(
              "px-2 py-1 rounded text-xs font-medium",
              idx === breadcrumbs.length - 1
                ? "bg-emerald-100 dark:bg-emerald-900/30 text-emerald-700 dark:text-emerald-300"
                : "bg-gray-100 dark:bg-slate-800 text-gray-600 dark:text-gray-400"
            )}>
              {crumb.label}
            </span>
            {idx < breadcrumbs.length - 1 && (
              <ChevronRight className="w-4 h-4 text-gray-300 flex-shrink-0" />
            )}
          </div>
        ))}
      </div>
    </div>
  )
}
