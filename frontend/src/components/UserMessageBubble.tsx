import { Card, CardContent } from './ui/card'
import type { ChatMessage } from '@/stores/chatStore'

interface UserMessageBubbleProps {
  message: ChatMessage
}

export function UserMessageBubble({ message }: UserMessageBubbleProps) {
  return (
    <Card className="min-w-[40%] max-w-[80%] bg-white dark:bg-slate-900 border border-gray-200 dark:border-slate-800 shadow-sm">
      <CardContent className="pt-2.5 pr-3.5 pb-2.5 pl-3.5 relative">
        <span className="chat-timestamp float-right ml-2 text-gray-400 dark:text-gray-300">
          {new Date(message.timestamp).toLocaleTimeString('en-US', { hour: 'numeric', minute: '2-digit' })}
        </span>
        <p className="text-sm text-gray-800 dark:text-gray-100 leading-relaxed whitespace-pre-wrap break-words">{message.content}</p>
      </CardContent>
    </Card>
  )
}
