
import { CheckCircle2, Circle, Clock } from 'lucide-react'
import { Progress } from '@/components/ui/progress'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { cn } from '@/utils/cn'

interface ExecutionTimelineProps {
    totalTimeMs: number
    steps?: Array<{ name: string; duration: number; status: 'completed' | 'running' | 'pending' | 'error' }>
}

export function ExecutionTimeline({ totalTimeMs, steps: providedSteps }: ExecutionTimelineProps) {
    // Simulate steps if not provided
    const steps = providedSteps || [
        { name: 'Query Parsing', duration: Math.max(10, Math.round(totalTimeMs * 0.05)), status: 'completed' },
        { name: 'Plan Optimization', duration: Math.max(20, Math.round(totalTimeMs * 0.1)), status: 'completed' },
        { name: 'Engine Execution', duration: Math.max(50, Math.round(totalTimeMs * 0.8)), status: 'completed' },
        { name: 'Result Fetching', duration: Math.max(10, Math.round(totalTimeMs * 0.05)), status: 'completed' },
    ] as const

    return (
        <Card className="border-none shadow-none bg-transparent">
            <CardHeader className="px-0 py-2">
                <CardTitle className="text-sm font-medium flex items-center gap-2">
                    <Clock className="w-4 h-4 text-emerald-500" />
                    Execution Timeline
                    <span className="text-xs font-normal text-muted-foreground ml-auto bg-slate-100 dark:bg-slate-800 px-2 py-1 rounded-full">
                        Total: {totalTimeMs}ms
                    </span>
                </CardTitle>
            </CardHeader>
            <CardContent className="px-0 py-2">
                <div className="space-y-4">
                    <div className="relative">
                        {/* Vertical Line */}
                        <div className="absolute left-2.5 top-2 bottom-4 w-px bg-gray-200 dark:bg-gray-800" />

                        {steps.map((step, index) => (
                            <div key={index} className="flex gap-4 items-start relative mb-4 last:mb-0 group">
                                <div className="relative z-10 pt-1">
                                    {step.status === 'completed' ? (
                                        <CheckCircle2 className="w-5 h-5 text-emerald-500 bg-white dark:bg-slate-900" />
                                    ) : step.status === 'running' ? (
                                        <div className="w-5 h-5 rounded-full border-2 border-primary border-t-transparent animate-spin bg-white dark:bg-slate-900" />
                                    ) : step.status === 'error' ? (
                                        <div className="w-5 h-5 rounded-full bg-red-500 flex items-center justify-center text-white font-bold text-xs">!</div>
                                    ) : (
                                        <Circle className="w-5 h-5 text-gray-300 dark:text-gray-600 bg-white dark:bg-slate-900" />
                                    )}
                                </div>
                                <div className="flex-1 space-y-1.5">
                                    <div className="flex items-center justify-between text-sm">
                                        <span className={cn(
                                            "font-medium",
                                            step.status === 'completed' ? "text-gray-900 dark:text-gray-100" : "text-gray-500"
                                        )}>
                                            {step.name}
                                        </span>
                                        <span className="text-xs text-muted-foreground">{step.duration}ms</span>
                                    </div>
                                    <Progress value={100} className={cn(
                                        "h-1.5",
                                        step.status === 'completed' ? "bg-emerald-100 dark:bg-emerald-900/30" : "bg-gray-100"
                                    )}>
                                        <div
                                            className="h-full bg-emerald-500 transition-all w-full"
                                            style={{ opacity: step.status === 'completed' ? 1 : 0.3 }}
                                        />
                                    </Progress>
                                </div>
                            </div>
                        ))}
                    </div>
                </div>
            </CardContent>
        </Card>
    )
}
