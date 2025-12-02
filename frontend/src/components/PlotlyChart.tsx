import { useEffect, useRef, useState } from 'react'
import { Loader2 } from 'lucide-react'

interface PlotlyChartProps {
  plotlyJson: any
  className?: string
}

export function PlotlyChart({ plotlyJson, className = '' }: PlotlyChartProps) {
  const containerRef = useRef<HTMLDivElement>(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)

  useEffect(() => {
    if (!plotlyJson || !containerRef.current) return

    const loadPlotly = async () => {
      try {
        setLoading(true)
        setError(null)

        // Dynamically import Plotly
        const Plotly = await import('plotly.js-dist-min')

        if (containerRef.current) {
          // Clear previous chart
          Plotly.purge(containerRef.current)

          // Render new chart
          await Plotly.newPlot(
            containerRef.current,
            plotlyJson.data,
            {
              ...plotlyJson.layout,
              autosize: true,
              responsive: true,
            },
            {
              responsive: true,
              displayModeBar: true,
              displaylogo: false,
              modeBarButtonsToRemove: ['lasso2d', 'select2d'],
            }
          )
        }
      } catch (err) {
        console.error('Failed to render Plotly chart:', err)
        setError('Failed to render chart')
      } finally {
        setLoading(false)
      }
    }

    loadPlotly()

    // Cleanup on unmount
    return () => {
      if (containerRef.current) {
        import('plotly.js-dist-min').then((Plotly) => {
          if (containerRef.current) {
            Plotly.purge(containerRef.current)
          }
        }).catch(() => {})
      }
    }
  }, [plotlyJson])

  if (error) {
    return (
      <div className={`flex items-center justify-center p-8 bg-red-50 dark:bg-red-900/20 rounded-lg ${className}`}>
        <p className="text-red-600 dark:text-red-400 text-sm">{error}</p>
      </div>
    )
  }

  return (
    <div className={`relative ${className}`}>
      {loading && (
        <div className="absolute inset-0 flex items-center justify-center bg-white/80 dark:bg-slate-900/80 z-10 rounded-lg">
          <Loader2 className="w-6 h-6 animate-spin text-emerald-500" />
        </div>
      )}
      <div ref={containerRef} className="w-full min-h-[400px]" />
    </div>
  )
}
