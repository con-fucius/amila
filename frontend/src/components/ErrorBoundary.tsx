import React, { Component, ReactNode } from 'react'
import { AlertTriangle, RefreshCw } from 'lucide-react'
import { Button } from './ui/button'
import { Card, CardContent } from './ui/card'

interface Props {
  children: ReactNode
  fallback?: ReactNode
}

interface State {
  hasError: boolean
  error: Error | null
  errorInfo: React.ErrorInfo | null
}

export class ErrorBoundary extends Component<Props, State> {
  constructor(props: Props) {
    super(props)
    this.state = {
      hasError: false,
      error: null,
      errorInfo: null,
    }
  }

  static getDerivedStateFromError(): Partial<State> {
    return { hasError: true }
  }

  override componentDidCatch(error: Error, errorInfo: React.ErrorInfo) {
    console.error('ErrorBoundary caught an error:', error, errorInfo)
    this.setState({
      error,
      errorInfo,
    })

    // Report error to backend
    this.reportErrorToBackend(error, errorInfo)
  }

  private async reportErrorToBackend(error: Error, errorInfo: React.ErrorInfo) {
    try {
      const API_BASE_URL = import.meta.env.VITE_API_URL || 'http://127.0.0.1:8000'
      const token = localStorage.getItem('access_token')
      
      await fetch(`${API_BASE_URL}/api/v1/errors/report`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          ...(token ? { 'Authorization': `Bearer ${token}` } : {}),
        },
        body: JSON.stringify({
          message: error.message || error.toString(),
          stack: error.stack,
          url: window.location.href,
          component: errorInfo.componentStack?.split('\n')[1]?.trim() || 'Unknown',
          user_agent: navigator.userAgent,
          additional_context: {
            timestamp: new Date().toISOString(),
            viewport: `${window.innerWidth}x${window.innerHeight}`,
          },
        }),
      })
    } catch (reportError) {
      console.error('Failed to report error to backend:', reportError)
    }
  }

  handleReset = () => {
    this.setState({
      hasError: false,
      error: null,
      errorInfo: null,
    })
  }

  override render() {
    if (this.state.hasError) {
      if (this.props.fallback) {
        return this.props.fallback
      }

      return (
        <div className="min-h-screen flex items-center justify-center bg-gray-50 p-4">
          <Card className="max-w-2xl w-full border-red-300 bg-red-50">
            <CardContent className="p-6">
              <div className="flex items-start gap-4">
                <AlertTriangle className="h-8 w-8 text-red-600 flex-shrink-0" />
                <div className="flex-1">
                  <h1 className="text-xl font-bold text-red-900 mb-2">
                    Something went wrong
                  </h1>
                  <p className="text-red-700 mb-4">
                    The application encountered an unexpected error. This has been logged and we'll look into it.
                  </p>

                  {this.state.error && (
                    <div className="bg-white rounded border border-red-200 p-3 mb-4">
                      <p className="font-mono text-sm text-red-800 mb-2">
                        {this.state.error.toString()}
                      </p>
                      {this.state.errorInfo && (
                        <details className="text-xs text-gray-600">
                          <summary className="cursor-pointer hover:text-gray-900">
                            Stack trace
                          </summary>
                          <pre className="mt-2 overflow-auto max-h-48 bg-gray-50 p-2 rounded">
                            {this.state.errorInfo.componentStack}
                          </pre>
                        </details>
                      )}
                    </div>
                  )}

                  <div className="flex gap-3">
                    <Button
                      onClick={this.handleReset}
                      className="bg-red-600 hover:bg-red-700"
                    >
                      <RefreshCw className="h-4 w-4 mr-2" />
                      Try Again
                    </Button>
                    <Button
                      variant="outline"
                      onClick={() => window.location.reload()}
                    >
                      Reload Page
                    </Button>
                  </div>
                </div>
              </div>
            </CardContent>
          </Card>
        </div>
      )
    }

    return this.props.children
  }
}
