import React from 'react'
import ReactDOM from 'react-dom/client'
import App from './App.tsx'
import './index.css'

// Global error handler for unhandled errors
const reportGlobalError = async (message: string, stack?: string) => {
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
        message,
        stack,
        url: window.location.href,
        component: 'global',
        user_agent: navigator.userAgent,
        additional_context: {
          timestamp: new Date().toISOString(),
          type: 'unhandled_error',
        },
      }),
    })
  } catch (e) {
    console.error('Failed to report global error:', e)
  }
}

// Handle unhandled errors
window.addEventListener('error', (event) => {
  reportGlobalError(event.message, event.error?.stack)
})

// Handle unhandled promise rejections
window.addEventListener('unhandledrejection', (event) => {
  const message = event.reason?.message || String(event.reason)
  const stack = event.reason?.stack
  reportGlobalError(`Unhandled Promise Rejection: ${message}`, stack)
})

// Theme is now handled in App.tsx with dark mode support
ReactDOM.createRoot(document.getElementById('root')!).render(
  <React.StrictMode>
    <App />
  </React.StrictMode>,
)