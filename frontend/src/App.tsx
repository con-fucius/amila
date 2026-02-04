import React, { useState, useEffect, useMemo, Suspense } from 'react'
import { BrowserRouter, Routes, Route } from 'react-router-dom'
import {
  CssBaseline,
  ThemeProvider,
  PaletteMode,
} from '@mui/material'
import { SnackbarProvider } from './contexts/SnackbarContext'
import { MainLayout } from './components/MainLayout'
const RealChatInterface = React.lazy(() => import('./components/RealChatInterface').then(m => ({ default: m.RealChatInterface })))
const QueryBuilder = React.lazy(() => import('./pages/QueryBuilder').then(m => ({ default: m.QueryBuilder })))
const SchemaBrowser = React.lazy(() => import('./pages/SchemaBrowser').then(m => ({ default: m.SchemaBrowser })))
const Login = React.lazy(() => import('./pages/Login').then(m => ({ default: m.Login })))
const Account = React.lazy(() => import('./pages/Account').then(m => ({ default: m.Account })))
const Settings = React.lazy(() => import('./pages/Settings').then(m => ({ default: m.Settings })))
const Governance = React.lazy(() => import('./pages/Governance').then(m => ({ default: m.Governance })))
const Webhooks = React.lazy(() => import('./pages/Webhooks').then(m => ({ default: m.Webhooks })))
const RateLimits = React.lazy(() => import('./pages/RateLimits').then(m => ({ default: m.RateLimits })))
const BudgetForecasting = React.lazy(() => import('./components/BudgetForecasting').then(m => ({ default: m.BudgetForecasting })))
const SkillGeneratorAdmin = React.lazy(() => import('./components/SkillGeneratorAdmin').then(m => ({ default: m.SkillGeneratorAdmin })))
import { AuthGuard } from './components/AuthGuard'
import { ErrorBoundary } from './components/ErrorBoundary'
import { createAppTheme } from './theme/theme'

// Create ThemeContext for dark mode toggle
export const ColorModeContext = React.createContext({
  toggleColorMode: () => { },
  mode: 'light' as PaletteMode
})

function App() {
  // Initialize theme mode from localStorage, default to 'light'
  const [mode, setMode] = useState<PaletteMode>(() => {
    const savedMode = localStorage.getItem('themeMode')
    const initialMode = (savedMode === 'dark' || savedMode === 'light') ? savedMode : 'light'
    // Apply dark class immediately during initialization to prevent flash
    if (typeof document !== 'undefined') {
      document.documentElement.classList.toggle('dark', initialMode === 'dark')
    }
    return initialMode
  })

  // Save theme mode to localStorage and sync document class whenever it changes
  useEffect(() => {
    localStorage.setItem('themeMode', mode)
    document.documentElement.classList.toggle('dark', mode === 'dark')
  }, [mode])

  const colorMode = useMemo(
    () => ({
      toggleColorMode: () => {
        setMode((prevMode) => (prevMode === 'light' ? 'dark' : 'light'))
      },
      mode,
    }),
    [mode]
  )

  const theme = useMemo(() => createAppTheme(mode), [mode])

  // Use new Tailwind interface
  const RouteFallback = (
    <div className="h-screen w-full flex items-center justify-center text-gray-600">
      Loading...
    </div>
  )

  return (
    <ErrorBoundary>
      <ColorModeContext.Provider value={colorMode}>
        <BrowserRouter>
          <ThemeProvider theme={theme}>
            <CssBaseline />
            <SnackbarProvider>
              <Suspense fallback={RouteFallback}>
                <Routes>
                  <Route path="/login" element={<Login />} />
                  <Route
                    path="/*"
                    element={
                      <AuthGuard>
                        <MainLayout>
                          <Routes>
                            <Route path="/" element={<RealChatInterface />} />
                            <Route path="/query-builder" element={<QueryBuilder />} />
                            <Route path="/schema-browser" element={<SchemaBrowser />} />
                            <Route path="/account" element={<Account />} />
                            <Route path="/settings" element={<Settings />} />
                            <Route path="/settings/budget" element={<BudgetForecasting />} />
                            <Route path="/settings/webhooks" element={<Webhooks />} />
                            <Route path="/settings/ratelimits" element={<RateLimits />} />
                            <Route path="/governance" element={<Governance />} />
                            <Route path="/governance/skills" element={<SkillGeneratorAdmin />} />
                          </Routes>
                        </MainLayout>
                      </AuthGuard>
                    }
                  />
                </Routes>
              </Suspense>
            </SnackbarProvider>
          </ThemeProvider>
        </BrowserRouter>
      </ColorModeContext.Provider>
    </ErrorBoundary>
  )
}

export default App
