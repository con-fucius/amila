import React, { useState, useEffect, useMemo } from 'react'
import { BrowserRouter, Routes, Route } from 'react-router-dom'
import {
  CssBaseline,
  ThemeProvider,
  PaletteMode,
} from '@mui/material'
import { SnackbarProvider } from './contexts/SnackbarContext'
import { RealChatInterface } from './components/RealChatInterface'
import { MainLayout } from './components/MainLayout'
import { QueryBuilder } from './pages/QueryBuilder'
import { SchemaBrowser } from './pages/SchemaBrowser'
import { Login } from './pages/Login'
import { Account } from './pages/Account'
import { AuthGuard } from './components/AuthGuard'
import { ErrorBoundary } from './components/ErrorBoundary'
import { createAppTheme } from './theme/theme'

// Create ThemeContext for dark mode toggle
export const ColorModeContext = React.createContext({ 
  toggleColorMode: () => {},
  mode: 'light' as PaletteMode
})

function App() {
  // Initialize theme mode from localStorage, default to 'light'
  const [mode, setMode] = useState<PaletteMode>(() => {
    const savedMode = localStorage.getItem('themeMode')
    return (savedMode === 'dark' || savedMode === 'light') ? savedMode : 'light'
  })

  // Save theme mode to localStorage whenever it changes
  useEffect(() => {
    localStorage.setItem('themeMode', mode)
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
  return (
    <ErrorBoundary>
      <ColorModeContext.Provider value={colorMode}>
        <BrowserRouter>
          <ThemeProvider theme={theme}>
            <CssBaseline />
            <SnackbarProvider>
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
                        </Routes>
                      </MainLayout>
                    </AuthGuard>
                  }
                />
              </Routes>
            </SnackbarProvider>
          </ThemeProvider>
        </BrowserRouter>
      </ColorModeContext.Provider>
    </ErrorBoundary>
  )
}

export default App