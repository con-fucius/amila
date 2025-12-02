import React, { createContext, useContext, useState, useCallback } from 'react'
import { Snackbar, Alert, AlertColor, Slide, SlideProps } from '@mui/material'

interface SnackbarMessage {
  id: string
  message: string
  severity: AlertColor
  autoHideDuration?: number
}

interface SnackbarContextType {
  showSnackbar: (message: string, severity?: AlertColor, autoHideDuration?: number) => void
  success: (message: string, autoHideDuration?: number) => void
  error: (message: string, autoHideDuration?: number) => void
  info: (message: string, autoHideDuration?: number) => void
  warning: (message: string, autoHideDuration?: number) => void
}

const SnackbarContext = createContext<SnackbarContextType | undefined>(undefined)

function SlideTransition(props: SlideProps) {
  return <Slide {...props} direction="up" />
}

export const SnackbarProvider: React.FC<{ children: React.ReactNode }> = ({ children }) => {
  const [snackbars, setSnackbars] = useState<SnackbarMessage[]>([])

  const showSnackbar = useCallback((
    message: string,
    severity: AlertColor = 'info',
    autoHideDuration: number = 4000
  ) => {
    const id = `snackbar-${Date.now()}-${Math.random()}`
    setSnackbars((prev) => [...prev, { id, message, severity, autoHideDuration }])
  }, [])

  const success = useCallback((message: string, autoHideDuration?: number) => {
    showSnackbar(message, 'success', autoHideDuration)
  }, [showSnackbar])

  const error = useCallback((message: string, autoHideDuration?: number) => {
    showSnackbar(message, 'error', autoHideDuration)
  }, [showSnackbar])

  const info = useCallback((message: string, autoHideDuration?: number) => {
    showSnackbar(message, 'info', autoHideDuration)
  }, [showSnackbar])

  const warning = useCallback((message: string, autoHideDuration?: number) => {
    showSnackbar(message, 'warning', autoHideDuration)
  }, [showSnackbar])

  const handleClose = useCallback((id: string) => {
    setSnackbars((prev) => prev.filter((snackbar) => snackbar.id !== id))
  }, [])

  return (
    <SnackbarContext.Provider value={{ showSnackbar, success, error, info, warning }}>
      {children}
      {snackbars.map((snackbar, index) => (
        <Snackbar
          key={snackbar.id}
          open={true}
          autoHideDuration={snackbar.autoHideDuration}
          onClose={() => handleClose(snackbar.id)}
          anchorOrigin={{ vertical: 'bottom', horizontal: 'right' }}
          TransitionComponent={SlideTransition}
          sx={{
            bottom: `${(index * 70) + 24}px !important`, // Stack snackbars vertically
          }}
        >
          <Alert
            onClose={() => handleClose(snackbar.id)}
            severity={snackbar.severity}
            variant="filled"
            sx={{
              width: '100%',
              fontFamily: '"Figtree", sans-serif',
              fontWeight: 500,
              boxShadow: '0 8px 24px rgba(0, 0, 0, 0.15)',
            }}
          >
            {snackbar.message}
          </Alert>
        </Snackbar>
      ))}
    </SnackbarContext.Provider>
  )
}

export const useSnackbar = (): SnackbarContextType => {
  const context = useContext(SnackbarContext)
  if (!context) {
    throw new Error('useSnackbar must be used within a SnackbarProvider')
  }
  return context
}