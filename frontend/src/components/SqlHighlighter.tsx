import { useState, useCallback } from 'react'

import { Light as SyntaxHighlighter } from 'react-syntax-highlighter'
import sql from 'react-syntax-highlighter/dist/esm/languages/hljs/sql'
import { atomOneDark, atomOneLight } from 'react-syntax-highlighter/dist/esm/styles/hljs'
import { Box, IconButton, Tooltip, useTheme, Collapse } from '@mui/material'

import { ContentCopy as CopyIcon, ExpandMore as ExpandIcon } from '@mui/icons-material'
import { motion } from 'framer-motion'
import { useSnackbar } from '../contexts/SnackbarContext'

// Register SQL language
SyntaxHighlighter.registerLanguage('sql', sql)

interface SqlHighlighterProps {
  code: string
  collapsible?: boolean
  defaultExpanded?: boolean
  maxHeight?: string | number
  showLineNumbers?: boolean
}

const SqlHighlighter: React.FC<SqlHighlighterProps> = ({
  code,
  collapsible = false,
  defaultExpanded = true,
  maxHeight = 'none',
  showLineNumbers = true,
}) => {
  const theme = useTheme()
  const { success } = useSnackbar()
  const [expanded, setExpanded] = useState(defaultExpanded)

  const handleCopy = useCallback(() => {
    navigator.clipboard.writeText(code)
    success('SQL copied to clipboard!', 2000)
  }, [code, success])

  const isDark = theme.palette.mode === 'dark'

  return (
    <Box
      sx={{
        position: 'relative',
        borderRadius: 2,
        overflow: 'hidden',
        border: `1px solid ${theme.palette.divider}`,
        bgcolor: isDark ? '#282c34' : '#fafafa',
      }}
    >
      {/* Header with Copy Button */}
      <Box
        sx={{
          display: 'flex',
          alignItems: 'center',
          justifyContent: 'space-between',
          px: 2,
          py: 1,
          bgcolor: isDark ? '#21252b' : '#f0f0f0',
          borderBottom: `1px solid ${theme.palette.divider}`,
        }}
      >
        <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
          <Box
            component="span"
            sx={{
              fontFamily: 'monospace',
              fontSize: '0.75rem',
              fontWeight: 600,
              color: theme.palette.text.secondary,
              letterSpacing: '0.5px',
            }}
          >
            SQL
          </Box>
          {collapsible && (
            <motion.div
              animate={{ rotate: expanded ? 180 : 0 }}
              transition={{ duration: 0.3 }}
            >
              <IconButton
                size="small"
                onClick={() => setExpanded(!expanded)}
                sx={{ padding: 0.5 }}
              >
                <ExpandIcon fontSize="small" />
              </IconButton>
            </motion.div>
          )}
        </Box>
        <Tooltip title="Copy SQL">
          <IconButton
            size="small"
            onClick={handleCopy}
            sx={{
              color: theme.palette.text.secondary,
              '&:hover': {
                color: theme.palette.primary.main,
                bgcolor: theme.palette.action.hover,
              },
            }}
          >
            <CopyIcon fontSize="small" />
          </IconButton>
        </Tooltip>
      </Box>

      {/* SQL Code with Collapse */}
      <Collapse in={expanded} timeout={300}>
        <Box
          sx={{
            maxHeight,
            overflow: 'auto',
            '&::-webkit-scrollbar': {
              width: 8,
              height: 8,
            },
            '&::-webkit-scrollbar-track': {
              background: isDark ? '#21252b' : '#f0f0f0',
            },
            '&::-webkit-scrollbar-thumb': {
              background: isDark ? '#4b5263' : '#c1c1c1',
              borderRadius: 4,
              '&:hover': {
                background: isDark ? '#5c6370' : '#a0a0a0',
              },
            },
          }}
        >
          <SyntaxHighlighter
            language="sql"
            style={isDark ? atomOneDark : atomOneLight}
            showLineNumbers={showLineNumbers}
            wrapLines={true}
            customStyle={{
              margin: 0,
              padding: '16px',
              fontSize: '0.813rem',
              fontFamily: 'Consolas, Monaco, "Courier New", monospace',
              background: 'transparent',
            }}
            lineNumberStyle={{
              minWidth: '2.5em',
              paddingRight: '1em',
              color: isDark ? '#5c6370' : '#9e9e9e',
              userSelect: 'none',
            }}
          >
            {code}
          </SyntaxHighlighter>
        </Box>
      </Collapse>
    </Box>
  )
}

export default SqlHighlighter