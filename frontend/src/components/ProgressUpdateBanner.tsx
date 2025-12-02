import React from 'react'
import { Alert, Box, Typography, Fade, alpha, useTheme } from '@mui/material'
import { Info as InfoIcon } from '@mui/icons-material'
import { ColumnTypeBadge } from './ColumnTypeBadge'
import { motion } from 'framer-motion'

interface DiscoveredColumn {
  name: string
  type: string
}

interface ProgressUpdateBannerProps {
  message: string
  columns?: DiscoveredColumn[]
  severity?: 'info' | 'success' | 'warning' | 'error'
  visible?: boolean
  onClose?: () => void
}

export const ProgressUpdateBanner: React.FC<ProgressUpdateBannerProps> = ({
  message,
  columns = [],
  severity = 'info',
  visible = true,
  onClose,
}) => {
  const theme = useTheme()

  if (!visible) return null

  return (
    <Fade in={visible} timeout={400}>
      <motion.div
        initial={{ opacity: 0, y: -10 }}
        animate={{ opacity: 1, y: 0 }}
        exit={{ opacity: 0, y: -10 }}
        transition={{ type: 'spring', stiffness: 300, damping: 30 }}
      >
        <Alert
          severity={severity}
          icon={<InfoIcon />}
          onClose={onClose}
          sx={{
            mb: 2,
            bgcolor: alpha(theme.palette[severity].main, 0.08),
            border: `1px solid ${alpha(theme.palette[severity].main, 0.2)}`,
            borderLeft: `4px solid ${theme.palette[severity].main}`,
            '& .MuiAlert-icon': {
              fontSize: '1.3rem',
            },
            '& .MuiAlert-message': {
              width: '100%',
            },
          }}
        >
          <Box>
            <Typography
              variant="body2"
              sx={{
                fontFamily: '"Figtree", sans-serif',
                fontSize: '0.875rem',
                fontWeight: 500,
                mb: columns.length > 0 ? 1 : 0,
              }}
            >
              {message}
            </Typography>

            {columns.length > 0 && (
              <Box
                sx={{
                  display: 'flex',
                  flexWrap: 'wrap',
                  gap: 0.75,
                  mt: 1,
                }}
              >
                {columns.map((col, index) => (
                  <motion.div
                    key={`${col.name}-${index}`}
                    initial={{ opacity: 0, scale: 0.8 }}
                    animate={{ opacity: 1, scale: 1 }}
                    transition={{
                      type: 'spring',
                      stiffness: 400,
                      damping: 25,
                      delay: index * 0.05,
                    }}
                  >
                    <ColumnTypeBadge
                      columnName={col.name}
                      columnType={col.type}
                      size="small"
                      variant="filled"
                    />
                  </motion.div>
                ))}
              </Box>
            )}
          </Box>
        </Alert>
      </motion.div>
    </Fade>
  )
}

// Specialized version for column discovery
interface ColumnDiscoveryBannerProps {
  tableName: string
  columns: DiscoveredColumn[]
  visible?: boolean
}

export const ColumnDiscoveryBanner: React.FC<ColumnDiscoveryBannerProps> = ({
  tableName,
  columns,
  visible = true,
}) => {
  return (
    <ProgressUpdateBanner
      message={`Progress update: identified relevant columns in ${tableName}:`}
      columns={columns}
      severity="info"
      visible={visible}
    />
  )
}
