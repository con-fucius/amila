import type { FC } from 'react'
import { Chip, alpha, useTheme } from '@mui/material'

interface ColumnTypeBadgeProps {
  columnName: string
  columnType: string
  size?: 'small' | 'medium'
  variant?: 'filled' | 'outlined'
}

export const getColumnTypeColor = (type: string): string => {
  const normalizedType = type.toLowerCase()
  
  // VARCHAR, CHAR, TEXT, STRING types - Blue
  if (normalizedType.includes('varchar') || normalizedType.includes('char') || 
      normalizedType.includes('text') || normalizedType.includes('string') ||
      normalizedType.includes('clob')) {
    return '#3b82f6' // blue
  }
  
  // NUMBER, INTEGER, DECIMAL, FLOAT types - Green
  if (normalizedType.includes('number') || normalizedType.includes('int') || 
      normalizedType.includes('decimal') || normalizedType.includes('float') ||
      normalizedType.includes('double') || normalizedType.includes('numeric')) {
    return '#10b981' // green
  }
  
  // DATE, TIMESTAMP, DATETIME types - Orange/Amber
  if (normalizedType.includes('date') || normalizedType.includes('time') ||
      normalizedType.includes('timestamp')) {
    return '#f59e0b' // orange
  }
  
  // BOOLEAN, BIT types - Purple
  if (normalizedType.includes('bool') || normalizedType.includes('bit')) {
    return '#8b5cf6' // purple
  }
  
  // BLOB, BINARY types - Red
  if (normalizedType.includes('blob') || normalizedType.includes('binary') ||
      normalizedType.includes('raw')) {
    return '#ef4444' // red
  }
  
  // JSON, XML types - Cyan
  if (normalizedType.includes('json') || normalizedType.includes('xml')) {
    return '#06b6d4' // cyan
  }
  
  // Default - Gray
  return '#6b7280' // gray
}

export const ColumnTypeBadge: FC<ColumnTypeBadgeProps> = ({
  columnName,
  columnType,
  size = 'small',
  variant = 'filled',
}) => {
  const color = getColumnTypeColor(columnType)
  
  return (
    <Chip
      label={columnName}
      size={size}
      variant={variant}
      sx={{
        bgcolor: variant === 'filled' ? color : 'transparent',
        color: variant === 'filled' ? 'white' : color,
        borderColor: variant === 'outlined' ? color : undefined,
        fontFamily: '"JetBrains Mono", monospace',
        fontSize: '0.75rem',
        height: size === 'small' ? 20 : 24,
        fontWeight: 600,
        letterSpacing: '0.02em',
        '& .MuiChip-label': {
          px: 1,
        },
        boxShadow: variant === 'filled' 
          ? `0 1px 3px ${alpha(color, 0.3)}`
          : 'none',
        transition: 'all 0.2s',
        '&:hover': {
          transform: 'translateY(-1px)',
          boxShadow: variant === 'filled'
            ? `0 2px 6px ${alpha(color, 0.4)}`
            : `0 1px 3px ${alpha(color, 0.2)}`,
        },
      }}
    />
  )
}

// Utility function to format column type for display
export const formatColumnType = (type: string): string => {
  // Remove precision/scale info for cleaner display
  return type.replace(/\(\d+(?:,\s*\d+)?\)/g, '').toUpperCase()
}

// Component that shows column name with type badge together
interface ColumnDisplayProps {
  columnName: string
  columnType: string
  showType?: boolean
}

export const ColumnDisplay: FC<ColumnDisplayProps> = ({
  columnName,
  columnType,
  showType = true,
}) => {
  const theme = useTheme()
  
  return (
    <span style={{ display: 'inline-flex', alignItems: 'center', gap: '6px' }}>
      <ColumnTypeBadge
        columnName={columnName}
        columnType={columnType}
        size="small"
        variant="filled"
      />
      {showType && (
        <span
          style={{
            fontFamily: '"JetBrains Mono", monospace',
            fontSize: '0.7rem',
            color: theme.palette.text.secondary,
            fontWeight: 500,
          }}
        >
          ({formatColumnType(columnType)})
        </span>
      )}
    </span>
  )
}
