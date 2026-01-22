/**
 * Natural Language Citation Matching
 * Identifies which table cells support statements in the assistant's response
 */

export interface CellCitation {
  rowIndex: number
  columnIndex: number
  value: any
  matchedText: string
}

/**
 * Extract numeric values and their context from text
 */
function extractNumericMentions(text: string): Array<{ value: number; context: string }> {
  const mentions: Array<{ value: number; context: string }> = []
  
  // Match patterns like "sales of $1,234.56" or "1234 customers" or "45.2%"
  const patterns = [
    /\$?([\d,]+\.?\d*)\s*(?:million|thousand|billion|M|K|B)?/gi,
    /([\d,]+\.?\d*)\s*%/gi,
    /([\d,]+\.?\d*)\s+(\w+)/gi
  ]
  
  patterns.forEach(pattern => {
    let match
    while ((match = pattern.exec(text)) !== null) {
      const numStr = match[1].replace(/,/g, '')
      const num = parseFloat(numStr)
      if (!isNaN(num)) {
        // Get surrounding context (20 chars before and after)
        const start = Math.max(0, match.index - 20)
        const end = Math.min(text.length, match.index + match[0].length + 20)
        const context = text.substring(start, end)
        mentions.push({ value: num, context })
      }
    }
  })
  
  return mentions
}

/**
 * Extract string mentions from text
 */
function extractStringMentions(text: string): string[] {
  const mentions: string[] = []
  
  // Match quoted strings
  const quotedPattern = /["']([^"']{2,})["']/g
  let match
  while ((match = quotedPattern.exec(text)) !== null) {
    mentions.push(match[1])
  }
  
  // Match capitalized words (potential entity names)
  const capitalizedPattern = /\b([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*)\b/g
  while ((match = capitalizedPattern.exec(text)) !== null) {
    if (match[1].length > 2) { // Avoid short words
      mentions.push(match[1])
    }
  }
  
  return mentions
}

/**
 * Find cells in table data that match values mentioned in text
 */
export function findCitedCells(
  assistantText: string,
  _columns: string[],
  rows: any[][]
): CellCitation[] {
  const citations: CellCitation[] = []
  
  // Extract mentions from text
  const numericMentions = extractNumericMentions(assistantText)
  const stringMentions = extractStringMentions(assistantText)
  
  // Search for matching cells
  rows.forEach((row, rowIndex) => {
    row.forEach((cellValue, colIndex) => {
      // Skip null/undefined
      if (cellValue === null || cellValue === undefined) return
      
      // Check numeric matches
      if (typeof cellValue === 'number') {
        numericMentions.forEach(mention => {
          // Allow small tolerance for floating point
          if (Math.abs(cellValue - mention.value) < 0.01) {
            citations.push({
              rowIndex,
              columnIndex: colIndex,
              value: cellValue,
              matchedText: mention.context
            })
          }
        })
      }
      
      // Check string matches
      if (typeof cellValue === 'string') {
        const cellLower = cellValue.toLowerCase()
        stringMentions.forEach(mention => {
          if (cellLower.includes(mention.toLowerCase()) || mention.toLowerCase().includes(cellLower)) {
            citations.push({
              rowIndex,
              columnIndex: colIndex,
              value: cellValue,
              matchedText: mention
            })
          }
        })
      }
    })
  })
  
  // Remove duplicates
  const uniqueCitations = citations.filter((citation, index, self) =>
    index === self.findIndex(c => 
      c.rowIndex === citation.rowIndex && c.columnIndex === citation.columnIndex
    )
  )
  
  return uniqueCitations
}

/**
 * Check if a specific cell is cited
 */
export function isCellCited(
  rowIndex: number,
  columnIndex: number,
  citations: CellCitation[]
): boolean {
  return citations.some(c => c.rowIndex === rowIndex && c.columnIndex === columnIndex)
}
