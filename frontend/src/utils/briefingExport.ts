/**
 * Executive Briefing Export Utility
 * Strips chat fluff and creates clean executive briefs
 */

export interface BriefingData {
    title: string
    queries: Array<{
        question: string
        answer: string
        sql?: string
        result?: {
            columns: string[]
            rows: any[][]
            rowCount: number
        }
        chart?: any
    }>
    timestamp: Date
}

/**
 * Generate HTML executive brief
 */
export function generateHTMLBrief(data: BriefingData): string {
    const html = `
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>${data.title}</title>
    <link rel="preconnect" href="https://fonts.googleapis.com">
    <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
    <link href="https://fonts.googleapis.com/css2?family=Kumbh+Sans:wght@400;500;600;700&display=swap" rel="stylesheet">
    <link href="https://fonts.googleapis.com/css2?family=Cantarell&display=swap" rel="stylesheet">
    <style>
        * { margin: 0; padding: 0; box-sizing: border-box; }
        body {
            font-family: 'Kumbh Sans', -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
            line-height: 1.4;
            color: #1f2937;
            max-width: 1000px;
            margin: 0 auto;
            padding: 24px 20px;
            background: #f9fafb;
            font-size: 13px;
        }
        .header {
            background: linear-gradient(135deg, #10b981 0%, #059669 100%);
            color: white;
            padding: 16px 20px;
            border-radius: 8px;
            margin-bottom: 20px;
            box-shadow: 0 4px 6px rgba(0,0,0,0.1);
        }
        .header h1 {
            font-size: 21px;
            font-weight: 700;
            margin-bottom: 4px;
        }
        .header .meta {
            font-size: 11px;
            opacity: 0.9;
        }
        .query-section {
            background: white;
            border-radius: 8px;
            padding: 16px;
            margin-bottom: 12px;
            box-shadow: 0 1px 2px rgba(0,0,0,0.05);
            border: 1px solid #e5e7eb;
        }
        .query-section h2 {
            font-size: 14px;
            color: #059669;
            margin-bottom: 8px;
            font-weight: 600;
        }
        .answer {
            font-size: 13px;
            color: #374151;
            margin-bottom: 12px;
            line-height: 1.5;
        }
        .data-table {
            width: 100%;
            border-collapse: collapse;
            margin-top: 12px;
            font-size: 11px;
        }
        .data-table th {
            background: #f3f4f6;
            padding: 6px 8px;
            text-align: left;
            font-weight: 600;
            border-bottom: 2px solid #e5e7eb;
        }
        .data-table td {
            padding: 6px 8px;
            border-bottom: 1px solid #e5e7eb;
        }
        .data-table tr:hover {
            background: #f9fafb;
        }
        .sql-code {
            background: #1f2937;
            color: #10b981;
            padding: 8px;
            border-radius: 4px;
            font-family: 'Cantarell', monospace;
            font-size: 10px;
            overflow-x: auto;
            margin-top: 8px;
        }
        .footer {
            text-align: center;
            color: #6b7280;
            font-size: 12px;
            margin-top: 40px;
            padding-top: 20px;
            border-top: 1px solid #e5e7eb;
        }
        @media print {
            body { background: white; }
            .query-section { page-break-inside: avoid; }
        }
    </style>
</head>
<body>
    <div class="header">
        <h1>${data.title}</h1>
        <div class="meta">Generated on ${data.timestamp.toLocaleString()}</div>
    </div>
    
    ${data.queries.map((q, idx) => `
        <div class="query-section">
            <h2>Query ${idx + 1}: ${q.question}</h2>
            <div class="answer">${q.answer}</div>
            
            ${q.result ? `
                <table class="data-table">
                    <thead>
                        <tr>
                            ${q.result.columns.map(col => `<th>${col}</th>`).join('')}
                        </tr>
                    </thead>
                    <tbody>
                        ${q.result.rows.slice(0, 50).map(row => `
                            <tr>
                                ${row.map(cell => `<td>${cell !== null && cell !== undefined ? cell : 'NULL'}</td>`).join('')}
                            </tr>
                        `).join('')}
                    </tbody>
                </table>
                ${q.result.rows.length > 50 ? `<div style="margin-top: 8px; font-size: 12px; color: #6b7280;">Showing first 50 of ${q.result.rowCount} rows</div>` : ''}
            ` : ''}
            
            ${q.sql ? `
                <details style="margin-top: 12px;">
                    <summary style="cursor: pointer; font-size: 13px; color: #6b7280;">View SQL Query</summary>
                    <pre class="sql-code">${q.sql}</pre>
                </details>
            ` : ''}
        </div>
    `).join('')}
    
    <div class="footer">
        <p>Executive Brief generated by Amila AI-Assisted Database Query Platform</p>
        <p>Confidential - For Internal Use Only</p>
    </div>
</body>
</html>
  `

    return html
}

/**
 * Download HTML brief as file
 */
export function downloadHTMLBrief(data: BriefingData, filename: string = 'executive-brief') {
    const html = generateHTMLBrief(data)
    const blob = new Blob([html], { type: 'text/html' })
    const url = URL.createObjectURL(blob)
    const a = document.createElement('a')
    a.href = url
    a.download = `${filename}-${Date.now()}.html`
    document.body.appendChild(a)
    a.click()
    document.body.removeChild(a)
    URL.revokeObjectURL(url)
}

/**
 * Print HTML brief
 */
export function printHTMLBrief(data: BriefingData) {
    const html = generateHTMLBrief(data)
    const printWindow = window.open('', '_blank')
    if (printWindow) {
        printWindow.document.write(html)
        printWindow.document.close()
        printWindow.focus()
        setTimeout(() => {
            printWindow.print()
        }, 250)
    }
}
