import Editor, { OnMount } from '@monaco-editor/react'
import { cn } from '@/utils/cn'

interface MonacoSQLEditorProps {
  value: string
  onChange?: (value: string | undefined) => void
  readOnly?: boolean
  height?: string
  className?: string
  onMount?: OnMount
}

export function MonacoSQLEditor({
  value,
  onChange,
  readOnly = false,
  height = '300px',
  className,
  onMount,
}: MonacoSQLEditorProps) {
  const handleEditorDidMount: OnMount = (editor, monaco) => {
    // Configure SQL language support
    monaco.languages.register({ id: 'sql' })

    // Add SQL keywords
    monaco.languages.setMonarchTokensProvider('sql', {
      defaultToken: '',
      tokenPostfix: '.sql',
      ignoreCase: true,

      keywords: [
        'SELECT', 'FROM', 'WHERE', 'INSERT', 'UPDATE', 'DELETE', 'CREATE', 'ALTER',
        'DROP', 'TABLE', 'INDEX', 'VIEW', 'JOIN', 'INNER', 'LEFT', 'RIGHT', 'OUTER',
        'ON', 'AS', 'AND', 'OR', 'NOT', 'NULL', 'IS', 'IN', 'BETWEEN', 'LIKE',
        'ORDER', 'BY', 'GROUP', 'HAVING', 'LIMIT', 'OFFSET', 'UNION', 'ALL', 'DISTINCT',
        'COUNT', 'SUM', 'AVG', 'MIN', 'MAX', 'CASE', 'WHEN', 'THEN', 'ELSE', 'END',
        'INTO', 'VALUES', 'SET', 'PRIMARY', 'KEY', 'FOREIGN', 'REFERENCES', 'DEFAULT',
        'CHECK', 'UNIQUE', 'CONSTRAINT', 'CASCADE', 'FETCH', 'FIRST', 'ROWS', 'ONLY',
        'WITH', 'RECURSIVE', 'OVER', 'PARTITION', 'RANK', 'DENSE_RANK', 'ROW_NUMBER',
        'TO_CHAR', 'TO_DATE', 'TO_NUMBER', 'CAST', 'EXTRACT', 'SUBSTRING', 'CONCAT',
        'COALESCE', 'NULLIF', 'QUARTER', 'YEAR', 'MONTH', 'DAY'
      ],

      operators: [
        '=', '>', '<', '!', '~', '?', ':', '==', '<=', '>=', '!=',
        '&&', '||', '++', '--', '+', '-', '*', '/', '&', '|', '^', '%',
        '<<', '>>', '>>>', '+=', '-=', '*=', '/=', '&=', '|=',
        '^=', '%=', '<<=', '>>=', '>>>='
      ],

      builtinFunctions: [
        'COUNT', 'SUM', 'AVG', 'MIN', 'MAX', 'TO_CHAR', 'TO_DATE', 'TO_NUMBER',
        'UPPER', 'LOWER', 'TRIM', 'LTRIM', 'RTRIM', 'LENGTH', 'SUBSTR',
        'REPLACE', 'INSTR', 'CONCAT', 'NVL', 'NVL2', 'DECODE', 'COALESCE'
      ],

      tokenizer: {
        root: [
          { include: '@comments' },
          { include: '@whitespace' },
          { include: '@numbers' },
          { include: '@strings' },
          { include: '@complexIdentifiers' },
          [/[;,.]/, 'delimiter'],
          [/[()]/, '@brackets'],
          [
            /[\w@#$]+/,
            {
              cases: {
                '@keywords': 'keyword',
                '@operators': 'operator',
                '@builtinFunctions': 'predefined',
                '@default': 'identifier',
              },
            },
          ],
          [/[<>=!%&+\-*/|~^]/, 'operator'],
        ],
        comments: [
          [/--+.*/, 'comment'],
          [/\/\*/, { token: 'comment.quote', next: '@comment' }],
        ],
        comment: [
          [/[^*/]+/, 'comment'],
          [/\*\//, { token: 'comment.quote', next: '@pop' }],
          [/./, 'comment'],
        ],
        whitespace: [[/\s+/, 'white']],
        numbers: [
          [/0[xX][0-9a-fA-F]*/, 'number'],
          [/[$][+-]*\d*(\.\d*)?/, 'number'],
          [/((\d+(\.\d*)?)|(\.\d+))([eE][-+]?\d+)?/, 'number'],
        ],
        strings: [
          [/'/, { token: 'string', next: '@string' }],
          [/"/, { token: 'string.double', next: '@stringDouble' }],
        ],
        string: [
          [/[^']+/, 'string'],
          [/''/, 'string'],
          [/'/, { token: 'string', next: '@pop' }],
        ],
        stringDouble: [
          [/[^"]+/, 'string.double'],
          [/""/, 'string.double'],
          [/"/, { token: 'string.double', next: '@pop' }],
        ],
        complexIdentifiers: [[/`/, { token: 'identifier.quote', next: '@quotedIdentifier' }]],
        quotedIdentifier: [
          [/[^`]+/, 'identifier'],
          [/``/, 'identifier'],
          [/`/, { token: 'identifier.quote', next: '@pop' }],
        ],
      },
    })

    // Oracle SQL theme
    monaco.editor.defineTheme('oracle-sql-dark', {
      base: 'vs-dark',
      inherit: true,
      rules: [
        { token: 'keyword', foreground: '569CD6', fontStyle: 'bold' },
        { token: 'identifier', foreground: '9CDCFE' },
        { token: 'string', foreground: 'CE9178' },
        { token: 'number', foreground: 'B5CEA8' },
        { token: 'comment', foreground: '6A9955', fontStyle: 'italic' },
        { token: 'operator', foreground: 'D4D4D4' },
        { token: 'predefined', foreground: 'DCDCAA' },
      ],
      colors: {
        'editor.background': '#1E1E1E',
        'editor.foreground': '#D4D4D4',
        'editorLineNumber.foreground': '#858585',
        'editorCursor.foreground': '#AEAFAD',
      },
    })

    monaco.editor.setTheme('oracle-sql-dark')

    if (onMount) {
      onMount(editor, monaco)
    }
  }

  return (
    <div style={{ height }} className={cn('border border-gray-700 rounded-lg overflow-hidden bg-[#1E1E1E] h-full min-h-[200px]', className)}>
      <Editor
        height="100%"
        defaultLanguage="sql"
        value={value}
        onChange={onChange}
        onMount={handleEditorDidMount}
        options={{
          readOnly,
          minimap: { enabled: false },
          fontSize: 13,
          fontFamily: "'Cantarell', 'Consolas', 'Courier New', monospace",
          lineNumbers: 'on',
          scrollBeyondLastLine: false,
          automaticLayout: true,
          tabSize: 2,
          wordWrap: 'on',
          contextmenu: true,
          selectOnLineNumbers: true,
          roundedSelection: true,
          glyphMargin: false,
          folding: true,
          lineDecorationsWidth: 10,
          lineNumbersMinChars: 3,
          renderLineHighlight: 'all',
          scrollbar: {
            vertical: 'visible',
            horizontal: 'visible',
            verticalScrollbarSize: 10,
            horizontalScrollbarSize: 10,
          },
          suggest: {
            snippetsPreventQuickSuggestions: false,
          },
        }}
        loading={<div className="flex items-center justify-center h-full bg-gray-800 text-gray-400">Loading editor...</div>}
      />
    </div>
  )
}
