import { useMemo } from 'react'
import ReactFlow, { Background, Controls, MiniMap, type Edge, type Node } from 'reactflow'
import 'reactflow/dist/style.css'

interface Relationship {
  constraint_name: string
  child_table: string
  child_column: string
  parent_table: string
  parent_column: string
}

interface SchemaERDiagramProps {
  baseTable: string
  relationships: Relationship[]
  onSelectTable?: (tableName: string) => void
}

function buildNodesAndEdges(baseTable: string, relationships: Relationship[]) {
  const centerX = 0
  const centerY = 0
  const radius = 260
  const nodes: Node[] = []
  const edges: Edge[] = []

  const tableSet = new Set<string>()
  tableSet.add(baseTable)

  relationships.forEach((rel) => {
    tableSet.add(rel.parent_table)
    tableSet.add(rel.child_table)
  })

  const tables = Array.from(tableSet)
  const relatedTables = tables.filter((t) => t !== baseTable)

  nodes.push({
    id: baseTable,
    position: { x: centerX, y: centerY },
    data: { label: baseTable, type: 'base' },
    style: {
      background: '#0f172a',
      color: '#e2e8f0',
      border: '1px solid #34d399',
      borderRadius: 10,
      padding: 8,
      fontSize: 12,
      minWidth: 140,
      textAlign: 'center',
    },
  })

  relatedTables.forEach((table, idx) => {
    const angle = (idx / Math.max(1, relatedTables.length)) * Math.PI * 2
    const x = centerX + radius * Math.cos(angle)
    const y = centerY + radius * Math.sin(angle)

    nodes.push({
      id: table,
      position: { x, y },
      data: { label: table, type: table === baseTable ? 'base' : 'related' },
      style: {
        background: '#0b1220',
        color: '#cbd5f5',
        border: '1px solid #334155',
        borderRadius: 10,
        padding: 8,
        fontSize: 11,
        minWidth: 130,
        textAlign: 'center',
      },
    })
  })

  relationships.forEach((rel, idx) => {
    const source = rel.parent_table
    const target = rel.child_table
    edges.push({
      id: `edge-${idx}-${source}-${target}`,
      source,
      target,
      label: `${rel.parent_column} → ${rel.child_column}`,
      animated: false,
      style: { stroke: '#94a3b8' },
      labelStyle: { fill: '#94a3b8', fontSize: 10 },
      labelBgStyle: { fill: '#0f172a', fillOpacity: 0.8 },
    })
  })

  return { nodes, edges }
}

export function SchemaERDiagram({ baseTable, relationships, onSelectTable }: SchemaERDiagramProps) {
  const { nodes, edges } = useMemo(
    () => buildNodesAndEdges(baseTable, relationships),
    [baseTable, relationships]
  )

  return (
    <div className="h-[420px] w-full rounded-lg border border-slate-800 bg-gradient-to-br from-slate-950 via-slate-900 to-slate-950">
      <ReactFlow
        nodes={nodes}
        edges={edges}
        fitView
        fitViewOptions={{ padding: 0.2 }}
        onNodeClick={(_, node) => {
          if (onSelectTable) onSelectTable(String(node.id))
        }}
        proOptions={{ hideAttribution: true }}
      >
        <MiniMap nodeColor={(n) => (n.data?.type === 'base' ? '#34d399' : '#64748b')} maskColor="#0b1220" />
        <Controls position="bottom-right" />
        <Background gap={24} size={1} color="#1f2937" />
      </ReactFlow>
    </div>
  )
}
