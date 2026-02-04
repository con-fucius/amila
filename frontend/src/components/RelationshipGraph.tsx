import React, { useMemo } from 'react';
import { ResponsiveContainer, ScatterChart, Scatter, XAxis, YAxis, ZAxis, Tooltip, Cell } from 'recharts';

interface Relationship {
    constraint_name: string;
    child_table: string;
    child_column: string;
    parent_table: string;
    parent_column: string;
}

interface RelationshipGraphProps {
    baseTable: string;
    relationships: Relationship[];
}

export const RelationshipGraph: React.FC<RelationshipGraphProps> = ({ baseTable, relationships }) => {
    const data = useMemo(() => {
        const nodes: any[] = [{ name: baseTable, x: 50, y: 50, size: 400, type: 'main' }];


        relationships.forEach((rel, index) => {
            const isParent = rel.parent_table === baseTable.toUpperCase();
            const otherTable = isParent ? rel.child_table : rel.parent_table;

            // Distribute other tables in a circle
            const angle = (index / relationships.length) * 2 * Math.PI;
            const x = 50 + 35 * Math.cos(angle);
            const y = 50 + 35 * Math.sin(angle);

            nodes.push({
                name: otherTable,
                x,
                y,
                size: 200,
                type: isParent ? 'child' : 'parent',
                relName: rel.constraint_name,
                cols: `${rel.child_column} -> ${rel.parent_column}`
            });
        });

        return nodes;
    }, [baseTable, relationships]);

    const CustomTooltip = ({ active, payload }: any) => {
        if (active && payload && payload.length) {
            const node = payload[0].payload;
            return (
                <div className="bg-slate-900 border border-slate-700 p-2 rounded shadow-lg text-xs">
                    <p className="font-bold text-blue-400">{node.name}</p>
                    {node.type !== 'main' && (
                        <>
                            <p className="text-slate-300">{node.type === 'child' ? 'Child of' : 'Parent of'} {baseTable}</p>
                            <p className="text-slate-400 mt-1 italic">{node.cols}</p>
                        </>
                    )}
                </div>
            );
        }
        return null;
    };

    return (
        <div className="h-[400px] w-full bg-slate-950/50 rounded-lg border border-slate-800 p-4 relative overflow-hidden">
            <div className="absolute top-2 left-4 z-10">
                <h4 className="text-sm font-medium text-slate-300">Relationship Topology</h4>
                <p className="text-[10px] text-slate-500">Visualizing foreign key dependencies</p>
            </div>

            <ResponsiveContainer width="100%" height="100%">
                <ScatterChart margin={{ top: 20, right: 20, bottom: 20, left: 20 }}>
                    <XAxis type="number" dataKey="x" hide domain={[0, 100]} />
                    <YAxis type="number" dataKey="y" hide domain={[0, 100]} />
                    <ZAxis type="number" dataKey="size" range={[100, 500]} />
                    <Tooltip content={<CustomTooltip />} cursor={{ strokeDasharray: '3 3' }} />
                    <Scatter data={data} fill="#8884d8">
                        {data.map((entry, index) => (
                            <Cell
                                key={`cell-${index}`}
                                fill={entry.type === 'main' ? '#3b82f6' : entry.type === 'parent' ? '#10b981' : '#f59e0b'}
                                className="cursor-pointer hover:opacity-80 transition-opacity"
                            />
                        ))}
                    </Scatter>

                    {/* Edge visualization placeholder - ideally we'd use a custom shape for lines, 
              but for this pass we'll use the topology scatter */}
                </ScatterChart>
            </ResponsiveContainer>

            <div className="absolute bottom-2 right-4 flex gap-4 text-[10px]">
                <div className="flex items-center gap-1">
                    <div className="w-2 h-2 rounded-full bg-blue-500" />
                    <span className="text-slate-400">Target</span>
                </div>
                <div className="flex items-center gap-1">
                    <div className="w-2 h-2 rounded-full bg-emerald-500" />
                    <span className="text-slate-400">Parent</span>
                </div>
                <div className="flex items-center gap-1">
                    <div className="w-2 h-2 rounded-full bg-amber-500" />
                    <span className="text-slate-400">Child</span>
                </div>
            </div>
        </div>
    );
};
