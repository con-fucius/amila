import React, { useState, useEffect } from 'react';
import { Search, Book, Tag, User, BarChart2, Info } from 'lucide-react';
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import { Input } from '@/components/ui/input';
import { Badge } from '@/components/ui/badge';
import { apiService } from '@/services/apiService';

interface Metric {
    metric_id: string;
    name: string;
    description: string;
    owner: string;
    tags: string[];
    business_definition: string;
    calculation_logic: string;
    usage_count: number;
}

export const MetricsGlossary: React.FC = () => {
    const [metrics, setMetrics] = useState<Metric[]>([]);
    const [search, setSearch] = useState('');
    const [loading, setLoading] = useState(true);

    useEffect(() => {
        const fetchMetrics = async () => {
            try {
                const response = await apiService.getMetricsGlossary();
                if (response.status === 'success') {
                    setMetrics(response.glossary);
                }
            } catch (error) {
                console.error('Failed to fetch glossary:', error);
            } finally {
                setLoading(false);
            }
        };
        fetchMetrics();
    }, []);

    const filteredMetrics = metrics.filter(m =>
        m.name.toLowerCase().includes(search.toLowerCase()) ||
        m.description.toLowerCase().includes(search.toLowerCase()) ||
        m.tags.some(t => t.toLowerCase().includes(search.toLowerCase()))
    );

    return (
        <div className="space-y-6 animate-in fade-in slide-in-from-bottom-4 duration-500">
            <div className="flex flex-col gap-2">
                <h2 className="text-2xl font-bold tracking-tight text-slate-100 flex items-center gap-2">
                    <Book className="w-6 h-6 text-blue-500" />
                    Business Metrics Glossary
                </h2>
                <p className="text-slate-400">
                    Canonical definitions for all approved business metrics across the organization.
                </p>
            </div>

            <div className="relative">
                <Search className="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-slate-500" />
                <Input
                    placeholder="Search metrics by name, description or tags..."
                    className="pl-10 bg-slate-900 border-slate-800 text-slate-200 focus:ring-blue-500"
                    value={search}
                    onChange={(e) => setSearch(e.target.value)}
                />
            </div>

            {loading ? (
                <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                    {[1, 2, 4].map(i => (
                        <div key={i} className="h-48 bg-slate-900/50 rounded-lg animate-pulse" />
                    ))}
                </div>
            ) : (
                <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                    {filteredMetrics.map((metric) => (
                        <Card key={metric.metric_id} className="bg-slate-900/40 border-slate-800 hover:border-blue-500/50 transition-colors group">
                            <CardHeader className="pb-2">
                                <div className="flex justify-between items-start">
                                    <div className="space-y-1">
                                        <CardTitle className="text-lg text-slate-100 group-hover:text-blue-400 transition-colors">
                                            {metric.name}
                                        </CardTitle>
                                        <div className="flex flex-wrap gap-1">
                                            {metric.tags.map(tag => (
                                                <Badge key={tag} variant="secondary" className="text-[10px] bg-slate-800 text-slate-400 hover:text-slate-200">
                                                    {tag}
                                                </Badge>
                                            ))}
                                        </div>
                                    </div>
                                    <Badge className="bg-blue-500/10 text-blue-400 border-none">
                                        <BarChart2 className="w-3 h-3 mr-1" />
                                        {metric.usage_count}
                                    </Badge>
                                </div>
                            </CardHeader>
                            <CardContent className="space-y-4">
                                <p className="text-sm text-slate-400 line-clamp-2 italic">
                                    "{metric.description}"
                                </p>

                                <div className="space-y-2 pt-2 border-t border-slate-800/50">
                                    <div className="flex items-start gap-2">
                                        <Info className="w-3.5 h-3.5 text-slate-500 mt-0.5" />
                                        <div className="text-xs">
                                            <span className="text-slate-300 font-medium">Business Rule:</span>
                                            <p className="text-slate-500 mt-0.5">{metric.business_definition}</p>
                                        </div>
                                    </div>
                                    <div className="flex items-start gap-2">
                                        <Tag className="w-3.5 h-3.5 text-slate-500 mt-0.5" />
                                        <div className="text-xs">
                                            <span className="text-slate-300 font-medium">Calculation:</span>
                                            <code className="text-blue-300/80 mt-0.5 block bg-slate-950 p-1 rounded">
                                                {metric.calculation_logic}
                                            </code>
                                        </div>
                                    </div>
                                </div>

                                <div className="flex justify-between items-center pt-2 text-[10px] text-slate-600">
                                    <div className="flex items-center gap-1">
                                        <User className="w-3 h-3" />
                                        <span>Owner: {metric.owner}</span>
                                    </div>
                                    <span>ID: {metric.metric_id}</span>
                                </div>
                            </CardContent>
                        </Card>
                    ))}
                </div>
            )}

            {filteredMetrics.length === 0 && !loading && (
                <div className="text-center py-12 bg-slate-900/20 rounded-xl border border-dashed border-slate-800">
                    <Book className="w-12 h-12 text-slate-700 mx-auto mb-4" />
                    <h3 className="text-lg font-medium text-slate-400">No metrics found</h3>
                    <p className="text-slate-500">Try adjusting your search terms</p>
                </div>
            )}
        </div>
    );
};
