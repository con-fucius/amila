"""
Report Generation Service
Generates professional executive reports from query results
"""

import logging
from typing import Dict, Any, List, Optional
from datetime import datetime, timezone
from io import BytesIO
import json

logger = logging.getLogger(__name__)

# Feature flag
REPORT_GENERATION_ENABLED = True


class ReportGenerationService:
    """
    Service for generating executive reports from query results
    
    Supports formats: PDF, DOCX, HTML
    Template structure:
    - Heading
    - Executive Summary
    - Metrics and Inference
    - Insights/Recommendations
    """
    
    TEMPLATE_SECTIONS = [
        "heading",
        "executive_summary",
        "metrics_inference",
        "insights_recommendations",
    ]
    
    @classmethod
    async def generate_report(
        cls,
        query_results: List[Dict[str, Any]],
        format: str = "html",
        title: Optional[str] = None,
        user_queries: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        """
        Generate an executive report from query results
        
        Args:
            query_results: List of query result dicts with columns, rows, sql_query
            format: Output format (html, pdf, docx)
            title: Report title
            user_queries: Original user queries for context
            
        Returns:
            Dict with report content and metadata
        """
        if not REPORT_GENERATION_ENABLED:
            return {"status": "error", "message": "Report generation is disabled"}
        
        if not query_results:
            return {"status": "error", "message": "No query results provided"}
        
        try:
            # Generate report content
            report_data = cls._build_report_data(query_results, title, user_queries)
            
            # Generate in requested format
            if format.lower() == "html":
                content = cls._generate_html(report_data)
                content_type = "text/html"
            elif format.lower() == "pdf":
                content = await cls._generate_pdf(report_data)
                content_type = "application/pdf"
            elif format.lower() == "docx":
                content = await cls._generate_docx(report_data)
                content_type = "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
            else:
                return {"status": "error", "message": f"Unsupported format: {format}"}
            
            return {
                "status": "success",
                "format": format,
                "content_type": content_type,
                "content": content,
                "title": report_data["title"],
                "generated_at": datetime.now(timezone.utc).isoformat(),
            }
            
        except Exception as e:
            logger.error(f"Report generation failed: {e}", exc_info=True)
            return {"status": "error", "message": str(e)}
    
    @classmethod
    def _build_report_data(
        cls,
        query_results: List[Dict[str, Any]],
        title: Optional[str],
        user_queries: Optional[List[str]],
    ) -> Dict[str, Any]:
        """Build structured report data from query results"""
        
        # Generate title if not provided
        if not title:
            title = f"Data Analysis Report - {datetime.now().strftime('%B %d, %Y')}"
        
        # Analyze results for insights
        total_rows = sum(r.get("row_count", len(r.get("rows", []))) for r in query_results)
        total_queries = len(query_results)
        
        # Extract key metrics from results
        metrics = []
        for i, result in enumerate(query_results):
            columns = result.get("columns", [])
            rows = result.get("rows", [])
            
            if not rows:
                continue
            
            def get_val(r, idx, name):
                if isinstance(r, dict):
                    return r.get(name)
                try:
                    return r[idx]
                except (IndexError, TypeError):
                    return None

            # Find numeric columns for metrics
            for col_idx, col in enumerate(columns):
                values = [get_val(row, col_idx, col) for row in rows if get_val(row, col_idx, col) is not None]
                numeric_values = []
                for v in values:
                    try:
                        numeric_values.append(float(v))
                    except (ValueError, TypeError):
                        pass
                
                if numeric_values:
                    metrics.append({
                        "name": col,
                        "query_index": i,
                        "count": len(numeric_values),
                        "sum": sum(numeric_values),
                        "avg": sum(numeric_values) / len(numeric_values),
                        "min": min(numeric_values),
                        "max": max(numeric_values),
                    })
        
        # Build executive summary
        summary_points = [
            f"This report analyzes {total_queries} data queries with {total_rows:,} total records.",
        ]
        
        if metrics:
            top_metric = max(metrics, key=lambda m: m["sum"])
            summary_points.append(
                f"Key metric: {top_metric['name']} with total of {top_metric['sum']:,.2f} "
                f"(avg: {top_metric['avg']:,.2f})"
            )
        
        # Generate insights (rule-based for sync context)
        # LLM insights are generated separately in async context
        insights = cls._generate_insights(query_results, metrics)
        
        return {
            "title": title,
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "executive_summary": summary_points,
            "query_count": total_queries,
            "total_rows": total_rows,
            "metrics": metrics[:10],  # Top 10 metrics
            "insights": insights,
            "query_results": query_results,
            "user_queries": user_queries or [],
        }
    
    @classmethod
    async def generate_report_with_llm_insights(
        cls,
        query_results: List[Dict[str, Any]],
        format: str = "html",
        title: Optional[str] = None,
        user_queries: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        """
        Generate report with LLM-enhanced insights.
        
        This is the preferred method when async context is available.
        Uses anti-hallucination constraints to ensure insights are grounded in data.
        """
        if not REPORT_GENERATION_ENABLED:
            return {"status": "error", "message": "Report generation is disabled"}
        
        if not query_results:
            return {"status": "error", "message": "No query results provided"}
        
        try:
            # Build base report data
            report_data = cls._build_report_data(query_results, title, user_queries)
            
            # Extract metrics for LLM insights
            metrics = report_data.get("metrics", [])
            
            # Generate LLM-enhanced insights with anti-hallucination
            llm_insights = await cls._generate_llm_insights(
                query_results, 
                metrics, 
                user_queries
            )
            
            # Replace rule-based insights with LLM insights
            report_data["insights"] = llm_insights
            report_data["insights_source"] = "llm_enhanced"
            
            # Generate in requested format
            if format.lower() == "html":
                content = cls._generate_html(report_data)
                content_type = "text/html"
            elif format.lower() == "pdf":
                content = await cls._generate_pdf(report_data)
                content_type = "application/pdf"
            elif format.lower() == "docx":
                content = await cls._generate_docx(report_data)
                content_type = "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
            else:
                return {"status": "error", "message": f"Unsupported format: {format}"}
            
            return {
                "status": "success",
                "format": format,
                "content_type": content_type,
                "content": content,
                "title": report_data["title"],
                "generated_at": datetime.now(timezone.utc).isoformat(),
                "insights_source": "llm_enhanced",
            }
            
        except Exception as e:
            logger.error(f"Report generation with LLM insights failed: {e}", exc_info=True)
            # Fallback to rule-based insights
            return await cls.generate_report(query_results, format, title, user_queries)

    @classmethod
    def _generate_insights(
        cls,
        query_results: List[Dict[str, Any]],
        metrics: List[Dict[str, Any]],
    ) -> List[str]:
        """Generate data-driven insights from results (rule-based)"""
        insights = []
        
        # Analyze trends in metrics
        for metric in metrics[:5]:
            if metric["max"] > metric["avg"] * 2:
                insights.append(
                    f"Notable outlier detected in {metric['name']}: "
                    f"maximum value ({metric['max']:,.2f}) is significantly higher than average ({metric['avg']:,.2f})"
                )
            
            if metric["min"] < metric["avg"] * 0.1 and metric["min"] >= 0:
                insights.append(
                    f"Wide variance in {metric['name']}: "
                    f"minimum ({metric['min']:,.2f}) is much lower than average ({metric['avg']:,.2f})"
                )
        
        # Analyze result patterns
        for i, result in enumerate(query_results):
            rows = result.get("rows", [])
            if len(rows) == 0:
                insights.append(f"Query {i+1} returned no results - consider broadening search criteria")
            elif len(rows) >= 1000:
                insights.append(f"Query {i+1} returned {len(rows):,} rows - consider adding filters for focused analysis")
        
        if not insights:
            insights.append("Data appears consistent with no significant anomalies detected")
        
        return insights
    
    @classmethod
    async def _generate_llm_insights(
        cls,
        query_results: List[Dict[str, Any]],
        metrics: List[Dict[str, Any]],
        user_queries: Optional[List[str]] = None,
    ) -> List[str]:
        """
        Generate LLM-enhanced insights with anti-hallucination constraints
        
        Uses constrained prompts to ensure insights are grounded in actual data.
        """
        try:
            from app.orchestrator.llm_config import get_llm
            
            llm = get_llm()
            if not llm:
                logger.warning("LLM not available for insights, using rule-based")
                return cls._generate_insights(query_results, metrics)
            
            # Build data summary for LLM
            data_summary = []
            for i, result in enumerate(query_results[:3]):
                columns = result.get("columns", [])
                rows = result.get("rows", [])[:5]  # First 5 rows only
                row_count = result.get("row_count", len(result.get("rows", [])))
                
                query_text = user_queries[i] if user_queries and i < len(user_queries) else f"Query {i+1}"
                
                data_summary.append(f"Query: {query_text}")
                data_summary.append(f"Columns: {', '.join(columns[:8])}")
                data_summary.append(f"Total rows: {row_count}")
                if rows:
                    data_summary.append(f"Sample data: {json.dumps(rows[:3], default=str)[:500]}")
                data_summary.append("")
            
            metrics_summary = []
            for m in metrics[:5]:
                metrics_summary.append(
                    f"- {m['name']}: sum={m['sum']:,.2f}, avg={m['avg']:,.2f}, min={m['min']:,.2f}, max={m['max']:,.2f}"
                )
            
            # Constrained prompt to prevent hallucination
            prompt = f"""Analyze the following data and generate 3-5 business insights.

CRITICAL RULES:
1. ONLY reference values that appear in the data below
2. DO NOT speculate or make assumptions beyond the data
3. Each insight MUST cite specific values from the data
4. If data is insufficient, say so rather than guessing

DATA:
{chr(10).join(data_summary)}

METRICS:
{chr(10).join(metrics_summary)}

Generate insights in this format:
- [Insight with specific data citation]

Example good insight: "Revenue of $456,789 from Widget A represents 37% of total, making it the top performer."
Example bad insight: "Sales are likely to increase next quarter." (speculation - DO NOT do this)

Your insights:"""
            
            response = await llm.ainvoke(prompt)
            response_text = response.content if hasattr(response, 'content') else str(response)
            
            # Parse insights from response
            llm_insights = []
            for line in response_text.strip().split('\n'):
                line = line.strip()
                if line.startswith('-') or line.startswith('*'):
                    insight = line.lstrip('-*').strip()
                    if insight and len(insight) > 10:
                        llm_insights.append(insight)
            
            # Combine with rule-based insights
            rule_insights = cls._generate_insights(query_results, metrics)
            
            # Deduplicate and limit
            all_insights = llm_insights[:3] + rule_insights[:2]
            seen = set()
            unique_insights = []
            for insight in all_insights:
                key = insight[:50].lower()
                if key not in seen:
                    seen.add(key)
                    unique_insights.append(insight)
            
            return unique_insights[:5]
            
        except Exception as e:
            logger.warning(f"LLM insights generation failed: {e}")
            return cls._generate_insights(query_results, metrics)
    
    @classmethod
    def _generate_html(cls, report_data: Dict[str, Any]) -> str:
        """Generate HTML report"""
        
        html = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>{report_data['title']}</title>
    <style>
        * {{ margin: 0; padding: 0; box-sizing: border-box; }}
        body {{ font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; line-height: 1.6; color: #333; max-width: 900px; margin: 0 auto; padding: 40px 20px; }}
        .header {{ text-align: center; margin-bottom: 40px; padding-bottom: 20px; border-bottom: 3px solid #10b981; }}
        .header h1 {{ color: #111827; font-size: 28px; margin-bottom: 10px; }}
        .header .date {{ color: #6b7280; font-size: 14px; }}
        .section {{ margin-bottom: 30px; }}
        .section h2 {{ color: #10b981; font-size: 20px; margin-bottom: 15px; padding-bottom: 8px; border-bottom: 1px solid #e5e7eb; }}
        .summary-list {{ list-style: none; }}
        .summary-list li {{ padding: 8px 0; padding-left: 20px; position: relative; }}
        .summary-list li::before {{ content: "-"; color: #10b981; position: absolute; left: 0; font-weight: bold; }}
        .metrics-grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 15px; }}
        .metric-card {{ background: #f9fafb; border: 1px solid #e5e7eb; border-radius: 8px; padding: 15px; }}
        .metric-card .name {{ font-weight: 600; color: #374151; margin-bottom: 8px; }}
        .metric-card .value {{ font-size: 24px; color: #10b981; font-weight: bold; }}
        .metric-card .details {{ font-size: 12px; color: #6b7280; margin-top: 5px; }}
        .insights-list {{ background: #f0fdf4; border-left: 4px solid #10b981; padding: 15px 20px; }}
        .insights-list li {{ margin-bottom: 10px; }}
        table {{ width: 100%; border-collapse: collapse; margin-top: 15px; font-size: 13px; }}
        th, td {{ padding: 10px; text-align: left; border-bottom: 1px solid #e5e7eb; }}
        th {{ background: #f9fafb; font-weight: 600; color: #374151; }}
        tr:hover {{ background: #f9fafb; }}
        .footer {{ margin-top: 40px; padding-top: 20px; border-top: 1px solid #e5e7eb; text-align: center; color: #9ca3af; font-size: 12px; }}
    </style>
</head>
<body>
    <div class="header">
        <h1>{report_data['title']}</h1>
        <div class="date">Generated on {datetime.now().strftime('%B %d, %Y at %I:%M %p')}</div>
    </div>
    
    <div class="section">
        <h2>Executive Summary</h2>
        <ul class="summary-list">
"""
        for point in report_data.get("executive_summary", []):
            html += f"            <li>{point}</li>\n"
        
        html += """        </ul>
    </div>
    
    <div class="section">
        <h2>Key Metrics</h2>
        <div class="metrics-grid">
"""
        for metric in report_data.get("metrics", [])[:6]:
            html += f"""            <div class="metric-card">
                <div class="name">{metric['name']}</div>
                <div class="value">{metric['sum']:,.2f}</div>
                <div class="details">Avg: {metric['avg']:,.2f} | Min: {metric['min']:,.2f} | Max: {metric['max']:,.2f}</div>
            </div>
"""
        
        html += """        </div>
    </div>
    
    <div class="section">
        <h2>Insights & Recommendations</h2>
        <ul class="insights-list">
"""
        for insight in report_data.get("insights", []):
            html += f"            <li>{insight}</li>\n"
        
        html += """        </ul>
    </div>
"""
        
        # Add data tables
        for i, result in enumerate(report_data.get("query_results", [])[:3]):
            columns = result.get("columns", [])
            rows = result.get("rows", [])[:10]  # First 10 rows
            
            if columns and rows:
                query_text = report_data.get("user_queries", [])[i] if i < len(report_data.get("user_queries", [])) else f"Query {i+1}"
                html += f"""
    <div class="section">
        <h2>Data: {query_text[:50]}{'...' if len(query_text) > 50 else ''}</h2>
        <table>
            <thead>
                <tr>
"""
                for col in columns[:8]:  # Max 8 columns
                    html += f"                    <th>{col}</th>\n"
                html += """                </tr>
            </thead>
            <tbody>
"""
                for row in rows:
                    html += "                <tr>\n"
                    for cell in row[:8]:
                        cell_str = str(cell) if cell is not None else "NULL"
                        html += f"                    <td>{cell_str[:50]}</td>\n"
                    html += "                </tr>\n"
                
                html += """            </tbody>
        </table>
    </div>
"""
        
        html += f"""
    <div class="footer">
        <p>Report generated by AMILA Business Intelligence System</p>
        <p>Total queries analyzed: {report_data['query_count']} | Total records: {report_data['total_rows']:,}</p>
    </div>
</body>
</html>"""
        
        return html
    
    @classmethod
    async def _generate_pdf(cls, report_data: Dict[str, Any]) -> bytes:
        """Generate PDF report using weasyprint or reportlab"""
        try:
            # Try weasyprint first (better HTML rendering)
            from weasyprint import HTML
            html_content = cls._generate_html(report_data)
            pdf_bytes = HTML(string=html_content).write_pdf()
            return pdf_bytes
        except ImportError:
            logger.warning("weasyprint not available, falling back to HTML")
            # Return HTML as fallback
            return cls._generate_html(report_data).encode('utf-8')
    
    @classmethod
    async def _generate_docx(cls, report_data: Dict[str, Any]) -> bytes:
        """Generate DOCX report using python-docx"""
        try:
            from docx import Document
            from docx.shared import Inches, Pt
            from docx.enum.text import WD_ALIGN_PARAGRAPH
            
            doc = Document()
            
            # Title
            title = doc.add_heading(report_data['title'], 0)
            title.alignment = WD_ALIGN_PARAGRAPH.CENTER
            
            doc.add_paragraph(f"Generated on {datetime.now().strftime('%B %d, %Y at %I:%M %p')}")
            doc.add_paragraph()
            
            # Executive Summary
            doc.add_heading('Executive Summary', level=1)
            for point in report_data.get("executive_summary", []):
                doc.add_paragraph(point, style='List Bullet')
            
            # Key Metrics
            doc.add_heading('Key Metrics', level=1)
            for metric in report_data.get("metrics", [])[:6]:
                p = doc.add_paragraph()
                p.add_run(f"{metric['name']}: ").bold = True
                p.add_run(f"{metric['sum']:,.2f} (Avg: {metric['avg']:,.2f})")
            
            # Insights
            doc.add_heading('Insights & Recommendations', level=1)
            for insight in report_data.get("insights", []):
                doc.add_paragraph(insight, style='List Bullet')
            
            # Save to bytes
            buffer = BytesIO()
            doc.save(buffer)
            buffer.seek(0)
            return buffer.read()
            
        except ImportError:
            logger.warning("python-docx not available")
            raise ValueError("DOCX generation requires python-docx package")
