
import logging
import base64
from typing import Dict, Any, Optional

from fastapi import APIRouter, HTTPException, Depends

from app.services.report_generation_service import ReportGenerationService
from app.services.visualization_service import VisualizationService
from app.core.rbac import rbac_manager
from .models import ReportRequest, VisualizationRequest

router = APIRouter()
logger = logging.getLogger(__name__)

@router.post("/report")
async def generate_report(
    request: ReportRequest,
    user: dict = Depends(rbac_manager.get_current_user)
) -> Dict[str, Any]:
    """
    Generate an executive report from query results
    """
    if not request.query_results:
        raise HTTPException(status_code=400, detail="Query results are required")
    
    if request.format.lower() not in ["html", "pdf", "docx"]:
        raise HTTPException(status_code=400, detail="Format must be html, pdf, or docx")
    
    try:
        result = await ReportGenerationService.generate_report_with_llm_insights(
            query_results=request.query_results,
            format=request.format,
            title=request.title,
            user_queries=request.user_queries,
        )
        
        if result.get("status") == "error":
            raise HTTPException(status_code=400, detail=result.get("message"))
        
        content = result.get("content")
        if isinstance(content, bytes):
            result["content"] = base64.b64encode(content).decode("utf-8")
            result["encoding"] = "base64"
        
        return result
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Report generation failed: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/visualize")
async def generate_visualization(
    request: VisualizationRequest,
    user: dict = Depends(rbac_manager.get_current_user)
) -> Dict[str, Any]:
    """
    Generate Python-based visualization using Plotly
    """
    if not request.columns or not request.rows:
        raise HTTPException(status_code=400, detail="Columns and rows are required")
    
    if len(request.rows) > 10000:
        raise HTTPException(status_code=400, detail="Too many rows for visualization (max 10000)")
    
    try:
        hints = {
            "show_mean": request.show_mean,
            "show_peaks": request.show_peaks,
            "color_scheme": request.color_scheme,
        }
        
        result = VisualizationService.generate_chart(
            columns=request.columns,
            rows=request.rows,
            chart_type=request.chart_type,
            title=request.title,
            hints=hints
        )
        
        return result
        
    except Exception as e:
        logger.error(f"Visualization generation failed: {e}", exc_info=True)
        return {
            "status": "error",
            "message": str(e),
            "fallback": "recharts"
        }
