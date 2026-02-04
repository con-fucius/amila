"""
Semantic Mappings API Endpoints
"""

from fastapi import APIRouter, HTTPException, Depends
from pydantic import BaseModel
from typing import Dict, Any, List
import logging

from app.services.semantic_layer import SemanticLayerService
from app.core.rbac import rbac_manager

router = APIRouter()
logger = logging.getLogger(__name__)


class MappingModel(BaseModel):
    concept: str
    mappings: Dict[str, str]  # {database_type: table_name}


@router.get("/mappings")
async def get_all_mappings(
    user: dict = Depends(rbac_manager.get_current_user),
) -> Dict[str, Any]:
    """
    Get all semantic mappings.
    
    Returns:
        Dict of all concept-to-table mappings
    """
    try:
        mappings = await SemanticLayerService.get_all_mappings()
        
        return {
            "status": "success",
            "mappings": mappings,
            "count": len(mappings)
        }
        
    except Exception as e:
        logger.error(f"Failed to fetch mappings: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to fetch mappings: {str(e)}"
        )


@router.post("/mappings")
async def add_mapping(
    mapping: MappingModel,
    user: dict = Depends(rbac_manager.get_current_user),
) -> Dict[str, Any]:
    """
    Add or update a semantic mapping.
    
    **RBAC:** Requires admin permission
    
    Args:
        mapping: Mapping to add (concept + database-specific table names)
        
    Returns:
        Status dict
    """
    try:
        # Check admin permission
        if user.get("role") != "admin":
            raise HTTPException(status_code=403, detail="Admin permission required")
        
        result = await SemanticLayerService.add_mapping(
            concept=mapping.concept,
            db_mappings=mapping.mappings
        )
        
        return result
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to add mapping: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to add mapping: {str(e)}"
        )


@router.delete("/mappings/{concept}")
async def delete_mapping(
    concept: str,
    user: dict = Depends(rbac_manager.get_current_user),
) -> Dict[str, Any]:
    """
    Delete a semantic mapping.
    
    **RBAC:** Requires admin permission
    
    Args:
        concept: Concept to delete
        
    Returns:
        Status dict
    """
    try:
        # Check admin permission
        if user.get("role") != "admin":
            raise HTTPException(status_code=403, detail="Admin permission required")
        
        result = await SemanticLayerService.delete_mapping(concept)
        
        return result
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to delete mapping: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to delete mapping: {str(e)}"
        )


@router.get("/resolve/{concept}")
async def resolve_concept(
    concept: str,
    database_type: str = "oracle",
    user: dict = Depends(rbac_manager.get_current_user),
) -> Dict[str, Any]:
    """
    Resolve a business concept to a physical table name.
    
    Args:
        concept: Business concept (e.g., "customers")
        database_type: Target database type
        
    Returns:
        Physical table name
    """
    try:
        table_name = await SemanticLayerService.resolve_concept_to_table(
            concept=concept,
            db_type=database_type
        )
        
        if table_name:
            return {
                "status": "success",
                "concept": concept,
                "database_type": database_type,
                "table_name": table_name
            }
        else:
            return {
                "status": "not_found",
                "concept": concept,
                "database_type": database_type,
                "message": f"No mapping found for '{concept}' in {database_type}"
            }
        
    except Exception as e:
        logger.error(f"Failed to resolve concept: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to resolve concept: {str(e)}"
        )
