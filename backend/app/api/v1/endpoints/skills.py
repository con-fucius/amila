"""
Skill Generator API Endpoints

Provides endpoints for managing auto-generated skills from query patterns.
Allows administrators to review, approve, and manage skills generated from
successful queries.

Features:
- List generated skills with filtering
- Review skill details
- Approve/reject skills
- Trigger skill generation from patterns
- Track skill effectiveness
- Export skills to YAML

Security:
- Admin-only access for management operations
- Read access for analysts
- Audit logging
"""

from typing import Dict, Any, List, Optional
from pydantic import BaseModel, Field
from fastapi import APIRouter, Depends, HTTPException, status, Query
from enum import Enum
import logging

from app.core.auth import get_current_user, require_permissions
from app.services.skill_generator_service import (
    skill_generator_service,
    SkillGeneratorService,
    SkillType,
    generate_skills_from_history,
    get_auto_generated_skills
)

logger = logging.getLogger(__name__)

router = APIRouter()


# Pydantic Models
class SkillTypeEnum(str, Enum):
    """Skill types for API"""
    COLUMN_MAPPING = "column_mapping"
    QUERY_PATTERN = "query_pattern"
    TABLE_JOIN = "table_join"
    AGGREGATION = "aggregation"
    FILTER = "filter"


class SkillListItem(BaseModel):
    """Skill list item response"""
    skill_id: str
    skill_type: str
    name: str
    description: str
    confidence: float
    generated_at: str
    effectiveness_score: float
    usage_count: int
    yaml_preview: Optional[str] = None


class SkillDetailResponse(BaseModel):
    """Detailed skill response"""
    skill_id: str
    skill_type: str
    name: str
    description: str
    yaml_content: str
    source_queries: List[str]
    confidence: float
    generated_at: str
    effectiveness_score: float
    usage_count: int


class SkillListResponse(BaseModel):
    """Skill list response"""
    skills: List[SkillListItem]
    total: int
    filters_applied: Dict[str, Any]


class SkillApprovalRequest(BaseModel):
    """Skill approval request"""
    approved: bool = Field(..., description="Whether to approve or reject the skill")
    reason: Optional[str] = Field(None, description="Reason for rejection if not approved")


class SkillApprovalResponse(BaseModel):
    """Skill approval response"""
    success: bool
    skill_id: str
    approved: bool
    message: str


class GenerateSkillsRequest(BaseModel):
    """Request to generate skills from patterns"""
    min_frequency: int = Field(3, description="Minimum pattern frequency to generate skill")
    min_confidence: float = Field(0.7, ge=0, le=1, description="Minimum confidence threshold")
    skill_type: Optional[SkillTypeEnum] = Field(None, description="Filter by skill type")


class GenerateSkillsResponse(BaseModel):
    """Skill generation response"""
    success: bool
    generated_count: int
    skills: List[Dict[str, Any]]
    message: str


class SkillEffectivenessRequest(BaseModel):
    """Update skill effectiveness"""
    used_successfully: bool = Field(..., description="Whether the skill was used successfully")


class SkillExportRequest(BaseModel):
    """Export skills request"""
    skill_ids: Optional[List[str]] = Field(None, description="Specific skills to export, or all if not provided")
    format: str = Field("yaml", description="Export format (currently only yaml)")


class SkillExportResponse(BaseModel):
    """Skill export response"""
    success: bool
    export_format: str
    content: str
    filename: str
    skill_count: int


class SkillPersistResponse(BaseModel):
    """Persist skill response"""
    success: bool
    skill_id: str
    file_path: str
    merged_terms: int
    message: str


class SkillStatsResponse(BaseModel):
    """Skill statistics response"""
    total_skills: int
    by_type: Dict[str, int]
    avg_confidence: float
    avg_effectiveness: float
    total_usage: int
    recently_generated: int


# API Endpoints

@router.get("/list", response_model=SkillListResponse)
async def list_skills(
    skill_type: Optional[SkillTypeEnum] = Query(None, description="Filter by skill type"),
    min_confidence: float = Query(0.0, ge=0, le=1, description="Minimum confidence threshold"),
    limit: int = Query(50, ge=1, le=200, description="Maximum number of skills to return"),
    offset: int = Query(0, ge=0, description="Offset for pagination"),
    current_user: Dict[str, Any] = Depends(require_permissions(["admin", "analyst", "view_skills"]))
) -> SkillListResponse:
    """
    List auto-generated skills with optional filtering.
    
    Returns a paginated list of skills with filtering options for type
    and confidence level. Available to analysts and admins.
    """
    try:
        # Convert skill_type to service enum if provided
        st = None
        if skill_type:
            st = SkillType(skill_type.value)
        
        # Get skills from service
        skills_data = await get_auto_generated_skills(
            skill_type=st.value if st else None,
            min_confidence=min_confidence
        )
        
        # Apply pagination
        total = len(skills_data)
        paginated = skills_data[offset:offset + limit]
        
        # Transform to response model
        skills = []
        for skill in paginated:
            yaml_content = skill.get("yaml_content", "")
            skills.append(SkillListItem(
                skill_id=skill.get("skill_id", ""),
                skill_type=skill.get("skill_type", ""),
                name=skill.get("name", ""),
                description=skill.get("description", ""),
                confidence=skill.get("confidence", 0.0),
                generated_at=skill.get("generated_at", ""),
                effectiveness_score=skill.get("effectiveness_score", 0.0),
                usage_count=skill.get("usage_count", 0),
                yaml_preview=yaml_content[:200] + "..." if len(yaml_content) > 200 else yaml_content
            ))
        
        return SkillListResponse(
            skills=skills,
            total=total,
            filters_applied={
                "skill_type": skill_type.value if skill_type else None,
                "min_confidence": min_confidence,
                "limit": limit,
                "offset": offset
            }
        )
        
    except Exception as e:
        logger.error(f"Failed to list skills: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to list skills: {str(e)}"
        )


@router.get("/{skill_id}", response_model=SkillDetailResponse)
async def get_skill_detail(
    skill_id: str,
    current_user: Dict[str, Any] = Depends(require_permissions(["admin", "analyst", "view_skills"]))
) -> SkillDetailResponse:
    """
    Get detailed information about a specific skill.
    
    Returns complete skill details including YAML content and source queries.
    Available to analysts and admins.
    """
    try:
        # Get skill from Redis
        from app.core.redis_client import redis_client
        skill_key = f"{SkillGeneratorService.SKILL_PREFIX}{skill_id}"
        skill_data = await redis_client.get(skill_key)
        
        if not skill_data:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Skill {skill_id} not found"
            )
        
        return SkillDetailResponse(
            skill_id=skill_data.get("skill_id", ""),
            skill_type=skill_data.get("skill_type", ""),
            name=skill_data.get("name", ""),
            description=skill_data.get("description", ""),
            yaml_content=skill_data.get("yaml_content", ""),
            source_queries=skill_data.get("source_queries", []),
            confidence=skill_data.get("confidence", 0.0),
            generated_at=skill_data.get("generated_at", ""),
            effectiveness_score=skill_data.get("effectiveness_score", 0.0),
            usage_count=skill_data.get("usage_count", 0)
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get skill detail: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to get skill detail: {str(e)}"
        )


@router.post("/generate", response_model=GenerateSkillsResponse)
async def generate_skills(
    request: GenerateSkillsRequest,
    current_user: Dict[str, Any] = Depends(require_permissions(["admin"]))
) -> GenerateSkillsResponse:
    """
    Generate skills from recorded patterns.
    
    Analyzes recorded query patterns and generates new skills based on
    frequency and confidence thresholds. Admin permission required.
    """
    try:
        generated = await generate_skills_from_history()
        
        # Filter by skill type if specified
        if request.skill_type:
            type_filter = request.skill_type.value
            generated = [s for s in generated if s.skill_type.value == type_filter]
        
        # Filter by confidence
        generated = [s for s in generated if s.confidence >= request.min_confidence]
        
        skills_data = []
        for skill in generated:
            skills_data.append({
                "skill_id": skill.skill_id,
                "skill_type": skill.skill_type.value,
                "name": skill.name,
                "confidence": skill.confidence,
                "generated_at": skill.generated_at
            })
        
        return GenerateSkillsResponse(
            success=True,
            generated_count=len(generated),
            skills=skills_data,
            message=f"Generated {len(generated)} skills from patterns"
        )
        
    except Exception as e:
        logger.error(f"Failed to generate skills: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to generate skills: {str(e)}"
        )


@router.post("/{skill_id}/approve", response_model=SkillApprovalResponse)
async def approve_skill(
    skill_id: str,
    request: SkillApprovalRequest,
    current_user: Dict[str, Any] = Depends(require_permissions(["admin"]))
) -> SkillApprovalResponse:
    """
    Approve or reject a generated skill.
    
    Marks a skill as approved or rejected. Rejected skills won't be
    used in query processing. Admin permission required.
    """
    try:
        from app.core.redis_client import redis_client
        skill_key = f"{SkillGeneratorService.SKILL_PREFIX}{skill_id}"
        skill_data = await redis_client.get(skill_key)
        
        if not skill_data:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Skill {skill_id} not found"
            )
        
        # Update approval status
        skill_data["approved"] = request.approved
        skill_data["approval_reason"] = request.reason
        skill_data["approved_by"] = current_user.get("id")
        from datetime import datetime, timezone
        skill_data["approved_at"] = datetime.now(timezone.utc).isoformat()
        
        await redis_client.set(skill_key, skill_data)
        
        logger.info(f"Skill {skill_id} {'approved' if request.approved else 'rejected'} by {current_user.get('id')}")
        
        return SkillApprovalResponse(
            success=True,
            skill_id=skill_id,
            approved=request.approved,
            message=f"Skill {skill_id} {'approved' if request.approved else 'rejected'}"
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to approve skill: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to approve skill: {str(e)}"
        )


@router.post("/{skill_id}/effectiveness")
async def update_skill_effectiveness(
    skill_id: str,
    request: SkillEffectivenessRequest,
    current_user: Dict[str, Any] = Depends(require_permissions(["admin", "analyst"]))
) -> Dict[str, Any]:
    """
    Update skill effectiveness tracking.
    
    Records whether a skill was used successfully to improve
    future skill recommendations. Available to analysts and admins.
    """
    try:
        await SkillGeneratorService.track_skill_effectiveness(
            skill_id=skill_id,
            used_successfully=request.used_successfully
        )
        
        return {
            "success": True,
            "skill_id": skill_id,
            "used_successfully": request.used_successfully,
            "message": "Effectiveness tracked"
        }
        
    except Exception as e:
        logger.error(f"Failed to track skill effectiveness: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to track effectiveness: {str(e)}"
        )


@router.post("/{skill_id}/persist", response_model=SkillPersistResponse)
async def persist_skill(
    skill_id: str,
    current_user: Dict[str, Any] = Depends(require_permissions(["admin"]))
) -> SkillPersistResponse:
    """
    Persist a generated skill to YAML and reload mappings.
    Currently supports column mapping skills only.
    """
    try:
        from app.core.redis_client import redis_client
        from app.services.skills_loader_service import SkillsLoaderService
        import yaml
        from pathlib import Path

        skill_key = f"{SkillGeneratorService.SKILL_PREFIX}{skill_id}"
        skill_data = await redis_client.get(skill_key)
        if not skill_data:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Skill {skill_id} not found"
            )

        if skill_data.get("skill_type") != SkillType.COLUMN_MAPPING.value:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Only column_mapping skills can be persisted at this time"
            )

        yaml_content = skill_data.get("yaml_content", "")
        if not yaml_content:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Skill YAML content is empty"
            )

        parsed = yaml.safe_load(yaml_content) or {}
        mappings = parsed.get("business_term_mappings", {})
        if not isinstance(mappings, dict) or not mappings:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="No business_term_mappings found in skill YAML"
            )

        skills_dir = SkillsLoaderService.get_skills_directory()
        auto_file = skills_dir / "auto_generated_mappings.yaml"

        if auto_file.exists():
            existing = yaml.safe_load(auto_file.read_text(encoding="utf-8")) or {}
        else:
            existing = {
                "name": "auto_generated_mappings",
                "version": "1.0",
                "enabled": True,
                "business_term_mappings": {}
            }

        existing_mappings = existing.get("business_term_mappings", {})
        if not isinstance(existing_mappings, dict):
            existing_mappings = {}

        # Merge mappings (auto mappings override existing duplicates)
        merged = {**existing_mappings, **mappings}
        existing["business_term_mappings"] = merged

        # Ensure directory exists
        auto_file.parent.mkdir(parents=True, exist_ok=True)
        auto_file.write_text(yaml.safe_dump(existing, sort_keys=False), encoding="utf-8")

        # Clear skills cache so new mappings are picked up
        SkillsLoaderService.clear_cache()

        return SkillPersistResponse(
            success=True,
            skill_id=skill_id,
            file_path=str(auto_file),
            merged_terms=len(mappings),
            message="Skill persisted to YAML and mappings reloaded"
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to persist skill: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to persist skill: {str(e)}"
        )


@router.post("/export", response_model=SkillExportResponse)
async def export_skills(
    request: SkillExportRequest,
    current_user: Dict[str, Any] = Depends(require_permissions(["admin"]))
) -> SkillExportResponse:
    """
    Export skills to YAML format.
    
    Exports selected or all skills as a combined YAML document
    suitable for version control or sharing. Admin permission required.
    """
    try:
        from app.core.redis_client import redis_client
        
        # Get skills to export
        if request.skill_ids:
            skills = []
            for skill_id in request.skill_ids:
                skill_key = f"{SkillGeneratorService.SKILL_PREFIX}{skill_id}"
                skill_data = await redis_client.get(skill_key)
                if skill_data:
                    skills.append(skill_data)
        else:
            # Get all skills
            skills = await get_auto_generated_skills()
        
        if not skills:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="No skills found to export"
            )
        
        # Build YAML content
        yaml_parts = ["# Auto-generated Skills Export\n"]
        yaml_parts.append(f"# Exported by: {current_user.get('id')}\n")
        from datetime import datetime, timezone
        yaml_parts.append(f"# Exported at: {datetime.now(timezone.utc).isoformat()}\n\n")
        
        for skill in skills:
            yaml_content = skill.get("yaml_content", "")
            yaml_parts.append(f"# Skill: {skill.get('name', 'unnamed')}\n")
            yaml_parts.append(f"# ID: {skill.get('skill_id', 'unknown')}\n")
            yaml_parts.append(f"# Confidence: {skill.get('confidence', 0)}\n")
            yaml_parts.append("---\n")
            yaml_parts.append(yaml_content)
            yaml_parts.append("\n")
        
        full_yaml = "".join(yaml_parts)
        
        return SkillExportResponse(
            success=True,
            export_format="yaml",
            content=full_yaml,
            filename=f"skills_export_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}.yaml",
            skill_count=len(skills)
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to export skills: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to export skills: {str(e)}"
        )


@router.get("/stats/summary", response_model=SkillStatsResponse)
async def get_skill_statistics(
    current_user: Dict[str, Any] = Depends(require_permissions(["admin", "analyst", "view_skills"]))
) -> SkillStatsResponse:
    """
    Get skill generation statistics.
    
    Returns summary statistics about generated skills including
    counts by type, average confidence, and effectiveness.
    Available to analysts and admins.
    """
    try:
        skills = await get_auto_generated_skills()
        
        if not skills:
            return SkillStatsResponse(
                total_skills=0,
                by_type={},
                avg_confidence=0.0,
                avg_effectiveness=0.0,
                total_usage=0,
                recently_generated=0
            )
        
        # Calculate statistics
        total = len(skills)
        by_type: Dict[str, int] = {}
        total_confidence = 0.0
        total_effectiveness = 0.0
        total_usage = 0
        
        from datetime import datetime, timezone, timedelta
        week_ago = (datetime.now(timezone.utc) - timedelta(days=7)).isoformat()
        recently_generated = 0
        
        for skill in skills:
            # Count by type
            st = skill.get("skill_type", "unknown")
            by_type[st] = by_type.get(st, 0) + 1
            
            # Sum confidence and effectiveness
            total_confidence += skill.get("confidence", 0)
            total_effectiveness += skill.get("effectiveness_score", 0)
            total_usage += skill.get("usage_count", 0)
            
            # Count recent
            if skill.get("generated_at", "") > week_ago:
                recently_generated += 1
        
        return SkillStatsResponse(
            total_skills=total,
            by_type=by_type,
            avg_confidence=round(total_confidence / total, 2),
            avg_effectiveness=round(total_effectiveness / total, 2),
            total_usage=total_usage,
            recently_generated=recently_generated
        )
        
    except Exception as e:
        logger.error(f"Failed to get skill statistics: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to get statistics: {str(e)}"
        )


@router.delete("/{skill_id}")
async def delete_skill(
    skill_id: str,
    current_user: Dict[str, Any] = Depends(require_permissions(["admin"]))
) -> Dict[str, Any]:
    """
    Delete a generated skill.
    
    Permanently removes a skill from the system. Admin permission required.
    """
    try:
        from app.core.redis_client import redis_client
        skill_key = f"{SkillGeneratorService.SKILL_PREFIX}{skill_id}"
        
        # Check if exists
        skill_data = await redis_client.get(skill_key)
        if not skill_data:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Skill {skill_id} not found"
            )
        
        # Delete from Redis
        await redis_client._client.delete(skill_key)
        
        logger.info(f"Skill {skill_id} deleted by {current_user.get('id')}")
        
        return {
            "success": True,
            "skill_id": skill_id,
            "message": f"Skill {skill_id} deleted successfully"
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to delete skill: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to delete skill: {str(e)}"
        )
