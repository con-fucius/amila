"""
Skills Loader Service
Loads and manages YAML-based skills for SQL generation

Based on Anthropic's Skills concept - provides structured guidance to the LLM
"""

import logging
import os
from typing import Dict, Any, List, Optional
from pathlib import Path

logger = logging.getLogger(__name__)

# Feature flag
SKILLS_ENABLED = True

try:
    import yaml
    YAML_AVAILABLE = True
except ImportError:
    YAML_AVAILABLE = False
    logger.warning("PyYAML not installed. Skills loading will be limited.")


class SkillsLoaderService:
    """
    Service for loading and managing YAML-based skills
    
    Skills provide:
    - Database-specific SQL patterns
    - Business term to column mappings
    - Query templates
    - Anti-patterns to avoid
    """
    
    _cache: Dict[str, Dict[str, Any]] = {}
    _skills_dir: Optional[Path] = None
    
    @classmethod
    def get_skills_directory(cls) -> Path:
        """Get the skills directory path"""
        if cls._skills_dir is None:
            # Default to app/skills relative to this file
            cls._skills_dir = Path(__file__).parent.parent / "skills"
        return cls._skills_dir
    
    @classmethod
    def is_available(cls) -> bool:
        """Check if skills loading is available"""
        return SKILLS_ENABLED and YAML_AVAILABLE
    
    @classmethod
    def load_skill(cls, skill_name: str) -> Optional[Dict[str, Any]]:
        """
        Load a skill from YAML file
        
        Args:
            skill_name: Name of the skill file (without .yaml extension)
            
        Returns:
            Skill configuration dict or None if not found
        """
        if not cls.is_available():
            return None
        
        # Check cache first
        if skill_name in cls._cache:
            return cls._cache[skill_name]
        
        skill_path = cls.get_skills_directory() / f"{skill_name}.yaml"
        
        if not skill_path.exists():
            logger.warning(f"Skill file not found: {skill_path}")
            return None
        
        try:
            with open(skill_path, 'r', encoding='utf-8') as f:
                skill_data = yaml.safe_load(f)
            
            # Cache the loaded skill
            cls._cache[skill_name] = skill_data
            logger.info(f"Loaded skill: {skill_name}")
            return skill_data
            
        except Exception as e:
            logger.error(f"Failed to load skill {skill_name}: {e}")
            return None
    
    @classmethod
    def load_database_skills(cls, database_type: str) -> Optional[Dict[str, Any]]:
        """
        Load database-specific skills
        
        Args:
            database_type: "oracle", "doris", or "postgres"/"postgresql"
            
        Returns:
            Database-specific skill configuration
        """
        # Normalize postgresql to postgres
        db_type = database_type.lower()
        if db_type == "postgresql":
            db_type = "postgres"
        
        skill_name = f"{db_type}_query_skills"
        return cls.load_skill(skill_name)
    
    @classmethod
    def load_schema_mapping_skills(cls) -> Optional[Dict[str, Any]]:
        """Load schema mapping skills"""
        base = cls.load_skill("schema_mapping_skills") or {}
        auto = cls.load_skill("auto_generated_mappings") or {}

        if not base and not auto:
            return None

        merged = dict(base) if isinstance(base, dict) else {}

        # Merge auto-generated business term mappings
        base_mappings = merged.get("business_term_mappings", {}) if isinstance(merged, dict) else {}
        auto_mappings = auto.get("business_term_mappings", {}) if isinstance(auto, dict) else {}

        if isinstance(base_mappings, dict) and isinstance(auto_mappings, dict):
            merged["business_term_mappings"] = {**base_mappings, **auto_mappings}

        # Preserve top-level metadata if missing
        if "name" not in merged:
            merged["name"] = "schema_mapping_skills"
        if "version" not in merged:
            merged["version"] = "1.0"
        if "enabled" not in merged:
            merged["enabled"] = True

        return merged
    
    @classmethod
    def get_dialect_hints(cls, database_type: str) -> str:
        """
        Get SQL dialect hints for prompt enhancement
        
        Args:
            database_type: "oracle", "doris", or "postgres"/"postgresql"
            
        Returns:
            Formatted string with dialect rules for LLM prompt
        """
        # Normalize postgresql to postgres
        db_type = database_type.lower()
        if db_type == "postgresql":
            db_type = "postgres"
        
        skills = cls.load_database_skills(db_type)
        if not skills:
            return ""
        
        hints = []
        
        # Add dialect rules
        dialect_rules = skills.get("dialect_rules", {})
        if dialect_rules:
            hints.append(f"=== {db_type.upper()} SQL DIALECT RULES ===")
            for rule_name, rule_data in dialect_rules.items():
                if isinstance(rule_data, dict):
                    pattern = rule_data.get("pattern") or rule_data.get("function", "")
                    example = rule_data.get("example", "")
                    note = rule_data.get("note", "")
                    hints.append(f"- {rule_name}: {pattern}")
                    if example:
                        hints.append(f"  Example: {example}")
                    if note:
                        hints.append(f"  Note: {note}")
        
        # Add anti-patterns
        anti_patterns = skills.get("anti_patterns", [])
        if anti_patterns:
            hints.append(f"\n=== AVOID THESE PATTERNS ===")
            for ap in anti_patterns:
                if isinstance(ap, dict):
                    hints.append(f"- DON'T: {ap.get('pattern', '')}")
                    hints.append(f"  Reason: {ap.get('reason', '')}")
                    hints.append(f"  FIX: {ap.get('fix', '')}")
        
        # Add performance hints
        perf_hints = skills.get("performance_hints", [])
        if perf_hints:
            hints.append(f"\n=== PERFORMANCE TIPS ===")
            for ph in perf_hints[:5]:  # Top 5 hints
                if isinstance(ph, dict):
                    hints.append(f"- {ph.get('rule', '')}")
        
        return "\n".join(hints)
    
    @classmethod
    def get_business_term_mapping(cls, term: str) -> Optional[Dict[str, Any]]:
        """
        Get mapping for a business term
        
        Args:
            term: Business term to look up
            
        Returns:
            Mapping configuration or None
        """
        skills = cls.load_schema_mapping_skills()
        if not skills:
            return None
        
        mappings = skills.get("business_term_mappings", {})
        term_lower = term.lower()
        
        # Direct match
        if term_lower in mappings:
            return mappings[term_lower]
        
        # Check synonyms
        for key, mapping in mappings.items():
            synonyms = mapping.get("synonyms", [])
            if term_lower in [s.lower() for s in synonyms]:
                return mapping
        
        return None
    
    @classmethod
    def clear_cache(cls):
        """Clear the skills cache"""
        cls._cache.clear()
        logger.info("Skills cache cleared")
