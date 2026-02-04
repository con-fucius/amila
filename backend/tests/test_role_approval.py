
import pytest
from app.services.approval_service import ApprovalService
from app.core.config import settings

@pytest.mark.asyncio
async def test_role_based_approval_bypass():
    """Test that roles can bypass approval based on configuration."""
    
    # Mock settings.ROLE_BASED_APPROVAL_BYPASS
    # Since it's a Pydantic model field, we might need a different way to patch if it was a global, 
    # but here we can rely on the default values we just added or patch the dict.
    
    # Default config:
    # "admin": ["critical", "high", "medium", "low", "safe"]
    # "manager": ["medium", "low", "safe"]
    # "finance": ["low", "safe"]
    
    # 1. Test Admin (should bypass everything)
    # A query that normally requires approval (e.g. DROP TABLE or system catalog access if allowed by validator but high risk)
    # Ideally we find a query that sql_validator marks as 'high' or 'critical'.
    
    # Note: sql_validator logic determines risk level. ApprovalService logic overrides requires_approval.
    # We can mock sql_validator or just trust that a "high" risk query exists.
    # Alternatively, we can patch sql_validator.validate_query to return a fixed risk.
    
    from unittest.mock import MagicMock, patch
    from app.core.sql_validator import ValidationResult, RiskLevel, QueryType

    with patch('app.core.sql_validator.sql_validator.validate_query') as mock_validator:
        # Case A: Critical Risk, Admin Role -> Should Bypass
        mock_validator.return_value = ValidationResult(
            is_valid=True,
            risk_level=RiskLevel.CRITICAL,
            query_type=QueryType.DDL,
            requires_approval=True,
            errors=[],
            warnings=["Critical operation"]
        )
        
        assessment = ApprovalService.assess_sql_risk("DROP TABLE important", user_role="admin")
        assert assessment["risk_level"] == "critical"
        assert assessment["requires_approval"] is False # Admin bypasses critical
        
        # Case B: Critical Risk, Manager Role -> Should NOT Bypass
        assessment = ApprovalService.assess_sql_risk("DROP TABLE important", user_role="manager")
        assert assessment["risk_level"] == "critical"
        assert assessment["requires_approval"] is True # Manager cannot bypass critical

        # Case C: Medium Risk, Manager Role -> Should Bypass
        mock_validator.return_value = ValidationResult(
            is_valid=True,
            risk_level=RiskLevel.MEDIUM,
            query_type=QueryType.SELECT,
            requires_approval=True,
            errors=[],
            warnings=["Full table scan"]
        )
        
        assessment = ApprovalService.assess_sql_risk("SELECT * FROM large_table", user_role="manager")
        assert assessment["risk_level"] == "medium"
        assert assessment["requires_approval"] is False # Manager bypasses medium
        
        # Case D: Medium Risk, Viewer Role -> Should NOT Bypass (Viewer has no bypass config usually)
        assessment = ApprovalService.assess_sql_risk("SELECT * FROM large_table", user_role="viewer")
        assert assessment["risk_level"] == "medium"
        assert assessment["requires_approval"] is True # Viewer cannot bypass medium
        
        # Case E: Finance Role (Custom)
        mock_validator.return_value = ValidationResult(
            is_valid=True,
            risk_level=RiskLevel.LOW,
            query_type=QueryType.SELECT,
            requires_approval=True, # Maybe auto-approve set to True by validator for some reason
            errors=[],
            warnings=[]
        )
        
        assessment = ApprovalService.assess_sql_risk("SELECT * FROM sales", user_role="finance")
        assert assessment["requires_approval"] is False # Finance bypasses low
