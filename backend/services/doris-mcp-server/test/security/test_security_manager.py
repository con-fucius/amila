#!/usr/bin/env python3
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
"""
Security manager integration tests
"""

import pytest

from doris_mcp_server.utils.security import (
    DorisSecurityManager,
    AuthContext,
    SecurityLevel,
    ValidationResult
)


class TestDorisSecurityManager:
    """Doris security manager integration tests"""

    @pytest.fixture
    def security_manager(self, test_config):
        """Create security manager instance"""
        return DorisSecurityManager(test_config)

    @pytest.mark.asyncio
    async def test_complete_security_workflow(self, security_manager, sample_data):
        """Test complete security workflow"""
        # 1. Authentication
        auth_info = {
            "type": "token",
            "token": "valid_token_123"
        }
        
        auth_context = await security_manager.authenticate_request(auth_info)
        assert isinstance(auth_context, AuthContext)
        assert auth_context.security_level == SecurityLevel.INTERNAL
        
        # 2. Authorization
        resource_uri = "/api/table/public_reports"
        has_access = await security_manager.authorize_resource_access(auth_context, resource_uri)
        assert has_access is True
        
        # 3. SQL Validation
        safe_sql = "SELECT name, email FROM users WHERE department = 'sales'"
        validation_result = await security_manager.validate_sql_security(safe_sql, auth_context)
        assert validation_result.is_valid is True
        
        # 4. Data Masking
        masked_data = await security_manager.apply_data_masking(sample_data, auth_context)
        assert masked_data[0]["phone"] == "138****5678"  # Should be masked

    @pytest.mark.asyncio
    async def test_admin_workflow(self, security_manager, sample_data):
        """Test admin user workflow"""
        # Admin authentication
        auth_info = {
            "type": "basic",
            "username": "admin",
            "password": "admin123"
        }
        
        auth_context = await security_manager.authenticate_request(auth_info)
        assert auth_context.security_level == SecurityLevel.SECRET
        
        # Admin should access secret resources
        resource_uri = "/api/table/payment_records"
        has_access = await security_manager.authorize_resource_access(auth_context, resource_uri)
        assert has_access is True
        
        # Admin should see original data (no masking)
        masked_data = await security_manager.apply_data_masking(sample_data, auth_context)
        assert masked_data[0]["phone"] == "13812345678"  # Original data

    @pytest.mark.asyncio
    async def test_security_violation_detection(self, security_manager):
        """Test security violation detection"""
        # Authenticate as regular user
        auth_info = {
            "type": "token",
            "token": "valid_token_123"
        }
        
        auth_context = await security_manager.authenticate_request(auth_info)
        
        # Try to access confidential resource (user_info is CONFIDENTIAL, user is INTERNAL)
        # INTERNAL(1) should not access CONFIDENTIAL(2) resource
        resource_uri = "/api/table/user_info"
        has_access = await security_manager.authorize_resource_access(auth_context, resource_uri)
        assert has_access is False
        
        # Try dangerous SQL
        dangerous_sql = "DROP TABLE users"
        validation_result = await security_manager.validate_sql_security(dangerous_sql, auth_context)
        assert validation_result.is_valid is False
        assert "DROP" in validation_result.blocked_operations

    @pytest.mark.asyncio
    async def test_sql_injection_prevention(self, security_manager):
        """Test SQL injection prevention"""
        auth_info = {
            "type": "token",
            "token": "valid_token_123"
        }
        
        auth_context = await security_manager.authenticate_request(auth_info)
        
        # Test various injection attempts
        injection_attempts = [
            "SELECT * FROM users WHERE id = 1; DROP TABLE users;",
            "SELECT * FROM users UNION SELECT password FROM admin_users",
            "SELECT * FROM users WHERE id = 1 OR 1=1",
            "SELECT * FROM users WHERE name = 'test' -- AND password = 'secret'"
        ]
        
        for sql in injection_attempts:
            result = await security_manager.validate_sql_security(sql, auth_context)
            assert result.is_valid is False
            assert result.risk_level in ["medium", "high"]

    @pytest.mark.asyncio
    async def test_authentication_failure_handling(self, security_manager):
        """Test authentication failure handling"""
        invalid_auth_info = {
            "type": "token",
            "token": "invalid_token"
        }
        
        with pytest.raises(Exception):
            await security_manager.authenticate_request(invalid_auth_info)

    @pytest.mark.asyncio
    async def test_configuration_loading(self, security_manager):
        """Test security configuration loading"""
        # Test blocked keywords loading
        assert "DROP" in security_manager.blocked_keywords
        assert "DELETE" in security_manager.blocked_keywords
        
        # Test sensitive tables loading
        assert SecurityLevel.CONFIDENTIAL in security_manager.sensitive_tables.values()
        assert SecurityLevel.SECRET in security_manager.sensitive_tables.values()
        
        # Test masking rules loading
        assert len(security_manager.masking_rules) > 0
        phone_rules = [rule for rule in security_manager.masking_rules 
                      if "phone" in rule.column_pattern]
        assert len(phone_rules) > 0

    def test_security_level_hierarchy(self, security_manager):
        """Test security level hierarchy"""
        # Test that hierarchy is correctly defined
        levels = [SecurityLevel.PUBLIC, SecurityLevel.INTERNAL, 
                 SecurityLevel.CONFIDENTIAL, SecurityLevel.SECRET]
        
        # Each level should be properly defined
        for level in levels:
            assert isinstance(level, SecurityLevel)
            assert level.value in ["public", "internal", "confidential", "secret"] 