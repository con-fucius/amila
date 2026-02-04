"""
LDAP Integration Service

Provides foundation for dynamic AD group resolution.
Supports Active Directory integration for enterprise authentication.

Features:
- LDAP connection pooling
- AD group membership resolution
- User attribute retrieval
- Caching for performance
- Fallback to static mappings
"""

import logging
import json
from typing import Dict, Any, List, Optional, Set
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum

from app.core.redis_client import redis_client
from app.core.config import settings

logger = logging.getLogger(__name__)


class LDAPStatus(Enum):
    """LDAP connection status"""
    CONNECTED = "connected"
    DISCONNECTED = "disconnected"
    ERROR = "error"
    DISABLED = "disabled"


@dataclass
class LDAPUser:
    """LDAP user information"""
    user_id: str
    email: str
    display_name: str
    groups: List[str]
    department: Optional[str] = None
    title: Optional[str] = None
    manager: Optional[str] = None
    attributes: Dict[str, Any] = None


@dataclass
class LDAPConfig:
    """LDAP configuration"""
    enabled: bool
    server_url: str
    bind_dn: str
    bind_password: str
    base_dn: str
    user_search_filter: str
    group_search_filter: str
    use_ssl: bool = True
    timeout_seconds: int = 10
    cache_ttl_seconds: int = 3600


class LDAPIntegrationService:
    """
    Service for LDAP/Active Directory integration.
    
    Provides:
    - Dynamic AD group resolution
    - User attribute retrieval
    - Group membership caching
    - Fallback to static mappings when LDAP unavailable
    """
    
    # Cache key prefixes
    USER_GROUPS_CACHE_PREFIX = "ldap:user_groups:"
    GROUP_MEMBERS_CACHE_PREFIX = "ldap:group_members:"
    USER_ATTRS_CACHE_PREFIX = "ldap:user_attrs:"
    
    def __init__(self):
        self.config = self._load_config()
        self._connection_pool = None
        self._status = LDAPStatus.DISABLED if not self.config.enabled else LDAPStatus.DISCONNECTED
    
    def _load_config(self) -> LDAPConfig:
        """Load LDAP configuration from settings"""
        return LDAPConfig(
            enabled=getattr(settings, 'LDAP_ENABLED', False),
            server_url=getattr(settings, 'LDAP_SERVER_URL', 'ldap://localhost:389'),
            bind_dn=getattr(settings, 'LDAP_BIND_DN', ''),
            bind_password=getattr(settings, 'LDAP_BIND_PASSWORD', ''),
            base_dn=getattr(settings, 'LDAP_BASE_DN', 'DC=company,DC=com'),
            user_search_filter=getattr(settings, 'LDAP_USER_SEARCH_FILTER', '(sAMAccountName={username})'),
            group_search_filter=getattr(settings, 'LDAP_GROUP_SEARCH_FILTER', '(member={user_dn})'),
            use_ssl=getattr(settings, 'LDAP_USE_SSL', True),
            timeout_seconds=getattr(settings, 'LDAP_TIMEOUT_SECONDS', 10),
            cache_ttl_seconds=getattr(settings, 'LDAP_CACHE_TTL_SECONDS', 3600)
        )
    
    async def get_user_groups(self, user_id: str, use_cache: bool = True) -> List[str]:
        """
        Get AD groups for a user.
        
        Args:
            user_id: User identifier (email or username)
            use_cache: Whether to use cached results
            
        Returns:
            List of AD group names
        """
        if not self.config.enabled:
            # Fallback to static mapping
            return await self._get_static_user_groups(user_id)
        
        cache_key = f"{self.USER_GROUPS_CACHE_PREFIX}{user_id}"
        
        # Try cache first
        if use_cache:
            try:
                cached = await redis_client.get(cache_key)
                if cached:
                    logger.debug(f"LDAP cache hit for user {user_id}")
                    return cached.get('groups', [])
            except Exception as e:
                logger.warning(f"Cache read failed: {e}")
        
        # Query LDAP
        try:
            groups = await self._query_ldap_for_groups(user_id)
            
            # Cache result
            if groups:
                try:
                    await redis_client.set(
                        cache_key,
                        {'groups': groups, 'timestamp': datetime.now(timezone.utc).isoformat()},
                        ttl=self.config.cache_ttl_seconds
                    )
                except Exception as e:
                    logger.warning(f"Cache write failed: {e}")
            
            return groups
            
        except Exception as e:
            logger.error(f"LDAP query failed for {user_id}: {e}")
            # Fallback to static mapping
            return await self._get_static_user_groups(user_id)
    
    async def _query_ldap_for_groups(self, user_id: str) -> List[str]:
        """
        Query LDAP for user groups.
        
        Note: This is a foundation implementation. Full LDAP integration
        requires the 'ldap3' package which can be added when needed.
        """
        try:
            # Try to import ldap3 for actual LDAP operations
            from ldap3 import Server, Connection, ALL, SUBTREE
            
            server = Server(self.config.server_url, get_info=ALL, use_ssl=self.config.use_ssl)
            conn = Connection(
                server,
                self.config.bind_dn,
                self.config.bind_password,
                auto_bind=True,
                read_only=True
            )
            
            # Search for user
            user_filter = self.config.user_search_filter.format(username=user_id)
            conn.search(
                search_base=self.config.base_dn,
                search_filter=user_filter,
                search_scope=SUBTREE,
                attributes=['memberOf', 'distinguishedName']
            )
            
            if not conn.entries:
                logger.warning(f"User {user_id} not found in LDAP")
                return []
            
            user_entry = conn.entries[0]
            user_dn = user_entry.distinguishedName.value
            
            # Get direct group memberships
            groups = []
            if hasattr(user_entry, 'memberOf'):
                for group_dn in user_entry.memberOf.values:
                    # Extract CN from DN
                    group_name = self._extract_cn_from_dn(group_dn)
                    if group_name:
                        groups.append(group_name)
            
            # Search for nested group memberships
            nested_filter = f"(&(objectClass=group)(member:1.2.840.113556.1.4.1941:={user_dn}))"
            conn.search(
                search_base=self.config.base_dn,
                search_filter=nested_filter,
                search_scope=SUBTREE,
                attributes=['cn']
            )
            
            for entry in conn.entries:
                if hasattr(entry, 'cn'):
                    group_name = entry.cn.value
                    if group_name and group_name not in groups:
                        groups.append(group_name)
            
            conn.unbind()
            logger.info(f"Retrieved {len(groups)} LDAP groups for user {user_id}")
            return groups
            
        except ImportError:
            logger.warning("ldap3 package not installed, using mock LDAP data")
            return await self._get_mock_ldap_groups(user_id)
        except Exception as e:
            logger.error(f"LDAP query error: {e}")
            raise
    
    def _extract_cn_from_dn(self, dn: str) -> Optional[str]:
        """Extract CN (Common Name) from Distinguished Name"""
        try:
            # Parse "CN=GroupName,OU=Groups,DC=company,DC=com"
            parts = dn.split(',')
            for part in parts:
                if part.strip().upper().startswith('CN='):
                    return part.strip()[3:]
            return None
        except Exception:
            return None
    
    async def _get_static_user_groups(self, user_id: str) -> List[str]:
        """
        Get groups from static configuration (fallback when LDAP unavailable).
        
        Loads from AD_GROUP_MAPPINGS environment variable or settings.
        """
        try:
            # Try to load from settings
            static_mappings = getattr(settings, 'AD_GROUP_STATIC_MAPPINGS', {})
            
            if user_id in static_mappings:
                return static_mappings[user_id]
            
            # Try email domain matching
            if '@' in user_id:
                domain = user_id.split('@')[1]
                domain_groups = static_mappings.get(f"@{domain}", [])
                return domain_groups
            
            return []
            
        except Exception as e:
            logger.warning(f"Static group lookup failed: {e}")
            return []
    
    async def _get_mock_ldap_groups(self, user_id: str) -> List[str]:
        """
        Return mock LDAP groups for testing/development.
        """
        # Development mock data based on user patterns
        mock_mappings = {
            'admin': ['Administrators', 'BI_Admins', 'Finance_Full_Access'],
            'analyst': ['BI_Analysts', 'Sales_ReadOnly', 'Finance_ReadOnly'],
            'finance': ['Finance_Full_Access', 'Finance_Analysts', 'BI_Analysts'],
            'sales': ['Sales_Team', 'Sales_ReadOnly', 'BI_Analysts'],
            'hr': ['HR_Team', 'HR_Full_Access', 'BI_Analysts'],
        }
        
        user_lower = user_id.lower()
        for key, groups in mock_mappings.items():
            if key in user_lower:
                return groups
        
        # Default group
        return ['BI_Users', 'General_Access']
    
    async def resolve_user_permissions(self, user_id: str) -> Dict[str, Any]:
        """
        Resolve comprehensive user permissions from LDAP.
        
        Returns:
            Dict with groups, attributes, and permission summary
        """
        groups = await self.get_user_groups(user_id)
        
        return {
            'user_id': user_id,
            'groups': groups,
            'group_count': len(groups),
            'source': 'ldap' if self.config.enabled else 'static',
            'resolved_at': datetime.now(timezone.utc).isoformat(),
            'ldap_status': self._status.value,
        }
    
    async def invalidate_user_cache(self, user_id: str):
        """Invalidate cached LDAP data for a user"""
        cache_key = f"{self.USER_GROUPS_CACHE_PREFIX}{user_id}"
        try:
            await redis_client._client.delete(cache_key)
            logger.info(f"Invalidated LDAP cache for user {user_id}")
        except Exception as e:
            logger.warning(f"Cache invalidation failed: {e}")
    
    async def get_service_status(self) -> Dict[str, Any]:
        """Get LDAP service status"""
        return {
            'status': self._status.value,
            'enabled': self.config.enabled,
            'server_url': self.config.server_url if self.config.enabled else None,
            'base_dn': self.config.base_dn if self.config.enabled else None,
            'cache_ttl': self.config.cache_ttl_seconds,
        }
    
    async def test_connection(self) -> Dict[str, Any]:
        """Test LDAP connectivity"""
        if not self.config.enabled:
            return {'success': False, 'error': 'LDAP is disabled'}
        
        try:
            from ldap3 import Server, Connection
            
            server = Server(self.config.server_url, use_ssl=self.config.use_ssl)
            conn = Connection(
                server,
                self.config.bind_dn,
                self.config.bind_password,
                auto_bind=True,
                read_only=True
            )
            
            if conn.bind():
                conn.unbind()
                self._status = LDAPStatus.CONNECTED
                return {
                    'success': True,
                    'message': 'LDAP connection successful',
                    'server': self.config.server_url
                }
            else:
                self._status = LDAPStatus.ERROR
                return {'success': False, 'error': 'LDAP bind failed'}
                
        except ImportError:
            return {'success': False, 'error': 'ldap3 package not installed'}
        except Exception as e:
            self._status = LDAPStatus.ERROR
            return {'success': False, 'error': str(e)}


# Global instance
ldap_service = LDAPIntegrationService()


# Convenience functions

async def get_user_ad_groups(user_id: str, use_cache: bool = True) -> List[str]:
    """Get AD groups for a user"""
    return await ldap_service.get_user_groups(user_id, use_cache)


async def resolve_user_ldap_permissions(user_id: str) -> Dict[str, Any]:
    """Resolve comprehensive LDAP permissions for a user"""
    return await ldap_service.resolve_user_permissions(user_id)


async def invalidate_ldap_cache(user_id: str):
    """Invalidate LDAP cache for a user"""
    await ldap_service.invalidate_user_cache(user_id)


async def get_ldap_status() -> Dict[str, Any]:
    """Get LDAP service status"""
    return await ldap_service.get_service_status()