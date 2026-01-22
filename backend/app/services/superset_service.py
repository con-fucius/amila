"""
Apache Superset API Client
Implements dashboard auto-generation and visualization capabilities
"""

import logging
from typing import Dict, Any, Optional, List
import httpx
from datetime import datetime, timezone

from app.core.config import settings
from app.core.exceptions import ExternalServiceException
from app.core.audit import log_audit_event

logger = logging.getLogger(__name__)


class SupersetClient:
    """
    Apache Superset (on-prem) API client.
    Implements dashboard creation, chart generation, and dataset management.
    """
    
    def __init__(
        self,
        base_url: str,
        username: Optional[str] = None,
        password: Optional[str] = None,
        timeout: int = 30,
    ):
        """
        Initialize Superset client.
        
        Args:
            base_url: Superset server base URL (e.g., http://superset:8088)
            username: Superset username
            password: Superset password
            timeout: Request timeout in seconds
        """
        self.base_url = base_url.rstrip('/')
        self.username = username
        self.password = password
        self.timeout = timeout
        self._client: Optional[httpx.AsyncClient] = None
        self._access_token: Optional[str] = None
        self._csrf_token: Optional[str] = None
        
    async def _get_client(self) -> httpx.AsyncClient:
        """Get or create HTTP client"""
        if not self._client:
            self._client = httpx.AsyncClient(
                timeout=self.timeout,
                follow_redirects=True,
            )
        return self._client
    
    async def close(self):
        """Close HTTP client"""
        if self._client:
            await self._client.aclose()
            self._client = None
    
    async def _authenticate(self) -> None:
        """
        Authenticate and get access token with Redis caching and automatic refresh.
        Tokens are stored securely in Redis with TTL.
        """
        token_key = f"superset:token:{self.username}"
        
        try:
            from app.core.redis_client import redis_client
            
            cached_token = await redis_client.get(token_key)
            if cached_token and isinstance(cached_token, dict):
                self._access_token = cached_token.get("access_token")
                self._csrf_token = cached_token.get("csrf_token")
                logger.debug("Using cached Superset authentication tokens")
                return
        except Exception as e:
            logger.warning(f"Failed to retrieve cached Superset token: {e}")
        
        try:
            client = await self._get_client()
            
            login_data = {
                "username": self.username,
                "password": self.password,
                "provider": "db",
                "refresh": True,
            }
            
            response = await client.post(
                f"{self.base_url}/api/v1/security/login",
                json=login_data,
            )
            response.raise_for_status()
            
            data = response.json()
            self._access_token = data.get("access_token")
            
            csrf_response = await client.get(
                f"{self.base_url}/api/v1/security/csrf_token/",
                headers={"Authorization": f"Bearer {self._access_token}"}
            )
            csrf_response.raise_for_status()
            
            csrf_data = csrf_response.json()
            self._csrf_token = csrf_data.get("result")
            
            try:
                from app.core.redis_client import redis_client
                await redis_client.setex(
                    token_key,
                    3600,
                    {
                        "access_token": self._access_token,
                        "csrf_token": self._csrf_token
                    }
                )
                logger.debug("Superset tokens cached in Redis with 1-hour TTL")
            except Exception as e:
                logger.warning(f"Failed to cache Superset token in Redis: {e}")
            
            logger.info("Superset authentication successful")
            
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 401:
                self._access_token = None
                self._csrf_token = None
                try:
                    from app.core.redis_client import redis_client
                    await redis_client.delete(token_key)
                except:
                    pass
            logger.error(f"Superset authentication failed: {e}")
            raise ExternalServiceException(f"Superset auth error: {e}", service_name="superset")
        except Exception as e:
            logger.error(f"Superset authentication failed: {e}")
            raise ExternalServiceException(f"Superset auth error: {e}", service_name="superset")
    
    def _get_headers(self) -> Dict[str, str]:
        """Get request headers with auth and CSRF token"""
        headers = {
            "Content-Type": "application/json",
        }
        if self._access_token:
            headers["Authorization"] = f"Bearer {self._access_token}"
        if self._csrf_token:
            headers["X-CSRFToken"] = self._csrf_token
        return headers
    
    async def list_dashboards(
        self,
        user_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        List Superset dashboards (read-only) with automatic token refresh.
        
        Args:
            user_id: User ID for audit logging
            
        Returns:
            List of dashboards
        """
        await self._authenticate()
        
        try:
            client = await self._get_client()
            
            response = await client.get(
                f"{self.base_url}/api/v1/dashboard/",
                headers=self._get_headers(),
            )
            response.raise_for_status()
            
            data = response.json()
            dashboards = data.get("result", [])
            
            await log_audit_event(
                event_type="superset_list_dashboards",
                user_id=user_id,
                details={
                    "dashboard_count": len(dashboards),
                    "status": "success",
                }
            )
            
            return {
                "status": "success",
                "dashboards": dashboards,
                "count": len(dashboards),
            }
            
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 401:
                logger.warning("Superset token expired, re-authenticating...")
                self._access_token = None
                self._csrf_token = None
                await self._authenticate()
                
                response = await client.get(
                    f"{self.base_url}/api/v1/dashboard/",
                    headers=self._get_headers(),
                )
                response.raise_for_status()
                
                data = response.json()
                dashboards = data.get("result", [])
                
                return {
                    "status": "success",
                    "dashboards": dashboards,
                    "count": len(dashboards),
                }
            
            logger.error(f"Superset API error: {e.response.status_code} - {e.response.text}")
            raise ExternalServiceException(f"Superset API error: {e}", service_name="superset")
            
        except Exception as e:
            logger.error(f"Failed to list Superset dashboards: {e}")
            raise ExternalServiceException(f"Superset connection error: {e}", service_name="superset")
    
    async def get_dashboard(
        self,
        dashboard_id: int,
        user_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Get Superset dashboard details.
        
        Args:
            dashboard_id: Dashboard ID
            user_id: User ID for audit logging
            
        Returns:
            Dashboard details
        """
        await self._authenticate()
        
        try:
            client = await self._get_client()
            
            response = await client.get(
                f"{self.base_url}/api/v1/dashboard/{dashboard_id}",
                headers=self._get_headers(),
            )
            response.raise_for_status()
            
            data = response.json()
            dashboard = data.get("result", {})
            
            await log_audit_event(
                event_type="superset_get_dashboard",
                user_id=user_id,
                details={
                    "dashboard_id": dashboard_id,
                    "dashboard_title": dashboard.get("dashboard_title"),
                    "status": "success",
                }
            )
            
            return {
                "status": "success",
                "dashboard": dashboard,
            }
            
        except httpx.HTTPStatusError as e:
            logger.error(f"Superset API error: {e.response.status_code} - {e.response.text}")
            raise ExternalServiceException(f"Superset API error: {e}", service_name="superset")
            
        except Exception as e:
            logger.error(f"Failed to get Superset dashboard: {e}")
            raise ExternalServiceException(f"Superset connection error: {e}", service_name="superset")
    
    async def create_dataset(
        self,
        database_id: int,
        schema: str,
        table_name: str,
        sql: Optional[str] = None,
        user_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Create a dataset in Superset from a table or custom SQL (virtual dataset).
        Required for dashboard generation.
        
        Args:
            database_id: Superset Database ID (where the table resides)
            schema: Database schema
            table_name: Table name
            sql: Optional custom SQL for virtual dataset
            user_id: User ID for audit logging
            
        Returns:
            Created dataset details
        """
        await self._authenticate()
        
        try:
            client = await self._get_client()
            
            payload = {
                "database": database_id,
                "schema": schema,
                "table_name": table_name,
            }
            if sql:
                payload["sql"] = sql
            
            response = await client.post(
                f"{self.base_url}/api/v1/dataset/",
                headers=self._get_headers(),
                json=payload,
            )
            response.raise_for_status()
            
            data = response.json()
            dataset = data.get("result", {})
            
            await log_audit_event(
                event_type="superset_create_dataset",
                user_id=user_id,
                details={
                    "dataset_id": data.get("id"),
                    "table": f"{schema}.{table_name}",
                    "is_virtual": sql is not None,
                    "status": "success",
                }
            )
            
            return {
                "status": "success",
                "dataset": dataset,
                "dataset_id": data.get("id") or dataset.get("id")
            }
            
        except httpx.HTTPStatusError as e:
            logger.error(f"Superset API error: {e.response.status_code} - {e.response.text}")
            raise ExternalServiceException(f"Superset API error: {e}", service_name="superset")
            
        except Exception as e:
            logger.error(f"Failed to create Superset dataset: {e}")
            raise ExternalServiceException(f"Superset connection error: {e}", service_name="superset")

    async def create_chart(
        self,
        chart_config: Dict[str, Any],
        user_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Create a new chart in Superset.
        
        Args:
            chart_config: Chart configuration
            user_id: User ID for audit logging
            
        Returns:
            Created chart details
        """
        await self._authenticate()
        
        try:
            client = await self._get_client()
            
            response = await client.post(
                f"{self.base_url}/api/v1/chart/",
                headers=self._get_headers(),
                json=chart_config,
            )
            response.raise_for_status()
            
            data = response.json()
            chart = data.get("result", {})
            
            await log_audit_event(
                event_type="superset_create_chart",
                user_id=user_id,
                details={
                    "chart_id": chart.get("id"),
                    "chart_name": chart_config.get("slice_name"),
                    "status": "success",
                }
            )
            
            return {
                "status": "success",
                "chart": chart,
            }
            
        except httpx.HTTPStatusError as e:
            logger.error(f"Superset API error: {e.response.status_code} - {e.response.text}")
            raise ExternalServiceException(f"Superset API error: {e}", service_name="superset")
            
        except Exception as e:
            logger.error(f"Failed to create Superset chart: {e}")
            raise ExternalServiceException(f"Superset connection error: {e}", service_name="superset")
    
    async def health_check(self) -> Dict[str, Any]:
        """Check Superset API health"""
        try:
            client = await self._get_client()
            
            response = await client.get(
                f"{self.base_url}/health",
                timeout=10,
            )
            response.raise_for_status()
            
            return {
                "status": "healthy",
            }
            
        except Exception as e:
            logger.error(f"Superset health check failed: {e}")
            return {
                "status": "unhealthy",
                "error": str(e)
            }


def create_superset_client(
    base_url: Optional[str] = None,
    username: Optional[str] = None,
    password: Optional[str] = None,
) -> SupersetClient:
    """
    Factory function to create Superset client.
    
    Args:
        base_url: Superset URL (from env if not provided)
        username: Superset username (from env if not provided)
        password: Superset password (from env if not provided)
        
    Returns:
        SupersetClient instance
    """
    superset_url = base_url or getattr(settings, "SUPERSET_BASE_URL", None)
    superset_user = username or getattr(settings, "SUPERSET_USERNAME", None)
    superset_pass = password or getattr(settings, "SUPERSET_PASSWORD", None)
    
    if not superset_url:
        raise ValueError("Superset base URL not configured")
    
    return SupersetClient(
        base_url=superset_url,
        username=superset_user,
        password=superset_pass,
    )

