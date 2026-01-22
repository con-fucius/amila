"""
Qlik Sense Enterprise API Client
Implements read-only operations for Qlik Sense on-premises integration
with enterprise-grade security
"""

import logging
import secrets
from typing import Dict, Any, Optional, List
import httpx
from datetime import datetime, timezone

from app.core.config import settings
from app.core.exceptions import ExternalServiceException
from app.core.audit import log_audit_event
from app.core.rate_limiter import rate_limiter, RateLimitTier

logger = logging.getLogger(__name__)

# Note: We use the global rate_limiter instance instead of a local one
# as the current RateLimiter class is designed as a singleton-style redis client.


class QlikSenseClient:
    """
    Qlik Sense Enterprise (on-prem) API client.
    Implements read-only operations via QRS API.
    """
    
    def __init__(
        self,
        base_url: str,
        xrfkey: Optional[str] = None,
        auth_user: Optional[str] = None,
        timeout: int = 30,
    ):
        """
        Initialize Qlik Sense client.
        
        Args:
            base_url: Qlik Sense server base URL (e.g., https://qlik-server:4242)
            xrfkey: 16-character Xrfkey for CSRF protection
            auth_user: User directory and ID (e.g., "UserDirectory=INTERNAL;UserId=sa_repository")
            timeout: Request timeout in seconds
        """
        self.base_url = base_url.rstrip('/')
        self.xrfkey = xrfkey or self._generate_xrfkey()
        self.auth_user = auth_user
        self.timeout = timeout
        self._client: Optional[httpx.AsyncClient] = None
        
    def _generate_xrfkey(self) -> str:
        """Generate 16-character random Xrfkey"""
        return secrets.token_urlsafe(12)[:16]
    
    def _get_headers(self) -> Dict[str, str]:
        """Get request headers with Xrfkey and auth"""
        headers = {
            "X-Qlik-Xrfkey": self.xrfkey,
            "Content-Type": "application/json",
        }
        if self.auth_user:
            headers["X-Qlik-User"] = self.auth_user
        return headers
    
    async def _get_client(self) -> httpx.AsyncClient:
        """Get or create HTTP client with proper certificate validation"""
        if not self._client:
            verify_ssl = True if settings.environment == "production" else False
            
            self._client = httpx.AsyncClient(
                timeout=self.timeout,
                verify=verify_ssl,
            )
            
            if not verify_ssl:
                logger.warning(
                    "Qlik API client running with SSL verification disabled. "
                    "Enable SSL verification in production!"
                )
        return self._client
    
    async def close(self):
        """Close HTTP client"""
        if self._client:
            await self._client.aclose()
            self._client = None
    
    async def list_apps(
        self,
        filter_query: Optional[str] = None,
        user_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        List Qlik Sense apps (read-only) with rate limiting.
        
        Args:
            filter_query: Optional filter (e.g., "name eq 'Sales Dashboard'")
            user_id: User ID for audit logging
            
        Returns:
            List of apps with metadata
        """
        await rate_limiter.check_rate_limit(
            user=user_id or "anonymous",
            endpoint="qlik_list_apps",
            tier=RateLimitTier.VIEWER
        )
        
        try:
            client = await self._get_client()
            
            url = f"{self.base_url}/qrs/app/full"
            params = {"Xrfkey": self.xrfkey}
            if filter_query:
                params["filter"] = filter_query
            
            response = await client.get(
                url,
                headers=self._get_headers(),
                params=params,
            )
            response.raise_for_status()
            
            apps = response.json()
            
            await log_audit_event(
                event_type="qlik_list_apps",
                user_id=user_id,
                details={
                    "app_count": len(apps) if isinstance(apps, list) else 0,
                    "filter": filter_query,
                    "status": "success",
                }
            )
            
            return {
                "status": "success",
                "apps": apps,
                "count": len(apps) if isinstance(apps, list) else 0,
            }
            
        except httpx.HTTPStatusError as e:
            logger.error(f"Qlik API error: {e.response.status_code} - {e.response.text}")
            await log_audit_event(
                event_type="qlik_list_apps_failed",
                user_id=user_id,
                details={
                    "error": str(e),
                    "status_code": e.response.status_code,
                }
            )
            raise ExternalServiceException(f"Qlik API error: {e}", service_name="qlik")
            
        except Exception as e:
            logger.error(f"Failed to list Qlik apps: {e}")
            raise ExternalServiceException(f"Qlik connection error: {e}", service_name="qlik")
    
    async def get_app(
        self,
        app_id: str,
        user_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Get Qlik Sense app details (read-only).
        
        Args:
            app_id: App GUID
            user_id: User ID for audit logging
            
        Returns:
            App details
        """
        try:
            client = await self._get_client()
            
            url = f"{self.base_url}/qrs/app/{app_id}"
            params = {"Xrfkey": self.xrfkey}
            
            response = await client.get(
                url,
                headers=self._get_headers(),
                params=params,
            )
            response.raise_for_status()
            
            app = response.json()
            
            await log_audit_event(
                event_type="qlik_get_app",
                user_id=user_id,
                details={
                    "app_id": app_id,
                    "app_name": app.get("name"),
                    "status": "success",
                }
            )
            
            return {
                "status": "success",
                "app": app,
            }
            
        except httpx.HTTPStatusError as e:
            logger.error(f"Qlik API error: {e.response.status_code} - {e.response.text}")
            raise ExternalServiceException(f"Qlik API error: {e}", service_name="qlik")
            
        except Exception as e:
            logger.error(f"Failed to get Qlik app: {e}")
            raise ExternalServiceException(f"Qlik connection error: {e}", service_name="qlik")
    
    async def list_sheets(
        self,
        app_id: str,
        user_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        List sheets in a Qlik Sense app (read-only).
        
        Args:
            app_id: App GUID
            user_id: User ID for audit logging
            
        Returns:
            List of sheets
        """
        try:
            client = await self._get_client()
            
            url = f"{self.base_url}/qrs/app/object/full"
            params = {
                "Xrfkey": self.xrfkey,
                "filter": f"app.id eq {app_id} and objectType eq 'sheet'"
            }
            
            response = await client.get(
                url,
                headers=self._get_headers(),
                params=params,
            )
            response.raise_for_status()
            
            sheets = response.json()
            
            await log_audit_event(
                event_type="qlik_list_sheets",
                user_id=user_id,
                details={
                    "app_id": app_id,
                    "sheet_count": len(sheets) if isinstance(sheets, list) else 0,
                    "status": "success",
                }
            )
            
            return {
                "status": "success",
                "sheets": sheets,
                "count": len(sheets) if isinstance(sheets, list) else 0,
            }
            
        except httpx.HTTPStatusError as e:
            logger.error(f"Qlik API error: {e.response.status_code} - {e.response.text}")
            raise ExternalServiceException(f"Qlik API error: {e}", service_name="qlik")
            
        except Exception as e:
            logger.error(f"Failed to list Qlik sheets: {e}")
            raise ExternalServiceException(f"Qlik connection error: {e}", service_name="qlik")
    
    async def health_check(self) -> Dict[str, Any]:
        """Check Qlik Sense API health"""
        try:
            client = await self._get_client()
            
            url = f"{self.base_url}/qrs/about"
            params = {"Xrfkey": self.xrfkey}
            
            response = await client.get(
                url,
                headers=self._get_headers(),
                params=params,
                timeout=10,
            )
            response.raise_for_status()
            
            about = response.json()
            
            return {
                "status": "healthy",
                "version": about.get("buildVersion"),
                "product": about.get("productName"),
            }
            
        except Exception as e:
            logger.error(f"Qlik health check failed: {e}")
            return {
                "status": "unhealthy",
                "error": str(e)
            }


def create_qlik_client(
    base_url: Optional[str] = None,
    xrfkey: Optional[str] = None,
    auth_user: Optional[str] = None,
) -> QlikSenseClient:
    """
    Factory function to create Qlik Sense client.
    
    Args:
        base_url: Qlik server URL (from env if not provided)
        xrfkey: Xrfkey (generated if not provided)
        auth_user: Auth user (from env if not provided)
        
    Returns:
        QlikSenseClient instance
    """
    qlik_url = base_url or getattr(settings, "QLIK_BASE_URL", None)
    qlik_user = auth_user or getattr(settings, "QLIK_AUTH_USER", None)
    
    if not qlik_url:
        raise ValueError("Qlik base URL not configured")
    
    return QlikSenseClient(
        base_url=qlik_url,
        xrfkey=xrfkey,
        auth_user=qlik_user,
    )

