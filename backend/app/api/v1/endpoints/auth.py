"""
Authentication Endpoints
JWT-based authentication and authorization
"""

from datetime import timedelta, datetime
from fastapi import APIRouter, HTTPException, Depends, status
from fastapi.security import OAuth2PasswordRequestForm, OAuth2PasswordBearer
from pydantic import BaseModel
from typing import Dict, Any
import logging

from app.core.config import settings
from app.core.auth import auth_manager, AuthenticationManager, verify_password, get_password_hash, decode_access_token

router = APIRouter()
logger = logging.getLogger(__name__)

# OAuth2 scheme for token authentication
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="api/v1/auth/login")


# Pydantic models for request/response
class Token(BaseModel):
    access_token: str
    token_type: str


class TokenPair(BaseModel):
    access_token: str
    refresh_token: str
    token_type: str


class TokenData(BaseModel):
    username: str | None = None


class UserLogin(BaseModel):
    username: str
    password: str


class RefreshTokenRequest(BaseModel):
    refresh_token: str


# For demonstration purposes, we'll use a simple in-memory user store
# In a real application, this would be replaced with a database
USERS_DB = {
    "admin": {
        "username": "admin",
        "hashed_password": get_password_hash("adminpassword"),
        "disabled": False,
        "role": "admin"  # Admin role bypasses cost limits and approval
    },
    "user": {
        "username": "user",
        "hashed_password": get_password_hash("userpassword"),
        "disabled": False,
        "role": "analyst"  # Analyst role has standard permissions
    }
}


async def authenticate_user(username: str, password: str) -> dict | None:
    """
    Authenticate a user by username and password using the AuthenticationManager

    Args:
        username: Username to authenticate
        password: Password to verify

    Returns:
        dict: User data if authentication successful, None otherwise
    """
    return await auth_manager.authenticate_user(username, password)


async def get_current_user(token: str = Depends(oauth2_scheme)) -> dict:
    """
    Get current user from JWT token
    
    Args:
        token: JWT token from Authorization header
        
    Returns:
        dict: User data if token is valid
        
    Raises:
        HTTPException: If token is invalid or user not found
    """
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Could not validate credentials",
        headers={"WWW-Authenticate": "Bearer"},
    )
    
    payload = decode_access_token(token)
    if payload is None:
        raise credentials_exception
    
    username: str = payload.get("sub")
    if username is None:
        raise credentials_exception
    
    token_data = TokenData(username=username)
    user = USERS_DB.get(token_data.username)
    if user is None:
        raise credentials_exception
    
    return user


@router.post("/login", response_model=TokenPair)
async def login(form_data: OAuth2PasswordRequestForm = Depends()) -> Dict[str, Any]:
    """
    User login endpoint
    Authenticates user and returns JWT access and refresh tokens
    """
    user = await authenticate_user(form_data.username, form_data.password)
    if not user:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect username or password",
            headers={"WWW-Authenticate": "Bearer"},
        )

    # Create tokens with role included in payload
    token_payload = {
        "sub": user["username"],
        "role": user.get("role", "analyst")  # Include role for RBAC
    }
    access_token = auth_manager.create_access_token(token_payload)
    refresh_token = auth_manager.create_refresh_token(token_payload)

    # Store refresh token
    refresh_payload = auth_manager.decode_token(refresh_token, "refresh")
    if refresh_payload:
        session_id = await auth_manager.create_session(user["username"])
        await auth_manager.store_refresh_token(user["username"], refresh_payload["jti"], session_id)

    logger.info(f"User {user['username']} logged in successfully")

    return {
        "access_token": access_token,
        "refresh_token": refresh_token,
        "token_type": "bearer"
    }


@router.post("/refresh", response_model=TokenPair)
async def refresh_token(request: RefreshTokenRequest) -> Dict[str, Any]:
    """
    Token refresh endpoint with rotation
    Generates new access and refresh tokens, revoking the old refresh token
    """
    try:
        # Validate refresh token and get username
        payload = auth_manager.decode_token(request.refresh_token, "refresh")
        if not payload:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid refresh token",
                headers={"WWW-Authenticate": "Bearer"},
            )

        username = payload.get("sub")
        if not username:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid token data",
                headers={"WWW-Authenticate": "Bearer"},
            )

        # Refresh tokens with rotation
        new_access_token, new_refresh_token = auth_manager.refresh_access_token(
            request.refresh_token, username
        )

        logger.info(f"Tokens refreshed and rotated for user {username}")

        return {
            "access_token": new_access_token,
            "refresh_token": new_refresh_token,
            "token_type": "bearer"
        }

    except Exception as e:
        logger.error(f"Token refresh failed: {e}")
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Token refresh failed",
            headers={"WWW-Authenticate": "Bearer"},
        )


@router.post("/logout")
async def logout() -> Dict[str, Any]:
    """
    User logout endpoint
    In a stateless JWT implementation, logout is typically handled client-side
    by discarding the token. This endpoint is for logging purposes.
    """
    # In a real application, you might want to implement token blacklisting
    # For now, we'll just log the logout action
    logger.info("User logged out")
    
    return {
        "message": "Successfully logged out",
        "status": "success"
    }