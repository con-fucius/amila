"""
User Model
Defines user data structures for authentication and authorization
"""

from typing import Optional
from pydantic import BaseModel, Field


class User(BaseModel):
    """User model for authentication"""
    username: str = Field(..., description="Username")
    email: Optional[str] = Field(None, description="User email")
    full_name: Optional[str] = Field(None, description="Full name")
    disabled: bool = Field(default=False, description="Whether user is disabled")
    role: str = Field(default="analyst", description="User role (admin, analyst, viewer)")
    
    class Config:
        json_schema_extra = {
            "example": {
                "username": "john_doe",
                "email": "john@example.com",
                "full_name": "John Doe",
                "disabled": False,
                "role": "analyst"
            }
        }


class UserInDB(User):
    """User model with hashed password for database storage"""
    hashed_password: str = Field(..., description="Hashed password")


class TokenData(BaseModel):
    """Token payload data"""
    username: Optional[str] = None
    role: Optional[str] = None
