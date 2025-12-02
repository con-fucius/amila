"""
Application Configuration Management
Provides backward compatibility by importing from centralized config_manager
"""

# Import from centralized configuration manager for backward compatibility
from .config_manager import settings, get_settings, AppSettings as Settings

# Maintain the same interface
create_settings = get_settings