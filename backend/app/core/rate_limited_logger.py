"""
Rate-Limited Logger
Prevents log flooding by rate-limiting repeated log messages
"""

import logging
import time
from typing import Dict, Tuple
from collections import defaultdict
from threading import Lock

class RateLimitedLogger:
    """
    Wrapper around standard logger that rate-limits repeated messages
    """
    
    def __init__(self, logger: logging.Logger, window_seconds: int = 60, max_per_window: int = 3):
        """
        Initialize rate-limited logger
        
        Args:
            logger: Underlying logger instance
            window_seconds: Time window for rate limiting (default: 60s)
            max_per_window: Maximum messages per window (default: 3)
        """
        self.logger = logger
        self.window_seconds = window_seconds
        self.max_per_window = max_per_window
        self._message_counts: Dict[Tuple[int, str], Tuple[int, float]] = defaultdict(lambda: (0, 0.0))
        self._lock = Lock()
    
    def _should_log(self, level: int, message: str) -> bool:
        """
        Check if message should be logged based on rate limit
        
        Args:
            level: Log level
            message: Log message
            
        Returns:
            True if message should be logged
        """
        key = (level, message)
        current_time = time.time()
        
        with self._lock:
            count, first_time = self._message_counts[key]
            
            # Reset if outside window
            if current_time - first_time > self.window_seconds:
                self._message_counts[key] = (1, current_time)
                return True
            
            # Check if within limit
            if count < self.max_per_window:
                self._message_counts[key] = (count + 1, first_time)
                return True
            
            # Suppressed - log once when hitting limit
            if count == self.max_per_window:
                self._message_counts[key] = (count + 1, first_time)
                self.logger.log(
                    level,
                    f"[RATE LIMIT] Suppressing repeated message: {message[:100]}... "
                    f"(will resume in {int(self.window_seconds - (current_time - first_time))}s)"
                )
            
            return False
    
    def debug(self, message: str, *args, **kwargs):
        """Log debug message with rate limiting"""
        if self._should_log(logging.DEBUG, message):
            self.logger.debug(message, *args, **kwargs)
    
    def info(self, message: str, *args, **kwargs):
        """Log info message with rate limiting"""
        if self._should_log(logging.INFO, message):
            self.logger.info(message, *args, **kwargs)
    
    def warning(self, message: str, *args, **kwargs):
        """Log warning message with rate limiting"""
        if self._should_log(logging.WARNING, message):
            self.logger.warning(message, *args, **kwargs)
    
    def error(self, message: str, *args, **kwargs):
        """Log error message with rate limiting"""
        if self._should_log(logging.ERROR, message):
            self.logger.error(message, *args, **kwargs)
    
    def critical(self, message: str, *args, **kwargs):
        """Log critical message with rate limiting"""
        if self._should_log(logging.CRITICAL, message):
            self.logger.critical(message, *args, **kwargs)


def get_rate_limited_logger(name: str, window_seconds: int = 60, max_per_window: int = 3) -> RateLimitedLogger:
    """
    Get a rate-limited logger instance
    
    Args:
        name: Logger name
        window_seconds: Time window for rate limiting
        max_per_window: Maximum messages per window
        
    Returns:
        RateLimitedLogger instance
    """
    logger = logging.getLogger(name)
    return RateLimitedLogger(logger, window_seconds, max_per_window)
