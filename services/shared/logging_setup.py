"""
Shared logging configuration for all microservices
Provides consistent logging format and setup
"""

import logging
import sys
from typing import Optional

from .config import get_log_level, get_environment


def setup_logger(
    name: str,
    level: Optional[str] = None,
) -> logging.Logger:
    """
    Setup a logger with consistent formatting
    
    Args:
        name: Logger name (usually __name__)
        level: Log level (INFO, DEBUG, WARNING, ERROR)
               If None, uses LOG_LEVEL environment variable
    
    Returns:
        Configured logger instance
    """
    log_level = level or get_log_level()
    
    logger = logging.getLogger(name)
    logger.setLevel(getattr(logging, log_level.upper(), logging.INFO))
    
    # Only add handler if logger doesn't have one already
    if not logger.handlers:
        # Create console handler
        handler = logging.StreamHandler(sys.stdout)
        handler.setLevel(getattr(logging, log_level.upper(), logging.INFO))
        
        # Create formatter
        formatter = logging.Formatter(
            fmt='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        
        handler.setFormatter(formatter)
        logger.addHandler(handler)
    
    return logger


def get_logger(name: str) -> logging.Logger:
    """Get or create a logger with the given name"""
    return logging.getLogger(name)


class LoggerFactory:
    """Factory for creating consistently configured loggers"""
    
    _loggers = {}
    
    @classmethod
    def get_logger(cls, name: str) -> logging.Logger:
        """Get a cached logger or create a new one"""
        if name not in cls._loggers:
            cls._loggers[name] = setup_logger(name)
        return cls._loggers[name]
    
    @classmethod
    def reset(cls):
        """Reset all cached loggers"""
        cls._loggers.clear()
