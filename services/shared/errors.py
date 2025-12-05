"""
Shared exception classes and error handling utilities for all microservices
Provides consistent error handling and reporting
"""

from typing import Optional


class NewsAppException(Exception):
    """Base exception for the News App"""
    
    def __init__(self, message: str, error_code: Optional[str] = None):
        self.message = message
        self.error_code = error_code or "UNKNOWN_ERROR"
        super().__init__(self.message)


class DatabaseException(NewsAppException):
    """Raised when database operations fail"""
    
    def __init__(self, message: str, original_error: Optional[Exception] = None):
        super().__init__(message, "DB_ERROR")
        self.original_error = original_error


class KafkaException(NewsAppException):
    """Raised when Kafka operations fail"""
    
    def __init__(self, message: str, original_error: Optional[Exception] = None):
        super().__init__(message, "KAFKA_ERROR")
        self.original_error = original_error


class ConfigurationException(NewsAppException):
    """Raised when configuration is invalid or missing"""
    
    def __init__(self, message: str):
        super().__init__(message, "CONFIG_ERROR")


class AnalysisException(NewsAppException):
    """Raised when LLM analysis fails"""
    
    def __init__(self, message: str, original_error: Optional[Exception] = None):
        super().__init__(message, "ANALYSIS_ERROR")
        self.original_error = original_error


class ValidationException(NewsAppException):
    """Raised when data validation fails"""
    
    def __init__(self, message: str, field: Optional[str] = None):
        super().__init__(message, "VALIDATION_ERROR")
        self.field = field


class FetchException(NewsAppException):
    """Raised when news fetching fails"""
    
    def __init__(self, message: str, source: Optional[str] = None):
        super().__init__(message, "FETCH_ERROR")
        self.source = source


def error_to_dict(error: Exception) -> dict:
    """Convert exception to dictionary for JSON serialization"""
    if isinstance(error, NewsAppException):
        return {
            "error": error.__class__.__name__,
            "message": error.message,
            "code": error.error_code,
        }
    else:
        return {
            "error": error.__class__.__name__,
            "message": str(error),
            "code": "UNKNOWN_ERROR",
        }
