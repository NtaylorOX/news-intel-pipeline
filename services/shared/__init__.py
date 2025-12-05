"""
Shared utilities for all microservices
Import common configuration, models, and utilities here
"""

# Configuration utilities
from .config import (
    get_database_url,
    get_kafka_config,
    get_database_config,
    get_environment,
    get_kafka_bootstrap_servers,
    get_openai_api_key,
    get_scheduler_interval,
    get_analysis_batch_size,
    get_log_level,
    is_production,
    is_development,
    KafkaConfig,
    DatabaseConfig,
)

# Logging utilities
from .logging_setup import setup_logger, get_logger, LoggerFactory

# Error handling
from .errors import (
    NewsAppException,
    DatabaseException,
    KafkaException,
    ConfigurationException,
    AnalysisException,
    ValidationException,
    FetchException,
    error_to_dict,
)

__all__ = [
    # Configuration
    'get_database_url',
    'get_kafka_config',
    'get_database_config',
    'get_environment',
    'get_kafka_bootstrap_servers',
    'get_openai_api_key',
    'get_scheduler_interval',
    'get_analysis_batch_size',
    'get_log_level',
    'is_production',
    'is_development',
    'KafkaConfig',
    'DatabaseConfig',
    # Logging
    'setup_logger',
    'get_logger',
    'LoggerFactory',
    # Errors
    'NewsAppException',
    'DatabaseException',
    'KafkaException',
    'ConfigurationException',
    'AnalysisException',
    'ValidationException',
    'FetchException',
    'error_to_dict',
]
