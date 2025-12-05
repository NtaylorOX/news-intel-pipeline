"""
Shared configuration utilities for all microservices
Centralized environment variable handling and configuration
"""

import os
from typing import Optional


def get_environment() -> str:
    """Get the current environment (development/production)"""
    return os.getenv("PYTHON_ENV", "development")


def get_database_url() -> str:
    """Get database connection URL from environment or construct it"""
    # Check if DATABASE_URL is already set (takes precedence)
    if "DATABASE_URL" in os.environ:
        return os.getenv("DATABASE_URL")
    
    # Otherwise construct from individual components
    db_user = os.getenv("DB_USER", "postgres")
    db_password = os.getenv("DB_PASSWORD", "postgres")
    db_host = os.getenv("DB_HOST", "postgres")
    db_port = os.getenv("DB_PORT", "5432")
    db_name = os.getenv("DB_NAME", "news_db")
    
    # Return PostgreSQL connection string
    return f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"


def get_kafka_bootstrap_servers() -> str:
    """Get Kafka bootstrap servers from environment"""
    return os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")


def get_openai_api_key() -> str:
    """Get OpenAI API key from environment"""
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        raise ValueError(
            "OPENAI_API_KEY environment variable not set. "
            "Please set it in .env file or environment."
        )
    return api_key


def get_scheduler_interval() -> int:
    """Get scheduler interval in minutes"""
    return int(os.getenv("SCHEDULER_INTERVAL", "5"))


def get_analysis_batch_size() -> int:
    """Get analysis batch size"""
    return int(os.getenv("ANALYSIS_BATCH_SIZE", "5"))


def get_log_level() -> str:
    """Get logging level"""
    return os.getenv("LOG_LEVEL", "INFO")


class KafkaConfig:
    """Kafka configuration dataclass"""
    
    def __init__(self, bootstrap_servers: Optional[str] = None):
        self.bootstrap_servers = bootstrap_servers or get_kafka_bootstrap_servers()
        self.topics = {
            "article_needs_analysis": "article_needs_analysis",
            "article_analyzed": "article_analyzed",
            "analysis_error": "analysis_error",
        }
        self.partitions = {
            "article_needs_analysis": 3,
            "article_analyzed": 3,
            "analysis_error": 1,
        }
        self.replication_factor = 1


class DatabaseConfig:
    """Database configuration dataclass"""
    
    def __init__(self):
        self.url = get_database_url()
        self.environment = get_environment()
        
        # SQLite-specific args (for development)
        self.echo = self.environment == "development"
        
        # SQLite connection args (if using SQLite)
        self.connect_args = {}
        if "sqlite" in self.url:
            self.connect_args = {"check_same_thread": False}


# Singleton instances
_kafka_config: Optional[KafkaConfig] = None
_db_config: Optional[DatabaseConfig] = None


def get_kafka_config() -> KafkaConfig:
    """Get singleton Kafka configuration"""
    global _kafka_config
    if _kafka_config is None:
        _kafka_config = KafkaConfig()
    return _kafka_config


def get_database_config() -> DatabaseConfig:
    """Get singleton database configuration"""
    global _db_config
    if _db_config is None:
        _db_config = DatabaseConfig()
    return _db_config


def is_production() -> bool:
    """Check if running in production mode"""
    return get_environment() == "production"


def is_development() -> bool:
    """Check if running in development mode"""
    return get_environment() == "development"
