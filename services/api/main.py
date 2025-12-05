"""
API Service - Thin FastAPI wrapper for read-only endpoints
Routes all analysis requests to Kafka message queue for asynchronous processing
"""

import os
import logging
from contextlib import asynccontextmanager
from typing import Optional

from fastapi import FastAPI
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware

# Add parent directories to path for imports
import sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../../'))

from app.core.database import Base, engine, init_db
from app.services.kafka_messaging import KafkaConfig, KafkaMessageProducer, create_topics_if_not_exist
from app.api.endpoints import (
    ingestion,
    news,
    insights,
    dashboard
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Kafka configuration
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
PYTHON_ENV = os.getenv("PYTHON_ENV", "development")

kafka_config = KafkaConfig(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS)
kafka_producer: Optional[KafkaMessageProducer] = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Startup and shutdown event handler"""
    # Startup
    logger.info("🚀 API Service starting up...")
    
    # Initialize database
    init_db()
    logger.info("✓ Database initialized")
    
    # Initialize Kafka topics and producer
    try:
        create_topics_if_not_exist(KAFKA_BOOTSTRAP_SERVERS)
        logger.info("✓ Kafka topics verified/created")
        
        global kafka_producer
        kafka_producer = KafkaMessageProducer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS)
        kafka_producer.connect()
        logger.info("✓ Kafka producer connected")
    except Exception as e:
        logger.error(f"❌ Kafka initialization failed: {e}")
        if PYTHON_ENV == "production":
            raise
        logger.warning("Continuing without Kafka (development mode)")
    
    logger.info("✅ API Service ready on http://0.0.0.0:8000")
    
    yield
    
    # Shutdown
    logger.info("🛑 API Service shutting down...")
    if kafka_producer:
        kafka_producer.close()
        logger.info("✓ Kafka producer closed")
    logger.info("✅ Shutdown complete")


# Create FastAPI application
app = FastAPI(
    title="News Think Tank - API Service",
    description="Read-only API endpoints for news ingestion and analysis",
    version="1.0.0",
    lifespan=lifespan,
    docs_url="/api/docs",
    openapi_url="/api/openapi.json"
)

# Add CORS middleware to allow dashboard requests
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/health", tags=["Health"])
async def health_check():
    """Health check endpoint for container orchestration"""
    return {
        "status": "healthy",
        "service": "api",
        "environment": PYTHON_ENV
    }


@app.get("/health/ready", tags=["Health"])
async def readiness_check():
    """Readiness check - only returns healthy if Kafka producer is connected"""
    status = {
        "ready": kafka_producer is not None,
        "kafka_connected": kafka_producer is not None if kafka_producer else False
    }
    
    if not status["ready"]:
        return JSONResponse(status_code=503, content=status)
    
    return status


# Register routers
app.include_router(
    ingestion.router,
    prefix="/api/ingestion"
)

app.include_router(
    news.router,
    prefix="/api/news"
)

app.include_router(
    insights.router,
    prefix="/api/insights"
)

app.include_router(
    dashboard.router,
    prefix="/dashboard"
)


@app.get("/", tags=["Root"])
async def root():
    """Root endpoint with service information"""
    return {
        "service": "News Think Tank - API Service",
        "version": "1.0.0",
        "environment": PYTHON_ENV,
        "docs": "/api/docs",
        "health": "/health",
        "readiness": "/health/ready",
        "endpoints": {
            "ingestion": "/api/ingestion",
            "news": "/api/news",
            "insights": "/api/insights",
            "scheduler": "/api/scheduler"
        }
    }


@app.exception_handler(Exception)
async def global_exception_handler(request, exc):
    """Global exception handler"""
    logger.error(f"Unhandled exception: {exc}", exc_info=True)
    return JSONResponse(
        status_code=500,
        content={"detail": "Internal server error"}
    )


if __name__ == "__main__":
    import uvicorn
    
    uvicorn.run(
        "services.api.main:app",
        host="0.0.0.0",
        port=8000,
        reload=PYTHON_ENV == "development",
        log_level="info"
    )
