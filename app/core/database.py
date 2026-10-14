"""Database models for news articles and ingestion tracking."""

from sqlalchemy import Column, Integer, String, Text, DateTime, Boolean, Float, create_engine
from sqlalchemy.orm import declarative_base
from sqlalchemy.orm import sessionmaker
from datetime import datetime
import os

Base = declarative_base()


class NewsArticle(Base):
    """Model for storing news articles."""

    __tablename__ = "news_articles"

    id = Column(Integer, primary_key=True, index=True)
    external_id = Column(String(255), nullable=True, index=True)  # External event ID from /ingest
    title = Column(String(500), nullable=False)
    content = Column(Text, nullable=False)
    source = Column(String(100), nullable=False)
    url = Column(String(500), unique=True, nullable=False, index=True)
    content_hash = Column(String(64), unique=True, nullable=False, index=True)
    
    # Metadata
    published_at = Column(DateTime, nullable=True)
    fetched_at = Column(DateTime, default=datetime.utcnow, index=True)
    
    # Processing status
    is_duplicate = Column(Boolean, default=False, index=True)
    passed_filter = Column(Boolean, default=None, nullable=True)
    is_analyzed = Column(Boolean, default=False, index=True)
    
    # Filter results (JSON) - contains detailed filter scores and reasoning
    filter_results = Column(Text, nullable=True)
    
    # Computed score columns for fast sorting (extracted from filter_results)
    relevance_score = Column(Float, default=0.0, index=True)
    quality_score = Column(Float, default=0.0, index=True)
    
    # AI analysis results (stored as JSON string for now)
    analysis_result = Column(Text, nullable=True)
    summary = Column(Text, nullable=True)
    
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


class IngestionLog(Base):
    """Log for tracking ingestion runs."""

    __tablename__ = "ingestion_logs"

    id = Column(Integer, primary_key=True, index=True)
    source = Column(String(100), nullable=False)
    articles_fetched = Column(Integer, default=0)
    articles_new = Column(Integer, default=0)
    articles_duplicate = Column(Integer, default=0)
    articles_filtered = Column(Integer, default=0)
    started_at = Column(DateTime, default=datetime.utcnow)
    completed_at = Column(DateTime, nullable=True)
    error_message = Column(Text, nullable=True)
    status = Column(String(20), default="running")  # running, success, failed


# Database setup
DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://postgres:postgres@localhost:5432/news_db"
)
# Pin the psycopg2 driver explicitly; newer SQLAlchemy defaults bare "postgresql://" to psycopg 3
if DATABASE_URL.startswith("postgresql://"):
    DATABASE_URL = DATABASE_URL.replace("postgresql://", "postgresql+psycopg2://", 1)

# Remove sqlite-specific connection args for PostgreSQL
if "postgresql" in DATABASE_URL or "postgres" in DATABASE_URL:
    engine = create_engine(DATABASE_URL, echo=False, pool_pre_ping=True)
else:
    # SQLite fallback for local development
    engine = create_engine(
        DATABASE_URL,
        connect_args={"check_same_thread": False},
    )

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


def init_db():
    """Initialize database tables."""
    Base.metadata.create_all(bind=engine)


def get_db():
    """Dependency for FastAPI to get DB session."""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
