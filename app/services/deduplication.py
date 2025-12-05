"""Service for detecting and handling duplicate articles."""

import logging
from sqlalchemy.orm import Session
from app.core.database import NewsArticle
from typing import Tuple

logger = logging.getLogger(__name__)


class DeduplicationService:
    """Service for checking if articles are duplicates."""

    @staticmethod
    def is_duplicate(db: Session, content_hash: str) -> bool:
        """Check if an article with this hash already exists.

        Args:
            db: Database session
            content_hash: MD5 hash of article content/URL

        Returns:
            True if article already exists, False otherwise
        """
        existing = db.query(NewsArticle).filter(NewsArticle.content_hash == content_hash).first()
        return existing is not None

    @staticmethod
    def mark_as_duplicate(db: Session, article_id: int) -> bool:
        """Mark an article as duplicate.

        Args:
            db: Database session
            article_id: ID of the article to mark

        Returns:
            True if successful, False otherwise
        """
        try:
            article = db.query(NewsArticle).filter(NewsArticle.id == article_id).first()
            if article:
                article.is_duplicate = True
                db.commit()
                return True
            return False
        except Exception as e:
            logger.error(f"Error marking article {article_id} as duplicate: {e}")
            db.rollback()
            return False

    @staticmethod
    def get_duplicate_count(db: Session) -> int:
        """Get count of articles marked as duplicates.

        Args:
            db: Database session

        Returns:
            Number of duplicate articles
        """
        return db.query(NewsArticle).filter(NewsArticle.is_duplicate == True).count()

    @staticmethod
    def get_recent_duplicate(db: Session, content_hash: str) -> Tuple[bool, dict]:
        """Get information about recent duplicate if it exists.

        Args:
            db: Database session
            content_hash: MD5 hash to check

        Returns:
            Tuple of (is_duplicate: bool, article_info: dict)
        """
        article = db.query(NewsArticle).filter(NewsArticle.content_hash == content_hash).first()

        if article:
            return (
                True,
                {
                    "id": article.id,
                    "title": article.title,
                    "source": article.source,
                    "fetched_at": article.fetched_at.isoformat() if article.fetched_at else None,
                },
            )

        return (False, {})
