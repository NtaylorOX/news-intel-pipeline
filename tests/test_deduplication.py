"""
Unit tests for DeduplicationService.

Tests duplicate detection and hash-based deduplication.
"""

import pytest
import sys
import os
import hashlib
from unittest.mock import MagicMock, patch

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from app.services.deduplication import DeduplicationService


def compute_hash(content: str) -> str:
    """Helper function to compute MD5 hash like the app does."""
    normalized = content.lower().strip()
    return hashlib.md5(normalized.encode()).hexdigest()



class TestContentHashing:
    """Test content hash generation."""

    def test_identical_content_same_hash(self):
        """Test that identical content produces the same hash."""
        content = "This is a news article about Kubernetes"
        hash1 = compute_hash(content)
        hash2 = compute_hash(content)
        assert hash1 == hash2

    def test_different_content_different_hash(self):
        """Test that different content produces different hashes."""
        content1 = "This is about Kubernetes"
        content2 = "This is about Docker"
        hash1 = compute_hash(content1)
        hash2 = compute_hash(content2)
        assert hash1 != hash2

    def test_case_insensitive_hash(self):
        """Test that hash is case-insensitive (normalized)."""
        content1 = "Kubernetes Article"
        content2 = "kubernetes article"
        hash1 = compute_hash(content1)
        hash2 = compute_hash(content2)
        assert hash1 == hash2

    def test_hash_deterministic(self):
        """Test that hash is always deterministic."""
        content = "AI and Machine Learning News"
        hashes = [compute_hash(content) for _ in range(10)]
        assert len(set(hashes)) == 1  # All hashes should be identical

    def test_hash_format_consistency(self):
        """Test that hash format is consistent (MD5 produces 32-char hex)."""
        content = "Test content for hashing"
        hash_result = compute_hash(content)
        
        # MD5 hex strings are 32 characters
        assert len(hash_result) == 32
        # Should be hexadecimal
        assert all(c in "0123456789abcdef" for c in hash_result)


class TestDuplicateDetection:
    """Test duplicate detection with mocked database."""

    def test_is_duplicate_returns_true_for_existing(self):
        """Test that is_duplicate returns True when article exists."""
        # Mock the database session and query
        mock_db = MagicMock()
        mock_article = MagicMock()
        mock_db.query.return_value.filter.return_value.first.return_value = mock_article
        
        result = DeduplicationService.is_duplicate(mock_db, "abc123def456")
        assert result is True

    def test_is_duplicate_returns_false_for_new(self):
        """Test that is_duplicate returns False for new content."""
        mock_db = MagicMock()
        mock_db.query.return_value.filter.return_value.first.return_value = None
        
        result = DeduplicationService.is_duplicate(mock_db, "abc123def456")
        assert result is False

    def test_mark_as_duplicate_updates_article(self):
        """Test marking an article as duplicate."""
        mock_db = MagicMock()
        mock_article = MagicMock()
        mock_db.query.return_value.filter.return_value.first.return_value = mock_article
        
        result = DeduplicationService.mark_as_duplicate(mock_db, 123)
        
        assert result is True
        assert mock_article.is_duplicate is True
        mock_db.commit.assert_called_once()

    def test_mark_as_duplicate_handles_missing_article(self):
        """Test that marking fails gracefully when article doesn't exist."""
        mock_db = MagicMock()
        mock_db.query.return_value.filter.return_value.first.return_value = None
        
        result = DeduplicationService.mark_as_duplicate(mock_db, 999)
        assert result is False

    def test_get_duplicate_count(self):
        """Test getting count of duplicate articles."""
        mock_db = MagicMock()
        mock_db.query.return_value.filter.return_value.count.return_value = 5
        
        result = DeduplicationService.get_duplicate_count(mock_db)
        assert result == 5

    def test_get_recent_duplicate_returns_info(self):
        """Test getting info about a duplicate."""
        mock_db = MagicMock()
        mock_article = MagicMock()
        mock_article.id = 123
        mock_article.title = "Test Article"
        mock_article.source = "TestSource"
        mock_article.fetched_at = MagicMock()
        mock_article.fetched_at.isoformat.return_value = "2025-11-04T10:00:00"
        
        mock_db.query.return_value.filter.return_value.first.return_value = mock_article
        
        is_dup, info = DeduplicationService.get_recent_duplicate(mock_db, "abc123")
        
        assert is_dup is True
        assert info["id"] == 123
        assert info["title"] == "Test Article"

    def test_get_recent_duplicate_returns_false_for_new(self):
        """Test that get_recent_duplicate returns False for new content."""
        mock_db = MagicMock()
        mock_db.query.return_value.filter.return_value.first.return_value = None
        
        is_dup, info = DeduplicationService.get_recent_duplicate(mock_db, "abc123")
        
        assert is_dup is False
        assert info == {}
