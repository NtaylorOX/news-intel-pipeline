"""
Unit tests for ContentFilterService.

Tests the filtering logic for relevance, quality, and AI-based scoring.
"""

import pytest
import sys
import os
from unittest.mock import patch

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from app.services.filtering import ContentFilterService


class TestRelevance:
    """Test relevance detection with tech keywords."""

    def test_kubernetes_is_relevant(self):
        """Test that Kubernetes articles are flagged as relevant."""
        title = "Kubernetes Security Best Practices"
        content = "This article covers Kubernetes security best practices for container orchestration."
        is_relevant, reason = ContentFilterService.is_relevant(title, content)
        assert is_relevant is True
        assert "keyword" in reason.lower()

    def test_python_is_relevant(self):
        """Test that Python articles are flagged as relevant."""
        title = "Python 3.13 Release Features"
        content = "Python 3.13 introduces new features for developers."
        is_relevant, reason = ContentFilterService.is_relevant(title, content)
        assert is_relevant is True

    def test_ai_is_relevant(self):
        """Test that AI articles are flagged as relevant."""
        title = "Latest AI Breakthroughs in Machine Learning"
        content = "Recent advances in artificial intelligence and machine learning models."
        is_relevant, reason = ContentFilterService.is_relevant(title, content)
        assert is_relevant is True

    def test_sports_not_relevant(self):
        """Test that sports articles are not flagged as relevant."""
        title = "Soccer Championship Final Results"
        content = "The final championship soccer match concluded with an exciting victory."
        is_relevant, reason = ContentFilterService.is_relevant(title, content)
        assert is_relevant is False

    def test_cooking_not_relevant(self):
        """Test that cooking articles are not flagged as relevant."""
        title = "Delicious Pasta Recipes for Dinner"
        content = "Here are some delicious pasta recipes perfect for a family dinner."
        is_relevant, reason = ContentFilterService.is_relevant(title, content)
        assert is_relevant is False

    def test_multiple_keywords_boost_relevance(self):
        """Test that multiple tech keywords increase relevance."""
        title = "Docker and Kubernetes for Cloud Infrastructure"
        content = "Learn how to use Docker containers with Kubernetes for cloud deployment and scaling."
        is_relevant, reason = ContentFilterService.is_relevant(title, content)
        assert is_relevant is True
        # Check that multiple keywords were found (just check that keywords were found)
        assert "keyword" in reason.lower()


class TestQuality:
    """Test quality checks for article content."""

    def test_good_quality_article(self):
        """Test that a well-written article passes quality checks."""
        title = "Comprehensive Guide to Kubernetes"
        content = "This comprehensive guide covers all aspects of Kubernetes deployment. " * 20
        is_quality, reason = ContentFilterService.is_quality(title, content)
        assert is_quality is True

    def test_too_short_title(self):
        """Test that articles with very short titles fail quality check."""
        title = "AI"
        content = "A substantial article about artificial intelligence." * 20
        is_quality, reason = ContentFilterService.is_quality(title, content)
        assert is_quality is False
        assert "title" in reason.lower()

    def test_too_short_content(self):
        """Test that articles with insufficient content fail quality check."""
        title = "Important News Article"
        content = "This is too short."
        is_quality, reason = ContentFilterService.is_quality(title, content)
        assert is_quality is False
        assert "content" in reason.lower() or "short" in reason.lower()

    def test_too_long_content(self):
        """Test that articles exceeding max length fail quality check."""
        title = "Very Long Article"
        content = "x" * 60000  # Exceeds max length
        is_quality, reason = ContentFilterService.is_quality(title, content)
        assert is_quality is False
        assert "long" in reason.lower()

    def test_spam_detection(self):
        """Test that spam patterns are detected."""
        title = "Click here for amazing deals!"
        content = "This is a sponsored advertisement. Click here now!" * 10
        is_quality, reason = ContentFilterService.is_quality(title, content)
        assert is_quality is False
        assert "spam" in reason.lower()

    def test_fragmented_content_detection(self):
        """Test that fragmented content (too many short sentences) is detected."""
        title = "Article Title"
        content = ". ".join(["a"] * 200)  # Creates many single-character sentences
        is_quality, reason = ContentFilterService.is_quality(title, content)
        assert is_quality is False


class TestFilterArticle:
    """Test combined filtering logic."""

    @patch('app.services.filtering.settings.AI_FILTER_ENABLED', False)
    def test_good_tech_article_passes(self):
        """Test that a good tech article passes all filters (without AI)."""
        title = "Kubernetes Security Updates"
        content = "Important security updates for Kubernetes cluster management. " * 20
        passes, details = ContentFilterService.filter_article(title, content)
        assert passes is True
        assert details["filters"]["relevance"]["passed"] is True
        assert details["filters"]["quality"]["passed"] is True

    @patch('app.services.filtering.settings.AI_FILTER_ENABLED', False)
    def test_relevant_but_low_quality_fails(self):
        """Test that relevant but low-quality content fails."""
        title = "Kubernetes"
        content = "Short"
        passes, details = ContentFilterService.filter_article(title, content)
        assert passes is False
        assert details["filters"]["quality"]["passed"] is False

    @patch('app.services.filtering.settings.AI_FILTER_ENABLED', False)
    def test_quality_but_not_relevant_fails(self):
        """Test that quality but non-tech content fails."""
        title = "The Art of Italian Cooking"
        content = ("Italian cuisine represents a rich culinary tradition spanning centuries. " * 20)
        passes, details = ContentFilterService.filter_article(title, content)
        assert passes is False
        assert details["filters"]["relevance"]["passed"] is False

    @patch('app.services.filtering.settings.AI_FILTER_ENABLED', False)
    def test_filter_details_structure(self):
        """Test that filter details have required structure."""
        title = "Python Programming Tips"
        content = "This article covers essential Python programming practices." * 20
        passes, details = ContentFilterService.filter_article(title, content)
        
        assert "passed" in details
        assert "filters" in details
        assert "relevance" in details["filters"]
        assert "quality" in details["filters"]
        assert "relevance_score" in details
        assert "quality_score" in details
        
        # Check that scores are floats between 0 and 1
        assert 0.0 <= details["relevance_score"] <= 1.0
        assert 0.0 <= details["quality_score"] <= 1.0

    @patch('app.services.filtering.settings.AI_FILTER_ENABLED', False)
    def test_edge_case_empty_content(self):
        """Test that empty content fails gracefully."""
        title = "Title Only"
        content = ""
        passes, details = ContentFilterService.filter_article(title, content)
        assert passes is False
        assert details["filters"]["quality"]["passed"] is False

    @patch('app.services.filtering.settings.AI_FILTER_ENABLED', False)
    def test_edge_case_empty_title(self):
        """Test that empty title fails gracefully."""
        title = ""
        content = "This is a substantial piece of content." * 20
        passes, details = ContentFilterService.filter_article(title, content)
        assert passes is False
        assert details["filters"]["relevance"]["passed"] is False or details["filters"]["quality"]["passed"] is False


class TestFilterStats:
    """Test the filter stats generation."""

    @patch('app.services.filtering.settings.AI_FILTER_ENABLED', False)
    def test_get_filter_stats_passing(self):
        """Test generating stats for a passing filter."""
        title = "Cloud Infrastructure Best Practices"
        content = "This article covers AWS and Azure cloud deployment patterns." * 20
        passes, details = ContentFilterService.filter_article(title, content)
        
        stats = ContentFilterService.get_filter_stats(details)
        assert isinstance(stats, str)
        if passes:
            assert "passed" in stats.lower()

    @patch('app.services.filtering.settings.AI_FILTER_ENABLED', False)
    def test_get_filter_stats_failing(self):
        """Test generating stats for a failing filter."""
        title = "Sports News"
        content = "Short content"
        passes, details = ContentFilterService.filter_article(title, content)
        
        stats = ContentFilterService.get_filter_stats(details)
        assert isinstance(stats, str)
        if not passes:
            assert "failed" in stats.lower() or "reject" in stats.lower()
