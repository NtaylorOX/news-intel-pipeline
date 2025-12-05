"""
Unit tests for AIService.

Tests AI-powered classification and analysis methods.
Note: These tests use mocking to avoid actual OpenAI API calls.
"""

import pytest
import sys
import os
import json
from unittest.mock import patch, MagicMock, Mock

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))


class TestClassifyRelevance:
    """Test AI-powered relevance classification."""

    @patch('app.services.ai_service.ChatOpenAI')
    @patch.dict(os.environ, {'OPENAI_API_KEY': 'test-key'})
    def test_relevant_tech_article(self, mock_llm):
        """Test classification of a relevant tech article."""
        from app.services.ai_service import AIService
        
        # Mock the LLM response
        mock_response = MagicMock()
        mock_response.content = '{"is_relevant": true, "relevance_score": 0.9, "quality_score": 0.85, "reason": "Discusses Kubernetes"}'
        
        mock_llm.return_value.invoke.return_value = mock_response
        
        ai_service = AIService()
        result = ai_service.classify_relevance("Kubernetes security best practices article")
        
        assert result["is_relevant"] is True
        assert result["relevance_score"] == 0.9
        assert result["quality_score"] == 0.85

    @patch('app.services.ai_service.ChatOpenAI')
    @patch.dict(os.environ, {'OPENAI_API_KEY': 'test-key'})
    def test_irrelevant_content(self, mock_llm):
        """Test classification of irrelevant content."""
        from app.services.ai_service import AIService
        
        mock_response = MagicMock()
        mock_response.content = '{"is_relevant": false, "relevance_score": 0.1, "quality_score": 0.2, "reason": "Sports content"}'
        
        mock_llm.return_value.invoke.return_value = mock_response
        
        ai_service = AIService()
        result = ai_service.classify_relevance("Soccer match results")
        
        assert result["is_relevant"] is False
        assert result["relevance_score"] == 0.1

    @patch('app.services.ai_service.ChatOpenAI')
    @patch.dict(os.environ, {'OPENAI_API_KEY': 'test-key'})
    def test_malformed_json_response_fallback(self, mock_llm):
        """Test graceful fallback when JSON parsing fails."""
        from app.services.ai_service import AIService
        
        mock_response = MagicMock()
        mock_response.content = "This is not JSON at all!"
        
        mock_llm.return_value.invoke.return_value = mock_response
        
        ai_service = AIService()
        result = ai_service.classify_relevance("Some article")
        
        # Should fall back to conservative defaults
        assert result["is_relevant"] is False
        assert result["relevance_score"] == 0.0
        assert result["quality_score"] == 0.0
        assert "Could not parse" in result["reason"]

    @patch('app.services.ai_service.ChatOpenAI')
    @patch.dict(os.environ, {'OPENAI_API_KEY': 'test-key'})
    def test_scores_clamped_to_0_1_range(self, mock_llm):
        """Test that scores are clamped to 0-1 range."""
        from app.services.ai_service import AIService
        
        mock_response = MagicMock()
        # Try to return out-of-range values
        mock_response.content = '{"is_relevant": true, "relevance_score": 1.5, "quality_score": -0.5, "reason": "test"}'
        
        mock_llm.return_value.invoke.return_value = mock_response
        
        ai_service = AIService()
        result = ai_service.classify_relevance("Test article")
        
        # Should be clamped
        assert 0.0 <= result["relevance_score"] <= 1.0
        assert 0.0 <= result["quality_score"] <= 1.0

    @patch('app.services.ai_service.ChatOpenAI')
    @patch.dict(os.environ, {'OPENAI_API_KEY': 'test-key'})
    def test_missing_optional_fields(self, mock_llm):
        """Test handling of missing optional fields in response."""
        from app.services.ai_service import AIService
        
        mock_response = MagicMock()
        mock_response.content = '{"is_relevant": true}'
        
        mock_llm.return_value.invoke.return_value = mock_response
        
        ai_service = AIService()
        result = ai_service.classify_relevance("Test article")
        
        assert "relevance_score" in result
        assert "quality_score" in result
        assert "reason" in result


class TestGenerateActionItems:
    """Test AI-powered action items generation."""

    @patch('app.services.ai_service.ChatOpenAI')
    @patch.dict(os.environ, {'OPENAI_API_KEY': 'test-key'})
    def test_valid_action_items_response(self, mock_llm):
        """Test parsing of valid action items response."""
        from app.services.ai_service import AIService
        
        mock_response = MagicMock()
        mock_response.content = json.dumps({
            "action_items": [
                {"priority": "HIGH", "description": "Upgrade Kubernetes"},
                {"priority": "MEDIUM", "description": "Review costs"}
            ],
            "key_trends": ["Cloud optimization", "Security"],
            "risks": ["Downtime risk"],
            "opportunities": ["Cost savings"]
        })
        
        mock_llm.return_value.invoke.return_value = mock_response
        
        ai_service = AIService()
        result = ai_service.generate_action_items("News articles text")
        
        assert len(result["action_items"]) == 2
        assert result["action_items"][0]["priority"] == "HIGH"
        assert len(result["key_trends"]) == 2
        assert len(result["risks"]) == 1
        assert len(result["opportunities"]) == 1

    @patch('app.services.ai_service.ChatOpenAI')
    @patch.dict(os.environ, {'OPENAI_API_KEY': 'test-key'})
    def test_json_wrapped_in_markdown_code_block(self, mock_llm):
        """Test extraction of JSON from markdown code blocks."""
        from app.services.ai_service import AIService
        
        mock_response = MagicMock()
        json_content = {
            "action_items": [{"priority": "HIGH", "description": "Test"}],
            "key_trends": ["Trend1"],
            "risks": [],
            "opportunities": []
        }
        mock_response.content = f"```json\n{json.dumps(json_content)}\n```"
        
        mock_llm.return_value.invoke.return_value = mock_response
        
        ai_service = AIService()
        result = ai_service.generate_action_items("News")
        
        assert len(result["action_items"]) == 1
        assert result["action_items"][0]["priority"] == "HIGH"

    @patch('app.services.ai_service.ChatOpenAI')
    @patch.dict(os.environ, {'OPENAI_API_KEY': 'test-key'})
    def test_malformed_action_items_fallback(self, mock_llm):
        """Test fallback when action items parsing fails."""
        from app.services.ai_service import AIService
        
        mock_response = MagicMock()
        mock_response.content = "Invalid JSON response"
        
        mock_llm.return_value.invoke.return_value = mock_response
        
        ai_service = AIService()
        result = ai_service.generate_action_items("News")
        
        # Should have fallback structure
        assert "action_items" in result
        assert "key_trends" in result
        assert len(result["action_items"]) > 0  # Has fallback item
        assert "Unable to parse" in result["action_items"][0]["description"]

    @patch('app.services.ai_service.ChatOpenAI')
    @patch.dict(os.environ, {'OPENAI_API_KEY': 'test-key'})
    def test_empty_arrays_in_response(self, mock_llm):
        """Test handling of empty arrays in response."""
        from app.services.ai_service import AIService
        
        mock_response = MagicMock()
        mock_response.content = json.dumps({
            "action_items": [],
            "key_trends": [],
            "risks": [],
            "opportunities": []
        })
        
        mock_llm.return_value.invoke.return_value = mock_response
        
        ai_service = AIService()
        result = ai_service.generate_action_items("News")
        
        assert result["action_items"] == []
        assert result["key_trends"] == []


class TestAIServiceStructure:
    """Test AIService initialization and configuration."""

    @patch('app.services.ai_service.ChatOpenAI')
    @patch.dict(os.environ, {'OPENAI_API_KEY': 'test-key'})
    def test_ai_service_initialization(self, mock_llm):
        """Test that AIService can be initialized."""
        from app.services.ai_service import AIService
        ai_service = AIService()
        assert ai_service is not None

    @patch('app.services.ai_service.ChatOpenAI')
    @patch.dict(os.environ, {'OPENAI_API_KEY': 'test-key'})
    def test_ai_service_has_llm(self, mock_llm):
        """Test that AIService has LLM initialized."""
        from app.services.ai_service import AIService
        ai_service = AIService()
        assert hasattr(ai_service, 'llm')
