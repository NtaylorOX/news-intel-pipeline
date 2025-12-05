"""AI Service for news analysis and processing using modern LangChain."""

from langchain_core.messages import HumanMessage, SystemMessage
from langchain_core.output_parsers import PydanticOutputParser
from langchain_openai import ChatOpenAI
from pydantic import BaseModel, Field
from typing import List, Literal
import json
import logging
import yaml
from pathlib import Path

from app.core.prompts import SYSTEM_PROMPTS, USER_PROMPTS

logger = logging.getLogger(__name__)


# Pydantic models for structured output
class RelevanceClassification(BaseModel):
    """Schema for article relevance classification."""
    is_relevant: bool = Field(description="Whether the article is relevant to IT professionals")
    relevance_score: float = Field(ge=0.0, le=1.0, description="Relevance score between 0 and 1")
    quality_score: float = Field(ge=0.0, le=1.0, description="Quality score between 0 and 1")
    reason: str = Field(description="Brief explanation of the classification")


class ActionItem(BaseModel):
    """Schema for a single action item."""
    priority: Literal["HIGH", "MEDIUM", "LOW"] = Field(description="Priority level")
    description: str = Field(description="Description of the action item")


class ActionItemsResponse(BaseModel):
    """Schema for action items generation response."""
    action_items: List[ActionItem] = Field(description="List of actionable items")
    key_trends: List[str] = Field(description="Key trends identified")
    risks: List[str] = Field(description="Identified risks")
    opportunities: List[str] = Field(description="Identified opportunities")



def load_model_config():
    """Load AI model configuration from YAML file.
    
    Returns:
        dict: Model configuration settings
    """
    config_path = Path(__file__).parent.parent / "core" / "ai_config.yaml"
    
    if not config_path.exists():
        # Fallback to default config if file doesn't exist
        logger.warning(f"AI config file not found at {config_path}, using defaults")
        return {
            "provider": "openai",
            "model_name": "gpt-3.5-turbo",
            "temperature": 0.7,
            "max_tokens": None,
        }
    
    with open(config_path, 'r') as f:
        config = yaml.safe_load(f)
    
    return config


class AIService:
    """Service for AI-powered news analysis using configurable LLM."""

    def __init__(self):
        """Initialize AI service with configured LLM."""
        self.config = load_model_config()
        self.system_prompts = SYSTEM_PROMPTS
        self.user_prompts = USER_PROMPTS
        
        # Initialize LLM based on configuration
        self.llm = ChatOpenAI(
            model=self.config.get("model_name", "gpt-4o-mini"),
            temperature=self.config.get("temperature", 0.7),
        )
        
        # Initialize structured output parsers
        self.relevance_parser = PydanticOutputParser(pydantic_object=RelevanceClassification)
        self.action_items_parser = PydanticOutputParser(pydantic_object=ActionItemsResponse)
        
        logger.info(f"AIService initialized with model: {self.config.get('model_name')}")

    def _invoke_llm(self, system_prompt_key: str, content: str) -> str:
        """
        Invoke LLM with system and user prompts.
        
        Args:
            system_prompt_key: Key to look up system prompt
            content: Content to analyze
            
        Returns:
            LLM response as string
        """
        system_text = self.system_prompts.get(system_prompt_key, "You are a helpful assistant.")
        user_template = self.user_prompts.get(system_prompt_key, "{content}")
        user_text = user_template.format(content=content)
        
        conversation = [
            SystemMessage(system_text),
            HumanMessage(user_text),
        ]
        
        response = self.llm.invoke(conversation)
        return response.content

    def analyze_news(self, article_content: str) -> dict:
        """Analyze news article using configured AI with conversation history."""
        analysis_text = self._invoke_llm("article_analyzer", article_content)
        return {
            "analysis": analysis_text,
            "model": self.config.get("model_name"),
        }

    def summarize_news(self, article_content: str) -> dict:
        """Summarize news article using configured AI."""
        summary_text = self._invoke_llm("article_summarizer", article_content)
        return {
            "summary": summary_text,
            "model": self.config.get("model_name"),
        }

    def classify_relevance(self, article_content: str) -> dict:
        """
        Classify article for IT/tech relevance using configured AI with structured output.
        
        Returns a dict with:
        - is_relevant: bool
        - relevance_score: float (0-1)
        - quality_score: float (0-1)
        - reason: str
        """
        try:
            # Add format instructions to the prompt
            system_text = self.system_prompts.get("relevance_classifier", "You are a helpful assistant.")
            user_template = self.user_prompts.get("relevance_classifier", "{content}")
            
            # Include schema instructions
            format_instructions = self.relevance_parser.get_format_instructions()
            user_text = user_template.format(content=article_content) + f"\n\n{format_instructions}"
            
            conversation = [
                SystemMessage(system_text),
                HumanMessage(user_text),
            ]
            
            response = self.llm.invoke(conversation)
            parsed: RelevanceClassification = self.relevance_parser.parse(response.content)
            
            return {
                "is_relevant": parsed.is_relevant,
                "relevance_score": parsed.relevance_score,
                "quality_score": parsed.quality_score,
                "reason": parsed.reason,
                "raw": response.content,
            }
        except Exception as e:
            logger.warning(f"Failed to parse relevance response with structured output: {e}")
            logger.debug(f"Error details: {str(e)}")
            return {
                "is_relevant": False,
                "relevance_score": 0.0,
                "quality_score": 0.0,
                "reason": f"Could not parse model output: {str(e)}",
                "raw": "",
            }

    def generate_action_items(self, articles_text: str) -> dict:
        """Generate action items and insights from news articles using structured output."""
        try:
            # Add format instructions to the prompt
            system_text = self.system_prompts.get("action_items_generator", "You are a helpful assistant.")
            user_template = self.user_prompts.get("action_items_generator", "{content}")
            
            # Include schema instructions
            format_instructions = self.action_items_parser.get_format_instructions()
            user_text = user_template.format(content=articles_text) + f"\n\n{format_instructions}"
            
            conversation = [
                SystemMessage(system_text),
                HumanMessage(user_text),
            ]
            
            response = self.llm.invoke(conversation)
            parsed: ActionItemsResponse = self.action_items_parser.parse(response.content)
            
            return {
                "action_items": [item.dict() for item in parsed.action_items],
                "key_trends": parsed.key_trends,
                "risks": parsed.risks,
                "opportunities": parsed.opportunities,
                "raw": response.content,
            }
        except Exception as e:
            logger.warning(f"Failed to parse action items response with structured output: {e}")
            logger.debug(f"Error details: {str(e)}")
            return {
                "action_items": [{"priority": "MEDIUM", "description": f"Unable to parse AI response: {str(e)}"}],
                "key_trends": [],
                "risks": [],
                "opportunities": [],
                "error": str(e),
                "raw": "",
            }


# Global instance
ai_service = AIService()
