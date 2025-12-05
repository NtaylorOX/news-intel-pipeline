"""Service for filtering articles based on quality and relevance criteria."""

import logging
from typing import Dict, Tuple
import logging
import json
from app.core.config import settings
from app.services.ai_service import ai_service

logger = logging.getLogger(__name__)


class ContentFilterService:
    """Service for filtering articles by relevance and quality."""

    # IT/Tech keywords for relevance checking
    TECH_KEYWORDS = {
        # AI/ML
        "ai",
        "artificial intelligence",
        "machine learning",
        "deep learning",
        "neural network",
        "llm",
        "gpt",
        "chatgpt",
        "transformers",
        # Cloud & Infrastructure
        "cloud",
        "aws",
        "azure",
        "gcp",
        "kubernetes",
        "docker",
        "devops",
        "infrastructure",
        "terraform",
        "ansible",
        # Security
        "security",
        "cybersecurity",
        "hack",
        "breach",
        "vulnerability",
        "encryption",
        "zero trust",
        # Programming & Development
        "python",
        "javascript",
        "rust",
        "golang",
        "typescript",
        "react",
        "vue",
        "web development",
        "api",
        "microservices",
        "database",
        "postgres",
        "mongodb",
        # Blockchain & Web3
        "blockchain",
        "crypto",
        "web3",
        "defi",
        "nft",
        # Productivity & Business Tech
        "startup",
        "saas",
        "open source",
        "github",
        "devtool",
        # Data & Analytics
        "analytics",
        "big data",
        "data science",
        "data pipeline",
        "dataops",
        # Other IT topics
        "quantum",
        "edge computing",
        "iot",
        "5g",
    }

    # Minimum content length to be considered substantial
    MIN_CONTENT_LENGTH = 100

    # Maximum content length (articles much longer may be paywalled extracts)
    MAX_CONTENT_LENGTH = 50000

    @staticmethod
    def is_relevant(title: str, content: str) -> Tuple[bool, str]:
        """Check if article is relevant to IT/tech topics.

        Args:
            title: Article title
            content: Article content

        Returns:
            Tuple of (is_relevant: bool, reason: str)
        """
        combined_text = f"{title} {content}".lower()

        # Check for tech keywords
        keyword_matches = sum(1 for keyword in ContentFilterService.TECH_KEYWORDS 
                             if keyword in combined_text)

        if keyword_matches > 0:
            return (True, f"Found {keyword_matches} tech keyword(s)")

        return (False, "No tech keywords found")

    @staticmethod
    def is_quality(title: str, content: str) -> Tuple[bool, str]:
        """Check if article meets quality thresholds.

        Args:
            title: Article title
            content: Article content

        Returns:
            Tuple of (passes_quality: bool, reason: str)
        """
        # Check title
        if not title or len(title.strip()) < 10:
            return (False, "Title too short or empty")

        # Check content length
        if not content or len(content) < ContentFilterService.MIN_CONTENT_LENGTH:
            return (False, f"Content too short (min {ContentFilterService.MIN_CONTENT_LENGTH} chars)")

        if len(content) > ContentFilterService.MAX_CONTENT_LENGTH:
            return (False, f"Content too long (max {ContentFilterService.MAX_CONTENT_LENGTH} chars)")

        # Check for common spam patterns
        spam_patterns = [
            "[removed]",
            "[deleted]",
            "click here",
            "sponsored",
            "advertisement",
        ]

        text_lower = f"{title} {content}".lower()
        if any(pattern in text_lower for pattern in spam_patterns):
            return (False, "Detected spam patterns")

        # Check for reasonable sentence structure
        sentences = content.split(".")
        avg_sentence_length = len(content.split()) / max(len(sentences), 1)

        if avg_sentence_length < 2:  # Too many very short sentences
            return (False, "Content appears fragmented")

        return (True, "Quality checks passed")

    @staticmethod
    def filter_article(title: str, content: str) -> Tuple[bool, Dict[str, any]]:
        """Apply all filters to an article.

        Args:
            title: Article title
            content: Article content

        Returns:
            Tuple of (passes_all_filters: bool, details: dict with filter results)
        """
        logger.info(f"🔍 Filtering article: title_len={len(title)}, content_len={len(content)}")
        
        details = {
            "passed": False,
            "filters": {},
        }

        # Check relevance
        is_relevant, relevance_reason = ContentFilterService.is_relevant(title, content)
        details["filters"]["relevance"] = {
            "passed": is_relevant,
            "reason": relevance_reason,
        }

        # Check quality
        is_quality, quality_reason = ContentFilterService.is_quality(title, content)
        details["filters"]["quality"] = {
            "passed": is_quality,
            "reason": quality_reason,
        }

        # Populate heuristic results
        details["filters"]["relevance"]["passed"] = is_relevant
        details["filters"]["quality"]["passed"] = is_quality

        # Default numeric scores
        details["relevance_score"] = 1.0 if is_relevant else 0.0
        details["quality_score"] = 1.0 if is_quality else 0.0

        # Optionally augment with AI-based classifier when enabled
        # BUT: Basic quality checks (length, spam) must pass BEFORE AI evaluation
        if getattr(settings, "AI_FILTER_ENABLED", False) and is_quality:
            try:
                logger.info("Invoking AI classifier...")
                ai_result = ai_service.classify_relevance(f"{title}\n\n{content}")
                # Merge AI results into details
                details["ai"] = ai_result
                # Use AI scores for relevance/quality scoring
                details["relevance_score"] = float(ai_result.get("relevance_score", details["relevance_score"]))
                details["quality_score"] = float(ai_result.get("quality_score", details["quality_score"]))
                
                logger.info(f"AI scores: relevance={details['relevance_score']:.2f}, quality={details['quality_score']:.2f}, heuristic_relevant={is_relevant}, heuristic_quality={is_quality}")
                logger.info(f"AI scores: relevance={details['relevance_score']:.2f}, quality={details['quality_score']:.2f}, heuristic_relevant={is_relevant}, heuristic_quality={is_quality}")
                
                # RULE 1: If heuristics detected NO tech keywords but AI gives high relevance,
                # require VERY high AI confidence (>0.8) to override
                if not is_relevant and details["relevance_score"] >= 0.3:
                    # Non-tech heuristic but AI sees relevance - be very conservative
                    if details["relevance_score"] < 0.8:
                        logger.warning(f"RULE 1 REJECTION: No keywords + AI score {details['relevance_score']:.2f} < 0.8")
                        details["filters"]["relevance"]["passed"] = False
                        details["filters"]["relevance"]["reason"] = (
                            f"No tech keywords found (AI score too low: {details['relevance_score']:.2f})"
                        )
                    else:
                        # Very high AI confidence (>0.8) can override no-keyword verdict
                        logger.info(f"RULE 1 OVERRIDE: No keywords but AI score {details['relevance_score']:.2f} >= 0.8")
                        details["filters"]["relevance"]["passed"] = True
                        details["filters"]["relevance"]["reason"] = (
                            f"No keywords but strong AI relevance signal ({details['relevance_score']:.2f})"
                        )
                # RULE 2: If heuristics DID find keywords but AI disagrees strongly, be skeptical
                elif is_relevant and details["relevance_score"] < 0.3:
                    logger.warning(f"RULE 2 REJECTION: Has keywords but AI score {details['relevance_score']:.2f} < 0.3")
                    details["filters"]["relevance"]["passed"] = False
                    details["filters"]["relevance"]["reason"] += " (low AI confidence)"
                
                # RULE 3: If AI quality is very low, reject
                if details["quality_score"] < 0.3:
                    logger.warning(f"RULE 3 REJECTION: AI quality score {details['quality_score']:.2f} < 0.3")
                    details["filters"]["quality"]["passed"] = False
                    details["filters"]["quality"]["reason"] = f"AI quality check failed (score: {details['quality_score']:.2f})"
                else:
                    # AI quality passed - update the reason
                    details["filters"]["quality"]["passed"] = True
                    details["filters"]["quality"]["reason"] = f"AI quality check passed (score: {details['quality_score']:.2f})"
            except Exception as e:
                logging.getLogger(__name__).warning(f"AI filter invocation failed: {e}")

        # Overall result: when AI is enabled, trust AI scores over heuristics for quality
        # (heuristics can't judge scraped HTML content quality reliably)
        if getattr(settings, "AI_FILTER_ENABLED", False):
            # AI-based decision: use AI-modified filter results
            details["passed"] = bool(details["filters"]["relevance"]["passed"] and details["filters"]["quality"]["passed"])
        else:
            # Heuristic-only decision: both must pass
            details["passed"] = bool(details["filters"]["relevance"]["passed"] and details["filters"]["quality"]["passed"])

        return (details["passed"], details)

    @staticmethod
    def get_filter_stats(filters_result: Dict) -> str:
        """Get human-readable filter result summary.

        Args:
            filters_result: Dictionary returned from filter_article()

        Returns:
            Human-readable summary string
        """
        passed = filters_result["passed"]
        filters = filters_result["filters"]

        status = "✓ PASSED" if passed else "✗ REJECTED"

        reasons = []
        for filter_name, filter_result in filters.items():
            if not filter_result["passed"]:
                reasons.append(f"{filter_name}: {filter_result['reason']}")

        if reasons:
            return f"{status} - {'; '.join(reasons)}"
        return f"{status} - All filters passed"
