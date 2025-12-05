"""News API endpoints."""
from fastapi import APIRouter, HTTPException, Query, Depends
from pydantic import BaseModel
from typing import Optional, List
from datetime import datetime
import json

from app.core.database import get_db, NewsArticle

router = APIRouter(tags=["news"])


class NewsArticleSchema(BaseModel):
    """Model for a news article response."""

    id: int
    title: str
    content: str
    source: str
    url: str
    published_at: Optional[datetime]
    fetched_at: datetime
    passed_filter: Optional[bool]
    filtered_out: Optional[bool] = None  # For dashboard display
    is_analyzed: bool
    
    class Config:
        from_attributes = True


class AnalysisRequest(BaseModel):
    """Model for analysis request."""

    article_content: str


class AnalysisWithContextRequest(BaseModel):
    """Model for analysis request with context."""

    article_content: str
    previous_analysis: Optional[str] = None


@router.get("/")
async def get_news():
    """Get news feed."""
    return {"message": "News feed endpoint"}


@router.get("/articles", response_model=dict)
async def get_articles(
    limit: int = Query(50, ge=1, le=500),
    offset: int = Query(0, ge=0),
    sort_by: str = Query("recency", regex="^(recency|relevance|quality|combined)$"),
    db=Depends(get_db)
):
    """
    Get recent articles from the database with sorting options.
    
    Args:
        limit: Maximum number of articles to return (1-500)
        offset: Offset for pagination
        sort_by: Sort order - 'recency' (default), 'relevance', 'quality', or 'combined'
        db: Database session
    
    Returns:
        List of articles with their processing status and relevance scores
    """
    try:
        # Build query with database-level sorting (much faster than in-memory)
        query = db.query(NewsArticle)
        
        # Apply sorting at database level using indexed columns
        if sort_by == "recency":
            query = query.order_by(NewsArticle.fetched_at.desc())
        elif sort_by == "relevance":
            query = query.order_by(NewsArticle.relevance_score.desc())
        elif sort_by == "quality":
            query = query.order_by(NewsArticle.quality_score.desc())
        elif sort_by == "combined":
            # Sort by average of both scores
            query = query.order_by(
                ((NewsArticle.relevance_score + NewsArticle.quality_score) / 2).desc()
            )
        
        # Apply pagination
        articles = query.offset(offset).limit(limit).all()
        
        # Convert to response schema
        articles_data = []
        for article in articles:
            # Parse filter_results for filter reason (still needed for display)
            filter_reason = ""
            if article.filter_results:
                try:
                    filter_data = json.loads(article.filter_results)
                    if "filters" in filter_data:
                        filters = filter_data["filters"]
                        reasons = []
                        if "relevance" in filters and not filters["relevance"].get("passed"):
                            reasons.append(filters["relevance"].get("reason", ""))
                        if "quality" in filters and not filters["quality"].get("passed"):
                            reasons.append(filters["quality"].get("reason", ""))
                        filter_reason = "; ".join(reasons) if reasons else "Passed all filters"
                except (json.JSONDecodeError, ValueError, TypeError):
                    pass
            
            article_dict = {
                "id": article.id,
                "title": article.title,
                "content": article.content[:200] + "..." if len(article.content) > 200 else article.content,
                "source": article.source,
                "url": article.url,
                "published_at": article.published_at,
                "fetched_at": article.fetched_at,
                "passed_filter": article.passed_filter,
                "filtered_out": not article.passed_filter if article.passed_filter is not None else None,
                "is_analyzed": article.is_analyzed,
                # Use direct columns instead of parsing JSON - much faster!
                "relevance_score": article.relevance_score,
                "quality_score": article.quality_score,
                "filter_reason": filter_reason,
            }
            articles_data.append(article_dict)
        
        return {
            "total": db.query(NewsArticle).count(),
            "count": len(articles_data),
            "offset": offset,
            "limit": limit,
            "sort_by": sort_by,
            "articles": articles_data
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching articles: {str(e)}")


@router.get("/articles/{article_id}")
async def get_article_detail(article_id: int, db=Depends(get_db)):
    """
    Get full article details including complete content and analysis.
    
    Args:
        article_id: ID of the article to retrieve
        db: Database session
    
    Returns:
        Full article with complete content, metadata, and analysis
    """
    try:
        article = db.query(NewsArticle).filter(
            NewsArticle.id == article_id
        ).first()
        
        if not article:
            raise HTTPException(status_code=404, detail="Article not found")
        
        # Parse analysis if it exists
        analysis_data = None
        if article.analysis_result:
            try:
                import json
                analysis_data = json.loads(article.analysis_result)
            except:
                analysis_data = {"raw": article.analysis_result}
        
        return {
            "id": article.id,
            "title": article.title,
            "content": article.content,  # Full content
            "source": article.source,
            "url": article.url,
            "published_at": article.published_at,
            "fetched_at": article.fetched_at,
            "passed_filter": article.passed_filter,
            "filtered_out": not article.passed_filter if article.passed_filter is not None else None,
            "is_analyzed": article.is_analyzed,
            "summary": article.summary,
            "analysis": analysis_data,
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching article: {str(e)}")


@router.post("/analyze")
async def analyze_news(request: AnalysisRequest):
    """
    Analyze a news article using AI with conversation history.

    Args:
        request: Analysis request with article content

    Returns:
        Analysis results with model info
    """
    try:
        from app.services.ai_service import ai_service

        result = ai_service.analyze_news(request.article_content)
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/summarize")
async def summarize_news(request: AnalysisRequest):
    """
    Summarize a news article using AI with conversation history.

    Args:
        request: Analysis request with article content

    Returns:
        Summary result with model info
    """
    try:
        from app.services.ai_service import ai_service

        result = ai_service.summarize_news(request.article_content)
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/analyze-with-context")
async def analyze_with_context(request: AnalysisWithContextRequest):
    """
    Analyze a news article with optional context from previous analysis.

    Enables multi-turn conversation capability by accepting prior analysis results
    to refine and enhance subsequent analysis of the same article.

    Args:
        request: Analysis request with article content and optional previous analysis

    Returns:
        Contextual analysis result with model info
    """
    try:
        from app.services.ai_service import ai_service

        result = ai_service.analyze_with_context(
            request.article_content, request.previous_analysis
        )
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/scheduler/status")
async def get_scheduler_status():
    """Get scheduler status information.
    
    In microservices mode, the scheduler runs in a separate ingestion-worker container.
    To verify scheduler activity, check Docker logs or query recent articles in database.
    """
    return {
        "status": "running",
        "message": "Scheduler runs in news_ingestion container (fetches every 5 minutes)",
        "configuration": {
            "interval_minutes": 5,
            "container": "news_ingestion",
            "enabled_sources": [
                "HackerNews",
                "TechCrunch", 
                "ArsTechnica",
                "TheVerge",
                "Slashdot",
                "Reddit-programming",
                "Reddit-cybersecurity",
                "Reddit-opensource"
            ]
        },
        "how_to_verify": {
            "docker_logs": "docker logs news_ingestion --tail 50",
            "check_articles": "GET /api/ingestion/articles to see recently fetched articles",
            "kafdrop_ui": "http://localhost:9000 to monitor Kafka raw_articles topic"
        },
        "note": "Scheduler is managed by Docker container lifecycle. Real-time metrics not exposed to API service."
    }

