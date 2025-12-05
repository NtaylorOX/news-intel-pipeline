"""Endpoints for analytics and insights from ingested articles."""

import json
import re
import logging
from collections import Counter
from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session
from sqlalchemy import func
from datetime import datetime, timedelta
from typing import List

from app.core.database import get_db, NewsArticle
from app.services.ai_service import ai_service

logger = logging.getLogger(__name__)

router = APIRouter(tags=["insights"])


def extract_keywords(text: str, min_length: int = 4) -> list:
    """Extract meaningful keywords from text."""
    # Remove common words and punctuation
    stop_words = {
        "the", "a", "an", "and", "or", "but", "in", "on", "at", "to", "for",
        "of", "with", "is", "are", "be", "by", "from", "as", "that", "this",
        "which", "who", "will", "have", "has", "had", "do", "does", "did",
        "can", "could", "should", "would", "may", "might", "must", "shall",
        "using", "new", "said", "says", "get", "got", "make", "made"
    }
    
    # Tokenize and clean
    words = re.findall(r'\b\w+\b', text.lower())
    keywords = [w for w in words if len(w) >= min_length and w not in stop_words]
    return keywords


@router.get("/summary")
async def get_insights_summary(db: Session = Depends(get_db)):
    """Get high-level summary of ingested articles."""
    total_articles = db.query(NewsArticle).count()
    total_analyzed = db.query(NewsArticle).filter(NewsArticle.is_analyzed == True).count()
    
    # Get source breakdown
    source_counts = (
        db.query(NewsArticle.source, func.count(NewsArticle.id))
        .group_by(NewsArticle.source)
        .all()
    )

    # Get filter pass rate
    total_filtered = db.query(NewsArticle).filter(NewsArticle.passed_filter == True).count()
    filter_pass_rate = (total_filtered / total_articles * 100) if total_articles > 0 else 0

    return {
        "total_articles": total_articles,
        "total_analyzed": total_analyzed,
        "analysis_rate": f"{(total_analyzed / total_articles * 100):.1f}%" if total_articles > 0 else "0%",
        "filter_pass_rate": f"{filter_pass_rate:.1f}%",
        "sources": {source: count for source, count in source_counts},
    }


@router.get("/topics")
async def get_top_topics(limit: int = 20, db: Session = Depends(get_db)):
    """Get most common topics/keywords across articles."""
    articles = db.query(NewsArticle.title, NewsArticle.content).filter(
        NewsArticle.passed_filter == True
    ).all()

    if not articles:
        return {"topics": []}

    # Extract all keywords
    all_keywords = []
    for title, content in articles:
        all_keywords.extend(extract_keywords(title, min_length=5))
        all_keywords.extend(extract_keywords(content, min_length=6))

    # Count keyword frequencies
    keyword_counts = Counter(all_keywords)
    top_topics = keyword_counts.most_common(limit)

    return {
        "total_unique_topics": len(keyword_counts),
        "topics": [{"keyword": keyword, "count": count} for keyword, count in top_topics],
    }


@router.get("/sources")
async def get_source_analytics(db: Session = Depends(get_db)):
    """Get detailed analytics per source."""
    sources = db.query(NewsArticle.source).distinct().all()
    source_names = [s[0] for s in sources]

    analytics = {}
    for source_name in source_names:
        total = db.query(NewsArticle).filter(NewsArticle.source == source_name).count()
        analyzed = db.query(NewsArticle).filter(
            (NewsArticle.source == source_name) & (NewsArticle.is_analyzed == True)
        ).count()
        filtered = db.query(NewsArticle).filter(
            (NewsArticle.source == source_name) & (NewsArticle.passed_filter == True)
        ).count()
        duplicates = db.query(NewsArticle).filter(
            (NewsArticle.source == source_name) & (NewsArticle.is_duplicate == True)
        ).count()

        analytics[source_name] = {
            "total_fetched": total,
            "analyzed": analyzed,
            "passed_filter": filtered,
            "duplicates": duplicates,
            "analysis_rate": f"{(analyzed / total * 100):.1f}%" if total > 0 else "0%",
        }

    return analytics


@router.get("/trends")
async def get_trends(days: int = 7, db: Session = Depends(get_db)):
    """Get article trends over time."""
    from datetime import datetime, timedelta

    if days < 1:
        days = 7

    cutoff_date = datetime.utcnow() - timedelta(days=days)

    # Get articles by day
    articles = db.query(
        func.date(NewsArticle.fetched_at).label("date"),
        func.count(NewsArticle.id).label("count")
    ).filter(
        NewsArticle.fetched_at >= cutoff_date
    ).group_by(
        func.date(NewsArticle.fetched_at)
    ).order_by(
        func.date(NewsArticle.fetched_at)
    ).all()

    trend_data = [{"date": str(date), "articles": count} for date, count in articles]

    return {
        "period_days": days,
        "trend_data": trend_data,
        "total_articles": sum(count for _, count in articles),
    }


@router.get("/analysis-quality")
async def get_analysis_quality(db: Session = Depends(get_db)):
    """Get metrics on analysis results."""
    total_articles = db.query(NewsArticle).count()
    analyzed = db.query(NewsArticle).filter(NewsArticle.is_analyzed == True).count()
    failed_analysis = db.query(NewsArticle).filter(
        (NewsArticle.is_analyzed == False) & (NewsArticle.passed_filter == True)
    ).count()

    # Sample analysis results to check average length
    sample_analyses = db.query(NewsArticle.analysis_result).filter(
        NewsArticle.analysis_result.isnot(None)
    ).limit(100).all()

    avg_analysis_length = 0
    if sample_analyses:
        lengths = []
        for (analysis_str,) in sample_analyses:
            if analysis_str:
                try:
                    analysis = json.loads(analysis_str)
                    if "analysis" in analysis:
                        lengths.append(len(analysis["analysis"]))
                except json.JSONDecodeError:
                    pass
        avg_analysis_length = sum(lengths) / len(lengths) if lengths else 0

    return {
        "total_articles": total_articles,
        "analyzed": analyzed,
        "pending_analysis": failed_analysis,
        "analysis_rate": f"{(analyzed / total_articles * 100):.1f}%" if total_articles > 0 else "0%",
        "avg_analysis_length": int(avg_analysis_length),
    }


@router.get("/export/csv")
async def export_articles_csv(
    analyzed_only: bool = False,
    source: str = None,
    db: Session = Depends(get_db)
):
    """Export articles as CSV format.

    Query parameters:
    - analyzed_only: Only export analyzed articles
    - source: Filter by source name
    """
    query = db.query(NewsArticle)

    if analyzed_only:
        query = query.filter(NewsArticle.is_analyzed == True)

    if source:
        query = query.filter(NewsArticle.source == source)

    articles = query.order_by(NewsArticle.fetched_at.desc()).all()

    # Build CSV
    csv_lines = ["ID,Title,Source,URL,Published,Fetched,IsAnalyzed,PassedFilter"]

    for article in articles:
        # Escape quotes in title
        title = article.title.replace('"', '""')
        published = article.published_at.isoformat() if article.published_at else ""
        fetched = article.fetched_at.isoformat() if article.fetched_at else ""
        
        csv_lines.append(
            f'{article.id},"{title}",{article.source},{article.url},'
            f'{published},{fetched},{article.is_analyzed},{article.passed_filter}'
        )

    return {
        "format": "csv",
        "count": len(articles),
        "data": "\n".join(csv_lines),
    }


@router.get("/action-items")
async def generate_action_items(
    hours: int = 24,
    max_articles: int = 10,
    db: Session = Depends(get_db)
):
    """Generate AI-powered action items from recent filtered news articles.
    
    This endpoint analyzes recently ingested articles that passed the content filter
    and generates actionable insights and recommendations based on the news trends.
    
    Query parameters:
    - hours: Look back this many hours for recent articles (default: 24)
    - max_articles: Maximum number of articles to analyze (default: 10)
    
    Returns:
    - action_items: List of prioritized action items
    - news_summary: Brief summary of the news analyzed
    - key_trends: Main trends identified
    - metadata: Number of articles analyzed and time period
    """
    try:
        # Get recent articles that passed the filter
        cutoff_time = datetime.utcnow() - timedelta(hours=hours)
        
        recent_articles = db.query(
            NewsArticle.id,
            NewsArticle.title,
            NewsArticle.content,
            NewsArticle.summary,
            NewsArticle.fetched_at
        ).filter(
            (NewsArticle.passed_filter == True) &
            (NewsArticle.fetched_at >= cutoff_time)
        ).order_by(
            NewsArticle.fetched_at.desc()
        ).limit(max_articles).all()
        
        if not recent_articles:
            return {
                "action_items": [],
                "news_summary": "No recent articles found in the specified time period.",
                "key_trends": [],
                "metadata": {
                    "articles_analyzed": 0,
                    "time_period_hours": hours,
                    "cutoff_time": cutoff_time.isoformat()
                }
            }
        
        # Compile article summaries
        article_summaries = []
        for article_id, title, content, summary, fetched_at in recent_articles:
            article_text = f"**{title}**\n"
            if summary:
                article_text += f"Summary: {summary}\n"
            else:
                # Use first 300 chars of content if no summary
                article_text += f"Content: {content[:300]}...\n"
            article_summaries.append(article_text)
        
        # Build prompt for AI analysis
        articles_text = "\n\n".join(article_summaries)
        prompt = f"""Based on the following recent news articles from the tech/IT industry, generate actionable insights and recommendations.

NEWS ARTICLES:
{articles_text}

Please provide:
1. **Action Items**: 3-5 specific, prioritized action items that an organization should consider based on these trends
2. **Key Trends**: 2-3 major trends identified across these articles
3. **Risk Assessment**: Any risks or threats mentioned that need attention
4. **Opportunities**: New opportunities or areas to explore

Format your response as structured JSON with keys: action_items (list), key_trends (list), risks (list), opportunities (list).
Each action item should have: priority (HIGH/MEDIUM/LOW), description, and rationale."""
        
        # Call AI service for analysis
        logger.info(f"Analyzing {len(recent_articles)} recent articles for action items...")
        ai_result = ai_service.generate_action_items(articles_text)
        
        # Extract results (already properly formatted by the new method)
        action_items = ai_result.get("action_items", [])
        key_trends = ai_result.get("key_trends", [])
        risks = ai_result.get("risks", [])
        opportunities = ai_result.get("opportunities", [])
        
        # Generate brief summary
        news_summary = f"Analyzed {len(recent_articles)} recent tech/IT news articles from the past {hours} hours. "
        news_summary += f"Identified key trends and opportunities for organizational focus."
        
        return {
            "action_items": action_items,
            "news_summary": news_summary,
            "key_trends": key_trends,
            "risks": risks,
            "opportunities": opportunities,
            "metadata": {
                "articles_analyzed": len(recent_articles),
                "time_period_hours": hours,
                "cutoff_time": cutoff_time.isoformat(),
                "generated_at": datetime.utcnow().isoformat()
            }
        }
        
    except Exception as e:
        logger.error(f"Error generating action items: {e}")
        return {
            "action_items": [],
            "news_summary": f"Error analyzing articles: {str(e)}",
            "key_trends": [],
            "risks": [],
            "opportunities": [],
            "metadata": {
                "error": str(e),
                "time_period_hours": hours
            }
        }
