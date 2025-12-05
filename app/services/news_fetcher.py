"""Service for fetching news from multiple sources."""

import hashlib
import requests
import feedparser
from datetime import datetime
from typing import List, Dict, Optional
from abc import ABC, abstractmethod
import logging
from newspaper import Article
from app.core.source_config import get_all_configs

logger = logging.getLogger(__name__)


class FetchedArticle:
    """Data class for normalized news article from fetchers."""

    def __init__(
        self,
        title: str,
        content: str,
        url: str,
        source: str,
        published_at: Optional[datetime] = None,
    ):
        self.title = title
        self.content = content
        self.url = url
        self.source = source
        self.published_at = published_at or datetime.utcnow()
        self.content_hash = self._compute_hash()

    def _compute_hash(self) -> str:
        """Compute MD5 hash of normalized content for deduplication."""
        # Normalize by combining URL (more reliable than content)
        normalized = f"{self.source}:{self.url}".lower().strip()
        return hashlib.md5(normalized.encode()).hexdigest()

    def to_dict(self) -> Dict:
        """Convert to dictionary for database storage."""
        return {
            "title": self.title,
            "content": self.content,
            "url": self.url,
            "source": self.source,
            "published_at": self.published_at,
            "content_hash": self.content_hash,
        }


class NewsSource(ABC):
    """Base class for news source connectors."""

    def fetch(self) -> List[FetchedArticle]:
        """Fetch articles from this source."""
        pass

    @property
    @abstractmethod
    def source_name(self) -> str:
        """Return the name of this news source."""
        pass


class HackerNewsSource(NewsSource):
    """Connector for HackerNews API."""

    BASE_URL = "https://hacker-news.firebaseio.com/v0"
    STORY_LIMIT = 30  # Fetch top 30 stories

    @property
    def source_name(self) -> str:
        return "HackerNews"

    def fetch(self) -> List[FetchedArticle]:
        """Fetch top stories from HackerNews."""
        articles = []

        try:
            # Get top story IDs
            url = f"{self.BASE_URL}/topstories.json"
            response = requests.get(url, timeout=10)
            response.raise_for_status()
            story_ids = response.json()[: self.STORY_LIMIT]

            # Fetch details for each story
            for story_id in story_ids:
                try:
                    story_url = f"{self.BASE_URL}/item/{story_id}.json"
                    story_response = requests.get(story_url, timeout=5)
                    story_response.raise_for_status()
                    story = story_response.json()

                    # Filter for IT/tech related stories
                    if self._is_tech_related(story):
                        article = self._story_to_article(story)
                        if article:
                            articles.append(article)
                except Exception as e:
                    logger.warning(f"Failed to fetch HackerNews story {story_id}: {e}")
                    continue

        except Exception as e:
            logger.error(f"Error fetching HackerNews: {e}")

        return articles

    def _is_tech_related(self, story: Dict) -> bool:
        """Filter stories for IT/tech relevance."""
        tech_keywords = [
            "ai",
            "machine learning",
            "cloud",
            "devops",
            "kubernetes",
            "docker",
            "python",
            "javascript",
            "rust",
            "golang",
            "security",
            "hack",
            "breach",
            "crypto",
            "blockchain",
            "startup",
            "ycombinator",
            "postgres",
            "database",
            "api",
            "llm",
            "gpt",
            "neural",
        ]

        title = (story.get("title") or "").lower()
        # Simple keyword matching
        return any(keyword in title for keyword in tech_keywords)

    def _story_to_article(self, story: Dict) -> Optional[FetchedArticle]:
        """Convert HackerNews story to FetchedArticle with full content extraction."""
        try:
            title = story.get("title", "").strip()
            url = story.get("url", "").strip()

            if not title or not url:
                return None

            # Extract full article content using newspaper3k
            content = ""
            published_at = None
            
            try:
                article = Article(url)
                article.download()
                article.parse()
                
                content = article.text
                if article.publish_date:
                    published_at = article.publish_date
                    
                logger.debug(f"Extracted {len(content)} chars from HackerNews link: {url}")
                
            except Exception as e:
                # Fall back to story text or title if extraction fails
                logger.debug(f"Failed to extract content from {url}: {e}")
                content = story.get("text", title)

            # Use HackerNews timestamp if we don't have article date
            if not published_at and "time" in story:
                try:
                    published_at = datetime.fromtimestamp(story["time"])
                except Exception:
                    pass

            # Limit content size
            if len(content) > 20000:
                content = content[:20000]

            return FetchedArticle(
                title=title,
                content=content,
                url=url,
                source=self.source_name,
                published_at=published_at,
            )
        except Exception as e:
            logger.warning(f"Error converting HackerNews story: {e}")
            return None


class RSSFeedSource(NewsSource):
    """Connector for RSS feeds."""

    def __init__(self, feed_url: str, source_name: str, keywords: Optional[List[str]] = None):
        self.feed_url = feed_url
        self._source_name = source_name
        self.keywords = keywords or []

    @property
    def source_name(self) -> str:
        return self._source_name

    def fetch(self) -> List[FetchedArticle]:
        """Fetch articles from RSS feed."""
        articles = []

        try:
            feed = feedparser.parse(self.feed_url)

            if feed.status != 200 and feed.status != 0:  # 0 = local file
                logger.warning(f"RSS feed {self._source_name} returned status {feed.status}")
                return articles

            for entry in feed.entries[:20]:  # Limit to 20 entries per feed
                try:
                    article = self._entry_to_article(entry)
                    if article:
                        articles.append(article)
                except Exception as e:
                    logger.warning(f"Error processing RSS entry from {self._source_name}: {e}")
                    continue

        except Exception as e:
            logger.error(f"Error fetching RSS feed {self._source_name}: {e}")

        return articles

    def _entry_to_article(self, entry) -> Optional[FetchedArticle]:
        """Convert RSS entry to FetchedArticle with full content extraction."""
        try:
            title = entry.get("title", "").strip()
            url = entry.get("link", "").strip()

            if not title or not url:
                return None

            # Use newspaper3k to extract full article content
            content = ""
            published_at = None
            
            try:
                article = Article(url)
                article.download()
                article.parse()
                
                # Use extracted content if available, otherwise fall back to RSS summary
                content = article.text or entry.get("summary", "").strip()
                
                # Use article's publish date if available, otherwise use feed's
                if article.publish_date:
                    published_at = article.publish_date
                    
                logger.debug(f"Extracted {len(content)} chars from {url}")
                
            except Exception as e:
                # Fall back to RSS summary if newspaper extraction fails
                logger.debug(f"Failed to extract full content from {url}: {e}")
                content = entry.get("summary", "").strip()

            # Use feed's published date if we don't have one from the article
            if not published_at and hasattr(entry, "published_parsed") and entry.published_parsed:
                try:
                    published_at = datetime(*entry.published_parsed[:6])
                except Exception:
                    pass

            # Limit content to reasonable size
            if len(content) > 20000:
                content = content[:20000]

            return FetchedArticle(
                title=title,
                content=content or title,
                url=url,
                source=self.source_name,
                published_at=published_at,
            )
        except Exception as e:
            logger.warning(f"Error converting RSS entry: {e}")
            return None


class RedditSource(NewsSource):
    """Connector for Reddit subreddits."""

    def __init__(self, subreddit: str = "programming", max_posts: int = 25):
        """Initialize Reddit source.
        
        Args:
            subreddit: Name of subreddit (without r/ prefix)
            max_posts: Maximum posts to fetch per call
        """
        self.subreddit = subreddit
        self.max_posts = max_posts
        self._source_name = f"Reddit-{subreddit}"

    @property
    def source_name(self) -> str:
        return self._source_name

    def fetch(self) -> List[FetchedArticle]:
        """Fetch top posts from subreddit."""
        articles = []

        try:
            # Reddit API endpoint for top posts (no auth needed for public data)
            url = f"https://www.reddit.com/r/{self.subreddit}/top.json"
            params = {
                "limit": self.max_posts,
                "t": "day",  # Get posts from today
            }
            
            headers = {
                "User-Agent": "NewsAggregator/1.0 (IT news aggregator)"
            }

            response = requests.get(url, params=params, headers=headers, timeout=10)
            response.raise_for_status()
            data = response.json()

            # Parse Reddit API response
            for post in data.get("data", {}).get("children", []):
                try:
                    article = self._post_to_article(post.get("data", {}))
                    if article:
                        articles.append(article)
                except Exception as e:
                    logger.warning(f"Error processing Reddit post: {e}")
                    continue

        except Exception as e:
            logger.error(f"Error fetching from Reddit r/{self.subreddit}: {e}")

        return articles

    def _post_to_article(self, post: Dict) -> Optional[FetchedArticle]:
        """Convert Reddit post to FetchedArticle with full content extraction."""
        try:
            title = post.get("title", "").strip()
            url = post.get("url", "").strip()
            
            # For self-posts, use the body as content
            if post.get("is_self", False):
                content = post.get("selftext", "").strip()
                # Use the subreddit as the link URL for self-posts
                url = f"https://reddit.com{post.get('permalink', '')}"
            else:
                # For link posts, extract full article content
                content = ""
                try:
                    article = Article(url)
                    article.download()
                    article.parse()
                    content = article.text
                    logger.debug(f"Extracted {len(content)} chars from Reddit link: {url}")
                except Exception as e:
                    logger.debug(f"Failed to extract content from Reddit link {url}: {e}")
                    content = title  # Fallback to title

            if not title or not url:
                return None

            # Convert Unix timestamp to datetime
            published_at = None
            if "created_utc" in post:
                try:
                    published_at = datetime.fromtimestamp(post["created_utc"])
                except Exception:
                    pass

            # Limit content size
            if len(content) > 20000:
                content = content[:20000]

            return FetchedArticle(
                title=title,
                content=content or title,
                url=url,
                source=self.source_name,
                published_at=published_at,
            )
        except Exception as e:
            logger.warning(f"Error converting Reddit post: {e}")
            return None


class NewsAggregator:
    """Main service for aggregating news from multiple sources."""

    def __init__(self):
        self.sources: List[NewsSource] = []
        self._initialize_sources()

    def _initialize_sources(self):
        """Set up news sources from YAML configuration."""
        configs = get_all_configs()
        
        for source_name, config in configs.items():
            # Skip disabled sources
            if not config.enabled:
                logger.info(f"Skipping disabled source: {source_name}")
                continue
            
            try:
                # Initialize source based on type from config
                if config.type == "hackernews":
                    self.sources.append(HackerNewsSource())
                    
                elif config.type == "reddit":
                    # Extract subreddit name from source_name (e.g., "Reddit-programming" -> "programming")
                    subreddit = source_name.replace("Reddit-", "")
                    self.sources.append(RedditSource(
                        subreddit=subreddit,
                        max_posts=config.max_articles
                    ))
                    
                elif config.type == "rss":
                    # Use feed_url from config
                    if not config.feed_url:
                        logger.warning(f"RSS source {source_name} has no feed_url, skipping")
                        continue
                    self.sources.append(RSSFeedSource(
                        feed_url=config.feed_url,
                        source_name=source_name
                    ))
                else:
                    logger.warning(f"Unknown source type '{config.type}' for {source_name}")
                    continue
                    
                logger.info(f"Initialized {config.type} source: {source_name} (max_articles={config.max_articles})")
                
            except Exception as e:
                logger.error(f"Failed to initialize source {source_name}: {e}")
        
        logger.info(f"Initialized {len(self.sources)} enabled sources")

    def fetch_all(self) -> Dict[str, List[FetchedArticle]]:
        """Fetch articles from all sources.

        Returns:
            Dictionary mapping source names to lists of articles.
        """
        results = {}

        for source in self.sources:
            try:
                logger.info(f"Fetching from {source.source_name}...")
                articles = source.fetch()
                results[source.source_name] = articles
                logger.info(f"  -> Fetched {len(articles)} articles from {source.source_name}")
            except Exception as e:
                logger.error(f"Error fetching from {source.source_name}: {e}")
                results[source.source_name] = []

        return results

    def add_source(self, source: NewsSource):
        """Add a custom news source."""
        self.sources.append(source)

    def get_source(self, source_name: str) -> Optional[NewsSource]:
        """Get a source by name."""
        return next(
            (s for s in self.sources if s.source_name.lower() == source_name.lower()), None
        )
