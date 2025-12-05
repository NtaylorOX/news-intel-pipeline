"""Service for detecting changes in RSS feeds efficiently."""

import hashlib
import logging
from datetime import datetime
from typing import Dict, Optional, Tuple
import requests
import feedparser

logger = logging.getLogger(__name__)


class FeedChangeDetector:
    """Detects changes in RSS feeds to avoid unnecessary full parsing."""

    def __init__(self):
        """Initialize feed change detector."""
        # Cache of feed metadata: feed_url -> {last_build_date, etag, last_check, change_count}
        self.feed_cache: Dict[str, dict] = {}

    def check_feed_changed(self, feed_url: str) -> Tuple[bool, dict]:
        """Check if RSS feed has changed since last check.

        Uses multiple strategies:
        1. ETag header from server (if supported)
        2. Last-Modified header comparison
        3. RSS feed's lastBuildDate or pubDate
        4. Content hash as fallback

        Args:
            feed_url: URL of RSS feed to check

        Returns:
            Tuple of (has_changed: bool, metadata: dict with details)
        """
        metadata = {
            "feed_url": feed_url,
            "has_changed": False,
            "reason": None,
            "checked_at": datetime.utcnow().isoformat(),
        }

        try:
            # Get HTTP headers first (lightweight)
            head_response = self._get_feed_headers(feed_url)

            if head_response is None:
                # Fall back to full fetch if HEAD request fails
                return self._check_feed_content(feed_url, metadata)

            etag = head_response.get("ETag")
            last_modified = head_response.get("Last-Modified")

            cached = self.feed_cache.get(feed_url, {})
            cached_etag = cached.get("etag")
            cached_modified = cached.get("last_modified")

            # Strategy 1: ETag changed
            if etag and cached_etag and etag != cached_etag:
                metadata["has_changed"] = True
                metadata["reason"] = f"ETag changed: {cached_etag} → {etag}"
                self._update_cache(feed_url, etag=etag, last_modified=last_modified)
                return (True, metadata)

            # Strategy 2: Last-Modified header changed
            if last_modified and cached_modified and last_modified != cached_modified:
                metadata["has_changed"] = True
                metadata["reason"] = f"Last-Modified changed: {cached_modified} → {last_modified}"
                self._update_cache(feed_url, etag=etag, last_modified=last_modified)
                return (True, metadata)

            # If we have cached values but they match, feed hasn't changed
            if (etag and cached_etag and etag == cached_etag) or \
               (last_modified and cached_modified and last_modified == cached_modified):
                metadata["has_changed"] = False
                metadata["reason"] = "Headers indicate no change"
                metadata["cache_hit"] = True
                return (False, metadata)

            # Strategy 3: Fall back to content check if no headers available
            return self._check_feed_content(feed_url, metadata)

        except Exception as e:
            logger.warning(f"Error checking feed {feed_url}: {e}")
            metadata["error"] = str(e)
            # On error, assume it changed (be conservative)
            metadata["has_changed"] = True
            metadata["reason"] = f"Error checking feed: {e}"
            return (True, metadata)

    def _get_feed_headers(self, feed_url: str) -> Optional[dict]:
        """Get feed HTTP headers without downloading full content.

        Args:
            feed_url: URL of RSS feed

        Returns:
            Dictionary of response headers or None if failed
        """
        try:
            response = requests.head(
                feed_url,
                timeout=5,
                allow_redirects=True,
                headers={"User-Agent": "Mozilla/5.0 (NewsBot)"},
            )
            if response.status_code == 200:
                return dict(response.headers)
            return None
        except Exception as e:
            logger.debug(f"HEAD request failed for {feed_url}: {e}")
            return None

    def _check_feed_content(self, feed_url: str, metadata: dict) -> Tuple[bool, dict]:
        """Check if RSS feed content has changed since last check.

        Falls back to parsing feed and comparing content hash.

        Args:
            feed_url: URL of RSS feed
            metadata: Metadata dict to update

        Returns:
            Tuple of (has_changed: bool, updated metadata)
        """
        try:
            feed = feedparser.parse(feed_url)

            # Check feed's lastBuildDate or latest entry pubDate
            last_build_date = None

            if hasattr(feed.feed, "updated_parsed") and feed.feed.updated_parsed:
                last_build_date = datetime(*feed.feed.updated_parsed[:6]).isoformat()
            elif feed.entries and hasattr(feed.entries[0], "published_parsed"):
                last_build_date = datetime(*feed.entries[0].published_parsed[:6]).isoformat()

            cached = self.feed_cache.get(feed_url, {})
            cached_build_date = cached.get("last_build_date")

            # Check if build date changed
            if last_build_date and cached_build_date:
                if last_build_date != cached_build_date:
                    metadata["has_changed"] = True
                    metadata["reason"] = f"Feed updated: {cached_build_date} → {last_build_date}"
                    self._update_cache(feed_url, last_build_date=last_build_date)
                    return (True, metadata)
                else:
                    metadata["has_changed"] = False
                    metadata["reason"] = "Feed build date unchanged"
                    metadata["cache_hit"] = True
                    return (False, metadata)

            # Strategy 4: Fallback - hash of first N entries
            if feed.entries:
                entry_hashes = [self._hash_entry(e) for e in feed.entries[:10]]
                current_hash = hashlib.md5("".join(entry_hashes).encode()).hexdigest()
                cached_hash = cached.get("content_hash")

                if cached_hash and current_hash != cached_hash:
                    metadata["has_changed"] = True
                    metadata["reason"] = "Entry content changed"
                    self._update_cache(feed_url, content_hash=current_hash, last_build_date=last_build_date)
                    return (True, metadata)
                elif cached_hash:
                    metadata["has_changed"] = False
                    metadata["reason"] = "Content hash unchanged"
                    metadata["cache_hit"] = True
                    return (False, metadata)
                else:
                    # First time checking this feed
                    metadata["has_changed"] = True
                    metadata["reason"] = "First check of this feed"
                    self._update_cache(feed_url, content_hash=current_hash, last_build_date=last_build_date)
                    return (True, metadata)

            metadata["has_changed"] = True
            metadata["reason"] = "No cached data available"
            return (True, metadata)

        except Exception as e:
            logger.error(f"Error parsing feed {feed_url}: {e}")
            metadata["error"] = str(e)
            metadata["has_changed"] = True
            metadata["reason"] = f"Error parsing: {e}"
            return (True, metadata)

    def _hash_entry(self, entry) -> str:
        """Compute hash of RSS entry."""
        title = entry.get("title", "")
        link = entry.get("link", "")
        combined = f"{title}:{link}".lower().encode()
        return hashlib.md5(combined).hexdigest()

    def _update_cache(
        self,
        feed_url: str,
        etag: Optional[str] = None,
        last_modified: Optional[str] = None,
        last_build_date: Optional[str] = None,
        content_hash: Optional[str] = None,
    ):
        """Update cached metadata for a feed.

        Args:
            feed_url: Feed URL
            etag: ETag from HTTP response
            last_modified: Last-Modified from HTTP response
            last_build_date: Feed's lastBuildDate
            content_hash: Hash of feed content
        """
        if feed_url not in self.feed_cache:
            self.feed_cache[feed_url] = {
                "first_check": datetime.utcnow().isoformat(),
                "check_count": 0,
                "change_count": 0,
            }

        cache_entry = self.feed_cache[feed_url]
        cache_entry["last_check"] = datetime.utcnow().isoformat()
        cache_entry["check_count"] = cache_entry.get("check_count", 0) + 1

        if etag:
            old_etag = cache_entry.get("etag")
            cache_entry["etag"] = etag
            if old_etag and etag != old_etag:
                cache_entry["change_count"] = cache_entry.get("change_count", 0) + 1

        if last_modified:
            cache_entry["last_modified"] = last_modified

        if last_build_date:
            old_date = cache_entry.get("last_build_date")
            cache_entry["last_build_date"] = last_build_date
            if old_date and last_build_date != old_date:
                cache_entry["change_count"] = cache_entry.get("change_count", 0) + 1

        if content_hash:
            old_hash = cache_entry.get("content_hash")
            cache_entry["content_hash"] = content_hash
            if old_hash and content_hash != old_hash:
                cache_entry["change_count"] = cache_entry.get("change_count", 0) + 1

    def get_feed_stats(self, feed_url: str) -> dict:
        """Get statistics about a feed's change frequency.

        Args:
            feed_url: Feed URL

        Returns:
            Dictionary with statistics
        """
        cached = self.feed_cache.get(feed_url, {})

        if not cached:
            return {"feed_url": feed_url, "status": "never_checked"}

        return {
            "feed_url": feed_url,
            "first_check": cached.get("first_check"),
            "last_check": cached.get("last_check"),
            "total_checks": cached.get("check_count", 0),
            "total_changes": cached.get("change_count", 0),
            "change_frequency": (
                f"{cached.get('change_count', 0) / max(cached.get('check_count', 1), 1) * 100:.1f}%"
            ),
        }

    def get_all_stats(self) -> dict:
        """Get statistics about all monitored feeds.

        Returns:
            Dictionary mapping feed URLs to their statistics
        """
        return {feed_url: self.get_feed_stats(feed_url) for feed_url in self.feed_cache}

    def clear_cache(self):
        """Clear all cached feed metadata."""
        self.feed_cache.clear()
        logger.info("Feed change detector cache cleared")
