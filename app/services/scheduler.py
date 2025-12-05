"""Background scheduler for continuous news ingestion via Kafka."""

import logging
from datetime import datetime
from apscheduler.schedulers.background import BackgroundScheduler

from app.services.news_fetcher import NewsAggregator
from app.services.kafka_messaging import KafkaMessageProducer
from app.services.feed_monitor import FeedChangeDetector
from app.core.source_config import get_all_configs

logger = logging.getLogger(__name__)

# Global scheduler instance and monitoring
scheduler = BackgroundScheduler()
feed_detector = FeedChangeDetector()
_is_running = False

# Track last job execution
_last_job_run = {
    "timestamp": None,
    "sources_checked": 0,
    "fetched": 0,
    "published": 0,
    "status": "pending"
}


def _fetch_and_publish_to_kafka():
    """Job: Fetch news from all sources and publish to Kafka raw_articles topic.
    
    This is a pure producer - all processing (dedup, filter, commit) happens
    in the Kafka consumer for a unified pipeline.
    """
    logger.info("=" * 60)
    logger.info("Starting smart news fetch job (publishing to Kafka)...")
    logger.info("=" * 60)

    try:
        aggregator = NewsAggregator()
        kafka_producer = KafkaMessageProducer()

        total_fetched = 0
        total_published = 0
        sources_checked = []
        sources_skipped = []

        # Get configured sources
        source_configs = get_all_configs()

        for source in aggregator.sources:
            source_name = source.source_name
            config = source_configs.get(source_name)

            if not config or not config.enabled:
                logger.info(f"⊘ {source_name}: Disabled")
                continue

            logger.info(f"→ Checking {source_name}...")

            # For RSS feeds, check if they changed first
            if hasattr(source, "feed_url"):
                feed_url = source.feed_url
                has_changed, change_info = feed_detector.check_feed_changed(feed_url)

                if not has_changed:
                    logger.info(
                        f"  ✓ {source_name}: No changes detected "
                        f"(reason: {change_info.get('reason', 'unknown')})"
                    )
                    sources_skipped.append(source_name)
                    continue
                else:
                    logger.info(f"  • {source_name}: Changes detected! ({change_info.get('reason')})")

            # Fetch articles from this source
            try:
                articles = source.fetch()
                logger.info(f"  Fetched {len(articles)} articles from {source_name}")
                sources_checked.append(source_name)

                for article in articles:
                    total_fetched += 1

                    # Publish raw article to Kafka - let the consumer handle everything else
                    try:
                        message = {
                            "title": article.title,
                            "content": article.content,
                            "url": article.url,
                            "source": article.source,
                            "published_at": article.published_at.isoformat() if article.published_at else None,
                            "content_hash": article.content_hash,
                        }
                        kafka_producer.send_message("raw_articles", message)
                        total_published += 1

                    except Exception as e:
                        logger.error(f"Error publishing article from {source_name}: {e}")

            except Exception as e:
                logger.error(f"Error fetching from {source_name}: {e}")

        kafka_producer.close()

        logger.info("\nJob summary:")
        if sources_checked:
            logger.info(f"  Sources checked: {', '.join(sources_checked)}")
        if sources_skipped:
            logger.info(f"  Sources skipped (no change): {', '.join(sources_skipped)}")
        logger.info(f"  Total fetched: {total_fetched}")
        logger.info(f"  Total published to Kafka: {total_published}")
        logger.info("Job completed successfully!")
        
        # Track job execution
        global _last_job_run
        _last_job_run = {
            "timestamp": datetime.utcnow().isoformat(),
            "sources_checked": len(sources_checked),
            "fetched": total_fetched,
            "published": total_published,
            "status": "completed"
        }

    except Exception as e:
        logger.error(f"Error in scheduled job: {e}")


def start_scheduler(interval_minutes: int = 5):
    """Start the background scheduler for continuous news fetching.

    Fetches articles and publishes to Kafka raw_articles topic.
    All processing (dedup, filter, commit) happens in the Kafka consumer.

    Args:
        interval_minutes: How often to fetch articles (default: 5 minutes).
    """
    global _is_running

    if _is_running:
        logger.warning("Scheduler is already running")
        return

    try:
        # Add main job to run frequently
        scheduler.add_job(
            _fetch_and_publish_to_kafka,
            "interval",
            minutes=interval_minutes,
            id="fetch_and_publish_job",
            name="Fetch news articles and publish to Kafka",
            replace_existing=True,
            max_instances=1,
        )

        # Start scheduler
        if not scheduler.running:
            scheduler.start()
            _is_running = True
            logger.info(
                f"✓ Scheduler started - fetching and publishing every {interval_minutes} minutes"
            )
            logger.info("  All processing (dedup/filter/commit) handled by Kafka consumer")

            # Run immediately on startup
            logger.info("Running initial fetch job on startup...")
            _fetch_and_publish_to_kafka()

    except Exception as e:
        logger.error(f"Error starting scheduler: {e}")


def stop_scheduler():
    """Stop the background scheduler."""
    global _is_running

    if scheduler.running:
        scheduler.shutdown()
        _is_running = False
        logger.info("✓ Scheduler stopped")
    else:
        logger.warning("Scheduler is not running")


def is_scheduler_running() -> bool:
    """Check if scheduler is running."""
    return scheduler.running


def get_last_job_run() -> dict:
    """Get the last job execution statistics."""
    return _last_job_run.copy()


def get_feed_stats():
    """Get detailed feed monitoring statistics."""
    return feed_detector.get_all_stats()


def reset_feed_cache():
    """Reset feed change detection cache (useful for testing)."""
    feed_detector.clear_cache()
    logger.info("Feed monitoring cache cleared")
