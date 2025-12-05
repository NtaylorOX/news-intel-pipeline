"""
Ingestion Service - Dual-mode ingestion worker
1. Kafka Consumer: Processes raw article events from /ingest endpoint
2. Scheduler: Polls RSS/Reddit feeds on schedule

Both run concurrently for complete ingestion coverage
"""

import os
import asyncio
import logging
import json
import hashlib
from datetime import datetime
import requests

# Add parent directories to path for imports
import sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../../'))

from app.services.kafka_messaging import KafkaMessageProducer, KafkaMessageConsumer, create_topics_if_not_exist
from app.core.database import SessionLocal, NewsArticle
from app.services.filtering import ContentFilterService
from app.services.scheduler import start_scheduler, stop_scheduler

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Configuration
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
PYTHON_ENV = os.getenv("PYTHON_ENV", "development")
SCHEDULER_INTERVAL = int(os.getenv("SCHEDULER_INTERVAL", "5"))  # minutes
ENABLE_KAFKA_CONSUMER = os.getenv("ENABLE_KAFKA_CONSUMER", "true").lower() == "true"
ENABLE_SCHEDULER = os.getenv("ENABLE_SCHEDULER", "true").lower() == "true"


class IngestionWorker:
    """Dual-mode ingestion worker: Kafka consumer + RSS/Reddit scheduler"""
    
    def __init__(self):
        self.kafka_bootstrap_servers = KAFKA_BOOTSTRAP_SERVERS
        self.kafka_producer = None
        self.kafka_consumer = None
        self.running = False
        self.consumer_task = None
        
    async def initialize(self):
        """Initialize Kafka connections and database"""
        logger.info("🚀 Ingestion Worker initializing...")
        logger.info(f"   Kafka Consumer: {'ENABLED' if ENABLE_KAFKA_CONSUMER else 'DISABLED'}")
        logger.info(f"   RSS/Reddit Scheduler: {'ENABLED' if ENABLE_SCHEDULER else 'DISABLED'}")
        
        try:
            # Initialize Kafka topics
            create_topics_if_not_exist(self.kafka_bootstrap_servers)
            logger.info("✓ Kafka topics verified/created")
            
            # Initialize Kafka producer (for publishing to article_needs_analysis)
            self.kafka_producer = KafkaMessageProducer(
                bootstrap_servers=self.kafka_bootstrap_servers
            )
            self.kafka_producer.connect()
            logger.info("✓ Kafka producer connected")
            
            # Initialize Kafka consumer if enabled
            if ENABLE_KAFKA_CONSUMER:
                self.kafka_consumer = KafkaMessageConsumer(
                    topic="raw_articles",
                    group_id="ingestion-worker-group",
                    bootstrap_servers=self.kafka_bootstrap_servers
                )
                self.kafka_consumer.connect()
                logger.info("✓ Kafka consumer connected to 'raw_articles' topic")
            
            logger.info("✅ Ingestion Worker ready")
            
        except Exception as e:
            logger.error(f"❌ Initialization failed: {e}", exc_info=True)
            raise
    
    def process_raw_article(self, message: dict) -> None:
        """Process a raw article event from Kafka.
        
        Pipeline:
        1. Deduplicate using content hash
        2. Apply content filtering
        3. Store to database with scores
        4. Publish to article_needs_analysis if passed filter
        
        Args:
            message: Raw article event from Kafka (from scheduler or /ingest endpoint)
        """
        logger.info(f"🔧 Processing raw article: {message.get('title', 'NO TITLE')[:50]}")
        db = SessionLocal()
        try:
            # Handle both scheduler format and manual /ingest format
            # Scheduler: {title, content, url, source, published_at, content_hash}
            # Manual: {id, source, title, body, published_at}
            
            external_id = message.get("id")  # External event ID from /ingest endpoint
            title = message.get("title")
            content = message.get("content") or message.get("body", "")
            url = message.get("url") or f"mock://{message.get('source')}/{message.get('id', 'unknown')}"
            source = message.get("source")
            published_at_str = message.get("published_at")
            content_hash_provided = message.get("content_hash")
            
            # Parse published_at
            published_at = None
            if published_at_str:
                try:
                    published_at = datetime.fromisoformat(published_at_str.replace('Z', '+00:00'))
                except Exception as e:
                    logger.warning(f"Could not parse published_at: {e}")
            
            # Generate or use provided content hash for deduplication
            if content_hash_provided:
                content_hash = content_hash_provided
            else:
                hash_input = f"{title}:{content}".encode('utf-8')
                content_hash = hashlib.md5(hash_input).hexdigest()
            
            # Check for duplicate
            existing = db.query(NewsArticle).filter(
                NewsArticle.content_hash == content_hash
            ).first()
            
            if existing:
                logger.debug(f"Article '{title[:50]}...' already exists (duplicate) - skipping")
                return
            
            logger.info(f"🆕 New article detected, will filter: {title[:50]}")
            
            # Apply content filtering
            passed_filter, filter_details = ContentFilterService.filter_article(
                title, content
            )
            
            relevance_score = float(filter_details.get("relevance_score", 0.0))
            quality_score = float(filter_details.get("quality_score", 0.0))
            
            # Store to database with computed score columns
            article = NewsArticle(
                external_id=external_id,  # Store external event ID
                title=title,
                content=content,
                source=source,
                url=url,
                published_at=published_at,
                content_hash=content_hash,
                passed_filter=passed_filter,
                filter_results=json.dumps(filter_details),
                relevance_score=relevance_score,
                quality_score=quality_score,
            )
            
            db.add(article)
            db.flush()
            
            logger.info(
                f"✓ Stored article '{title[:50]}...' from {source} "
                f"(filter: {passed_filter}, relevance: {relevance_score:.2f}, quality: {quality_score:.2f})"
            )
            
            # If passed filter, publish to analysis queue
            if passed_filter:
                analysis_message = {
                    "article_id": article.id,
                    "title": article.title,
                    "content": article.content,
                    "url": article.url,
                    "source": article.source,
                }
                self.kafka_producer.send_message("article_needs_analysis", analysis_message)
                logger.info(f"📤 Published article {article.id} to analysis queue")
            
            db.commit()
            
        except Exception as e:
            logger.error(f"Error processing article event: {e}", exc_info=True)
            db.rollback()
        finally:
            db.close()
    
    async def run_kafka_consumer(self):
        """Run the Kafka consumer loop"""
        logger.info("📥 Starting Kafka consumer loop for 'raw_articles' topic...")
        
        try:
            processed_count = 0
            
            # Use the generator method to consume messages
            for message in self.kafka_consumer.consume_messages():
                if not self.running:
                    break
                
                self.process_raw_article(message)
                processed_count += 1
                
                if processed_count % 10 == 0:
                    logger.info(f"📊 Processed {processed_count} Kafka articles so far")
                
                # Small async sleep to allow other tasks
                await asyncio.sleep(0.01)
                
        except Exception as e:
            logger.error(f"❌ Kafka consumer error: {e}", exc_info=True)
    
    async def run(self):
        """Run both scheduler and Kafka consumer concurrently"""
        self.running = True
        tasks = []
        
        try:
            # Start scheduler if enabled (polls RSS/Reddit feeds)
            if ENABLE_SCHEDULER:
                start_scheduler(interval_minutes=SCHEDULER_INTERVAL)
                logger.info(f"🔄 Scheduler started with {SCHEDULER_INTERVAL} minute interval")
            
            # Start Kafka consumer if enabled (processes /ingest events)
            if ENABLE_KAFKA_CONSUMER:
                self.consumer_task = asyncio.create_task(self.run_kafka_consumer())
                tasks.append(self.consumer_task)
            
            # Keep running
            if not tasks:
                logger.warning("⚠️ Both scheduler and Kafka consumer are disabled!")
                while self.running:
                    await asyncio.sleep(1)
            else:
                # Wait for tasks while running
                while self.running:
                    await asyncio.sleep(1)
                    
        except KeyboardInterrupt:
            logger.info("⌨️ Received keyboard interrupt")
        except Exception as e:
            logger.error(f"❌ Worker error: {e}", exc_info=True)
            raise
        finally:
            await self.shutdown()
    
    async def shutdown(self):
        """Graceful shutdown"""
        logger.info("🛑 Ingestion Worker shutting down...")
        self.running = False
        
        # Stop scheduler if it was running
        if ENABLE_SCHEDULER:
            try:
                stop_scheduler()
                logger.info("✓ Scheduler stopped")
            except Exception as e:
                logger.error(f"Error stopping scheduler: {e}")
        
        # Stop Kafka consumer
        if ENABLE_KAFKA_CONSUMER and self.consumer_task:
            try:
                self.consumer_task.cancel()
                logger.info("✓ Kafka consumer task cancelled")
            except Exception as e:
                logger.error(f"Error cancelling consumer task: {e}")
        
        try:
            if self.kafka_consumer:
                self.kafka_consumer.close()
                logger.info("✓ Kafka consumer closed")
        except Exception as e:
            logger.error(f"Error closing Kafka consumer: {e}")
        
        try:
            if self.kafka_producer:
                self.kafka_producer.close()
                logger.info("✓ Kafka producer closed")
        except Exception as e:
            logger.error(f"Error closing Kafka producer: {e}")
        
        logger.info("✅ Ingestion Worker shutdown complete")


async def main():
    """Main entry point"""
    worker = IngestionWorker()
    
    try:
        await worker.initialize()
        await worker.run()
    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
        exit(1)


if __name__ == "__main__":
    logger.info(f"🌍 Starting Ingestion Service in {PYTHON_ENV} environment")
    logger.info(f"📡 Kafka Bootstrap Servers: {KAFKA_BOOTSTRAP_SERVERS}")
    
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Ingestion Service stopped")
    except Exception as e:
        logger.error(f"Ingestion Service crashed: {e}", exc_info=True)
        exit(1)
