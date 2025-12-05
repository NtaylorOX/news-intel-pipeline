"""
Analysis Service - LLM-powered article analysis
Consumes article messages from Kafka topic, performs AI analysis,
stores results in database, and publishes completion events
"""

import os
import json
import asyncio
import logging
from typing import Optional, Dict, Any

# Add parent directories to path for imports
import sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../../'))

from app.core.database import SessionLocal, NewsArticle
from app.services.kafka_messaging import (
    KafkaConfig,
    KafkaMessageConsumer,
    KafkaMessageProducer,
    create_topics_if_not_exist
)
from app.services.ai_service import AIService

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Configuration
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
PYTHON_ENV = os.getenv("PYTHON_ENV", "development")
ANALYSIS_BATCH_SIZE = int(os.getenv("ANALYSIS_BATCH_SIZE", "5"))


class AnalysisWorker:
    """Background worker for LLM-powered article analysis"""
    
    def __init__(self):
        self.kafka_bootstrap_servers = KAFKA_BOOTSTRAP_SERVERS
        self.consumer = None
        self.producer = None
        self.analysis_service = None
        self.running = False
        self.articles_processed = 0
        self.articles_failed = 0
        
    async def initialize(self):
        """Initialize Kafka connections (database already initialized by API)"""
        logger.info("🚀 Analysis Worker initializing...")
        
        try:
            # Initialize Kafka topics
            create_topics_if_not_exist(self.kafka_bootstrap_servers)
            logger.info("✓ Kafka topics verified/created")
            
            # Initialize Kafka consumer (subscribe to analysis queue)
            self.consumer = KafkaMessageConsumer(
                topic='article_needs_analysis',
                group_id="analysis-service",
                bootstrap_servers=self.kafka_bootstrap_servers
            )
            self.consumer.connect()
            logger.info("✓ Kafka consumer connected")
            logger.info("✓ Subscribed to article_needs_analysis topic")
            
            # Initialize Kafka producer (for publishing results)
            self.producer = KafkaMessageProducer(
                bootstrap_servers=self.kafka_bootstrap_servers
            )
            self.producer.connect()
            logger.info("✓ Kafka producer connected")
            
            # Initialize AI analysis service
            self.analysis_service = AIService()
            logger.info("✓ AI analysis service initialized")
            
            logger.info("✅ Analysis Worker ready")
            
        except Exception as e:
            logger.error(f"❌ Initialization failed: {e}", exc_info=True)
            raise
    
    async def process_article(self, article_data: Dict[str, Any]) -> bool:
        """
        Process a single article through AI analysis
        
        Args:
            article_data: Dictionary with article_id, title, content, etc.
            
        Returns:
            True if successful, False if failed
        """
        try:
            article_id = article_data.get("article_id")
            title = article_data.get("title", "")
            content = article_data.get("content", "")
            url = article_data.get("url", "")
            
            if not all([article_id, title, content]):
                logger.warning(f"Incomplete article data for {article_id}: missing required fields")
                return False
            
            logger.info(f"🔬 Analyzing article {article_id}: {title[:60]}...")
            
            # Perform AI analysis - combine title and content for analysis
            analysis_result = await asyncio.to_thread(
                self.analysis_service.analyze_news,
                f"Title: {title}\n\nContent: {content}"
            )
            
            # Update article in database
            db = SessionLocal()
            try:
                article = db.query(NewsArticle).filter(
                    NewsArticle.id == article_id
                ).first()
                
                if article:
                    article.analysis_result = json.dumps(analysis_result)
                    article.is_analyzed = True
                    db.commit()
                    logger.info(f"✓ Stored analysis for article {article_id}")
                    
                    # Publish completion event
                    self.producer.send_analysis_complete(
                        article_id=article_id,
                        analysis_result=analysis_result
                    )
                    logger.info(f"📤 Published analysis_complete for article {article_id}")
                    
                    self.articles_processed += 1
                    return True
                else:
                    logger.warning(f"Article {article_id} not found in database")
                    self.producer.send_analysis_error(
                        article_id=article_id,
                        error_message=f"Article {article_id} not found in database"
                    )
                    return False
                    
            except Exception as db_error:
                logger.error(f"Database error for article {article_id}: {db_error}")
                self.producer.send_analysis_error(
                    article_id=article_id,
                    error_message=f"Database error: {str(db_error)}"
                )
                return False
            finally:
                db.close()
                
        except Exception as e:
            logger.error(f"Analysis failed for article {article_data.get('article_id')}: {e}", exc_info=True)
            article_id = article_data.get("article_id")
            if article_id:
                self.producer.send_analysis_error(
                    article_id=article_id,
                    error_message=str(e)
                )
            self.articles_failed += 1
            return False
    
    async def run(self):
        """Run the analysis worker in continuous message loop"""
        self.running = True
        logger.info(f"🧠 Starting continuous article analysis (batch size: {ANALYSIS_BATCH_SIZE})")
        
        try:
            while self.running:
                try:
                    # Consume message with timeout
                    msg = self.consumer.consumer.poll(timeout_ms=1000)
                    
                    if not msg:
                        continue
                    
                    # Process messages from each topic partition
                    for topic_partition, messages in msg.items():
                        for message in messages:
                            try:
                                # Parse message (already deserialized by consumer)
                                article_data = message.value if isinstance(message.value, dict) else json.loads(message.value.decode('utf-8'))
                                
                                # Process article
                                success = await self.process_article(article_data)
                                
                                if not success and self.articles_failed >= 10:
                                    logger.warning(f"High failure rate ({self.articles_failed} failures)")
                                    
                            except json.JSONDecodeError as je:
                                logger.error(f"Failed to parse message: {je}")
                            except Exception as e:
                                logger.error(f"Error processing message: {e}", exc_info=True)
                    
                    # Log stats periodically
                    if (self.articles_processed + self.articles_failed) % ANALYSIS_BATCH_SIZE == 0:
                        logger.info(
                            f"📊 Analysis stats: {self.articles_processed} processed, "
                            f"{self.articles_failed} failed"
                        )
                        
                except asyncio.CancelledError:
                    logger.info("Analysis worker cancelled")
                    break
                except Exception as e:
                    logger.error(f"Error in message loop: {e}", exc_info=True)
                    await asyncio.sleep(5)  # Brief backoff before retry
                    
        except KeyboardInterrupt:
            logger.info("⌨️ Received keyboard interrupt")
        except Exception as e:
            logger.error(f"❌ Worker error: {e}", exc_info=True)
            raise
        finally:
            await self.shutdown()
    
    async def shutdown(self):
        """Graceful shutdown"""
        logger.info("🛑 Analysis Worker shutting down...")
        self.running = False
        
        try:
            if self.consumer:
                self.consumer.close()
                logger.info("✓ Kafka consumer closed")
        except Exception as e:
            logger.error(f"Error closing consumer: {e}")
        
        try:
            if self.producer:
                self.producer.close()
                logger.info("✓ Kafka producer closed")
        except Exception as e:
            logger.error(f"Error closing producer: {e}")
        
        logger.info(
            f"✅ Analysis Worker shutdown complete "
            f"(Processed: {self.articles_processed}, Failed: {self.articles_failed})"
        )


async def main():
    """Main entry point"""
    worker = AnalysisWorker()
    
    try:
        await worker.initialize()
        await worker.run()
    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
        exit(1)


if __name__ == "__main__":
    logger.info(f"🌍 Starting Analysis Service in {PYTHON_ENV} environment")
    logger.info(f"📡 Kafka Bootstrap Servers: {KAFKA_BOOTSTRAP_SERVERS}")
    logger.info(f"🧠 Batch size: {ANALYSIS_BATCH_SIZE}")
    
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Analysis Service stopped")
    except Exception as e:
        logger.error(f"Analysis Service crashed: {e}", exc_info=True)
        exit(1)
