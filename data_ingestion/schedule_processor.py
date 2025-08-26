"""
Scheduled processing system for automated news scraping and sentiment analysis
Runs as a background service with configurable intervals
"""
import asyncio
import schedule
import time
import logging
import os
import sys
from datetime import datetime, timedelta
from typing import Dict, Optional
import signal
import json

# Add parent directory to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from news_scraper import MarketNewsScraper
from sentiment_pipeline_processor import SentimentProcessingOrchestrator
from backend.app.database import SessionLocal
from backend.app.models import ProcessingQueue, NewsArticle, Company

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('processing.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class ScheduledProcessor:
    """
    Orchestrates scheduled processing of news and sentiment analysis
    """
    
    def __init__(self, database_url: str, news_api_key: Optional[str] = None):
        self.database_url = database_url
        self.news_scraper = MarketNewsScraper(news_api_key)
        self.sentiment_orchestrator = SentimentProcessingOrchestrator(database_url)
        self.running = False
        self.stats = {
            'start_time': None,
            'last_news_scrape': None,
            'last_sentiment_analysis': None,
            'total_articles_processed': 0,
            'total_sentiment_scores': 0,
            'errors': []
        }
    
    async def scrape_news_task(self) -> Dict:
        """
        Scheduled news scraping task
        """
        logger.info("Starting scheduled news scraping")
        start_time = datetime.now()
        
        try:
            # Scrape from all sources
            articles = await self.news_scraper.scrape_all_sources(
                include_newsapi=True,
                include_rss=True,
                include_web=True
            )
            
            if not articles:
                logger.warning("No articles found during scraping")
                return {'status': 'completed', 'articles_found': 0}
            
            # Store articles in database
            stored_count = await self._store_scraped_articles(articles)
            
            # Update stats
            self.stats['last_news_scrape'] = datetime.now()
            self.stats['total_articles_processed'] += stored_count
            
            duration = (datetime.now() - start_time).total_seconds()
            
            result = {
                'status': 'completed',
                'articles_scraped': len(articles),
                'articles_stored': stored_count,
                'duration_seconds': duration,
                'timestamp': datetime.now().isoformat()
            }
            
            logger.info(f"News scraping completed: {stored_count} articles stored in {duration:.2f}s")
            return result
            
        except Exception as e:
            error_msg = f"News scraping failed: {str(e)}"
            logger.error(error_msg)
            self.stats['errors'].append({
                'timestamp': datetime.now().isoformat(),
                'task': 'news_scraping',
                'error': error_msg
            })
            
            return {
                'status': 'failed',
                'error': error_msg,
                'timestamp': datetime.now().isoformat()
            }
    
    async def _store_scraped_articles(self, articles) -> int:
        """
        Store scraped articles in database with company mentions
        """
        stored_count = 0
        
        with SessionLocal() as db:
            try:
                for article_data in articles:
                    # Check if article already exists
                    existing = db.query(NewsArticle).filter(
                        NewsArticle.source_url == article_data.url
                    ).first()
                    
                    if existing:
                        continue
                    
                    # Get or create news source
                    from backend.app.models import NewsSource
                    source = db.query(NewsSource).filter(
                        NewsSource.name == article_data.source
                    ).first()
                    
                    if not source:
                        source = NewsSource(
                            name=article_data.source,
                            base_url="",
                            is_active=True
                        )
                        db.add(source)
                        db.flush()
                    
                    # Create news article
                    new_article = NewsArticle(
                        title=article_data.title,
                        content=article_data.content,
                        summary=article_data.summary,
                        source_id=source.id,
                        source_url=article_data.url,
                        author=article_data.author,
                        published_at=article_data.published_at,
                        scraped_at=article_data.scraped_at,
                        word_count=article_data.word_count,
                        is_processed=False
                    )
                    
                    db.add(new_article)
                    db.flush()
                    
                    # Create company mentions
                    from backend.app.models import CompanyMention
                    for company_symbol in article_data.company_mentions:
                        company = db.query(Company).filter(
                            Company.symbol == company_symbol
                        ).first()
                        
                        if company:
                            # Calculate relevance score
                            relevance = self.news_scraper.deduplicator.seen_hashes  # Placeholder
                            relevance_score = 0.5  # Default relevance
                            
                            mention = CompanyMention(
                                article_id=new_article.id,
                                company_id=company.id,
                                mention_count=1,
                                relevance_score=relevance_score,
                                context_window=article_data.content[:500] if article_data.content else ""
                            )
                            db.add(mention)
                    
                    stored_count += 1
                
                db.commit()
                logger.info(f"Stored {stored_count} new articles")
                
            except Exception as e:
                logger.error(f"Error storing articles: {e}")
                db.rollback()
                raise
        
        return stored_count
    
    async def sentiment_analysis_task(self) -> Dict:
        """
        Scheduled sentiment analysis task
        """
        logger.info("Starting scheduled sentiment analysis")
        start_time = datetime.now()
        
        try:
            # Run incremental processing
            results = await self.sentiment_orchestrator.run_incremental_processing()
            
            # Update stats
            self.stats['last_sentiment_analysis'] = datetime.now()
            if 'processing_results' in results:
                self.stats['total_sentiment_scores'] += results['processing_results'].get('total_sentiment_scores', 0)
            
            duration = (datetime.now() - start_time).total_seconds()
            results['duration_seconds'] = duration
            results['timestamp'] = datetime.now().isoformat()
            
            logger.info(f"Sentiment analysis completed in {duration:.2f}s")
            return results
            
        except Exception as e:
            error_msg = f"Sentiment analysis failed: {str(e)}"
            logger.error(error_msg)
            self.stats['errors'].append({
                'timestamp': datetime.now().isoformat(),
                'task': 'sentiment_analysis',
                'error': error_msg
            })
            
            return {
                'status': 'failed',
                'error': error_msg,
                'timestamp': datetime.now().isoformat()
            }
    
    async def daily_aggregation_task(self) -> Dict:
        """
        Daily aggregation task
        """
        logger.info("Starting daily aggregation task")
        start_time = datetime.now()
        
        try:
            # Create aggregates for today and yesterday
            today = datetime.now().date()
            yesterday = today - timedelta(days=1)
            
            today_results = self.sentiment_orchestrator.db_processor.create_daily_aggregates(today)
            yesterday_results = self.sentiment_orchestrator.db_processor.create_daily_aggregates(yesterday)
            
            duration = (datetime.now() - start_time).total_seconds()
            
            result = {
                'status': 'completed',
                'today_aggregates': today_results,
                'yesterday_aggregates': yesterday_results,
                'duration_seconds': duration,
                'timestamp': datetime.now().isoformat()
            }
            
            logger.info(f"Daily aggregation completed in {duration:.2f}s")
            return result
            
        except Exception as e:
            error_msg = f"Daily aggregation failed: {str(e)}"
            logger.error(error_msg)
            self.stats['errors'].append({
                'timestamp': datetime.now().isoformat(),
                'task': 'daily_aggregation',
                'error': error_msg
            })
            
            return {
                'status': 'failed',
                'error': error_msg,
                'timestamp': datetime.now().isoformat()
            }
    
    async def weekly_cleanup_task(self) -> Dict:
        """
        Weekly cleanup task
        """
        logger.info("Starting weekly cleanup task")
        start_time = datetime.now()
        
        try:
            cleanup_results = self.sentiment_orchestrator.db_processor.cleanup_old_data(days_to_keep=90)
            
            duration = (datetime.now() - start_time).total_seconds()
            cleanup_results['duration_seconds'] = duration
            cleanup_results['timestamp'] = datetime.now().isoformat()
            
            logger.info(f"Weekly cleanup completed in {duration:.2f}s")
            return cleanup_results
            
        except Exception as e:
            error_msg = f"Weekly cleanup failed: {str(e)}"
            logger.error(error_msg)
            self.stats['errors'].append({
                'timestamp': datetime.now().isoformat(),
                'task': 'weekly_cleanup',
                'error': error_msg
            })
            
            return {
                'status': 'failed',
                'error': error_msg,
                'timestamp': datetime.now().isoformat()
            }
    
    def schedule_tasks(self):
        """
        Set up scheduled tasks
        """
        logger.info("Setting up scheduled tasks")
        
        # News scraping every 2 hours
        schedule.every(2).hours.do(lambda: asyncio.create_task(self.scrape_news_task()))
        
        # Sentiment analysis every 30 minutes
        schedule.every(30).minutes.do(lambda: asyncio.create_task(self.sentiment_analysis_task()))
        
        # Daily aggregation at 1 AM
        schedule.every().day.at("01:00").do(lambda: asyncio.create_task(self.daily_aggregation_task()))
        
        # Weekly cleanup on Sunday at 2 AM
        schedule.every().sunday.at("02:00").do(lambda: asyncio.create_task(self.weekly_cleanup_task()))
        
        logger.info("Scheduled tasks configured:")
        logger.info("  - News scraping: every 2 hours")
        logger.info("  - Sentiment analysis: every 30 minutes")
        logger.info("  - Daily aggregation: daily at 1:00 AM")
        logger.info("  - Weekly cleanup: Sundays at 2:00 AM")
    
    def get_status(self) -> Dict:
        """
        Get current processor status
        """
        with SessionLocal() as db:
            # Get database stats
            total_articles = db.query(NewsArticle).count()
            processed_articles = db.query(NewsArticle).filter(NewsArticle.is_processed == True).count()
            pending_articles = total_articles - processed_articles
            
            # Get recent activity
            recent_articles = db.query(NewsArticle).filter(
                NewsArticle.scraped_at >= datetime.now() - timedelta(hours=24)
            ).count()
        
        return {
            'processor_running': self.running,
            'stats': self.stats,
            'database_stats': {
                'total_articles': total_articles,
                'processed_articles': processed_articles,
                'pending_articles': pending_articles,
                'processing_rate': round((processed_articles / total_articles * 100), 2) if total_articles > 0 else 0,
                'articles_last_24h': recent_articles
            },
            'next_scheduled_runs': self._get_next_runs(),
            'uptime_seconds': (datetime.now() - self.stats['start_time']).total_seconds() if self.stats['start_time'] else 0
        }
    
    def _get_next_runs(self) -> Dict:
        """
        Get next scheduled run times
        """
        jobs = schedule.get_jobs()
        next_runs = {}
        
        for job in jobs:
            task_name = str(job.job_func).split('lambda')[0].strip()
            if hasattr(job, 'next_run'):
                next_runs[task_name] = job.next_run.isoformat() if job.next_run else None
        
        return next_runs
    
    async def run_scheduler(self):
        """
        Main scheduler loop
        """
        self.running = True
        self.stats['start_time'] = datetime.now()
        
        logger.info("Starting scheduled processor")
        
        # Set up signal handlers for graceful shutdown
        def signal_handler(signum, frame):
            logger.info(f"Received signal {signum}, shutting down gracefully...")
            self.running = False
        
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)
        
        # Schedule tasks
        self.schedule_tasks()
        
        # Run initial tasks
        logger.info("Running initial tasks...")
        await self.scrape_news_task()
        await self.sentiment_analysis_task()
        
        # Main scheduler loop
        while self.running:
            try:
                schedule.run_pending()
                await asyncio.sleep(60)  # Check every minute
                
            except KeyboardInterrupt:
                logger.info("Keyboard interrupt received, shutting down...")
                break
            except Exception as e:
                logger.error(f"Scheduler error: {e}")
                await asyncio.sleep(60)
        
        logger.info("Scheduled processor stopped")
        self.running = False
    
    async def run_once(self, task_type: str = 'all') -> Dict:
        """
        Run processing tasks once (for testing/manual execution)
        """
        results = {}
        
        if task_type in ['all', 'news']:
            logger.info("Running news scraping task...")
            results['news_scraping'] = await self.scrape_news_task()
        
        if task_type in ['all', 'sentiment']:
            logger.info("Running sentiment analysis task...")
            results['sentiment_analysis'] = await self.sentiment_analysis_task()
        
        if task_type in ['all', 'aggregates']:
            logger.info("Running aggregation task...")
            results['aggregation'] = await self.daily_aggregation_task()
        
        if task_type == 'cleanup':
            logger.info("Running cleanup task...")
            results['cleanup'] = await self.weekly_cleanup_task()
        
        return results


class ProcessingMonitor:
    """
    Monitor and report on processing activities
    """
    
    def __init__(self, database_url: str):
        self.database_url = database_url
    
    def get_processing_metrics(self, hours: int = 24) -> Dict:
        """
        Get processing metrics for the specified time period
        """
        with SessionLocal() as db:
            time_threshold = datetime.now() - timedelta(hours=hours)
            
            # Article metrics
            total_articles = db.query(NewsArticle).filter(
                NewsArticle.scraped_at >= time_threshold
            ).count()
            
            processed_articles = db.query(NewsArticle).filter(
                NewsArticle.scraped_at >= time_threshold,
                NewsArticle.is_processed == True
            ).count()
            
            # Sentiment metrics
            from backend.app.models import SentimentScore
            sentiment_scores = db.query(SentimentScore).filter(
                SentimentScore.processed_at >= time_threshold
            ).count()
            
            # Company mention metrics
            from backend.app.models import CompanyMention
            company_mentions = db.query(CompanyMention).join(NewsArticle).filter(
                NewsArticle.scraped_at >= time_threshold
            ).count()
            
            # Error metrics
            failed_tasks = db.query(ProcessingQueue).filter(
                ProcessingQueue.created_at >= time_threshold,
                ProcessingQueue.status == 'failed'
            ).count()
            
            return {
                'time_period_hours': hours,
                'articles': {
                    'total_scraped': total_articles,
                    'processed': processed_articles,
                    'pending': total_articles - processed_articles,
                    'processing_rate': round((processed_articles / total_articles * 100), 2) if total_articles > 0 else 0
                },
                'sentiment_analysis': {
                    'total_scores': sentiment_scores,
                    'avg_per_article': round(sentiment_scores / processed_articles, 2) if processed_articles > 0 else 0
                },
                'company_mentions': {
                    'total_mentions': company_mentions,
                    'avg_per_article': round(company_mentions / total_articles, 2) if total_articles > 0 else 0
                },
                'errors': {
                    'failed_tasks': failed_tasks
                },
                'timestamp': datetime.now().isoformat()
            }
    
    def get_company_activity_report(self, days: int = 7) -> Dict:
        """
        Get company activity report
        """
        with SessionLocal() as db:
            time_threshold = datetime.now() - timedelta(days=days)
            
            # Get most mentioned companies
            from sqlalchemy import func, desc
            company_mentions = db.query(
                Company.symbol,
                Company.name,
                func.count(NewsArticle.id).label('article_count'),
                func.avg(SentimentScore.sentiment_score).label('avg_sentiment')
            ).join(CompanyMention).join(NewsArticle).outerjoin(SentimentScore).filter(
                NewsArticle.published_at >= time_threshold
            ).group_by(Company.id, Company.symbol, Company.name).order_by(
                desc('article_count')
            ).limit(20).all()
            
            companies_data = []
            for mention in company_mentions:
                companies_data.append({
                    'symbol': mention.symbol,
                    'name': mention.name,
                    'article_count': int(mention.article_count),
                    'avg_sentiment': float(mention.avg_sentiment) if mention.avg_sentiment else 0.0,
                    'sentiment_label': 'positive' if (mention.avg_sentiment or 0) > 0.1 else 'negative' if (mention.avg_sentiment or 0) < -0.1 else 'neutral'
                })
            
            return {
                'time_period_days': days,
                'top_companies': companies_data,
                'total_companies_mentioned': len(companies_data),
                'timestamp': datetime.now().isoformat()
            }


# CLI interface
async def main():
    """
    Command-line interface for scheduled processor
    """
    import argparse
    
    parser = argparse.ArgumentParser(description='Scheduled Processing System')
    parser.add_argument('--mode', choices=['scheduler', 'once', 'status', 'monitor'], 
                       default='once', help='Processing mode')
    parser.add_argument('--task', choices=['all', 'news', 'sentiment', 'aggregates', 'cleanup'],
                       default='all', help='Task type for once mode')
    parser.add_argument('--database-url', default=os.getenv('DATABASE_URL',
                       'postgresql://admin:password@localhost:5432/market_intelligence'))
    parser.add_argument('--news-api-key', default=os.getenv('NEWS_API_KEY'))
    
    args = parser.parse_args()
    
    if args.mode == 'scheduler':
        # Run continuous scheduler
        processor = ScheduledProcessor(args.database_url, args.news_api_key)
        await processor.run_scheduler()
        
    elif args.mode == 'once':
        # Run tasks once
        processor = ScheduledProcessor(args.database_url, args.news_api_key)
        results = await processor.run_once(args.task)
        print(json.dumps(results, indent=2, default=str))
        
    elif args.mode == 'status':
        # Get processor status
        processor = ScheduledProcessor(args.database_url, args.news_api_key)
        status = processor.get_status()
        print(json.dumps(status, indent=2, default=str))
        
    elif args.mode == 'monitor':
        # Get processing metrics
        monitor = ProcessingMonitor(args.database_url)
        metrics = monitor.get_processing_metrics(24)
        company_report = monitor.get_company_activity_report(7)
        
        print("=== Processing Metrics (24 hours) ===")
        print(json.dumps(metrics, indent=2))
        print("\n=== Company Activity Report (7 days) ===")
        print(json.dumps(company_report, indent=2))


if __name__ == "__main__":
    # Configure logging for CLI
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    
    asyncio.run(main())