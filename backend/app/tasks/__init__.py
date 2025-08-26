from app.core.logging import get_logger
from fastapi import APIRouter
from sqlalchemy import and_
from datetime import datetime
from app.database import get_db
from app.models import NewsArticle, SentimentScore, ProcessingQueue
from data_ingestion.news_scraper import news_scraper
from data_ingestion.sentiment_processor import SentimentAnalysisAPI
from app.models import NewsSource, CompanySchema, CompanyMention

router = APIRouter(prefix="/api", tags=["Ingestion"])
logger = get_logger()

async def process_news_scraping_task(
    task_id: int,
    include_newsapi: bool,
    include_rss: bool,
    include_web: bool
):
    """Background task for news scraping"""
    logger.info("Starting news scraping task", task_id=task_id)
    
    # Update task status
    with next(get_db()) as db:
        task = db.query(ProcessingQueue).filter(ProcessingQueue.id == task_id).first()
        if task:
            task.status = "processing"
            task.started_at = datetime.utcnow()
            task.attempts += 1
            db.commit()
    
    try:
        # Perform news scraping
        articles = await news_scraper.scrape_all_sources(
            include_newsapi=include_newsapi,
            include_rss=include_rss,
            include_web=include_web
        )
        
        # Store articles in database
        with next(get_db()) as db:
            stored_count = 0
            
            for article_data in articles:
                # Check if article already exists (by URL)
                existing = db.query(NewsArticle).filter(
                    NewsArticle.source_url == article_data.url
                ).first()
                
                if existing:
                    continue
                
                # Get or create news source
                source = db.query(NewsSource).filter(
                    NewsSource.name == article_data.source
                ).first()
                
                if not source:
                    source = NewsSource(
                        name=article_data.source,
                        base_url="",  # Will be updated later
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
                for company_symbol in article_data.company_mentions:
                    company = db.query(CompanySchema).filter(
                        CompanySchema.symbol == company_symbol
                    ).first()
                    
                    if company:
                        # Calculate relevance score
                        from data_ingestion.news_scraper import CompanyMatcher
                        matcher = CompanyMatcher()
                        relevance = matcher.get_relevance_score(
                            f"{article_data.title} {article_data.content}", 
                            company_symbol
                        )
                        
                        mention = CompanyMention(
                            article_id=new_article.id,
                            company_id=CompanySchema.id,
                            mention_count=1,
                            relevance_score=relevance,
                            context_window=article_data.content[:500]
                        )
                        db.add(mention)
                
                stored_count += 1
            
            db.commit()
        
        # Update task status to completed
        with next(get_db()) as db:
            task = db.query(ProcessingQueue).filter(ProcessingQueue.id == task_id).first()
            if task:
                task.status = "completed"
                task.completed_at = datetime.utcnow()
                task.payload["result"] = {
                    "articles_scraped": len(articles),
                    "articles_stored": stored_count,
                    "duplicates_skipped": len(articles) - stored_count
                }
                db.commit()
        
        logger.info("News scraping task completed", 
                   task_id=task_id, 
                   articles_scraped=len(articles),
                   articles_stored=stored_count)
        
    except Exception as e:
        logger.error("News scraping task failed", task_id=task_id, error=str(e))
        
        # Update task status to failed
        with next(get_db()) as db:
            task = db.query(ProcessingQueue).filter(ProcessingQueue.id == task_id).first()
            if task:
                task.status = "failed"
                task.completed_at = datetime.utcnow()
                task.error_message = str(e)
                db.commit()

async def process_sentiment_analysis_task(task_id: int, article_limit: int):
    """Background task for sentiment analysis"""
    logger.info("Starting sentiment analysis task", task_id=task_id)
    
    # Update task status
    with next(get_db()) as db:
        task = db.query(ProcessingQueue).filter(ProcessingQueue.id == task_id).first()
        if task:
            task.status = "processing"
            task.started_at = datetime.utcnow()
            task.attempts += 1
            db.commit()
    
    try:
        # Get unprocessed articles
        with next(get_db()) as db:
            articles = db.query(NewsArticle).filter(
                NewsArticle.is_processed == False
            ).limit(article_limit).all()
            
            # Convert to dict format for sentiment processing
            article_data = []
            for article in articles:
                mentions = db.query(CompanyMention).join(CompanySchema).filter(
                    CompanyMention.article_id == article.id
                ).all()
                
                article_dict = {
                    'id': article.id,
                    'title': article.title,
                    'content': article.content or '',
                    'url': article.source_url,
                    'company_mentions': [mention.CompanySchema.symbol for mention in mentions]
                }
                article_data.append(article_dict)
        
        if not article_data:
            logger.info("No articles to process for sentiment analysis")
            return
        
        # Process sentiment
        results = await SentimentAnalysisAPI.process_articles(article_data)
        
        # Store sentiment results
        with next(get_db()) as db:
            stored_count = 0
            
            for result in results['individual_results']:
                # Get article and company
                article = db.query(NewsArticle).filter(NewsArticle.id == result['article_id']).first()
                company = db.query(CompanySchema).filter(CompanySchema.symbol == result['company_symbol']).first()
                
                if not article or not company:
                    continue
                
                # Check if sentiment already exists
                existing = db.query(SentimentScore).filter(
                    and_(
                        SentimentScore.article_id == article.id,
                        SentimentScore.company_id == CompanySchema.id,
                        SentimentScore.model_name == result['model_name']
                    )
                ).first()
                
                if existing:
                    continue
                
                # Create sentiment score
                sentiment_score = SentimentScore(
                    article_id=article.id,
                    company_id=CompanySchema.id,
                    sentiment_score=result['sentiment_score'],
                    confidence=result['confidence'],
                    sentiment_label=result['sentiment_label'],
                    model_name=result['model_name'],
                    model_version=result['model_version'],
                    compound_score=result.get('compound_score'),
                    positive_score=result.get('positive_score'),
                    negative_score=result.get('negative_score'),
                    neutral_score=result.get('neutral_score')
                )
                
                db.add(sentiment_score)
                stored_count += 1
            
            # Mark articles as processed
            for article_dict in article_data:
                article = db.query(NewsArticle).filter(NewsArticle.id == article_dict['id']).first()
                if article:
                    article.is_processed = True
            
            db.commit()
        
        # Update task status to completed
        with next(get_db()) as db:
            task = db.query(ProcessingQueue).filter(ProcessingQueue.id == task_id).first()
            if task:
                task.status = "completed"
                task.completed_at = datetime.utcnow()
                task.payload["result"] = {
                    "articles_processed": len(article_data),
                    "sentiment_scores_created": stored_count,
                    "companies_analyzed": len(results['company_aggregates'])
                }
                db.commit()
        
        logger.info("Sentiment analysis task completed",
                   task_id=task_id,
                   articles_processed=len(article_data),
                   scores_created=stored_count)
        
    except Exception as e:
        logger.error("Sentiment analysis task failed", task_id=task_id, error=str(e))
        
        # Update task status to failed
        with next(get_db()) as db:
            task = db.query(ProcessingQueue).filter(ProcessingQueue.id == task_id).first()
            if task:
                task.status = "failed"
                task.completed_at = datetime.utcnow()
                task.error_message = str(e)
                db.commit()
