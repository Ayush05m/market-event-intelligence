"""
FastAPI application for Market Intelligence Platform
High-performance REST API with <200ms response times
"""
import os
import time
import asyncio
import logging
from datetime import datetime, timedelta
from typing import List, Optional, Dict, Any
from contextlib import asynccontextmanager

from fastapi import FastAPI, Depends, HTTPException, BackgroundTasks, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.trustedhost import TrustedHostMiddleware
from fastapi.responses import JSONResponse
from sqlalchemy.orm import Session
from sqlalchemy import desc, func, and_
import structlog

# Import our models and database
from app.database import get_db, db_manager, init_db, get_db_stats
from app.models import (
    Company, NewsArticle, SentimentScore, CompanyMention, SentimentAggregate,
    NewsSource, ProcessingQueue
)
import app.models as schemas

# Import data processing components
import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..', 'data_ingestion'))
from data_ingestion.news_scraper import MarketNewsScraper
from data_ingestion.sentiment_processor import SentimentPipeline

# Configure structured logging
structlog.configure(
    processors=[
        structlog.stdlib.filter_by_level,
        structlog.stdlib.add_logger_name,
        structlog.stdlib.add_log_level,
        structlog.stdlib.PositionalArgumentsFormatter(),
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
        structlog.processors.UnicodeDecoder(),
        structlog.processors.JSONRenderer()
    ],
    context_class=dict,
    logger_factory=structlog.stdlib.LoggerFactory(),
    cache_logger_on_first_use=True,
)

logger = structlog.get_logger()

# Global instances
news_scraper = None
sentiment_pipeline = None

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifecycle management"""
    # Startup
    logger.info("Starting Market Intelligence API...")
    
    # Initialize database
    try:
        init_db()
        logger.info("Database initialized successfully")
    except Exception as e:
        logger.error("Database initialization failed", error=str(e))
        raise
    
    # Initialize global instances
    global news_scraper, sentiment_pipeline
    news_api_key = os.getenv('NEWS_API_KEY')
    news_scraper = MarketNewsScraper(news_api_key)
    sentiment_pipeline = SentimentPipeline()
    
    logger.info("Application startup completed")
    yield
    
    # Shutdown
    logger.info("Shutting down Market Intelligence API...")

# Create FastAPI app
app = FastAPI(
    title="Market Intelligence Platform API",
    description="Real-time financial news sentiment analysis with <200ms response times",
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
    lifespan=lifespan
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000", "http://127.0.0.1:3000"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Add trusted host middleware
app.add_middleware(
    TrustedHostMiddleware,
    allowed_hosts=["localhost", "127.0.0.1", "0.0.0.0"]
)

# Request timing middleware
@app.middleware("http")
async def add_process_time_header(request, call_next):
    start_time = time.time()
    response = await call_next(request)
    process_time = time.time() - start_time
    response.headers["X-Process-Time"] = str(process_time)
    
    # Log slow requests
    if process_time > 0.2:  # 200ms threshold
        logger.warning("Slow request detected", 
                      path=request.url.path, 
                      method=request.method,
                      duration=process_time)
    
    return response

# Health check endpoints
@app.get("/health")
async def health_check():
    """Basic health check"""
    return {"status": "healthy", "timestamp": datetime.utcnow().isoformat()}

@app.get("/health/detailed")
async def detailed_health_check(db: Session = Depends(get_db)):
    """Detailed health check with database connectivity"""
    health_data = {
        "status": "healthy",
        "timestamp": datetime.utcnow().isoformat(),
        "database": "connected" if db_manager.health_check() else "disconnected",
        "version": "1.0.0"
    }
    
    try:
        # Get basic database stats
        stats = get_db_stats()
        health_data["stats"] = stats
        
        # Check connection pool
        pool_info = db_manager.get_connection_info()
        health_data["connection_pool"] = pool_info
        
    except Exception as e:
        health_data["status"] = "degraded"
        health_data["error"] = str(e)
        logger.error("Health check failed", error=str(e))
    
    return health_data

# Company endpoints
@app.get("/api/companies", response_model=List[schemas.Company])
async def get_companies(
    skip: int = 0,
    limit: int = 100,
    active_only: bool = True,
    db: Session = Depends(get_db)
):
    """Get list of tracked companies"""
    query = db.query(Company)
    
    if active_only:
        query = query.filter(Company.is_active == True)
    
    companies = query.offset(skip).limit(limit).all()
    return companies

@app.get("/api/companies/{symbol}", response_model=schemas.Company)
async def get_company(symbol: str, db: Session = Depends(get_db)):
    """Get specific company by symbol"""
    company = db.query(Company).filter(Company.symbol == symbol.upper()).first()
    if not company:
        raise HTTPException(status_code=404, detail="Company not found")
    return company

@app.get("/api/companies/{symbol}/sentiment")
async def get_company_sentiment(
    symbol: str,
    days: int = Query(default=30, le=90, ge=1),
    db: Session = Depends(get_db)
):
    """Get sentiment analysis for a company over specified days"""
    company = db.query(Company).filter(Company.symbol == symbol.upper()).first()
    if not company:
        raise HTTPException(status_code=404, detail="Company not found")
    
    # Calculate date range
    end_date = datetime.now()
    start_date = end_date - timedelta(days=days)
    
    # Get sentiment scores
    sentiment_scores = db.query(SentimentScore).join(NewsArticle).filter(
        and_(
            SentimentScore.company_id == company.id,
            NewsArticle.published_at >= start_date,
            NewsArticle.published_at <= end_date
        )
    ).order_by(desc(NewsArticle.published_at)).all()
    
    # Get aggregated data
    aggregates = db.query(SentimentAggregate).filter(
        and_(
            SentimentAggregate.company_id == company.id,
            SentimentAggregate.date >= start_date.date(),
            SentimentAggregate.hour.is_(None)  # Daily aggregates only
        )
    ).order_by(desc(SentimentAggregate.date)).all()
    
    # Calculate summary statistics
    if sentiment_scores:
        scores = [float(score.sentiment_score) for score in sentiment_scores]
        avg_sentiment = sum(scores) / len(scores)
        positive_count = len([s for s in scores if s > 0.1])
        negative_count = len([s for s in scores if s < -0.1])
        neutral_count = len(scores) - positive_count - negative_count
    else:
        avg_sentiment = 0.0
        positive_count = negative_count = neutral_count = 0
    
    return {
        "company": {
            "symbol": company.symbol,
            "name": company.name,
            "sector": company.sector
        },
        "period": {
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
            "days": days
        },
        "summary": {
            "avg_sentiment": round(avg_sentiment, 3),
            "total_articles": len(sentiment_scores),
            "positive_count": positive_count,
            "negative_count": negative_count,
            "neutral_count": neutral_count,
            "sentiment_trend": "positive" if avg_sentiment > 0.1 else "negative" if avg_sentiment < -0.1 else "neutral"
        },
        "daily_aggregates": [
            {
                "date": agg.date.isoformat(),
                "avg_sentiment": float(agg.avg_sentiment),
                "article_count": agg.article_count,
                "positive_count": agg.positive_count,
                "negative_count": agg.negative_count,
                "neutral_count": agg.neutral_count
            } for agg in aggregates
        ],
        "recent_scores": [
            {
                "sentiment_score": float(score.sentiment_score),
                "confidence": float(score.confidence),
                "sentiment_label": score.sentiment_label,
                "processed_at": score.processed_at.isoformat()
            } for score in sentiment_scores[:10]  # Last 10 scores
        ]
    }

# News endpoints
@app.get("/api/news/latest")
async def get_latest_news(
    limit: int = Query(default=50, le=100, ge=1),
    company_symbol: Optional[str] = None,
    hours: int = Query(default=24, le=168, ge=1),  # Max 1 week
    db: Session = Depends(get_db)
):
    """Get latest news articles with sentiment"""
    # Calculate time threshold
    time_threshold = datetime.now() - timedelta(hours=hours)
    
    # Base query
    query = db.query(NewsArticle).filter(
        NewsArticle.published_at >= time_threshold
    )
    
    # Filter by company if specified
    if company_symbol:
        company = db.query(Company).filter(Company.symbol == company_symbol.upper()).first()
        if not company:
            raise HTTPException(status_code=404, detail="Company not found")
        
        # Join with company mentions
        query = query.join(CompanyMention).filter(
            CompanyMention.company_id == company.id
        )
    
    # Execute query
    articles = query.order_by(desc(NewsArticle.published_at)).limit(limit).all()
    
    # Prepare response with sentiment data
    result = []
    for article in articles:
        # Get sentiment scores for this article
        sentiment_scores = db.query(SentimentScore).filter(
            SentimentScore.article_id == article.id
        ).all()
        
        # Get company mentions
        mentions = db.query(CompanyMention).join(Company).filter(
            CompanyMention.article_id == article.id
        ).all()
        
        article_data = {
            "id": article.id,
            "title": article.title,
            "summary": article.summary or article.content[:200] + "..." if article.content else None,
            "source": article.source.name if article.source else "Unknown",
            "url": article.source_url,
            "author": article.author,
            "published_at": article.published_at.isoformat() if article.published_at else None,
            "word_count": article.word_count,
            "companies_mentioned": [
                {
                    "symbol": mention.company.symbol,
                    "name": mention.company.name,
                    "relevance_score": float(mention.relevance_score)
                } for mention in mentions
            ],
            "sentiment_scores": [
                {
                    "company_symbol": score.company.symbol,
                    "sentiment_score": float(score.sentiment_score),
                    "confidence": float(score.confidence),
                    "sentiment_label": score.sentiment_label
                } for score in sentiment_scores
            ]
        }
        result.append(article_data)
    
    return {
        "articles": result,
        "total": len(result),
        "time_range": {
            "hours": hours,
            "from": time_threshold.isoformat()
        }
    }

@app.get("/api/sentiment/aggregate")
async def get_sentiment_aggregate(
    days: int = Query(default=7, le=30, ge=1),
    db: Session = Depends(get_db)
):
    """Get aggregated sentiment across all companies"""
    # Calculate date range
    end_date = datetime.now().date()
    start_date = end_date - timedelta(days=days)
    
    # Get daily aggregates for all companies
    aggregates = db.query(SentimentAggregate).filter(
        and_(
            SentimentAggregate.date >= start_date,
            SentimentAggregate.date <= end_date,
            SentimentAggregate.hour.is_(None)  # Daily aggregates only
        )
    ).all()
    
    if not aggregates:
        return {
            "market_sentiment": {
                "overall_sentiment": 0.0,
                "total_articles": 0,
                "companies_tracked": 0,
                "sentiment_distribution": {
                    "positive_pct": 0.0,
                    "negative_pct": 0.0,
                    "neutral_pct": 0.0
                }
            },
            "company_breakdown": [],
            "time_series": []
        }
    
    # Calculate overall market sentiment
    total_articles = sum(agg.article_count for agg in aggregates)
    weighted_sentiment = sum(agg.avg_sentiment * agg.article_count for agg in aggregates) / total_articles
    
    total_positive = sum(agg.positive_count for agg in aggregates)
    total_negative = sum(agg.negative_count for agg in aggregates)
    total_neutral = sum(agg.neutral_count for agg in aggregates)
    
    # Group by company
    company_data = {}
    for agg in aggregates:
        symbol = agg.company.symbol
        if symbol not in company_data:
            company_data[symbol] = {
                "symbol": symbol,
                "name": agg.company.name,
                "total_articles": 0,
                "avg_sentiment": 0.0,
                "positive_count": 0,
                "negative_count": 0,
                "neutral_count": 0
            }
        
        company_data[symbol]["total_articles"] += agg.article_count
        company_data[symbol]["avg_sentiment"] += agg.avg_sentiment * agg.article_count
        company_data[symbol]["positive_count"] += agg.positive_count
        company_data[symbol]["negative_count"] += agg.negative_count
        company_data[symbol]["neutral_count"] += agg.neutral_count
    
    # Finalize company averages
    for company in company_data.values():
        if company["total_articles"] > 0:
            company["avg_sentiment"] /= company["total_articles"]
            company["avg_sentiment"] = round(company["avg_sentiment"], 3)
    
    # Group by date for time series
    date_data = {}
    for agg in aggregates:
        date_str = agg.date.isoformat()
        if date_str not in date_data:
            date_data[date_str] = {
                "date": date_str,
                "total_articles": 0,
                "avg_sentiment": 0.0,
                "positive_count": 0,
                "negative_count": 0,
                "neutral_count": 0
            }
        
        date_data[date_str]["total_articles"] += agg.article_count
        date_data[date_str]["avg_sentiment"] += agg.avg_sentiment * agg.article_count
        date_data[date_str]["positive_count"] += agg.positive_count
        date_data[date_str]["negative_count"] += agg.negative_count
        date_data[date_str]["neutral_count"] += agg.neutral_count
    
    # Finalize daily averages
    for day_data in date_data.values():
        if day_data["total_articles"] > 0:
            day_data["avg_sentiment"] /= day_data["total_articles"]
            day_data["avg_sentiment"] = round(day_data["avg_sentiment"], 3)
    
    return {
        "market_sentiment": {
            "overall_sentiment": round(weighted_sentiment, 3),
            "total_articles": total_articles,
            "companies_tracked": len(company_data),
            "sentiment_distribution": {
                "positive_pct": round(total_positive / total_articles * 100, 1) if total_articles > 0 else 0.0,
                "negative_pct": round(total_negative / total_articles * 100, 1) if total_articles > 0 else 0.0,
                "neutral_pct": round(total_neutral / total_articles * 100, 1) if total_articles > 0 else 0.0
            }
        },
        "company_breakdown": sorted(company_data.values(), key=lambda x: x["avg_sentiment"], reverse=True),
        "time_series": sorted(date_data.values(), key=lambda x: x["date"])
    }

# Data ingestion endpoints
@app.post("/api/scrape/trigger")
async def trigger_news_scraping(
    background_tasks: BackgroundTasks,
    include_newsapi: bool = True,
    include_rss: bool = True,
    include_web: bool = True,
    db: Session = Depends(get_db)
):
    """Trigger background news scraping task"""
    
    # Add task to processing queue
    queue_item = ProcessingQueue(
        task_type="scrape_news",
        payload={
            "include_newsapi": include_newsapi,
            "include_rss": include_rss,
            "include_web": include_web,
            "requested_at": datetime.utcnow().isoformat()
        },
        priority=7  # High priority
    )
    
    db.add(queue_item)
    db.commit()
    db.refresh(queue_item)
    
    # Schedule background task
    background_tasks.add_task(
        process_news_scraping_task, 
        queue_item.id, 
        include_newsapi, 
        include_rss, 
        include_web
    )
    
    return {
        "message": "News scraping task queued successfully",
        "task_id": queue_item.id,
        "estimated_completion": "2-5 minutes",
        "sources": {
            "newsapi": include_newsapi,
            "rss_feeds": include_rss,
            "web_scraping": include_web
        }
    }

@app.post("/api/sentiment/analyze")
async def trigger_sentiment_analysis(
    background_tasks: BackgroundTasks,
    article_limit: int = Query(default=100, le=500, ge=1),
    db: Session = Depends(get_db)
):
    """Trigger sentiment analysis for unprocessed articles"""
    
    # Count unprocessed articles
    unprocessed_count = db.query(NewsArticle).filter(
        NewsArticle.is_processed == False
    ).count()
    
    if unprocessed_count == 0:
        return {
            "message": "No unprocessed articles found",
            "unprocessed_count": 0
        }
    
    # Add task to processing queue
    queue_item = ProcessingQueue(
        task_type="analyze_sentiment",
        payload={
            "article_limit": article_limit,
            "unprocessed_count": unprocessed_count,
            "requested_at": datetime.utcnow().isoformat()
        },
        priority=8  # High priority
    )
    
    db.add(queue_item)
    db.commit()
    db.refresh(queue_item)
    
    # Schedule background task
    background_tasks.add_task(
        process_sentiment_analysis_task,
        queue_item.id,
        article_limit
    )
    
    return {
        "message": "Sentiment analysis task queued successfully",
        "task_id": queue_item.id,
        "unprocessed_articles": min(unprocessed_count, article_limit),
        "estimated_completion": f"{min(unprocessed_count, article_limit) // 10 + 1} minutes"
    }

# Processing queue endpoints
@app.get("/api/queue/status")
async def get_queue_status(db: Session = Depends(get_db)):
    """Get current processing queue status"""
    
    # Count tasks by status
    pending_count = db.query(ProcessingQueue).filter(ProcessingQueue.status == "pending").count()
    processing_count = db.query(ProcessingQueue).filter(ProcessingQueue.status == "processing").count()
    completed_count = db.query(ProcessingQueue).filter(
        and_(
            ProcessingQueue.status == "completed",
            ProcessingQueue.completed_at >= datetime.utcnow() - timedelta(hours=24)
        )
    ).count()
    failed_count = db.query(ProcessingQueue).filter(ProcessingQueue.status == "failed").count()
    
    # Get recent tasks
    recent_tasks = db.query(ProcessingQueue).order_by(
        desc(ProcessingQueue.created_at)
    ).limit(10).all()
    
    return {
        "queue_stats": {
            "pending": pending_count,
            "processing": processing_count,
            "completed_24h": completed_count,
            "failed": failed_count
        },
        "recent_tasks": [
            {
                "id": task.id,
                "task_type": task.task_type,
                "status": task.status,
                "created_at": task.created_at.isoformat(),
                "completed_at": task.completed_at.isoformat() if task.completed_at else None,
                "error_message": task.error_message
            } for task in recent_tasks
        ]
    }

@app.get("/api/queue/task/{task_id}")
async def get_task_status(task_id: int, db: Session = Depends(get_db)):
    """Get specific task status"""
    task = db.query(ProcessingQueue).filter(ProcessingQueue.id == task_id).first()
    if not task:
        raise HTTPException(status_code=404, detail="Task not found")
    
    return {
        "id": task.id,
        "task_type": task.task_type,
        "status": task.status,
        "payload": task.payload,
        "attempts": task.attempts,
        "max_attempts": task.max_attempts,
        "priority": task.priority,
        "created_at": task.created_at.isoformat(),
        "scheduled_at": task.scheduled_at.isoformat(),
        "started_at": task.started_at.isoformat() if task.started_at else None,
        "completed_at": task.completed_at.isoformat() if task.completed_at else None,
        "error_message": task.error_message
    }

# Statistics and monitoring endpoints
@app.get("/api/stats/overview")
async def get_platform_stats(db: Session = Depends(get_db)):
    """Get comprehensive platform statistics"""
    
    # Basic counts
    total_companies = db.query(Company).filter(Company.is_active == True).count()
    total_articles = db.query(NewsArticle).count()
    processed_articles = db.query(NewsArticle).filter(NewsArticle.is_processed == True).count()
    total_sentiment_scores = db.query(SentimentScore).count()
    
    # Recent activity (last 24 hours)
    last_24h = datetime.utcnow() - timedelta(hours=24)
    recent_articles = db.query(NewsArticle).filter(NewsArticle.scraped_at >= last_24h).count()
    recent_sentiment = db.query(SentimentScore).filter(SentimentScore.processed_at >= last_24h).count()
    
    # Top companies by mention count (last 7 days)
    last_week = datetime.utcnow() - timedelta(days=7)
    top_companies = db.query(
        Company.symbol,
        Company.name,
        func.count(CompanyMention.id).label('mention_count')
    ).join(CompanyMention).join(NewsArticle).filter(
        NewsArticle.published_at >= last_week
    ).group_by(Company.id, Company.symbol, Company.name).order_by(
        desc('mention_count')
    ).limit(10).all()
    
    # Average sentiment by company (last 7 days)
    avg_sentiment = db.query(
        Company.symbol,
        func.avg(SentimentScore.sentiment_score).label('avg_sentiment'),
        func.count(SentimentScore.id).label('score_count')
    ).join(SentimentScore).join(NewsArticle).filter(
        NewsArticle.published_at >= last_week
    ).group_by(Company.id, Company.symbol).order_by(
        desc('avg_sentiment')
    ).all()
    
    return {
        "overview": {
            "total_companies": total_companies,
            "total_articles": total_articles,
            "processed_articles": processed_articles,
            "processing_rate": round(processed_articles / total_articles * 100, 1) if total_articles > 0 else 0,
            "total_sentiment_scores": total_sentiment_scores,
            "recent_activity_24h": {
                "new_articles": recent_articles,
                "sentiment_analyses": recent_sentiment
            }
        },
        "top_mentioned_companies": [
            {
                "symbol": company.symbol,
                "name": company.name,
                "mentions": int(company.mention_count)
            } for company in top_companies
        ],
        "sentiment_leaders": [
            {
                "symbol": sentiment.symbol,
                "avg_sentiment": round(float(sentiment.avg_sentiment), 3),
                "analysis_count": int(sentiment.score_count)
            } for sentiment in avg_sentiment if sentiment.score_count >= 5  # Minimum 5 analyses
        ]
    }

# Background task functions
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
                    company = db.query(Company).filter(
                        Company.symbol == company_symbol
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
                            company_id=company.id,
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
                mentions = db.query(CompanyMention).join(Company).filter(
                    CompanyMention.article_id == article.id
                ).all()
                
                article_dict = {
                    'id': article.id,
                    'title': article.title,
                    'content': article.content or '',
                    'url': article.source_url,
                    'company_mentions': [mention.company.symbol for mention in mentions]
                }
                article_data.append(article_dict)
        
        if not article_data:
            logger.info("No articles to process for sentiment analysis")
            return
        
        # Process sentiment
        results = await sentiment_pipeline.process_articles(article_data)
        
        # Store sentiment results
        with next(get_db()) as db:
            stored_count = 0
            
            for result in results['individual_results']:
                # Get article and company
                article = db.query(NewsArticle).filter(NewsArticle.id == result['article_id']).first()
                company = db.query(Company).filter(Company.symbol == result['company_symbol']).first()
                
                if not article or not company:
                    continue
                
                # Check if sentiment already exists
                existing = db.query(SentimentScore).filter(
                    and_(
                        SentimentScore.article_id == article.id,
                        SentimentScore.company_id == company.id,
                        SentimentScore.model_name == result['model_name']
                    )
                ).first()
                
                if existing:
                    continue
                
                # Create sentiment score
                sentiment_score = SentimentScore(
                    article_id=article.id,
                    company_id=company.id,
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

# Error handlers
@app.exception_handler(HTTPException)
async def http_exception_handler(request, exc):
    logger.error("HTTP exception occurred",
                path=request.url.path,
                method=request.method,
                status_code=exc.status_code,
                detail=exc.detail)
    
    return JSONResponse(
        status_code=exc.status_code,
        content={
            "error": exc.detail,
            "status_code": exc.status_code,
            "timestamp": datetime.utcnow().isoformat()
        }
    )

@app.exception_handler(Exception)
async def general_exception_handler(request, exc):
    logger.error("Unhandled exception occurred",
                path=request.url.path,
                method=request.method,
                error=str(exc),
                exc_info=True)
    
    return JSONResponse(
        status_code=500,
        content={
            "error": "Internal server error",
            "status_code": 500,
            "timestamp": datetime.utcnow().isoformat()
        }
    )

# Startup message
if __name__ == "__main__":
    import uvicorn
    
    print("""
    🚀 Market Intelligence Platform API Starting...
    
    📊 Features:
    • Real-time financial news scraping
    • Advanced sentiment analysis
    • <200ms API response times
    • Comprehensive company tracking
    
    📋 Endpoints:
    • GET /docs - API documentation
    • GET /health - Health check
    • GET /api/companies - Company list
    • GET /api/companies/{symbol}/sentiment - Company sentiment
    • GET /api/news/latest - Latest news
    • POST /api/scrape/trigger - Trigger news scraping
    
    🔗 Access: http://localhost:8000/docs
    """)
    
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
        log_level="info"
    )