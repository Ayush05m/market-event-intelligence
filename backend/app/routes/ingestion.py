
from fastapi import APIRouter, BackgroundTasks, Depends, Query
from sqlalchemy.orm import Session
from datetime import datetime
from app.database import get_db
from app.models import NewsArticle, ProcessingQueue
from app.tasks import process_news_scraping_task, process_sentiment_analysis_task

router = APIRouter(prefix="/api", tags=["Ingestion"])

@router.post("/api/scrape/trigger")
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

@router.post("/sentiment/analyze")
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
