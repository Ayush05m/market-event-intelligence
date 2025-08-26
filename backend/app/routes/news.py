from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from datetime import datetime, timedelta
from sqlalchemy import desc, and_
from typing import List, Optional
from app.database import get_db
from app.models import Company as CompanySchema, SentimentScore, NewsArticle, SentimentAggregate, CompanyMention

router = APIRouter(prefix="/api/news", tags=["News"])

@router.get("/latest", response_model=List[NewsArticle])
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
        company = db.query(CompanySchema).filter(CompanySchema.symbol == company_symbol.upper()).first()
        if not company:
            raise HTTPException(status_code=404, detail="Company not found")
        
        # Join with company mentions
        query = query.join(CompanyMention).filter(
            CompanyMention.company_id == CompanySchema.id
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
        mentions = db.query(CompanyMention).join(CompanySchema).filter(
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
                    "symbol": mention.CompanySchema.symbol,
                    "name": mention.CompanySchema.name,
                    "relevance_score": float(mention.relevance_score)
                } for mention in mentions
            ],
            "sentiment_scores": [
                {
                    "company_symbol": score.CompanySchema.symbol,
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

@router.get("/sentiment/aggregate")
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
        symbol = agg.CompanySchema.symbol
        if symbol not in company_data:
            company_data[symbol] = {
                "symbol": symbol,
                "name": agg.CompanySchema.name,
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
