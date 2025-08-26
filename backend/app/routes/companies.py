from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from datetime import datetime, timedelta
from sqlalchemy import desc, and_
from typing import List
from app.database import get_db
from app.models import Company as CompanySchema, SentimentScore, NewsArticle, SentimentAggregate
from app.models import Company as CompanyORM  # Assuming ORM model is defined in models.py
router = APIRouter(prefix="/api/companies", tags=["Companies"])
@router.get("/companies", response_model=List[CompanySchema])
async def get_companies(
    skip: int = 0,
    limit: int = 100,
    active_only: bool = True,
    db: Session = Depends(get_db)
):
    """Get list of tracked companies"""
    query = db.query(CompanyORM)  # Use SQLAlchemy ORM model here
    
    if active_only:
        query = query.filter(CompanyORM.is_active == True)
    
    companies = query.offset(skip).limit(limit).all()
    return companies

@router.get("/companies/{symbol}", response_model=CompanySchema)
async def get_company(symbol: str, db: Session = Depends(get_db)):
    """Get specific company by symbol"""
    company = db.query(CompanySchema).filter(CompanySchema.symbol == symbol.upper()).first()
    if not company:
        raise HTTPException(status_code=404, detail="Company not found")
    return company

@router.get("/companies/{symbol}/sentiment")
async def get_company_sentiment(
    symbol: str,
    days: int = Query(default=30, le=90, ge=1),
    db: Session = Depends(get_db)
):
    """Get sentiment analysis for a company over specified days"""
    company = db.query(CompanySchema).filter(CompanySchema.symbol == symbol.upper()).first()
    if not company:
        raise HTTPException(status_code=404, detail="Company not found")
    
    # Calculate date range
    end_date = datetime.now()
    start_date = end_date - timedelta(days=days)
    
    # Get sentiment scores
    sentiment_scores = db.query(SentimentScore).join(NewsArticle).filter(
        and_(
            SentimentScore.company_id == CompanySchema.id,
            NewsArticle.published_at >= start_date,
            NewsArticle.published_at <= end_date
        )
    ).order_by(desc(NewsArticle.published_at)).all()
    
    # Get aggregated data
    aggregates = db.query(SentimentAggregate).filter(
        and_(
            SentimentAggregate.company_id == CompanySchema.id,
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
            "symbol": CompanySchema.symbol,
            "name": CompanySchema.name,
            "sector": CompanySchema.sector
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
