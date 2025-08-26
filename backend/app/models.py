"""
SQLAlchemy models for Market Intelligence Platform
"""
from datetime import datetime, date
from decimal import Decimal
from typing import Optional, List
from uuid import uuid4

from sqlalchemy import (
    Boolean, Column, Date, DateTime, ForeignKey, Integer, 
    String, Text, DECIMAL, BigInteger, JSON
)
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func

Base = declarative_base()


class Company(Base):
    __tablename__ = "companies"
    
    id = Column(Integer, primary_key=True, index=True)
    symbol = Column(String(10), unique=True, nullable=False, index=True)
    name = Column(String(200), nullable=False)
    sector = Column(String(100))
    market_cap = Column(BigInteger)
    country = Column(String(50), default="US")
    is_active = Column(Boolean, default=True, index=True)
    created_at = Column(DateTime, default=func.now())
    updated_at = Column(DateTime, default=func.now(), onupdate=func.now())
    
    # Relationships
    mentions = relationship("CompanyMention", back_populates="company", cascade="all, delete-orphan")
    sentiment_scores = relationship("SentimentScore", back_populates="company", cascade="all, delete-orphan")
    sentiment_aggregates = relationship("SentimentAggregate", back_populates="company", cascade="all, delete-orphan")
    market_events = relationship("MarketEvent", back_populates="company", cascade="all, delete-orphan")


class NewsSource(Base):
    __tablename__ = "news_sources"
    
    id = Column(Integer, primary_key=True, index=True)
    name = Column(String(100), unique=True, nullable=False)
    base_url = Column(String(500))
    api_key_required = Column(Boolean, default=False)
    rate_limit_per_hour = Column(Integer, default=100)
    reliability_score = Column(DECIMAL(2, 1), default=5.0)
    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime, default=func.now())
    
    # Relationships
    articles = relationship("NewsArticle", back_populates="source")


class NewsArticle(Base):
    __tablename__ = "news_articles"
    
    id = Column(Integer, primary_key=True, index=True)
    uuid = Column(UUID(as_uuid=True), default=uuid4, unique=True)
    title = Column(String(500), nullable=False)
    content = Column(Text)
    summary = Column(Text)
    source_id = Column(Integer, ForeignKey("news_sources.id"), index=True)
    source_url = Column(String(1000), unique=True)
    author = Column(String(200))
    published_at = Column(DateTime, index=True)
    scraped_at = Column(DateTime, default=func.now(), index=True)
    word_count = Column(Integer)
    language = Column(String(10), default="en")
    is_processed = Column(Boolean, default=False, index=True)
    created_at = Column(DateTime, default=func.now())
    updated_at = Column(DateTime, default=func.now(), onupdate=func.now())
    
    # Relationships
    source = relationship("NewsSource", back_populates="articles")
    mentions = relationship("CompanyMention", back_populates="article", cascade="all, delete-orphan")
    sentiment_scores = relationship("SentimentScore", back_populates="article", cascade="all, delete-orphan")


class CompanyMention(Base):
    __tablename__ = "company_mentions"
    
    id = Column(Integer, primary_key=True, index=True)
    article_id = Column(Integer, ForeignKey("news_articles.id"), index=True)
    company_id = Column(Integer, ForeignKey("companies.id"), index=True)
    mention_count = Column(Integer, default=1)
    relevance_score = Column(DECIMAL(3, 2), default=0.50, index=True)
    context_window = Column(Text)
    created_at = Column(DateTime, default=func.now())
    
    # Relationships
    article = relationship("NewsArticle", back_populates="mentions")
    company = relationship("Company", back_populates="mentions")


class SentimentScore(Base):
    __tablename__ = "sentiment_scores"
    
    id = Column(Integer, primary_key=True, index=True)
    article_id = Column(Integer, ForeignKey("news_articles.id"), index=True)
    company_id = Column(Integer, ForeignKey("companies.id"), index=True)
    sentiment_score = Column(DECIMAL(4, 3), nullable=False, index=True)
    confidence = Column(DECIMAL(3, 2), nullable=False)
    sentiment_label = Column(String(20), nullable=False)
    model_name = Column(String(50), default="vader")
    model_version = Column(String(20), default="1.0")
    compound_score = Column(DECIMAL(4, 3))
    positive_score = Column(DECIMAL(3, 2))
    negative_score = Column(DECIMAL(3, 2))
    neutral_score = Column(DECIMAL(3, 2))
    processed_at = Column(DateTime, default=func.now(), index=True)
    
    # Relationships
    article = relationship("NewsArticle", back_populates="sentiment_scores")
    company = relationship("Company", back_populates="sentiment_scores")


class SentimentAggregate(Base):
    __tablename__ = "sentiment_aggregates"
    
    id = Column(Integer, primary_key=True, index=True)
    company_id = Column(Integer, ForeignKey("companies.id"), index=True)
    date = Column(Date, nullable=False, index=True)
    hour = Column(Integer)  # NULL for daily aggregates, 0-23 for hourly
    avg_sentiment = Column(DECIMAL(4, 3), nullable=False)
    sentiment_stddev = Column(DECIMAL(4, 3))
    article_count = Column(Integer, nullable=False)
    positive_count = Column(Integer, default=0)
    negative_count = Column(Integer, default=0)
    neutral_count = Column(Integer, default=0)
    total_mentions = Column(Integer, default=0)
    created_at = Column(DateTime, default=func.now())
    updated_at = Column(DateTime, default=func.now(), onupdate=func.now())
    
    # Relationships
    company = relationship("Company", back_populates="sentiment_aggregates")


class MarketEvent(Base):
    __tablename__ = "market_events"
    
    id = Column(Integer, primary_key=True, index=True)
    title = Column(String(300), nullable=False)
    description = Column(Text)
    event_type = Column(String(50), nullable=False)
    company_id = Column(Integer, ForeignKey("companies.id"), index=True)
    event_date = Column(Date, nullable=False, index=True)
    impact_score = Column(DECIMAL(2, 1))
    source = Column(String(100))
    is_verified = Column(Boolean, default=False)
    created_at = Column(DateTime, default=func.now())
    
    # Relationships
    company = relationship("Company", back_populates="market_events")


class ProcessingQueue(Base):
    __tablename__ = "processing_queue"
    
    id = Column(Integer, primary_key=True, index=True)
    task_type = Column(String(50), nullable=False)
    payload = Column(JSON, nullable=False)
    status = Column(String(20), default="pending", index=True)
    priority = Column(Integer, default=5, index=True)
    attempts = Column(Integer, default=0)
    max_attempts = Column(Integer, default=3)
    error_message = Column(Text)
    scheduled_at = Column(DateTime, default=func.now(), index=True)
    started_at = Column(DateTime)
    completed_at = Column(DateTime)
    created_at = Column(DateTime, default=func.now())


# Pydantic schemas for API serialization
from pydantic import BaseModel, Field
from typing import Optional, List
from datetime import datetime, date
from decimal import Decimal


class CompanyBase(BaseModel):
    symbol: str = Field(..., max_length=10)
    name: str = Field(..., max_length=200)
    sector: Optional[str] = Field(None, max_length=100)
    market_cap: Optional[int] = None
    country: str = Field(default="US", max_length=50)
    is_active: bool = True


class CompanyCreate(CompanyBase):
    pass


class Company(CompanyBase):
    id: int
    created_at: datetime
    updated_at: datetime
    
    class Config:
        from_attributes = True


class NewsSourceBase(BaseModel):
    name: str = Field(..., max_length=100)
    base_url: Optional[str] = Field(None, max_length=500)
    api_key_required: bool = False
    rate_limit_per_hour: int = 100
    reliability_score: Decimal = Field(default=5.0, ge=0, le=10)
    is_active: bool = True


class NewsSourceCreate(NewsSourceBase):
    pass


class NewsSource(NewsSourceBase):
    id: int
    created_at: datetime
    
    class Config:
        from_attributes = True


class NewsArticleBase(BaseModel):
    title: str = Field(..., max_length=500)
    content: Optional[str] = None
    summary: Optional[str] = None
    source_url: Optional[str] = Field(None, max_length=1000)
    author: Optional[str] = Field(None, max_length=200)
    published_at: Optional[datetime] = None
    word_count: Optional[int] = None
    language: str = Field(default="en", max_length=10)


class NewsArticleCreate(NewsArticleBase):
    source_id: int


class NewsArticle(NewsArticleBase):
    id: int
    uuid: str
    source_id: int
    scraped_at: datetime
    is_processed: bool
    created_at: datetime
    updated_at: datetime
    source: Optional[NewsSource] = None
    
    class Config:
        from_attributes = True


class CompanyMentionBase(BaseModel):
    mention_count: int = 1
    relevance_score: Decimal = Field(default=0.50, ge=0, le=1)
    context_window: Optional[str] = None


class CompanyMentionCreate(CompanyMentionBase):
    article_id: int
    company_id: int


class CompanyMention(CompanyMentionBase):
    id: int
    article_id: int
    company_id: int
    created_at: datetime
    company: Optional[Company] = None
    
    class Config:
        from_attributes = True


class SentimentScoreBase(BaseModel):
    sentiment_score: Decimal = Field(..., ge=-1, le=1)
    confidence: Decimal = Field(..., ge=0, le=1)
    sentiment_label: str = Field(..., max_length=20)
    model_name: str = Field(default="vader", max_length=50)
    model_version: str = Field(default="1.0", max_length=20)
    compound_score: Optional[Decimal] = Field(None, ge=-1, le=1)
    positive_score: Optional[Decimal] = Field(None, ge=0, le=1)
    negative_score: Optional[Decimal] = Field(None, ge=0, le=1)
    neutral_score: Optional[Decimal] = Field(None, ge=0, le=1)


class SentimentScoreCreate(SentimentScoreBase):
    article_id: int
    company_id: int


class SentimentScore(SentimentScoreBase):
    id: int
    article_id: int
    company_id: int
    processed_at: datetime
    company: Optional[Company] = None
    article: Optional[NewsArticle] = None
    
    class Config:
        from_attributes = True


class ProcessingQueueBase(BaseModel):
    task_type: str = Field(..., max_length=50)
    payload: dict
    priority: int = Field(default=5, ge=1, le=10)
    max_attempts: int = Field(default=3, ge=1, le=10)


class ProcessingQueueCreate(ProcessingQueueBase):
    pass


class ProcessingQueue(ProcessingQueueBase):
    id: int
    status: str
    attempts: int
    error_message: Optional[str] = None
    scheduled_at: datetime
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    created_at: datetime
    
    class Config:
        from_attributes = True  