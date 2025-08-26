"""
Database connection and session management
"""
import os
from typing import Generator
import logging

from sqlalchemy import create_engine, event
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.pool import StaticPool

logger = logging.getLogger(__name__)

# Database URL from environment
DATABASE_URL = os.getenv(
    "DATABASE_URL", 
    "postgresql://admin:password@localhost:5432/market_intelligence"
)

# Create SQLAlchemy engine
engine = create_engine(
    DATABASE_URL,
    pool_pre_ping=True,
    pool_recycle=300,
    pool_size=20,
    max_overflow=30,
    echo=os.getenv("DEBUG", "false").lower() == "true",
)

# Create SessionLocal class
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# Base class for models
Base = declarative_base()


def get_db() -> Generator[Session, None, None]:
    """
    Database dependency for FastAPI
    """
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def init_db() -> None:
    """
    Initialize database tables
    """
    try:
        # Import all models to ensure they're registered with Base
        from app.models import (
            Company, NewsSource, NewsArticle, CompanyMention,
            SentimentScore, SentimentAggregate, MarketEvent, ProcessingQueue
        )
        
        # Create all tables
        Base.metadata.create_all(bind=engine)
        logger.info("Database tables initialized successfully")
        
    except Exception as e:
        logger.error(f"Failed to initialize database: {e}")
        raise


def get_db_stats() -> dict:
    """
    Get database statistics for monitoring
    """
    try:
        with SessionLocal() as db:
            stats = {}
            
            # Import models
            from app.models import (
                Company, NewsArticle, SentimentScore, CompanyMention
            )
            
            # Count records in key tables
            stats['companies'] = db.query(Company).filter(Company.is_active == True).count()
            stats['total_articles'] = db.query(NewsArticle).count()
            stats['processed_articles'] = db.query(NewsArticle).filter(NewsArticle.is_processed == True).count()
            stats['sentiment_scores'] = db.query(SentimentScore).count()
            stats['company_mentions'] = db.query(CompanyMention).count()
            
            # Calculate processing rate
            if stats['total_articles'] > 0:
                stats['processing_rate'] = round(
                    (stats['processed_articles'] / stats['total_articles']) * 100, 2
                )
            else:
                stats['processing_rate'] = 0
            
            return stats
            
    except Exception as e:
        logger.error(f"Failed to get database stats: {e}")
        return {}


class DatabaseManager:
    """
    Database manager for advanced operations
    """
    
    def __init__(self):
        self.engine = engine
        self.SessionLocal = SessionLocal
    
    def health_check(self) -> bool:
        """
        Check if database is accessible
        """
        try:
            with self.SessionLocal() as db:
                db.execute("SELECT 1")
            return True
        except Exception as e:
            logger.error(f"Database health check failed: {e}")
            return False
    
    def get_connection_info(self) -> dict:
        """
        Get database connection information
        """
        return {
            'url': DATABASE_URL.replace(DATABASE_URL.split('@')[0].split('//')[1], '***'),
            'pool_size': engine.pool.size(),
            'checked_in': engine.pool.checkedin(),
            'checked_out': engine.pool.checkedout(),
            'overflow': engine.pool.overflow(),
        }
    
    def execute_raw_query(self, query: str, params: dict = None) -> list:
        """
        Execute raw SQL query (use with caution)
        """
        try:
            with self.SessionLocal() as db:
                result = db.execute(query, params or {})
                return result.fetchall()
        except Exception as e:
            logger.error(f"Raw query execution failed: {e}")
            raise
    
    def vacuum_analyze(self) -> bool:
        """
        Run VACUUM ANALYZE for PostgreSQL maintenance
        """
        try:
            with engine.connect() as conn:
                # Note: VACUUM cannot be run inside a transaction
                conn.execute("VACUUM ANALYZE")
            logger.info("Database vacuum analyze completed")
            return True
        except Exception as e:
            logger.error(f"Vacuum analyze failed: {e}")
            return False


# Global database manager instance
db_manager = DatabaseManager()


# Event listeners for connection management
@event.listens_for(engine, "connect")
def set_sqlite_pragma(dbapi_connection, connection_record):
    """
    Set database-specific connection parameters
    """
    if 'sqlite' in DATABASE_URL:
        cursor = dbapi_connection.cursor()
        cursor.execute("PRAGMA foreign_keys=ON")
        cursor.close()


@event.listens_for(engine, "checkout")
def receive_checkout(dbapi_connection, connection_record, connection_proxy):
    """
    Log database connection checkout
    """
    logger.debug("Database connection checked out")


@event.listens_for(engine, "checkin")
def receive_checkin(dbapi_connection, connection_record):
    """
    Log database connection checkin
    """
    logger.debug("Database connection checked in")


# Database utilities
def create_test_data():
    """
    Create test data for development
    """
    try:
        from app.models import Company, NewsSource
        
        with SessionLocal() as db:
            # Check if data already exists
            if db.query(Company).count() > 0:
                logger.info("Test data already exists, skipping creation")
                return
            
            # Create sample companies
            companies = [
                Company(symbol="AAPL", name="Apple Inc.", sector="Technology", market_cap=3000000000000),
                Company(symbol="GOOGL", name="Alphabet Inc.", sector="Technology", market_cap=1800000000000),
                Company(symbol="MSFT", name="Microsoft Corporation", sector="Technology", market_cap=2800000000000),
                Company(symbol="TSLA", name="Tesla Inc.", sector="Automotive", market_cap=800000000000),
                Company(symbol="AMZN", name="Amazon.com Inc.", sector="E-commerce", market_cap=1500000000000),
            ]
            
            # Create sample news sources
            sources = [
                NewsSource(name="NewsAPI", base_url="https://newsapi.org", api_key_required=True, rate_limit_per_hour=100),
                NewsSource(name="Yahoo Finance RSS", base_url="https://finance.yahoo.com/news", rate_limit_per_hour=1000),
                NewsSource(name="Reuters Business", base_url="https://www.reuters.com/business", rate_limit_per_hour=500),
            ]
            
            db.add_all(companies + sources)
            db.commit()
            logger.info("Test data created successfully")
            
    except Exception as e:
        logger.error(f"Failed to create test data: {e}")
        raise


# Async database operations (for async endpoints)
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker

# Async engine setup
async_engine = None
AsyncSessionLocal = None

def init_async_db():
    """
    Initialize async database connection
    """
    global async_engine, AsyncSessionLocal
    
    if async_engine is None:
        # Convert sync DATABASE_URL to async
        async_url = DATABASE_URL.replace("postgresql://", "postgresql+asyncpg://")
        
        async_engine = create_async_engine(
            async_url,
            pool_pre_ping=True,
            pool_recycle=300,
            pool_size=10,
            max_overflow=20,
            echo=os.getenv("DEBUG", "false").lower() == "true",
        )
        
        AsyncSessionLocal = async_sessionmaker(
            async_engine, class_=AsyncSession, expire_on_commit=False
        )


async def get_async_db() -> AsyncSession:
    """
    Async database dependency for FastAPI
    """
    if AsyncSessionLocal is None:
        init_async_db()
    
    async with AsyncSessionLocal() as session:
        try:
            yield session
        finally:
            await session.close()


# Database middleware for request tracking
import time
from contextlib import contextmanager

@contextmanager
def db_transaction_timer():
    """
    Context manager to time database transactions
    """
    start_time = time.time()
    try:
        yield
    finally:
        duration = time.time() - start_time
        if duration > 1.0:  # Log slow queries (>1 second)
            logger.warning(f"Slow database transaction: {duration:.2f}s")
        else:
            logger.debug(f"Database transaction completed in {duration:.3f}s")


# Connection pool monitoring
def monitor_connection_pool():
    """
    Monitor database connection pool status
    """
    pool = engine.pool
    return {
        'size': pool.size(),
        'checked_in': pool.checkedin(),
        'checked_out': pool.checkedout(),
        'overflow': pool.overflow(),
        'invalid': getattr(pool, 'invalid', 0),
    }