from contextlib import asynccontextmanager
from app.core.logging import logger
from app.database import init_db
from data_ingestion.news_scraper import MarketNewsScraper
from data_ingestion.sentiment_processor import SentimentAnalysisAPI
import os

news_scraper = None
sentiment_pipeline = None

@asynccontextmanager
async def lifespan(app):
    logger.info("Starting Market Intelligence API...")
    try:
        init_db()
        logger.info("Database initialized successfully")
    except Exception as e:
        logger.error("Database initialization failed", error=str(e))
        raise

    global news_scraper, sentiment_pipeline
    news_scraper = MarketNewsScraper(os.getenv("NEWS_API_KEY"))
    sentiment_pipeline = SentimentAnalysisAPI()

    yield
    logger.info("Shutting down Market Intelligence API...")

