"""
Comprehensive news scraping system for financial data
Supports NewsAPI, RSS feeds, and web scraping
"""
import asyncio
import aiohttp
import feedparser
import requests
import logging
import re
import time
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Set
from dataclasses import dataclass
from urllib.parse import urljoin, urlparse
from bs4 import BeautifulSoup
import hashlib
from asyncio_throttle import Throttler

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@dataclass
class NewsArticle:
    title: str
    content: str
    summary: Optional[str]
    source: str
    url: str
    author: Optional[str]
    published_at: Optional[datetime]
    scraped_at: datetime
    word_count: int
    company_mentions: List[str]
    
    def to_dict(self) -> dict:
        return {
            'title': self.title,
            'content': self.content,
            'summary': self.summary,
            'source': self.source,
            'url': self.url,
            'author': self.author,
            'published_at': self.published_at.isoformat() if self.published_at else None,
            'scraped_at': self.scraped_at.isoformat(),
            'word_count': self.word_count,
            'company_mentions': self.company_mentions
        }
    
    def get_hash(self) -> str:
        """Generate unique hash for deduplication"""
        content_hash = hashlib.md5(
            f"{self.title}{self.url}".encode('utf-8')
        ).hexdigest()
        return content_hash


class CompanyMatcher:
    """
    Intelligent company/ticker recognition in news text
    """
    
    def __init__(self):
        # Major company symbols and aliases
        self.companies = {
            'AAPL': ['Apple', 'Apple Inc', 'AAPL', 'iPhone', 'iPad', 'Mac', 'Tim Cook'],
            'GOOGL': ['Google', 'Alphabet', 'GOOGL', 'GOOG', 'YouTube', 'Gmail', 'Sundar Pichai'],
            'MSFT': ['Microsoft', 'MSFT', 'Windows', 'Office', 'Azure', 'Satya Nadella'],
            'TSLA': ['Tesla', 'TSLA', 'Elon Musk', 'Model S', 'Model 3', 'Model Y', 'Cybertruck'],
            'AMZN': ['Amazon', 'AMZN', 'AWS', 'Prime', 'Jeff Bezos', 'Andy Jassy'],
            'META': ['Meta', 'Facebook', 'META', 'FB', 'Instagram', 'WhatsApp', 'Mark Zuckerberg'],
            'NVDA': ['NVIDIA', 'NVDA', 'GeForce', 'RTX', 'Jensen Huang'],
            'NFLX': ['Netflix', 'NFLX', 'Reed Hastings'],
            'JPM': ['JPMorgan', 'JP Morgan', 'JPM', 'Chase', 'Jamie Dimon'],
            'JNJ': ['Johnson & Johnson', 'J&J', 'JNJ', 'Janssen'],
            'V': ['Visa', 'V Inc', 'credit card'],
            'PG': ['Procter & Gamble', 'P&G', 'PG Inc'],
            'UNH': ['UnitedHealth', 'UNH', 'United Healthcare'],
            'HD': ['Home Depot', 'HD Inc'],
            'MA': ['Mastercard', 'MA Inc', 'MasterCard']
        }
        
        # Compile regex patterns for efficient matching
        self.patterns = {}
        for symbol, keywords in self.companies.items():
            # Create case-insensitive pattern
            pattern = '|'.join(re.escape(keyword) for keyword in keywords)
            self.patterns[symbol] = re.compile(pattern, re.IGNORECASE)
    
    def find_companies(self, text: str) -> List[str]:
        """Find mentioned companies in text"""
        mentioned = []
        text_lower = text.lower()
        
        for symbol, pattern in self.patterns.items():
            if pattern.search(text):
                mentioned.append(symbol)
        
        return list(set(mentioned))  # Remove duplicates
    
    def get_relevance_score(self, text: str, symbol: str) -> float:
        """Calculate relevance score for a company mention"""
        if symbol not in self.companies:
            return 0.0
        
        keywords = self.companies[symbol]
        mentions = sum(1 for keyword in keywords if keyword.lower() in text.lower())
        
        # Score based on mention frequency and keyword importance
        base_score = min(mentions / len(keywords), 1.0)
        
        # Boost score if company symbol is mentioned directly
        if symbol in text.upper():
            base_score += 0.3
        
        return min(base_score, 1.0)


class NewsAPIClient:
    """
    NewsAPI.org integration for financial news
    """
    
    def __init__(self, api_key: str):
        self.api_key = api_key
        self.base_url = "https://newsapi.org/v2"
        self.session = requests.Session()
        self.rate_limit = 100  # Free tier: 100 requests/day
        self.requests_made = 0
        
    def fetch_financial_news(self, query: str = "finance OR stock OR market", 
                           sources: str = None, 
                           from_date: datetime = None,
                           page_size: int = 50) -> List[NewsArticle]:
        """Fetch financial news from NewsAPI"""
        if self.requests_made >= self.rate_limit:
            logger.warning("NewsAPI rate limit reached")
            return []
        
        params = {
            'q': query,
            'apiKey': self.api_key,
            'language': 'en',
            'sortBy': 'publishedAt',
            'pageSize': min(page_size, 100)
        }
        
        if sources:
            params['sources'] = sources
        
        if from_date:
            params['from'] = from_date.strftime('%Y-%m-%d')
        
        try:
            response = self.session.get(f"{self.base_url}/everything", params=params)
            response.raise_for_status()
            self.requests_made += 1
            
            data = response.json()
            articles = []
            company_matcher = CompanyMatcher()
            
            for article_data in data.get('articles', []):
                if not article_data.get('title') or not article_data.get('url'):
                    continue
                
                # Parse published date
                published_at = None
                if article_data.get('publishedAt'):
                    try:
                        published_at = datetime.fromisoformat(
                            article_data['publishedAt'].replace('Z', '+00:00')
                        )
                    except:
                        pass
                
                # Extract content
                content = article_data.get('content', '') or article_data.get('description', '')
                title = article_data['title']
                full_text = f"{title} {content}"
                
                article = NewsArticle(
                    title=title,
                    content=content,
                    summary=article_data.get('description'),
                    source='NewsAPI',
                    url=article_data['url'],
                    author=article_data.get('author'),
                    published_at=published_at,
                    scraped_at=datetime.now(),
                    word_count=len(full_text.split()),
                    company_mentions=company_matcher.find_companies(full_text)
                )
                
                articles.append(article)
            
            logger.info(f"Fetched {len(articles)} articles from NewsAPI")
            return articles
            
        except requests.RequestException as e:
            logger.error(f"NewsAPI request failed: {e}")
            return []


class RSSFeedScraper:
    """
    RSS feed scraper for financial news sources
    """
    
    def __init__(self):
        self.feeds = {
            'Yahoo Finance': 'https://finance.yahoo.com/news/rssindex',
            'MarketWatch': 'https://www.marketwatch.com/rss/realtimeheadlines',
            'Reuters Business': 'https://www.reuters.com/business/finance/rss',
            'CNBC': 'https://www.cnbc.com/id/10001147/device/rss/rss.html',
            'Bloomberg': 'https://feeds.bloomberg.com/markets/news.rss'
        }
        
    async def fetch_feed(self, feed_name: str, feed_url: str, 
                        session: aiohttp.ClientSession) -> List[NewsArticle]:
        """Fetch articles from RSS feed"""
        try:
            async with session.get(feed_url) as response:
                if response.status != 200:
                    logger.warning(f"RSS feed {feed_name} returned status {response.status}")
                    return []
                
                content = await response.text()
                feed = feedparser.parse(content)
                
                articles = []
                company_matcher = CompanyMatcher()
                
                for entry in feed.entries[:20]:  # Limit to 20 most recent
                    if not hasattr(entry, 'title') or not hasattr(entry, 'link'):
                        continue
                    
                    # Parse published date
                    published_at = None
                    if hasattr(entry, 'published_parsed') and entry.published_parsed:
                        published_at = datetime(*entry.published_parsed[:6])
                    elif hasattr(entry, 'updated_parsed') and entry.updated_parsed:
                        published_at = datetime(*entry.updated_parsed[:6])
                    
                    # Extract content
                    content = getattr(entry, 'summary', '') or getattr(entry, 'description', '')
                    title = entry.title
                    full_text = f"{title} {content}"
                    
                    article = NewsArticle(
                        title=title,
                        content=content,
                        summary=content[:200] + "..." if len(content) > 200 else content,
                        source=feed_name,
                        url=entry.link,
                        author=getattr(entry, 'author', None),
                        published_at=published_at,
                        scraped_at=datetime.now(),
                        word_count=len(full_text.split()),
                        company_mentions=company_matcher.find_companies(full_text)
                    )
                    
                    articles.append(article)
                
                logger.info(f"Fetched {len(articles)} articles from {feed_name}")
                return articles
                
        except Exception as e:
            logger.error(f"Failed to fetch RSS feed {feed_name}: {e}")
            return []
    
    async def fetch_all_feeds(self) -> List[NewsArticle]:
        """Fetch articles from all RSS feeds"""
        all_articles = []
        
        async with aiohttp.ClientSession() as session:
            tasks = []
            for feed_name, feed_url in self.feeds.items():
                task = self.fetch_feed(feed_name, feed_url, session)
                tasks.append(task)
            
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            for result in results:
                if isinstance(result, list):
                    all_articles.extend(result)
                elif isinstance(result, Exception):
                    logger.error(f"RSS feed task failed: {result}")
        
        return all_articles


class WebScraper:
    """
    Direct web scraping for financial news sites
    """
    
    def __init__(self):
        self.sites = {
            'Reuters': {
                'base_url': 'https://www.reuters.com',
                'sections': ['/business/finance', '/technology']
            },
            'Yahoo Finance': {
                'base_url': 'https://finance.yahoo.com',
                'sections': ['/news']
            }
        }
        self.throttler = Throttler(rate_limit=1, period=2)  # 1 request per 2 seconds
        
    async def scrape_reuters_article(self, url: str, 
                                   session: aiohttp.ClientSession) -> Optional[NewsArticle]:
        """Scrape individual Reuters article"""
        try:
            async with self.throttler:
                async with session.get(url) as response:
                    if response.status != 200:
                        return None
                    
                    html = await response.text()
                    soup = BeautifulSoup(html, 'lxml')
                    
                    # Extract title
                    title_elem = soup.find('h1', {'data-testid': 'headline'})
                    if not title_elem:
                        return None
                    title = title_elem.get_text(strip=True)
                    
                    # Extract content
                    content_div = soup.find('div', {'data-testid': 'body-content'})
                    content = ""
                    if content_div:
                        paragraphs = content_div.find_all('p')
                        content = " ".join(p.get_text(strip=True) for p in paragraphs)
                    
                    # Extract author
                    author = None
                    author_elem = soup.find('span', {'data-testid': 'author-name'})
                    if author_elem:
                        author = author_elem.get_text(strip=True)
                    
                    # Extract published date
                    published_at = None
                    time_elem = soup.find('time')
                    if time_elem and time_elem.get('datetime'):
                        try:
                            published_at = datetime.fromisoformat(
                                time_elem['datetime'].replace('Z', '+00:00')
                            )
                        except:
                            pass
                    
                    if not content:
                        return None
                    
                    full_text = f"{title} {content}"
                    company_matcher = CompanyMatcher()
                    
                    article = NewsArticle(
                        title=title,
                        content=content,
                        summary=content[:300] + "..." if len(content) > 300 else content,
                        source='Reuters',
                        url=url,
                        author=author,
                        published_at=published_at,
                        scraped_at=datetime.now(),
                        word_count=len(full_text.split()),
                        company_mentions=company_matcher.find_companies(full_text)
                    )
                    
                    return article
                    
        except Exception as e:
            logger.error(f"Failed to scrape Reuters article {url}: {e}")
            return None
    
    async def get_reuters_article_urls(self, section_url: str,
                                     session: aiohttp.ClientSession) -> List[str]:
        """Get article URLs from Reuters section page"""
        try:
            async with self.throttler:
                async with session.get(section_url) as response:
                    if response.status != 200:
                        return []
                    
                    html = await response.text()
                    soup = BeautifulSoup(html, 'lxml')
                    
                    urls = []
                    # Find article links
                    for link in soup.find_all('a', href=True):
                        href = link['href']
                        if '/business/' in href and 'reuters.com' not in href:
                            full_url = urljoin('https://www.reuters.com', href)
                            urls.append(full_url)
                    
                    return urls[:10]  # Limit to 10 articles per section
                    
        except Exception as e:
            logger.error(f"Failed to get Reuters URLs from {section_url}: {e}")
            return []
    
    async def scrape_reuters_section(self, section_url: str) -> List[NewsArticle]:
        """Scrape articles from Reuters section"""
        articles = []
        
        async with aiohttp.ClientSession() as session:
            # Get article URLs
            urls = await self.get_reuters_article_urls(section_url, session)
            
            # Scrape each article
            tasks = []
            for url in urls:
                task = self.scrape_reuters_article(url, session)
                tasks.append(task)
            
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            for result in results:
                if isinstance(result, NewsArticle):
                    articles.append(result)
                elif isinstance(result, Exception):
                    logger.error(f"Article scraping task failed: {result}")
        
        logger.info(f"Scraped {len(articles)} articles from Reuters")
        return articles


class NewsDeduplicator:
    """
    Deduplicate news articles based on content similarity
    """
    
    def __init__(self):
        self.seen_hashes: Set[str] = set()
        self.similarity_threshold = 0.8
    
    def is_duplicate(self, article: NewsArticle) -> bool:
        """Check if article is a duplicate"""
        article_hash = article.get_hash()
        
        if article_hash in self.seen_hashes:
            return True
        
        self.seen_hashes.add(article_hash)
        return False
    
    def deduplicate(self, articles: List[NewsArticle]) -> List[NewsArticle]:
        """Remove duplicate articles"""
        unique_articles = []
        
        for article in articles:
            if not self.is_duplicate(article):
                unique_articles.append(article)
        
        logger.info(f"Removed {len(articles) - len(unique_articles)} duplicate articles")
        return unique_articles


class MarketNewsScraper:
    """
    Main orchestrator for all news scraping operations
    """
    
    def __init__(self, news_api_key: Optional[str] = None):
        self.news_api = NewsAPIClient(news_api_key) if news_api_key else None
        self.rss_scraper = RSSFeedScraper()
        self.web_scraper = WebScraper()
        self.deduplicator = NewsDeduplicator()
        
    async def scrape_all_sources(self, 
                                include_newsapi: bool = True,
                                include_rss: bool = True, 
                                include_web: bool = True) -> List[NewsArticle]:
        """Scrape news from all configured sources"""
        all_articles = []
        
        # Scrape from NewsAPI
        if include_newsapi and self.news_api:
            try:
                newsapi_articles = self.news_api.fetch_financial_news(
                    query="(Apple OR Google OR Microsoft OR Tesla OR Amazon OR Meta OR NVIDIA) AND (stock OR earnings OR financial)",
                    from_date=datetime.now() - timedelta(days=1)
                )
                all_articles.extend(newsapi_articles)
            except Exception as e:
                logger.error(f"NewsAPI scraping failed: {e}")
        
        # Scrape from RSS feeds
        if include_rss:
            try:
                rss_articles = await self.rss_scraper.fetch_all_feeds()
                all_articles.extend(rss_articles)
            except Exception as e:
                logger.error(f"RSS scraping failed: {e}")
        
        # Scrape from web sources
        if include_web:
            try:
                reuters_articles = await self.web_scraper.scrape_reuters_section(
                    'https://www.reuters.com/business/finance'
                )
                all_articles.extend(reuters_articles)
            except Exception as e:
                logger.error(f"Web scraping failed: {e}")
        
        # Deduplicate articles
        unique_articles = self.deduplicator.deduplicate(all_articles)
        
        # Filter articles with company mentions
        relevant_articles = [
            article for article in unique_articles 
            if article.company_mentions
        ]
        
        logger.info(f"Scraped {len(relevant_articles)} unique relevant articles")
        return relevant_articles
    
    def get_scraping_stats(self) -> dict:
        """Get scraping statistics"""
        return {
            'newsapi_requests_made': getattr(self.news_api, 'requests_made', 0) if self.news_api else 0,
            'newsapi_rate_limit': getattr(self.news_api, 'rate_limit', 0) if self.news_api else 0,
            'deduplicator_seen_count': len(self.deduplicator.seen_hashes),
            'supported_companies': len(CompanyMatcher().companies),
        }


# Example usage and testing
async def main():
    """Example usage of the news scraper"""
    import os
    
    # Initialize scraper
    news_api_key = os.getenv('NEWS_API_KEY')  # Get from environment
    scraper = MarketNewsScraper(news_api_key)
    
    # Scrape news from all sources
    articles = await scraper.scrape_all_sources()
    
    # Print results
    print(f"\nFound {len(articles)} relevant articles:")
    for i, article in enumerate(articles[:5]):  # Show first 5
        print(f"\n{i+1}. {article.title}")
        print(f"   Source: {article.source}")
        print(f"   Companies: {', '.join(article.company_mentions)}")
        print(f"   Published: {article.published_at}")
        print(f"   URL: {article.url}")
    
    # Print statistics
    stats = scraper.get_scraping_stats()
    print(f"\nScraping Statistics:")
    for key, value in stats.items():
        print(f"  {key}: {value}")


if __name__ == "__main__":
    asyncio.run(main())