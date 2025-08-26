-- Market Event Intelligence Platform Database Schema
-- Created for PostgreSQL 15+

-- Enable required extensions
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- Companies table - stores tracked companies/tickers
CREATE TABLE companies (
    id SERIAL PRIMARY KEY,
    symbol VARCHAR(10) UNIQUE NOT NULL,
    name VARCHAR(200) NOT NULL,
    sector VARCHAR(100),
    market_cap BIGINT,
    country VARCHAR(50) DEFAULT 'US',
    is_active BOOLEAN DEFAULT true,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- News sources table - tracks different news providers
CREATE TABLE news_sources (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100) UNIQUE NOT NULL,
    base_url VARCHAR(500),
    api_key_required BOOLEAN DEFAULT false,
    rate_limit_per_hour INTEGER DEFAULT 100,
    reliability_score DECIMAL(2,1) DEFAULT 5.0,
    is_active BOOLEAN DEFAULT true,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- News articles table - stores all scraped news articles
CREATE TABLE news_articles (
    id SERIAL PRIMARY KEY,
    uuid UUID DEFAULT uuid_generate_v4() UNIQUE,
    title VARCHAR(500) NOT NULL,
    content TEXT,
    summary TEXT,
    source_id INTEGER REFERENCES news_sources(id),
    source_url VARCHAR(1000) UNIQUE,
    author VARCHAR(200),
    published_at TIMESTAMP,
    scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    word_count INTEGER,
    language VARCHAR(10) DEFAULT 'en',
    is_processed BOOLEAN DEFAULT false,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Company mentions - tracks which companies are mentioned in articles
CREATE TABLE company_mentions (
    id SERIAL PRIMARY KEY,
    article_id INTEGER REFERENCES news_articles(id) ON DELETE CASCADE,
    company_id INTEGER REFERENCES companies(id) ON DELETE CASCADE,
    mention_count INTEGER DEFAULT 1,
    relevance_score DECIMAL(3,2) DEFAULT 0.50, -- 0 to 1
    context_window TEXT, -- surrounding text where company was mentioned
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(article_id, company_id)
);

-- Sentiment scores - stores NLP analysis results
CREATE TABLE sentiment_scores (
    id SERIAL PRIMARY KEY,
    article_id INTEGER REFERENCES news_articles(id) ON DELETE CASCADE,
    company_id INTEGER REFERENCES companies(id) ON DELETE CASCADE,
    sentiment_score DECIMAL(4,3) NOT NULL, -- -1.000 to 1.000
    confidence DECIMAL(3,2) NOT NULL, -- 0.00 to 1.00
    sentiment_label VARCHAR(20) NOT NULL, -- positive, negative, neutral
    model_name VARCHAR(50) DEFAULT 'vader', -- vader, textblob, etc.
    model_version VARCHAR(20) DEFAULT '1.0',
    compound_score DECIMAL(4,3), -- VADER compound score
    positive_score DECIMAL(3,2), -- positive component
    negative_score DECIMAL(3,2), -- negative component
    neutral_score DECIMAL(3,2), -- neutral component
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(article_id, company_id, model_name)
);

-- Sentiment aggregates - pre-computed daily/hourly sentiment summaries
CREATE TABLE sentiment_aggregates (
    id SERIAL PRIMARY KEY,
    company_id INTEGER REFERENCES companies(id) ON DELETE CASCADE,
    date DATE NOT NULL,
    hour INTEGER, -- NULL for daily aggregates, 0-23 for hourly
    avg_sentiment DECIMAL(4,3) NOT NULL,
    sentiment_stddev DECIMAL(4,3),
    article_count INTEGER NOT NULL,
    positive_count INTEGER DEFAULT 0,
    negative_count INTEGER DEFAULT 0,
    neutral_count INTEGER DEFAULT 0,
    total_mentions INTEGER DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(company_id, date, hour)
);

-- Market events - significant events that might affect sentiment
CREATE TABLE market_events (
    id SERIAL PRIMARY KEY,
    title VARCHAR(300) NOT NULL,
    description TEXT,
    event_type VARCHAR(50) NOT NULL, -- earnings, acquisition, ipo, etc.
    company_id INTEGER REFERENCES companies(id),
    event_date DATE NOT NULL,
    impact_score DECIMAL(2,1), -- 1-10 scale
    source VARCHAR(100),
    is_verified BOOLEAN DEFAULT false,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Processing queue - manages async processing tasks
CREATE TABLE processing_queue (
    id SERIAL PRIMARY KEY,
    task_type VARCHAR(50) NOT NULL, -- scrape_news, analyze_sentiment, etc.
    payload JSONB NOT NULL,
    status VARCHAR(20) DEFAULT 'pending', -- pending, processing, completed, failed
    priority INTEGER DEFAULT 5, -- 1-10, higher is more urgent
    attempts INTEGER DEFAULT 0,
    max_attempts INTEGER DEFAULT 3,
    error_message TEXT,
    scheduled_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    started_at TIMESTAMP,
    completed_at TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create indexes for performance optimization
CREATE INDEX idx_companies_symbol ON companies(symbol);
CREATE INDEX idx_companies_active ON companies(is_active);

CREATE INDEX idx_news_articles_published ON news_articles(published_at DESC);
CREATE INDEX idx_news_articles_source ON news_articles(source_id);
CREATE INDEX idx_news_articles_processed ON news_articles(is_processed);
CREATE INDEX idx_news_articles_scraped ON news_articles(scraped_at DESC);

CREATE INDEX idx_company_mentions_article ON company_mentions(article_id);
CREATE INDEX idx_company_mentions_company ON company_mentions(company_id);
CREATE INDEX idx_company_mentions_relevance ON company_mentions(relevance_score DESC);

CREATE INDEX idx_sentiment_scores_article ON sentiment_scores(article_id);
CREATE INDEX idx_sentiment_scores_company ON sentiment_scores(company_id);
CREATE INDEX idx_sentiment_scores_processed ON sentiment_scores(processed_at DESC);
CREATE INDEX idx_sentiment_scores_compound ON sentiment_scores(sentiment_score DESC);

CREATE INDEX idx_sentiment_aggregates_company_date ON sentiment_aggregates(company_id, date DESC);
CREATE INDEX idx_sentiment_aggregates_date ON sentiment_aggregates(date DESC);

CREATE INDEX idx_market_events_company ON market_events(company_id);
CREATE INDEX idx_market_events_date ON market_events(event_date DESC);

CREATE INDEX idx_processing_queue_status ON processing_queue(status);
CREATE INDEX idx_processing_queue_scheduled ON processing_queue(scheduled_at);
CREATE INDEX idx_processing_queue_priority ON processing_queue(priority DESC);

-- Create composite indexes for common queries
CREATE INDEX idx_sentiment_company_date_score ON sentiment_scores(company_id, processed_at DESC, sentiment_score);
CREATE INDEX idx_news_source_published ON news_articles(source_id, published_at DESC);

-- Insert sample data for development and demo
INSERT INTO news_sources (name, base_url, rate_limit_per_hour, reliability_score) VALUES
    ('NewsAPI', 'https://newsapi.org', 100, 8.5),
    ('Yahoo Finance RSS', 'https://finance.yahoo.com/news', 1000, 7.8),
    ('Reuters Business', 'https://www.reuters.com/business', 500, 9.2),
    ('MarketWatch RSS', 'https://www.marketwatch.com/rss', 1000, 7.5),
    ('Financial Times', 'https://www.ft.com', 200, 9.0),
    ('Bloomberg RSS', 'https://www.bloomberg.com/news', 300, 8.8),
    ('CNBC RSS', 'https://www.cnbc.com/rss', 800, 7.2),
    ('The Wall Street Journal', 'https://www.wsj.com', 150, 9.3);

INSERT INTO companies (symbol, name, sector, market_cap, country) VALUES
    ('AAPL', 'Apple Inc.', 'Technology', 3000000000000, 'US'),
    ('GOOGL', 'Alphabet Inc.', 'Technology', 1800000000000, 'US'),
    ('MSFT', 'Microsoft Corporation', 'Technology', 2800000000000, 'US'),
    ('TSLA', 'Tesla Inc.', 'Automotive', 800000000000, 'US'),
    ('AMZN', 'Amazon.com Inc.', 'E-commerce', 1500000000000, 'US'),
    ('META', 'Meta Platforms Inc.', 'Social Media', 800000000000, 'US'),
    ('NVDA', 'NVIDIA Corporation', 'Semiconductors', 1200000000000, 'US'),
    ('NFLX', 'Netflix Inc.', 'Entertainment', 200000000000, 'US'),
    ('JPM', 'JPMorgan Chase & Co.', 'Banking', 450000000000, 'US'),
    ('JNJ', 'Johnson & Johnson', 'Healthcare', 400000000000, 'US'),
    ('V', 'Visa Inc.', 'Financial Services', 500000000000, 'US'),
    ('PG', 'Procter & Gamble Co.', 'Consumer Goods', 350000000000, 'US'),
    ('UNH', 'UnitedHealth Group Inc.', 'Healthcare', 500000000000, 'US'),
    ('HD', 'The Home Depot Inc.', 'Retail', 350000000000, 'US'),
    ('MA', 'Mastercard Inc.', 'Financial Services', 350000000000, 'US');

-- Create a function to update the updated_at timestamp
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ language 'plpgsql';

-- Create triggers to automatically update updated_at timestamps
CREATE TRIGGER update_companies_updated_at BEFORE UPDATE ON companies
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_news_articles_updated_at BEFORE UPDATE ON news_articles
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_sentiment_aggregates_updated_at BEFORE UPDATE ON sentiment_aggregates
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- Create views for common queries
CREATE VIEW recent_news_with_sentiment AS
SELECT 
    na.id,
    na.title,
    na.published_at,
    ns.name as source_name,
    c.symbol,
    c.name as company_name,
    ss.sentiment_score,
    ss.confidence,
    ss.sentiment_label
FROM news_articles na
JOIN news_sources ns ON na.source_id = ns.id
JOIN company_mentions cm ON na.id = cm.article_id
JOIN companies c ON cm.company_id = c.id
LEFT JOIN sentiment_scores ss ON na.id = ss.article_id AND c.id = ss.company_id
WHERE na.published_at >= CURRENT_DATE - INTERVAL '7 days'
ORDER BY na.published_at DESC;

CREATE VIEW daily_sentiment_summary AS
SELECT 
    c.symbol,
    c.name,
    DATE(ss.processed_at) as date,
    AVG(ss.sentiment_score) as avg_sentiment,
    COUNT(*) as article_count,
    SUM(CASE WHEN ss.sentiment_label = 'positive' THEN 1 ELSE 0 END) as positive_count,
    SUM(CASE WHEN ss.sentiment_label = 'negative' THEN 1 ELSE 0 END) as negative_count,
    SUM(CASE WHEN ss.sentiment_label = 'neutral' THEN 1 ELSE 0 END) as neutral_count
FROM sentiment_scores ss
JOIN companies c ON ss.company_id = c.id
WHERE ss.processed_at >= CURRENT_DATE - INTERVAL '30 days'
GROUP BY c.symbol, c.name, DATE(ss.processed_at)
ORDER BY DATE(ss.processed_at) DESC, avg_sentiment DESC;

-- Grant permissions (adjust as needed for production)
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO admin;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO admin;

-- Print setup completion message
DO $$
BEGIN
    RAISE NOTICE 'Market Intelligence Database Schema Created Successfully!';
    RAISE NOTICE 'Tables created: companies, news_sources, news_articles, company_mentions, sentiment_scores, sentiment_aggregates, market_events, processing_queue';
    RAISE NOTICE 'Sample data inserted: % news sources, % companies', 
        (SELECT COUNT(*) FROM news_sources), 
        (SELECT COUNT(*) FROM companies);
END
$$;