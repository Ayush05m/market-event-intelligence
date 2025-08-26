import re
import logging
import asyncio
from datetime import datetime
from typing import List, Dict, Tuple, Optional
from dataclasses import dataclass
from decimal import Decimal

import nltk
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize, sent_tokenize
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
from textblob import TextBlob
import pandas as pd
import structlog

# Download required NLTK data
try:
    nltk.data.find('tokenizers/punkt')
    nltk.data.find('corpora/stopwords')
except LookupError:
    nltk.download('punkt')
    nltk.download('stopwords')

# Configure structlog to match main.py
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

logger = structlog.get_logger(__name__)

@dataclass
class SentimentResult:
    """Structured sentiment analysis result"""
    sentiment_score: float  # -1 to 1
    confidence: float      # 0 to 1
    sentiment_label: str   # positive, negative, neutral
    compound_score: float  # VADER compound score
    positive_score: float  # Positive component
    negative_score: float  # Negative component
    neutral_score: float   # Neutral component
    model_name: str = "hybrid"
    model_version: str = "1.0"
    
    def to_dict(self) -> dict:
        return {
            'sentiment_score': float(self.sentiment_score),
            'confidence': float(self.confidence),
            'sentiment_label': self.sentiment_label,
            'compound_score': float(self.compound_score),
            'positive_score': float(self.positive_score),
            'negative_score': float(self.negative_score),
            'neutral_score': float(self.neutral_score),
            'model_name': self.model_name,
            'model_version': self.model_version
        }

class FinancialLexicon:
    """
    Financial-specific sentiment lexicon for enhanced accuracy
    """
    
    def __init__(self):
        # Positive financial terms
        self.positive_terms = {
            'bullish': 0.8, 'rally': 0.7, 'surge': 0.8, 'soar': 0.9, 'gains': 0.6,
            'profit': 0.7, 'revenue': 0.5, 'earnings': 0.5, 'growth': 0.7, 'beat': 0.8,
            'outperform': 0.8, 'upgrade': 0.7, 'buy': 0.6, 'strong': 0.6, 'positive': 0.6,
            'upside': 0.7, 'momentum': 0.6, 'breakthrough': 0.8, 'milestone': 0.7,
            'expansion': 0.6, 'acquisition': 0.5, 'merger': 0.4, 'innovation': 0.7,
            'dividend': 0.5, 'buyback': 0.6, 'record': 0.7, 'exceeded': 0.8
        }
        
        # Negative financial terms
        self.negative_terms = {
            'bearish': -0.8, 'crash': -0.9, 'plunge': -0.8, 'tumble': -0.7, 'losses': -0.6,
            'decline': -0.6, 'fall': -0.5, 'drop': -0.5, 'weakness': -0.6, 'miss': -0.8,
            'underperform': -0.8, 'downgrade': -0.7, 'sell': -0.6, 'weak': -0.6, 'negative': -0.6,
            'downside': -0.7, 'volatility': -0.4, 'concern': -0.5, 'risk': -0.4, 'uncertainty': -0.5,
            'lawsuit': -0.7, 'investigation': -0.6, 'fraud': -0.9, 'scandal': -0.8,
            'bankruptcy': -0.9, 'debt': -0.4, 'layoffs': -0.7, 'restructuring': -0.5
        }
        
        # Market context terms (neutral but important)
        self.context_terms = {
            'market', 'stock', 'share', 'trading', 'investment', 'analyst', 'forecast',
            'quarter', 'guidance', 'outlook', 'strategy', 'performance', 'financial',
            'valuation', 'price', 'target', 'estimate', 'consensus'
        }
        
        # Compile regex patterns for efficiency
        self.positive_pattern = self._compile_pattern(self.positive_terms.keys())
        self.negative_pattern = self._compile_pattern(self.negative_terms.keys())
        self.context_pattern = self._compile_pattern(self.context_terms)
    
    def _compile_pattern(self, terms: List[str]) -> re.Pattern:
        """Compile regex pattern for term matching"""
        pattern = r'\b(?:' + '|'.join(re.escape(term) for term in terms) + r')\b'
        return re.compile(pattern, re.IGNORECASE)
    
    def calculate_financial_sentiment(self, text: str) -> Tuple[float, float]:
        """
        Calculate sentiment based on financial lexicon
        Returns (sentiment_score, confidence)
        """
        text_lower = text.lower()
        
        # Count positive and negative terms
        positive_matches = self.positive_pattern.findall(text_lower)
        negative_matches = self.negative_pattern.findall(text_lower)
        context_matches = self.context_pattern.findall(text_lower)
        
        # Calculate weighted scores
        positive_score = sum(self.positive_terms.get(term.lower(), 0.5) for term in positive_matches)
        negative_score = sum(abs(self.negative_terms.get(term.lower(), -0.5)) for term in negative_matches)
        
        # Normalize by text length (word count)
        word_count = len(text.split())
        if word_count > 0:
            positive_score /= word_count
            negative_score /= word_count
        
        # Calculate net sentiment
        net_sentiment = positive_score - negative_score
        
        # Calculate confidence based on financial term density
        financial_term_count = len(positive_matches) + len(negative_matches) + len(context_matches)
        confidence = min(financial_term_count / max(word_count / 10, 1), 1.0)  # Max confidence at 10% density
        
        return net_sentiment, confidence

class TextPreprocessor:
    """
    Advanced text preprocessing for financial news
    """
    
    def __init__(self):
        self.stop_words = set(stopwords.words('english'))
        # Remove some sentiment-bearing words from stop words
        sentiment_words = {'not', 'no', 'very', 'too', 'more', 'most', 'much'}
        self.stop_words -= sentiment_words
        
        # Financial abbreviations and their expansions
        self.financial_abbreviations = {
            r'\bQ[1-4]\b': 'quarter',
            r'\bFY\b': 'fiscal year',
            r'\bYoY\b': 'year over year',
            r'\bEPS\b': 'earnings per share',
            r'\bP/E\b': 'price to earnings',
            r'\bROI\b': 'return on investment',
            r'\bIPO\b': 'initial public offering',
            r'\bCEO\b': 'chief executive officer',
            r'\bCFO\b': 'chief financial officer'
        }
    
    def clean_text(self, text: str) -> str:
        """Clean and normalize text for sentiment analysis"""
        if not text:
            return ""
        
        # Expand financial abbreviations
        for abbr_pattern, expansion in self.financial_abbreviations.items():
            text = re.sub(abbr_pattern, expansion, text, flags=re.IGNORECASE)
        
        # Remove URLs
        text = re.sub(r'http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\\(\\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+', '', text)
        
        # Remove email addresses
        text = re.sub(r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b', '', text)
        
        # Remove extra whitespace
        text = re.sub(r'\s+', ' ', text)
        
        # Remove special characters but keep punctuation that affects sentiment
        text = re.sub(r'[^\w\s.!?,-]', '', text)
        
        return text.strip()
    
    def extract_key_sentences(self, text: str, max_sentences: int = 5) -> List[str]:
        """Extract key sentences that are most likely to contain sentiment"""
        sentences = sent_tokenize(text)
        
        # Score sentences based on financial term presence
        financial_lexicon = FinancialLexicon()
        scored_sentences = []
        
        for sentence in sentences:
            # Skip very short sentences
            if len(sentence.split()) < 5:
                continue
                
            # Score based on financial term presence
            pos_matches = len(financial_lexicon.positive_pattern.findall(sentence))
            neg_matches = len(financial_lexicon.negative_pattern.findall(sentence))
            ctx_matches = len(financial_lexicon.context_pattern.findall(sentence))
            
            score = pos_matches + neg_matches + (ctx_matches * 0.5)
            scored_sentences.append((sentence, score))
        
        # Sort by score and return top sentences
        scored_sentences.sort(key=lambda x: x[1], reverse=True)
        return [sent for sent, _ in scored_sentences[:max_sentences]]

class HybridSentimentAnalyzer:
    """
    Hybrid sentiment analyzer combining multiple approaches
    """
    
    def __init__(self):
        self.vader_analyzer = SentimentIntensityAnalyzer()
        self.financial_lexicon = FinancialLexicon()
        self.preprocessor = TextPreprocessor()
        
        # Model weights for ensemble
        self.weights = {
            'vader': 0.4,
            'textblob': 0.3,
            'financial_lexicon': 0.3
        }
    
    def analyze_sentiment(self, text: str, company_context: str = None) -> SentimentResult:
        """
        Perform comprehensive sentiment analysis
        """
        if not text:
            return self._neutral_result()
        
        # Clean and preprocess text
        clean_text = self.preprocessor.clean_text(text)
        if not clean_text:
            return self._neutral_result()
        
        # Extract key sentences for focused analysis
        key_sentences = self.preprocessor.extract_key_sentences(clean_text)
        analysis_text = ' '.join(key_sentences) if key_sentences else clean_text
        
        # VADER analysis
        vader_scores = self.vader_analyzer.polarity_scores(analysis_text)
        
        # TextBlob analysis
        blob = TextBlob(analysis_text)
        textblob_polarity = blob.sentiment.polarity
        textblob_subjectivity = blob.sentiment.subjectivity
        
        # Financial lexicon analysis
        financial_sentiment, financial_confidence = self.financial_lexicon.calculate_financial_sentiment(analysis_text)
        
        # Combine scores using weighted ensemble
        combined_sentiment = (
            self.weights['vader'] * vader_scores['compound'] +
            self.weights['textblob'] * textblob_polarity +
            self.weights['financial_lexicon'] * financial_sentiment
        )
        
        # Calculate confidence score
        base_confidence = abs(combined_sentiment)  # Higher absolute sentiment = higher confidence
        
        # Adjust confidence based on text characteristics
        text_length_factor = min(len(analysis_text.split()) / 50, 1.0)  # Longer text = higher confidence
        subjectivity_factor = textblob_subjectivity  # More subjective = higher confidence for sentiment
        financial_factor = financial_confidence  # More financial terms = higher confidence
        
        final_confidence = min(
            (base_confidence * 0.4 + text_length_factor * 0.2 + 
             subjectivity_factor * 0.2 + financial_factor * 0.2),
            1.0
        )
        
        # Determine sentiment label
        if combined_sentiment >= 0.1:
            sentiment_label = "positive"
        elif combined_sentiment <= -0.1:
            sentiment_label = "negative"
        else:
            sentiment_label = "neutral"
        
        # Normalize scores to 0-1 range for individual components
        positive_score = max(0, combined_sentiment)
        negative_score = max(0, -combined_sentiment)
        neutral_score = 1 - abs(combined_sentiment)
        
        return SentimentResult(
            sentiment_score=combined_sentiment,
            confidence=final_confidence,
            sentiment_label=sentiment_label,
            compound_score=vader_scores['compound'],
            positive_score=positive_score,
            negative_score=negative_score,
            neutral_score=neutral_score
        )
    
    def _neutral_result(self) -> SentimentResult:
        """Return neutral sentiment result for empty/invalid text"""
        return SentimentResult(
            sentiment_score=0.0,
            confidence=0.0,
            sentiment_label="neutral",
            compound_score=0.0,
            positive_score=0.0,
            negative_score=0.0,
            neutral_score=1.0
        )
    
    async def batch_analyze(self, texts: List[str], company_contexts: Optional[List[str]] = None) -> List[SentimentResult]:
        """
        Analyze sentiment for multiple texts efficiently
        """
        if company_contexts is None:
            company_contexts = [None] * len(texts)
        
        results = []
        for text, context in zip(texts, company_contexts):
            result = self.analyze_sentiment(text, context)
            results.append(result)
        
        logger.info("Batch sentiment analysis completed", num_texts=len(texts))
        return results

class SentimentPipeline:
    """
    Orchestrates sentiment analysis for articles, producing results for database storage
    """
    
    def __init__(self):
        self.analyzer = HybridSentimentAnalyzer()
    
    async def process_articles(self, articles: List[Dict]) -> Dict:
        """
        Process a list of articles and return sentiment results
        Expected article format: {'id': int, 'title': str, 'content': str, 'url': str, 'company_mentions': List[str]}
        Returns: {'individual_results': List[Dict], 'company_aggregates': Dict}
        """
        logger.info("Starting sentiment analysis pipeline", num_articles=len(articles))
        
        individual_results = []
        company_aggregates = {}
        
        # Process each article
        for article in articles:
            article_id = article['id']
            text = f"{article['title']} {article['content'] or ''}".strip()
            company_mentions = article.get('company_mentions', [])
            
            if not text or not company_mentions:
                logger.warning("Skipping article due to missing content or mentions", article_id=article_id)
                continue
            
            # Analyze sentiment for each company mentioned
            for company_symbol in company_mentions:
                try:
                    # Perform sentiment analysis with company context
                    result = await asyncio.get_event_loop().run_in_executor(
                        None,
                        lambda: self.analyzer.analyze_sentiment(text, company_symbol)
                    )
                    
                    # Store individual result
                    result_dict = result.to_dict()
                    result_dict.update({
                        'article_id': article_id,
                        'company_symbol': company_symbol
                    })
                    individual_results.append(result_dict)
                    
                    # Update company aggregates
                    if company_symbol not in company_aggregates:
                        company_aggregates[company_symbol] = {
                            'total_score': 0.0,
                            'count': 0,
                            'positive_count': 0,
                            'negative_count': 0,
                            'neutral_count': 0
                        }
                    
                    company_aggregates[company_symbol]['total_score'] += result.sentiment_score
                    company_aggregates[company_symbol]['count'] += 1
                    if result.sentiment_label == 'positive':
                        company_aggregates[company_symbol]['positive_count'] += 1
                    elif result.sentiment_label == 'negative':
                        company_aggregates[company_symbol]['negative_count'] += 1
                    else:
                        company_aggregates[company_symbol]['neutral_count'] += 1
                    
                except Exception as e:
                    logger.error("Error processing article for company",
                                article_id=article_id,
                                company_symbol=company_symbol,
                                error=str(e))
                    continue
        
        # Calculate company averages
        for company_symbol, data in company_aggregates.items():
            if data['count'] > 0:
                data['avg_sentiment'] = data['total_score'] / data['count']
            else:
                data['avg_sentiment'] = 0.0
            del data['total_score']  # Remove temporary field
        
        logger.info("Sentiment pipeline completed",
                   num_results=len(individual_results),
                   num_companies=len(company_aggregates))
        
        return {
            'individual_results': individual_results,
            'company_aggregates': company_aggregates
        }