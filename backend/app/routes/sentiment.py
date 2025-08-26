from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from datetime import datetime, timedelta
from sqlalchemy import desc, and_
from typing import List
from app.database import get_db
from app.models import Company, SentimentScore, NewsArticle, SentimentAggregate
import app.models as schemas

router = APIRouter(prefix="/api/companies", tags=["Companies"])

@router.get("", response_model=List[schemas.Company])
async def get_companies(skip: int = 0, limit: int = 100, active_only: bool = True, db: Session = Depends(get_db)):
    query = db.query(Company)
    if active_only:
        query = query.filter(Company.is_active == True)
    return query.offset(skip).limit(limit).all()

@router.get("/{symbol}", response_model=schemas.Company)
async def get_company(symbol: str, db: Session = Depends(get_db)):
    company = db.query(Company).filter(Company.symbol == symbol.upper()).first()
    if not company:
        raise HTTPException(status_code=404, detail="Company not found")
    return company

# (you’d also move /{symbol}/sentiment here)
