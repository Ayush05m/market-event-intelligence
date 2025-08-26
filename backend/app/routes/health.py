from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session
from datetime import datetime
from app.database import get_db, db_manager, get_db_stats

from app.core.logging import get_logger

logger = get_logger()
router = APIRouter(prefix="/health", tags=["Health"])

@router.get("")
async def health_check():
    return {"status": "healthy", "timestamp": datetime.utcnow().isoformat()}

@router.get("/detailed")
async def detailed_health_check(db: Session = Depends(get_db)):
    return {
        "status": "healthy",
        "timestamp": datetime.utcnow().isoformat(),
        "database": "connected" if db_manager.health_check() else "disconnected",
        "stats": get_db_stats()
    }

@router.get("/detailed")
async def detailed_health_check(db: Session = Depends(get_db)):
    """Detailed health check with database connectivity"""
    health_data = {
        "status": "healthy",
        "timestamp": datetime.utcnow().isoformat(),
        "database": "connected" if db_manager.health_check() else "disconnected",
        "version": "1.0.0"
    }
    
    try:
        # Get basic database stats
        stats = get_db_stats()
        health_data["stats"] = stats
        
        # Check connection pool
        pool_info = db_manager.get_connection_info()
        health_data["connection_pool"] = pool_info
        
    except Exception as e:
        health_data["status"] = "degraded"
        health_data["error"] = str(e)
        logger.error("Health check failed", error=str(e))
    
    return health_data
