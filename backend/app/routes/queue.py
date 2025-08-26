
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import desc, and_
from sqlalchemy.orm import Session
from datetime import datetime, timedelta
from app.database import get_db
from app.models import NewsArticle, SentimentScore, ProcessingQueue

from sqlalchemy.orm import Session
from datetime import datetime, timedelta
from app.database import get_db
from app.models import NewsArticle, SentimentScore, ProcessingQueue

router = APIRouter(prefix="/api/queue", tags=["Queue"])


@router.get("/queue/status")
async def get_queue_status(db: Session = Depends(get_db)):
    """Get current processing queue status"""
    
    # Count tasks by status
    pending_count = db.query(ProcessingQueue).filter(ProcessingQueue.status == "pending").count()
    processing_count = db.query(ProcessingQueue).filter(ProcessingQueue.status == "processing").count()
    completed_count = db.query(ProcessingQueue).filter(
        and_(
            ProcessingQueue.status == "completed",
            ProcessingQueue.completed_at >= datetime.utcnow() - timedelta(hours=24)
        )
    ).count()
    failed_count = db.query(ProcessingQueue).filter(ProcessingQueue.status == "failed").count()
    
    # Get recent tasks
    recent_tasks = db.query(ProcessingQueue).order_by(
        desc(ProcessingQueue.created_at)
    ).limit(10).all()
    
    return {
        "queue_stats": {
            "pending": pending_count,
            "processing": processing_count,
            "completed_24h": completed_count,
            "failed": failed_count
        },
        "recent_tasks": [
            {
                "id": task.id,
                "task_type": task.task_type,
                "status": task.status,
                "created_at": task.created_at.isoformat(),
                "completed_at": task.completed_at.isoformat() if task.completed_at else None,
                "error_message": task.error_message
            } for task in recent_tasks
        ]
    }

@router.get("/queue/task/{task_id}")
async def get_task_status(task_id: int, db: Session = Depends(get_db)):
    """Get specific task status"""
    task = db.query(ProcessingQueue).filter(ProcessingQueue.id == task_id).first()
    if not task:
        raise HTTPException(status_code=404, detail="Task not found")
    
    return {
        "id": task.id,
        "task_type": task.task_type,
        "status": task.status,
        "payload": task.payload,
        "attempts": task.attempts,
        "max_attempts": task.max_attempts,
        "priority": task.priority,
        "created_at": task.created_at.isoformat(),
        "scheduled_at": task.scheduled_at.isoformat(),
        "started_at": task.started_at.isoformat() if task.started_at else None,
        "completed_at": task.completed_at.isoformat() if task.completed_at else None,
        "error_message": task.error_message
    }
