import time
from fastapi import Request
from fastapi.responses import Response
from fastapi.middleware.trustedhost import TrustedHostMiddleware
from app.core.logging import logger

async def process_time_middleware(request: Request, call_next):
    start_time = time.time()
    response: Response = await call_next(request)
    duration = time.time() - start_time
    response.headers["X-Process-Time"] = str(duration)

    if duration > 0.2:
        logger.warning("Slow request", path=request.url.path, method=request.method, duration=duration)
    return response
