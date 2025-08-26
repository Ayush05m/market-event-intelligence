import os
from pydantic import BaseSettings

class Settings(BaseSettings):
    APP_NAME: str = "Market Intelligence Platform API"
    APP_VERSION: str = "1.0.0"
    NEWS_API_KEY: str = os.getenv("NEWS_API_KEY", "")
    ALLOWED_ORIGINS: list[str] = [
        "http://localhost:3000",
        "http://127.0.0.1:3000",
        "http://localhost:5173",
        "http://localhost:32100",
        "*"
    ]

settings = Settings()

def get_settings() -> Settings:
    return settings