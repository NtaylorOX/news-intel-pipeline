import os
from dotenv import load_dotenv

load_dotenv()

class Settings:
    PROJECT_NAME: str = "News Intelligence Pipeline"
    PROJECT_VERSION: str = "0.1.0"
    PYTHON_ENV: str = os.getenv("PYTHON_ENV", "development")
    # Feature toggle for using AI-powered filtering
    AI_FILTER_ENABLED: bool = os.getenv("AI_FILTER_ENABLED", "false").lower() in ("1", "true", "yes")

settings = Settings()
