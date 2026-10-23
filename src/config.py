"""
TransactionFlow Configuration Module
Defines runtime configurations, environment settings, risk rule parameters,
and database connection parameters.
"""

from pathlib import Path
from typing import Optional

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

BASE_DIR = Path(__file__).resolve().parent.parent


class Settings(BaseSettings):
    """Application settings with environment variable override support."""

    model_config = SettingsConfigDict(
        env_file=str(BASE_DIR / ".env"),
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # General
    ENVIRONMENT: str = "development"
    LOG_LEVEL: str = "INFO"

    # Database
    DATABASE_URL: str = Field(
        default=f"sqlite:///{BASE_DIR / 'data' / 'transactionflow.db'}",
        description="SQLAlchemy database URL. Defaults to SQLite for zero-dependency local runs, or PostgreSQL.",
    )
    DB_POOL_SIZE: int = 10
    DB_MAX_OVERFLOW: int = 20
    DB_TIMEOUT: int = 30

    # ETL Batch Processing
    DEFAULT_BATCH_SIZE: int = 10000
    MAX_WORKERS: int = 4

    # Risk Rules
    RISK_HIGH_AMOUNT_THRESHOLD: float = 10000.00
    RISK_VELOCITY_WINDOW_MINUTES: int = 5
    RISK_VELOCITY_MAX_COUNT: int = 3
    RISK_RAPID_IDENTICAL_WINDOW_SECONDS: int = 60
    RISK_ATM_CATEGORY_THRESHOLD: float = 2000.00

    # Mock API & External Ingestion
    API_BASE_URL: str = "http://localhost:8000"
    API_TIMEOUT_SECONDS: float = 10.0
    API_MAX_RETRIES: int = 4
    API_BACKOFF_FACTOR: float = 1.5
    API_PAGE_SIZE: int = 500
    API_RATE_LIMIT_RPS: int = 20

    # Azure Blob Storage / ADLS Gen2
    AZURE_STORAGE_CONNECTION_STRING: Optional[str] = None
    AZURE_STORAGE_CONTAINER_NAME: str = "transactions-raw"
    AZURE_ADLS_ACCOUNT_NAME: Optional[str] = None
    AZURE_USE_MANAGED_IDENTITY: bool = False


# Global settings singleton
settings = Settings()
