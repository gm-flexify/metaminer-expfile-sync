"""Application configuration settings."""

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict
from typing import Optional


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    app_name: str = "metaminer_expfile_sync"
    app_version: str = "1.0.0"
    debug: bool = False

    database_url: Optional[str] = None
    database_echo: bool = False

    keitaro_api_url: Optional[str] = Field(
        default=None,
        description="Keitaro tracker base URL (e.g. https://your-domain.com)",
    )
    keitaro_api_key: Optional[str] = Field(
        default=None,
        description="Keitaro Admin API key",
    )
    keitaro_timeout: int = Field(default=120, description="HTTP timeout for Keitaro API")

    cors_origins: str = "*"
    root_path: str = ""


settings = Settings()
