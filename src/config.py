from datetime import datetime
from typing import ClassVar
from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import BaseModel, Field, SecretStr

from src.models.article_models import FeedItem


# -----------------------------
# Supabase db settings
# -----------------------------
class SupabaseDBSettings(BaseModel):
    table_name: str = Field(
        default="substack_articles", description="Supabase table name"
    )
    host: str = Field(default="localhost", description="Database host")
    name: str = Field(default="postgres", description="Database name")
    user: str = Field(default="postgres", description="Database user")
    password: SecretStr = Field(
        default=SecretStr("password"), description="Database password"
    )
    port: int = Field(default=6543, description="Database port")
    test_database: str = Field(
        default="substack_test", description="Test database name"
    )


# -----------------------------
# RSSSettings
# -----------------------------
class RSSSettings(BaseModel):
    feeds: list[FeedItem] = Field(
        default_factory=list[FeedItem], description="List of RSS feed items"
    )
    default_start_date: datetime = Field(
        default="2026-05-04", description="Default cutoff date"
    )
    batch_size: int = Field(
        default=5, description="Number of articles to parse and ingest in a branch"
    )


# -----------------------------
# Main Settings
# -----------------------------
class Settings(BaseSettings):
    supabase_db: SupabaseDBSettings = Field(default_factory=SupabaseDBSettings)
    rss: RSSSettings = Field(default_factory=RSSSettings)

    rss_config_yaml_path: str = "src/configs/feeds_rss.yaml"
    # Pydantic v2 model config
    model_config: ClassVar[SettingsConfigDict] = SettingsConfigDict(
        env_file=[".env"],
        env_file_encoding="utf-8",  # handle proper special characters
        extra="ignore",
        env_nested_delimiter="__",
        case_sensitive=False,
        frozen=True,  # immutable. not changeble after creation
    )


# -----------------------------
# Instantiate settings
# -----------------------------
settings = Settings()
