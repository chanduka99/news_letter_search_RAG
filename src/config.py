import yaml
import os
from datetime import datetime
from typing import ClassVar
from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import BaseModel, Field, SecretStr, model_validator

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
# YAML loader
# -----------------------------
def load_yaml_feeds(path: str):
    """
    Load RSS feed items from a YAML file.
    If the file does not exist or is empty, returns an empty list.

    Args:
        path (str): Path to the YAML file.

    Returns:
        list[FeedItem]: List of FeedItem instances loaded from the file.
    """
    if not os.path.exists(path):
        return []
    with open(path, encoding="utf-8") as f:
        data = yaml.safe_load(f)
    feed_list = data.get("feeds", [])
    return [FeedItem(**feed) for feed in feed_list]


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

    @model_validator(mode="after")
    def load_yaml_rss_feeds(self) -> "Settings":
        """
        Load RSS feeds from a YAML file after model initialization.
        If the file does not exist or is empty, the feeds list remains unchanged.

        Args:
            self (Settings): The settings instance.

        Returns:
            Settings: The updated settings instance.
        """
        yaml_feeds = load_yaml_feeds(self.rss_config_yaml_path)
        if yaml_feeds:
            self.rss.feeds = yaml_feeds
        return self


# -----------------------------
# Instantiate settings
# -----------------------------
settings = Settings()
