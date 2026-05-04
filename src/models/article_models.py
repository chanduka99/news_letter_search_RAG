from pydantic import BaseModel, Field


# -----------------------------
# Feed settings
# -----------------------------
class FeedItem(BaseModel):
    name: str = Field(default="", description="Name of the feed")
    author: str = Field(default="", description="Author of the feed")
    url: str = Field(default="", description="URL of the feed")
