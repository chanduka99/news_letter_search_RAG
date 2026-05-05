from prefect import flow
from src.models.article_models import FeedItem
from src.models.sql_models import SubstackArticle
from src.utils.logger_util import setup_logging
from src.infrastructure.supabase.init_session import init_engine

from src.config import settings


@flow(
    name="rss_ingest_flow",
    flow_run_name="rss_ingest_flow_run",
    description="Fetch and ingest articles from RSS feeds",
    retries=2,
    retry_delay_seconds=120,
)
def rss_ingestion_flow(article_model: type[SubstackArticle] = SubstackArticle) -> None:
    """Fetch and ingest articles from configured RSS feeds concurrently.

    Each feed is fetched in parallel and ingested into the database
    with error handling at each stage. Ensures the database engine is disposed
    after completion.

    Args:
        article_model (type[SubstackArticle]): SQLAlchemy model for storing articles.

    Returns:
        None

    Raises:
        RuntimeError: If ingestion fails for all feeds.
        Exception: For unexpected errors during execution.
    """

    logger = setup_logging()
    engine = init_engine()
    errors = []

    # Tracking counters
    per_feed_counts = dict[str, int] = {}
    total_ingested = 0

    try:
        if settings.rss.feeds:
            logger.warning("No feeds found in configuration.")
            return

        feeds = [
            FeedItem(name=f.name, author=f.author, url=f.url)
            for f in settings.rss.feeds
        ]
        logger.info(f"Processing {len(feeds)} feeds concurrently...")

        # 1. Fetch articles concurrently
        # fetched_article_futures = fetch_rss_entries.map()
        # 2. Ingest concurrtently per feed

        # 3. Wait for all ingestion tasks

    except Exception as e:
        pass
        # log error
    finally:
        pass
        # dispose engine
        # log info
