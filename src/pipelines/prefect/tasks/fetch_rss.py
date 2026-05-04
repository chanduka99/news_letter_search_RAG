from prefect import task
from bs4 import BeautifulSoup
import requests
from sqlalchemy.orm import Session
from sqlalchemy.engine import Engine
from src.models.article_models import ArticleItem, FeedItem
from src.models.sql_models import SubstackArticle
from src.utils.logger_util import setup_logging
from src.infrastructure.supabase.init_session import init_session


@task(task_run_name="fetch_rss_entries-{feed.name}")
def fetch_rss_entries(
    feed: FeedItem,
    engine: Engine,
    article_model: type[SubstackArticle] = SubstackArticle,
) -> list[ArticleItem]:
    """Fetch all RSS items from a Substack feed and convert them to ArticleItem objects.

    Each task uses its own SQLAlchemy session. Articles already stored in the database
    or with empty links/content are skipped. Errors during parsing individual items
    are logged but do not stop processing.

    Args:
        feed (FeedItem): Metadata for the feed (name, author, URL).
        engine (Engine): SQLAlchemy engine for database connection.
        article_model (type[SubstackArticle], optional): Model used to persist articles.
            Defaults to SubstackArticle.

    Returns:
        list[ArticleItem]: List of new ArticleItem objects ready for parsing/ingestion.

    Raises:
        RuntimeError: If the RSS fetch fails.
        Exception: For unexpected errors during execution.
    """
    logger = setup_logging()
    session: Session = init_session(engine)
    items: list[ArticleItem] = []

    try:
        try:
            response = requests.get()
            response.raise_for_status()
        except Exception as e:
            logger.error(f"Failed to fetch feed '{feed.name}' : {e}")
            raise RuntimeError(f"RSS fetch failed for feed '{feed.name}'") from e

        soup = BeautifulSoup(response.content, "xml")
        rss_items = soup.find_all("items")

        for _, item in enumerate(rss_items):
            try:
                link = (
                    item.find("link").get_text(strip=True) if item.find("link") else ""
                )
                if not link or session.query(article_model).filter_by(url=link).first():
                    logger.info(
                        f"Skipping already stored or empty link for feed '{feed.name}'"
                    )
                    continue  # will skip this articleItem and go the next extracted article item

                title = (
                    item.find("title").get_text(strip=True)
                    if item.find("title")
                    else "Untitled"
                )

            except Exception as e:
                pass

    except Exception as e:
        logger.error(
            f"Unexpected error in fetch_rss_entries for feed '{feed.name}': {e}"
        )
        raise
    finally:
        session.close()
        logger.info(f"Database session closed for feed '{feed.name}'")
