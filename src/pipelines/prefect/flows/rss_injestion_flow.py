from prefect import flow
from src.models.sql_models import SubstackArticle


@flow(
    name="rss_ingest_flow",
    flow_run_name="rss_ingest_flow_run",
    description="Fetch and ingest articles from RSS feeds",
    retries=2,
    retry_delay_seconds=120,
)
def rss_ingestion_flow(article_model: type[SubstackArticle] = SubstackArticle) -> None:
    # logger = setup_loggin()
    # engine = init_engine()
    errors = []

    # Tracking counters
    per_feed_counts = dict(str,int) = {}
    total_ingested = 0
    
    try:
        pass
        # 1. Fetch articles concurrently

        # 2. Ingest concurrtently per feed

        # 3. Wait for all ingestion tasks

    except Exception as e:
        pass
        # log error
    finally:
        pass
        # dispose engine
        # log info
