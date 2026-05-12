from datetime import UTC, datetime, timedelta
from prefect import flow
from dateutil import parser
from utils.logger_util import setup_logging
from src.config import settings
from src.pipelines.prefect.tasks.ingest_embeddings import ingest_qdrant


async def get_last_successfull_run(flow_name: str) -> datetime | None:
    """Get the start time of the last successfully completed run for a given flow.

    Queries the Prefect API for recent completed runs of the exact flow `flow_name`.
    Returns the start time of the most recent completed run, or None if no runs exist.

    Args:
        flow_name (str): Exact name of the Prefect flow.

    Returns:
        datetime | None: Start time of the last completed run, or None if no run exists.

    Raises:
        Exception: If Prefect API calls fail unexpectedly.

    """
    logger = setup_logging()
    logger.info(f"Looking for last successful run of flow: {flow_name}")

    try:
        pass
    except Exception as e:
        logger.error(f"Error fetching last successful run flow '{flow_name}': {e}")
        raise


@flow(
    name="qdrant_ingestion_flow",
    flow_run_name="qdrant_ingestion_flow_run",
    description="Ochestrates SQL -> Qdrant ingestion",
    retries=2,
    retry_delay_seconds=120,
)
async def qdrant_ingestion_flow(from_date: str | None = None) -> None:
    """Prefect Flow: Orchestrates ingestion of articles from SQL into Qdrant.

    Determines the starting cutoff date for ingestion (user-provided, last run date,
    or default fallback) and runs the Qdrant ingestion task.

    Args:
        from_date (str | None, optional): Start date in YYYY-MM-DD format. If None,
            falls back to last successful run or the configured default.

    Returns:
        None

    Raises:
        RuntimeError: If ingestion fails.
        Exception: For unexpected errors during execution.

    """

    try:
        logger = setup_logging()
        rss = settings.rss

        if from_date:
            # Parse user provided date and assume UTC midnight
            from_date_dt = parser.parse(from_date).replace(tzinfo=UTC)
            logger.info(f"Using user-provided from date: {from_date_dt}")
        else:
            # Fallback to last_run_date, default_start_date , or 30 days ago
            last_run_date = await get_last_successfull_run("qdrant_ingest_flow")

            from_date_dt = (
                last_run_date
                or parser.parse(rss.default_start_date).replace(tzinfo=UTC)
                or (datetime.now(UTC) - timedelta(days=30))
            )

            logger.info(f"Using fallback from_date: {from_date_dt}")

        await ingest_qdrant(from_date=from_date_dt)

    except Exception as e:
        logger.erro(f"Error during Qdrant ingestion flow: {e}")
        raise RuntimeError("Qdrant ingestion flow failed") from e
