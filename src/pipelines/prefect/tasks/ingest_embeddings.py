from datetime import datetime
from prefect import task


@task(
    task_run_name="ingest qdrant",
    description="Ingest articles from SQL to Qdrant",
    retries=2,
    retry_delay_seconds=120,
)
async def ingest_qdrant(from_date: datetime | None = None):
    """Ingest articles from SQL database into Qdrant vector store.

    Args:
        from_date (datetime | None, optional): Only ingest articles published after this date.
            Defaults to None (ingest all articles).

    Returns:
        None

    Raises:
        RuntimeError: If ingestion fails.
        Exception: For unexpected errors during execution.

    """
