import time
import asyncio
from collections.abc import AsyncGenerator
from datetime import datetime
from sqlalchemy.orm import Session
from qdrant_client import AsyncQdrantClient
from utils.logger_util import setup_logging
from src.config import settings
from src.models.sql_models import SubstackArticle


class AsyncQdrantVectorStore:
    """Manages asynchronous interactions with Qdrant vector store for article ingestion.

    Initializes Qdrant client, embedding models, and configurations for dense and sparse
    vector storage. Handles collection creation, indexing, and ingestion from SQL.

    Attributes:
        client (AsyncQdrantClient): Qdrant client for vector store operations.
        collection_name (str): Name of the Qdrant collection.
        dense_model (TextEmbedding): Model for dense vector embeddings.
        sparse_model (SparseTextEmbedding): Model for sparse vector embeddings.
        splitter (TextSplitter): Utility for splitting article content into chunks.
        logger: Logger instance for tracking operations and errors.

    """

    def __init__(self, cache_dir: str | None = None):
        """Initialize AsyncQdrantVectorStore with Qdrant client and embedding models."""

        vector_db = settings.qdrant
        # -----------------------------
        # Qdrant client & collection
        # -----------------------------
        self.client = AsyncQdrantClient(url=vector_db.url, api_key=vector_db.api_key)
        self.collection_name = vector_db.collection_name
        self.article_batch_size = vector_db.article_batch_size
        self.max_concurrent = vector_db.max_concurrent

        # -----------------------------
        # Logging
        # -----------------------------
        self.logger = setup_logging()

    async def ingest_from_sql(self, session: Session, from_date: datetime | None):
        """Ingest articles from SQL database into Qdrant vector store.

        Fetches articles in batches, generates embeddings, and upserts them to Qdrant.
        Skips existing articles and logs throughput.

        Args:
            session (Session): SQLAlchemy session for querying articles.
            from_date (datetime, optional): Filter articles from this date.

        Returns:
            None

        Raises:
            RuntimeError: If ingestion or upsert fails.
            Exception: For unexpected errors.

        """

        self.logger = setup_logging(
            f"Starting ingestion in Qdrant collection '{self.collection_name}'"
            f"from SQL (batch size: {self.article})"
        )

        try:
            # Limit concurrency to avoid ingestion overload into Qdrant
            samaphore = asyncio.Semaphore(max(2, self.max_concurrent))

            total_articles = 0
            total_chunks = 0
            start_time = time.time()

            async for articles in self._article_batch_generator(
                session=session, from_date=from_date
            ):
                all_chunks, all_ids, all_payloads = [], [], []

                for article in articles:
                    pass
        except Exception as e:
            self.logger.error(f"Failed to ingest articles to Qdrant: {e}")
            raise RuntimeError("Error during SQL to Qdrant ingestion")

    # -----------------------------
    # Embedding helpers
    # -----------------------------

    async def _article_batch_generator(
        self, session: Session, from_date: datetime | None = None
    ) -> AsyncGenerator[list[SubstackArticle], None]:
        """Yield batches of articles from SQL database.

        Args:
            session (Session): SQLAlchemy session for querying articles.
            from_date (datetime, optional): Filter articles from this date.

        Yields:
            list[SubstackArticle]: Batch of articles.

        Raises:
            Exception: If database query fails.

        """
        # Query is synchronous. For 5 articles ok
        # But concurrent requests may be needed for larger batches (e.g. 100+ articles).
        # In this case change to async the init_session.py
        try:
            offset = 0
            while True:
                query = session.query(SubstackArticle).order_by(
                    SubstackArticle.published_at
                )
                if from_date:
                    query = query.filter(SubstackArticle.published_at >= from_date)
                articles = query.offset(offset).limit(self.article_batch_size).all()

                if not articles:
                    break
                yield articles
                offset += self.article_batch_size
        except Exception as e:
            self.logger.error(f"Failed to fetch article batch: {e}")
            raise
