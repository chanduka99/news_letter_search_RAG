import hashlib
import time
import asyncio
from collections.abc import AsyncGenerator
from datetime import datetime
from fastembed import TextEmbedding, SparseTextEmbedding
import uuid
from models.vectorstore_models import ArticleChunkPayload
from sqlalchemy.orm import Session
from qdrant_client import AsyncQdrantClient
from qdrant_client.models import SparseVector
from utils.logger_util import setup_logging
from utils.text_splitter import TextSplitter
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
        # Models & config
        # -----------------------------
        self.dense_verctors = TextEmbedding(
            model_name=vector_db.dense_model_name,
            cache_dir=cache_dir,  # only uses cache_dir is provided
        )
        self.sparse_model = SparseTextEmbedding(
            model_name=vector_db.sparse_model_name,
            cache_dir=cache_dir,  # only uses chache_dir is provided
        )
        self.sparse_batch_size = vector_db.sparse_batch_size
        # -----------------------------
        # Qdrant client & collection
        # -----------------------------
        self.client = AsyncQdrantClient(url=vector_db.url, api_key=vector_db.api_key)
        self.collection_name = vector_db.collection_name
        self.article_batch_size = vector_db.article_batch_size
        self.max_concurrent = vector_db.max_concurrent
        self.upsert_batch_size = vector_db.upser_batch_size
        self.text_splitter = TextSplitter()

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
                    chunks = self.text_splitter.split_text(text=article.content)

                    NAMESPACE = uuid.UUID(
                        "6ba7b810-9dad-11d1-80b4-00c04fd430c8"
                    )  # custom namespace
                    ids = [
                        str(uuid.uuid5(NAMESPACE, f"{article.url}_{chunk}"))
                        for chunk in chunks
                    ]

                    payloads = [
                        ArticleChunkPayload(
                            feed_name=article.feed_name,
                            feed_author=article.feed_author,
                            article_authors=article.article_authors,
                            url=article.url,
                            published_at=article.published_at,
                            created_at=article.created_at,
                            chunk_index=i,
                            chunk_text=chunk,
                        )
                        for i, chunk in enumerate(chunks)
                    ]

                    # check existing ids
                    existing_points = await self.client.retrieve(
                        collection_name=self.collection_name, ids=ids
                    )
                    existing_ids = {p.id for p in existing_points}

                    new_chunks = [
                        c
                        for c, id_ in enumerate(zip(chunks, ids, strict=False))
                        if id_ not in existing_ids
                    ]

                    new_ids = [id_ for id_ in ids if id_ not in existing_ids]

                    new_payloads = [
                        p
                        for p, id_ in enumerate(zip(payloads, ids, strict=False))
                        if id_ not in existing_ids
                    ]

                    self.logger.info(
                        f"Article '{article.title}': total chunks = {len(chunks)}, "
                        f"existing chunks = {len(existing_ids)} new chunks = {len(new_chunks)}"
                    )

                    all_chunks.extend(new_chunks)
                    all_ids.extend(new_ids)
                    all_payloads.extend(new_payloads)
                    total_articles += 1

                # -----------------------------
                # Process all chunks in batches.Below for loop starts fresth for each batch we get from the sql db
                # -----------------------------
                for start in range(0, len(all_chunks), self.upsert_batch_size):
                    sub_chunks = all_chunks[start : start + self.upsert_batch_size]
                    sub_ids = list[int | str] = all_ids[
                        start : start + self.upsert_batch_size
                    ]
                    sub_payloads = all_payloads[start : start + self.upsert_batch_size]

                    batch_start_time = time.time()  # start time for the batch
                    # generate sparse and dense vec embeddings for the sub_chunks
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

    async def embed_batch_async(
        self, text: list[str]
    ) -> tuple[list[list[float]], list[SparseVector]]:
        """Generate dense and sparse embeddings concurrently for a batch of texts.

        Args:
            texts (list[str]): List of text strings to embed.

        Returns:
            tuple[list[list[float]], list[SparseVector]]: Dense and sparse embeddings.

        Raises:
            RuntimeError: If embedding generation fails.

        """
        try:
            # Run embeddings concurrently in threads
            dense_task = asyncio.to_thread(self.dense_verctors, text)
            sparse_task = asyncio.to_thread(
                self.sparse_model.embed, batch_size=self.sparse_batch_size
            )

            dense_results, sparse_restults = asyncio.gather(dense_task, sparse_task)

            # Convert to upsert-friendly format
            dense_vecs = []
            sparse_vecs = []

            # Free memory
            del dense_results, sparse_restults
            return dense_vecs, sparse_vecs
        except Exception as e:
            self.logger.error(f"Failed to generate embeddings: {e}")
            raise RuntimeError("Error generating batch embeddings") from e
