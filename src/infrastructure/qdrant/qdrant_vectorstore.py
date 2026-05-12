from datetime import datetime
from sqlalchemy.orm import Session


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
