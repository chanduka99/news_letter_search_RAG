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
