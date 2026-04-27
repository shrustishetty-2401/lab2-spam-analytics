"""
Homework #10 - Pinecone Article Recommendation DAG
DATA-226 | SJSU Spring 2026
"""

from __future__ import annotations

import logging
import requests
import pandas as pd

from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.models import Variable

logger = logging.getLogger(__name__)

INDEX_NAME  = "semantic-search-fast"
MEDIUM_URL  = "https://s3-geospatial.s3.us-west-2.amazonaws.com/medium_data.csv"
DATA_PATH   = "/opt/airflow/dags/medium_data.csv"
DIMENSION   = 384
METRIC      = "dotproduct"


@dag(
    dag_id      = "pinecone_medium_articles",
    description = "Download Medium articles, embed them, ingest into Pinecone, and run search",
    schedule    = None,
    start_date  = datetime(2026, 4, 1),
    catchup     = False,
    default_args = {
        "owner":       "airflow",
        "retries":     1,
        "retry_delay": timedelta(minutes=2),
    },
    tags = ["pinecone", "embeddings", "nlp"],
)
def pinecone_medium_pipeline():

    @task()
    def download_data() -> str:
        logger.info("Downloading Medium article data from S3...")
        response = requests.get(MEDIUM_URL)
        response.raise_for_status()
        with open(DATA_PATH, "wb") as f:
            f.write(response.content)
        logger.info(f"Data saved to {DATA_PATH}")
        return DATA_PATH

    @task()
    def preprocess_data(file_path: str) -> str:
        logger.info("Preprocessing Medium article data...")
        df = pd.read_csv(file_path)
        df = df.dropna(subset=["title"])
        df = df.fillna("")
        df = df.reset_index(drop=True)
        df["id"] = df.index.astype(str)
        df["metadata"] = df.apply(
            lambda row: {"title": row["title"] + " " + row["subtitle"]}, axis=1
        )
        processed_path = "/opt/airflow/dags/medium_processed.csv"
        df.to_csv(processed_path, index=False)
        logger.info(f"Preprocessed data saved. Total records: {len(df)}")
        return processed_path

    @task()
    def create_index() -> str:
        from pinecone import Pinecone, ServerlessSpec
        api_key = Variable.get("PINECONE_API_KEY")
        pc = Pinecone(api_key=api_key)
        existing_indexes = [idx.name for idx in pc.list_indexes()]
        if INDEX_NAME not in existing_indexes:
            logger.info(f"Creating Pinecone index: {INDEX_NAME}")
            pc.create_index(
                INDEX_NAME,
                dimension=DIMENSION,
                metric=METRIC,
                spec=ServerlessSpec(cloud="aws", region="us-east-1")
            )
            logger.info(f"Index '{INDEX_NAME}' created successfully.")
        else:
            logger.info(f"Index '{INDEX_NAME}' already exists. Skipping creation.")
        return INDEX_NAME

    @task()
    def generate_and_ingest(processed_path: str, index_name: str) -> dict:
        import ast
        from sentence_transformers import SentenceTransformer
        from pinecone import Pinecone

        logger.info("Loading embedding model: all-MiniLM-L6-v2")
        model = SentenceTransformer("all-MiniLM-L6-v2")

        logger.info("Loading preprocessed data...")
        df = pd.read_csv(processed_path)
        df["metadata"] = df["metadata"].apply(
            lambda x: ast.literal_eval(x) if isinstance(x, str) else x
        )

        logger.info("Generating sentence embeddings...")
        titles = df["metadata"].apply(lambda x: x["title"]).tolist()
        embeddings = model.encode(titles, show_progress_bar=True)

        logger.info("Connecting to Pinecone...")
        api_key = Variable.get("PINECONE_API_KEY")
        pc = Pinecone(api_key=api_key)
        index = pc.Index(index_name)

        # Upsert in batches of 100
        batch_size = 100
        total = len(df)
        logger.info(f"Upserting {total} records in batches of {batch_size}...")

        for i in range(0, total, batch_size):
            batch_ids = df["id"].iloc[i:i+batch_size].tolist()
            batch_embeddings = embeddings[i:i+batch_size].tolist()
            batch_metadata = df["metadata"].iloc[i:i+batch_size].tolist()

            vectors = [
                {"id": str(id_), "values": emb, "metadata": meta}
                for id_, emb, meta in zip(batch_ids, batch_embeddings, batch_metadata)
            ]

            index.upsert(vectors=vectors)
            logger.info(f"Upserted batch {i//batch_size + 1}: records {i} to {min(i+batch_size, total)}")

        stats = index.describe_index_stats()
        logger.info(f"Upsert complete! Index stats: {stats}")
        return {"records_ingested": total}

    @task()
    def search_pinecone(index_name: str) -> dict:
        from sentence_transformers import SentenceTransformer
        from pinecone import Pinecone

        logger.info("Loading embedding model for search...")
        model = SentenceTransformer("all-MiniLM-L6-v2")

        api_key = Variable.get("PINECONE_API_KEY")
        pc = Pinecone(api_key=api_key)
        index = pc.Index(index_name)

        query = "what is ethics in AI"
        logger.info(f"Running search for: '{query}'")
        query_vector = model.encode(query).tolist()

        results = index.query(
            vector=query_vector,
            top_k=10,
            include_metadata=True,
            include_values=True
        )

        logger.info(f"Top 10 results for '{query}':")
        for match in results["matches"]:
            logger.info(f"  Score: {match['score']:.4f} | Title: {match['metadata']['title']}")

        return {"query": query, "top_results": len(results["matches"])}

    file_path      = download_data()
    processed_path = preprocess_data(file_path)
    index_name     = create_index()
    generate_and_ingest(processed_path, index_name)
    search_pinecone(index_name)


pinecone_medium_pipeline()
