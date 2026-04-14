# Week 2, Lesson 5 — API Integration: Building Ingestion Pipelines
import requests
import pandas as pd
import logging
from pathlib import Path
from time import sleep

# response = requests.get("https://jsonplaceholder.typicode.com/users")
# response.raise_for_status()
# data = response.json()


# print(response.status_code)
# print(type(response.json()))
# print(len(response.json()))


# user = response.json()[0]
# print(user["name"])
# print(user["email"])

# response = requests.get("https://jsonplaceholder.typicode.com/posts")
# response2 = requests.get("https://jsonplaceholder.typicode.com/posts/1")
# response2.raise_for_status()
# data = response2.json()

# print(data)

# # Print the title of the first 3 posts
# # for post in data[:3]:
# #     print(post["title"])


# # Fetch a single post: GET https://jsonplaceholder.typicode.com/posts/1



# Building a Complete Ingestion Pipeline
# Let's put everything together — fetching from an API, handling pagination, retrying failures, and saving to Parquet:




logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(message)s")
logger = logging.getLogger("ingestion")

BASE_URL = "https://jsonplaceholder.typicode.com"
OUTPUT_DIR = Path("data/bronze")

def fetch_endpoint(endpoint: str) -> list[dict]:
    """Fetch all records from an API endpoint with retry."""
    url = f"{BASE_URL}/{endpoint}"

    for attempt in range(3):
        try:
            response = requests.get(url, timeout=30)
            response.raise_for_status()
            data = response.json()
            logger.info(f"Fetched {len(data)} records from /{endpoint}")
            return data
        except requests.RequestException as e:
            if attempt == 2:
                raise
            wait = 2 ** attempt
            logger.warning(f"Retry {attempt + 1}: {e}. Waiting {wait}s...")
            sleep(wait)

def save_as_parquet(data: list[dict], name: str):
    """Convert API data to DataFrame and save as Parquet."""
    df = pd.DataFrame(data)

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    output_path = OUTPUT_DIR / f"{name}.parquet"

    df.to_parquet(output_path, engine="pyarrow", compression="snappy", index=False)
    logger.info(f"Saved {len(df)} rows to {output_path}")


def load_parquet(name: str) -> pd.DataFrame:
    """Load a Parquet file from the bronze layer."""
    path = OUTPUT_DIR / f"{name}.parquet"
    return pd.read_parquet(path, engine="pyarrow")


def create_silver_layer():
    """Build the Silver enriched_posts table from Bronze Parquet files."""
    SILVER_DIR = Path("data/silver")

    users = load_parquet("users")
    posts = load_parquet("posts")
    comments = load_parquet("comments")

    comment_counts = (
        comments.groupby("postId").size().rename("comment_count").reset_index()
    )

    enriched = posts.merge(
        users,
        left_on="userId",
        right_on="id",
        how="left",
        suffixes=("", "_user"),
    )

    enriched = enriched.merge(
        comment_counts,
        left_on="id",
        right_on="postId",
        how="left",
    )

    enriched["comment_count"] = enriched["comment_count"].fillna(0).astype(int)
    enriched = enriched.drop(columns=["postId"])

    SILVER_DIR.mkdir(parents=True, exist_ok=True)
    silver_path = SILVER_DIR / "enriched_posts.parquet"
    enriched.to_parquet(silver_path, engine="pyarrow", compression="snappy", index=False)
    logger.info(f"Saved {len(enriched)} rows to {silver_path}")


def run_pipeline():
    """Ingest data from multiple API endpoints into Bronze layer and build Silver."""
    endpoints = ["users", "posts", "comments"]

    for endpoint in endpoints:
        data = fetch_endpoint(endpoint)
        save_as_parquet(data, endpoint)

    create_silver_layer()
    logger.info("Pipeline complete!")


if __name__ == "__main__":
    run_pipeline()





# Week 2 Summary
# Over 5 lessons, you've built a complete toolkit for data manipulation:

# File formats — CSV for ingestion, Parquet for analytics, Delta for lakehouse
# pandas essentials — inspect, filter, transform with vectorized operations
# Aggregations & joins — groupby, merge, pivot, lag/lead, running totals
# Memory optimization — category, downcast, chunked reads, pyarrow backend
# API integration — requests, pagination, retry, complete ingestion pipeline
# These skills are the foundation of every data engineering pipeline. Whether you're building on Azure, Databricks, Spark, or Fabric — you'll process data with these same patterns.