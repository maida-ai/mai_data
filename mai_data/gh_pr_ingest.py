"""GitHub PR data ingestion from BigQuery.

This module handles fetching PR data from BigQuery and saving it to local NDJSON files.
"""

import json
from collections.abc import Iterator
from datetime import datetime
from pathlib import Path

from google.cloud import bigquery
from tqdm import tqdm


def get_bq_client() -> bigquery.Client:
    """Initialize and return a BigQuery client."""
    return bigquery.Client()


def ensure_data_dir() -> Path:
    """Ensure data/raw directory exists and return its path."""
    data_dir = Path("data/raw")
    data_dir.mkdir(parents=True, exist_ok=True)
    return data_dir


def stream_pr_data(client: bigquery.Client, query: str) -> Iterator[dict]:
    """Stream PR data from BigQuery query results."""
    query_job = client.query(query)
    return query_job.result()


def save_to_ndjson(rows: Iterator[dict], output_dir: Path) -> tuple[int, float]:
    """Save rows to NDJSON file and return row count and average LOC."""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_file = output_dir / f"gh_prs_{timestamp}.ndjson"

    total_rows = 0
    total_loc = 0

    with open(output_file, "w") as f:
        for row in tqdm(rows, desc="Saving PR data"):
            row_dict = dict(row)
            f.write(json.dumps(row_dict) + "\n")
            total_rows += 1
            total_loc += row_dict["loc"]

    avg_loc = total_loc / total_rows if total_rows > 0 else 0
    return total_rows, avg_loc


def main():
    """Main entry point for PR data ingestion."""
    # BigQuery query for PR data
    query = """
    SELECT
      id AS pr_id,
      repo.name AS repo,
      additions + deletions AS loc,
      body,
      diff_url
    FROM `bigquery-public-data.github_repos.pull_requests`
    WHERE loc > 2000
      AND created_at >= '2023-01-01'
      AND state = 'MERGED'
    LIMIT 100000;
    """

    # Initialize components
    client = get_bq_client()
    output_dir = ensure_data_dir()

    # Process data
    print("Fetching PR data from BigQuery...")
    rows = stream_pr_data(client, query)
    total_rows, avg_loc = save_to_ndjson(rows, output_dir)

    # Print summary
    print("\nIngestion Summary:")
    print(f"Total PRs processed: {total_rows:,}")
    print(f"Average LOC per PR: {avg_loc:,.1f}")


if __name__ == "__main__":
    main()
