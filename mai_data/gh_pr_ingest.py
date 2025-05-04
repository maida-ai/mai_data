"""GitHub PR data ingestion from BigQuery.

This module handles fetching PR data from BigQuery and saving it to local NDJSON files.


TODO(z-a-f): This file doesn't work -- fix!!!
"""

import json
import logging
from collections.abc import Iterator
from datetime import datetime
from pathlib import Path
from typing import Annotated

import typer
from google.cloud import bigquery
from tqdm import tqdm

logger = logging.getLogger(__name__)


def get_bq_client() -> bigquery.Client:
    """Initialize and return a BigQuery client."""
    return bigquery.Client()


def ensure_data_dir(output_dir: Path) -> Path:
    """Ensure output directory exists and return its path."""
    output_dir.mkdir(parents=True, exist_ok=True)
    return output_dir


def stream_pr_data(
    client: bigquery.Client,
    start_date: str,
    end_date: str | None,
    min_loc: int,
    limit: int | None,
    custom_where: str | None = None,
) -> Iterator[dict]:
    """Stream PR data from BigQuery query results."""
    # Convert dates to YYYYMM format for table wildcards
    start_year = start_date[:4]
    # start_month = start_date[5:7]
    end_year = end_date[:4] if end_date else datetime.now().strftime("%Y")
    # end_month = end_date[5:7] if end_date else datetime.now().strftime("%m")

    # Build table wildcard pattern
    table_pattern = f"`githubarchive.year.{start_year}`"
    if start_year != end_year:
        table_pattern = "`githubarchive.year.*`"

    where_clauses = [
        f"created_at >= '{start_date}'",
        "CAST(JSON_EXTRACT(payload, '$.pull_request.additions') AS INT64)"
        "+ CAST(JSON_EXTRACT(payload, '$.pull_request.deletions') AS INT64) > {min_loc}",
        "type = 'PullRequestEvent'",
        "JSON_EXTRACT(payload, '$.action') = 'closed'",
        "CAST(JSON_EXTRACT(payload, '$.pull_request.merged') AS BOOL) = true",
    ]

    if end_date:
        where_clauses.append(f"created_at < '{end_date}'")

    if custom_where:
        where_clauses.append(f"({custom_where})")

    where_clause = " AND ".join(where_clauses)
    limit_clause = f"LIMIT {limit}" if limit else ""

    query = f"""
    SELECT
      JSON_EXTRACT(payload, '$.pull_request.number') AS pr_id,
      JSON_EXTRACT(payload, '$.repository.full_name') AS repo,
      CAST(
        JSON_EXTRACT(payload, '$.pull_request.additions') AS INT64
      ) + CAST(
        JSON_EXTRACT(payload, '$.pull_request.deletions') AS INT64
      ) AS loc,
      JSON_EXTRACT(payload, '$.pull_request.body') AS body,
      JSON_EXTRACT(payload, '$.pull_request.diff_url') AS diff_url
    FROM {table_pattern}
    WHERE {where_clause}
    {limit_clause}
    """

    logger.info(f"Executing\n{query}\n")
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


def main(
    start_date: Annotated[str, typer.Option(help="Start date (YYYY-MM-DD)")] = "2023-01-01",
    end_date: Annotated[str | None, typer.Option(help="End date (YYYY-MM-DD)")] = None,
    min_loc: Annotated[int, typer.Option(help="Minimum lines of code (additions + deletions)")] = 2000,
    limit: Annotated[int | None, typer.Option(help="Maximum number of PRs to fetch")] = 100000,
    output_dir: Annotated[Path, typer.Option(help="Output directory for NDJSON files")] = Path("data/raw"),
    custom_where: Annotated[str | None, typer.Option(help="Additional WHERE clause conditions")] = None,
):
    """Fetch GitHub PR data from BigQuery and save to NDJSON files.

    Example:
        python -m mai_data.gh_pr_ingest --start-date 2023-01-01 --min-loc 5000 --limit 1000
    """
    # Initialize components
    client = get_bq_client()
    output_dir = ensure_data_dir(output_dir)

    # Process data
    print("Fetching PR data from BigQuery...")
    rows = stream_pr_data(
        client=client,
        start_date=start_date,
        end_date=end_date,
        min_loc=min_loc,
        limit=limit,
        custom_where=custom_where,
    )
    logging.info(f"{list(rows)}")
    total_rows, avg_loc = save_to_ndjson(rows, output_dir)

    # Print summary
    print("\nIngestion Summary:")
    print(f"Total PRs processed: {total_rows:,}")
    print(f"Average LOC per PR: {avg_loc:,.1f}")


if __name__ == "__main__":
    import os

    logging.basicConfig(level=os.environ.get("LOG_LEVEL", "ERROR"))
    typer.run(main)
