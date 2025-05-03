"""CLI tool to split PRs into atomic diffs."""

import json
from collections.abc import Iterator
from pathlib import Path

import click
from tqdm import tqdm

from .pr_split import split_pr


def read_ndjson(file_path: Path) -> Iterator[dict]:
    """Read NDJSON file line by line."""
    with open(file_path) as f:
        for line in f:
            if line.strip():
                yield json.loads(line)


def write_ndjson(file_path: Path, records: Iterator[dict]) -> None:
    """Write records to NDJSON file."""
    with open(file_path, "w") as f:
        for record in records:
            if record:  # Skip None records
                f.write(json.dumps(record) + "\n")


@click.command()
@click.argument("input_file", type=click.Path(exists=True, path_type=Path))
@click.argument("output_file", type=click.Path(path_type=Path))
def main(input_file: Path, output_file: Path) -> None:
    """Split PRs in input NDJSON file into atomic diffs.

    INPUT_FILE: Path to input NDJSON file with raw PR records
    OUTPUT_FILE: Path to output NDJSON file for atomic diffs
    """
    # Ensure output directory exists
    output_file.parent.mkdir(parents=True, exist_ok=True)

    # Process records with progress bar
    records = read_ndjson(input_file)
    processed = (split_pr(record) for record in records)

    # Count total lines for progress bar
    total = sum(1 for _ in open(input_file))

    # Process with progress bar
    with tqdm(total=total, desc="Processing PRs") as pbar:
        write_ndjson(output_file, processed)
        pbar.update(total)


if __name__ == "__main__":
    main()
