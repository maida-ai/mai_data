"""CLI tool to split PRs into atomic diffs."""

import json
from collections.abc import Iterator
from pathlib import Path

import hydra
import typer
from omegaconf import OmegaConf
from tqdm import tqdm
from typer import Option

from mai_data.pr_split import split_pr

app = typer.Typer(help="Split GitHub PRs into atomic diffs")


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


@app.command()
def split_dump(
    input_file: Path = Option(
        ...,
        help="Path to input NDJSON file with raw PR records",
        exists=True,
    ),
    output_file: Path = Option(
        ...,
        help="Path to output NDJSON file for atomic diffs",
    ),
    config_path: str | None = Option(
        None,
        help="Path to custom config file (optional)",
    ),
) -> None:
    """Split PRs in input NDJSON file into atomic diffs."""
    # Load configuration
    if config_path:
        cfg = OmegaConf.load(config_path)
    else:
        with hydra.initialize_config_dir(config_dir="config"):
            cfg = hydra.compose(config_name="config")

    # Ensure output directory exists
    output_file.parent.mkdir(parents=True, exist_ok=True)

    # Process records with progress bar
    records = read_ndjson(input_file)
    processed = (split_pr(record, cfg.pr_split) for record in records)

    # Count total lines for progress bar
    total = sum(1 for _ in open(input_file))

    # Process with progress bar
    with tqdm(total=total, desc="Processing PRs") as pbar:
        write_ndjson(output_file, processed)
        pbar.update(total)


if __name__ == "__main__":
    app()
