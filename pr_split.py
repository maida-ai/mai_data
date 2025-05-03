"""Split GitHub PRs into atomic diffs based on directory structure and size."""

import hashlib
import time
from pathlib import Path

import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry

# Configure requests with retry logic
session = requests.Session()
retries = Retry(total=3, backoff_factor=0.5)
session.mount("https://", HTTPAdapter(max_retries=retries))

# Cache directory for downloaded diffs
CACHE_DIR = Path(".cache/diffs")
CACHE_DIR.mkdir(parents=True, exist_ok=True)


def get_cached_diff(url: str) -> str | None:
    """Get a diff from cache if available."""
    cache_key = hashlib.sha256(url.encode()).hexdigest()
    cache_path = CACHE_DIR / cache_key

    if cache_path.exists():
        return cache_path.read_text()
    return None


def cache_diff(url: str, content: str) -> None:
    """Cache a downloaded diff."""
    cache_key = hashlib.sha256(url.encode()).hexdigest()
    cache_path = CACHE_DIR / cache_key
    cache_path.write_text(content)


def fetch_diff(url: str) -> str:
    """Fetch a diff from GitHub with caching and rate limiting."""
    # Check cache first
    cached = get_cached_diff(url)
    if cached:
        return cached

    # Rate limit: 1 second between requests to same host
    time.sleep(1)

    response = session.get(url)
    response.raise_for_status()
    content = response.text

    # Cache the result
    cache_diff(url, content)
    return content


def parse_diff(diff_text: str) -> list[dict[str, str]]:
    """Parse a git diff into a list of file changes."""
    files = []
    current_file = None
    current_patch = []

    for line in diff_text.splitlines():
        if line.startswith("diff --git"):
            if current_file and current_patch:
                files.append({"path": current_file, "patch": "\n".join(current_patch)})
            current_file = line.split(" b/")[-1]
            current_patch = [line]
        elif current_file:
            current_patch.append(line)

    if current_file and current_patch:
        files.append({"path": current_file, "patch": "\n".join(current_patch)})

    return files


def count_loc(patch: str) -> int:
    """Count lines of code in a patch."""
    return sum(1 for line in patch.splitlines() if line.startswith("+") and not line.startswith("+++"))


def group_by_directory(files: list[dict[str, str]]) -> dict[str, list[dict[str, str]]]:
    """Group files by their top-level directory."""
    groups = {}
    for file in files:
        path = Path(file["path"])
        if len(path.parts) > 1:
            top_dir = path.parts[0]
            if top_dir not in groups:
                groups[top_dir] = []
            groups[top_dir].append(file)
    return groups


def split_pr(raw_json: dict) -> dict | None:
    """Split a PR into atomic diffs based on directory structure and size."""
    try:
        # Fetch the diff
        diff_text = fetch_diff(raw_json["diff_url"])

        # Parse into file changes
        files = parse_diff(diff_text)

        # Group by directory
        dir_groups = group_by_directory(files)

        # Check if we need to split further
        atomic_diffs = []
        for dir_name, dir_files in dir_groups.items():
            total_loc = sum(count_loc(f["patch"]) for f in dir_files)

            if total_loc > 500 or len(dir_groups) >= 3:
                # Split by individual files
                for file in dir_files:
                    atomic_diffs.append({"title": f"Update {file['path']}", "patch": file["patch"]})
            else:
                # Keep as one atomic diff
                atomic_diffs.append(
                    {"title": f"Update {dir_name} directory", "patch": "\n".join(f["patch"] for f in dir_files)}
                )

        # Quality filter
        if len(atomic_diffs) < 2:
            return None

        return {
            "pr_id": raw_json["pr_id"],
            "repo": raw_json["repo"],
            "original_diff": diff_text,
            "atomic_diffs": atomic_diffs,
        }

    except Exception as e:
        print(f"Error processing PR {raw_json.get('pr_id')}: {str(e)}")
        return None
