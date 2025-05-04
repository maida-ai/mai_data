"""Split GitHub PRs into atomic diffs based on directory structure and size."""

import hashlib
import logging
import time
from pathlib import Path

import requests
from omegaconf import DictConfig
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry

logger = logging.getLogger(__name__)

# Configure requests with retry logic
session = requests.Session()
retries = Retry(total=3, backoff_factor=0.5)
session.mount("https://", HTTPAdapter(max_retries=retries))


def get_cached_diff(url: str, cache_dir: Path) -> str | None:
    """Get a diff from cache if available."""
    cache_key = hashlib.sha256(url.encode()).hexdigest()
    cache_path = cache_dir / cache_key

    if cache_path.exists():
        return cache_path.read_text()
    return None


def cache_diff(url: str, content: str, cache_dir: Path) -> None:
    """Cache a downloaded diff."""
    cache_key = hashlib.sha256(url.encode()).hexdigest()
    cache_path = cache_dir / cache_key
    cache_path.write_text(content)


def fetch_diff(url: str, cfg: DictConfig) -> str:
    """Fetch a diff from GitHub with caching and rate limiting."""
    cache_dir = Path(cfg.cache.dir)
    cache_dir.mkdir(parents=True, exist_ok=True)

    # Check cache first if enabled
    if cfg.cache.enabled:
        cached = get_cached_diff(url, cache_dir)
        if cached:
            return cached

    # Rate limit if enabled
    if cfg.rate_limit.enabled:
        time.sleep(cfg.rate_limit.seconds)

    response = session.get(url)
    response.raise_for_status()
    content = response.text

    # Cache the result if enabled
    if cfg.cache.enabled:
        cache_diff(url, content, cache_dir)
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


def split_pr(raw_json: dict, cfg: DictConfig) -> dict | None:
    """Split a PR into atomic diffs based on directory structure and size."""
    try:
        if not raw_json:
            logger.warning("Received empty PR record")
            return None

        pr_id = raw_json.get("pr_id")
        diff_url = raw_json.get("diff_url")

        if not diff_url:
            logger.warning(f"PR {pr_id} has no diff_url. The available keys are {raw_json.keys()}")
            return None

        logger.info(f"Processing PR {pr_id} from {raw_json.get('repo')}")

        # Fetch the diff
        diff_text = fetch_diff(diff_url, cfg)

        # Parse into file changes
        files = parse_diff(diff_text)
        logger.info(f"Found {len(files)} files in PR {pr_id}")

        # Group by directory
        dir_groups = group_by_directory(files)
        logger.info(f"Grouped into {len(dir_groups)} directories")

        # Check if we need to split further
        atomic_diffs = []
        for dir_name, dir_files in dir_groups.items():
            total_loc = sum(count_loc(f["patch"]) for f in dir_files)
            logger.debug(f"Directory {dir_name} has {total_loc} LOC")

            if total_loc > cfg.max_loc or len(dir_groups) >= cfg.max_dirs:
                # Split by individual files
                for file in dir_files:
                    atomic_diffs.append({"title": f"Update {file['path']}", "patch": file["patch"]})
            else:
                # Keep as one atomic diff
                atomic_diffs.append(
                    {"title": f"Update {dir_name} directory", "patch": "\n".join(f["patch"] for f in dir_files)}
                )

        # Quality filter
        if len(atomic_diffs) < cfg.min_diffs:
            logger.info(f"PR {pr_id} has too few diffs ({len(atomic_diffs)} < {cfg.min_diffs})")
            return None

        return {
            "pr_id": pr_id,
            "repo": raw_json.get("repo"),
            "original_diff": diff_text,
            "atomic_diffs": atomic_diffs,
        }

    except Exception as e:
        logger.error(f"Error processing PR {raw_json.get('pr_id')}: {str(e)}", exc_info=True)
        return None
