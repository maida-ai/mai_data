"""Split GitHub PRs into atomic diffs based on directory structure and size."""

import hashlib
import logging
import os
import time
from datetime import datetime
from functools import cache
from pathlib import Path
from urllib.parse import urlparse

import requests
from omegaconf import DictConfig
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry

_log = logging.getLogger(__name__)
GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")  # personal access-token optional
HEADERS = {"Authorization": f"token {GITHUB_TOKEN}"} if GITHUB_TOKEN else {}
HEADERS["Accept"] = "application/vnd.github.v3.diff"  # Request diff format

# Log token status (without exposing the actual token)
if GITHUB_TOKEN:
    _log.info("GitHub token found in environment")
    _log.info("Token prefix: %s...", GITHUB_TOKEN[:4])  # Show first 4 chars for verification
    _log.info("Headers being used: %s", {k: "***" if k == "Authorization" else v for k, v in HEADERS.items()})
else:
    _log.warning("No GitHub token found in environment - using unauthenticated requests")

# Rate limit tracking
_rate_limit_info = {
    "core": {"remaining": 5000, "reset": 0},  # Default values
    "search": {"remaining": 30, "reset": 0},
}


def update_rate_limit_info():
    """Update rate limit information from GitHub API."""
    try:
        _log.info(
            "Checking rate limits with headers: %s",
            {k: "***" if k == "Authorization" else v for k, v in HEADERS.items()},
        )
        resp = requests.get("https://api.github.com/rate_limit", headers=HEADERS)
        resp.raise_for_status()
        data = resp.json()
        _rate_limit_info["core"] = data["resources"]["core"]
        _rate_limit_info["search"] = data["resources"]["search"]

        reset_time = datetime.fromtimestamp(_rate_limit_info["core"]["reset"])
        _log.info(
            "Rate limits - Core: %d remaining (resets at %s), Search: %d remaining",
            _rate_limit_info["core"]["remaining"],
            reset_time.strftime("%H:%M:%S"),
            _rate_limit_info["search"]["remaining"],
        )

        # Log response headers for debugging
        _log.info("Response headers: %s", dict(resp.headers))
    except Exception as e:
        _log.warning("Failed to update rate limit info: %s", e)


# Update rate limits on startup
update_rate_limit_info()

# simple in-memory token-bucket keyed by hostname
_last_hit: dict[str, float] = {}
HOST_RATE = {
    "api.github.com": 1.0,  # seconds between hits
    "patch-diff.githubusercontent.com": 1.6,  #   »    ≈ 37/min
}

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


def convert_to_api_url(web_url: str) -> str:
    """Convert GitHub web URL to API URL."""
    # Example: https://github.com/owner/repo/pull/123.diff
    # to: https://api.github.com/repos/owner/repo/pulls/123
    parts = urlparse(web_url)
    path_parts = parts.path.strip("/").split("/")
    if len(path_parts) >= 4 and path_parts[-1].endswith(".diff"):
        owner = path_parts[0]
        repo = path_parts[1]
        pr_number = path_parts[3].replace(".diff", "")
        return f"https://api.github.com/repos/{owner}/{repo}/pulls/{pr_number}"
    return web_url


@cache
def fetch_diff(url: str, timeout: int = 30) -> str:
    # Convert web URL to API URL
    api_url = convert_to_api_url(url)
    _log.info("Converted %s to API URL: %s", url, api_url)

    host = urlparse(api_url).hostname

    # Update rate limit info periodically
    if time.time() % 60 < 1:  # Update roughly once per minute
        update_rate_limit_info()

    # Check if we're close to rate limit
    if _rate_limit_info["core"]["remaining"] < 100:
        reset_time = datetime.fromtimestamp(_rate_limit_info["core"]["reset"])
        _log.warning(
            "Rate limit low (%d remaining, resets at %s) - sleeping for 60s",
            _rate_limit_info["core"]["remaining"],
            reset_time.strftime("%H:%M:%S"),
        )
        time.sleep(60)
        update_rate_limit_info()

    # Apply host-specific rate limiting
    sleep_for = HOST_RATE.get(host, 0.0)
    now = time.time()
    if host in _last_hit:
        delta = now - _last_hit[host]
        if delta < sleep_for:
            time.sleep(sleep_for - delta)
    _last_hit[host] = time.time()

    _log.info(
        "Fetching %s with headers: %s", api_url, {k: "***" if k == "Authorization" else v for k, v in HEADERS.items()}
    )
    resp = requests.get(api_url, timeout=timeout, headers=HEADERS)

    # Log response headers for debugging
    _log.info("Response headers: %s", dict(resp.headers))

    # Update rate limit info after each request
    if "X-RateLimit-Remaining" in resp.headers:
        _rate_limit_info["core"]["remaining"] = int(resp.headers["X-RateLimit-Remaining"])
        _rate_limit_info["core"]["reset"] = int(resp.headers["X-RateLimit-Reset"])
        _log.info(
            "Updated rate limit - Remaining: %d, Reset: %s",
            _rate_limit_info["core"]["remaining"],
            datetime.fromtimestamp(_rate_limit_info["core"]["reset"]).strftime("%H:%M:%S"),
        )

    if resp.status_code == 404:
        raise FileNotFoundError("diff_url vanished (repo deleted/private)")
    if resp.status_code == 429:
        retry = int(resp.headers.get("Retry-After", "60"))
        _log.warning("429 from %s – sleeping %s s", host, retry)
        time.sleep(retry)
        update_rate_limit_info()  # Update rate limits after retry
        return fetch_diff(url)  # one retry
    resp.raise_for_status()
    return resp.text


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
            _log.warning("Received empty PR record")
            return None

        pr_id = raw_json.get("pr_id")
        diff_url = raw_json.get("diff_url")

        if not diff_url:
            _log.warning(f"PR {pr_id} has no diff_url. The available keys are {raw_json.keys()}")
            return None

        _log.info(f"Processing PR {pr_id} from {raw_json.get('repo')}")

        # Fetch the diff
        try:
            diff_text = fetch_diff(diff_url)
        except FileNotFoundError:
            _log.info(f"PR {pr_id} diff vanished (repo deleted/private) - skipping")
            return None
        except Exception as exc:
            _log.error(f"Fatal fetch for {diff_url}: {exc}")
            return None

        # Parse into file changes
        files = parse_diff(diff_text)
        _log.info(f"Found {len(files)} files in PR {pr_id}")

        # Group by directory
        dir_groups = group_by_directory(files)
        _log.info(f"Grouped into {len(dir_groups)} directories")

        # Check if we need to split further
        atomic_diffs = []
        for dir_name, dir_files in dir_groups.items():
            total_loc = sum(count_loc(f["patch"]) for f in dir_files)
            _log.debug(f"Directory {dir_name} has {total_loc} LOC")

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
            _log.info(f"PR {pr_id} has too few diffs ({len(atomic_diffs)} < {cfg.min_diffs})")
            return None

        return {
            "pr_id": pr_id,
            "repo": raw_json.get("repo"),
            "original_diff": diff_text,
            "atomic_diffs": atomic_diffs,
        }

    except Exception as e:
        _log.error(f"Error processing PR {raw_json.get('pr_id')}: {str(e)}", exc_info=True)
        return None
