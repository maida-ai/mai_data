"""Size guard utilities for checking file sizes in the repository."""

from collections.abc import Iterator
from pathlib import Path


def get_large_files(
    root_dir: str | Path,
    max_size_mb: int = 200,
    ignore_patterns: list[str] | None = None,
) -> Iterator[tuple[Path, int]]:
    """Find files larger than the specified size limit.

    Args:
        root_dir: Root directory to search
        max_size_mb: Maximum file size in megabytes
        ignore_patterns: List of file extensions to ignore

    Yields:
        Tuples of (file_path, size_in_bytes) for files exceeding the size limit
    """
    ignore_patterns = [".md", ".txt"] if ignore_patterns is None else ignore_patterns
    max_size_bytes = max_size_mb * 1024 * 1024
    root_path = Path(root_dir)

    for path in root_path.rglob("*"):
        if not path.is_file():
            continue

        # Skip ignored file patterns
        if any(path.suffix.endswith(ext) for ext in ignore_patterns):
            continue

        size = path.stat().st_size
        if size >= max_size_bytes:
            yield path, size


def check_repo_size(root_dir: str | Path = ".", max_size_mb: int = 200) -> bool:
    """Check if any files in the repository exceed the size limit.

    Args:
        root_dir: Root directory to check
        max_size_mb: Maximum file size in megabytes

    Returns:
        True if all files are within size limits, False otherwise
    """
    large_files = list(get_large_files(root_dir, max_size_mb))

    if large_files:
        print("Found files exceeding size limit:")
        for path, size in large_files:
            size_mb = size / (1024 * 1024)
            print(f"  {path}: {size_mb:.1f} MB")
        return False

    return True
