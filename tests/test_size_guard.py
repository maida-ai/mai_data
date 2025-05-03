"""Tests for the size guard utility."""

import os
import tempfile
from pathlib import Path

import pytest

from mai_data.size_guard import check_repo_size, get_large_files


def test_get_large_files_empty_dir():
    """Test get_large_files with an empty directory."""
    with tempfile.TemporaryDirectory() as tmpdir:
        large_files = list(get_large_files(tmpdir))
        assert len(large_files) == 0


def test_get_large_files_with_large_file():
    """Test get_large_files with a file exceeding the size limit."""
    with tempfile.TemporaryDirectory() as tmpdir:
        # Create a 201MB file
        large_file = Path(tmpdir) / "large.bin"
        with open(large_file, "wb") as f:
            f.write(b"0" * (201 * 1024 * 1024))

        large_files = list(get_large_files(tmpdir))
        assert len(large_files) == 1
        assert large_files[0][0] == large_file


def test_get_large_files_ignore_patterns():
    """Test that get_large_files ignores specified file patterns."""
    with tempfile.TemporaryDirectory() as tmpdir:
        # Create a large .md file
        large_md = Path(tmpdir) / "large.md"
        with open(large_md, "wb") as f:
            f.write(b"0" * (201 * 1024 * 1024))

        large_files = list(get_large_files(tmpdir))
        assert len(large_files) == 0


def test_check_repo_size():
    """Test the check_repo_size function."""
    with tempfile.TemporaryDirectory() as tmpdir:
        # Create a small file
        small_file = Path(tmpdir) / "small.txt"
        with open(small_file, "wb") as f:
            f.write(b"0" * 1024)

        assert check_repo_size(tmpdir)

        # Create a large file
        large_file = Path(tmpdir) / "large.bin"
        with open(large_file, "wb") as f:
            f.write(b"0" * (201 * 1024 * 1024))

        assert not check_repo_size(tmpdir)
