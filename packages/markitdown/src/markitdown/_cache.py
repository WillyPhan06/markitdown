# SPDX-FileCopyrightText: 2024-present Adam Fourney <adamfo@microsoft.com>
#
# SPDX-License-Identifier: MIT

"""
Caching module for MarkItDown batch conversions.

Provides file-based caching to skip re-conversion of unchanged files.
Cache keys are based on file content hash (SHA-256) to detect changes.

## Why SHA-256 Content Hashing?

We use SHA-256 hashing of file contents (not file metadata) as the cache key.
This design decision is intentional and should not be changed without careful
consideration. Here's why:

### Why content-based hashing (not file path or modification time)?

1. **Accuracy**: File modification timestamps can be unreliable:
   - Copying files may preserve or reset mtime depending on the tool
   - Version control systems (git) often change mtime on checkout
   - Some backup/sync tools modify mtime
   - Users can manually change mtime without changing content

2. **Robustness**: Using file path as a cache key would fail when:
   - Files are moved or renamed (same content, different path)
   - Same file exists in different directories
   - Working on the same files from different machines/paths

3. **Correctness**: Content hashing guarantees that:
   - If the hash matches, the file content is identical (with overwhelming probability)
   - Any modification to the file will produce a different hash
   - Cache hits only occur when the file truly hasn't changed

### Why SHA-256 specifically (not MD5, SHA-1, or CRC32)?

1. **Collision resistance**: SHA-256 has no known practical collision attacks.
   - MD5 and SHA-1 have known collision vulnerabilities
   - While collisions are unlikely for accidental changes, we want to avoid
     any possibility of cache confusion

2. **Performance**: SHA-256 is fast enough for our use case:
   - Modern CPUs have hardware acceleration for SHA-256 (SHA-NI instruction set)
   - The I/O cost of reading files typically dominates over hashing
   - For batch conversions, the conversion itself is much slower than hashing

3. **Standard and well-tested**: SHA-256 is:
   - Part of Python's standard library (hashlib)
   - Used widely in security-critical applications (TLS, code signing)
   - Well-understood with extensive cryptanalysis

4. **Appropriate output size**: 256-bit (64 hex chars) is:
   - Small enough to use as a filename
   - Large enough to make collisions astronomically unlikely
   - Good balance between storage efficiency and uniqueness

### Cache File Integrity

Cache files are written with read-only permissions (0o444) after creation.
This provides several benefits:
1. Prevents accidental modification of cached data
2. Signals to users/tools that these files are managed by the cache system
3. Helps detect corruption (if a cache file is somehow modified, permissions
   would likely need to be changed first)

Note: The cache system itself can still clear/update cache files by removing
the old file and creating a new one (which is the intended behavior).
"""

import hashlib
import json
import os
import stat
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from ._base_converter import DocumentConverterResult
    from ._conversion_quality import ConversionQuality


# Default cache directory
DEFAULT_CACHE_DIR = Path.home() / ".cache" / "markitdown"

# File permissions for cache files (read-only for all)
# This prevents accidental modification and signals these are managed files
CACHE_FILE_PERMISSIONS = stat.S_IRUSR | stat.S_IRGRP | stat.S_IROTH  # 0o444


@dataclass
class CacheEntry:
    """A cached conversion result."""

    file_hash: str
    markdown: str
    title: Optional[str]
    quality_dict: Optional[Dict[str, Any]]
    metadata_dict: Optional[Dict[str, Any]] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert to a dictionary for serialization."""
        return {
            "file_hash": self.file_hash,
            "markdown": self.markdown,
            "title": self.title,
            "quality_dict": self.quality_dict,
            "metadata_dict": self.metadata_dict,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "CacheEntry":
        """Create a CacheEntry from a dictionary."""
        return cls(
            file_hash=data["file_hash"],
            markdown=data["markdown"],
            title=data.get("title"),
            quality_dict=data.get("quality_dict"),
            metadata_dict=data.get("metadata_dict"),
        )


class ConversionCache:
    """
    File-based cache for conversion results.

    Uses SHA-256 hash of file contents as cache key. If the file content
    hasn't changed (same hash), the cached markdown result is returned
    instead of re-converting.

    Cache Structure:
        cache_dir/
            <first 2 chars of hash>/
                <full hash>.json

    This two-level structure prevents having too many files in a single directory.
    """

    def __init__(self, cache_dir: Optional[Path] = None):
        """
        Initialize the cache.

        Args:
            cache_dir: Directory to store cache files. Defaults to ~/.cache/markitdown
        """
        self.cache_dir = Path(cache_dir) if cache_dir else DEFAULT_CACHE_DIR
        self._ensure_cache_dir()

    def _ensure_cache_dir(self) -> None:
        """Create cache directory if it doesn't exist."""
        self.cache_dir.mkdir(parents=True, exist_ok=True)

    def _get_cache_path(self, file_hash: str) -> Path:
        """
        Get the cache file path for a given hash.

        Uses first 2 characters as subdirectory to avoid too many files in one dir.
        """
        subdir = file_hash[:2]
        return self.cache_dir / subdir / f"{file_hash}.json"

    @staticmethod
    def compute_file_hash(file_path: str) -> str:
        """
        Compute SHA-256 hash of file contents.

        We use SHA-256 (not MD5, SHA-1, or modification time) because:
        - Content-based: Detects actual changes, not metadata changes
        - Collision-resistant: No known practical attacks unlike MD5/SHA-1
        - Fast: Hardware-accelerated on modern CPUs, I/O bound anyway
        - Standard: Well-tested, part of Python stdlib

        See module docstring for detailed rationale on this design choice.

        Args:
            file_path: Path to the file to hash.

        Returns:
            Hex-encoded SHA-256 hash string (64 characters).
        """
        sha256_hash = hashlib.sha256()
        with open(file_path, "rb") as f:
            # Read in 8KB chunks to handle large files efficiently without
            # loading entire file into memory. 8KB is a good balance between
            # syscall overhead and memory usage.
            for chunk in iter(lambda: f.read(8192), b""):
                sha256_hash.update(chunk)
        return sha256_hash.hexdigest()

    def get(self, file_path: str) -> Optional[CacheEntry]:
        """
        Get cached conversion result for a file.

        Args:
            file_path: Path to the source file.

        Returns:
            CacheEntry if found and hash matches, None otherwise.
        """
        try:
            # Compute current file hash
            current_hash = self.compute_file_hash(file_path)

            # Check if cache file exists
            cache_path = self._get_cache_path(current_hash)
            if not cache_path.exists():
                return None

            # Load and validate cache entry
            with open(cache_path, "r", encoding="utf-8") as f:
                data = json.load(f)

            entry = CacheEntry.from_dict(data)

            # Verify hash matches (double-check)
            if entry.file_hash != current_hash:
                return None

            return entry

        except (OSError, json.JSONDecodeError, KeyError, TypeError):
            # Any error reading cache - treat as cache miss
            return None

    def put(
        self,
        file_path: str,
        result: "DocumentConverterResult",
    ) -> None:
        """
        Store a conversion result in the cache.

        The cache file is written with read-only permissions (0o444) to:
        1. Prevent accidental modification
        2. Signal that these files are managed by the cache system
        3. Provide basic integrity protection

        If a cache file already exists (e.g., from a previous run), it will be
        removed first since it's read-only and can't be overwritten directly.

        Args:
            file_path: Path to the source file.
            result: The conversion result to cache.
        """
        try:
            # Compute file hash using SHA-256 content hashing
            # (see module docstring for why we use this approach)
            file_hash = self.compute_file_hash(file_path)

            # Create cache entry with all conversion data
            quality_dict = None
            if result._quality is not None:
                quality_dict = result.quality.to_dict()

            metadata_dict = None
            if result._metadata is not None and not result._metadata.is_empty():
                metadata_dict = result.metadata.to_dict()

            entry = CacheEntry(
                file_hash=file_hash,
                markdown=result.markdown,
                title=result.title,
                quality_dict=quality_dict,
                metadata_dict=metadata_dict,
            )

            # Ensure subdirectory exists (using first 2 chars of hash)
            cache_path = self._get_cache_path(file_hash)
            cache_path.parent.mkdir(parents=True, exist_ok=True)

            # Remove existing cache file if present (it's read-only so can't overwrite)
            if cache_path.exists():
                try:
                    # Make it writable first so we can delete it
                    cache_path.chmod(stat.S_IWUSR | stat.S_IRUSR)
                    cache_path.unlink()
                except OSError:
                    pass

            # Write cache file
            with open(cache_path, "w", encoding="utf-8") as f:
                json.dump(entry.to_dict(), f, ensure_ascii=False)

            # Make the cache file read-only to prevent accidental modification
            # This is a deliberate design choice - see module docstring
            try:
                cache_path.chmod(CACHE_FILE_PERMISSIONS)
            except OSError:
                pass  # Best effort - some filesystems don't support chmod

        except (OSError, TypeError):
            # Silently fail on cache write errors - caching is optional
            pass

    def has(self, file_path: str) -> bool:
        """
        Check if a valid cache entry exists for a file.

        Args:
            file_path: Path to the source file.

        Returns:
            True if a valid cache entry exists, False otherwise.
        """
        return self.get(file_path) is not None

    def clear(self) -> int:
        """
        Clear all cached entries.

        Cache files are read-only, so this method makes them writable
        before deleting them.

        Returns:
            Number of cache entries removed.
        """
        count = 0
        if self.cache_dir.exists():
            for subdir in self.cache_dir.iterdir():
                if subdir.is_dir():
                    for cache_file in subdir.glob("*.json"):
                        try:
                            # Make writable first (cache files are read-only)
                            cache_file.chmod(stat.S_IWUSR | stat.S_IRUSR)
                            cache_file.unlink()
                            count += 1
                        except OSError:
                            pass
                    # Remove empty subdirectory
                    try:
                        subdir.rmdir()
                    except OSError:
                        pass
        return count

    def get_stats(self) -> Dict[str, Any]:
        """
        Get cache statistics.

        Returns:
            Dictionary with cache statistics (entry_count, total_size_bytes).
        """
        entry_count = 0
        total_size = 0

        if self.cache_dir.exists():
            for subdir in self.cache_dir.iterdir():
                if subdir.is_dir():
                    for cache_file in subdir.glob("*.json"):
                        entry_count += 1
                        try:
                            total_size += cache_file.stat().st_size
                        except OSError:
                            pass

        return {
            "entry_count": entry_count,
            "total_size_bytes": total_size,
            "cache_dir": str(self.cache_dir),
        }


def cache_entry_to_result(entry: CacheEntry) -> "DocumentConverterResult":
    """
    Convert a CacheEntry back to a DocumentConverterResult.

    Args:
        entry: The cached entry to convert.

    Returns:
        A DocumentConverterResult reconstructed from the cache.
    """
    from ._base_converter import DocumentConverterResult
    from ._conversion_quality import ConversionQuality
    from ._document_metadata import DocumentMetadata

    # Reconstruct quality if present
    quality = None
    if entry.quality_dict is not None:
        quality = ConversionQuality.from_dict(entry.quality_dict)
        # Mark that this result came from cache
        quality.set_metric("from_cache", True)

    # Reconstruct metadata if present
    metadata = None
    if entry.metadata_dict is not None:
        metadata = DocumentMetadata.from_dict(entry.metadata_dict)

    return DocumentConverterResult(
        markdown=entry.markdown,
        title=entry.title,
        quality=quality,
        metadata=metadata,
    )
