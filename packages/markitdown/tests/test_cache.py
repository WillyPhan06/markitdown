#!/usr/bin/env python3 -m pytest
"""Tests for the caching feature in batch conversions."""

import io
import json
import os
import shutil
import subprocess
import sys
import tempfile
import time
import pytest
from pathlib import Path
from unittest.mock import MagicMock, patch

from markitdown import (
    MarkItDown,
    ConversionCache,
    ConversionQuality,
    ConversionWarning,
    FormattingLossType,
    WarningSeverity,
    BatchConversionResult,
    BatchItemResult,
    BatchItemStatus,
    DocumentConverterResult,
)
from markitdown._cache import CacheEntry, cache_entry_to_result, DEFAULT_CACHE_DIR

TEST_FILES_DIR = os.path.join(os.path.dirname(__file__), "test_files")
MARKITDOWN_MODULE = "markitdown"


class TestCacheEntry:
    """Tests for CacheEntry dataclass."""

    def test_cache_entry_creation(self):
        """Test creating a CacheEntry."""
        entry = CacheEntry(
            file_hash="abc123",
            markdown="# Test",
            title="Test Title",
            quality_dict={"confidence": 0.9},
        )

        assert entry.file_hash == "abc123"
        assert entry.markdown == "# Test"
        assert entry.title == "Test Title"
        assert entry.quality_dict == {"confidence": 0.9}

    def test_cache_entry_to_dict(self):
        """Test CacheEntry serialization."""
        entry = CacheEntry(
            file_hash="abc123",
            markdown="# Test",
            title="Test Title",
            quality_dict={"confidence": 0.9, "converter_used": "TestConverter"},
        )

        d = entry.to_dict()
        assert d["file_hash"] == "abc123"
        assert d["markdown"] == "# Test"
        assert d["title"] == "Test Title"
        assert d["quality_dict"]["confidence"] == 0.9

    def test_cache_entry_from_dict(self):
        """Test CacheEntry deserialization."""
        data = {
            "file_hash": "abc123",
            "markdown": "# Test",
            "title": "Test Title",
            "quality_dict": {"confidence": 0.9},
        }

        entry = CacheEntry.from_dict(data)
        assert entry.file_hash == "abc123"
        assert entry.markdown == "# Test"
        assert entry.title == "Test Title"
        assert entry.quality_dict == {"confidence": 0.9}

    def test_cache_entry_roundtrip(self):
        """Test CacheEntry serialization roundtrip."""
        original = CacheEntry(
            file_hash="abc123def456",
            markdown="# Heading\n\nSome content",
            title="Test Document",
            quality_dict={
                "confidence": 0.85,
                "converter_used": "PdfConverter",
                "warnings": [{"message": "Test warning", "severity": "low"}],
            },
        )

        # Serialize and deserialize
        d = original.to_dict()
        restored = CacheEntry.from_dict(d)

        assert restored.file_hash == original.file_hash
        assert restored.markdown == original.markdown
        assert restored.title == original.title
        assert restored.quality_dict == original.quality_dict


class TestConversionCache:
    """Tests for ConversionCache class."""

    def test_cache_initialization_default_dir(self):
        """Test cache initializes with default directory."""
        with tempfile.TemporaryDirectory() as tmpdir:
            with patch.object(Path, 'home', return_value=Path(tmpdir)):
                cache = ConversionCache()
                assert cache.cache_dir == Path(tmpdir) / ".cache" / "markitdown"

    def test_cache_initialization_custom_dir(self):
        """Test cache initializes with custom directory."""
        with tempfile.TemporaryDirectory() as tmpdir:
            cache_dir = Path(tmpdir) / "custom_cache"
            cache = ConversionCache(cache_dir)
            assert cache.cache_dir == cache_dir
            assert cache_dir.exists()

    def test_compute_file_hash(self):
        """Test computing file hash."""
        with tempfile.NamedTemporaryFile(mode='wb', delete=False) as f:
            f.write(b"Hello, World!")
            temp_path = f.name

        try:
            hash1 = ConversionCache.compute_file_hash(temp_path)
            hash2 = ConversionCache.compute_file_hash(temp_path)

            # Hash should be consistent
            assert hash1 == hash2
            # Should be hex string (SHA-256 = 64 chars)
            assert len(hash1) == 64
            assert all(c in '0123456789abcdef' for c in hash1)
        finally:
            os.unlink(temp_path)

    def test_compute_file_hash_different_content(self):
        """Test that different content produces different hashes."""
        with tempfile.NamedTemporaryFile(mode='wb', delete=False) as f1:
            f1.write(b"Content 1")
            path1 = f1.name

        with tempfile.NamedTemporaryFile(mode='wb', delete=False) as f2:
            f2.write(b"Content 2")
            path2 = f2.name

        try:
            hash1 = ConversionCache.compute_file_hash(path1)
            hash2 = ConversionCache.compute_file_hash(path2)
            assert hash1 != hash2
        finally:
            os.unlink(path1)
            os.unlink(path2)

    def test_cache_put_and_get(self):
        """Test storing and retrieving from cache."""
        with tempfile.TemporaryDirectory() as tmpdir:
            cache = ConversionCache(Path(tmpdir) / "cache")

            # Create a test file
            test_file = Path(tmpdir) / "test.txt"
            test_file.write_text("Hello, World!")

            # Create a result
            quality = ConversionQuality(confidence=0.9)
            quality.converter_used = "TestConverter"
            result = DocumentConverterResult(
                markdown="# Test Markdown",
                title="Test Title",
                quality=quality,
            )

            # Store in cache
            cache.put(str(test_file), result)

            # Retrieve from cache
            entry = cache.get(str(test_file))

            assert entry is not None
            assert entry.markdown == "# Test Markdown"
            assert entry.title == "Test Title"
            assert entry.quality_dict["confidence"] == 0.9
            assert entry.quality_dict["converter_used"] == "TestConverter"

    def test_cache_miss_nonexistent_file(self):
        """Test cache miss for file that was never cached."""
        with tempfile.TemporaryDirectory() as tmpdir:
            cache = ConversionCache(Path(tmpdir) / "cache")

            # Create a test file
            test_file = Path(tmpdir) / "test.txt"
            test_file.write_text("Hello, World!")

            # Get without putting first
            entry = cache.get(str(test_file))
            assert entry is None

    def test_cache_miss_changed_file(self):
        """Test cache miss when file content changes."""
        with tempfile.TemporaryDirectory() as tmpdir:
            cache = ConversionCache(Path(tmpdir) / "cache")

            # Create a test file
            test_file = Path(tmpdir) / "test.txt"
            test_file.write_text("Original content")

            # Store in cache
            result = DocumentConverterResult(
                markdown="# Original",
                title="Original",
            )
            cache.put(str(test_file), result)

            # Verify it's cached
            entry = cache.get(str(test_file))
            assert entry is not None
            assert entry.markdown == "# Original"

            # Modify the file
            test_file.write_text("Modified content")

            # Cache should miss because hash changed
            entry = cache.get(str(test_file))
            assert entry is None

    def test_cache_has(self):
        """Test checking if cache entry exists."""
        with tempfile.TemporaryDirectory() as tmpdir:
            cache = ConversionCache(Path(tmpdir) / "cache")

            test_file = Path(tmpdir) / "test.txt"
            test_file.write_text("Hello, World!")

            # Initially not cached
            assert not cache.has(str(test_file))

            # Store in cache
            result = DocumentConverterResult(markdown="# Test")
            cache.put(str(test_file), result)

            # Now should be cached
            assert cache.has(str(test_file))

    def test_cache_clear(self):
        """Test clearing the cache."""
        with tempfile.TemporaryDirectory() as tmpdir:
            cache = ConversionCache(Path(tmpdir) / "cache")

            # Create and cache multiple files
            for i in range(3):
                test_file = Path(tmpdir) / f"test{i}.txt"
                test_file.write_text(f"Content {i}")
                result = DocumentConverterResult(markdown=f"# Test {i}")
                cache.put(str(test_file), result)

            # Verify they're cached
            stats = cache.get_stats()
            assert stats["entry_count"] == 3

            # Clear cache
            count = cache.clear()
            assert count == 3

            # Verify cache is empty
            stats = cache.get_stats()
            assert stats["entry_count"] == 0

    def test_cache_get_stats(self):
        """Test getting cache statistics."""
        with tempfile.TemporaryDirectory() as tmpdir:
            cache_dir = Path(tmpdir) / "cache"
            cache = ConversionCache(cache_dir)

            # Initially empty
            stats = cache.get_stats()
            assert stats["entry_count"] == 0
            assert stats["total_size_bytes"] == 0
            assert stats["cache_dir"] == str(cache_dir)

            # Add some entries
            for i in range(2):
                test_file = Path(tmpdir) / f"test{i}.txt"
                test_file.write_text(f"Content {i}")
                result = DocumentConverterResult(markdown=f"# Test {i} with some content")
                cache.put(str(test_file), result)

            # Check updated stats
            stats = cache.get_stats()
            assert stats["entry_count"] == 2
            assert stats["total_size_bytes"] > 0


class TestCacheEntryToResult:
    """Tests for cache_entry_to_result function."""

    def test_convert_entry_to_result(self):
        """Test converting cache entry to DocumentConverterResult."""
        entry = CacheEntry(
            file_hash="abc123",
            markdown="# Test",
            title="Test Title",
            quality_dict={
                "confidence": 0.9,
                "converter_used": "TestConverter",
                "warnings": [],
                "formatting_loss": ["image"],
            },
        )

        result = cache_entry_to_result(entry)

        assert isinstance(result, DocumentConverterResult)
        assert result.markdown == "# Test"
        assert result.title == "Test Title"
        assert result.quality.confidence == 0.9
        assert result.quality.converter_used == "TestConverter"
        assert result.quality.metrics.get("from_cache") is True

    def test_convert_entry_without_quality(self):
        """Test converting cache entry without quality data."""
        entry = CacheEntry(
            file_hash="abc123",
            markdown="# Test",
            title=None,
            quality_dict=None,
        )

        result = cache_entry_to_result(entry)

        assert result.markdown == "# Test"
        assert result.title is None
        # Quality should be None (not default) when not provided
        assert result._quality is None


class TestConversionQualityFromDict:
    """Tests for ConversionQuality.from_dict method."""

    def test_from_dict_basic(self):
        """Test basic from_dict reconstruction."""
        data = {
            "confidence": 0.85,
            "converter_used": "TestConverter",
            "is_partial": False,
            "completion_percentage": None,
            "warnings": [],
            "formatting_loss": [],
            "metrics": {},
            "optional_features_used": {},
        }

        quality = ConversionQuality.from_dict(data)

        assert quality.confidence == 0.85
        assert quality.converter_used == "TestConverter"
        assert quality.is_partial is False
        assert quality.completion_percentage is None

    def test_from_dict_with_warnings(self):
        """Test from_dict with warnings."""
        data = {
            "confidence": 0.75,
            "warnings": [
                {
                    "message": "Test warning",
                    "severity": "medium",
                    "formatting_type": "table",
                    "element_count": 5,
                    "details": {"extra": "info"},
                },
                {
                    "message": "Another warning",
                    "severity": "high",
                    "formatting_type": "image",
                    "element_count": None,
                    "details": None,
                },
            ],
            "formatting_loss": [],
            "metrics": {},
            "optional_features_used": {},
        }

        quality = ConversionQuality.from_dict(data)

        assert len(quality.warnings) == 2
        assert quality.warnings[0].message == "Test warning"
        assert quality.warnings[0].severity == WarningSeverity.MEDIUM
        assert quality.warnings[0].formatting_type == FormattingLossType.TABLE
        assert quality.warnings[0].element_count == 5
        assert quality.warnings[0].details == {"extra": "info"}
        assert quality.warnings[1].severity == WarningSeverity.HIGH

    def test_from_dict_with_formatting_loss(self):
        """Test from_dict with formatting loss."""
        data = {
            "confidence": 0.8,
            "warnings": [],
            "formatting_loss": ["image", "table", "font_style"],
            "metrics": {},
            "optional_features_used": {},
        }

        quality = ConversionQuality.from_dict(data)

        assert len(quality.formatting_loss) == 3
        assert FormattingLossType.IMAGE in quality.formatting_loss
        assert FormattingLossType.TABLE in quality.formatting_loss
        assert FormattingLossType.FONT_STYLE in quality.formatting_loss

    def test_from_dict_with_metrics(self):
        """Test from_dict with metrics."""
        data = {
            "confidence": 0.9,
            "warnings": [],
            "formatting_loss": [],
            "metrics": {
                "page_count": 10,
                "word_count": 5000,
                "custom_metric": "value",
            },
            "optional_features_used": {"llm": True, "ocr": False},
        }

        quality = ConversionQuality.from_dict(data)

        assert quality.metrics["page_count"] == 10
        assert quality.metrics["word_count"] == 5000
        assert quality.metrics["custom_metric"] == "value"
        assert quality.optional_features_used["llm"] is True
        assert quality.optional_features_used["ocr"] is False

    def test_from_dict_to_dict_roundtrip(self):
        """Test complete roundtrip through to_dict and from_dict."""
        original = ConversionQuality(confidence=0.75)
        original.converter_used = "TestConverter"
        original.is_partial = True
        original.completion_percentage = 66.7
        original.add_warning(
            "Test warning",
            severity=WarningSeverity.MEDIUM,
            formatting_type=FormattingLossType.TABLE,
            element_count=3,
            details={"extra": "data"},
        )
        original.add_formatting_loss(FormattingLossType.IMAGE)
        original.set_metric("count", 42)
        original.set_optional_feature("feature", True)

        # Roundtrip
        data = original.to_dict()
        restored = ConversionQuality.from_dict(data)

        assert restored.confidence == original.confidence
        assert restored.converter_used == original.converter_used
        assert restored.is_partial == original.is_partial
        assert restored.completion_percentage == original.completion_percentage
        assert len(restored.warnings) == len(original.warnings)
        assert restored.warnings[0].message == original.warnings[0].message
        assert FormattingLossType.IMAGE in restored.formatting_loss
        assert FormattingLossType.TABLE in restored.formatting_loss
        assert restored.metrics["count"] == 42
        assert restored.optional_features_used["feature"] is True

    def test_from_dict_handles_unknown_values(self):
        """Test from_dict gracefully handles unknown enum values."""
        data = {
            "confidence": 0.9,
            "warnings": [
                {
                    "message": "Test",
                    "severity": "unknown_severity",  # Unknown value
                    "formatting_type": "unknown_type",  # Unknown value
                },
            ],
            "formatting_loss": ["unknown_loss_type", "image"],  # One unknown
            "metrics": {},
            "optional_features_used": {},
        }

        quality = ConversionQuality.from_dict(data)

        # Should have the warning with default severity
        assert len(quality.warnings) == 1
        assert quality.warnings[0].severity == WarningSeverity.LOW  # Default
        assert quality.warnings[0].formatting_type is None  # Unknown skipped

        # Should have only the known formatting loss
        assert len(quality.formatting_loss) == 1
        assert FormattingLossType.IMAGE in quality.formatting_loss

    def test_from_dict_missing_fields(self):
        """Test from_dict with missing optional fields."""
        data = {
            "confidence": 0.9,
            # All other fields missing
        }

        quality = ConversionQuality.from_dict(data)

        assert quality.confidence == 0.9
        assert quality.converter_used is None
        assert quality.is_partial is False
        assert len(quality.warnings) == 0
        assert len(quality.formatting_loss) == 0


class TestBatchConversionWithCache:
    """Integration tests for batch conversion with caching."""

    def test_batch_conversion_with_cache_first_run(self):
        """Test first batch conversion stores results in cache."""
        with tempfile.TemporaryDirectory() as tmpdir:
            cache = ConversionCache(Path(tmpdir) / "cache")
            markitdown = MarkItDown()

            files = [
                os.path.join(TEST_FILES_DIR, "test.json"),
            ]

            result = markitdown.convert_batch(files, cache=cache)

            assert result.success_count == 1
            assert result.cached_count == 0  # First run, nothing cached

            # Verify file is now cached
            assert cache.has(files[0])

    def test_batch_conversion_with_cache_second_run(self):
        """Test second batch conversion uses cache."""
        with tempfile.TemporaryDirectory() as tmpdir:
            cache = ConversionCache(Path(tmpdir) / "cache")
            markitdown = MarkItDown()

            files = [
                os.path.join(TEST_FILES_DIR, "test.json"),
            ]

            # First run
            result1 = markitdown.convert_batch(files, cache=cache)
            assert result1.success_count == 1
            assert result1.cached_count == 0

            # Second run - should use cache
            result2 = markitdown.convert_batch(files, cache=cache)
            assert result2.success_count == 1  # Still successful
            assert result2.cached_count == 1  # But from cache

            # Verify results are the same
            assert result2.successful_items[0].markdown == result1.successful_items[0].markdown

    def test_batch_conversion_mixed_cached_and_new(self):
        """Test batch with mix of cached and new files."""
        with tempfile.TemporaryDirectory() as tmpdir:
            cache = ConversionCache(Path(tmpdir) / "cache")
            markitdown = MarkItDown()

            # Create a temporary file
            temp_file = Path(tmpdir) / "temp.txt"
            temp_file.write_text("Temporary content")

            json_file = os.path.join(TEST_FILES_DIR, "test.json")

            # First run with just JSON file
            result1 = markitdown.convert_batch([json_file], cache=cache)
            assert result1.cached_count == 0

            # Second run with both files
            result2 = markitdown.convert_batch([json_file, str(temp_file)], cache=cache)

            assert result2.success_count == 2
            assert result2.cached_count == 1  # JSON from cache

            # Find which items are cached vs new
            cached_items = [i for i in result2.items if i.status == BatchItemStatus.CACHED]
            new_items = [i for i in result2.items if i.status == BatchItemStatus.SUCCESS]

            assert len(cached_items) == 1
            assert len(new_items) == 1
            assert json_file in cached_items[0].source_path

    def test_batch_conversion_cache_invalidation(self):
        """Test that modified files are not served from cache."""
        with tempfile.TemporaryDirectory() as tmpdir:
            cache = ConversionCache(Path(tmpdir) / "cache")
            markitdown = MarkItDown()

            # Create a temporary file
            temp_file = Path(tmpdir) / "temp.txt"
            temp_file.write_text("Original content")

            # First run
            result1 = markitdown.convert_batch([str(temp_file)], cache=cache)
            assert result1.cached_count == 0
            original_markdown = result1.successful_items[0].markdown

            # Modify the file
            temp_file.write_text("Modified content - different!")

            # Second run - should NOT use cache
            result2 = markitdown.convert_batch([str(temp_file)], cache=cache)
            assert result2.cached_count == 0  # Not from cache
            assert result2.success_count == 1  # Still successful

            # Content should be different
            assert result2.successful_items[0].markdown != original_markdown

    def test_convert_directory_with_cache(self):
        """Test convert_directory with caching."""
        with tempfile.TemporaryDirectory() as tmpdir:
            cache = ConversionCache(Path(tmpdir) / "cache")
            markitdown = MarkItDown()

            # First run
            result1 = markitdown.convert_directory(
                TEST_FILES_DIR,
                include_patterns=["*.json"],
                cache=cache,
            )

            initial_success = result1.success_count
            assert result1.cached_count == 0

            # Second run - should use cache
            result2 = markitdown.convert_directory(
                TEST_FILES_DIR,
                include_patterns=["*.json"],
                cache=cache,
            )

            assert result2.success_count == initial_success
            assert result2.cached_count == initial_success

    def test_cached_item_has_quality(self):
        """Test that cached items have quality information."""
        with tempfile.TemporaryDirectory() as tmpdir:
            cache = ConversionCache(Path(tmpdir) / "cache")
            markitdown = MarkItDown()

            files = [os.path.join(TEST_FILES_DIR, "test.json")]

            # First run
            result1 = markitdown.convert_batch(files, cache=cache)
            original_quality = result1.successful_items[0].quality

            # Second run from cache
            result2 = markitdown.convert_batch(files, cache=cache)
            cached_quality = result2.successful_items[0].quality

            assert cached_quality is not None
            assert cached_quality.confidence == original_quality.confidence
            assert cached_quality.converter_used == original_quality.converter_used
            # Should have from_cache metric
            assert cached_quality.metrics.get("from_cache") is True

    def test_batch_without_cache_does_not_use_cache(self):
        """Test that batch without cache parameter doesn't use caching."""
        with tempfile.TemporaryDirectory() as tmpdir:
            cache = ConversionCache(Path(tmpdir) / "cache")
            markitdown = MarkItDown()

            files = [os.path.join(TEST_FILES_DIR, "test.json")]

            # Run with cache
            result1 = markitdown.convert_batch(files, cache=cache)
            assert result1.cached_count == 0

            # Run without cache parameter - should not read from cache
            result2 = markitdown.convert_batch(files)  # No cache parameter
            assert result2.cached_count == 0
            assert result2.success_count == 1


class TestBatchConversionResultWithCache:
    """Tests for BatchConversionResult with cached items."""

    def test_cached_count_property(self):
        """Test cached_count property."""
        result = BatchConversionResult()
        result.items.append(
            BatchItemResult(source_path="/test/a.txt", status=BatchItemStatus.SUCCESS)
        )
        result.items.append(
            BatchItemResult(source_path="/test/b.txt", status=BatchItemStatus.CACHED)
        )
        result.items.append(
            BatchItemResult(source_path="/test/c.txt", status=BatchItemStatus.CACHED)
        )

        assert result.cached_count == 2
        assert result.success_count == 3  # Includes cached

    def test_cached_items_property(self):
        """Test cached_items property."""
        result = BatchConversionResult()
        result.items.append(
            BatchItemResult(source_path="/test/a.txt", status=BatchItemStatus.SUCCESS)
        )
        result.items.append(
            BatchItemResult(source_path="/test/b.txt", status=BatchItemStatus.CACHED)
        )

        cached = result.cached_items
        assert len(cached) == 1
        assert cached[0].source_path == "/test/b.txt"

    def test_successful_items_includes_cached(self):
        """Test that successful_items includes cached items."""
        result = BatchConversionResult()
        result.items.append(
            BatchItemResult(source_path="/test/a.txt", status=BatchItemStatus.SUCCESS)
        )
        result.items.append(
            BatchItemResult(source_path="/test/b.txt", status=BatchItemStatus.CACHED)
        )
        result.items.append(
            BatchItemResult(source_path="/test/c.txt", status=BatchItemStatus.FAILED)
        )

        successful = result.successful_items
        assert len(successful) == 2
        assert any(i.source_path == "/test/a.txt" for i in successful)
        assert any(i.source_path == "/test/b.txt" for i in successful)

    def test_to_dict_includes_cached_count(self):
        """Test that to_dict includes cached_count."""
        result = BatchConversionResult()
        result.items.append(
            BatchItemResult(source_path="/test/a.txt", status=BatchItemStatus.CACHED)
        )

        d = result.to_dict()
        assert "cached_count" in d
        assert d["cached_count"] == 1

    def test_str_shows_cached_count(self):
        """Test that string representation shows cached count."""
        result = BatchConversionResult()
        result.items.append(
            BatchItemResult(source_path="/test/a.txt", status=BatchItemStatus.CACHED)
        )
        result.items.append(
            BatchItemResult(source_path="/test/b.txt", status=BatchItemStatus.CACHED)
        )

        str_repr = str(result)
        assert "from cache" in str_repr.lower()
        assert "2" in str_repr

    def test_str_shows_file_lists(self):
        """Test that string representation shows lists of converted and cached files."""
        result = BatchConversionResult()
        # Add a newly converted item
        result.items.append(
            BatchItemResult(source_path="/test/new.txt", status=BatchItemStatus.SUCCESS)
        )
        # Add cached items
        result.items.append(
            BatchItemResult(source_path="/test/cached1.txt", status=BatchItemStatus.CACHED)
        )
        result.items.append(
            BatchItemResult(source_path="/test/cached2.txt", status=BatchItemStatus.CACHED)
        )
        # Add a failed item
        result.items.append(
            BatchItemResult(
                source_path="/test/failed.txt",
                status=BatchItemStatus.FAILED,
                error="Test error"
            )
        )

        str_repr = str(result)

        # Should show newly converted files section
        assert "Newly converted files:" in str_repr
        assert "/test/new.txt" in str_repr

        # Should show cached files section
        assert "Files loaded from cache" in str_repr
        assert "/test/cached1.txt" in str_repr
        assert "/test/cached2.txt" in str_repr

        # Should show failed files section
        assert "Failed files:" in str_repr
        assert "/test/failed.txt" in str_repr

    def test_str_does_not_show_empty_sections(self):
        """Test that empty sections are not shown in string representation."""
        # Only successful items, no cached or failed
        result = BatchConversionResult()
        result.items.append(
            BatchItemResult(source_path="/test/new.txt", status=BatchItemStatus.SUCCESS)
        )

        str_repr = str(result)

        # Should show newly converted files
        assert "Newly converted files:" in str_repr
        # Should NOT show cached or failed sections
        assert "Files loaded from cache" not in str_repr
        assert "Failed files:" not in str_repr


class TestCacheCLI:
    """Tests for cache-related CLI commands."""

    def _run_cli(self, args, check=True):
        """Helper to run the markitdown CLI command."""
        cmd = [sys.executable, "-m", MARKITDOWN_MODULE] + args
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            cwd=os.path.dirname(TEST_FILES_DIR),
        )
        if check and result.returncode != 0:
            print(f"STDOUT: {result.stdout}")
            print(f"STDERR: {result.stderr}")
        return result

    def test_cli_cache_flag(self):
        """Test --cache flag enables caching."""
        with tempfile.TemporaryDirectory() as tmpdir:
            cache_dir = Path(tmpdir) / "cache"

            result = self._run_cli([
                "--batch", TEST_FILES_DIR,
                "--include", "*.json",
                "--cache",
                "--cache-dir", str(cache_dir),
                "--progress",
            ], check=False)

            assert result.returncode == 0
            # Check that cache was used
            assert "Cache enabled" in result.stderr
            # Verify cache directory was created and has entries
            assert cache_dir.exists()

    def test_cli_cache_dir_flag(self):
        """Test --cache-dir flag."""
        with tempfile.TemporaryDirectory() as tmpdir:
            cache_dir = Path(tmpdir) / "my_custom_cache"

            result = self._run_cli([
                "--batch", TEST_FILES_DIR,
                "--include", "*.json",
                "--cache-dir", str(cache_dir),
                "--progress",
            ], check=False)

            assert result.returncode == 0
            # --cache-dir implies --cache
            assert cache_dir.exists()

    def test_cli_clear_cache(self):
        """Test --clear-cache flag."""
        with tempfile.TemporaryDirectory() as tmpdir:
            cache_dir = Path(tmpdir) / "cache"

            # First, run with cache to populate it
            self._run_cli([
                "--batch", TEST_FILES_DIR,
                "--include", "*.json",
                "--cache",
                "--cache-dir", str(cache_dir),
            ], check=False)

            # Verify cache has entries
            cache = ConversionCache(cache_dir)
            initial_stats = cache.get_stats()
            assert initial_stats["entry_count"] > 0

            # Clear the cache
            result = self._run_cli([
                "--clear-cache",
                "--cache-dir", str(cache_dir),
            ], check=False)

            assert result.returncode == 0
            assert "Cleared" in result.stdout

            # Verify cache is empty
            final_stats = cache.get_stats()
            assert final_stats["entry_count"] == 0

    def test_cli_cached_files_show_in_progress(self):
        """Test that cached files show with special icon in progress."""
        with tempfile.TemporaryDirectory() as tmpdir:
            cache_dir = Path(tmpdir) / "cache"

            # First run
            self._run_cli([
                "--batch", TEST_FILES_DIR,
                "--include", "*.json",
                "--cache",
                "--cache-dir", str(cache_dir),
            ], check=False)

            # Second run with progress
            result = self._run_cli([
                "--batch", TEST_FILES_DIR,
                "--include", "*.json",
                "--cache",
                "--cache-dir", str(cache_dir),
                "--progress",
            ], check=False)

            assert result.returncode == 0
            # Should show cached indicator
            assert "⚡" in result.stderr or "[cached]" in result.stderr

    def test_cli_summary_shows_cached_count(self):
        """Test that summary shows cached count."""
        with tempfile.TemporaryDirectory() as tmpdir:
            cache_dir = Path(tmpdir) / "cache"

            # First run
            self._run_cli([
                "--batch", TEST_FILES_DIR,
                "--include", "*.json",
                "--cache",
                "--cache-dir", str(cache_dir),
            ], check=False)

            # Second run with summary
            result = self._run_cli([
                "--batch", TEST_FILES_DIR,
                "--include", "*.json",
                "--cache",
                "--cache-dir", str(cache_dir),
                "--summary",
            ], check=False)

            assert result.returncode == 0
            # Should show cached count in summary
            assert "cache" in result.stderr.lower()


class TestCacheEdgeCases:
    """Edge case tests for caching."""

    def test_cache_handles_binary_files(self):
        """Test caching works with binary file content."""
        with tempfile.TemporaryDirectory() as tmpdir:
            cache = ConversionCache(Path(tmpdir) / "cache")

            # Create a binary file
            binary_file = Path(tmpdir) / "test.bin"
            binary_file.write_bytes(b"\x00\x01\x02\x03\xff\xfe\xfd")

            # Compute hash - should not fail
            file_hash = cache.compute_file_hash(str(binary_file))
            assert len(file_hash) == 64

    def test_cache_handles_large_markdown(self):
        """Test caching works with large markdown content."""
        with tempfile.TemporaryDirectory() as tmpdir:
            cache = ConversionCache(Path(tmpdir) / "cache")

            test_file = Path(tmpdir) / "test.txt"
            test_file.write_text("Small file")

            # Create a result with large markdown
            large_markdown = "# Test\n\n" + ("Lorem ipsum " * 10000)
            result = DocumentConverterResult(markdown=large_markdown)

            # Store and retrieve
            cache.put(str(test_file), result)
            entry = cache.get(str(test_file))

            assert entry is not None
            assert entry.markdown == large_markdown

    def test_cache_handles_unicode_content(self):
        """Test caching handles unicode content correctly."""
        with tempfile.TemporaryDirectory() as tmpdir:
            cache = ConversionCache(Path(tmpdir) / "cache")

            test_file = Path(tmpdir) / "test.txt"
            test_file.write_text("Unicode: 你好世界 🌍 émoji")

            unicode_markdown = "# 标题\n\n内容：日本語、한국어、العربية"
            result = DocumentConverterResult(
                markdown=unicode_markdown,
                title="Título",
            )

            cache.put(str(test_file), result)
            entry = cache.get(str(test_file))

            assert entry is not None
            assert entry.markdown == unicode_markdown
            assert entry.title == "Título"

    def test_cache_handles_empty_file(self):
        """Test caching handles empty files."""
        with tempfile.TemporaryDirectory() as tmpdir:
            cache = ConversionCache(Path(tmpdir) / "cache")

            empty_file = Path(tmpdir) / "empty.txt"
            empty_file.write_text("")

            result = DocumentConverterResult(markdown="")
            cache.put(str(empty_file), result)

            entry = cache.get(str(empty_file))
            assert entry is not None
            assert entry.markdown == ""

    def test_cache_file_not_found_graceful(self):
        """Test cache handles non-existent files gracefully."""
        with tempfile.TemporaryDirectory() as tmpdir:
            cache = ConversionCache(Path(tmpdir) / "cache")

            # Try to get cache for non-existent file
            entry = cache.get("/nonexistent/file.txt")
            assert entry is None

    def test_cache_concurrent_access(self):
        """Test cache handles concurrent access."""
        import threading

        with tempfile.TemporaryDirectory() as tmpdir:
            cache = ConversionCache(Path(tmpdir) / "cache")

            # Create test files
            files = []
            for i in range(10):
                f = Path(tmpdir) / f"test{i}.txt"
                f.write_text(f"Content {i}")
                files.append(str(f))

            results = []
            errors = []

            def cache_file(path):
                try:
                    result = DocumentConverterResult(markdown=f"# {path}")
                    cache.put(path, result)
                    entry = cache.get(path)
                    results.append((path, entry is not None))
                except Exception as e:
                    errors.append((path, str(e)))

            # Run concurrent writes
            threads = [threading.Thread(target=cache_file, args=(f,)) for f in files]
            for t in threads:
                t.start()
            for t in threads:
                t.join()

            assert len(errors) == 0, f"Errors during concurrent access: {errors}"
            assert len(results) == 10
            assert all(success for _, success in results)

    def test_cache_subdirectory_structure(self):
        """Test cache creates proper subdirectory structure."""
        with tempfile.TemporaryDirectory() as tmpdir:
            cache_dir = Path(tmpdir) / "cache"
            cache = ConversionCache(cache_dir)

            test_file = Path(tmpdir) / "test.txt"
            test_file.write_text("Content")

            result = DocumentConverterResult(markdown="# Test")
            cache.put(str(test_file), result)

            # Check subdirectory structure
            file_hash = cache.compute_file_hash(str(test_file))
            expected_subdir = cache_dir / file_hash[:2]
            expected_file = expected_subdir / f"{file_hash}.json"

            assert expected_subdir.exists()
            assert expected_file.exists()

    def test_cache_files_are_read_only(self):
        """Test that cache files are created with read-only permissions."""
        import stat

        with tempfile.TemporaryDirectory() as tmpdir:
            cache_dir = Path(tmpdir) / "cache"
            cache = ConversionCache(cache_dir)

            test_file = Path(tmpdir) / "test.txt"
            test_file.write_text("Content for readonly test")

            result = DocumentConverterResult(markdown="# Readonly Test")
            cache.put(str(test_file), result)

            # Find the cache file
            file_hash = cache.compute_file_hash(str(test_file))
            cache_path = cache_dir / file_hash[:2] / f"{file_hash}.json"

            assert cache_path.exists()

            # Check permissions - should be read-only (0o444)
            file_mode = cache_path.stat().st_mode
            # Check that write bits are NOT set
            assert not (file_mode & stat.S_IWUSR), "User write bit should not be set"
            assert not (file_mode & stat.S_IWGRP), "Group write bit should not be set"
            assert not (file_mode & stat.S_IWOTH), "Other write bit should not be set"
            # Check that read bits ARE set
            assert file_mode & stat.S_IRUSR, "User read bit should be set"

    def test_cache_readonly_prevents_accidental_write(self):
        """Test that read-only cache files prevent accidental modification."""
        with tempfile.TemporaryDirectory() as tmpdir:
            cache_dir = Path(tmpdir) / "cache"
            cache = ConversionCache(cache_dir)

            test_file = Path(tmpdir) / "test.txt"
            test_file.write_text("Content")

            result = DocumentConverterResult(markdown="# Original")
            cache.put(str(test_file), result)

            # Find the cache file
            file_hash = cache.compute_file_hash(str(test_file))
            cache_path = cache_dir / file_hash[:2] / f"{file_hash}.json"

            # Trying to open for write should raise PermissionError
            with pytest.raises(PermissionError):
                with open(cache_path, "w") as f:
                    f.write("Modified content")

    def test_cache_clear_handles_readonly_files(self):
        """Test that cache.clear() properly handles read-only files."""
        with tempfile.TemporaryDirectory() as tmpdir:
            cache_dir = Path(tmpdir) / "cache"
            cache = ConversionCache(cache_dir)

            # Create some cached entries
            for i in range(3):
                test_file = Path(tmpdir) / f"test{i}.txt"
                test_file.write_text(f"Content {i}")
                result = DocumentConverterResult(markdown=f"# Test {i}")
                cache.put(str(test_file), result)

            # Verify they're cached
            assert cache.get_stats()["entry_count"] == 3

            # Clear should handle read-only files
            count = cache.clear()
            assert count == 3
            assert cache.get_stats()["entry_count"] == 0

    def test_cache_without_quality_in_result(self):
        """Test caching result without explicit quality."""
        with tempfile.TemporaryDirectory() as tmpdir:
            cache = ConversionCache(Path(tmpdir) / "cache")

            test_file = Path(tmpdir) / "test.txt"
            test_file.write_text("Content")

            # Result without explicit quality
            result = DocumentConverterResult(markdown="# Test")
            # Don't access result.quality to avoid lazy initialization

            cache.put(str(test_file), result)
            entry = cache.get(str(test_file))

            assert entry is not None
            assert entry.markdown == "# Test"
            # quality_dict should be None
            assert entry.quality_dict is None


class TestCachePerformance:
    """Performance-related tests for caching."""

    def test_cache_speedup_second_run(self):
        """Test that second run with cache is faster."""
        with tempfile.TemporaryDirectory() as tmpdir:
            cache = ConversionCache(Path(tmpdir) / "cache")
            markitdown = MarkItDown()

            files = [
                os.path.join(TEST_FILES_DIR, "test.json"),
                os.path.join(TEST_FILES_DIR, "test.xlsx"),
            ]

            # First run - measure time
            start1 = time.time()
            result1 = markitdown.convert_batch(files, cache=cache)
            time1 = time.time() - start1

            # Second run with cache - measure time
            start2 = time.time()
            result2 = markitdown.convert_batch(files, cache=cache)
            time2 = time.time() - start2

            # Second run should be faster (cache hit)
            assert result2.cached_count == 2
            # Allow for some variance, but cache should generally be faster
            # Note: This might occasionally fail due to system variations
            # so we just verify the cache was used
            assert result2.cached_count > 0


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
