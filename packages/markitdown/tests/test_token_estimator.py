#!/usr/bin/env python3 -m pytest
"""Tests for the token estimation feature in batch conversions."""

import os
import subprocess
import sys
import tempfile
import json
import pytest
from pathlib import Path
from unittest.mock import MagicMock, patch

from markitdown import (
    ConversionCache,
    estimate_batch_tokens,
    estimate_file_tokens,
    BatchTokenEstimate,
    FileTokenEstimate,
    FileCategory,
)
from markitdown._token_estimator import (
    _estimate_image_tokens,
    _estimate_pptx_image_count,
    IMAGE_EXTENSIONS,
    PPTX_EXTENSIONS,
    DEFAULT_PROMPT_TOKENS,
    DEFAULT_OUTPUT_TOKENS,
    BASE_IMAGE_TOKENS,
    TOKENS_PER_TILE,
)

TEST_FILES_DIR = os.path.join(os.path.dirname(__file__), "test_files")
MARKITDOWN_MODULE = "markitdown"


class TestFileCategory:
    """Tests for FileCategory enum."""

    def test_file_category_values(self):
        """Test FileCategory enum has expected values."""
        assert FileCategory.IMAGE.value == "image"
        assert FileCategory.PPTX.value == "pptx"
        assert FileCategory.NO_LLM.value == "no_llm"
        assert FileCategory.CACHED.value == "cached"
        assert FileCategory.RESUMED.value == "resumed"


class TestFileTokenEstimate:
    """Tests for FileTokenEstimate dataclass."""

    def test_file_token_estimate_creation(self):
        """Test creating a FileTokenEstimate."""
        estimate = FileTokenEstimate(
            source_path="/test/image.jpg",
            category=FileCategory.IMAGE,
            input_tokens=500,
            output_tokens=150,
            image_count=1,
            file_size_bytes=1024,
        )

        assert estimate.source_path == "/test/image.jpg"
        assert estimate.category == FileCategory.IMAGE
        assert estimate.input_tokens == 500
        assert estimate.output_tokens == 150
        assert estimate.image_count == 1
        assert estimate.file_size_bytes == 1024

    def test_file_token_estimate_total_tokens(self):
        """Test total_tokens property."""
        estimate = FileTokenEstimate(
            source_path="/test/image.jpg",
            category=FileCategory.IMAGE,
            input_tokens=500,
            output_tokens=150,
        )

        assert estimate.total_tokens == 650

    def test_file_token_estimate_with_skip_reason(self):
        """Test FileTokenEstimate with skip_reason."""
        estimate = FileTokenEstimate(
            source_path="/test/doc.pdf",
            category=FileCategory.NO_LLM,
            skip_reason="File type does not use LLM",
        )

        assert estimate.skip_reason == "File type does not use LLM"
        assert estimate.total_tokens == 0

    def test_file_token_estimate_to_dict(self):
        """Test FileTokenEstimate serialization."""
        estimate = FileTokenEstimate(
            source_path="/test/image.jpg",
            category=FileCategory.IMAGE,
            input_tokens=500,
            output_tokens=150,
            image_count=1,
            file_size_bytes=1024,
        )

        d = estimate.to_dict()

        assert d["source_path"] == "/test/image.jpg"
        assert d["category"] == "image"
        assert d["input_tokens"] == 500
        assert d["output_tokens"] == 150
        assert d["total_tokens"] == 650
        assert d["image_count"] == 1
        assert d["file_size_bytes"] == 1024
        assert "skip_reason" not in d  # Not included when None

    def test_file_token_estimate_to_dict_with_skip_reason(self):
        """Test FileTokenEstimate serialization with skip_reason."""
        estimate = FileTokenEstimate(
            source_path="/test/doc.pdf",
            category=FileCategory.NO_LLM,
            skip_reason="File type does not use LLM",
        )

        d = estimate.to_dict()

        assert d["skip_reason"] == "File type does not use LLM"


class TestBatchTokenEstimate:
    """Tests for BatchTokenEstimate dataclass."""

    def test_batch_token_estimate_empty(self):
        """Test empty BatchTokenEstimate."""
        batch = BatchTokenEstimate()

        assert len(batch.files) == 0
        assert batch.total_input_tokens == 0
        assert batch.total_output_tokens == 0
        assert batch.total_tokens == 0
        assert batch.total_image_count == 0

    def test_batch_token_estimate_with_files(self):
        """Test BatchTokenEstimate with multiple files."""
        batch = BatchTokenEstimate()
        batch.files.append(FileTokenEstimate(
            source_path="/test/image1.jpg",
            category=FileCategory.IMAGE,
            input_tokens=500,
            output_tokens=150,
            image_count=1,
        ))
        batch.files.append(FileTokenEstimate(
            source_path="/test/image2.png",
            category=FileCategory.IMAGE,
            input_tokens=600,
            output_tokens=150,
            image_count=1,
        ))
        batch.files.append(FileTokenEstimate(
            source_path="/test/doc.pdf",
            category=FileCategory.NO_LLM,
        ))

        assert batch.total_input_tokens == 1100
        assert batch.total_output_tokens == 300
        assert batch.total_tokens == 1400
        assert batch.total_image_count == 2

    def test_batch_token_estimate_files_using_llm(self):
        """Test files_using_llm property."""
        batch = BatchTokenEstimate()
        batch.files.append(FileTokenEstimate(
            source_path="/test/image.jpg",
            category=FileCategory.IMAGE,
            input_tokens=500,
            output_tokens=150,
        ))
        batch.files.append(FileTokenEstimate(
            source_path="/test/doc.pdf",
            category=FileCategory.NO_LLM,
        ))

        llm_files = batch.files_using_llm
        assert len(llm_files) == 1
        assert llm_files[0].source_path == "/test/image.jpg"

    def test_batch_token_estimate_files_skipped(self):
        """Test files_skipped property."""
        batch = BatchTokenEstimate()
        batch.files.append(FileTokenEstimate(
            source_path="/test/image.jpg",
            category=FileCategory.IMAGE,
            input_tokens=500,
            output_tokens=150,
        ))
        batch.files.append(FileTokenEstimate(
            source_path="/test/doc.pdf",
            category=FileCategory.NO_LLM,
        ))
        batch.files.append(FileTokenEstimate(
            source_path="/test/cached.jpg",
            category=FileCategory.CACHED,
        ))

        skipped = batch.files_skipped
        assert len(skipped) == 2

    def test_batch_token_estimate_cached_files(self):
        """Test cached_files property."""
        batch = BatchTokenEstimate()
        batch.files.append(FileTokenEstimate(
            source_path="/test/cached.jpg",
            category=FileCategory.CACHED,
        ))
        batch.files.append(FileTokenEstimate(
            source_path="/test/image.jpg",
            category=FileCategory.IMAGE,
            input_tokens=500,
            output_tokens=150,
        ))

        cached = batch.cached_files
        assert len(cached) == 1
        assert cached[0].source_path == "/test/cached.jpg"

    def test_batch_token_estimate_resumed_files(self):
        """Test resumed_files property."""
        batch = BatchTokenEstimate()
        batch.files.append(FileTokenEstimate(
            source_path="/test/resumed.jpg",
            category=FileCategory.RESUMED,
        ))
        batch.files.append(FileTokenEstimate(
            source_path="/test/image.jpg",
            category=FileCategory.IMAGE,
            input_tokens=500,
            output_tokens=150,
        ))

        resumed = batch.resumed_files
        assert len(resumed) == 1
        assert resumed[0].source_path == "/test/resumed.jpg"

    def test_batch_token_estimate_to_dict(self):
        """Test BatchTokenEstimate serialization."""
        batch = BatchTokenEstimate()
        batch.files.append(FileTokenEstimate(
            source_path="/test/image.jpg",
            category=FileCategory.IMAGE,
            input_tokens=500,
            output_tokens=150,
            image_count=1,
        ))

        d = batch.to_dict()

        assert "summary" in d
        assert "files" in d
        assert d["summary"]["total_files"] == 1
        assert d["summary"]["files_using_llm"] == 1
        assert d["summary"]["total_tokens"] == 650
        assert len(d["files"]) == 1

    def test_batch_token_estimate_str(self):
        """Test BatchTokenEstimate string representation."""
        batch = BatchTokenEstimate()
        batch.files.append(FileTokenEstimate(
            source_path="/test/image.jpg",
            category=FileCategory.IMAGE,
            input_tokens=500,
            output_tokens=150,
            image_count=1,
        ))

        str_repr = str(batch)

        assert "TOKEN ESTIMATION SUMMARY" in str_repr
        assert "Total files: 1" in str_repr
        assert "Files using LLM: 1" in str_repr
        assert "TOTAL TOKENS:" in str_repr


class TestEstimateImageTokens:
    """Tests for _estimate_image_tokens function."""

    def test_estimate_small_image(self):
        """Test token estimation for small image."""
        with tempfile.NamedTemporaryFile(suffix=".jpg", delete=False) as f:
            # Write ~10KB of data (small image)
            f.write(b"x" * 10000)
            temp_path = f.name

        try:
            tokens = _estimate_image_tokens(temp_path)
            # Should have base tokens plus some tiles
            assert tokens >= BASE_IMAGE_TOKENS
            assert tokens < BASE_IMAGE_TOKENS + TOKENS_PER_TILE * 20
        finally:
            os.unlink(temp_path)

    def test_estimate_medium_image(self):
        """Test token estimation for medium image."""
        with tempfile.NamedTemporaryFile(suffix=".jpg", delete=False) as f:
            # Write ~500KB of data (medium image)
            f.write(b"x" * 500000)
            temp_path = f.name

        try:
            tokens = _estimate_image_tokens(temp_path)
            # Should have more tokens than small image
            assert tokens >= BASE_IMAGE_TOKENS + TOKENS_PER_TILE
        finally:
            os.unlink(temp_path)

    def test_estimate_large_image(self):
        """Test token estimation for large image."""
        with tempfile.NamedTemporaryFile(suffix=".jpg", delete=False) as f:
            # Write ~5MB of data (large image)
            f.write(b"x" * 5000000)
            temp_path = f.name

        try:
            tokens = _estimate_image_tokens(temp_path)
            # Should have significant tokens for large image
            assert tokens >= BASE_IMAGE_TOKENS + TOKENS_PER_TILE * 4
        finally:
            os.unlink(temp_path)

    def test_estimate_nonexistent_file(self):
        """Test token estimation for nonexistent file returns default."""
        tokens = _estimate_image_tokens("/nonexistent/file.jpg")
        # Should return default estimate
        assert tokens == BASE_IMAGE_TOKENS + TOKENS_PER_TILE * 4


class TestEstimatePptxImageCount:
    """Tests for _estimate_pptx_image_count function."""

    def test_estimate_tiny_pptx(self):
        """Test estimation for tiny PPTX (<0.5MB) - likely no images."""
        with tempfile.NamedTemporaryFile(suffix=".pptx", delete=False) as f:
            # Write ~100KB
            f.write(b"x" * 100000)
            temp_path = f.name

        try:
            count = _estimate_pptx_image_count(temp_path)
            assert count == 0
        finally:
            os.unlink(temp_path)

    def test_estimate_small_pptx(self):
        """Test estimation for small PPTX (0.5-1MB)."""
        with tempfile.NamedTemporaryFile(suffix=".pptx", delete=False) as f:
            # Write ~750KB
            f.write(b"x" * 750000)
            temp_path = f.name

        try:
            count = _estimate_pptx_image_count(temp_path)
            assert count == 1
        finally:
            os.unlink(temp_path)

    def test_estimate_medium_pptx(self):
        """Test estimation for medium PPTX (1-2MB)."""
        with tempfile.NamedTemporaryFile(suffix=".pptx", delete=False) as f:
            # Write ~1.5MB
            f.write(b"x" * 1500000)
            temp_path = f.name

        try:
            count = _estimate_pptx_image_count(temp_path)
            # Should be 1-3 images
            assert 1 <= count <= 4
        finally:
            os.unlink(temp_path)

    def test_estimate_large_pptx(self):
        """Test estimation for large PPTX (10-50MB)."""
        with tempfile.NamedTemporaryFile(suffix=".pptx", delete=False) as f:
            # Write ~20MB
            f.write(b"x" * 20000000)
            temp_path = f.name

        try:
            count = _estimate_pptx_image_count(temp_path)
            # Should use logarithmic scaling, not linear
            # Old formula would give 40+ images, new formula gives ~15-18
            assert count < 30
            assert count >= 8
        finally:
            os.unlink(temp_path)

    def test_estimate_very_large_pptx(self):
        """Test estimation for very large PPTX (>50MB) is capped."""
        with tempfile.NamedTemporaryFile(suffix=".pptx", delete=False) as f:
            # Write ~60MB
            f.write(b"x" * 60000000)
            temp_path = f.name

        try:
            count = _estimate_pptx_image_count(temp_path)
            # Should be capped at ~25 images max
            # Old formula would give 90 images, new formula caps at ~20-25
            assert count <= 25
            assert count >= 15
        finally:
            os.unlink(temp_path)

    def test_estimate_huge_pptx_is_reasonable(self):
        """Test that huge PPTX files don't overestimate."""
        with tempfile.NamedTemporaryFile(suffix=".pptx", delete=False) as f:
            # Write ~100MB (might have embedded videos)
            f.write(b"x" * 100000000)
            temp_path = f.name

        try:
            count = _estimate_pptx_image_count(temp_path)
            # Should not estimate more than 25 images for very large files
            # because large files often contain video/audio, not more images
            assert count <= 25
        finally:
            os.unlink(temp_path)

    def test_estimate_nonexistent_pptx(self):
        """Test estimation for nonexistent file returns 0."""
        count = _estimate_pptx_image_count("/nonexistent/file.pptx")
        assert count == 0


class TestEstimateFileTokens:
    """Tests for estimate_file_tokens function."""

    def test_estimate_jpg_file(self):
        """Test estimation for JPG file."""
        with tempfile.NamedTemporaryFile(suffix=".jpg", delete=False) as f:
            f.write(b"x" * 100000)  # 100KB
            temp_path = f.name

        try:
            estimate = estimate_file_tokens(temp_path)

            assert estimate.category == FileCategory.IMAGE
            assert estimate.input_tokens > 0
            assert estimate.output_tokens == DEFAULT_OUTPUT_TOKENS
            assert estimate.image_count == 1
            assert estimate.file_size_bytes == 100000
        finally:
            os.unlink(temp_path)

    def test_estimate_png_file(self):
        """Test estimation for PNG file."""
        with tempfile.NamedTemporaryFile(suffix=".png", delete=False) as f:
            f.write(b"x" * 100000)  # 100KB
            temp_path = f.name

        try:
            estimate = estimate_file_tokens(temp_path)

            assert estimate.category == FileCategory.IMAGE
            assert estimate.image_count == 1
        finally:
            os.unlink(temp_path)

    def test_estimate_jpeg_file(self):
        """Test estimation for JPEG file (alternative extension)."""
        with tempfile.NamedTemporaryFile(suffix=".jpeg", delete=False) as f:
            f.write(b"x" * 100000)  # 100KB
            temp_path = f.name

        try:
            estimate = estimate_file_tokens(temp_path)

            assert estimate.category == FileCategory.IMAGE
        finally:
            os.unlink(temp_path)

    def test_estimate_pptx_file(self):
        """Test estimation for PPTX file."""
        with tempfile.NamedTemporaryFile(suffix=".pptx", delete=False) as f:
            f.write(b"x" * 2000000)  # 2MB
            temp_path = f.name

        try:
            estimate = estimate_file_tokens(temp_path)

            assert estimate.category == FileCategory.PPTX
            assert estimate.input_tokens > 0
            assert estimate.output_tokens > 0
            assert estimate.image_count > 0
        finally:
            os.unlink(temp_path)

    def test_estimate_small_pptx_no_images(self):
        """Test estimation for small PPTX with no estimated images."""
        with tempfile.NamedTemporaryFile(suffix=".pptx", delete=False) as f:
            f.write(b"x" * 100000)  # 100KB - very small
            temp_path = f.name

        try:
            estimate = estimate_file_tokens(temp_path)

            assert estimate.category == FileCategory.NO_LLM
            assert estimate.input_tokens == 0
            assert estimate.output_tokens == 0
            assert estimate.image_count == 0
            assert "no estimated images" in estimate.skip_reason.lower()
        finally:
            os.unlink(temp_path)

    def test_estimate_pdf_file(self):
        """Test estimation for PDF file (no LLM)."""
        with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as f:
            f.write(b"x" * 100000)
            temp_path = f.name

        try:
            estimate = estimate_file_tokens(temp_path)

            assert estimate.category == FileCategory.NO_LLM
            assert estimate.input_tokens == 0
            assert estimate.output_tokens == 0
            assert estimate.skip_reason == "File type does not use LLM"
        finally:
            os.unlink(temp_path)

    def test_estimate_docx_file(self):
        """Test estimation for DOCX file (no LLM)."""
        with tempfile.NamedTemporaryFile(suffix=".docx", delete=False) as f:
            f.write(b"x" * 100000)
            temp_path = f.name

        try:
            estimate = estimate_file_tokens(temp_path)

            assert estimate.category == FileCategory.NO_LLM
        finally:
            os.unlink(temp_path)

    def test_estimate_resumed_file(self):
        """Test estimation for resumed file."""
        with tempfile.NamedTemporaryFile(suffix=".jpg", delete=False) as f:
            f.write(b"x" * 100000)
            temp_path = f.name

        try:
            estimate = estimate_file_tokens(temp_path, is_resumed=True)

            assert estimate.category == FileCategory.RESUMED
            assert estimate.input_tokens == 0
            assert estimate.output_tokens == 0
            assert estimate.skip_reason == "Output file already exists"
        finally:
            os.unlink(temp_path)

    def test_estimate_cached_file(self):
        """Test estimation for cached file."""
        with tempfile.TemporaryDirectory() as tmpdir:
            cache = ConversionCache(Path(tmpdir) / "cache")

            # Create and cache a test file
            test_file = Path(tmpdir) / "test.jpg"
            test_file.write_bytes(b"x" * 100000)

            # Manually add to cache
            from markitdown import DocumentConverterResult
            result = DocumentConverterResult(markdown="# Cached")
            cache.put(str(test_file), result)

            # Now estimate with cache
            estimate = estimate_file_tokens(str(test_file), cache=cache)

            assert estimate.category == FileCategory.CACHED
            assert estimate.input_tokens == 0
            assert estimate.output_tokens == 0
            assert estimate.skip_reason == "File is cached"


class TestEstimateBatchTokens:
    """Tests for estimate_batch_tokens function."""

    def test_estimate_batch_empty(self):
        """Test estimation for empty batch."""
        batch = estimate_batch_tokens([])

        assert len(batch.files) == 0
        assert batch.total_tokens == 0

    def test_estimate_batch_mixed_files(self):
        """Test estimation for batch with mixed file types."""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Create test files
            jpg_file = Path(tmpdir) / "image.jpg"
            jpg_file.write_bytes(b"x" * 100000)

            pdf_file = Path(tmpdir) / "doc.pdf"
            pdf_file.write_bytes(b"x" * 100000)

            pptx_file = Path(tmpdir) / "slides.pptx"
            pptx_file.write_bytes(b"x" * 2000000)  # 2MB - has images

            files = [str(jpg_file), str(pdf_file), str(pptx_file)]

            batch = estimate_batch_tokens(files)

            assert len(batch.files) == 3
            assert len(batch.files_using_llm) == 2  # jpg and pptx
            assert len(batch.files_skipped) == 1  # pdf
            assert batch.total_tokens > 0

    def test_estimate_batch_with_cache(self):
        """Test estimation for batch with cached files."""
        with tempfile.TemporaryDirectory() as tmpdir:
            cache = ConversionCache(Path(tmpdir) / "cache")

            # Create and cache a file
            cached_file = Path(tmpdir) / "cached.jpg"
            cached_file.write_bytes(b"x" * 100000)

            from markitdown import DocumentConverterResult
            result = DocumentConverterResult(markdown="# Cached")
            cache.put(str(cached_file), result)

            # Create an uncached file
            new_file = Path(tmpdir) / "new.jpg"
            new_file.write_bytes(b"x" * 100000)

            files = [str(cached_file), str(new_file)]

            batch = estimate_batch_tokens(files, cache=cache)

            assert len(batch.files) == 2
            assert len(batch.cached_files) == 1
            assert len(batch.files_using_llm) == 1

    def test_estimate_batch_with_resumed_files(self):
        """Test estimation for batch with resumed files."""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Create test files
            resumed_file = Path(tmpdir) / "resumed.jpg"
            resumed_file.write_bytes(b"x" * 100000)

            new_file = Path(tmpdir) / "new.jpg"
            new_file.write_bytes(b"x" * 100000)

            files = [str(resumed_file), str(new_file)]
            resumed_files = {str(resumed_file): Path(tmpdir) / "resumed.md"}

            batch = estimate_batch_tokens(files, resumed_files=resumed_files)

            assert len(batch.files) == 2
            assert len(batch.resumed_files) == 1
            assert len(batch.files_using_llm) == 1


class TestTokenEstimationIntegration:
    """Integration tests for token estimation."""

    def test_estimation_with_real_test_files(self):
        """Test estimation with actual test files from test_files directory."""
        # Find image files in test directory
        test_files = []
        for ext in IMAGE_EXTENSIONS:
            for f in Path(TEST_FILES_DIR).glob(f"*{ext}"):
                test_files.append(str(f))

        if test_files:
            batch = estimate_batch_tokens(test_files)
            assert batch.total_tokens > 0
            assert batch.total_image_count == len(test_files)


class TestTokenEstimationCLI:
    """Tests for token estimation CLI commands."""

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

    def test_cli_estimate_tokens_requires_batch(self):
        """Test --estimate-tokens requires --batch."""
        result = self._run_cli([
            "--estimate-tokens",
            "test_files/test.json",
        ], check=False)

        assert result.returncode != 0
        assert "batch" in result.stdout.lower() or "batch" in result.stderr.lower()

    def test_cli_estimate_tokens_basic(self):
        """Test basic --estimate-tokens usage."""
        result = self._run_cli([
            "--batch", TEST_FILES_DIR,
            "--include", "*.jpg",
            "--estimate-tokens",
        ], check=False)

        assert result.returncode == 0
        assert "TOKEN ESTIMATION SUMMARY" in result.stderr

    def test_cli_estimate_tokens_with_cache(self):
        """Test --estimate-tokens with --cache."""
        with tempfile.TemporaryDirectory() as tmpdir:
            cache_dir = Path(tmpdir) / "cache"

            result = self._run_cli([
                "--batch", TEST_FILES_DIR,
                "--include", "*.jpg",
                "--estimate-tokens",
                "--cache",
                "--cache-dir", str(cache_dir),
            ], check=False)

            assert result.returncode == 0
            assert "TOKEN ESTIMATION SUMMARY" in result.stderr

    def test_cli_estimate_tokens_export_manifest(self):
        """Test --estimate-tokens with --export-manifest."""
        with tempfile.TemporaryDirectory() as tmpdir:
            manifest_path = Path(tmpdir) / "tokens.json"

            result = self._run_cli([
                "--batch", TEST_FILES_DIR,
                "--include", "*.jpg",
                "--estimate-tokens",
                "--export-manifest", str(manifest_path),
            ], check=False)

            assert result.returncode == 0
            assert manifest_path.exists()

            # Verify manifest structure
            with open(manifest_path) as f:
                manifest = json.load(f)

            assert "summary" in manifest
            assert "files" in manifest
            assert "total_tokens" in manifest["summary"]

    def test_cli_estimate_tokens_no_conversion(self):
        """Test that --estimate-tokens doesn't perform actual conversion."""
        with tempfile.TemporaryDirectory() as tmpdir:
            output_dir = Path(tmpdir) / "output"

            result = self._run_cli([
                "--batch", TEST_FILES_DIR,
                "--include", "*.jpg",
                "--estimate-tokens",
                "-o", str(output_dir),
            ], check=False)

            assert result.returncode == 0
            # Output directory should NOT be created (no conversion happened)
            # Note: it might be created empty, so check for .md files
            md_files = list(output_dir.glob("*.md")) if output_dir.exists() else []
            assert len(md_files) == 0


class TestTokenEstimationEdgeCases:
    """Edge case tests for token estimation."""

    def test_estimate_empty_file(self):
        """Test estimation for empty file."""
        with tempfile.NamedTemporaryFile(suffix=".jpg", delete=False) as f:
            # Empty file
            temp_path = f.name

        try:
            estimate = estimate_file_tokens(temp_path)
            # Should still categorize as image
            assert estimate.category == FileCategory.IMAGE
            assert estimate.file_size_bytes == 0
        finally:
            os.unlink(temp_path)

    def test_estimate_case_insensitive_extension(self):
        """Test that extension matching is case insensitive."""
        with tempfile.NamedTemporaryFile(suffix=".JPG", delete=False) as f:
            f.write(b"x" * 100000)
            temp_path = f.name

        try:
            estimate = estimate_file_tokens(temp_path)
            assert estimate.category == FileCategory.IMAGE
        finally:
            os.unlink(temp_path)

    def test_estimate_uppercase_pptx(self):
        """Test estimation for uppercase PPTX extension."""
        with tempfile.NamedTemporaryFile(suffix=".PPTX", delete=False) as f:
            f.write(b"x" * 2000000)
            temp_path = f.name

        try:
            estimate = estimate_file_tokens(temp_path)
            assert estimate.category == FileCategory.PPTX
        finally:
            os.unlink(temp_path)

    def test_estimate_nonexistent_file(self):
        """Test estimation for nonexistent file."""
        estimate = estimate_file_tokens("/nonexistent/file.jpg")

        # Should handle gracefully
        assert estimate.category == FileCategory.IMAGE
        assert estimate.file_size_bytes == 0

    def test_estimate_special_characters_in_path(self):
        """Test estimation for path with special characters."""
        with tempfile.TemporaryDirectory() as tmpdir:
            special_path = Path(tmpdir) / "file with spaces & symbols!.jpg"
            special_path.write_bytes(b"x" * 100000)

            estimate = estimate_file_tokens(str(special_path))
            assert estimate.category == FileCategory.IMAGE

    def test_estimate_unicode_path(self):
        """Test estimation for path with unicode characters."""
        with tempfile.TemporaryDirectory() as tmpdir:
            unicode_path = Path(tmpdir) / "文件_файл_αρχείο.jpg"
            unicode_path.write_bytes(b"x" * 100000)

            estimate = estimate_file_tokens(str(unicode_path))
            assert estimate.category == FileCategory.IMAGE

    def test_batch_estimate_order_preserved(self):
        """Test that file order is preserved in batch estimation."""
        with tempfile.TemporaryDirectory() as tmpdir:
            files = []
            for i in range(5):
                f = Path(tmpdir) / f"file{i}.jpg"
                f.write_bytes(b"x" * (i + 1) * 10000)
                files.append(str(f))

            batch = estimate_batch_tokens(files)

            # Order should be preserved
            for i, estimate in enumerate(batch.files):
                assert f"file{i}.jpg" in estimate.source_path


class TestTokenEstimationDocumentation:
    """Tests verifying documentation claims in the module docstring."""

    def test_no_llm_file_types_documented(self):
        """Verify that documented NO_LLM file types are correctly categorized."""
        with tempfile.TemporaryDirectory() as tmpdir:
            # These file types are documented as NO_LLM
            no_llm_extensions = [".pdf", ".docx", ".xlsx", ".html", ".txt", ".csv"]

            for ext in no_llm_extensions:
                test_file = Path(tmpdir) / f"test{ext}"
                test_file.write_bytes(b"x" * 100000)

                estimate = estimate_file_tokens(str(test_file))
                assert estimate.category == FileCategory.NO_LLM, f"{ext} should be NO_LLM"
                assert estimate.total_tokens == 0, f"{ext} should have 0 tokens"

    def test_image_file_types_documented(self):
        """Verify that documented IMAGE file types are correctly categorized."""
        with tempfile.TemporaryDirectory() as tmpdir:
            # These file types are documented as using LLM for images
            image_extensions = [".jpg", ".jpeg", ".png"]

            for ext in image_extensions:
                test_file = Path(tmpdir) / f"test{ext}"
                test_file.write_bytes(b"x" * 100000)

                estimate = estimate_file_tokens(str(test_file))
                assert estimate.category == FileCategory.IMAGE, f"{ext} should be IMAGE"
                assert estimate.total_tokens > 0, f"{ext} should have tokens > 0"

    def test_pptx_file_type_documented(self):
        """Verify that PPTX files are correctly categorized."""
        with tempfile.TemporaryDirectory() as tmpdir:
            # PPTX with estimated images
            pptx_file = Path(tmpdir) / "slides.pptx"
            pptx_file.write_bytes(b"x" * 2000000)  # 2MB

            estimate = estimate_file_tokens(str(pptx_file))
            assert estimate.category == FileCategory.PPTX
            assert estimate.total_tokens > 0


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
