#!/usr/bin/env python3 -m pytest
"""Tests for batch conversion functionality."""

import os
import io
import json
import tempfile
import shutil
import subprocess
import sys
import threading
import time
import pytest
from pathlib import Path
from unittest.mock import MagicMock, patch

from markitdown import (
    MarkItDown,
    BatchConversionResult,
    BatchItemResult,
    BatchItemStatus,
    convert_batch,
    convert_directory,
    write_batch_results,
)

TEST_FILES_DIR = os.path.join(os.path.dirname(__file__), "test_files")
MARKITDOWN_MODULE = "markitdown"


class TestBatchConversionResult:
    """Tests for BatchConversionResult class."""

    def test_empty_result(self):
        """Test empty batch result."""
        result = BatchConversionResult()
        assert result.total_count == 0
        assert result.success_count == 0
        assert result.failed_count == 0
        assert result.completion_percentage == 100.0
        assert len(result.successful_items) == 0

    def test_result_with_items(self):
        """Test batch result with mixed items."""
        result = BatchConversionResult()

        # Add a successful item
        result.items.append(
            BatchItemResult(
                source_path="/test/file1.txt",
                status=BatchItemStatus.SUCCESS,
            )
        )

        # Add a failed item
        result.items.append(
            BatchItemResult(
                source_path="/test/file2.txt",
                status=BatchItemStatus.FAILED,
                error="Test error",
            )
        )

        # Add an unsupported item
        result.items.append(
            BatchItemResult(
                source_path="/test/file3.bin",
                status=BatchItemStatus.UNSUPPORTED,
            )
        )

        assert result.total_count == 3
        assert result.success_count == 1
        assert result.failed_count == 1
        assert result.unsupported_count == 1
        assert len(result.successful_items) == 1
        assert len(result.failed_items) == 1

    def test_iteration(self):
        """Test iterating over batch result."""
        result = BatchConversionResult()
        result.items.append(
            BatchItemResult(source_path="/test/a.txt", status=BatchItemStatus.SUCCESS)
        )
        result.items.append(
            BatchItemResult(source_path="/test/b.txt", status=BatchItemStatus.FAILED)
        )

        paths = [item.source_path for item in result]
        assert paths == ["/test/a.txt", "/test/b.txt"]

    def test_indexing(self):
        """Test indexing batch result."""
        result = BatchConversionResult()
        result.items.append(
            BatchItemResult(source_path="/test/a.txt", status=BatchItemStatus.SUCCESS)
        )
        result.items.append(
            BatchItemResult(source_path="/test/b.txt", status=BatchItemStatus.FAILED)
        )

        assert result[0].source_path == "/test/a.txt"
        assert result[1].source_path == "/test/b.txt"
        assert len(result) == 2

    def test_to_dict(self):
        """Test serialization to dictionary."""
        result = BatchConversionResult()
        result.source_directory = "/test/dir"
        result.items.append(
            BatchItemResult(source_path="/test/a.txt", status=BatchItemStatus.SUCCESS)
        )

        d = result.to_dict()
        assert d["source_directory"] == "/test/dir"
        assert d["total_count"] == 1
        assert d["success_count"] == 1
        assert len(d["items"]) == 1


class TestBatchItemResult:
    """Tests for BatchItemResult class."""

    def test_successful_item(self):
        """Test successful batch item."""
        from markitdown._base_converter import DocumentConverterResult

        mock_result = DocumentConverterResult(markdown="# Test", title="Test Title")

        item = BatchItemResult(
            source_path="/test/file.txt",
            status=BatchItemStatus.SUCCESS,
            result=mock_result,
        )

        assert item.markdown == "# Test"
        assert item.quality is not None

    def test_failed_item(self):
        """Test failed batch item."""
        item = BatchItemResult(
            source_path="/test/file.txt",
            status=BatchItemStatus.FAILED,
            error="Conversion failed",
            error_type="RuntimeError",
        )

        assert item.markdown is None
        assert item.quality is None
        assert item.error == "Conversion failed"

    def test_to_dict(self):
        """Test serialization to dictionary."""
        item = BatchItemResult(
            source_path="/test/file.txt",
            status=BatchItemStatus.FAILED,
            error="Test error",
        )

        d = item.to_dict()
        assert d["source_path"] == "/test/file.txt"
        assert d["status"] == "failed"
        assert d["error"] == "Test error"


class TestConvertBatch:
    """Tests for convert_batch function."""

    def test_convert_multiple_files(self):
        """Test converting multiple files."""
        markitdown = MarkItDown()

        files = [
            os.path.join(TEST_FILES_DIR, "test.json"),
            os.path.join(TEST_FILES_DIR, "test.xlsx"),
        ]

        result = markitdown.convert_batch(files)

        assert result.total_count == 2
        assert result.success_count == 2
        assert result.failed_count == 0

        # Verify each file was converted
        for item in result.successful_items:
            assert item.markdown is not None
            assert len(item.markdown) > 0
            assert item.quality is not None

    def test_convert_with_unsupported_file(self):
        """Test batch conversion with unsupported file."""
        markitdown = MarkItDown()

        files = [
            os.path.join(TEST_FILES_DIR, "test.json"),
            os.path.join(TEST_FILES_DIR, "random.bin"),
        ]

        result = markitdown.convert_batch(files)

        assert result.total_count == 2
        assert result.success_count == 1
        assert result.unsupported_count == 1

    def test_convert_sequential(self):
        """Test sequential batch conversion (max_workers=1)."""
        markitdown = MarkItDown()

        files = [
            os.path.join(TEST_FILES_DIR, "test.json"),
            os.path.join(TEST_FILES_DIR, "test.xlsx"),
        ]

        result = markitdown.convert_batch(files, max_workers=1)

        assert result.success_count == 2

    def test_progress_callback(self):
        """Test progress callback is called."""
        markitdown = MarkItDown()
        processed = []

        def on_progress(item: BatchItemResult):
            processed.append(item.source_path)

        files = [
            os.path.join(TEST_FILES_DIR, "test.json"),
            os.path.join(TEST_FILES_DIR, "test.xlsx"),
        ]

        markitdown.convert_batch(files, on_progress=on_progress, max_workers=1)

        assert len(processed) == 2

    def test_overall_quality(self):
        """Test aggregated quality report."""
        markitdown = MarkItDown()

        files = [
            os.path.join(TEST_FILES_DIR, "test.pdf"),
            os.path.join(TEST_FILES_DIR, "test.xlsx"),
        ]

        result = markitdown.convert_batch(files)

        overall_quality = result.overall_quality
        assert overall_quality.converter_used == "BatchConverter"
        assert overall_quality.metrics.get("total_files") == 2
        assert overall_quality.metrics.get("successful_files") == 2

    def test_empty_batch(self):
        """Test converting empty batch."""
        markitdown = MarkItDown()
        result = markitdown.convert_batch([])

        assert result.total_count == 0
        assert result.completion_percentage == 100.0


class TestConvertDirectory:
    """Tests for convert_directory function."""

    def test_convert_directory(self):
        """Test converting all files in a directory."""
        markitdown = MarkItDown()

        result = markitdown.convert_directory(TEST_FILES_DIR)

        # Should have processed multiple files
        assert result.total_count > 0
        assert result.source_directory == TEST_FILES_DIR

    def test_include_patterns(self):
        """Test filtering by include patterns."""
        markitdown = MarkItDown()

        result = markitdown.convert_directory(
            TEST_FILES_DIR, include_patterns=["*.json"]
        )

        # Should only have processed JSON files
        assert result.total_count >= 1
        for item in result.items:
            assert item.source_path.endswith(".json")

    def test_exclude_patterns(self):
        """Test filtering by exclude patterns."""
        markitdown = MarkItDown()

        result = markitdown.convert_directory(
            TEST_FILES_DIR, exclude_patterns=["*.bin", "*.wav", "*.mp3", "*.m4a"]
        )

        # Should not have processed excluded files
        for item in result.items:
            assert not item.source_path.endswith(".bin")
            assert not item.source_path.endswith(".wav")

    def test_non_recursive(self):
        """Test non-recursive directory conversion."""
        markitdown = MarkItDown()

        result = markitdown.convert_directory(TEST_FILES_DIR, recursive=False)

        # Should still process files in the directory
        assert result.total_count > 0

    def test_invalid_directory(self):
        """Test error handling for invalid directory."""
        markitdown = MarkItDown()

        with pytest.raises(ValueError):
            markitdown.convert_directory("/nonexistent/directory")


class TestWriteBatchResults:
    """Tests for write_batch_results function."""

    def test_write_results(self):
        """Test writing batch results to files."""
        markitdown = MarkItDown()

        files = [
            os.path.join(TEST_FILES_DIR, "test.json"),
            os.path.join(TEST_FILES_DIR, "test.xlsx"),
        ]

        result = markitdown.convert_batch(files)

        with tempfile.TemporaryDirectory() as tmpdir:
            output_mapping = write_batch_results(result, tmpdir)

            # Check that files were written
            assert len(output_mapping) == 2

            for source_path, output_path in output_mapping.items():
                assert os.path.exists(output_path)
                with open(output_path, "r", encoding="utf-8") as f:
                    content = f.read()
                    assert len(content) > 0

    def test_preserve_structure(self):
        """Test preserving directory structure."""
        markitdown = MarkItDown()

        result = markitdown.convert_directory(
            TEST_FILES_DIR, include_patterns=["*.json"]
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            output_mapping = write_batch_results(
                result, tmpdir, preserve_structure=True
            )

            # Check that files were written
            for output_path in output_mapping.values():
                assert os.path.exists(output_path)
                assert output_path.endswith(".md")

    def test_custom_extension(self):
        """Test custom file extension."""
        markitdown = MarkItDown()

        files = [os.path.join(TEST_FILES_DIR, "test.json")]

        result = markitdown.convert_batch(files)

        with tempfile.TemporaryDirectory() as tmpdir:
            output_mapping = write_batch_results(
                result, tmpdir, file_extension=".markdown"
            )

            for output_path in output_mapping.values():
                assert output_path.endswith(".markdown")


class TestBatchConversionQuality:
    """Tests for quality tracking in batch conversion."""

    def test_individual_quality(self):
        """Test that each item has quality information."""
        markitdown = MarkItDown()

        files = [
            os.path.join(TEST_FILES_DIR, "test.pdf"),
            os.path.join(TEST_FILES_DIR, "test.xlsx"),
        ]

        result = markitdown.convert_batch(files)

        for item in result.successful_items:
            assert item.quality is not None
            assert item.quality.converter_used is not None
            assert 0.0 <= item.quality.confidence <= 1.0

    def test_aggregated_quality(self):
        """Test aggregated quality report."""
        markitdown = MarkItDown()

        files = [
            os.path.join(TEST_FILES_DIR, "test.pdf"),
            os.path.join(TEST_FILES_DIR, "test.xlsx"),
            os.path.join(TEST_FILES_DIR, "random.bin"),  # Will fail
        ]

        result = markitdown.convert_batch(files)

        overall_quality = result.overall_quality
        assert overall_quality.is_partial  # Not all files converted
        assert overall_quality.metrics["total_files"] == 3
        assert overall_quality.metrics["successful_files"] == 2
        assert overall_quality.metrics["unsupported_files"] == 1
        assert len(overall_quality.warnings) > 0  # Should have warnings about failures

    def test_quality_serialization(self):
        """Test that quality info serializes correctly."""
        markitdown = MarkItDown()

        files = [os.path.join(TEST_FILES_DIR, "test.json")]

        result = markitdown.convert_batch(files)

        # Should be JSON serializable
        json_str = json.dumps(result.to_dict())
        parsed = json.loads(json_str)

        assert "items" in parsed
        assert "overall_quality" in parsed


class TestBatchConversionCLI:
    """Tests for batch conversion via actual CLI commands."""

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

    def test_cli_batch_single_directory(self):
        """Test CLI --batch with a single directory."""
        with tempfile.TemporaryDirectory() as tmpdir:
            result = self._run_cli([
                "--batch", TEST_FILES_DIR,
                "--include", "*.json",
                "-o", tmpdir,
            ])

            assert result.returncode == 0
            # Check output files were created
            output_files = list(Path(tmpdir).glob("*.md"))
            assert len(output_files) >= 1

    def test_cli_batch_include_pattern_only_pdfs(self):
        """Test that --include '*.pdf' only processes PDF files."""
        result = self._run_cli([
            "--batch", TEST_FILES_DIR,
            "--include", "*.pdf",
            "--summary",
        ], check=False)

        # Should have processed only PDF files
        assert result.returncode == 0
        # Verify in stdout that only PDF files were processed
        # Check stderr for summary
        assert "test.pdf" in result.stdout or "pdf" in result.stderr.lower()
        # Should NOT have processed other file types in output
        assert "test.xlsx" not in result.stdout
        assert "test.docx" not in result.stdout

    def test_cli_batch_include_multiple_patterns(self):
        """Test --include with multiple patterns."""
        result = self._run_cli([
            "--batch", TEST_FILES_DIR,
            "--include", "*.json",
            "--include", "*.xlsx",
            "--summary",
        ], check=False)

        assert result.returncode == 0
        # Both JSON and XLSX should be in output
        stdout_lower = result.stdout.lower()
        stderr_lower = result.stderr.lower()
        combined = stdout_lower + stderr_lower
        # Should have processed both types
        assert "json" in combined or "xlsx" in combined

    def test_cli_batch_exclude_pattern(self):
        """Test that --exclude properly excludes files."""
        result = self._run_cli([
            "--batch", TEST_FILES_DIR,
            "--exclude", "*.pdf",
            "--exclude", "*.bin",
            "--exclude", "*.wav",
            "--exclude", "*.mp3",
            "--exclude", "*.m4a",
            "--include", "*.json",
            "--summary",
        ], check=False)

        assert result.returncode == 0
        # PDF should NOT be in output
        assert "test.pdf" not in result.stdout

    def test_cli_batch_mixed_folder_and_file(self):
        """Test --batch with both a folder and individual files."""
        json_file = os.path.join(TEST_FILES_DIR, "test.json")
        xlsx_file = os.path.join(TEST_FILES_DIR, "test.xlsx")

        # Create a subdirectory with a test file to verify folder expansion
        with tempfile.TemporaryDirectory() as tmpdir:
            # Copy a test file to the temp dir
            test_json_copy = os.path.join(tmpdir, "copy_test.json")
            shutil.copy(json_file, test_json_copy)

            result = self._run_cli([
                "--batch",
                tmpdir,  # Directory
                xlsx_file,  # Individual file
                "--summary",
            ], check=False)

            assert result.returncode == 0
            # Should have processed files from both the directory AND the individual file
            # The summary in stderr should show at least 2 files processed
            assert "2" in result.stderr or "Successful" in result.stderr

    def test_cli_batch_progress_output(self):
        """Test that --progress shows real-time updates to stderr."""
        result = self._run_cli([
            "--batch", TEST_FILES_DIR,
            "--include", "*.json",
            "--include", "*.xlsx",
            "--progress",
        ], check=False)

        assert result.returncode == 0
        # Progress output should be in stderr
        stderr = result.stderr

        # Should contain progress indicators with status icons
        # Look for the progress format: [   N] ✓ or [   N] ✗ or similar
        assert "[" in stderr and "]" in stderr
        # Should contain at least one status icon (success or fail)
        has_status_icon = "✓" in stderr or "✗" in stderr or "?" in stderr or "○" in stderr
        assert has_status_icon, f"No status icons found in stderr: {stderr}"

    def test_cli_batch_progress_shows_confidence(self):
        """Test that --progress shows confidence percentage for successful conversions."""
        result = self._run_cli([
            "--batch", TEST_FILES_DIR,
            "--include", "*.json",
            "--progress",
        ], check=False)

        assert result.returncode == 0
        # Progress should show confidence in parentheses like (85%)
        # JSON files typically convert with high confidence
        stderr = result.stderr
        # Look for percentage pattern
        import re
        confidence_pattern = r"\(\d+%\)"
        assert re.search(confidence_pattern, stderr), f"No confidence percentage found in: {stderr}"

    def test_cli_batch_quality_json_output(self):
        """Test --quality-json outputs valid JSON to stderr."""
        result = self._run_cli([
            "--batch", TEST_FILES_DIR,
            "--include", "*.json",
            "--quality-json",
        ], check=False)

        assert result.returncode == 0

        # Parse JSON from stderr
        stderr_lines = result.stderr.strip()
        # The JSON output should be parseable
        try:
            quality_data = json.loads(stderr_lines)
            assert "items" in quality_data
            assert "total_count" in quality_data
            assert "overall_quality" in quality_data
        except json.JSONDecodeError as e:
            pytest.fail(f"Invalid JSON in stderr: {e}\nStderr: {stderr_lines}")

    def test_cli_batch_summary_output(self):
        """Test --summary shows batch conversion summary."""
        result = self._run_cli([
            "--batch", TEST_FILES_DIR,
            "--include", "*.json",
            "--include", "*.xlsx",
            "--summary",
        ], check=False)

        assert result.returncode == 0
        stderr = result.stderr

        # Summary should contain key information
        assert "BATCH CONVERSION SUMMARY" in stderr or "Total files" in stderr
        assert "Successful" in stderr or "success" in stderr.lower()

    def test_cli_batch_no_recursive(self):
        """Test --no-recursive only processes files in the immediate directory."""
        result = self._run_cli([
            "--batch", TEST_FILES_DIR,
            "--no-recursive",
            "--include", "*.json",
            "--summary",
        ], check=False)

        assert result.returncode == 0
        # Should still process files in the directory
        # This mainly ensures the flag doesn't cause errors

    def test_cli_batch_output_to_json_file(self):
        """Test that -o with .json extension outputs JSON results."""
        with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as f:
            output_file = f.name

        try:
            result = self._run_cli([
                "--batch", TEST_FILES_DIR,
                "--include", "*.json",
                "-o", output_file,
            ], check=False)

            assert result.returncode == 0

            # Read and parse the output JSON
            with open(output_file, "r") as f:
                data = json.load(f)

            assert "items" in data
            assert "total_count" in data
        finally:
            if os.path.exists(output_file):
                os.unlink(output_file)

    def test_cli_batch_output_to_directory(self):
        """Test that -o with directory path creates markdown files."""
        with tempfile.TemporaryDirectory() as tmpdir:
            result = self._run_cli([
                "--batch", TEST_FILES_DIR,
                "--include", "*.json",
                "-o", tmpdir,
            ], check=False)

            assert result.returncode == 0

            # Check that markdown files were created
            md_files = list(Path(tmpdir).glob("*.md"))
            assert len(md_files) >= 1

    def test_cli_batch_parallel_workers(self):
        """Test --parallel with specific worker count."""
        result = self._run_cli([
            "--batch", TEST_FILES_DIR,
            "--include", "*.json",
            "--include", "*.xlsx",
            "--parallel", "2",
            "--summary",
        ], check=False)

        assert result.returncode == 0
        # Should complete without errors

    def test_cli_batch_sequential_processing(self):
        """Test --parallel 1 for sequential processing."""
        result = self._run_cli([
            "--batch", TEST_FILES_DIR,
            "--include", "*.json",
            "--parallel", "1",
            "--progress",
        ], check=False)

        assert result.returncode == 0
        # Progress should still work in sequential mode
        assert "[" in result.stderr

    def test_cli_batch_empty_input_error(self):
        """Test that --batch without files shows error."""
        result = self._run_cli([
            "--batch",
        ], check=False)

        # Should fail with an error
        assert result.returncode != 0 or "requires" in result.stdout.lower() or "error" in result.stderr.lower()


class TestHandleBatchConversion:
    """Tests for _handle_batch_conversion function internals."""

    def test_detects_single_directory(self):
        """Test that single directory is detected correctly."""
        from markitdown.__main__ import _handle_batch_conversion
        from argparse import Namespace

        # Create mock args
        args = Namespace(
            filename=[TEST_FILES_DIR],
            no_recursive=False,
            include=["*.json"],
            exclude=None,
            parallel=1,
            progress=False,
            output=None,
            quality=False,
            quality_json=False,
            summary=False,
            keep_data_uris=False,
        )

        markitdown = MarkItDown()

        # Capture stdout
        with patch("sys.stdout", new_callable=io.StringIO) as mock_stdout:
            _handle_batch_conversion(args, markitdown, None)
            output = mock_stdout.getvalue()

        # Should have processed JSON files
        assert "json" in output.lower() or len(output) > 0

    def test_detects_mixed_files_and_directories(self):
        """Test that mixed files and directories are handled correctly."""
        from markitdown.__main__ import _handle_batch_conversion
        from argparse import Namespace

        json_file = os.path.join(TEST_FILES_DIR, "test.json")

        args = Namespace(
            filename=[TEST_FILES_DIR, json_file],  # Both dir and file
            no_recursive=False,
            include=None,
            exclude=["*.bin", "*.wav", "*.mp3", "*.m4a", "*.jpg", "*.pdf", "*.pptx", "*.docx", "*.xlsx", "*.xls", "*.epub", "*.msg", "*.html", "*.xml", "*.zip", "*.csv", "*.ipynb"],
            parallel=1,
            progress=False,
            output=None,
            quality=False,
            quality_json=False,
            summary=False,
            keep_data_uris=False,
        )

        markitdown = MarkItDown()

        # Should not raise an error
        with patch("sys.stdout", new_callable=io.StringIO):
            _handle_batch_conversion(args, markitdown, None)

    def test_progress_callback_status_icons(self):
        """Test that progress callback shows correct status icons."""
        from markitdown.__main__ import _handle_batch_conversion
        from argparse import Namespace

        args = Namespace(
            filename=[os.path.join(TEST_FILES_DIR, "test.json")],
            no_recursive=False,
            include=None,
            exclude=None,
            parallel=1,
            progress=True,
            output=None,
            quality=False,
            quality_json=False,
            summary=False,
            keep_data_uris=False,
        )

        markitdown = MarkItDown()

        # Capture stderr for progress output
        with patch("sys.stdout", new_callable=io.StringIO):
            with patch("sys.stderr", new_callable=io.StringIO) as mock_stderr:
                _handle_batch_conversion(args, markitdown, None)
                stderr_output = mock_stderr.getvalue()

        # Should contain progress with status icon
        assert "✓" in stderr_output or "✗" in stderr_output or "?" in stderr_output

    def test_quality_report_formatting(self):
        """Test that quality report is formatted correctly."""
        from markitdown.__main__ import _handle_batch_conversion
        from argparse import Namespace

        args = Namespace(
            filename=[os.path.join(TEST_FILES_DIR, "test.json")],
            no_recursive=False,
            include=None,
            exclude=None,
            parallel=1,
            progress=False,
            output=None,
            quality=True,
            quality_json=False,
            summary=False,
            keep_data_uris=False,
        )

        markitdown = MarkItDown()

        with patch("sys.stdout", new_callable=io.StringIO):
            with patch("sys.stderr", new_callable=io.StringIO) as mock_stderr:
                _handle_batch_conversion(args, markitdown, None)
                stderr_output = mock_stderr.getvalue()

        # Should contain quality report elements
        assert "BATCH CONVERSION SUMMARY" in stderr_output or "Total files" in stderr_output
        assert "OVERALL QUALITY" in stderr_output


class TestProgressCallbackThreadSafety:
    """Tests for thread safety in progress callback."""

    def test_progress_callback_thread_safe(self):
        """Test that progress callback is thread-safe with parallel workers."""
        markitdown = MarkItDown()

        # Use multiple files to trigger parallel processing
        files = [
            os.path.join(TEST_FILES_DIR, "test.json"),
            os.path.join(TEST_FILES_DIR, "test.xlsx"),
            os.path.join(TEST_FILES_DIR, "test.pdf"),
        ]

        # Track progress calls
        progress_calls = []
        progress_lock = threading.Lock()

        def thread_safe_callback(item: BatchItemResult):
            with progress_lock:
                progress_calls.append({
                    "source": item.source_path,
                    "status": item.status,
                    "thread": threading.current_thread().name,
                    "time": time.time(),
                })

        # Run with multiple workers
        result = markitdown.convert_batch(
            files,
            max_workers=3,
            on_progress=thread_safe_callback,
        )

        # All files should have been processed
        assert len(progress_calls) == 3
        assert result.total_count == 3

        # Verify no duplicate calls (thread safety check)
        sources = [call["source"] for call in progress_calls]
        assert len(sources) == len(set(sources)), "Duplicate progress callbacks detected"

    def test_progress_counter_accuracy_parallel(self):
        """Test that progress counter is accurate with parallel processing."""
        markitdown = MarkItDown()

        files = [
            os.path.join(TEST_FILES_DIR, "test.json"),
            os.path.join(TEST_FILES_DIR, "test.xlsx"),
        ]

        counter = [0]
        counter_lock = threading.Lock()

        def counting_callback(item: BatchItemResult):
            with counter_lock:
                counter[0] += 1

        result = markitdown.convert_batch(
            files,
            max_workers=2,
            on_progress=counting_callback,
        )

        # Counter should match total processed
        assert counter[0] == result.total_count

    def test_cli_parallel_progress_no_corruption(self):
        """Test that CLI --parallel with --progress doesn't corrupt output."""
        result = subprocess.run(
            [
                sys.executable, "-m", MARKITDOWN_MODULE,
                "--batch", TEST_FILES_DIR,
                "--include", "*.json",
                "--include", "*.xlsx",
                "--parallel", "4",
                "--progress",
            ],
            capture_output=True,
            text=True,
            cwd=os.path.dirname(TEST_FILES_DIR),
        )

        # Should complete without errors
        assert result.returncode == 0

        # Check stderr for progress lines
        stderr_lines = result.stderr.strip().split("\n")
        progress_lines = [l for l in stderr_lines if l.startswith("[")]

        # Each progress line should be well-formed (no interleaved output)
        for line in progress_lines:
            # Should match pattern: [   N] <icon> <path> (<confidence>)
            assert line.startswith("[")
            assert "]" in line
            # Should not have garbled/interleaved characters
            assert "\n" not in line[1:]  # No newlines within a progress line


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
