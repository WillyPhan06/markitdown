#!/usr/bin/env python3 -m pytest
"""Tests for the DocumentMetadata feature."""

import io
import os
import tempfile
import warnings
from datetime import datetime
from pathlib import Path
from unittest.mock import MagicMock, patch
from zipfile import ZipFile

import pytest

from markitdown import (
    MarkItDown,
    DocumentConverterResult,
    DocumentMetadata,
    ConversionCache,
    BatchItemStatus,
)
from markitdown._metadata_extractor import (
    extract_metadata,
    _count_words,
    _count_characters,
    _parse_iso_date,
)
from markitdown._stream_info import StreamInfo
from markitdown._cache import CacheEntry, cache_entry_to_result

TEST_FILES_DIR = os.path.join(os.path.dirname(__file__), "test_files")


class TestDocumentMetadata:
    """Tests for DocumentMetadata dataclass."""

    def test_metadata_creation_empty(self):
        """Test creating empty metadata."""
        metadata = DocumentMetadata()

        assert metadata.title is None
        assert metadata.author is None
        assert metadata.date_created is None
        assert metadata.date_modified is None
        assert metadata.language is None
        assert metadata.page_count is None
        assert metadata.word_count is None
        assert metadata.character_count is None
        assert metadata.description is None
        assert metadata.keywords is None
        assert metadata.custom == {}
        assert metadata.is_empty()

    def test_metadata_creation_with_values(self):
        """Test creating metadata with values."""
        now = datetime.now()
        metadata = DocumentMetadata(
            title="Test Document",
            author="John Doe",
            date_created=now,
            date_modified=now,
            language="en-US",
            page_count=10,
            word_count=5000,
            character_count=25000,
            description="A test document",
            keywords=["test", "document"],
            custom={"extra": "value"},
        )

        assert metadata.title == "Test Document"
        assert metadata.author == "John Doe"
        assert metadata.date_created == now
        assert metadata.date_modified == now
        assert metadata.language == "en-US"
        assert metadata.page_count == 10
        assert metadata.word_count == 5000
        assert metadata.character_count == 25000
        assert metadata.description == "A test document"
        assert metadata.keywords == ["test", "document"]
        assert metadata.custom == {"extra": "value"}
        assert not metadata.is_empty()

    def test_metadata_is_empty_with_partial_data(self):
        """Test is_empty with partially filled metadata."""
        # Only title
        metadata1 = DocumentMetadata(title="Title")
        assert not metadata1.is_empty()

        # Only word count
        metadata2 = DocumentMetadata(word_count=100)
        assert not metadata2.is_empty()

        # Only custom data
        metadata3 = DocumentMetadata(custom={"key": "value"})
        assert not metadata3.is_empty()

        # Empty keywords list should still be empty
        metadata4 = DocumentMetadata(keywords=[])
        assert metadata4.is_empty()

    def test_metadata_to_dict(self):
        """Test metadata serialization to dict."""
        now = datetime(2024, 1, 15, 10, 30, 0)
        metadata = DocumentMetadata(
            title="Test",
            author="Author",
            date_created=now,
            date_modified=now,
            language="en",
            page_count=5,
            word_count=1000,
            character_count=5000,
            description="Description",
            keywords=["a", "b"],
            custom={"extra": "data"},
        )

        d = metadata.to_dict()

        assert d["title"] == "Test"
        assert d["author"] == "Author"
        assert d["date_created"] == "2024-01-15T10:30:00"
        assert d["date_modified"] == "2024-01-15T10:30:00"
        assert d["language"] == "en"
        assert d["page_count"] == 5
        assert d["word_count"] == 1000
        assert d["character_count"] == 5000
        assert d["description"] == "Description"
        assert d["keywords"] == ["a", "b"]
        assert d["custom"] == {"extra": "data"}

    def test_metadata_to_dict_omits_none(self):
        """Test that to_dict omits None values."""
        metadata = DocumentMetadata(title="Only Title")
        d = metadata.to_dict()

        assert "title" in d
        assert "author" not in d
        assert "date_created" not in d
        assert "word_count" not in d

    def test_metadata_to_dict_omits_empty_keywords(self):
        """Test that to_dict omits empty keywords list."""
        metadata = DocumentMetadata(title="Title", keywords=[])
        d = metadata.to_dict()

        assert "title" in d
        assert "keywords" not in d

    def test_metadata_from_dict(self):
        """Test metadata deserialization from dict."""
        data = {
            "title": "Test",
            "author": "Author",
            "date_created": "2024-01-15T10:30:00",
            "date_modified": "2024-01-15T14:00:00",
            "language": "en",
            "page_count": 5,
            "word_count": 1000,
            "character_count": 5000,
            "description": "Description",
            "keywords": ["a", "b"],
            "custom": {"extra": "data"},
        }

        metadata = DocumentMetadata.from_dict(data)

        assert metadata.title == "Test"
        assert metadata.author == "Author"
        assert metadata.date_created == datetime(2024, 1, 15, 10, 30, 0)
        assert metadata.date_modified == datetime(2024, 1, 15, 14, 0, 0)
        assert metadata.language == "en"
        assert metadata.page_count == 5
        assert metadata.word_count == 1000
        assert metadata.character_count == 5000
        assert metadata.description == "Description"
        assert metadata.keywords == ["a", "b"]
        assert metadata.custom == {"extra": "data"}

    def test_metadata_from_dict_missing_fields(self):
        """Test from_dict with missing optional fields."""
        data = {"title": "Only Title"}
        metadata = DocumentMetadata.from_dict(data)

        assert metadata.title == "Only Title"
        assert metadata.author is None
        assert metadata.date_created is None
        assert metadata.word_count is None
        assert metadata.custom == {}

    def test_metadata_from_dict_invalid_dates(self):
        """Test from_dict handles invalid date strings gracefully."""
        data = {
            "title": "Test",
            "date_created": "invalid-date",
            "date_modified": "also-invalid",
        }

        metadata = DocumentMetadata.from_dict(data)

        assert metadata.title == "Test"
        assert metadata.date_created is None  # Invalid, should be None
        assert metadata.date_modified is None

    def test_metadata_roundtrip(self):
        """Test complete roundtrip through to_dict and from_dict."""
        now = datetime(2024, 6, 15, 12, 0, 0)
        original = DocumentMetadata(
            title="Complete Document",
            author="Test Author",
            date_created=now,
            date_modified=now,
            language="fr-FR",
            page_count=100,
            word_count=50000,
            character_count=250000,
            description="A complete test document",
            keywords=["test", "complete", "metadata"],
            custom={"source": "test", "version": 2},
        )

        # Roundtrip
        data = original.to_dict()
        restored = DocumentMetadata.from_dict(data)

        assert restored.title == original.title
        assert restored.author == original.author
        assert restored.date_created == original.date_created
        assert restored.date_modified == original.date_modified
        assert restored.language == original.language
        assert restored.page_count == original.page_count
        assert restored.word_count == original.word_count
        assert restored.character_count == original.character_count
        assert restored.description == original.description
        assert restored.keywords == original.keywords
        assert restored.custom == original.custom

    def test_metadata_str_representation(self):
        """Test string representation of metadata."""
        metadata = DocumentMetadata(
            title="Test Doc",
            author="Author",
            page_count=5,
            word_count=1000,
            character_count=5000,
        )

        str_repr = str(metadata)

        assert "Title: Test Doc" in str_repr
        assert "Author: Author" in str_repr
        assert "Pages: 5" in str_repr
        assert "Words: 1000" in str_repr
        assert "Characters: 5000" in str_repr

    def test_metadata_str_empty(self):
        """Test string representation of empty metadata."""
        metadata = DocumentMetadata()
        assert str(metadata) == "No metadata available"


class TestDocumentMetadataDateHelpers:
    """Tests for date helper methods in DocumentMetadata."""

    def test_get_date_created_iso(self):
        """Test get_date_created_iso helper."""
        dt = datetime(2024, 1, 15, 10, 30, 45)
        metadata = DocumentMetadata(date_created=dt)

        assert metadata.get_date_created_iso() == "2024-01-15T10:30:45"

    def test_get_date_created_iso_none(self):
        """Test get_date_created_iso returns None when not set."""
        metadata = DocumentMetadata()
        assert metadata.get_date_created_iso() is None

    def test_get_date_modified_iso(self):
        """Test get_date_modified_iso helper."""
        dt = datetime(2024, 3, 20, 14, 0, 0)
        metadata = DocumentMetadata(date_modified=dt)

        assert metadata.get_date_modified_iso() == "2024-03-20T14:00:00"

    def test_get_date_modified_iso_none(self):
        """Test get_date_modified_iso returns None when not set."""
        metadata = DocumentMetadata()
        assert metadata.get_date_modified_iso() is None

    def test_get_date_created_formatted_default(self):
        """Test get_date_created_formatted with default format."""
        dt = datetime(2024, 1, 15, 10, 30, 45)
        metadata = DocumentMetadata(date_created=dt)

        assert metadata.get_date_created_formatted() == "2024-01-15 10:30:45"

    def test_get_date_created_formatted_custom(self):
        """Test get_date_created_formatted with custom format."""
        dt = datetime(2024, 1, 15, 10, 30, 45)
        metadata = DocumentMetadata(date_created=dt)

        assert metadata.get_date_created_formatted("%B %d, %Y") == "January 15, 2024"
        assert metadata.get_date_created_formatted("%Y/%m/%d") == "2024/01/15"
        assert metadata.get_date_created_formatted("%d-%m-%Y") == "15-01-2024"

    def test_get_date_created_formatted_none(self):
        """Test get_date_created_formatted returns None when not set."""
        metadata = DocumentMetadata()
        assert metadata.get_date_created_formatted() is None

    def test_get_date_modified_formatted_default(self):
        """Test get_date_modified_formatted with default format."""
        dt = datetime(2024, 3, 20, 14, 0, 0)
        metadata = DocumentMetadata(date_modified=dt)

        assert metadata.get_date_modified_formatted() == "2024-03-20 14:00:00"

    def test_get_date_modified_formatted_custom(self):
        """Test get_date_modified_formatted with custom format."""
        dt = datetime(2024, 3, 20, 14, 0, 0)
        metadata = DocumentMetadata(date_modified=dt)

        assert metadata.get_date_modified_formatted("%B %d, %Y") == "March 20, 2024"

    def test_get_date_modified_formatted_none(self):
        """Test get_date_modified_formatted returns None when not set."""
        metadata = DocumentMetadata()
        assert metadata.get_date_modified_formatted() is None

    def test_get_date_created_date_only(self):
        """Test get_date_created_date_only helper."""
        dt = datetime(2024, 1, 15, 10, 30, 45)
        metadata = DocumentMetadata(date_created=dt)

        assert metadata.get_date_created_date_only() == "2024-01-15"

    def test_get_date_created_date_only_none(self):
        """Test get_date_created_date_only returns None when not set."""
        metadata = DocumentMetadata()
        assert metadata.get_date_created_date_only() is None

    def test_get_date_modified_date_only(self):
        """Test get_date_modified_date_only helper."""
        dt = datetime(2024, 3, 20, 14, 0, 0)
        metadata = DocumentMetadata(date_modified=dt)

        assert metadata.get_date_modified_date_only() == "2024-03-20"

    def test_get_date_modified_date_only_none(self):
        """Test get_date_modified_date_only returns None when not set."""
        metadata = DocumentMetadata()
        assert metadata.get_date_modified_date_only() is None


class TestMetadataExtractorHelpers:
    """Tests for helper functions in metadata extractor."""

    def test_count_words_simple(self):
        """Test word counting."""
        assert _count_words("Hello world") == 2
        assert _count_words("One two three four five") == 5
        assert _count_words("") == 0
        assert _count_words("Single") == 1

    def test_count_words_with_whitespace(self):
        """Test word counting with various whitespace."""
        assert _count_words("Hello   world") == 2
        assert _count_words("One\ntwo\nthree") == 3
        assert _count_words("Tab\tseparated\twords") == 3
        assert _count_words("  Leading and trailing  ") == 3

    def test_count_characters_simple(self):
        """Test character counting."""
        assert _count_characters("Hello") == 5
        assert _count_characters("") == 0

    def test_count_characters_excludes_whitespace(self):
        """Test that character counting excludes whitespace."""
        assert _count_characters("Hello world") == 10  # No space
        assert _count_characters("One\ntwo") == 6  # No newline
        assert _count_characters("Tab\there") == 7  # No tab
        assert _count_characters("  spaced  ") == 6  # No spaces
        assert _count_characters("   ") == 0  # Only spaces
        assert _count_characters("\n\t\r") == 0  # Only whitespace

    def test_count_characters_handles_unicode_whitespace(self):
        """Test that character counting handles Unicode whitespace correctly."""
        # Non-breaking space (U+00A0) and other Unicode whitespace
        assert _count_characters("a\u00A0b") == 2  # Non-breaking space excluded
        assert _count_characters("a\u2003b") == 2  # Em space excluded
        assert _count_characters("a\u3000b") == 2  # Ideographic space excluded

    def test_parse_iso_date_full(self):
        """Test parsing full ISO date."""
        result = _parse_iso_date("2024-01-15T10:30:45")
        assert result == datetime(2024, 1, 15, 10, 30, 45)

    def test_parse_iso_date_with_z(self):
        """Test parsing ISO date with Z suffix."""
        result = _parse_iso_date("2024-01-15T10:30:45Z")
        assert result == datetime(2024, 1, 15, 10, 30, 45)

    def test_parse_iso_date_date_only(self):
        """Test parsing date-only string."""
        result = _parse_iso_date("2024-01-15")
        assert result == datetime(2024, 1, 15, 0, 0, 0)

    def test_parse_iso_date_with_timezone(self):
        """Test parsing date with timezone offset."""
        result = _parse_iso_date("2024-01-15T10:30:45+05:00")
        # Timezone is stripped, only local time parsed
        assert result is not None
        assert result.year == 2024
        assert result.month == 1
        assert result.day == 15

    def test_parse_iso_date_invalid(self):
        """Test parsing invalid date returns None."""
        assert _parse_iso_date("not-a-date") is None
        assert _parse_iso_date("") is None
        assert _parse_iso_date("   ") is None


class TestMetadataExtraction:
    """Tests for metadata extraction from various formats."""

    def test_extract_metadata_computes_word_count(self):
        """Test that extract_metadata computes word count from markdown."""
        stream = io.BytesIO(b"test content")
        stream_info = StreamInfo(extension=".txt")
        markdown = "This is a test document with ten words total here."

        metadata = extract_metadata(stream, stream_info, markdown)

        assert metadata.word_count == 10

    def test_extract_metadata_computes_character_count(self):
        """Test that extract_metadata computes character count from markdown."""
        stream = io.BytesIO(b"test content")
        stream_info = StreamInfo(extension=".txt")
        markdown = "Hello world"  # 10 chars without space

        metadata = extract_metadata(stream, stream_info, markdown)

        assert metadata.character_count == 10

    def test_extract_metadata_empty_markdown(self):
        """Test extract_metadata with empty markdown."""
        stream = io.BytesIO(b"")
        stream_info = StreamInfo(extension=".txt")

        metadata = extract_metadata(stream, stream_info, "")

        # Should return empty metadata without crashing
        assert metadata.word_count is None
        assert metadata.character_count is None

    def test_extract_metadata_restores_stream_position(self):
        """Test that extract_metadata restores stream position."""
        stream = io.BytesIO(b"test content here")
        stream.seek(5)  # Position at byte 5
        stream_info = StreamInfo(extension=".txt")

        extract_metadata(stream, stream_info, "markdown")

        # Position should be restored
        assert stream.tell() == 5

    def test_extract_metadata_unknown_format(self):
        """Test extract_metadata with unknown format."""
        stream = io.BytesIO(b"unknown content")
        stream_info = StreamInfo(extension=".unknown")
        markdown = "Some converted text here"

        metadata = extract_metadata(stream, stream_info, markdown)

        # Should still compute word/char count
        assert metadata.word_count == 4
        assert metadata.character_count is not None


class TestMetadataExtractionFromRealFiles:
    """Tests for metadata extraction from real test files."""

    @pytest.mark.skipif(
        not os.path.exists(os.path.join(TEST_FILES_DIR, "test.docx")),
        reason="Test file not found"
    )
    def test_extract_docx_metadata(self):
        """Test extracting metadata from a DOCX file."""
        docx_path = os.path.join(TEST_FILES_DIR, "test.docx")
        markitdown = MarkItDown()

        result = markitdown.convert(docx_path)

        # Should have metadata populated
        metadata = result.metadata
        assert metadata is not None
        # Word count should be computed from markdown at minimum
        assert metadata.word_count is not None
        assert metadata.word_count > 0
        assert metadata.character_count is not None
        assert metadata.character_count > 0

    @pytest.mark.skipif(
        not os.path.exists(os.path.join(TEST_FILES_DIR, "test.pdf")),
        reason="Test file not found"
    )
    def test_extract_pdf_metadata(self):
        """Test extracting metadata from a PDF file."""
        pdf_path = os.path.join(TEST_FILES_DIR, "test.pdf")
        markitdown = MarkItDown()

        result = markitdown.convert(pdf_path)

        metadata = result.metadata
        assert metadata is not None
        assert metadata.word_count is not None
        assert metadata.character_count is not None

    @pytest.mark.skipif(
        not os.path.exists(os.path.join(TEST_FILES_DIR, "test.xlsx")),
        reason="Test file not found"
    )
    def test_extract_xlsx_metadata(self):
        """Test extracting metadata from an XLSX file."""
        xlsx_path = os.path.join(TEST_FILES_DIR, "test.xlsx")
        markitdown = MarkItDown()

        result = markitdown.convert(xlsx_path)

        metadata = result.metadata
        assert metadata is not None
        assert metadata.word_count is not None

    @pytest.mark.skipif(
        not os.path.exists(os.path.join(TEST_FILES_DIR, "test.pptx")),
        reason="Test file not found"
    )
    def test_extract_pptx_metadata(self):
        """Test extracting metadata from a PPTX file."""
        pptx_path = os.path.join(TEST_FILES_DIR, "test.pptx")
        markitdown = MarkItDown()

        result = markitdown.convert(pptx_path)

        metadata = result.metadata
        assert metadata is not None
        assert metadata.word_count is not None

    @pytest.mark.skipif(
        not os.path.exists(os.path.join(TEST_FILES_DIR, "test_blog.html")),
        reason="Test file not found"
    )
    def test_extract_html_metadata(self):
        """Test extracting metadata from an HTML file."""
        html_path = os.path.join(TEST_FILES_DIR, "test_blog.html")
        markitdown = MarkItDown()

        result = markitdown.convert(html_path)

        metadata = result.metadata
        assert metadata is not None
        assert metadata.word_count is not None

    @pytest.mark.skipif(
        not os.path.exists(os.path.join(TEST_FILES_DIR, "test.epub")),
        reason="Test file not found"
    )
    def test_extract_epub_metadata(self):
        """Test extracting metadata from an EPUB file."""
        epub_path = os.path.join(TEST_FILES_DIR, "test.epub")
        markitdown = MarkItDown()

        result = markitdown.convert(epub_path)

        metadata = result.metadata
        assert metadata is not None
        assert metadata.word_count is not None


class TestMetadataErrorHandling:
    """Tests for error handling in metadata extraction."""

    def test_metadata_extraction_does_not_interrupt_conversion(self):
        """Test that metadata extraction errors don't interrupt conversion."""
        # Create a file that will convert but fail metadata extraction
        with tempfile.NamedTemporaryFile(mode='w', suffix='.txt', delete=False) as f:
            f.write("Simple text content")
            temp_path = f.name

        try:
            markitdown = MarkItDown()

            # Mock extract_metadata to raise an exception
            with patch('markitdown._markitdown.extract_metadata') as mock_extract:
                mock_extract.side_effect = RuntimeError("Metadata extraction failed!")

                # Conversion should still succeed
                with warnings.catch_warnings(record=True) as w:
                    warnings.simplefilter("always")
                    result = markitdown.convert(temp_path)

                    # Should have warning about metadata failure
                    assert len(w) >= 1
                    assert any("Metadata extraction failed" in str(warning.message) for warning in w)

                # Result should still be valid
                assert result is not None
                assert "Simple text content" in result.markdown
        finally:
            os.unlink(temp_path)

    def test_metadata_extraction_warning_includes_details(self):
        """Test that metadata extraction warning includes error details."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.txt', delete=False) as f:
            f.write("Test content")
            temp_path = f.name

        try:
            markitdown = MarkItDown()

            with patch('markitdown._markitdown.extract_metadata') as mock_extract:
                mock_extract.side_effect = ValueError("Specific error message")

                with warnings.catch_warnings(record=True) as w:
                    warnings.simplefilter("always")
                    markitdown.convert(temp_path)

                    # Find the metadata warning
                    metadata_warnings = [
                        warning for warning in w
                        if "Metadata extraction failed" in str(warning.message)
                    ]
                    assert len(metadata_warnings) >= 1

                    # Should include exception type and message
                    warning_msg = str(metadata_warnings[0].message)
                    assert "ValueError" in warning_msg
                    assert "Specific error message" in warning_msg
                    # Should include file type info
                    assert "type:" in warning_msg
                    # Should include traceback
                    assert "Traceback" in warning_msg
        finally:
            os.unlink(temp_path)

    def test_corrupt_stream_does_not_crash(self):
        """Test that corrupt stream content doesn't crash metadata extraction."""
        # Corrupt PDF-like content
        corrupt_stream = io.BytesIO(b"%PDF-corrupt-data-here\x00\x01\x02")
        stream_info = StreamInfo(extension=".pdf", mimetype="application/pdf")

        # Should not raise, just return empty/partial metadata
        metadata = extract_metadata(corrupt_stream, stream_info, "Some markdown")

        assert metadata is not None
        # Word count from markdown should still work
        assert metadata.word_count == 2


class TestMetadataInCache:
    """Tests for metadata caching."""

    def test_cache_entry_with_metadata(self):
        """Test CacheEntry stores metadata."""
        metadata_dict = {
            "title": "Cached Title",
            "author": "Cached Author",
            "word_count": 500,
        }

        entry = CacheEntry(
            file_hash="abc123",
            markdown="# Test",
            title="Test Title",
            quality_dict={"confidence": 0.9},
            metadata_dict=metadata_dict,
        )

        assert entry.metadata_dict == metadata_dict

    def test_cache_entry_to_dict_includes_metadata(self):
        """Test CacheEntry.to_dict includes metadata."""
        metadata_dict = {"title": "Test", "word_count": 100}
        entry = CacheEntry(
            file_hash="abc123",
            markdown="# Test",
            title=None,
            quality_dict=None,
            metadata_dict=metadata_dict,
        )

        d = entry.to_dict()
        assert "metadata_dict" in d
        assert d["metadata_dict"] == metadata_dict

    def test_cache_entry_from_dict_restores_metadata(self):
        """Test CacheEntry.from_dict restores metadata."""
        data = {
            "file_hash": "abc123",
            "markdown": "# Test",
            "title": None,
            "quality_dict": None,
            "metadata_dict": {"title": "Restored", "page_count": 10},
        }

        entry = CacheEntry.from_dict(data)
        assert entry.metadata_dict == {"title": "Restored", "page_count": 10}

    def test_cache_entry_from_dict_handles_missing_metadata(self):
        """Test CacheEntry.from_dict handles missing metadata (backward compat)."""
        # Old cache entry format without metadata_dict
        data = {
            "file_hash": "abc123",
            "markdown": "# Test",
            "title": None,
            "quality_dict": None,
            # No metadata_dict key
        }

        entry = CacheEntry.from_dict(data)
        assert entry.metadata_dict is None

    def test_cache_entry_to_result_restores_metadata(self):
        """Test cache_entry_to_result properly restores metadata."""
        metadata_dict = {
            "title": "Test Document",
            "author": "Test Author",
            "date_created": "2024-01-15T10:30:00",
            "word_count": 1000,
            "character_count": 5000,
        }

        entry = CacheEntry(
            file_hash="abc123",
            markdown="# Test",
            title="Title",
            quality_dict={"confidence": 0.9},
            metadata_dict=metadata_dict,
        )

        result = cache_entry_to_result(entry)

        assert result.metadata is not None
        assert result.metadata.title == "Test Document"
        assert result.metadata.author == "Test Author"
        assert result.metadata.date_created == datetime(2024, 1, 15, 10, 30, 0)
        assert result.metadata.word_count == 1000
        assert result.metadata.character_count == 5000

    def test_cache_entry_to_result_handles_no_metadata(self):
        """Test cache_entry_to_result handles missing metadata."""
        entry = CacheEntry(
            file_hash="abc123",
            markdown="# Test",
            title="Title",
            quality_dict=None,
            metadata_dict=None,
        )

        result = cache_entry_to_result(entry)

        # Should have None metadata (not default empty)
        assert result._metadata is None

    def test_metadata_roundtrip_through_cache(self):
        """Test metadata survives full cache roundtrip."""
        with tempfile.TemporaryDirectory() as tmpdir:
            cache = ConversionCache(Path(tmpdir) / "cache")

            # Create test file
            test_file = Path(tmpdir) / "test.txt"
            test_file.write_text("Test content for caching")

            # Create result with metadata
            now = datetime(2024, 6, 15, 12, 0, 0)
            metadata = DocumentMetadata(
                title="Cache Test",
                author="Tester",
                date_created=now,
                word_count=100,
                character_count=500,
                keywords=["cache", "test"],
            )
            result = DocumentConverterResult(
                markdown="# Cached Content",
                title="Title",
                metadata=metadata,
            )

            # Store in cache
            cache.put(str(test_file), result)

            # Retrieve from cache
            entry = cache.get(str(test_file))
            assert entry is not None

            # Convert back to result
            restored_result = cache_entry_to_result(entry)

            # Verify metadata is preserved
            assert restored_result.metadata.title == "Cache Test"
            assert restored_result.metadata.author == "Tester"
            assert restored_result.metadata.date_created == now
            assert restored_result.metadata.word_count == 100
            assert restored_result.metadata.character_count == 500
            assert restored_result.metadata.keywords == ["cache", "test"]


class TestMetadataInBatchConversion:
    """Tests for metadata in batch conversion."""

    def test_batch_item_has_metadata(self):
        """Test that batch items have metadata property."""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Create test file
            test_file = Path(tmpdir) / "test.txt"
            test_file.write_text("Test content for batch")

            markitdown = MarkItDown()
            result = markitdown.convert_batch([str(test_file)])

            assert result.success_count == 1
            item = result.successful_items[0]

            # Should have metadata
            assert item.metadata is not None
            assert item.metadata.word_count is not None

    def test_batch_item_to_dict_includes_metadata(self):
        """Test that batch item to_dict includes metadata."""
        with tempfile.TemporaryDirectory() as tmpdir:
            test_file = Path(tmpdir) / "test.txt"
            test_file.write_text("Content")

            markitdown = MarkItDown()
            result = markitdown.convert_batch([str(test_file)])

            item = result.successful_items[0]
            item_dict = item.to_dict()

            # Should have metadata in dict
            assert "metadata" in item_dict
            assert item_dict["metadata"] is not None
            assert "word_count" in item_dict["metadata"]

    def test_cached_batch_item_preserves_metadata(self):
        """Test that cached batch items preserve metadata."""
        with tempfile.TemporaryDirectory() as tmpdir:
            cache = ConversionCache(Path(tmpdir) / "cache")

            test_file = Path(tmpdir) / "test.txt"
            test_file.write_text("Cached batch content here")

            markitdown = MarkItDown()

            # First run - stores in cache
            result1 = markitdown.convert_batch([str(test_file)], cache=cache)
            original_metadata = result1.successful_items[0].metadata

            # Second run - from cache
            result2 = markitdown.convert_batch([str(test_file)], cache=cache)
            cached_metadata = result2.successful_items[0].metadata

            assert result2.cached_count == 1
            assert cached_metadata is not None
            assert cached_metadata.word_count == original_metadata.word_count


class TestMetadataInDocumentConverterResult:
    """Tests for metadata in DocumentConverterResult."""

    def test_result_has_metadata_property(self):
        """Test DocumentConverterResult has metadata property."""
        result = DocumentConverterResult(markdown="# Test")

        # Accessing metadata should work
        metadata = result.metadata
        assert metadata is not None
        assert isinstance(metadata, DocumentMetadata)

    def test_result_lazy_initializes_metadata(self):
        """Test metadata is lazily initialized."""
        result = DocumentConverterResult(markdown="# Test")

        # Internal _metadata should be None initially
        assert result._metadata is None

        # Accessing property creates it
        _ = result.metadata
        assert result._metadata is not None

    def test_result_accepts_metadata_in_constructor(self):
        """Test result can accept metadata in constructor."""
        metadata = DocumentMetadata(title="Provided", word_count=100)
        result = DocumentConverterResult(
            markdown="# Test",
            metadata=metadata,
        )

        assert result.metadata.title == "Provided"
        assert result.metadata.word_count == 100

    def test_result_metadata_setter(self):
        """Test metadata can be set via property."""
        result = DocumentConverterResult(markdown="# Test")
        metadata = DocumentMetadata(title="Set via setter")

        result.metadata = metadata

        assert result.metadata.title == "Set via setter"


class TestMetadataEdgeCases:
    """Edge case tests for metadata."""

    def test_metadata_with_unicode(self):
        """Test metadata handles unicode correctly."""
        metadata = DocumentMetadata(
            title="文档标题",
            author="作者名",
            description="日本語の説明",
            keywords=["한국어", "العربية", "emoji 🎉"],
        )

        # Roundtrip
        d = metadata.to_dict()
        restored = DocumentMetadata.from_dict(d)

        assert restored.title == "文档标题"
        assert restored.author == "作者名"
        assert restored.description == "日本語の説明"
        assert "emoji 🎉" in restored.keywords

    def test_metadata_with_special_characters(self):
        """Test metadata handles special characters."""
        metadata = DocumentMetadata(
            title="Title with \"quotes\" and 'apostrophes'",
            description="Line1\nLine2\tTabbed",
            keywords=["back\\slash", "forward/slash"],
        )

        d = metadata.to_dict()
        restored = DocumentMetadata.from_dict(d)

        assert restored.title == "Title with \"quotes\" and 'apostrophes'"
        assert "Line1\nLine2" in restored.description

    def test_metadata_with_large_values(self):
        """Test metadata handles large values."""
        large_description = "x" * 10000
        many_keywords = [f"keyword{i}" for i in range(1000)]

        metadata = DocumentMetadata(
            description=large_description,
            keywords=many_keywords,
            word_count=10000000,
            character_count=50000000,
        )

        d = metadata.to_dict()
        restored = DocumentMetadata.from_dict(d)

        assert len(restored.description) == 10000
        assert len(restored.keywords) == 1000
        assert restored.word_count == 10000000

    def test_metadata_with_zero_counts(self):
        """Test metadata handles zero counts."""
        metadata = DocumentMetadata(
            word_count=0,
            character_count=0,
            page_count=0,
        )

        assert not metadata.is_empty()  # Zero is still a value
        d = metadata.to_dict()
        assert d["word_count"] == 0
        assert d["character_count"] == 0
        assert d["page_count"] == 0

    def test_metadata_dates_at_boundaries(self):
        """Test metadata handles date boundary values."""
        # Very old date
        old_date = datetime(1900, 1, 1, 0, 0, 0)
        # Future date
        future_date = datetime(2100, 12, 31, 23, 59, 59)

        metadata = DocumentMetadata(
            date_created=old_date,
            date_modified=future_date,
        )

        d = metadata.to_dict()
        restored = DocumentMetadata.from_dict(d)

        assert restored.date_created == old_date
        assert restored.date_modified == future_date


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
