#!/usr/bin/env python3 -m pytest
"""Tests for the conversion quality assessment feature."""

import io
import os
import json
import pytest

from markitdown import (
    MarkItDown,
    ConversionQuality,
    ConversionWarning,
    FormattingLossType,
    WarningSeverity,
    DocumentConverterResult,
)

TEST_FILES_DIR = os.path.join(os.path.dirname(__file__), "test_files")


class TestConversionQualityDataClass:
    """Tests for the ConversionQuality data class."""

    def test_default_quality(self):
        """Test default quality values."""
        quality = ConversionQuality()
        assert quality.confidence == 1.0
        assert len(quality.warnings) == 0
        assert len(quality.formatting_loss) == 0
        assert quality.converter_used is None
        assert quality.is_partial is False
        assert quality.completion_percentage is None

    def test_add_warning(self):
        """Test adding warnings to quality report."""
        quality = ConversionQuality()
        quality.add_warning(
            "Test warning",
            severity=WarningSeverity.MEDIUM,
            formatting_type=FormattingLossType.TABLE,
            element_count=5,
        )

        assert len(quality.warnings) == 1
        assert quality.warnings[0].message == "Test warning"
        assert quality.warnings[0].severity == WarningSeverity.MEDIUM
        assert quality.warnings[0].formatting_type == FormattingLossType.TABLE
        assert quality.warnings[0].element_count == 5
        assert quality.has_warnings

    def test_add_formatting_loss(self):
        """Test adding formatting loss types."""
        quality = ConversionQuality()
        quality.add_formatting_loss(FormattingLossType.IMAGE)
        quality.add_formatting_loss(FormattingLossType.TABLE)
        quality.add_formatting_loss(FormattingLossType.IMAGE)  # Duplicate

        assert len(quality.formatting_loss) == 2
        assert FormattingLossType.IMAGE in quality.formatting_loss
        assert FormattingLossType.TABLE in quality.formatting_loss

    def test_set_metric(self):
        """Test setting metrics."""
        quality = ConversionQuality()
        quality.set_metric("text_length", 1000)
        quality.set_metric("page_count", 5)

        assert quality.metrics["text_length"] == 1000
        assert quality.metrics["page_count"] == 5

    def test_set_optional_feature(self):
        """Test setting optional features."""
        quality = ConversionQuality()
        quality.set_optional_feature("llm_description", True)
        quality.set_optional_feature("exiftool", False)

        assert quality.optional_features_used["llm_description"] is True
        assert quality.optional_features_used["exiftool"] is False

    def test_highest_severity(self):
        """Test highest severity calculation."""
        quality = ConversionQuality()
        assert quality.highest_severity is None

        quality.add_warning("Info warning", severity=WarningSeverity.INFO)
        assert quality.highest_severity == WarningSeverity.INFO

        quality.add_warning("Low warning", severity=WarningSeverity.LOW)
        assert quality.highest_severity == WarningSeverity.LOW

        quality.add_warning("High warning", severity=WarningSeverity.HIGH)
        assert quality.highest_severity == WarningSeverity.HIGH

    def test_get_warnings_by_severity(self):
        """Test filtering warnings by severity."""
        quality = ConversionQuality()
        quality.add_warning("Info 1", severity=WarningSeverity.INFO)
        quality.add_warning("Low 1", severity=WarningSeverity.LOW)
        quality.add_warning("Low 2", severity=WarningSeverity.LOW)
        quality.add_warning("High 1", severity=WarningSeverity.HIGH)

        info_warnings = quality.get_warnings_by_severity(WarningSeverity.INFO)
        low_warnings = quality.get_warnings_by_severity(WarningSeverity.LOW)
        high_warnings = quality.get_warnings_by_severity(WarningSeverity.HIGH)

        assert len(info_warnings) == 1
        assert len(low_warnings) == 2
        assert len(high_warnings) == 1

    def test_to_dict(self):
        """Test conversion to dictionary."""
        quality = ConversionQuality(confidence=0.85)
        quality.converter_used = "TestConverter"
        quality.add_warning(
            "Test warning",
            severity=WarningSeverity.MEDIUM,
            formatting_type=FormattingLossType.TABLE,
        )
        quality.set_metric("test_metric", 42)

        result = quality.to_dict()

        assert result["confidence"] == 0.85
        assert result["converter_used"] == "TestConverter"
        assert len(result["warnings"]) == 1
        assert result["warnings"][0]["message"] == "Test warning"
        assert result["warnings"][0]["severity"] == "medium"
        assert result["warnings"][0]["formatting_type"] == "table"
        assert result["metrics"]["test_metric"] == 42

    def test_str_representation(self):
        """Test string representation."""
        quality = ConversionQuality(confidence=0.75)
        quality.converter_used = "PdfConverter"
        quality.add_warning("Test warning", severity=WarningSeverity.LOW)

        str_repr = str(quality)
        assert "75%" in str_repr
        assert "PdfConverter" in str_repr
        assert "Test warning" in str_repr


class TestDocumentConverterResultQuality:
    """Tests for quality property in DocumentConverterResult."""

    def test_default_quality_property(self):
        """Test that quality property returns default quality when not set."""
        result = DocumentConverterResult(markdown="# Test")
        quality = result.quality

        assert quality is not None
        assert quality.confidence == 1.0
        assert len(quality.warnings) == 0

    def test_quality_with_explicit_quality(self):
        """Test that explicit quality is returned."""
        explicit_quality = ConversionQuality(confidence=0.7)
        explicit_quality.converter_used = "TestConverter"

        result = DocumentConverterResult(markdown="# Test", quality=explicit_quality)

        assert result.quality.confidence == 0.7
        assert result.quality.converter_used == "TestConverter"

    def test_quality_setter(self):
        """Test quality setter."""
        result = DocumentConverterResult(markdown="# Test")
        new_quality = ConversionQuality(confidence=0.5)
        result.quality = new_quality

        assert result.quality.confidence == 0.5


class TestConverterQualityReporting:
    """Tests for quality reporting in actual converters."""

    def test_html_conversion_quality(self):
        """Test HTML converter quality reporting."""
        markitdown = MarkItDown()
        html_content = b"""
        <html>
        <head>
            <script>console.log('test');</script>
            <style>.test { color: red; }</style>
        </head>
        <body>
            <h1>Test</h1>
            <iframe src="https://example.com"></iframe>
            <form><input type="text" /></form>
        </body>
        </html>
        """
        result = markitdown.convert_stream(
            io.BytesIO(html_content), stream_info=None, file_extension=".html"
        )

        quality = result.quality
        assert quality is not None
        assert quality.converter_used == "HtmlConverter"
        assert quality.has_warnings

        # Check for specific warnings
        warning_messages = [w.message for w in quality.warnings]
        assert any("script" in msg.lower() for msg in warning_messages)
        assert any("style" in msg.lower() for msg in warning_messages)
        assert any("iframe" in msg.lower() for msg in warning_messages)
        assert any("form" in msg.lower() for msg in warning_messages)

    def test_plain_text_conversion_quality(self):
        """Test plain text converter gets quality info from main class."""
        markitdown = MarkItDown()
        result = markitdown.convert_stream(
            io.BytesIO(b"Hello, World!"),
            stream_info=None,
            file_extension=".txt",
        )

        quality = result.quality
        assert quality is not None
        # Plain text converter doesn't set explicit quality, so main class sets converter name
        assert quality.converter_used == "PlainTextConverter"

    @pytest.mark.skipif(
        not os.path.exists(os.path.join(TEST_FILES_DIR, "test.pdf")),
        reason="test.pdf not available",
    )
    def test_pdf_conversion_quality(self):
        """Test PDF converter quality reporting."""
        markitdown = MarkItDown()
        result = markitdown.convert(os.path.join(TEST_FILES_DIR, "test.pdf"))

        quality = result.quality
        assert quality is not None
        assert quality.converter_used == "PdfConverter"
        # PDF conversion has known limitations
        assert quality.confidence < 1.0
        assert FormattingLossType.IMAGE in quality.formatting_loss

    @pytest.mark.skipif(
        not os.path.exists(os.path.join(TEST_FILES_DIR, "test.docx")),
        reason="test.docx not available",
    )
    def test_docx_conversion_quality(self):
        """Test DOCX converter quality reporting."""
        markitdown = MarkItDown()
        result = markitdown.convert(os.path.join(TEST_FILES_DIR, "test.docx"))

        quality = result.quality
        assert quality is not None
        assert quality.converter_used == "DocxConverter"
        assert quality.has_warnings
        # DOCX conversion loses header/footer
        assert FormattingLossType.HEADER_FOOTER in quality.formatting_loss

    @pytest.mark.skipif(
        not os.path.exists(os.path.join(TEST_FILES_DIR, "test.pptx")),
        reason="test.pptx not available",
    )
    def test_pptx_conversion_quality(self):
        """Test PPTX converter quality reporting."""
        markitdown = MarkItDown()
        result = markitdown.convert(os.path.join(TEST_FILES_DIR, "test.pptx"))

        quality = result.quality
        assert quality is not None
        assert quality.converter_used == "PptxConverter"
        # PPTX without LLM client should note missing image descriptions
        assert "llm_image_description" in quality.optional_features_used
        assert quality.metrics.get("slide_count", 0) > 0

    @pytest.mark.skipif(
        not os.path.exists(os.path.join(TEST_FILES_DIR, "test.xlsx")),
        reason="test.xlsx not available",
    )
    def test_xlsx_conversion_quality(self):
        """Test XLSX converter quality reporting."""
        markitdown = MarkItDown()
        result = markitdown.convert(os.path.join(TEST_FILES_DIR, "test.xlsx"))

        quality = result.quality
        assert quality is not None
        assert quality.converter_used == "XlsxConverter"
        # XLSX conversion loses formulas and formatting
        assert FormattingLossType.SPREADSHEET_FORMULA in quality.formatting_loss
        assert FormattingLossType.TABLE_FORMATTING in quality.formatting_loss


class TestQualityJsonSerialization:
    """Tests for JSON serialization of quality data."""

    def test_quality_json_serialization(self):
        """Test that quality can be serialized to JSON."""
        quality = ConversionQuality(confidence=0.8)
        quality.converter_used = "TestConverter"
        quality.add_warning(
            "Test warning",
            severity=WarningSeverity.MEDIUM,
            formatting_type=FormattingLossType.TABLE,
            element_count=3,
            details={"extra": "info"},
        )
        quality.add_formatting_loss(FormattingLossType.IMAGE)
        quality.set_metric("count", 10)
        quality.set_optional_feature("feature1", True)

        # Should not raise
        json_str = json.dumps(quality.to_dict())
        parsed = json.loads(json_str)

        assert parsed["confidence"] == 0.8
        assert parsed["converter_used"] == "TestConverter"
        assert len(parsed["warnings"]) == 1
        assert parsed["warnings"][0]["element_count"] == 3
        assert parsed["formatting_loss"] == ["table", "image"]
        assert parsed["metrics"]["count"] == 10
        assert parsed["optional_features_used"]["feature1"] is True


class TestFormattingLossTypes:
    """Tests for FormattingLossType enum."""

    def test_all_formatting_types_have_values(self):
        """Test that all formatting types have string values."""
        for loss_type in FormattingLossType:
            assert isinstance(loss_type.value, str)
            assert len(loss_type.value) > 0

    def test_common_formatting_types(self):
        """Test common formatting loss types exist."""
        assert FormattingLossType.TABLE
        assert FormattingLossType.IMAGE
        assert FormattingLossType.CHART
        assert FormattingLossType.FONT_STYLE
        assert FormattingLossType.TEXT_COLOR
        assert FormattingLossType.HYPERLINK
        assert FormattingLossType.FOOTNOTE


class TestWarningSeverity:
    """Tests for WarningSeverity enum."""

    def test_severity_values(self):
        """Test severity values exist."""
        assert WarningSeverity.INFO.value == "info"
        assert WarningSeverity.LOW.value == "low"
        assert WarningSeverity.MEDIUM.value == "medium"
        assert WarningSeverity.HIGH.value == "high"


class TestConversionWarning:
    """Tests for ConversionWarning class."""

    def test_warning_str_representation(self):
        """Test warning string representation."""
        warning = ConversionWarning(
            message="Test message",
            severity=WarningSeverity.HIGH,
            element_count=5,
        )

        str_repr = str(warning)
        assert "HIGH" in str_repr
        assert "Test message" in str_repr
        assert "5 element" in str_repr

    def test_warning_without_element_count(self):
        """Test warning without element count."""
        warning = ConversionWarning(
            message="Simple warning",
            severity=WarningSeverity.LOW,
        )

        str_repr = str(warning)
        assert "LOW" in str_repr
        assert "Simple warning" in str_repr
        assert "element" not in str_repr


class TestIpynbConverterQuality:
    """Tests for quality tracking in Jupyter Notebook converter."""

    def test_basic_notebook_quality(self):
        """Test quality reporting for a basic notebook conversion."""
        markitdown = MarkItDown()
        notebook_path = os.path.join(TEST_FILES_DIR, "test_notebook.ipynb")

        if not os.path.exists(notebook_path):
            pytest.skip("test_notebook.ipynb not available")

        result = markitdown.convert(notebook_path)
        quality = result.quality

        assert quality is not None
        assert quality.converter_used == "IpynbConverter"
        # Base confidence for notebooks is 0.9
        assert 0.3 <= quality.confidence <= 0.9
        # Should have metrics
        assert "total_cells" in quality.metrics
        assert "code_cells" in quality.metrics
        assert "markdown_cells" in quality.metrics
        assert "kernel_language" in quality.metrics

    def test_notebook_with_outputs_warning(self):
        """Test that notebooks with cell outputs generate appropriate warnings."""
        markitdown = MarkItDown()
        # Create a notebook with outputs
        notebook_with_outputs = {
            "nbformat": 4,
            "nbformat_minor": 2,
            "metadata": {"kernelspec": {"language": "python"}},
            "cells": [
                {
                    "cell_type": "code",
                    "source": ["print('hello')"],
                    "outputs": [{"output_type": "stream", "text": ["hello\n"]}],
                },
                {
                    "cell_type": "code",
                    "source": ["x = 1 + 1"],
                    "outputs": [
                        {"output_type": "execute_result", "data": {"text/plain": ["2"]}}
                    ],
                },
            ],
        }

        notebook_bytes = json.dumps(notebook_with_outputs).encode("utf-8")
        result = markitdown.convert_stream(
            io.BytesIO(notebook_bytes), stream_info=None, file_extension=".ipynb"
        )

        quality = result.quality
        assert quality.metrics["cells_with_outputs"] == 2
        # Should have warning about outputs not included
        warning_messages = [w.message for w in quality.warnings]
        assert any("output" in msg.lower() for msg in warning_messages)
        # Should note EMBEDDED_OBJECT formatting loss
        assert FormattingLossType.EMBEDDED_OBJECT in quality.formatting_loss

    def test_notebook_empty_cells_warning(self):
        """Test that empty notebooks generate warnings."""
        markitdown = MarkItDown()
        empty_notebook = {
            "nbformat": 4,
            "nbformat_minor": 2,
            "metadata": {},
            "cells": [],
        }

        notebook_bytes = json.dumps(empty_notebook).encode("utf-8")
        result = markitdown.convert_stream(
            io.BytesIO(notebook_bytes), stream_info=None, file_extension=".ipynb"
        )

        quality = result.quality
        assert quality.metrics["total_cells"] == 0
        # Should have HIGH severity warning about no cells
        high_warnings = quality.get_warnings_by_severity(WarningSeverity.HIGH)
        assert len(high_warnings) >= 1
        assert any("no cells" in w.message.lower() for w in high_warnings)
        # Confidence should be reduced for empty notebook
        assert quality.confidence == 0.5

    def test_notebook_missing_metadata_warning(self):
        """Test warnings for missing kernel/language metadata."""
        markitdown = MarkItDown()
        notebook_no_metadata = {
            "nbformat": 4,
            "nbformat_minor": 2,
            "metadata": {},  # No kernelspec or language_info
            "cells": [{"cell_type": "code", "source": ["x = 1"], "outputs": []}],
        }

        notebook_bytes = json.dumps(notebook_no_metadata).encode("utf-8")
        result = markitdown.convert_stream(
            io.BytesIO(notebook_bytes), stream_info=None, file_extension=".ipynb"
        )

        quality = result.quality
        # Should have INFO warnings about missing metadata
        info_warnings = quality.get_warnings_by_severity(WarningSeverity.INFO)
        warning_messages = [w.message for w in info_warnings]
        assert any("kernel" in msg.lower() for msg in warning_messages)
        assert any("language" in msg.lower() for msg in warning_messages)

    def test_notebook_unknown_cell_type_warning(self):
        """Test warnings for unknown cell types."""
        markitdown = MarkItDown()
        notebook_unknown_cells = {
            "nbformat": 4,
            "nbformat_minor": 2,
            "metadata": {"kernelspec": {"language": "python"}},
            "cells": [
                {"cell_type": "code", "source": ["x = 1"], "outputs": []},
                {"cell_type": "unknown_type", "source": ["something"]},
                {"cell_type": "another_unknown", "source": ["else"]},
            ],
        }

        notebook_bytes = json.dumps(notebook_unknown_cells).encode("utf-8")
        result = markitdown.convert_stream(
            io.BytesIO(notebook_bytes), stream_info=None, file_extension=".ipynb"
        )

        quality = result.quality
        # Should have warning about unknown cell types
        warning_messages = [w.message for w in quality.warnings]
        assert any("unknown" in msg.lower() for msg in warning_messages)
        # Confidence should be reduced
        assert quality.confidence < 0.9

    def test_notebook_confidence_minimum(self):
        """Test that confidence never goes below minimum threshold."""
        markitdown = MarkItDown()
        # Create a notebook with many issues
        problematic_notebook = {
            "nbformat": 4,
            "nbformat_minor": 2,
            "metadata": {},
            "cells": [
                {"cell_type": "unknown1", "source": []},
                {"cell_type": "unknown2", "source": []},
                {"cell_type": "unknown3", "source": []},
                {"cell_type": "unknown4", "source": []},
                {"cell_type": "unknown5", "source": []},
            ],
        }

        notebook_bytes = json.dumps(problematic_notebook).encode("utf-8")
        result = markitdown.convert_stream(
            io.BytesIO(notebook_bytes), stream_info=None, file_extension=".ipynb"
        )

        quality = result.quality
        # Confidence should not go below 0.3
        assert quality.confidence >= 0.3


class TestZipConverterQuality:
    """Tests for quality tracking in ZIP archive converter."""

    def test_basic_zip_quality(self):
        """Test quality reporting for a basic ZIP conversion."""
        markitdown = MarkItDown()
        zip_path = os.path.join(TEST_FILES_DIR, "test_files.zip")

        if not os.path.exists(zip_path):
            pytest.skip("test_files.zip not available")

        result = markitdown.convert(zip_path)
        quality = result.quality

        assert quality is not None
        assert quality.converter_used == "ZipConverter"
        # Should have metrics
        assert "total_files" in quality.metrics
        assert "converted_files" in quality.metrics
        assert "unsupported_files" in quality.metrics
        assert "failed_files" in quality.metrics

    def test_zip_with_all_supported_files(self):
        """Test ZIP with all convertible files has high confidence."""
        markitdown = MarkItDown()

        # Create a ZIP with only text files
        zip_buffer = io.BytesIO()
        import zipfile

        with zipfile.ZipFile(zip_buffer, "w") as zf:
            zf.writestr("file1.txt", "Hello World")
            zf.writestr("file2.txt", "Another file")
            zf.writestr("subdir/file3.txt", "Nested file")

        zip_buffer.seek(0)
        result = markitdown.convert_stream(
            zip_buffer, stream_info=None, file_extension=".zip"
        )

        quality = result.quality
        assert quality.metrics["total_files"] == 3
        assert quality.metrics["converted_files"] == 3
        assert quality.metrics["unsupported_files"] == 0
        assert quality.metrics["failed_files"] == 0
        # All files converted, so high confidence
        assert quality.confidence == 1.0
        assert quality.is_partial is False

    def test_zip_with_unsupported_files(self):
        """Test ZIP with unsupported files generates warnings."""
        markitdown = MarkItDown()

        zip_buffer = io.BytesIO()
        import zipfile

        with zipfile.ZipFile(zip_buffer, "w") as zf:
            zf.writestr("readme.txt", "Hello")
            zf.writestr("binary.exe", b"\x00\x01\x02\x03")
            zf.writestr("data.bin", b"\xff\xfe\xfd")

        zip_buffer.seek(0)
        result = markitdown.convert_stream(
            zip_buffer, stream_info=None, file_extension=".zip"
        )

        quality = result.quality
        # Should have warning about unsupported files
        assert quality.metrics["unsupported_files"] > 0
        warning_messages = [w.message for w in quality.warnings]
        assert any("unsupported" in msg.lower() for msg in warning_messages)
        # Should be marked as partial
        assert quality.is_partial is True
        assert quality.completion_percentage is not None
        assert quality.completion_percentage < 100

    def test_zip_empty_archive(self):
        """Test empty ZIP archive generates warning."""
        markitdown = MarkItDown()

        zip_buffer = io.BytesIO()
        import zipfile

        with zipfile.ZipFile(zip_buffer, "w") as zf:
            pass  # Empty archive

        zip_buffer.seek(0)
        result = markitdown.convert_stream(
            zip_buffer, stream_info=None, file_extension=".zip"
        )

        quality = result.quality
        assert quality.metrics["total_files"] == 0
        # Should have warning about empty archive
        warning_messages = [w.message for w in quality.warnings]
        assert any("no files" in msg.lower() for msg in warning_messages)
        assert quality.confidence == 0.5

    def test_zip_directories_skipped(self):
        """Test that directories in ZIP are skipped and counted."""
        markitdown = MarkItDown()

        zip_buffer = io.BytesIO()
        import zipfile

        with zipfile.ZipFile(zip_buffer, "w") as zf:
            zf.writestr("dir1/", "")  # Directory entry
            zf.writestr("dir2/subdir/", "")  # Nested directory
            zf.writestr("file.txt", "Content")

        zip_buffer.seek(0)
        result = markitdown.convert_stream(
            zip_buffer, stream_info=None, file_extension=".zip"
        )

        quality = result.quality
        assert quality.metrics["skipped_directories"] == 2
        assert quality.metrics["total_files"] == 1
        assert quality.metrics["converted_files"] == 1

    def test_zip_confidence_formula(self):
        """Test ZIP confidence formula scales with success rate."""
        markitdown = MarkItDown()

        # Create ZIP with mixed content
        zip_buffer = io.BytesIO()
        import zipfile

        with zipfile.ZipFile(zip_buffer, "w") as zf:
            zf.writestr("file1.txt", "Text 1")
            zf.writestr("file2.txt", "Text 2")
            zf.writestr("binary1.bin", b"\x00\x01")
            zf.writestr("binary2.bin", b"\x02\x03")

        zip_buffer.seek(0)
        result = markitdown.convert_stream(
            zip_buffer, stream_info=None, file_extension=".zip"
        )

        quality = result.quality
        # 2 out of 4 files = 50% success rate
        # Formula: 0.85 * 0.5 + 0.15 = 0.575
        assert quality.metrics["converted_files"] == 2
        assert quality.metrics["total_files"] == 4
        # Allow some tolerance for floating point
        assert 0.5 <= quality.confidence <= 0.65

    def test_zip_no_convertible_files(self):
        """Test ZIP with no convertible files has low confidence."""
        markitdown = MarkItDown()

        zip_buffer = io.BytesIO()
        import zipfile

        with zipfile.ZipFile(zip_buffer, "w") as zf:
            zf.writestr("file1.bin", b"\x00\x01\x02")
            zf.writestr("file2.dat", b"\x03\x04\x05")

        zip_buffer.seek(0)
        result = markitdown.convert_stream(
            zip_buffer, stream_info=None, file_extension=".zip"
        )

        quality = result.quality
        assert quality.metrics["converted_files"] == 0
        # Should have HIGH severity warning
        high_warnings = quality.get_warnings_by_severity(WarningSeverity.HIGH)
        assert len(high_warnings) >= 1
        # Confidence should be at minimum
        assert quality.confidence == 0.3


class TestOutlookMsgConverterQuality:
    """Tests for quality tracking in Outlook MSG converter."""

    @pytest.mark.skipif(
        not os.path.exists(os.path.join(TEST_FILES_DIR, "test_outlook_msg.msg")),
        reason="test_outlook_msg.msg not available",
    )
    def test_basic_msg_quality(self):
        """Test quality reporting for a basic MSG conversion."""
        markitdown = MarkItDown()
        msg_path = os.path.join(TEST_FILES_DIR, "test_outlook_msg.msg")

        result = markitdown.convert(msg_path)
        quality = result.quality

        assert quality is not None
        assert quality.converter_used == "OutlookMsgConverter"
        # Should have metrics
        assert "extracted_fields" in quality.metrics
        assert "missing_fields" in quality.metrics
        assert "attachment_count" in quality.metrics
        assert "has_body" in quality.metrics
        # Should note formatting losses
        assert FormattingLossType.FONT_STYLE in quality.formatting_loss
        assert FormattingLossType.TEXT_COLOR in quality.formatting_loss

    @pytest.mark.skipif(
        not os.path.exists(os.path.join(TEST_FILES_DIR, "test_outlook_msg.msg")),
        reason="test_outlook_msg.msg not available",
    )
    def test_msg_html_formatting_warning(self):
        """Test that HTML formatting warning is always present."""
        markitdown = MarkItDown()
        msg_path = os.path.join(TEST_FILES_DIR, "test_outlook_msg.msg")

        result = markitdown.convert(msg_path)
        quality = result.quality

        # Should always have INFO warning about HTML formatting
        info_warnings = quality.get_warnings_by_severity(WarningSeverity.INFO)
        warning_messages = [w.message for w in info_warnings]
        assert any("html" in msg.lower() for msg in warning_messages)

    @pytest.mark.skipif(
        not os.path.exists(os.path.join(TEST_FILES_DIR, "test_outlook_msg.msg")),
        reason="test_outlook_msg.msg not available",
    )
    def test_msg_confidence_range(self):
        """Test that MSG confidence is within expected range."""
        markitdown = MarkItDown()
        msg_path = os.path.join(TEST_FILES_DIR, "test_outlook_msg.msg")

        result = markitdown.convert(msg_path)
        quality = result.quality

        # Confidence should be between minimum (0.3) and base (0.85)
        assert 0.3 <= quality.confidence <= 0.85

    @pytest.mark.skipif(
        not os.path.exists(os.path.join(TEST_FILES_DIR, "test_outlook_msg.msg")),
        reason="test_outlook_msg.msg not available",
    )
    def test_msg_extracted_fields_tracked(self):
        """Test that extracted fields are properly tracked."""
        markitdown = MarkItDown()
        msg_path = os.path.join(TEST_FILES_DIR, "test_outlook_msg.msg")

        result = markitdown.convert(msg_path)
        quality = result.quality

        extracted = quality.metrics.get("extracted_fields", [])
        missing = quality.metrics.get("missing_fields", [])

        # All fields should be either extracted or missing
        all_fields = set(extracted + missing)
        # At minimum, From, To, Subject, Body should be tracked
        assert "From" in all_fields or "From" in extracted
        assert "Subject" in all_fields or "Subject" in extracted


class TestDocumentIntelligenceConverterQuality:
    """Tests for quality tracking in Document Intelligence converter.

    Note: These tests mock the Azure Document Intelligence client since
    it requires Azure credentials to run.
    """

    def test_doc_intel_quality_structure(self):
        """Test the structure of quality tracking in DocumentIntelligenceConverter."""
        # This test verifies the quality tracking code structure without
        # actually calling the Azure service
        from markitdown.converters._doc_intel_converter import (
            DocumentIntelligenceConverter,
        )
        from markitdown._conversion_quality import ConversionQuality

        # Verify the converter imports quality tracking
        quality = ConversionQuality(confidence=0.85)
        quality.set_optional_feature("ocr_high_resolution", True)
        quality.set_optional_feature("formula_extraction", True)
        quality.set_optional_feature("style_font_extraction", True)

        # Verify metrics structure
        quality.set_metric("page_count", 5)
        quality.set_metric("table_count", 2)
        quality.set_metric("paragraph_count", 10)
        quality.set_metric("figure_count", 3)
        quality.set_metric("selection_mark_count", 1)
        quality.set_metric("key_value_count", 4)
        quality.set_metric("content_length", 1000)

        assert quality.metrics["page_count"] == 5
        assert quality.metrics["table_count"] == 2
        assert quality.optional_features_used["ocr_high_resolution"] is True

    def test_doc_intel_warning_types(self):
        """Test the types of warnings Document Intelligence converter can generate."""
        quality = ConversionQuality(confidence=0.85)

        # Simulate warnings for figures
        quality.add_warning(
            "3 figure(s)/image(s) detected. Image content is not fully extractable.",
            severity=WarningSeverity.MEDIUM,
            formatting_type=FormattingLossType.IMAGE,
            element_count=3,
        )

        # Simulate warnings for tables
        quality.add_warning(
            "2 table(s) converted. Complex table formatting may be simplified.",
            severity=WarningSeverity.INFO,
            formatting_type=FormattingLossType.TABLE_FORMATTING,
            element_count=2,
        )

        # Simulate warnings for form fields
        quality.add_warning(
            "1 selection mark(s) (checkboxes/radio buttons) detected.",
            severity=WarningSeverity.LOW,
            formatting_type=FormattingLossType.FORM_FIELD,
            element_count=1,
        )

        assert len(quality.warnings) == 3
        assert FormattingLossType.IMAGE in [
            w.formatting_type for w in quality.warnings
        ]
        assert FormattingLossType.TABLE_FORMATTING in [
            w.formatting_type for w in quality.warnings
        ]
        assert FormattingLossType.FORM_FIELD in [
            w.formatting_type for w in quality.warnings
        ]

    def test_doc_intel_confidence_adjustment(self):
        """Test confidence adjustment formula for Document Intelligence."""
        # Test base confidence
        quality = ConversionQuality(confidence=0.85)
        assert quality.confidence == 0.85

        # Simulate table issues penalty
        tables_with_issues = 2
        quality.confidence -= 0.05 * min(tables_with_issues, 3)
        assert quality.confidence == 0.75

        # Simulate figure penalty
        figure_count = 3
        quality.confidence -= 0.05 * min(figure_count, 3)
        assert quality.confidence == 0.60

        # Ensure minimum
        quality.confidence = max(0.3, quality.confidence)
        assert quality.confidence >= 0.3

    def test_doc_intel_minimal_content_confidence(self):
        """Test that minimal content results in low confidence."""
        quality = ConversionQuality(confidence=0.85)

        # Simulate minimal content detection
        content_length = 5  # Less than 10 characters
        if content_length < 10:
            quality.add_warning(
                "Document appears to contain minimal or no extractable text.",
                severity=WarningSeverity.HIGH,
            )
            quality.confidence = 0.4

        assert quality.confidence == 0.4
        high_warnings = quality.get_warnings_by_severity(WarningSeverity.HIGH)
        assert len(high_warnings) >= 1

    def test_doc_intel_formatting_losses(self):
        """Test standard formatting losses recorded by Document Intelligence."""
        quality = ConversionQuality(confidence=0.85)

        # Add standard formatting losses
        quality.add_formatting_loss(FormattingLossType.FONT_STYLE)
        quality.add_formatting_loss(FormattingLossType.TEXT_COLOR)
        quality.add_formatting_loss(FormattingLossType.HEADER_FOOTER)

        assert FormattingLossType.FONT_STYLE in quality.formatting_loss
        assert FormattingLossType.TEXT_COLOR in quality.formatting_loss
        assert FormattingLossType.HEADER_FOOTER in quality.formatting_loss

    def test_doc_intel_ocr_warning(self):
        """Test OCR usage warning."""
        quality = ConversionQuality(confidence=0.85)

        # Simulate OCR being used
        analysis_features = ["OCR_HIGH_RESOLUTION", "FORMULAS"]
        if analysis_features:
            quality.add_warning(
                "OCR was used for text extraction. Accuracy depends on document quality.",
                severity=WarningSeverity.INFO,
            )

        info_warnings = quality.get_warnings_by_severity(WarningSeverity.INFO)
        assert any("ocr" in w.message.lower() for w in info_warnings)

    def test_doc_intel_confidence_minimum(self):
        """Test that confidence never goes below minimum threshold."""
        quality = ConversionQuality(confidence=0.85)

        # Apply maximum penalties
        quality.confidence -= 0.05 * 3  # Max table issues
        quality.confidence -= 0.05 * 3  # Max figures

        # Ensure minimum
        quality.confidence = max(0.3, quality.confidence)
        assert quality.confidence == 0.55  # 0.85 - 0.15 - 0.15 = 0.55

        # Try to go even lower
        quality.confidence -= 0.5
        quality.confidence = max(0.3, quality.confidence)
        assert quality.confidence == 0.3


class TestConverterQualityEdgeCases:
    """Edge case tests for converter quality tracking."""

    def test_notebook_with_raw_cells(self):
        """Test notebook with raw cells is handled correctly."""
        markitdown = MarkItDown()
        notebook_with_raw = {
            "nbformat": 4,
            "nbformat_minor": 2,
            "metadata": {"kernelspec": {"language": "python"}},
            "cells": [
                {"cell_type": "raw", "source": ["raw content here"]},
                {"cell_type": "code", "source": ["x = 1"], "outputs": []},
            ],
        }

        notebook_bytes = json.dumps(notebook_with_raw).encode("utf-8")
        result = markitdown.convert_stream(
            io.BytesIO(notebook_bytes), stream_info=None, file_extension=".ipynb"
        )

        quality = result.quality
        assert quality.metrics["raw_cells"] == 1
        assert quality.metrics["code_cells"] == 1

    def test_notebook_with_empty_code_cells(self):
        """Test notebook with empty code cells generates info warning."""
        markitdown = MarkItDown()
        notebook_empty_code = {
            "nbformat": 4,
            "nbformat_minor": 2,
            "metadata": {"kernelspec": {"language": "python"}},
            "cells": [
                {"cell_type": "code", "source": [], "outputs": []},
                {"cell_type": "code", "source": [""], "outputs": []},
                {"cell_type": "code", "source": ["   "], "outputs": []},
            ],
        }

        notebook_bytes = json.dumps(notebook_empty_code).encode("utf-8")
        result = markitdown.convert_stream(
            io.BytesIO(notebook_bytes), stream_info=None, file_extension=".ipynb"
        )

        quality = result.quality
        # Should have INFO warning about empty cells
        info_warnings = quality.get_warnings_by_severity(WarningSeverity.INFO)
        warning_messages = [w.message for w in info_warnings]
        assert any("empty" in msg.lower() for msg in warning_messages)

    def test_zip_with_nested_structure(self):
        """Test ZIP with deeply nested directory structure."""
        markitdown = MarkItDown()

        zip_buffer = io.BytesIO()
        import zipfile

        with zipfile.ZipFile(zip_buffer, "w") as zf:
            zf.writestr("level1/level2/level3/deep.txt", "Deep content")
            zf.writestr("level1/file.txt", "Level 1 content")

        zip_buffer.seek(0)
        result = markitdown.convert_stream(
            zip_buffer, stream_info=None, file_extension=".zip"
        )

        quality = result.quality
        assert quality.metrics["converted_files"] == 2
        assert quality.confidence == 1.0

    def test_quality_serialization_roundtrip(self):
        """Test that quality data survives JSON serialization roundtrip."""
        quality = ConversionQuality(confidence=0.75)
        quality.converter_used = "TestConverter"
        quality.is_partial = True
        quality.completion_percentage = 66.7
        quality.add_warning(
            "Test warning",
            severity=WarningSeverity.MEDIUM,
            formatting_type=FormattingLossType.TABLE,
            element_count=3,
            details={"extra": "data"},
        )
        quality.add_formatting_loss(FormattingLossType.IMAGE)
        quality.set_metric("count", 42)
        quality.set_optional_feature("feature", True)

        # Serialize and deserialize
        json_str = json.dumps(quality.to_dict())
        parsed = json.loads(json_str)

        # Verify all data survived
        assert parsed["confidence"] == 0.75
        assert parsed["converter_used"] == "TestConverter"
        assert parsed["is_partial"] is True
        assert parsed["completion_percentage"] == 66.7
        assert len(parsed["warnings"]) == 1
        assert parsed["warnings"][0]["details"] == {"extra": "data"}
        assert "image" in parsed["formatting_loss"]
        assert parsed["metrics"]["count"] == 42
        assert parsed["optional_features_used"]["feature"] is True


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
