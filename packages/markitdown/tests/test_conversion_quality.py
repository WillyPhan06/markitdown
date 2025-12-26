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


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
