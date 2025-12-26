import csv
import io
from typing import BinaryIO, Any
from charset_normalizer import from_bytes
from .._base_converter import DocumentConverter, DocumentConverterResult
from .._stream_info import StreamInfo
from .._conversion_quality import (
    ConversionQuality,
    FormattingLossType,
    WarningSeverity,
)

ACCEPTED_MIME_TYPE_PREFIXES = [
    "text/csv",
    "application/csv",
]
ACCEPTED_FILE_EXTENSIONS = [".csv"]


class CsvConverter(DocumentConverter):
    """
    Converts CSV files to Markdown tables.
    """

    def __init__(self):
        super().__init__()

    def accepts(
        self,
        file_stream: BinaryIO,
        stream_info: StreamInfo,
        **kwargs: Any,  # Options to pass to the converter
    ) -> bool:
        mimetype = (stream_info.mimetype or "").lower()
        extension = (stream_info.extension or "").lower()
        if extension in ACCEPTED_FILE_EXTENSIONS:
            return True
        for prefix in ACCEPTED_MIME_TYPE_PREFIXES:
            if mimetype.startswith(prefix):
                return True
        return False

    def convert(
        self,
        file_stream: BinaryIO,
        stream_info: StreamInfo,
        **kwargs: Any,  # Options to pass to the converter
    ) -> DocumentConverterResult:
        # Quality tracking
        quality = ConversionQuality(confidence=0.95)
        charset_used = None
        charset_detected = False
        rows_with_missing_columns = 0
        rows_with_extra_columns = 0

        # Read the file content
        raw_content = file_stream.read()
        if stream_info.charset:
            charset_used = stream_info.charset
            content = raw_content.decode(stream_info.charset)
        else:
            charset_result = from_bytes(raw_content).best()
            if charset_result is not None:
                charset_used = charset_result.encoding
                charset_detected = True
                content = str(charset_result)
            else:
                # Fallback to utf-8
                charset_used = "utf-8"
                content = raw_content.decode("utf-8", errors="replace")
                quality.add_warning(
                    "Could not detect charset, falling back to UTF-8 with error replacement.",
                    severity=WarningSeverity.MEDIUM,
                )

        # Record charset info
        quality.set_metric("charset", charset_used)
        quality.set_metric("charset_detected", charset_detected)

        # Parse CSV content
        reader = csv.reader(io.StringIO(content))
        rows = list(reader)

        if not rows:
            quality.confidence = 0.5
            quality.add_warning(
                "CSV file appears to be empty.",
                severity=WarningSeverity.HIGH,
            )
            return DocumentConverterResult(markdown="", quality=quality)

        # Track column count from header
        header_col_count = len(rows[0])
        quality.set_metric("column_count", header_col_count)
        quality.set_metric("row_count", len(rows) - 1)  # Excluding header

        # Create markdown table
        markdown_table = []

        # Add header row
        markdown_table.append("| " + " | ".join(rows[0]) + " |")

        # Add separator row
        markdown_table.append("| " + " | ".join(["---"] * len(rows[0])) + " |")

        # Add data rows
        for row in rows[1:]:
            original_len = len(row)

            # Make sure row has the same number of columns as header
            if original_len < header_col_count:
                rows_with_missing_columns += 1
                while len(row) < header_col_count:
                    row.append("")

            # Truncate if row has more columns than header
            if original_len > header_col_count:
                rows_with_extra_columns += 1
                row = row[:header_col_count]

            markdown_table.append("| " + " | ".join(row) + " |")

        result = "\n".join(markdown_table)

        # Add warnings for column mismatches
        if rows_with_missing_columns > 0:
            quality.add_warning(
                f"{rows_with_missing_columns} row(s) had fewer columns than the header. Empty cells were added.",
                severity=WarningSeverity.LOW,
                formatting_type=FormattingLossType.TABLE_FORMATTING,
                element_count=rows_with_missing_columns,
            )

        if rows_with_extra_columns > 0:
            quality.add_warning(
                f"{rows_with_extra_columns} row(s) had more columns than the header. Extra columns were truncated.",
                severity=WarningSeverity.MEDIUM,
                formatting_type=FormattingLossType.TABLE_FORMATTING,
                element_count=rows_with_extra_columns,
            )
            # Lower confidence when data is truncated
            quality.confidence = max(0.7, quality.confidence - 0.1)

        # Standard CSV formatting notes
        quality.add_warning(
            "Cell formatting (alignment, width) is not preserved in markdown tables.",
            severity=WarningSeverity.INFO,
            formatting_type=FormattingLossType.TABLE_FORMATTING,
        )

        quality.set_metric("rows_with_missing_columns", rows_with_missing_columns)
        quality.set_metric("rows_with_extra_columns", rows_with_extra_columns)

        return DocumentConverterResult(markdown=result, quality=quality)
