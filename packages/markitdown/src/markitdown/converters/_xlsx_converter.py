import sys
from typing import BinaryIO, Any
from ._html_converter import HtmlConverter
from .._base_converter import DocumentConverter, DocumentConverterResult
from .._exceptions import MissingDependencyException, MISSING_DEPENDENCY_MESSAGE
from .._stream_info import StreamInfo
from .._conversion_quality import (
    ConversionQuality,
    FormattingLossType,
    WarningSeverity,
)

# Try loading optional (but in this case, required) dependencies
# Save reporting of any exceptions for later
_xlsx_dependency_exc_info = None
try:
    import pandas as pd
    import openpyxl  # noqa: F401
except ImportError:
    _xlsx_dependency_exc_info = sys.exc_info()

_xls_dependency_exc_info = None
try:
    import pandas as pd  # noqa: F811
    import xlrd  # noqa: F401
except ImportError:
    _xls_dependency_exc_info = sys.exc_info()

ACCEPTED_XLSX_MIME_TYPE_PREFIXES = [
    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
]
ACCEPTED_XLSX_FILE_EXTENSIONS = [".xlsx"]

ACCEPTED_XLS_MIME_TYPE_PREFIXES = [
    "application/vnd.ms-excel",
    "application/excel",
]
ACCEPTED_XLS_FILE_EXTENSIONS = [".xls"]


class XlsxConverter(DocumentConverter):
    """
    Converts XLSX files to Markdown, with each sheet presented as a separate Markdown table.
    """

    def __init__(self):
        super().__init__()
        self._html_converter = HtmlConverter()

    def accepts(
        self,
        file_stream: BinaryIO,
        stream_info: StreamInfo,
        **kwargs: Any,  # Options to pass to the converter
    ) -> bool:
        mimetype = (stream_info.mimetype or "").lower()
        extension = (stream_info.extension or "").lower()

        if extension in ACCEPTED_XLSX_FILE_EXTENSIONS:
            return True

        for prefix in ACCEPTED_XLSX_MIME_TYPE_PREFIXES:
            if mimetype.startswith(prefix):
                return True

        return False

    def convert(
        self,
        file_stream: BinaryIO,
        stream_info: StreamInfo,
        **kwargs: Any,  # Options to pass to the converter
    ) -> DocumentConverterResult:
        # Check the dependencies
        if _xlsx_dependency_exc_info is not None:
            raise MissingDependencyException(
                MISSING_DEPENDENCY_MESSAGE.format(
                    converter=type(self).__name__,
                    extension=".xlsx",
                    feature="xlsx",
                )
            ) from _xlsx_dependency_exc_info[
                1
            ].with_traceback(  # type: ignore[union-attr]
                _xlsx_dependency_exc_info[2]
            )

        sheets = pd.read_excel(file_stream, sheet_name=None, engine="openpyxl")
        md_content = ""

        # Quality tracking
        quality = ConversionQuality(confidence=0.85)
        sheet_count = len(sheets)
        total_rows = 0
        total_cols = 0

        for s in sheets:
            md_content += f"## {s}\n"
            df = sheets[s]
            total_rows += len(df)
            total_cols = max(total_cols, len(df.columns))

            html_content = df.to_html(index=False)
            md_content += (
                self._html_converter.convert_string(
                    html_content, **kwargs
                ).markdown.strip()
                + "\n\n"
            )

        # Set metrics
        quality.set_metric("sheet_count", sheet_count)
        quality.set_metric("total_rows", total_rows)
        quality.set_metric("max_columns", total_cols)

        # XLSX formatting that is lost
        quality.add_warning(
            "Cell formatting (colors, fonts, borders) is not preserved.",
            severity=WarningSeverity.LOW,
            formatting_type=FormattingLossType.TABLE_FORMATTING,
        )

        quality.add_warning(
            "Formulas are converted to their calculated values only.",
            severity=WarningSeverity.MEDIUM,
            formatting_type=FormattingLossType.SPREADSHEET_FORMULA,
        )

        quality.add_warning(
            "Merged cells are not represented in markdown tables.",
            severity=WarningSeverity.MEDIUM,
            formatting_type=FormattingLossType.TABLE_FORMATTING,
        )

        quality.add_formatting_loss(FormattingLossType.TEXT_COLOR)
        quality.add_formatting_loss(FormattingLossType.CHART)
        quality.add_formatting_loss(FormattingLossType.IMAGE)

        quality.add_warning(
            "Charts and images embedded in the spreadsheet are not extracted.",
            severity=WarningSeverity.MEDIUM,
            formatting_type=FormattingLossType.CHART,
        )

        return DocumentConverterResult(markdown=md_content.strip(), quality=quality)


class XlsConverter(DocumentConverter):
    """
    Converts XLS files to Markdown, with each sheet presented as a separate Markdown table.
    """

    def __init__(self):
        super().__init__()
        self._html_converter = HtmlConverter()

    def accepts(
        self,
        file_stream: BinaryIO,
        stream_info: StreamInfo,
        **kwargs: Any,  # Options to pass to the converter
    ) -> bool:
        mimetype = (stream_info.mimetype or "").lower()
        extension = (stream_info.extension or "").lower()

        if extension in ACCEPTED_XLS_FILE_EXTENSIONS:
            return True

        for prefix in ACCEPTED_XLS_MIME_TYPE_PREFIXES:
            if mimetype.startswith(prefix):
                return True

        return False

    def convert(
        self,
        file_stream: BinaryIO,
        stream_info: StreamInfo,
        **kwargs: Any,  # Options to pass to the converter
    ) -> DocumentConverterResult:
        # Load the dependencies
        if _xls_dependency_exc_info is not None:
            raise MissingDependencyException(
                MISSING_DEPENDENCY_MESSAGE.format(
                    converter=type(self).__name__,
                    extension=".xls",
                    feature="xls",
                )
            ) from _xls_dependency_exc_info[
                1
            ].with_traceback(  # type: ignore[union-attr]
                _xls_dependency_exc_info[2]
            )

        sheets = pd.read_excel(file_stream, sheet_name=None, engine="xlrd")
        md_content = ""

        # Quality tracking
        quality = ConversionQuality(confidence=0.80)  # Slightly lower for legacy format
        sheet_count = len(sheets)
        total_rows = 0
        total_cols = 0

        for s in sheets:
            md_content += f"## {s}\n"
            df = sheets[s]
            total_rows += len(df)
            total_cols = max(total_cols, len(df.columns))

            html_content = df.to_html(index=False)
            md_content += (
                self._html_converter.convert_string(
                    html_content, **kwargs
                ).markdown.strip()
                + "\n\n"
            )

        # Set metrics
        quality.set_metric("sheet_count", sheet_count)
        quality.set_metric("total_rows", total_rows)
        quality.set_metric("max_columns", total_cols)

        # XLS formatting that is lost
        quality.add_warning(
            "Cell formatting (colors, fonts, borders) is not preserved.",
            severity=WarningSeverity.LOW,
            formatting_type=FormattingLossType.TABLE_FORMATTING,
        )

        quality.add_warning(
            "Formulas are converted to their calculated values only.",
            severity=WarningSeverity.MEDIUM,
            formatting_type=FormattingLossType.SPREADSHEET_FORMULA,
        )

        quality.add_warning(
            "Merged cells are not represented in markdown tables.",
            severity=WarningSeverity.MEDIUM,
            formatting_type=FormattingLossType.TABLE_FORMATTING,
        )

        quality.add_warning(
            "Legacy XLS format may have limited feature support compared to XLSX.",
            severity=WarningSeverity.INFO,
        )

        quality.add_formatting_loss(FormattingLossType.TEXT_COLOR)
        quality.add_formatting_loss(FormattingLossType.CHART)
        quality.add_formatting_loss(FormattingLossType.IMAGE)

        return DocumentConverterResult(markdown=md_content.strip(), quality=quality)
