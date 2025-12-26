import sys
import io

from typing import BinaryIO, Any


from .._base_converter import DocumentConverter, DocumentConverterResult
from .._stream_info import StreamInfo
from .._exceptions import MissingDependencyException, MISSING_DEPENDENCY_MESSAGE
from .._conversion_quality import (
    ConversionQuality,
    FormattingLossType,
    WarningSeverity,
)


# Try loading optional (but in this case, required) dependencies
# Save reporting of any exceptions for later
_dependency_exc_info = None
try:
    import pdfminer
    import pdfminer.high_level
except ImportError:
    # Preserve the error and stack trace for later
    _dependency_exc_info = sys.exc_info()


ACCEPTED_MIME_TYPE_PREFIXES = [
    "application/pdf",
    "application/x-pdf",
]

ACCEPTED_FILE_EXTENSIONS = [".pdf"]


class PdfConverter(DocumentConverter):
    """
    Converts PDFs to Markdown. Most style information is ignored, so the results are essentially plain-text.
    """

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
        # Check the dependencies
        if _dependency_exc_info is not None:
            raise MissingDependencyException(
                MISSING_DEPENDENCY_MESSAGE.format(
                    converter=type(self).__name__,
                    extension=".pdf",
                    feature="pdf",
                )
            ) from _dependency_exc_info[
                1
            ].with_traceback(  # type: ignore[union-attr]
                _dependency_exc_info[2]
            )

        assert isinstance(file_stream, io.IOBase)  # for mypy

        # Extract text from PDF
        markdown = pdfminer.high_level.extract_text(file_stream)

        # Build quality report for PDF conversion
        quality = ConversionQuality(confidence=0.7)

        # PDF conversion is essentially plain text extraction - many formatting elements are lost
        quality.add_warning(
            "PDF conversion extracts text only. Most style and formatting information is lost.",
            severity=WarningSeverity.MEDIUM,
            formatting_type=FormattingLossType.FONT_STYLE,
        )

        quality.add_formatting_loss(FormattingLossType.TEXT_COLOR)
        quality.add_formatting_loss(FormattingLossType.HIGHLIGHT)
        quality.add_formatting_loss(FormattingLossType.HEADER_FOOTER)

        # Check for potential images (we can't really detect them, but warn about it)
        quality.add_warning(
            "Images in the PDF are not extracted or described.",
            severity=WarningSeverity.MEDIUM,
            formatting_type=FormattingLossType.IMAGE,
        )

        # Tables in PDFs are typically lost
        quality.add_warning(
            "Table structures may not be preserved. Data may appear as plain text.",
            severity=WarningSeverity.MEDIUM,
            formatting_type=FormattingLossType.TABLE,
        )

        # Set metrics about the extraction
        quality.set_metric("extraction_method", "pdfminer")
        quality.set_metric("text_length", len(markdown))

        return DocumentConverterResult(markdown=markdown, quality=quality)
