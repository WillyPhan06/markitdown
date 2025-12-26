import sys
import io
from warnings import warn

from typing import BinaryIO, Any

from ._html_converter import HtmlConverter
from ..converter_utils.docx.pre_process import pre_process_docx
from .._base_converter import DocumentConverterResult
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
    import mammoth
    import mammoth.docx.files

    def mammoth_files_open(self, uri):
        warn("DOCX: processing of r:link resources (e.g., linked images) is disabled.")
        return io.BytesIO(b"")

    mammoth.docx.files.Files.open = mammoth_files_open

except ImportError:
    # Preserve the error and stack trace for later
    _dependency_exc_info = sys.exc_info()


ACCEPTED_MIME_TYPE_PREFIXES = [
    "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
]

ACCEPTED_FILE_EXTENSIONS = [".docx"]


class DocxConverter(HtmlConverter):
    """
    Converts DOCX files to Markdown. Style information (e.g.m headings) and tables are preserved where possible.
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
        # Check: the dependencies
        if _dependency_exc_info is not None:
            raise MissingDependencyException(
                MISSING_DEPENDENCY_MESSAGE.format(
                    converter=type(self).__name__,
                    extension=".docx",
                    feature="docx",
                )
            ) from _dependency_exc_info[
                1
            ].with_traceback(  # type: ignore[union-attr]
                _dependency_exc_info[2]
            )

        style_map = kwargs.get("style_map", None)
        pre_process_stream = pre_process_docx(file_stream)

        # Convert using mammoth and capture any messages
        mammoth_result = mammoth.convert_to_html(
            pre_process_stream, style_map=style_map
        )
        html_content = mammoth_result.value
        mammoth_messages = mammoth_result.messages

        # Convert HTML to markdown
        result = self._html_converter.convert_string(html_content, **kwargs)

        # Build quality report
        quality = ConversionQuality(confidence=0.85)

        # Process mammoth warnings/messages
        warning_count = 0
        for msg in mammoth_messages:
            warning_count += 1
            msg_text = str(msg)

            # Categorize the warning
            if "image" in msg_text.lower():
                quality.add_warning(
                    msg_text,
                    severity=WarningSeverity.MEDIUM,
                    formatting_type=FormattingLossType.IMAGE,
                )
            elif "style" in msg_text.lower():
                quality.add_warning(
                    msg_text,
                    severity=WarningSeverity.LOW,
                    formatting_type=FormattingLossType.CUSTOM_STYLE,
                )
            else:
                quality.add_warning(msg_text, severity=WarningSeverity.LOW)

        # Note about linked resources being disabled
        quality.add_warning(
            "Linked images (r:link resources) are not processed.",
            severity=WarningSeverity.LOW,
            formatting_type=FormattingLossType.IMAGE,
        )

        # Common DOCX formatting that may be lost
        quality.add_warning(
            "Header and footer content is not extracted.",
            severity=WarningSeverity.LOW,
            formatting_type=FormattingLossType.HEADER_FOOTER,
        )

        quality.add_formatting_loss(FormattingLossType.TEXT_COLOR)
        quality.add_formatting_loss(FormattingLossType.HIGHLIGHT)
        quality.add_formatting_loss(FormattingLossType.PAGE_BREAK)

        # Set metrics
        quality.set_metric("mammoth_warnings", warning_count)
        quality.set_metric("html_intermediate_length", len(html_content))

        # Adjust confidence based on number of warnings
        if warning_count > 0:
            quality.confidence = max(0.5, quality.confidence - (warning_count * 0.05))

        result._quality = quality
        return result
