import sys

from typing import BinaryIO, Any
from charset_normalizer import from_bytes
from .._base_converter import DocumentConverter, DocumentConverterResult
from .._stream_info import StreamInfo
from .._conversion_quality import (
    ConversionQuality,
    WarningSeverity,
)

# Try loading optional (but in this case, required) dependencies
# Save reporting of any exceptions for later
_dependency_exc_info = None
try:
    import mammoth  # noqa: F401
except ImportError:
    # Preserve the error and stack trace for later
    _dependency_exc_info = sys.exc_info()

ACCEPTED_MIME_TYPE_PREFIXES = [
    "text/",
    "application/json",
    "application/markdown",
]

ACCEPTED_FILE_EXTENSIONS = [
    ".txt",
    ".text",
    ".md",
    ".markdown",
    ".json",
    ".jsonl",
]


class PlainTextConverter(DocumentConverter):
    """Anything with content type text/plain"""

    def accepts(
        self,
        file_stream: BinaryIO,
        stream_info: StreamInfo,
        **kwargs: Any,  # Options to pass to the converter
    ) -> bool:
        mimetype = (stream_info.mimetype or "").lower()
        extension = (stream_info.extension or "").lower()

        # If we have a charset, we can safely assume it's text
        # With Magika in the earlier stages, this handles most cases
        if stream_info.charset is not None:
            return True

        # Otherwise, check the mimetype and extension
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
        quality = ConversionQuality(confidence=1.0)
        charset_used = None
        charset_detected = False
        charset_confidence = None

        # Read raw content
        raw_content = file_stream.read()

        if stream_info.charset:
            charset_used = stream_info.charset
            try:
                text_content = raw_content.decode(stream_info.charset)
            except (UnicodeDecodeError, LookupError) as e:
                # Fallback if specified charset fails
                quality.add_warning(
                    f"Failed to decode with specified charset '{stream_info.charset}': {e}. Attempting auto-detection.",
                    severity=WarningSeverity.MEDIUM,
                )
                charset_result = from_bytes(raw_content).best()
                if charset_result is not None:
                    charset_used = charset_result.encoding
                    charset_detected = True
                    text_content = str(charset_result)
                else:
                    charset_used = "utf-8"
                    text_content = raw_content.decode("utf-8", errors="replace")
                    quality.add_warning(
                        "Could not detect charset, using UTF-8 with error replacement.",
                        severity=WarningSeverity.MEDIUM,
                    )
                    quality.confidence = 0.7
        else:
            charset_result = from_bytes(raw_content).best()
            if charset_result is not None:
                charset_used = charset_result.encoding
                charset_detected = True
                charset_confidence = (
                    charset_result.encoding_aliases
                    if hasattr(charset_result, "encoding_aliases")
                    else None
                )
                text_content = str(charset_result)
            else:
                # Fallback to utf-8
                charset_used = "utf-8"
                text_content = raw_content.decode("utf-8", errors="replace")
                quality.add_warning(
                    "Could not detect charset, falling back to UTF-8 with error replacement.",
                    severity=WarningSeverity.MEDIUM,
                )
                quality.confidence = 0.7

        # Record charset metrics
        quality.set_metric("charset", charset_used)
        quality.set_metric("charset_detected", charset_detected)
        quality.set_metric("text_length", len(text_content))
        quality.set_metric("original_size_bytes", len(raw_content))

        if charset_confidence is not None:
            quality.set_metric("charset_alternatives", charset_confidence)

        # Check for potential encoding issues (replacement characters)
        replacement_count = text_content.count("\ufffd")
        if replacement_count > 0:
            quality.add_warning(
                f"Found {replacement_count} replacement character(s) indicating encoding issues.",
                severity=WarningSeverity.MEDIUM,
                element_count=replacement_count,
            )
            quality.confidence = max(0.5, quality.confidence - 0.2)

        quality.set_metric("replacement_characters", replacement_count)

        # Note about plain text
        if charset_detected:
            quality.add_warning(
                f"Character encoding was auto-detected as '{charset_used}'.",
                severity=WarningSeverity.INFO,
            )

        return DocumentConverterResult(markdown=text_content, quality=quality)
