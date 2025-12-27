import sys
from typing import Any, Union, BinaryIO, List
from .._stream_info import StreamInfo
from .._base_converter import DocumentConverter, DocumentConverterResult
from .._exceptions import MissingDependencyException, MISSING_DEPENDENCY_MESSAGE
from .._conversion_quality import (
    ConversionQuality,
    FormattingLossType,
    WarningSeverity,
)

# Try loading optional (but in this case, required) dependencies
# Save reporting of any exceptions for later
_dependency_exc_info = None
olefile = None
try:
    import olefile  # type: ignore[no-redef]
except ImportError:
    # Preserve the error and stack trace for later
    _dependency_exc_info = sys.exc_info()

ACCEPTED_MIME_TYPE_PREFIXES = [
    "application/vnd.ms-outlook",
]

ACCEPTED_FILE_EXTENSIONS = [".msg"]


class OutlookMsgConverter(DocumentConverter):
    """Converts Outlook .msg files to markdown by extracting email metadata and content.

    Uses the olefile package to parse the .msg file structure and extract:
    - Email headers (From, To, Subject)
    - Email body content
    """

    def accepts(
        self,
        file_stream: BinaryIO,
        stream_info: StreamInfo,
        **kwargs: Any,  # Options to pass to the converter
    ) -> bool:
        mimetype = (stream_info.mimetype or "").lower()
        extension = (stream_info.extension or "").lower()

        # Check the extension and mimetype
        if extension in ACCEPTED_FILE_EXTENSIONS:
            return True

        for prefix in ACCEPTED_MIME_TYPE_PREFIXES:
            if mimetype.startswith(prefix):
                return True

        # Brute force, check if we have an OLE file
        cur_pos = file_stream.tell()
        try:
            if olefile and not olefile.isOleFile(file_stream):
                return False
        finally:
            file_stream.seek(cur_pos)

        # Brue force, check if it's an Outlook file
        try:
            if olefile is not None:
                msg = olefile.OleFileIO(file_stream)
                toc = "\n".join([str(stream) for stream in msg.listdir()])
                return (
                    "__properties_version1.0" in toc
                    and "__recip_version1.0_#00000000" in toc
                )
        except Exception as e:
            pass
        finally:
            file_stream.seek(cur_pos)

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
                    extension=".msg",
                    feature="outlook",
                )
            ) from _dependency_exc_info[
                1
            ].with_traceback(  # type: ignore[union-attr]
                _dependency_exc_info[2]
            )

        assert (
            olefile is not None
        )  # If we made it this far, olefile should be available
        msg = olefile.OleFileIO(file_stream)

        # Quality tracking
        quality = ConversionQuality(confidence=0.85)

        # Track what was successfully extracted
        missing_fields: List[str] = []
        extracted_fields: List[str] = []

        # Extract email metadata
        md_content = "# Email Message\n\n"

        # Get headers
        headers = {
            "From": self._get_stream_data(msg, "__substg1.0_0C1F001F"),
            "To": self._get_stream_data(msg, "__substg1.0_0E04001F"),
            "Subject": self._get_stream_data(msg, "__substg1.0_0037001F"),
        }

        # Track header extraction success
        for key, value in headers.items():
            if value:
                extracted_fields.append(key)
                md_content += f"**{key}:** {value}\n"
            else:
                missing_fields.append(key)

        # Try to get additional headers (CC, Date)
        cc = self._get_stream_data(msg, "__substg1.0_0E03001F")
        if cc:
            extracted_fields.append("CC")
            md_content += f"**CC:** {cc}\n"

        date = self._get_stream_data(msg, "__substg1.0_0039001F")
        if date:
            extracted_fields.append("Date")
            md_content += f"**Date:** {date}\n"

        md_content += "\n## Content\n\n"

        # Get email body
        body = self._get_stream_data(msg, "__substg1.0_1000001F")
        has_body = False
        if body:
            md_content += body
            has_body = True
            extracted_fields.append("Body")
        else:
            missing_fields.append("Body")

        # Check for attachments
        attachment_count = 0
        attachment_names: List[str] = []
        try:
            # Look for attachment streams
            for entry in msg.listdir():
                entry_path = "/".join(entry)
                if "__attach_version1.0" in entry_path:
                    attachment_count += 1
                    # Try to get attachment filename
                    attach_prefix = "/".join(entry[:2]) if len(entry) >= 2 else ""
                    if attach_prefix:
                        attach_name = self._get_stream_data(
                            msg, f"{attach_prefix}/__substg1.0_3707001F"
                        )
                        if attach_name:
                            attachment_names.append(attach_name)
        except Exception:
            pass

        # Get unique attachment count from the names we successfully extracted
        # This is more reliable than estimating from stream counts
        unique_attachments = len(set(attachment_names)) if attachment_names else 0

        msg.close()

        # Build quality report
        quality.set_metric("extracted_fields", extracted_fields)
        quality.set_metric("missing_fields", missing_fields)
        quality.set_metric("attachment_count", unique_attachments)
        quality.set_metric("has_body", has_body)

        # Warnings for missing critical fields
        if "From" in missing_fields:
            quality.add_warning(
                "Could not extract sender (From) field.",
                severity=WarningSeverity.MEDIUM,
            )

        if "Subject" in missing_fields:
            quality.add_warning(
                "Could not extract subject field.",
                severity=WarningSeverity.LOW,
            )

        if "Body" in missing_fields:
            quality.add_warning(
                "Could not extract email body content.",
                severity=WarningSeverity.HIGH,
            )
            quality.confidence -= 0.2

        # Warning about attachments not being converted
        if unique_attachments > 0:
            quality.add_warning(
                f"{unique_attachments} attachment(s) detected but not included in conversion.",
                severity=WarningSeverity.MEDIUM,
                formatting_type=FormattingLossType.EMBEDDED_OBJECT,
                element_count=unique_attachments,
                details={"attachment_names": attachment_names[:10]} if attachment_names else None,
            )
            quality.add_formatting_loss(FormattingLossType.EMBEDDED_OBJECT)

        # Note about formatting loss
        quality.add_warning(
            "HTML formatting in email body is converted to plain text.",
            severity=WarningSeverity.INFO,
        )
        quality.add_formatting_loss(FormattingLossType.FONT_STYLE)
        quality.add_formatting_loss(FormattingLossType.TEXT_COLOR)

        # Confidence Adjustment Formula Explanation:
        # Base confidence: 0.85 (MSG files are generally well-structured)
        #
        # Penalty for missing body (applied above): -0.2
        # - The body is the primary content of an email
        # - Without it, the conversion provides minimal value
        #
        # Penalty for critical missing fields:
        # - Critical fields are "From" and "Body" (essential email components)
        # - Penalty: 0.1 per critical field missing (max 20% reduction for both)
        # - "From" identifies the sender; without it, context is lost
        # - "Body" penalty may stack with the -0.2 above for severe cases
        # - Non-critical fields (To, Subject, CC, Date) don't reduce confidence
        #   as the email is still usable without them
        #
        # Minimum confidence: 0.3 (email structure was readable, some metadata extracted)
        if missing_fields:
            critical_missing = len([f for f in missing_fields if f in ["From", "Body"]])
            quality.confidence -= 0.1 * critical_missing

        quality.confidence = max(0.3, quality.confidence)

        return DocumentConverterResult(
            markdown=md_content.strip(),
            title=headers.get("Subject"),
            quality=quality,
        )

    def _get_stream_data(self, msg: Any, stream_path: str) -> Union[str, None]:
        """Helper to safely extract and decode stream data from the MSG file."""
        assert olefile is not None
        assert isinstance(
            msg, olefile.OleFileIO
        )  # Ensure msg is of the correct type (type hinting is not possible with the optional olefile package)

        try:
            if msg.exists(stream_path):
                data = msg.openstream(stream_path).read()
                # Try UTF-16 first (common for .msg files)
                try:
                    return data.decode("utf-16-le").strip()
                except UnicodeDecodeError:
                    # Fall back to UTF-8
                    try:
                        return data.decode("utf-8").strip()
                    except UnicodeDecodeError:
                        # Last resort - ignore errors
                        return data.decode("utf-8", errors="ignore").strip()
        except Exception:
            pass
        return None
