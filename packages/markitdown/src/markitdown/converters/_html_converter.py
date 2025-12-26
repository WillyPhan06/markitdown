import io
from typing import Any, BinaryIO, Optional
from bs4 import BeautifulSoup

from .._base_converter import DocumentConverter, DocumentConverterResult
from .._stream_info import StreamInfo
from ._markdownify import _CustomMarkdownify
from .._conversion_quality import (
    ConversionQuality,
    FormattingLossType,
    WarningSeverity,
)

ACCEPTED_MIME_TYPE_PREFIXES = [
    "text/html",
    "application/xhtml",
]

ACCEPTED_FILE_EXTENSIONS = [
    ".html",
    ".htm",
]


class HtmlConverter(DocumentConverter):
    """Anything with content type text/html"""

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
        # Parse the stream
        encoding = "utf-8" if stream_info.charset is None else stream_info.charset
        soup = BeautifulSoup(file_stream, "html.parser", from_encoding=encoding)

        # Quality tracking
        quality = ConversionQuality(confidence=0.85)

        # Count elements that will be removed or may be lost
        script_count = len(soup.find_all("script"))
        style_count = len(soup.find_all("style"))
        iframe_count = len(soup.find_all("iframe"))
        form_count = len(soup.find_all("form"))
        canvas_count = len(soup.find_all("canvas"))
        svg_count = len(soup.find_all("svg"))

        # Remove javascript and style blocks
        for script in soup(["script", "style"]):
            script.extract()

        # Print only the main content
        body_elm = soup.find("body")
        webpage_text = ""
        if body_elm:
            webpage_text = _CustomMarkdownify(**kwargs).convert_soup(body_elm)
        else:
            webpage_text = _CustomMarkdownify(**kwargs).convert_soup(soup)

        assert isinstance(webpage_text, str)

        # remove leading and trailing \n
        webpage_text = webpage_text.strip()

        # Build quality report
        if script_count > 0:
            quality.add_warning(
                f"Removed {script_count} script block(s). Dynamic content may be missing.",
                severity=WarningSeverity.INFO,
                element_count=script_count,
            )

        if style_count > 0:
            quality.add_warning(
                f"Removed {style_count} style block(s). CSS styling is not preserved.",
                severity=WarningSeverity.INFO,
                formatting_type=FormattingLossType.CUSTOM_STYLE,
                element_count=style_count,
            )

        if iframe_count > 0:
            quality.add_warning(
                f"Found {iframe_count} iframe(s). Embedded content is not extracted.",
                severity=WarningSeverity.MEDIUM,
                formatting_type=FormattingLossType.EMBEDDED_OBJECT,
                element_count=iframe_count,
            )

        if form_count > 0:
            quality.add_warning(
                f"Found {form_count} form(s). Form elements are converted to text only.",
                severity=WarningSeverity.LOW,
                formatting_type=FormattingLossType.FORM_FIELD,
                element_count=form_count,
            )

        if canvas_count > 0:
            quality.add_warning(
                f"Found {canvas_count} canvas element(s). Canvas content cannot be converted.",
                severity=WarningSeverity.MEDIUM,
                formatting_type=FormattingLossType.EMBEDDED_OBJECT,
                element_count=canvas_count,
            )

        if svg_count > 0:
            quality.add_warning(
                f"Found {svg_count} SVG element(s). Vector graphics are not preserved.",
                severity=WarningSeverity.LOW,
                formatting_type=FormattingLossType.DIAGRAM,
                element_count=svg_count,
            )

        quality.add_formatting_loss(FormattingLossType.TEXT_COLOR)
        quality.add_formatting_loss(FormattingLossType.CUSTOM_STYLE)

        # Set metrics
        quality.set_metric("original_script_count", script_count)
        quality.set_metric("original_style_count", style_count)
        quality.set_metric("text_length", len(webpage_text))

        return DocumentConverterResult(
            markdown=webpage_text,
            title=None if soup.title is None else soup.title.string,
            quality=quality,
        )

    def convert_string(
        self, html_content: str, *, url: Optional[str] = None, **kwargs
    ) -> DocumentConverterResult:
        """
        Non-standard convenience method to convert a string to markdown.
        Given that many converters produce HTML as intermediate output, this
        allows for easy conversion of HTML to markdown.
        """
        return self.convert(
            file_stream=io.BytesIO(html_content.encode("utf-8")),
            stream_info=StreamInfo(
                mimetype="text/html",
                extension=".html",
                charset="utf-8",
                url=url,
            ),
            **kwargs,
        )
