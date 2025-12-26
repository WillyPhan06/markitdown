import re
import bs4
from typing import Any, BinaryIO

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


class WikipediaConverter(DocumentConverter):
    """Handle Wikipedia pages separately, focusing only on the main document content."""

    def accepts(
        self,
        file_stream: BinaryIO,
        stream_info: StreamInfo,
        **kwargs: Any,  # Options to pass to the converter
    ) -> bool:
        """
        Make sure we're dealing with HTML content *from* Wikipedia.
        """

        url = stream_info.url or ""
        mimetype = (stream_info.mimetype or "").lower()
        extension = (stream_info.extension or "").lower()

        if not re.search(r"^https?:\/\/[a-zA-Z]{2,3}\.wikipedia.org\/", url):
            # Not a Wikipedia URL
            return False

        if extension in ACCEPTED_FILE_EXTENSIONS:
            return True

        for prefix in ACCEPTED_MIME_TYPE_PREFIXES:
            if mimetype.startswith(prefix):
                return True

        # Not HTML content
        return False

    def convert(
        self,
        file_stream: BinaryIO,
        stream_info: StreamInfo,
        **kwargs: Any,  # Options to pass to the converter
    ) -> DocumentConverterResult:
        # Quality tracking
        quality = ConversionQuality(confidence=0.85)
        main_content_found = False
        sections_detected = 0
        infobox_found = False
        references_found = False
        images_detected = 0

        # Parse the stream
        encoding = "utf-8" if stream_info.charset is None else stream_info.charset
        soup = bs4.BeautifulSoup(file_stream, "html.parser", from_encoding=encoding)

        # Remove javascript and style blocks
        for script in soup(["script", "style"]):
            script.extract()

        # Print only the main content
        body_elm = soup.find("div", {"id": "mw-content-text"})
        title_elm = soup.find("span", {"class": "mw-page-title-main"})

        webpage_text = ""
        main_title = None if soup.title is None else soup.title.string

        if body_elm:
            main_content_found = True

            # What's the title
            if title_elm and isinstance(title_elm, bs4.Tag):
                main_title = title_elm.string

            # Count sections (headings)
            if isinstance(body_elm, bs4.Tag):
                for heading_level in ["h2", "h3", "h4"]:
                    sections_detected += len(body_elm.find_all(heading_level))

                # Check for infobox
                infobox = body_elm.find("table", {"class": "infobox"})
                if infobox:
                    infobox_found = True

                # Count images
                images_detected = len(body_elm.find_all("img"))

                # Check for references section
                ref_section = body_elm.find("div", {"class": "reflist"})
                if ref_section:
                    references_found = True

            # Convert the page
            webpage_text = f"# {main_title}\n\n" + _CustomMarkdownify(
                **kwargs
            ).convert_soup(body_elm)
        else:
            webpage_text = _CustomMarkdownify(**kwargs).convert_soup(soup)
            quality.add_warning(
                "Main content div (mw-content-text) not found. Full page was converted.",
                severity=WarningSeverity.MEDIUM,
            )
            quality.confidence = 0.6

        # Build quality report
        quality.set_metric("main_content_found", main_content_found)
        quality.set_metric("sections_detected", sections_detected)
        quality.set_metric("images_detected", images_detected)
        quality.set_metric("has_infobox", infobox_found)
        quality.set_metric("has_references", references_found)

        if not main_title:
            quality.add_warning(
                "Article title could not be extracted.",
                severity=WarningSeverity.LOW,
            )

        if infobox_found:
            quality.add_warning(
                "Infobox detected. Table formatting may not be fully preserved.",
                severity=WarningSeverity.INFO,
                formatting_type=FormattingLossType.TABLE_FORMATTING,
            )

        if images_detected > 0:
            quality.add_warning(
                f"Found {images_detected} image(s). Image descriptions may not be fully captured.",
                severity=WarningSeverity.INFO,
                formatting_type=FormattingLossType.IMAGE,
                element_count=images_detected,
            )

        # Wikipedia-specific notes
        quality.add_warning(
            "Wikipedia navigation elements, sidebars, and edit links are excluded.",
            severity=WarningSeverity.INFO,
        )

        quality.add_warning(
            "References and citations are converted to plain text.",
            severity=WarningSeverity.INFO,
            formatting_type=FormattingLossType.FOOTNOTE,
        )

        quality.add_formatting_loss(FormattingLossType.HYPERLINK)  # External links simplified

        return DocumentConverterResult(
            markdown=webpage_text,
            title=main_title,
            quality=quality,
        )
