from defusedxml import minidom
from xml.dom.minidom import Document, Element
from typing import BinaryIO, Any, Union
from bs4 import BeautifulSoup

from ._markdownify import _CustomMarkdownify
from .._stream_info import StreamInfo
from .._base_converter import DocumentConverter, DocumentConverterResult
from .._conversion_quality import (
    ConversionQuality,
    FormattingLossType,
    WarningSeverity,
)

PRECISE_MIME_TYPE_PREFIXES = [
    "application/rss",
    "application/rss+xml",
    "application/atom",
    "application/atom+xml",
]

PRECISE_FILE_EXTENSIONS = [".rss", ".atom"]

CANDIDATE_MIME_TYPE_PREFIXES = [
    "text/xml",
    "application/xml",
]

CANDIDATE_FILE_EXTENSIONS = [
    ".xml",
]


class RssConverter(DocumentConverter):
    """Convert RSS / Atom type to markdown"""

    def __init__(self):
        super().__init__()
        self._kwargs = {}

    def accepts(
        self,
        file_stream: BinaryIO,
        stream_info: StreamInfo,
        **kwargs: Any,  # Options to pass to the converter
    ) -> bool:
        mimetype = (stream_info.mimetype or "").lower()
        extension = (stream_info.extension or "").lower()

        # Check for precise mimetypes and file extensions
        if extension in PRECISE_FILE_EXTENSIONS:
            return True

        for prefix in PRECISE_MIME_TYPE_PREFIXES:
            if mimetype.startswith(prefix):
                return True

        # Check for precise mimetypes and file extensions
        if extension in CANDIDATE_FILE_EXTENSIONS:
            return self._check_xml(file_stream)

        for prefix in CANDIDATE_MIME_TYPE_PREFIXES:
            if mimetype.startswith(prefix):
                return self._check_xml(file_stream)

        return False

    def _check_xml(self, file_stream: BinaryIO) -> bool:
        cur_pos = file_stream.tell()
        try:
            doc = minidom.parse(file_stream)
            return self._feed_type(doc) is not None
        except BaseException as _:
            pass
        finally:
            file_stream.seek(cur_pos)
        return False

    def _feed_type(self, doc: Any) -> str | None:
        if doc.getElementsByTagName("rss"):
            return "rss"
        elif doc.getElementsByTagName("feed"):
            root = doc.getElementsByTagName("feed")[0]
            if root.getElementsByTagName("entry"):
                # An Atom feed must have a root element of <feed> and at least one <entry>
                return "atom"
        return None

    def convert(
        self,
        file_stream: BinaryIO,
        stream_info: StreamInfo,
        **kwargs: Any,  # Options to pass to the converter
    ) -> DocumentConverterResult:
        self._kwargs = kwargs
        doc = minidom.parse(file_stream)
        feed_type = self._feed_type(doc)

        if feed_type == "rss":
            return self._parse_rss_type(doc)
        elif feed_type == "atom":
            return self._parse_atom_type(doc)
        else:
            raise ValueError("Unknown feed type")

    def _parse_atom_type(self, doc: Document) -> DocumentConverterResult:
        """Parse the type of an Atom feed.

        Returns None if the feed type is not recognized or something goes wrong.
        """
        # Quality tracking
        quality = ConversionQuality(confidence=0.85)
        entries_detected = 0
        entries_extracted = 0
        entries_with_title = 0
        entries_with_content = 0
        entries_missing_fields = 0

        root = doc.getElementsByTagName("feed")[0]
        title = self._get_data_by_tag_name(root, "title")
        subtitle = self._get_data_by_tag_name(root, "subtitle")
        entries = root.getElementsByTagName("entry")
        entries_detected = len(entries)

        md_text = f"# {title}\n"
        if subtitle:
            md_text += f"{subtitle}\n"

        if not title:
            quality.add_warning(
                "Feed title is missing.",
                severity=WarningSeverity.LOW,
            )

        for entry in entries:
            entry_title = self._get_data_by_tag_name(entry, "title")
            entry_summary = self._get_data_by_tag_name(entry, "summary")
            entry_updated = self._get_data_by_tag_name(entry, "updated")
            entry_content = self._get_data_by_tag_name(entry, "content")

            # Track what was found
            has_content = False
            if entry_title:
                entries_with_title += 1
                md_text += f"\n## {entry_title}\n"
            if entry_updated:
                md_text += f"Updated on: {entry_updated}\n"
            if entry_summary:
                md_text += self._parse_content(entry_summary)
                has_content = True
            if entry_content:
                md_text += self._parse_content(entry_content)
                has_content = True

            if has_content:
                entries_with_content += 1
                entries_extracted += 1
            elif entry_title:
                entries_extracted += 1
                entries_missing_fields += 1

        # Build quality report
        quality.set_metric("feed_type", "atom")
        quality.set_metric("entries_detected", entries_detected)
        quality.set_metric("entries_extracted", entries_extracted)
        quality.set_metric("entries_with_title", entries_with_title)
        quality.set_metric("entries_with_content", entries_with_content)

        if entries_detected == 0:
            quality.add_warning(
                "No entries found in the Atom feed.",
                severity=WarningSeverity.HIGH,
            )
            quality.confidence = 0.5
        elif entries_missing_fields > 0:
            quality.add_warning(
                f"{entries_missing_fields} entry/entries missing content or summary.",
                severity=WarningSeverity.LOW,
                element_count=entries_missing_fields,
            )

        # Note about formatting
        quality.add_warning(
            "Feed entry formatting (images, embedded media) may not be fully preserved.",
            severity=WarningSeverity.INFO,
            formatting_type=FormattingLossType.EMBEDDED_OBJECT,
        )

        return DocumentConverterResult(
            markdown=md_text,
            title=title,
            quality=quality,
        )

    def _parse_rss_type(self, doc: Document) -> DocumentConverterResult:
        """Parse the type of an RSS feed.

        Returns None if the feed type is not recognized or something goes wrong.
        """
        # Quality tracking
        quality = ConversionQuality(confidence=0.85)
        items_detected = 0
        items_extracted = 0
        items_with_title = 0
        items_with_content = 0
        items_missing_fields = 0

        root = doc.getElementsByTagName("rss")[0]
        channel_list = root.getElementsByTagName("channel")
        if not channel_list:
            raise ValueError("No channel found in RSS feed")
        channel = channel_list[0]
        channel_title = self._get_data_by_tag_name(channel, "title")
        channel_description = self._get_data_by_tag_name(channel, "description")
        items = channel.getElementsByTagName("item")
        items_detected = len(items)

        md_text = ""
        if channel_title:
            md_text = f"# {channel_title}\n"
        else:
            quality.add_warning(
                "Channel title is missing.",
                severity=WarningSeverity.LOW,
            )

        if channel_description:
            md_text += f"{channel_description}\n"

        for item in items:
            title = self._get_data_by_tag_name(item, "title")
            description = self._get_data_by_tag_name(item, "description")
            pubDate = self._get_data_by_tag_name(item, "pubDate")
            content = self._get_data_by_tag_name(item, "content:encoded")

            # Track what was found
            has_content = False
            if title:
                items_with_title += 1
                md_text += f"\n## {title}\n"
            if pubDate:
                md_text += f"Published on: {pubDate}\n"
            if description:
                md_text += self._parse_content(description)
                has_content = True
            if content:
                md_text += self._parse_content(content)
                has_content = True

            if has_content:
                items_with_content += 1
                items_extracted += 1
            elif title:
                items_extracted += 1
                items_missing_fields += 1

        # Build quality report
        quality.set_metric("feed_type", "rss")
        quality.set_metric("items_detected", items_detected)
        quality.set_metric("items_extracted", items_extracted)
        quality.set_metric("items_with_title", items_with_title)
        quality.set_metric("items_with_content", items_with_content)

        if items_detected == 0:
            quality.add_warning(
                "No items found in the RSS feed.",
                severity=WarningSeverity.HIGH,
            )
            quality.confidence = 0.5
        elif items_missing_fields > 0:
            quality.add_warning(
                f"{items_missing_fields} item(s) missing description or content.",
                severity=WarningSeverity.LOW,
                element_count=items_missing_fields,
            )

        # Note about formatting
        quality.add_warning(
            "Feed item formatting (images, embedded media) may not be fully preserved.",
            severity=WarningSeverity.INFO,
            formatting_type=FormattingLossType.EMBEDDED_OBJECT,
        )

        return DocumentConverterResult(
            markdown=md_text,
            title=channel_title,
            quality=quality,
        )

    def _parse_content(self, content: str) -> str:
        """Parse the content of an RSS feed item"""
        try:
            # using bs4 because many RSS feeds have HTML-styled content
            soup = BeautifulSoup(content, "html.parser")
            return _CustomMarkdownify(**self._kwargs).convert_soup(soup)
        except BaseException as _:
            return content

    def _get_data_by_tag_name(
        self, element: Element, tag_name: str
    ) -> Union[str, None]:
        """Get data from first child element with the given tag name.
        Returns None when no such element is found.
        """
        nodes = element.getElementsByTagName(tag_name)
        if not nodes:
            return None
        fc = nodes[0].firstChild
        if fc:
            if hasattr(fc, "data"):
                return fc.data
        return None
