import os
import zipfile
from defusedxml import minidom
from xml.dom.minidom import Document

from typing import BinaryIO, Any, Dict, List

from ._html_converter import HtmlConverter
from .._base_converter import DocumentConverterResult
from .._stream_info import StreamInfo
from .._conversion_quality import (
    ConversionQuality,
    FormattingLossType,
    WarningSeverity,
)

ACCEPTED_MIME_TYPE_PREFIXES = [
    "application/epub",
    "application/epub+zip",
    "application/x-epub+zip",
]

ACCEPTED_FILE_EXTENSIONS = [".epub"]

MIME_TYPE_MAPPING = {
    ".html": "text/html",
    ".xhtml": "application/xhtml+xml",
}


class EpubConverter(HtmlConverter):
    """
    Converts EPUB files to Markdown. Style information (e.g.m headings) and tables are preserved where possible.
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
        # Quality tracking
        quality = ConversionQuality(confidence=0.85)
        sections_detected = 0
        sections_converted = 0
        sections_skipped = 0
        skipped_files: List[str] = []
        has_title = False
        has_authors = False
        metadata_fields_found: List[str] = []

        with zipfile.ZipFile(file_stream, "r") as z:
            # Extracts metadata (title, authors, language, publisher, date, description, cover) from an EPUB file."""

            # Locate content.opf
            container_dom = minidom.parse(z.open("META-INF/container.xml"))
            opf_path = container_dom.getElementsByTagName("rootfile")[0].getAttribute(
                "full-path"
            )

            # Parse content.opf
            opf_dom = minidom.parse(z.open(opf_path))
            metadata: Dict[str, Any] = {
                "title": self._get_text_from_node(opf_dom, "dc:title"),
                "authors": self._get_all_texts_from_nodes(opf_dom, "dc:creator"),
                "language": self._get_text_from_node(opf_dom, "dc:language"),
                "publisher": self._get_text_from_node(opf_dom, "dc:publisher"),
                "date": self._get_text_from_node(opf_dom, "dc:date"),
                "description": self._get_text_from_node(opf_dom, "dc:description"),
                "identifier": self._get_text_from_node(opf_dom, "dc:identifier"),
            }

            # Track metadata fields found
            for key, value in metadata.items():
                if value:
                    metadata_fields_found.append(key)
                    if key == "title":
                        has_title = True
                    elif key == "authors":
                        has_authors = True

            # Extract manifest items (ID → href mapping)
            manifest = {
                item.getAttribute("id"): item.getAttribute("href")
                for item in opf_dom.getElementsByTagName("item")
            }

            # Extract spine order (ID refs)
            spine_items = opf_dom.getElementsByTagName("itemref")
            spine_order = [item.getAttribute("idref") for item in spine_items]
            sections_detected = len(spine_order)

            # Convert spine order to actual file paths
            base_path = "/".join(
                opf_path.split("/")[:-1]
            )  # Get base directory of content.opf
            spine = [
                f"{base_path}/{manifest[item_id]}" if base_path else manifest[item_id]
                for item_id in spine_order
                if item_id in manifest
            ]

            # Extract and convert the content
            markdown_content: List[str] = []
            for file in spine:
                if file in z.namelist():
                    try:
                        with z.open(file) as f:
                            filename = os.path.basename(file)
                            extension = os.path.splitext(filename)[1].lower()
                            mimetype = MIME_TYPE_MAPPING.get(extension)
                            converted_content = self._html_converter.convert(
                                f,
                                StreamInfo(
                                    mimetype=mimetype,
                                    extension=extension,
                                    filename=filename,
                                ),
                            )
                            markdown_content.append(converted_content.markdown.strip())
                            sections_converted += 1
                    except Exception as e:
                        sections_skipped += 1
                        skipped_files.append(os.path.basename(file))
                        quality.add_warning(
                            f"Failed to convert section '{os.path.basename(file)}': {e}",
                            severity=WarningSeverity.MEDIUM,
                        )
                else:
                    sections_skipped += 1
                    skipped_files.append(os.path.basename(file))

            # Format and add the metadata
            metadata_markdown = []
            for key, value in metadata.items():
                if isinstance(value, list):
                    value = ", ".join(value)
                if value:
                    metadata_markdown.append(f"**{key.capitalize()}:** {value}")

            markdown_content.insert(0, "\n".join(metadata_markdown))

            # Build quality report
            quality.set_metric("sections_detected", sections_detected)
            quality.set_metric("sections_converted", sections_converted)
            quality.set_metric("sections_skipped", sections_skipped)
            quality.set_metric("metadata_fields", metadata_fields_found)
            quality.set_metric("manifest_items", len(manifest))

            if sections_skipped > 0:
                quality.add_warning(
                    f"{sections_skipped} section(s) could not be found or converted: {', '.join(skipped_files[:5])}{'...' if len(skipped_files) > 5 else ''}",
                    severity=WarningSeverity.MEDIUM,
                    element_count=sections_skipped,
                )
                quality.is_partial = True
                quality.completion_percentage = (
                    (sections_converted / sections_detected * 100)
                    if sections_detected > 0
                    else 0
                )

            if not has_title:
                quality.add_warning(
                    "EPUB title metadata is missing.",
                    severity=WarningSeverity.LOW,
                )

            if not has_authors:
                quality.add_warning(
                    "EPUB author metadata is missing.",
                    severity=WarningSeverity.LOW,
                )

            # EPUB-specific notes
            quality.add_warning(
                "EPUB CSS styling is not preserved in markdown output.",
                severity=WarningSeverity.INFO,
                formatting_type=FormattingLossType.CUSTOM_STYLE,
            )

            quality.add_warning(
                "Embedded images are not extracted from the EPUB.",
                severity=WarningSeverity.INFO,
                formatting_type=FormattingLossType.IMAGE,
            )

            quality.add_warning(
                "Table of contents navigation structure is not preserved.",
                severity=WarningSeverity.INFO,
                formatting_type=FormattingLossType.TOC,
            )

            quality.add_formatting_loss(FormattingLossType.PAGE_BREAK)
            quality.add_formatting_loss(FormattingLossType.FONT_STYLE)

            # Adjust confidence based on conversion success
            if sections_detected > 0:
                conversion_ratio = sections_converted / sections_detected
                quality.confidence = max(0.4, min(0.95, 0.5 + (conversion_ratio * 0.45)))

            return DocumentConverterResult(
                markdown="\n\n".join(markdown_content),
                title=metadata["title"],
                quality=quality,
            )

    def _get_text_from_node(self, dom: Document, tag_name: str) -> str | None:
        """Convenience function to extract a single occurrence of a tag (e.g., title)."""
        texts = self._get_all_texts_from_nodes(dom, tag_name)
        if len(texts) > 0:
            return texts[0]
        else:
            return None

    def _get_all_texts_from_nodes(self, dom: Document, tag_name: str) -> List[str]:
        """Helper function to extract all occurrences of a tag (e.g., multiple authors)."""
        texts: List[str] = []
        for node in dom.getElementsByTagName(tag_name):
            if node.firstChild and hasattr(node.firstChild, "nodeValue"):
                texts.append(node.firstChild.nodeValue.strip())
        return texts
