# SPDX-FileCopyrightText: 2024-present Adam Fourney <adamfo@microsoft.com>
#
# SPDX-License-Identifier: MIT

"""
Metadata extraction utilities for MarkItDown.

This module provides functions to extract document metadata from various file formats.
The metadata extraction is designed to work generically across all converters without
requiring changes to individual converter files.

Supported formats:
    - PDF: Title, author, subject, keywords, creation/modification dates, page count
    - DOCX: Title, author, subject, keywords, language, dates, page/word count
    - XLSX: Title, author, subject, keywords, dates
    - PPTX: Title, author, subject, keywords, dates, slide/word count
    - HTML: Title, author, description, keywords, language, date (from meta tags)
    - EPUB: Title, author, description, language, date, subjects

Design principles:
    - All metadata fields come directly from the document - nothing is fabricated
    - Extraction failures are silently handled and never interrupt conversion
    - Stream position is always restored after extraction
    - Word/character counts are computed from the converted markdown as fallback

The main entry point is extract_metadata(), which dispatches to format-specific
extractors based on file extension or MIME type.
"""

from datetime import datetime
from typing import Any, BinaryIO, Optional

from ._document_metadata import DocumentMetadata
from ._stream_info import StreamInfo


def extract_metadata(
    file_stream: BinaryIO,
    stream_info: StreamInfo,
    result_markdown: str,
    **kwargs: Any,
) -> DocumentMetadata:
    """
    Extract metadata from a document after conversion.

    This function attempts to extract metadata using format-specific extractors
    based on the stream_info (extension or MIME type). If no specific extractor
    is available, it returns basic metadata computed from the conversion result
    (word count and character count from the markdown).

    The function is designed to be safe and non-disruptive:
    - Exceptions are caught internally and never propagate
    - Stream position is always restored after extraction
    - Returns empty metadata on failure rather than raising

    Args:
        file_stream: The file stream to extract metadata from. Must support
            seek() and tell(). Position will be restored after extraction.
        stream_info: Information about the file type, including extension
            and MIME type used to select the appropriate extractor.
        result_markdown: The converted markdown text, used to compute
            word count and character count as fallback values.
        **kwargs: Additional arguments passed through (currently unused,
            reserved for future extensibility like exiftool_path).

    Returns:
        DocumentMetadata with any available metadata fields populated.
        Returns empty metadata if extraction fails completely.

    Example:
        >>> import io
        >>> from markitdown._stream_info import StreamInfo
        >>> stream = io.BytesIO(b"PDF content...")
        >>> info = StreamInfo(extension=".pdf", mimetype="application/pdf")
        >>> metadata = extract_metadata(stream, info, "# Converted markdown")
        >>> print(metadata.word_count)
        3
    """
    metadata = DocumentMetadata()

    # Get extension and mimetype for dispatcher
    extension = (stream_info.extension or "").lower()
    mimetype = (stream_info.mimetype or "").lower()

    # Save stream position
    cur_pos = file_stream.tell()

    try:
        # Try format-specific extractors
        if extension in [".pdf"] or mimetype.startswith("application/pdf"):
            metadata = _extract_pdf_metadata(file_stream)
        elif extension in [".docx"] or "wordprocessingml" in mimetype:
            metadata = _extract_docx_metadata(file_stream)
        elif extension in [".xlsx"] or "spreadsheetml" in mimetype:
            metadata = _extract_xlsx_metadata(file_stream)
        elif extension in [".pptx"] or "presentationml" in mimetype:
            metadata = _extract_pptx_metadata(file_stream)
        elif extension in [".html", ".htm"] or mimetype.startswith("text/html"):
            metadata = _extract_html_metadata(file_stream, stream_info)
        elif extension in [".epub"]:
            metadata = _extract_epub_metadata(file_stream)

        # Compute word count and character count from markdown if not already set
        if result_markdown:
            if metadata.word_count is None:
                metadata.word_count = _count_words(result_markdown)
            if metadata.character_count is None:
                metadata.character_count = _count_characters(result_markdown)

    except Exception:
        # Metadata extraction should never fail the conversion
        # Just return what we have (possibly empty)
        pass
    finally:
        # Always restore stream position
        file_stream.seek(cur_pos)

    return metadata


def _count_words(text: str) -> int:
    """
    Count words in text.

    Uses Python's str.split() which splits on any whitespace and handles
    multiple consecutive whitespace characters correctly.

    Args:
        text: The text to count words in (typically converted markdown).

    Returns:
        Number of whitespace-separated words in the text.

    Example:
        >>> _count_words("Hello world")
        2
        >>> _count_words("One   two\\nthree")
        3
        >>> _count_words("")
        0
    """
    words = text.split()
    return len(words)


def _count_characters(text: str) -> int:
    """
    Count non-whitespace characters in text.

    Excludes all Unicode whitespace characters (spaces, tabs, newlines,
    and other Unicode whitespace) to provide a meaningful character count
    that represents actual content. Uses str.isspace() for comprehensive
    Unicode whitespace detection.

    Args:
        text: The text to count characters in (typically converted markdown).

    Returns:
        Number of non-whitespace characters in the text.

    Example:
        >>> _count_characters("Hello world")
        10
        >>> _count_characters("One\\ttwo\\nthree")
        11
        >>> _count_characters("")
        0
        >>> _count_characters("   ")
        0
    """
    return sum(1 for char in text if not char.isspace())


def _extract_pdf_metadata(file_stream: BinaryIO) -> DocumentMetadata:
    """Extract metadata from a PDF file using pdfminer."""
    metadata = DocumentMetadata()

    try:
        from pdfminer.pdfparser import PDFParser
        from pdfminer.pdfdocument import PDFDocument
        from pdfminer.pdfpage import PDFPage

        file_stream.seek(0)
        parser = PDFParser(file_stream)
        doc = PDFDocument(parser)

        # Get document info
        if doc.info:
            info = doc.info[0] if isinstance(doc.info, list) else doc.info

            # Title
            if b"Title" in info:
                title = _decode_pdf_string(info[b"Title"])
                if title:
                    metadata.title = title

            # Author
            if b"Author" in info:
                author = _decode_pdf_string(info[b"Author"])
                if author:
                    metadata.author = author

            # Subject/Description
            if b"Subject" in info:
                subject = _decode_pdf_string(info[b"Subject"])
                if subject:
                    metadata.description = subject

            # Keywords
            if b"Keywords" in info:
                keywords_str = _decode_pdf_string(info[b"Keywords"])
                if keywords_str:
                    # Keywords are often comma or semicolon separated
                    keywords = [k.strip() for k in keywords_str.replace(";", ",").split(",")]
                    metadata.keywords = [k for k in keywords if k]

            # Creation date
            if b"CreationDate" in info:
                date = _parse_pdf_date(info[b"CreationDate"])
                if date:
                    metadata.date_created = date

            # Modification date
            if b"ModDate" in info:
                date = _parse_pdf_date(info[b"ModDate"])
                if date:
                    metadata.date_modified = date

        # Count pages
        file_stream.seek(0)
        page_count = sum(1 for _ in PDFPage.create_pages(doc))
        if page_count > 0:
            metadata.page_count = page_count

    except Exception:
        pass

    return metadata


def _decode_pdf_string(value: Any) -> Optional[str]:
    """Decode a PDF string value to a Python string."""
    if value is None:
        return None
    if isinstance(value, bytes):
        # Try UTF-16 first (common in PDFs), then UTF-8, then latin-1
        for encoding in ["utf-16", "utf-8", "latin-1"]:
            try:
                decoded = value.decode(encoding)
                # Remove BOM if present
                if decoded.startswith("\ufeff"):
                    decoded = decoded[1:]
                return decoded.strip() if decoded.strip() else None
            except (UnicodeDecodeError, LookupError):
                continue
        return None
    return str(value).strip() if str(value).strip() else None


def _parse_pdf_date(value: Any) -> Optional[datetime]:
    """Parse a PDF date string (D:YYYYMMDDHHmmSS format)."""
    try:
        date_str = _decode_pdf_string(value)
        if not date_str:
            return None

        # Remove D: prefix if present
        if date_str.startswith("D:"):
            date_str = date_str[2:]

        # Handle timezone offset (e.g., +05'30' or Z)
        # Strip timezone for simplicity
        for tz_marker in ["+", "-", "Z"]:
            if tz_marker in date_str:
                date_str = date_str.split(tz_marker)[0]
                break

        # Try parsing various lengths
        for fmt, length in [
            ("%Y%m%d%H%M%S", 14),
            ("%Y%m%d%H%M", 12),
            ("%Y%m%d%H", 10),
            ("%Y%m%d", 8),
            ("%Y%m", 6),
            ("%Y", 4),
        ]:
            if len(date_str) >= length:
                try:
                    return datetime.strptime(date_str[:length], fmt)
                except ValueError:
                    continue
    except Exception:
        pass
    return None


def _extract_docx_metadata(file_stream: BinaryIO) -> DocumentMetadata:
    """Extract metadata from a DOCX file."""
    metadata = DocumentMetadata()

    try:
        from zipfile import ZipFile
        import xml.etree.ElementTree as ET

        file_stream.seek(0)
        with ZipFile(file_stream, "r") as zf:
            # Core properties are in docProps/core.xml
            if "docProps/core.xml" in zf.namelist():
                with zf.open("docProps/core.xml") as core_file:
                    tree = ET.parse(core_file)
                    root = tree.getroot()

                    # Namespaces used in core.xml
                    ns = {
                        "cp": "http://schemas.openxmlformats.org/package/2006/metadata/core-properties",
                        "dc": "http://purl.org/dc/elements/1.1/",
                        "dcterms": "http://purl.org/dc/terms/",
                    }

                    # Title
                    title_elem = root.find("dc:title", ns)
                    if title_elem is not None and title_elem.text:
                        metadata.title = title_elem.text.strip()

                    # Author/Creator
                    creator_elem = root.find("dc:creator", ns)
                    if creator_elem is not None and creator_elem.text:
                        metadata.author = creator_elem.text.strip()

                    # Description/Subject
                    subject_elem = root.find("dc:subject", ns)
                    if subject_elem is not None and subject_elem.text:
                        metadata.description = subject_elem.text.strip()

                    # Keywords
                    keywords_elem = root.find("cp:keywords", ns)
                    if keywords_elem is not None and keywords_elem.text:
                        keywords = [k.strip() for k in keywords_elem.text.replace(";", ",").split(",")]
                        metadata.keywords = [k for k in keywords if k]

                    # Language
                    lang_elem = root.find("dc:language", ns)
                    if lang_elem is not None and lang_elem.text:
                        metadata.language = lang_elem.text.strip()

                    # Created date
                    created_elem = root.find("dcterms:created", ns)
                    if created_elem is not None and created_elem.text:
                        metadata.date_created = _parse_iso_date(created_elem.text)

                    # Modified date
                    modified_elem = root.find("dcterms:modified", ns)
                    if modified_elem is not None and modified_elem.text:
                        metadata.date_modified = _parse_iso_date(modified_elem.text)

            # App properties are in docProps/app.xml (for page count)
            if "docProps/app.xml" in zf.namelist():
                with zf.open("docProps/app.xml") as app_file:
                    tree = ET.parse(app_file)
                    root = tree.getroot()

                    ns = {
                        "ep": "http://schemas.openxmlformats.org/officeDocument/2006/extended-properties"
                    }

                    # Page count
                    pages_elem = root.find("ep:Pages", ns)
                    if pages_elem is not None and pages_elem.text:
                        try:
                            metadata.page_count = int(pages_elem.text)
                        except ValueError:
                            pass

                    # Word count (from app.xml is more accurate than counting)
                    words_elem = root.find("ep:Words", ns)
                    if words_elem is not None and words_elem.text:
                        try:
                            metadata.word_count = int(words_elem.text)
                        except ValueError:
                            pass

    except Exception:
        pass

    return metadata


def _extract_xlsx_metadata(file_stream: BinaryIO) -> DocumentMetadata:
    """Extract metadata from an XLSX file."""
    metadata = DocumentMetadata()

    try:
        from zipfile import ZipFile
        import xml.etree.ElementTree as ET

        file_stream.seek(0)
        with ZipFile(file_stream, "r") as zf:
            # Core properties
            if "docProps/core.xml" in zf.namelist():
                with zf.open("docProps/core.xml") as core_file:
                    tree = ET.parse(core_file)
                    root = tree.getroot()

                    ns = {
                        "cp": "http://schemas.openxmlformats.org/package/2006/metadata/core-properties",
                        "dc": "http://purl.org/dc/elements/1.1/",
                        "dcterms": "http://purl.org/dc/terms/",
                    }

                    # Title
                    title_elem = root.find("dc:title", ns)
                    if title_elem is not None and title_elem.text:
                        metadata.title = title_elem.text.strip()

                    # Author
                    creator_elem = root.find("dc:creator", ns)
                    if creator_elem is not None and creator_elem.text:
                        metadata.author = creator_elem.text.strip()

                    # Description
                    subject_elem = root.find("dc:subject", ns)
                    if subject_elem is not None and subject_elem.text:
                        metadata.description = subject_elem.text.strip()

                    # Keywords
                    keywords_elem = root.find("cp:keywords", ns)
                    if keywords_elem is not None and keywords_elem.text:
                        keywords = [k.strip() for k in keywords_elem.text.replace(";", ",").split(",")]
                        metadata.keywords = [k for k in keywords if k]

                    # Created
                    created_elem = root.find("dcterms:created", ns)
                    if created_elem is not None and created_elem.text:
                        metadata.date_created = _parse_iso_date(created_elem.text)

                    # Modified
                    modified_elem = root.find("dcterms:modified", ns)
                    if modified_elem is not None and modified_elem.text:
                        metadata.date_modified = _parse_iso_date(modified_elem.text)

    except Exception:
        pass

    return metadata


def _extract_pptx_metadata(file_stream: BinaryIO) -> DocumentMetadata:
    """Extract metadata from a PPTX file."""
    metadata = DocumentMetadata()

    try:
        from zipfile import ZipFile
        import xml.etree.ElementTree as ET

        file_stream.seek(0)
        with ZipFile(file_stream, "r") as zf:
            # Core properties
            if "docProps/core.xml" in zf.namelist():
                with zf.open("docProps/core.xml") as core_file:
                    tree = ET.parse(core_file)
                    root = tree.getroot()

                    ns = {
                        "cp": "http://schemas.openxmlformats.org/package/2006/metadata/core-properties",
                        "dc": "http://purl.org/dc/elements/1.1/",
                        "dcterms": "http://purl.org/dc/terms/",
                    }

                    # Title
                    title_elem = root.find("dc:title", ns)
                    if title_elem is not None and title_elem.text:
                        metadata.title = title_elem.text.strip()

                    # Author
                    creator_elem = root.find("dc:creator", ns)
                    if creator_elem is not None and creator_elem.text:
                        metadata.author = creator_elem.text.strip()

                    # Description
                    subject_elem = root.find("dc:subject", ns)
                    if subject_elem is not None and subject_elem.text:
                        metadata.description = subject_elem.text.strip()

                    # Keywords
                    keywords_elem = root.find("cp:keywords", ns)
                    if keywords_elem is not None and keywords_elem.text:
                        keywords = [k.strip() for k in keywords_elem.text.replace(";", ",").split(",")]
                        metadata.keywords = [k for k in keywords if k]

                    # Created
                    created_elem = root.find("dcterms:created", ns)
                    if created_elem is not None and created_elem.text:
                        metadata.date_created = _parse_iso_date(created_elem.text)

                    # Modified
                    modified_elem = root.find("dcterms:modified", ns)
                    if modified_elem is not None and modified_elem.text:
                        metadata.date_modified = _parse_iso_date(modified_elem.text)

            # App properties for slide count
            if "docProps/app.xml" in zf.namelist():
                with zf.open("docProps/app.xml") as app_file:
                    tree = ET.parse(app_file)
                    root = tree.getroot()

                    ns = {
                        "ep": "http://schemas.openxmlformats.org/officeDocument/2006/extended-properties"
                    }

                    # Slide count (use as page_count)
                    slides_elem = root.find("ep:Slides", ns)
                    if slides_elem is not None and slides_elem.text:
                        try:
                            metadata.page_count = int(slides_elem.text)
                        except ValueError:
                            pass

                    # Word count
                    words_elem = root.find("ep:Words", ns)
                    if words_elem is not None and words_elem.text:
                        try:
                            metadata.word_count = int(words_elem.text)
                        except ValueError:
                            pass

    except Exception:
        pass

    return metadata


def _extract_html_metadata(file_stream: BinaryIO, stream_info: StreamInfo) -> DocumentMetadata:
    """Extract metadata from an HTML file."""
    metadata = DocumentMetadata()

    try:
        from bs4 import BeautifulSoup

        file_stream.seek(0)
        encoding = stream_info.charset or "utf-8"
        soup = BeautifulSoup(file_stream, "html.parser", from_encoding=encoding)

        # Title
        if soup.title and soup.title.string:
            metadata.title = soup.title.string.strip()

        # Meta tags
        for meta in soup.find_all("meta"):
            name = (meta.get("name") or meta.get("property") or "").lower()
            content = meta.get("content", "")

            if not content:
                continue

            if name in ["author", "dc.creator"]:
                metadata.author = content.strip()
            elif name in ["description", "dc.description", "og:description"]:
                metadata.description = content.strip()
            elif name in ["keywords"]:
                keywords = [k.strip() for k in content.split(",")]
                metadata.keywords = [k for k in keywords if k]
            elif name in ["language", "dc.language"]:
                metadata.language = content.strip()
            elif name in ["date", "dc.date", "article:published_time"]:
                metadata.date_created = _parse_iso_date(content)

        # Language from html tag
        html_tag = soup.find("html")
        if html_tag and html_tag.get("lang") and not metadata.language:
            metadata.language = html_tag.get("lang")

    except Exception:
        pass

    return metadata


def _extract_epub_metadata(file_stream: BinaryIO) -> DocumentMetadata:
    """Extract metadata from an EPUB file."""
    metadata = DocumentMetadata()

    try:
        from zipfile import ZipFile
        import xml.etree.ElementTree as ET

        file_stream.seek(0)
        with ZipFile(file_stream, "r") as zf:
            # Find the OPF file from container.xml
            opf_path = None
            if "META-INF/container.xml" in zf.namelist():
                with zf.open("META-INF/container.xml") as container:
                    tree = ET.parse(container)
                    root = tree.getroot()
                    ns = {"c": "urn:oasis:names:tc:opendocument:xmlns:container"}
                    rootfile = root.find(".//c:rootfile", ns)
                    if rootfile is not None:
                        opf_path = rootfile.get("full-path")

            if opf_path and opf_path in zf.namelist():
                with zf.open(opf_path) as opf:
                    tree = ET.parse(opf)
                    root = tree.getroot()

                    ns = {
                        "opf": "http://www.idpf.org/2007/opf",
                        "dc": "http://purl.org/dc/elements/1.1/",
                    }

                    # Find metadata element
                    metadata_elem = root.find("opf:metadata", ns)
                    if metadata_elem is None:
                        # Try without namespace
                        metadata_elem = root.find("metadata")

                    if metadata_elem is not None:
                        # Title
                        title_elem = metadata_elem.find("dc:title", ns)
                        if title_elem is not None and title_elem.text:
                            metadata.title = title_elem.text.strip()

                        # Author
                        creator_elem = metadata_elem.find("dc:creator", ns)
                        if creator_elem is not None and creator_elem.text:
                            metadata.author = creator_elem.text.strip()

                        # Description
                        desc_elem = metadata_elem.find("dc:description", ns)
                        if desc_elem is not None and desc_elem.text:
                            metadata.description = desc_elem.text.strip()

                        # Language
                        lang_elem = metadata_elem.find("dc:language", ns)
                        if lang_elem is not None and lang_elem.text:
                            metadata.language = lang_elem.text.strip()

                        # Date
                        date_elem = metadata_elem.find("dc:date", ns)
                        if date_elem is not None and date_elem.text:
                            metadata.date_created = _parse_iso_date(date_elem.text)

                        # Subject (as keywords)
                        subjects = metadata_elem.findall("dc:subject", ns)
                        if subjects:
                            keywords = [s.text.strip() for s in subjects if s.text]
                            if keywords:
                                metadata.keywords = keywords

    except Exception:
        pass

    return metadata


def _parse_iso_date(date_str: str) -> Optional[datetime]:
    """Parse an ISO 8601 date string."""
    if not date_str:
        return None

    date_str = date_str.strip()

    # Try various ISO formats
    formats = [
        "%Y-%m-%dT%H:%M:%SZ",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%dT%H:%M:%S.%f",
        "%Y-%m-%dT%H:%M:%S.%fZ",
        "%Y-%m-%d",
        "%Y-%m",
        "%Y",
    ]

    # Handle timezone offset (e.g., +00:00 or -05:00)
    if "+" in date_str[10:] or (date_str.count("-") > 2 and "-" in date_str[10:]):
        # Strip timezone for simplicity
        for sep in ["+", "-"]:
            if sep in date_str[10:]:
                date_str = date_str[:10] + date_str[10:].split(sep)[0]
                break

    for fmt in formats:
        try:
            return datetime.strptime(date_str, fmt)
        except ValueError:
            continue

    return None
