# SPDX-FileCopyrightText: 2024-present Adam Fourney <adamfo@microsoft.com>
#
# SPDX-License-Identifier: MIT

"""
Document metadata module for MarkItDown.

This module provides the DocumentMetadata dataclass for storing document metadata
extracted during file conversion. Metadata is automatically extracted from supported
file formats (PDF, DOCX, XLSX, PPTX, HTML, EPUB) and made available alongside the
converted markdown content.

Key principles:
- All metadata fields are optional and only populated if available in the source document
- Metadata is never fabricated or guessed - it comes directly from the document
- The metadata extraction process never interrupts the main conversion flow

Example usage:
    >>> from markitdown import MarkItDown
    >>> md = MarkItDown()
    >>> result = md.convert("document.pdf")
    >>> print(result.metadata.title)
    "My Document Title"
    >>> print(result.metadata.author)
    "John Doe"
    >>> print(result.metadata.get_date_created_formatted("%B %d, %Y"))
    "January 15, 2024"

The metadata is also included in batch conversion manifests:
    >>> result = md.convert_batch(["doc1.pdf", "doc2.docx"])
    >>> for item in result.successful_items:
    ...     print(f"{item.source_path}: {item.metadata.word_count} words")
"""

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Optional


@dataclass
class DocumentMetadata:
    """
    Metadata extracted from a document during conversion.

    All fields are optional and only populated if the information is
    available in the source document. Fields are never fabricated or guessed.

    Attributes:
        title: Document title (from document properties, not filename).
        author: Document author or creator.
        date_created: When the document was created (as datetime object).
        date_modified: When the document was last modified (as datetime object).
        language: Document language code (e.g., "en", "en-US", "fr-FR").
        page_count: Number of pages (for paginated documents like PDF, DOCX).
        word_count: Approximate word count (computed from converted markdown).
        character_count: Approximate character count excluding whitespace.
        description: Document description, subject, or abstract.
        keywords: List of keywords/tags associated with the document.
        custom: Dictionary for format-specific metadata fields not covered above.

    Date Access:
        Dates are stored as datetime objects. For convenience, helper methods
        are provided to get dates in common formats:

        - get_date_created_iso() / get_date_modified_iso() -> "2024-01-15T10:30:00"
        - get_date_created_formatted(fmt) -> custom strftime format
        - get_date_created_date_only() -> "2024-01-15"

    Serialization:
        Use to_dict() and from_dict() for JSON serialization. Dates are
        automatically converted to/from ISO 8601 strings.

    Example:
        >>> metadata = DocumentMetadata(
        ...     title="Annual Report",
        ...     author="Finance Team",
        ...     word_count=5000
        ... )
        >>> metadata.is_empty()
        False
        >>> data = metadata.to_dict()
        >>> restored = DocumentMetadata.from_dict(data)
        >>> restored.title
        "Annual Report"
    """

    title: Optional[str] = None
    author: Optional[str] = None
    date_created: Optional[datetime] = None
    date_modified: Optional[datetime] = None
    language: Optional[str] = None
    page_count: Optional[int] = None
    word_count: Optional[int] = None
    character_count: Optional[int] = None
    description: Optional[str] = None
    keywords: Optional[List[str]] = None
    custom: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        """
        Convert to a dictionary for serialization.

        Datetime objects are converted to ISO 8601 format strings.
        None values are omitted from the output to keep it clean.
        """
        result: Dict[str, Any] = {}

        if self.title is not None:
            result["title"] = self.title
        if self.author is not None:
            result["author"] = self.author
        if self.date_created is not None:
            result["date_created"] = self.date_created.isoformat()
        if self.date_modified is not None:
            result["date_modified"] = self.date_modified.isoformat()
        if self.language is not None:
            result["language"] = self.language
        if self.page_count is not None:
            result["page_count"] = self.page_count
        if self.word_count is not None:
            result["word_count"] = self.word_count
        if self.character_count is not None:
            result["character_count"] = self.character_count
        if self.description is not None:
            result["description"] = self.description
        if self.keywords is not None and len(self.keywords) > 0:
            result["keywords"] = self.keywords
        if self.custom:
            result["custom"] = self.custom

        return result

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "DocumentMetadata":
        """
        Create a DocumentMetadata from a dictionary.

        Args:
            data: Dictionary representation (as produced by to_dict()).

        Returns:
            A new DocumentMetadata instance.
        """
        # Parse datetime strings back to datetime objects
        date_created = None
        if data.get("date_created"):
            try:
                date_created = datetime.fromisoformat(data["date_created"])
            except (ValueError, TypeError):
                pass

        date_modified = None
        if data.get("date_modified"):
            try:
                date_modified = datetime.fromisoformat(data["date_modified"])
            except (ValueError, TypeError):
                pass

        return cls(
            title=data.get("title"),
            author=data.get("author"),
            date_created=date_created,
            date_modified=date_modified,
            language=data.get("language"),
            page_count=data.get("page_count"),
            word_count=data.get("word_count"),
            character_count=data.get("character_count"),
            description=data.get("description"),
            keywords=data.get("keywords"),
            custom=data.get("custom", {}),
        )

    def is_empty(self) -> bool:
        """Check if all metadata fields are empty/None."""
        return (
            self.title is None
            and self.author is None
            and self.date_created is None
            and self.date_modified is None
            and self.language is None
            and self.page_count is None
            and self.word_count is None
            and self.character_count is None
            and self.description is None
            and (self.keywords is None or len(self.keywords) == 0)
            and not self.custom
        )

    # -------------------------------------------------------------------------
    # Date helper methods
    # These methods provide consistent access to dates in various formats.
    # The underlying date_created/date_modified fields store datetime objects,
    # but these helpers make it easy to get them as ISO strings or formatted strings.
    # -------------------------------------------------------------------------

    def get_date_created_iso(self) -> Optional[str]:
        """
        Get the creation date as an ISO 8601 formatted string.

        Returns:
            ISO 8601 string (e.g., "2024-01-15T10:30:00") or None if not set.
        """
        if self.date_created is None:
            return None
        return self.date_created.isoformat()

    def get_date_modified_iso(self) -> Optional[str]:
        """
        Get the modification date as an ISO 8601 formatted string.

        Returns:
            ISO 8601 string (e.g., "2024-01-15T10:30:00") or None if not set.
        """
        if self.date_modified is None:
            return None
        return self.date_modified.isoformat()

    def get_date_created_formatted(self, fmt: str = "%Y-%m-%d %H:%M:%S") -> Optional[str]:
        """
        Get the creation date formatted with a custom format string.

        Args:
            fmt: A strftime format string. Default: "%Y-%m-%d %H:%M:%S"

        Returns:
            Formatted date string or None if not set.

        Examples:
            >>> metadata.get_date_created_formatted()  # "2024-01-15 10:30:00"
            >>> metadata.get_date_created_formatted("%B %d, %Y")  # "January 15, 2024"
            >>> metadata.get_date_created_formatted("%Y/%m/%d")  # "2024/01/15"
        """
        if self.date_created is None:
            return None
        return self.date_created.strftime(fmt)

    def get_date_modified_formatted(self, fmt: str = "%Y-%m-%d %H:%M:%S") -> Optional[str]:
        """
        Get the modification date formatted with a custom format string.

        Args:
            fmt: A strftime format string. Default: "%Y-%m-%d %H:%M:%S"

        Returns:
            Formatted date string or None if not set.

        Examples:
            >>> metadata.get_date_modified_formatted()  # "2024-01-15 10:30:00"
            >>> metadata.get_date_modified_formatted("%B %d, %Y")  # "January 15, 2024"
            >>> metadata.get_date_modified_formatted("%Y/%m/%d")  # "2024/01/15"
        """
        if self.date_modified is None:
            return None
        return self.date_modified.strftime(fmt)

    def get_date_created_date_only(self) -> Optional[str]:
        """
        Get the creation date as a date-only string (YYYY-MM-DD).

        Returns:
            Date string (e.g., "2024-01-15") or None if not set.
        """
        if self.date_created is None:
            return None
        return self.date_created.strftime("%Y-%m-%d")

    def get_date_modified_date_only(self) -> Optional[str]:
        """
        Get the modification date as a date-only string (YYYY-MM-DD).

        Returns:
            Date string (e.g., "2024-01-15") or None if not set.
        """
        if self.date_modified is None:
            return None
        return self.date_modified.strftime("%Y-%m-%d")

    def __str__(self) -> str:
        """Return a human-readable summary of the metadata."""
        lines = []

        if self.title:
            lines.append(f"Title: {self.title}")
        if self.author:
            lines.append(f"Author: {self.author}")
        if self.date_created:
            lines.append(f"Created: {self.date_created.strftime('%Y-%m-%d %H:%M:%S')}")
        if self.date_modified:
            lines.append(f"Modified: {self.date_modified.strftime('%Y-%m-%d %H:%M:%S')}")
        if self.language:
            lines.append(f"Language: {self.language}")
        if self.page_count is not None:
            lines.append(f"Pages: {self.page_count}")
        if self.word_count is not None:
            lines.append(f"Words: {self.word_count}")
        if self.character_count is not None:
            lines.append(f"Characters: {self.character_count}")
        if self.description:
            lines.append(f"Description: {self.description}")
        if self.keywords:
            lines.append(f"Keywords: {', '.join(self.keywords)}")
        if self.custom:
            for key, value in self.custom.items():
                lines.append(f"{key}: {value}")

        if not lines:
            return "No metadata available"

        return "\n".join(lines)
