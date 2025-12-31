from typing import Any, BinaryIO, Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from ._conversion_quality import ConversionQuality
    from ._document_metadata import DocumentMetadata

from ._stream_info import StreamInfo


class DocumentConverterResult:
    """The result of converting a document to Markdown."""

    def __init__(
        self,
        markdown: str,
        *,
        title: Optional[str] = None,
        quality: Optional["ConversionQuality"] = None,
        metadata: Optional["DocumentMetadata"] = None,
    ):
        """
        Initialize the DocumentConverterResult.

        The only required parameter is the converted Markdown text.
        The title, quality info, and any other metadata that may be added in the future, are optional.

        Parameters:
        - markdown: The converted Markdown text.
        - title: Optional title of the document.
        - quality: Optional ConversionQuality object with metadata about the conversion quality.
        - metadata: Optional DocumentMetadata object with document metadata (author, date, etc.).
        """
        self.markdown = markdown
        self.title = title
        self._quality = quality
        self._metadata = metadata

    @property
    def quality(self) -> "ConversionQuality":
        """
        Get the quality information for this conversion.

        Returns a ConversionQuality object. If no quality info was provided
        during conversion, returns a default object with 100% confidence.
        """
        if self._quality is None:
            # Lazy import to avoid circular dependencies
            from ._conversion_quality import ConversionQuality

            self._quality = ConversionQuality()
        return self._quality

    @quality.setter
    def quality(self, value: "ConversionQuality") -> None:
        """Set the quality information for this conversion."""
        self._quality = value

    @property
    def metadata(self) -> "DocumentMetadata":
        """
        Get the document metadata for this conversion.

        Returns a DocumentMetadata object. If no metadata was provided
        during conversion, returns an empty metadata object.
        """
        if self._metadata is None:
            # Lazy import to avoid circular dependencies
            from ._document_metadata import DocumentMetadata

            self._metadata = DocumentMetadata()
        return self._metadata

    @metadata.setter
    def metadata(self, value: "DocumentMetadata") -> None:
        """Set the document metadata for this conversion."""
        self._metadata = value

    @property
    def text_content(self) -> str:
        """Soft-deprecated alias for `markdown`. New code should migrate to using `markdown` or __str__."""
        return self.markdown

    @text_content.setter
    def text_content(self, markdown: str):
        """Soft-deprecated alias for `markdown`. New code should migrate to using `markdown` or __str__."""
        self.markdown = markdown

    def __str__(self) -> str:
        """Return the converted Markdown text."""
        return self.markdown


class DocumentConverter:
    """Abstract superclass of all DocumentConverters."""

    def accepts(
        self,
        file_stream: BinaryIO,
        stream_info: StreamInfo,
        **kwargs: Any,  # Options to pass to the converter
    ) -> bool:
        """
        Return a quick determination on if the converter should attempt converting the document.
        This is primarily based `stream_info` (typically, `stream_info.mimetype`, `stream_info.extension`).
        In cases where the data is retrieved via HTTP, the `steam_info.url` might also be referenced to
        make a determination (e.g., special converters for Wikipedia, YouTube etc).
        Finally, it is conceivable that the `stream_info.filename` might be used to in cases
        where the filename is well-known (e.g., `Dockerfile`, `Makefile`, etc)

        NOTE: The method signature is designed to match that of the convert() method. This provides some
        assurance that, if accepts() returns True, the convert() method will also be able to handle the document.

        IMPORTANT: In rare cases, (e.g., OutlookMsgConverter) we need to read more from the stream to make a final
        determination. Read operations inevitably advances the position in file_stream. In these case, the position
        MUST be reset it MUST be reset before returning. This is because the convert() method may be called immediately
        after accepts(), and will expect the file_stream to be at the original position.

        E.g.,
        cur_pos = file_stream.tell() # Save the current position
        data = file_stream.read(100) # ... peek at the first 100 bytes, etc.
        file_stream.seek(cur_pos)    # Reset the position to the original position

        Parameters:
        - file_stream: The file-like object to convert. Must support seek(), tell(), and read() methods.
        - stream_info: The StreamInfo object containing metadata about the file (mimetype, extension, charset, set)
        - kwargs: Additional keyword arguments for the converter.

        Returns:
        - bool: True if the converter can handle the document, False otherwise.
        """
        raise NotImplementedError(
            f"The subclass, {type(self).__name__}, must implement the accepts() method to determine if they can handle the document."
        )

    def convert(
        self,
        file_stream: BinaryIO,
        stream_info: StreamInfo,
        **kwargs: Any,  # Options to pass to the converter
    ) -> DocumentConverterResult:
        """
        Convert a document to Markdown text.

        Parameters:
        - file_stream: The file-like object to convert. Must support seek(), tell(), and read() methods.
        - stream_info: The StreamInfo object containing metadata about the file (mimetype, extension, charset, set)
        - kwargs: Additional keyword arguments for the converter.

        Returns:
        - DocumentConverterResult: The result of the conversion, which includes the title and markdown content.

        Raises:
        - FileConversionException: If the mimetype is recognized, but the conversion fails for some other reason.
        - MissingDependencyException: If the converter requires a dependency that is not installed.
        """
        raise NotImplementedError("Subclasses must implement this method")
