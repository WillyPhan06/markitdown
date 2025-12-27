import sys
import re
import os
from typing import BinaryIO, Any, List
from enum import Enum

from .._base_converter import DocumentConverter, DocumentConverterResult
from .._stream_info import StreamInfo
from .._exceptions import MissingDependencyException
from .._conversion_quality import (
    ConversionQuality,
    FormattingLossType,
    WarningSeverity,
)

# Try loading optional (but in this case, required) dependencies
# Save reporting of any exceptions for later
_dependency_exc_info = None
try:
    from azure.ai.documentintelligence import DocumentIntelligenceClient
    from azure.ai.documentintelligence.models import (
        AnalyzeDocumentRequest,
        AnalyzeResult,
        DocumentAnalysisFeature,
    )
    from azure.core.credentials import AzureKeyCredential, TokenCredential
    from azure.identity import DefaultAzureCredential
except ImportError:
    # Preserve the error and stack trace for later
    _dependency_exc_info = sys.exc_info()

    # Define these types for type hinting when the package is not available
    class AzureKeyCredential:
        pass

    class TokenCredential:
        pass

    class DocumentIntelligenceClient:
        pass

    class AnalyzeDocumentRequest:
        pass

    class AnalyzeResult:
        pass

    class DocumentAnalysisFeature:
        pass

    class DefaultAzureCredential:
        pass


# TODO: currently, there is a bug in the document intelligence SDK with importing the "ContentFormat" enum.
# This constant is a temporary fix until the bug is resolved.
CONTENT_FORMAT = "markdown"


class DocumentIntelligenceFileType(str, Enum):
    """Enum of file types supported by the Document Intelligence Converter."""

    # No OCR
    DOCX = "docx"
    PPTX = "pptx"
    XLSX = "xlsx"
    HTML = "html"
    # OCR
    PDF = "pdf"
    JPEG = "jpeg"
    PNG = "png"
    BMP = "bmp"
    TIFF = "tiff"


def _get_mime_type_prefixes(types: List[DocumentIntelligenceFileType]) -> List[str]:
    """Get the MIME type prefixes for the given file types."""
    prefixes: List[str] = []
    for type_ in types:
        if type_ == DocumentIntelligenceFileType.DOCX:
            prefixes.append(
                "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
            )
        elif type_ == DocumentIntelligenceFileType.PPTX:
            prefixes.append(
                "application/vnd.openxmlformats-officedocument.presentationml"
            )
        elif type_ == DocumentIntelligenceFileType.XLSX:
            prefixes.append(
                "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )
        elif type_ == DocumentIntelligenceFileType.HTML:
            prefixes.append("text/html")
            prefixes.append("application/xhtml+xml")
        elif type_ == DocumentIntelligenceFileType.PDF:
            prefixes.append("application/pdf")
            prefixes.append("application/x-pdf")
        elif type_ == DocumentIntelligenceFileType.JPEG:
            prefixes.append("image/jpeg")
        elif type_ == DocumentIntelligenceFileType.PNG:
            prefixes.append("image/png")
        elif type_ == DocumentIntelligenceFileType.BMP:
            prefixes.append("image/bmp")
        elif type_ == DocumentIntelligenceFileType.TIFF:
            prefixes.append("image/tiff")
    return prefixes


def _get_file_extensions(types: List[DocumentIntelligenceFileType]) -> List[str]:
    """Get the file extensions for the given file types."""
    extensions: List[str] = []
    for type_ in types:
        if type_ == DocumentIntelligenceFileType.DOCX:
            extensions.append(".docx")
        elif type_ == DocumentIntelligenceFileType.PPTX:
            extensions.append(".pptx")
        elif type_ == DocumentIntelligenceFileType.XLSX:
            extensions.append(".xlsx")
        elif type_ == DocumentIntelligenceFileType.PDF:
            extensions.append(".pdf")
        elif type_ == DocumentIntelligenceFileType.JPEG:
            extensions.append(".jpg")
            extensions.append(".jpeg")
        elif type_ == DocumentIntelligenceFileType.PNG:
            extensions.append(".png")
        elif type_ == DocumentIntelligenceFileType.BMP:
            extensions.append(".bmp")
        elif type_ == DocumentIntelligenceFileType.TIFF:
            extensions.append(".tiff")
        elif type_ == DocumentIntelligenceFileType.HTML:
            extensions.append(".html")
    return extensions


class DocumentIntelligenceConverter(DocumentConverter):
    """Specialized DocumentConverter that uses Document Intelligence to extract text from documents."""

    def __init__(
        self,
        *,
        endpoint: str,
        api_version: str = "2024-07-31-preview",
        credential: AzureKeyCredential | TokenCredential | None = None,
        file_types: List[DocumentIntelligenceFileType] = [
            DocumentIntelligenceFileType.DOCX,
            DocumentIntelligenceFileType.PPTX,
            DocumentIntelligenceFileType.XLSX,
            DocumentIntelligenceFileType.PDF,
            DocumentIntelligenceFileType.JPEG,
            DocumentIntelligenceFileType.PNG,
            DocumentIntelligenceFileType.BMP,
            DocumentIntelligenceFileType.TIFF,
        ],
    ):
        """
        Initialize the DocumentIntelligenceConverter.

        Args:
            endpoint (str): The endpoint for the Document Intelligence service.
            api_version (str): The API version to use. Defaults to "2024-07-31-preview".
            credential (AzureKeyCredential | TokenCredential | None): The credential to use for authentication.
            file_types (List[DocumentIntelligenceFileType]): The file types to accept. Defaults to all supported file types.
        """

        super().__init__()
        self._file_types = file_types

        # Raise an error if the dependencies are not available.
        # This is different than other converters since this one isn't even instantiated
        # unless explicitly requested.
        if _dependency_exc_info is not None:
            raise MissingDependencyException(
                "DocumentIntelligenceConverter requires the optional dependency [az-doc-intel] (or [all]) to be installed. E.g., `pip install markitdown[az-doc-intel]`"
            ) from _dependency_exc_info[
                1
            ].with_traceback(  # type: ignore[union-attr]
                _dependency_exc_info[2]
            )

        if credential is None:
            if os.environ.get("AZURE_API_KEY") is None:
                credential = DefaultAzureCredential()
            else:
                credential = AzureKeyCredential(os.environ["AZURE_API_KEY"])

        self.endpoint = endpoint
        self.api_version = api_version
        self.doc_intel_client = DocumentIntelligenceClient(
            endpoint=self.endpoint,
            api_version=self.api_version,
            credential=credential,
        )

    def accepts(
        self,
        file_stream: BinaryIO,
        stream_info: StreamInfo,
        **kwargs: Any,  # Options to pass to the converter
    ) -> bool:
        mimetype = (stream_info.mimetype or "").lower()
        extension = (stream_info.extension or "").lower()

        if extension in _get_file_extensions(self._file_types):
            return True

        for prefix in _get_mime_type_prefixes(self._file_types):
            if mimetype.startswith(prefix):
                return True

        return False

    def _analysis_features(self, stream_info: StreamInfo) -> List[str]:
        """
        Helper needed to determine which analysis features to use.
        Certain document analysis features are not availiable for
        office filetypes (.xlsx, .pptx, .html, .docx)
        """
        mimetype = (stream_info.mimetype or "").lower()
        extension = (stream_info.extension or "").lower()

        # Types that don't support ocr
        no_ocr_types = [
            DocumentIntelligenceFileType.DOCX,
            DocumentIntelligenceFileType.PPTX,
            DocumentIntelligenceFileType.XLSX,
            DocumentIntelligenceFileType.HTML,
        ]

        if extension in _get_file_extensions(no_ocr_types):
            return []

        for prefix in _get_mime_type_prefixes(no_ocr_types):
            if mimetype.startswith(prefix):
                return []

        return [
            DocumentAnalysisFeature.FORMULAS,  # enable formula extraction
            DocumentAnalysisFeature.OCR_HIGH_RESOLUTION,  # enable high resolution OCR
            DocumentAnalysisFeature.STYLE_FONT,  # enable font style extraction
        ]

    def convert(
        self,
        file_stream: BinaryIO,
        stream_info: StreamInfo,
        **kwargs: Any,  # Options to pass to the converter
    ) -> DocumentConverterResult:
        # Determine analysis features once and reuse
        analysis_features = self._analysis_features(stream_info)

        # Extract the text using Azure Document Intelligence
        poller = self.doc_intel_client.begin_analyze_document(
            model_id="prebuilt-layout",
            body=AnalyzeDocumentRequest(bytes_source=file_stream.read()),
            features=analysis_features,
            output_content_format=CONTENT_FORMAT,  # TODO: replace with "ContentFormat.MARKDOWN" when the bug is fixed
        )
        result: AnalyzeResult = poller.result()

        # Quality tracking
        quality = ConversionQuality(confidence=0.85)

        # Track analysis features used
        quality.set_optional_feature("ocr_high_resolution", DocumentAnalysisFeature.OCR_HIGH_RESOLUTION in analysis_features if analysis_features else False)
        quality.set_optional_feature("formula_extraction", DocumentAnalysisFeature.FORMULAS in analysis_features if analysis_features else False)
        quality.set_optional_feature("style_font_extraction", DocumentAnalysisFeature.STYLE_FONT in analysis_features if analysis_features else False)

        # Track detected elements
        page_count = len(result.pages) if result.pages else 0
        table_count = len(result.tables) if result.tables else 0
        paragraph_count = len(result.paragraphs) if result.paragraphs else 0
        figure_count = len(result.figures) if hasattr(result, 'figures') and result.figures else 0

        # Track selection marks (form fields like checkboxes)
        selection_mark_count = 0
        if result.pages:
            for page in result.pages:
                if hasattr(page, 'selection_marks') and page.selection_marks:
                    selection_mark_count += len(page.selection_marks)

        # Track key-value pairs (form fields)
        key_value_count = len(result.key_value_pairs) if hasattr(result, 'key_value_pairs') and result.key_value_pairs else 0

        # Set metrics
        quality.set_metric("page_count", page_count)
        quality.set_metric("table_count", table_count)
        quality.set_metric("paragraph_count", paragraph_count)
        quality.set_metric("figure_count", figure_count)
        quality.set_metric("selection_mark_count", selection_mark_count)
        quality.set_metric("key_value_count", key_value_count)
        quality.set_metric("content_length", len(result.content) if result.content else 0)

        # Analyze tables for potential issues
        tables_with_issues = 0
        if result.tables:
            for table in result.tables:
                # Check for tables with low confidence or spanning issues
                if hasattr(table, 'confidence') and table.confidence and table.confidence < 0.8:
                    tables_with_issues += 1

        # Check for figures/images
        if figure_count > 0:
            quality.add_warning(
                f"{figure_count} figure(s)/image(s) detected. Image content is not fully extractable.",
                severity=WarningSeverity.MEDIUM,
                formatting_type=FormattingLossType.IMAGE,
                element_count=figure_count,
            )
            quality.add_formatting_loss(FormattingLossType.IMAGE)

        # Check for tables
        if table_count > 0:
            if tables_with_issues > 0:
                quality.add_warning(
                    f"{tables_with_issues} table(s) may have been partially converted due to complex structure.",
                    severity=WarningSeverity.MEDIUM,
                    formatting_type=FormattingLossType.TABLE,
                    element_count=tables_with_issues,
                )
            else:
                quality.add_warning(
                    f"{table_count} table(s) converted. Complex table formatting may be simplified.",
                    severity=WarningSeverity.INFO,
                    formatting_type=FormattingLossType.TABLE_FORMATTING,
                    element_count=table_count,
                )
            quality.add_formatting_loss(FormattingLossType.TABLE_FORMATTING)

        # Check for form fields
        if selection_mark_count > 0:
            quality.add_warning(
                f"{selection_mark_count} selection mark(s) (checkboxes/radio buttons) detected but may not be fully preserved.",
                severity=WarningSeverity.LOW,
                formatting_type=FormattingLossType.FORM_FIELD,
                element_count=selection_mark_count,
            )
            quality.add_formatting_loss(FormattingLossType.FORM_FIELD)

        if key_value_count > 0:
            quality.add_warning(
                f"{key_value_count} form field(s) (key-value pairs) detected.",
                severity=WarningSeverity.INFO,
                formatting_type=FormattingLossType.FORM_FIELD,
                element_count=key_value_count,
            )

        # Check for empty or minimal content
        if not result.content or len(result.content.strip()) < 10:
            quality.add_warning(
                "Document appears to contain minimal or no extractable text.",
                severity=WarningSeverity.HIGH,
            )
            quality.confidence = 0.4

        # Note about OCR if applicable
        if analysis_features:
            quality.add_warning(
                "OCR was used for text extraction. Accuracy depends on document quality.",
                severity=WarningSeverity.INFO,
            )

        # Standard formatting losses for Document Intelligence
        quality.add_formatting_loss(FormattingLossType.FONT_STYLE)
        quality.add_formatting_loss(FormattingLossType.TEXT_COLOR)
        quality.add_formatting_loss(FormattingLossType.HEADER_FOOTER)

        # Confidence Adjustment Formula Explanation:
        # Base confidence: 0.85 (Document Intelligence is generally reliable but has limitations)
        #
        # Special case for minimal/empty content (applied above): set to 0.4
        # - If the document has < 10 characters, OCR likely failed or document is empty
        # - This overrides other calculations as it indicates fundamental failure
        #
        # Penalty for tables with issues:
        # - Tables with confidence < 0.8 from the API indicate complex or poorly recognized tables
        # - Penalty: 0.05 per problematic table, capped at 3 tables (max 15% reduction)
        # - Capped because table issues don't compound indefinitely; user is already warned
        #
        # Penalty for figures/images:
        # - Images cannot have their visual content extracted (only detected)
        # - Penalty: 0.05 per figure, capped at 3 figures (max 15% reduction)
        # - Capped because missing images is a known limitation, not a conversion failure
        #
        # Minimum confidence: 0.3 (document was processed, some structure extracted)
        if tables_with_issues > 0:
            quality.confidence -= 0.05 * min(tables_with_issues, 3)

        if figure_count > 0:
            quality.confidence -= 0.05 * min(figure_count, 3)

        quality.confidence = max(0.3, quality.confidence)

        # remove comments from the markdown content generated by Doc Intelligence and append to markdown string
        markdown_text = re.sub(r"<!--.*?-->", "", result.content, flags=re.DOTALL)
        return DocumentConverterResult(markdown=markdown_text, quality=quality)
