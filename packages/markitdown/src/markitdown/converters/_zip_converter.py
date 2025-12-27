import zipfile
import io
import os

from typing import BinaryIO, Any, TYPE_CHECKING, List

from .._base_converter import DocumentConverter, DocumentConverterResult
from .._stream_info import StreamInfo
from .._exceptions import UnsupportedFormatException, FileConversionException
from .._conversion_quality import (
    ConversionQuality,
    FormattingLossType,
    WarningSeverity,
)

# Break otherwise circular import for type hinting
if TYPE_CHECKING:
    from .._markitdown import MarkItDown

ACCEPTED_MIME_TYPE_PREFIXES = [
    "application/zip",
]

ACCEPTED_FILE_EXTENSIONS = [".zip"]


class ZipConverter(DocumentConverter):
    """Converts ZIP files to markdown by extracting and converting all contained files.

    The converter extracts the ZIP contents to a temporary directory, processes each file
    using appropriate converters based on file extensions, and then combines the results
    into a single markdown document. The temporary directory is cleaned up after processing.

    Example output format:
    ```markdown
    Content from the zip file `example.zip`:

    ## File: docs/readme.txt

    This is the content of readme.txt
    Multiple lines are preserved

    ## File: images/example.jpg

    ImageSize: 1920x1080
    DateTimeOriginal: 2024-02-15 14:30:00
    Description: A beautiful landscape photo

    ## File: data/report.xlsx

    ## Sheet1
    | Column1 | Column2 | Column3 |
    |---------|---------|---------|
    | data1   | data2   | data3   |
    | data4   | data5   | data6   |
    ```

    Key features:
    - Maintains original file structure in headings
    - Processes nested files recursively
    - Uses appropriate converters for each file type
    - Preserves formatting of converted content
    - Cleans up temporary files after processing
    """

    def __init__(
        self,
        *,
        markitdown: "MarkItDown",
    ):
        super().__init__()
        self._markitdown = markitdown

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
        file_path = stream_info.url or stream_info.local_path or stream_info.filename
        md_content = f"Content from the zip file `{file_path}`:\n\n"

        # Quality tracking
        quality = ConversionQuality(confidence=0.85)

        # Track file conversion statistics
        total_files = 0
        converted_files = 0
        skipped_directories = 0
        unsupported_files: List[str] = []
        failed_files: List[str] = []

        with zipfile.ZipFile(file_stream, "r") as zipObj:
            all_entries = zipObj.namelist()

            for name in all_entries:
                # Skip directories
                if name.endswith("/"):
                    skipped_directories += 1
                    continue

                total_files += 1

                try:
                    z_file_stream = io.BytesIO(zipObj.read(name))
                    z_file_stream_info = StreamInfo(
                        extension=os.path.splitext(name)[1],
                        filename=os.path.basename(name),
                    )
                    result = self._markitdown.convert_stream(
                        stream=z_file_stream,
                        stream_info=z_file_stream_info,
                    )
                    if result is not None:
                        md_content += f"## File: {name}\n\n"
                        md_content += result.markdown + "\n\n"
                        converted_files += 1
                except UnsupportedFormatException:
                    unsupported_files.append(name)
                except FileConversionException:
                    failed_files.append(name)

        # Build quality report
        quality.set_metric("total_files", total_files)
        quality.set_metric("converted_files", converted_files)
        quality.set_metric("unsupported_files", len(unsupported_files))
        quality.set_metric("failed_files", len(failed_files))
        quality.set_metric("skipped_directories", skipped_directories)

        # Calculate completion percentage
        if total_files > 0:
            completion = (converted_files / total_files) * 100
            quality.completion_percentage = completion
            if converted_files < total_files:
                quality.is_partial = True

        # Warnings for unsupported files
        if unsupported_files:
            quality.add_warning(
                f"{len(unsupported_files)} file(s) had unsupported formats and were skipped.",
                severity=WarningSeverity.MEDIUM,
                element_count=len(unsupported_files),
                details={"files": unsupported_files[:10]},  # Limit to first 10
            )

        # Warnings for failed conversions
        if failed_files:
            quality.add_warning(
                f"{len(failed_files)} file(s) failed to convert.",
                severity=WarningSeverity.HIGH,
                element_count=len(failed_files),
                details={"files": failed_files[:10]},  # Limit to first 10
            )

        # Warning if no files could be converted
        if total_files > 0 and converted_files == 0:
            quality.add_warning(
                "No files in the archive could be converted.",
                severity=WarningSeverity.HIGH,
            )
            quality.confidence = 0.3
        elif total_files == 0:
            quality.add_warning(
                "The archive contains no files.",
                severity=WarningSeverity.MEDIUM,
            )
            quality.confidence = 0.5

        # Confidence Adjustment Formula Explanation:
        # Base confidence: 0.85 (zip archives have variable content quality)
        #
        # Linear scaling formula: confidence = 0.85 * success_rate + 0.15
        # - success_rate = converted_files / total_files (0.0 to 1.0)
        # - At 100% success: 0.85 * 1.0 + 0.15 = 1.0 (perfect confidence)
        # - At 50% success: 0.85 * 0.5 + 0.15 = 0.575 (moderate confidence)
        # - At 0% success: 0.85 * 0.0 + 0.15 = 0.15 (low confidence, but not zero)
        # - The +0.15 base ensures we never go below 15% just from success rate,
        #   acknowledging that even failed conversions provide some value (file listing)
        #
        # Additional penalty for failed conversions (vs unsupported):
        # - Failed files are worse than unsupported files (indicates errors, not just format)
        # - Penalty: 0.1 * (failed_ratio), capped at 0.3 ratio (max 3% additional reduction)
        # - This distinguishes between "can't convert .exe" (unsupported) vs "corrupted .docx" (failed)
        #
        # Minimum confidence: 0.3 (archive structure was readable)
        # Maximum confidence: 1.0 (all files converted successfully)
        if total_files > 0:
            success_rate = converted_files / total_files
            quality.confidence = 0.85 * success_rate + 0.15

            if failed_files:
                quality.confidence -= 0.1 * min(len(failed_files) / total_files, 0.3)

        quality.confidence = max(0.3, min(1.0, quality.confidence))

        return DocumentConverterResult(markdown=md_content.strip(), quality=quality)
