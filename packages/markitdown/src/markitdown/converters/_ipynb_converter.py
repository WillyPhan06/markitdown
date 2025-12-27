from typing import BinaryIO, Any
import json

from .._base_converter import DocumentConverter, DocumentConverterResult
from .._exceptions import FileConversionException
from .._stream_info import StreamInfo
from .._conversion_quality import (
    ConversionQuality,
    FormattingLossType,
    WarningSeverity,
)

CANDIDATE_MIME_TYPE_PREFIXES = [
    "application/json",
]

ACCEPTED_FILE_EXTENSIONS = [".ipynb"]


class IpynbConverter(DocumentConverter):
    """Converts Jupyter Notebook (.ipynb) files to Markdown."""

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

        for prefix in CANDIDATE_MIME_TYPE_PREFIXES:
            if mimetype.startswith(prefix):
                # Read further to see if it's a notebook
                cur_pos = file_stream.tell()
                try:
                    encoding = stream_info.charset or "utf-8"
                    notebook_content = file_stream.read().decode(encoding)
                    return (
                        "nbformat" in notebook_content
                        and "nbformat_minor" in notebook_content
                    )
                finally:
                    file_stream.seek(cur_pos)

        return False

    def convert(
        self,
        file_stream: BinaryIO,
        stream_info: StreamInfo,
        **kwargs: Any,  # Options to pass to the converter
    ) -> DocumentConverterResult:
        # Parse and convert the notebook
        encoding = stream_info.charset or "utf-8"
        notebook_content = file_stream.read().decode(encoding=encoding)
        return self._convert(json.loads(notebook_content))

    def _convert(self, notebook_content: dict) -> DocumentConverterResult:
        """Helper function that converts notebook JSON content to Markdown."""
        try:
            md_output = []
            title = None

            # Quality tracking
            quality = ConversionQuality(confidence=0.9)

            # Track cell counts and issues
            cells = notebook_content.get("cells", [])
            total_cells = len(cells)
            markdown_cells = 0
            code_cells = 0
            raw_cells = 0
            cells_with_outputs = 0
            cells_missing_source = 0
            empty_code_cells = 0
            unknown_cell_types = 0

            # Check for notebook metadata
            metadata = notebook_content.get("metadata", {})
            has_kernel_info = "kernelspec" in metadata or "kernel_info" in metadata
            has_language_info = "language_info" in metadata
            kernel_language = metadata.get("language_info", {}).get(
                "name", metadata.get("kernelspec", {}).get("language", "python")
            )

            for cell in cells:
                cell_type = cell.get("cell_type", "")
                source_lines = cell.get("source", [])
                source_content = "".join(source_lines) if source_lines else ""

                # Track missing source
                if not source_lines or not source_content.strip():
                    if cell_type == "code":
                        empty_code_cells += 1

                if cell_type == "markdown":
                    markdown_cells += 1
                    md_output.append(source_content)

                    # Extract the first # heading as title if not already found
                    if title is None:
                        for line in source_lines:
                            if line.startswith("# "):
                                title = line.lstrip("# ").strip()
                                break

                elif cell_type == "code":
                    code_cells += 1
                    # Code cells are wrapped in Markdown code blocks
                    md_output.append(f"```{kernel_language}\n{source_content}\n```")

                    # Check if cell has outputs (these are not included in markdown)
                    outputs = cell.get("outputs", [])
                    if outputs:
                        cells_with_outputs += 1

                elif cell_type == "raw":
                    raw_cells += 1
                    md_output.append(f"```\n{source_content}\n```")
                else:
                    unknown_cell_types += 1

            md_text = "\n\n".join(md_output)

            # Check for title in notebook metadata
            title = metadata.get("title", title)

            # Build quality report
            quality.set_metric("total_cells", total_cells)
            quality.set_metric("markdown_cells", markdown_cells)
            quality.set_metric("code_cells", code_cells)
            quality.set_metric("raw_cells", raw_cells)
            quality.set_metric("cells_with_outputs", cells_with_outputs)
            quality.set_metric("kernel_language", kernel_language)

            # Warnings for missing or incomplete content
            if cells_with_outputs > 0:
                quality.add_warning(
                    f"{cells_with_outputs} code cell(s) had outputs that were not included in the conversion.",
                    severity=WarningSeverity.MEDIUM,
                    formatting_type=FormattingLossType.EMBEDDED_OBJECT,
                    element_count=cells_with_outputs,
                )

            if empty_code_cells > 0:
                quality.add_warning(
                    f"{empty_code_cells} code cell(s) were empty.",
                    severity=WarningSeverity.INFO,
                    element_count=empty_code_cells,
                )

            if unknown_cell_types > 0:
                quality.add_warning(
                    f"{unknown_cell_types} cell(s) had unknown cell types and were skipped.",
                    severity=WarningSeverity.MEDIUM,
                    element_count=unknown_cell_types,
                )

            if not has_kernel_info:
                quality.add_warning(
                    "Notebook metadata is missing kernel information.",
                    severity=WarningSeverity.INFO,
                )

            if not has_language_info:
                quality.add_warning(
                    "Notebook metadata is missing language information; defaulting to Python.",
                    severity=WarningSeverity.INFO,
                )

            if total_cells == 0:
                quality.add_warning(
                    "Notebook contains no cells.",
                    severity=WarningSeverity.HIGH,
                )
                quality.confidence = 0.5

            # Note about lost formatting
            quality.add_formatting_loss(FormattingLossType.EMBEDDED_OBJECT)  # Cell outputs

            # Confidence Adjustment Formula Explanation:
            # Base confidence: 0.9 (notebooks are generally well-structured and convert reliably)
            #
            # Penalty for lost outputs:
            # - Cell outputs (plots, tables, printed results) are valuable content that gets lost
            # - We calculate output_ratio = cells_with_outputs / code_cells to measure impact
            # - Penalty: 0.1 * min(output_ratio, 0.5) = max 5% reduction
            # - Capped at 0.5 ratio because even if all cells have outputs, it's still a partial loss
            #
            # Penalty for unknown cell types:
            # - Unknown cells indicate potential data loss or format issues
            # - Penalty: 0.05 per unknown cell, capped at 3 cells (max 15% reduction)
            # - Capped because additional unknown cells don't compound the uncertainty much more
            #
            # Minimum confidence: 0.3 (even problematic notebooks have some extractable content)
            if cells_with_outputs > 0:
                output_ratio = cells_with_outputs / max(code_cells, 1)
                quality.confidence -= 0.1 * min(output_ratio, 0.5)

            if unknown_cell_types > 0:
                quality.confidence -= 0.05 * min(unknown_cell_types, 3)

            quality.confidence = max(0.3, quality.confidence)

            return DocumentConverterResult(
                markdown=md_text,
                title=title,
                quality=quality,
            )

        except Exception as e:
            raise FileConversionException(
                f"Error converting .ipynb file: {str(e)}"
            ) from e
