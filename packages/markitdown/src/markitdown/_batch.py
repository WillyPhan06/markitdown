# SPDX-FileCopyrightText: 2024-present Adam Fourney <adamfo@microsoft.com>
#
# SPDX-License-Identifier: MIT

"""
Batch conversion module for MarkItDown.

Provides functionality to convert multiple files at once, including entire directories.
"""

import os
import fnmatch
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import (
    Any,
    Callable,
    Dict,
    Iterator,
    List,
    Optional,
    Tuple,
    Union,
    TYPE_CHECKING,
)

if TYPE_CHECKING:
    from ._markitdown import MarkItDown
    from ._cache import ConversionCache

from ._base_converter import DocumentConverterResult
from ._conversion_quality import ConversionQuality, WarningSeverity
from ._stream_info import StreamInfo


class BatchItemStatus(Enum):
    """Status of a single file in a batch conversion."""

    SUCCESS = "success"
    FAILED = "failed"
    SKIPPED = "skipped"
    UNSUPPORTED = "unsupported"
    CACHED = "cached"  # Retrieved from cache without re-conversion


@dataclass
class BatchItemResult:
    """Result of converting a single file in a batch operation."""

    source_path: str
    status: BatchItemStatus
    result: Optional[DocumentConverterResult] = None
    error: Optional[str] = None
    error_type: Optional[str] = None

    @property
    def markdown(self) -> Optional[str]:
        """Get the markdown content if conversion was successful."""
        if self.result is not None:
            return self.result.markdown
        return None

    @property
    def quality(self) -> Optional[ConversionQuality]:
        """Get the quality information if conversion was successful."""
        if self.result is not None:
            return self.result.quality
        return None

    def to_dict(self) -> Dict[str, Any]:
        """Convert to a dictionary for serialization."""
        result_dict: Dict[str, Any] = {
            "source_path": self.source_path,
            "status": self.status.value,
        }
        if self.result is not None:
            result_dict["title"] = self.result.title
            result_dict["quality"] = self.result.quality.to_dict()
        if self.error is not None:
            result_dict["error"] = self.error
            result_dict["error_type"] = self.error_type
        return result_dict


@dataclass
class BatchConversionResult:
    """Result of a batch conversion operation."""

    items: List[BatchItemResult] = field(default_factory=list)
    source_directory: Optional[str] = None

    @property
    def total_count(self) -> int:
        """Total number of files processed."""
        return len(self.items)

    @property
    def success_count(self) -> int:
        """Number of successfully converted files (including cached)."""
        return sum(
            1 for item in self.items
            if item.status in (BatchItemStatus.SUCCESS, BatchItemStatus.CACHED)
        )

    @property
    def cached_count(self) -> int:
        """Number of files retrieved from cache."""
        return sum(1 for item in self.items if item.status == BatchItemStatus.CACHED)

    @property
    def failed_count(self) -> int:
        """Number of failed conversions."""
        return sum(1 for item in self.items if item.status == BatchItemStatus.FAILED)

    @property
    def skipped_count(self) -> int:
        """Number of skipped files."""
        return sum(1 for item in self.items if item.status == BatchItemStatus.SKIPPED)

    @property
    def unsupported_count(self) -> int:
        """Number of unsupported files."""
        return sum(
            1 for item in self.items if item.status == BatchItemStatus.UNSUPPORTED
        )

    @property
    def successful_items(self) -> List[BatchItemResult]:
        """Get all successfully converted items (including cached)."""
        return [
            item for item in self.items
            if item.status in (BatchItemStatus.SUCCESS, BatchItemStatus.CACHED)
        ]

    @property
    def cached_items(self) -> List[BatchItemResult]:
        """Get all items retrieved from cache."""
        return [item for item in self.items if item.status == BatchItemStatus.CACHED]

    @property
    def failed_items(self) -> List[BatchItemResult]:
        """Get all failed items."""
        return [item for item in self.items if item.status == BatchItemStatus.FAILED]

    @property
    def completion_percentage(self) -> float:
        """Percentage of files that were successfully converted."""
        if self.total_count == 0:
            return 100.0
        return (self.success_count / self.total_count) * 100

    @property
    def overall_quality(self) -> ConversionQuality:
        """
        Get an aggregated quality report for the entire batch.

        This combines quality information from all successful conversions
        and provides overall statistics.
        """
        quality = ConversionQuality()

        if self.total_count == 0:
            return quality

        # Calculate average confidence from successful conversions
        successful = self.successful_items
        if successful:
            total_confidence = sum(
                item.quality.confidence for item in successful if item.quality
            )
            quality.confidence = total_confidence / len(successful)
        else:
            quality.confidence = 0.0

        # Set metrics
        quality.set_metric("total_files", self.total_count)
        quality.set_metric("successful_files", self.success_count)
        quality.set_metric("failed_files", self.failed_count)
        quality.set_metric("skipped_files", self.skipped_count)
        quality.set_metric("unsupported_files", self.unsupported_count)

        # Aggregate formatting losses from all successful conversions
        all_formatting_losses = set()
        for item in successful:
            if item.quality and item.quality.formatting_loss:
                for loss in item.quality.formatting_loss:
                    all_formatting_losses.add(loss)
        for loss in all_formatting_losses:
            quality.add_formatting_loss(loss)

        # Track converters used
        converters_used: Dict[str, int] = {}
        for item in successful:
            if item.quality and item.quality.converter_used:
                converter = item.quality.converter_used
                converters_used[converter] = converters_used.get(converter, 0) + 1
        quality.set_metric("converters_used", converters_used)

        # Mark as partial if not all files were converted
        if self.success_count < self.total_count:
            quality.is_partial = True
            quality.completion_percentage = self.completion_percentage

        # Add warnings for failures
        if self.failed_count > 0:
            failed_files = [item.source_path for item in self.failed_items]
            quality.add_warning(
                f"{self.failed_count} file(s) failed to convert",
                severity=WarningSeverity.HIGH,
                element_count=self.failed_count,
                details={"failed_files": failed_files[:10]},  # Limit to first 10
            )

        if self.unsupported_count > 0:
            quality.add_warning(
                f"{self.unsupported_count} file(s) had unsupported formats",
                severity=WarningSeverity.MEDIUM,
                element_count=self.unsupported_count,
            )

        quality.converter_used = "BatchConverter"
        return quality

    def __iter__(self) -> Iterator[BatchItemResult]:
        """Iterate over all batch items."""
        return iter(self.items)

    def __len__(self) -> int:
        """Return the number of items in the batch."""
        return len(self.items)

    def __getitem__(self, index: int) -> BatchItemResult:
        """Get a batch item by index."""
        return self.items[index]

    def to_dict(self) -> Dict[str, Any]:
        """Convert to a dictionary for serialization."""
        return {
            "source_directory": self.source_directory,
            "total_count": self.total_count,
            "success_count": self.success_count,
            "cached_count": self.cached_count,
            "failed_count": self.failed_count,
            "skipped_count": self.skipped_count,
            "unsupported_count": self.unsupported_count,
            "completion_percentage": self.completion_percentage,
            "overall_quality": self.overall_quality.to_dict(),
            "items": [item.to_dict() for item in self.items],
        }

    def __str__(self) -> str:
        """Return a human-readable summary."""
        lines = []
        lines.append("=" * 60)
        lines.append("BATCH CONVERSION SUMMARY")
        lines.append("=" * 60)

        if self.source_directory:
            lines.append(f"Source: {self.source_directory}")

        lines.append(f"Total files: {self.total_count}")
        lines.append(f"  Successful: {self.success_count}")
        if self.cached_count > 0:
            lines.append(f"    (from cache: {self.cached_count})")
        lines.append(f"  Failed: {self.failed_count}")
        lines.append(f"  Skipped: {self.skipped_count}")
        lines.append(f"  Unsupported: {self.unsupported_count}")
        lines.append(f"Completion: {self.completion_percentage:.1f}%")

        if self.successful_items:
            avg_confidence = sum(
                item.quality.confidence
                for item in self.successful_items
                if item.quality
            ) / len(self.successful_items)
            lines.append(f"Average confidence: {avg_confidence:.0%}")

        # Show converted files (newly processed, not from cache)
        newly_converted = [
            item for item in self.items if item.status == BatchItemStatus.SUCCESS
        ]
        if newly_converted:
            lines.append("\nNewly converted files:")
            for item in newly_converted[:10]:  # Show first 10
                confidence_str = ""
                if item.quality:
                    confidence_str = f" ({item.quality.confidence:.0%})"
                lines.append(f"  ✓ {item.source_path}{confidence_str}")
            if len(newly_converted) > 10:
                lines.append(f"  ... and {len(newly_converted) - 10} more")

        # Show cached files (loaded from cache, not re-converted)
        if self.cached_items:
            lines.append("\nFiles loaded from cache (unchanged):")
            for item in self.cached_items[:10]:  # Show first 10
                confidence_str = ""
                if item.quality:
                    confidence_str = f" ({item.quality.confidence:.0%})"
                lines.append(f"  ⚡ {item.source_path}{confidence_str}")
            if len(self.cached_items) > 10:
                lines.append(f"  ... and {len(self.cached_items) - 10} more")

        # Show failed files
        if self.failed_items:
            lines.append("\nFailed files:")
            for item in self.failed_items[:10]:  # Show first 10
                lines.append(f"  ✗ {item.source_path}: {item.error}")
            if len(self.failed_items) > 10:
                lines.append(f"  ... and {len(self.failed_items) - 10} more")

        lines.append("=" * 60)
        return "\n".join(lines)


def convert_batch(
    markitdown: "MarkItDown",
    sources: List[Union[str, Path]],
    *,
    stream_info: Optional[StreamInfo] = None,
    max_workers: Optional[int] = None,
    on_progress: Optional[Callable[[BatchItemResult], None]] = None,
    skip_errors: bool = True,
    cache: Optional["ConversionCache"] = None,
    **kwargs: Any,
) -> BatchConversionResult:
    """
    Convert multiple files to markdown.

    Args:
        markitdown: The MarkItDown instance to use for conversion.
        sources: List of file paths or URLs to convert.
        stream_info: Optional stream info hints for all files.
        max_workers: Maximum number of parallel workers. Defaults to min(32, cpu_count + 4).
        on_progress: Optional callback called after each file is processed.
        skip_errors: If True, continue processing on errors. If False, raise on first error.
        cache: Optional ConversionCache instance for caching results. When provided,
               unchanged files will be retrieved from cache instead of re-converting.
        **kwargs: Additional arguments passed to each conversion.

    Returns:
        BatchConversionResult containing results for all files.
    """
    result = BatchConversionResult()

    def convert_single(source: Union[str, Path]) -> BatchItemResult:
        source_str = str(source)
        source_path = Path(source_str)

        # Check cache first (only for local files)
        if cache is not None and source_path.is_file():
            try:
                cache_entry = cache.get(source_str)
                if cache_entry is not None:
                    from ._cache import cache_entry_to_result

                    conversion_result = cache_entry_to_result(cache_entry)
                    return BatchItemResult(
                        source_path=source_str,
                        status=BatchItemStatus.CACHED,
                        result=conversion_result,
                    )
            except Exception:
                # Cache read failed, proceed with conversion
                pass

        try:
            conversion_result = markitdown.convert(
                source_str, stream_info=stream_info, **kwargs
            )

            # Store in cache (only for local files)
            if cache is not None and source_path.is_file():
                try:
                    cache.put(source_str, conversion_result)
                except Exception:
                    # Cache write failed, ignore
                    pass

            return BatchItemResult(
                source_path=source_str,
                status=BatchItemStatus.SUCCESS,
                result=conversion_result,
            )
        except Exception as e:
            from ._exceptions import UnsupportedFormatException

            if isinstance(e, UnsupportedFormatException):
                return BatchItemResult(
                    source_path=source_str,
                    status=BatchItemStatus.UNSUPPORTED,
                    error=str(e),
                    error_type=type(e).__name__,
                )
            return BatchItemResult(
                source_path=source_str,
                status=BatchItemStatus.FAILED,
                error=str(e),
                error_type=type(e).__name__,
            )

    if max_workers == 1:
        # Sequential processing
        for source in sources:
            item_result = convert_single(source)
            result.items.append(item_result)
            if on_progress:
                on_progress(item_result)
            if not skip_errors and item_result.status == BatchItemStatus.FAILED:
                raise RuntimeError(
                    f"Conversion failed for {item_result.source_path}: {item_result.error}"
                )
    else:
        # Parallel processing
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_source = {
                executor.submit(convert_single, source): source for source in sources
            }
            for future in as_completed(future_to_source):
                item_result = future.result()
                result.items.append(item_result)
                if on_progress:
                    on_progress(item_result)
                if not skip_errors and item_result.status == BatchItemStatus.FAILED:
                    # Cancel remaining futures
                    for f in future_to_source:
                        f.cancel()
                    raise RuntimeError(
                        f"Conversion failed for {item_result.source_path}: {item_result.error}"
                    )

    return result


def convert_directory(
    markitdown: "MarkItDown",
    directory: Union[str, Path],
    *,
    glob_pattern: str = "*",
    recursive: bool = True,
    include_patterns: Optional[List[str]] = None,
    exclude_patterns: Optional[List[str]] = None,
    stream_info: Optional[StreamInfo] = None,
    max_workers: Optional[int] = None,
    on_progress: Optional[Callable[[BatchItemResult], None]] = None,
    skip_errors: bool = True,
    cache: Optional["ConversionCache"] = None,
    **kwargs: Any,
) -> BatchConversionResult:
    """
    Convert all files in a directory to markdown.

    Args:
        markitdown: The MarkItDown instance to use for conversion.
        directory: Path to the directory to convert.
        glob_pattern: Glob pattern to match files (default: "*" for all files).
        recursive: If True, search subdirectories recursively.
        include_patterns: List of glob patterns for files to include (e.g., ["*.pdf", "*.docx"]).
        exclude_patterns: List of glob patterns for files to exclude (e.g., ["*.tmp", ".*"]).
        stream_info: Optional stream info hints for all files.
        max_workers: Maximum number of parallel workers.
        on_progress: Optional callback called after each file is processed.
        skip_errors: If True, continue processing on errors.
        cache: Optional ConversionCache instance for caching results.
        **kwargs: Additional arguments passed to each conversion.

    Returns:
        BatchConversionResult containing results for all files.
    """
    directory = Path(directory)
    if not directory.is_dir():
        raise ValueError(f"Not a directory: {directory}")

    # Collect files to process
    files: List[Path] = []

    if recursive:
        pattern = f"**/{glob_pattern}"
    else:
        pattern = glob_pattern

    for file_path in directory.glob(pattern):
        if not file_path.is_file():
            continue

        # Check include patterns
        if include_patterns:
            matched = any(
                fnmatch.fnmatch(file_path.name, pat) for pat in include_patterns
            )
            if not matched:
                continue

        # Check exclude patterns
        if exclude_patterns:
            excluded = any(
                fnmatch.fnmatch(file_path.name, pat) for pat in exclude_patterns
            )
            if excluded:
                continue

        files.append(file_path)

    # Convert collected files
    result = convert_batch(
        markitdown,
        files,
        stream_info=stream_info,
        max_workers=max_workers,
        on_progress=on_progress,
        skip_errors=skip_errors,
        cache=cache,
        **kwargs,
    )
    result.source_directory = str(directory)

    return result


def write_batch_results(
    batch_result: BatchConversionResult,
    output_directory: Union[str, Path],
    *,
    preserve_structure: bool = True,
    file_extension: str = ".md",
    overwrite: bool = False,
) -> Dict[str, str]:
    """
    Write batch conversion results to files.

    Args:
        batch_result: The batch conversion result to write.
        output_directory: Directory to write output files to.
        preserve_structure: If True, preserve the source directory structure.
        file_extension: Extension for output files (default: ".md").
        overwrite: If True, overwrite existing files.

    Returns:
        Dictionary mapping source paths to output paths.
    """
    output_directory = Path(output_directory)
    output_directory.mkdir(parents=True, exist_ok=True)

    output_mapping: Dict[str, str] = {}
    source_dir = (
        Path(batch_result.source_directory) if batch_result.source_directory else None
    )

    for item in batch_result.successful_items:
        source_path = Path(item.source_path)

        if preserve_structure and source_dir:
            try:
                # Preserve relative directory structure
                relative_path = source_path.relative_to(source_dir)
                output_path = output_directory / relative_path.with_suffix(
                    file_extension
                )
            except ValueError:
                # Source not relative to source_dir
                output_path = output_directory / (source_path.stem + file_extension)
        else:
            output_path = output_directory / (source_path.stem + file_extension)

        # Handle filename conflicts
        if not overwrite and output_path.exists():
            counter = 1
            base_path = output_path.with_suffix("")
            while output_path.exists():
                output_path = Path(f"{base_path}_{counter}{file_extension}")
                counter += 1

        # Create parent directories if needed
        output_path.parent.mkdir(parents=True, exist_ok=True)

        # Write the markdown content
        if item.markdown:
            with open(output_path, "w", encoding="utf-8") as f:
                f.write(item.markdown)

        output_mapping[item.source_path] = str(output_path)

    return output_mapping
