# SPDX-FileCopyrightText: 2024-present Adam Fourney <adamfo@microsoft.com>
#
# SPDX-License-Identifier: MIT

"""
Token estimation module for MarkItDown batch conversions.

Provides functionality to estimate the number of LLM tokens that will be used
during batch conversion, helping users plan their conversions effectively.

Token estimation is relevant when an LLM client is configured for MarkItDown,
as certain converters (like ImageConverter and PptxConverter) use the LLM to
generate descriptions for images.

IMPORTANT ASSUMPTIONS AND LIMITATIONS:
--------------------------------------

1. LLM Usage Scope:
   This module estimates tokens ONLY for the built-in LLM usage in MarkItDown,
   which currently includes:
   - ImageConverter: Generates descriptions for standalone image files (.jpg, .jpeg, .png)
   - PptxConverter: Generates descriptions for images embedded in PowerPoint files (.pptx)

2. NO_LLM Category:
   Files categorized as NO_LLM (e.g., PDF, DOCX, XLSX, HTML, etc.) are assumed to
   NOT use LLM tokens during conversion. This is accurate for MarkItDown's built-in
   converters, but may NOT be accurate if:
   - You have custom plugins that use LLM for text analysis
   - You're using an external LLM pipeline that processes the converted markdown
   - You have custom converters that call LLM APIs

   If your workflow uses LLM for additional processing beyond image descriptions,
   you should account for those tokens separately.

3. Estimation Accuracy:
   - Image token estimates are based on file size as a proxy for image dimensions
   - PPTX image counts are estimated from file size using conservative heuristics
   - Actual token usage may vary based on:
     * Actual image dimensions and complexity
     * LLM model used (different models have different token calculations)
     * Prompt customization via llm_prompt parameter
   - Estimates are designed to be slightly higher than actual usage to help with
     budgeting, but large PPTX files with mostly text/charts may overestimate.

4. Cache and Resume:
   - Cached files (already converted and stored) will show 0 tokens
   - Resumed files (output already exists) will show 0 tokens
   - These accurately reflect that no NEW tokens will be used for these files
"""

import os
import math
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional, Union, TYPE_CHECKING
from enum import Enum

if TYPE_CHECKING:
    from ._cache import ConversionCache


class FileCategory(Enum):
    """Category of file based on how it uses LLM tokens."""

    IMAGE = "image"  # Standalone image files (jpg, png)
    PPTX = "pptx"  # PowerPoint files (may contain embedded images)
    NO_LLM = "no_llm"  # Files that don't use LLM
    CACHED = "cached"  # File is cached, no tokens needed
    RESUMED = "resumed"  # Output already exists, no tokens needed


# Image file extensions that use LLM for description
IMAGE_EXTENSIONS = {".jpg", ".jpeg", ".png"}

# PowerPoint extensions that may use LLM for embedded image descriptions
PPTX_EXTENSIONS = {".pptx"}

# Default prompt token count (the prompt "Write a detailed caption for this image.")
DEFAULT_PROMPT_TOKENS = 10

# Estimated output tokens for image description
DEFAULT_OUTPUT_TOKENS = 150

# Token estimation constants for images
# Based on OpenAI's vision model token calculation:
# - Images are scaled to fit within a max size
# - Then divided into 512x512 tiles
# - Each tile costs ~85 tokens (for low detail) or ~170 tokens (for high detail)
# We use conservative estimates assuming "auto" detail mode
TOKENS_PER_TILE = 170  # High detail tokens per tile
BASE_IMAGE_TOKENS = 85  # Base tokens for any image
MAX_IMAGE_DIMENSION = 2048  # Max dimension before scaling
TILE_SIZE = 512  # Tile size for token calculation


@dataclass
class FileTokenEstimate:
    """Token estimate for a single file."""

    source_path: str
    category: FileCategory
    input_tokens: int = 0
    output_tokens: int = 0
    image_count: int = 0  # Number of images (1 for image files, varies for pptx)
    file_size_bytes: int = 0
    skip_reason: Optional[str] = None  # Why this file won't use tokens

    @property
    def total_tokens(self) -> int:
        """Total estimated tokens (input + output)."""
        return self.input_tokens + self.output_tokens

    def to_dict(self) -> Dict:
        """Convert to dictionary for serialization."""
        result = {
            "source_path": self.source_path,
            "category": self.category.value,
            "input_tokens": self.input_tokens,
            "output_tokens": self.output_tokens,
            "total_tokens": self.total_tokens,
            "image_count": self.image_count,
            "file_size_bytes": self.file_size_bytes,
        }
        if self.skip_reason:
            result["skip_reason"] = self.skip_reason
        return result


@dataclass
class BatchTokenEstimate:
    """Token estimate for an entire batch conversion."""

    files: List[FileTokenEstimate] = field(default_factory=list)

    @property
    def total_input_tokens(self) -> int:
        """Total estimated input tokens for the batch."""
        return sum(f.input_tokens for f in self.files)

    @property
    def total_output_tokens(self) -> int:
        """Total estimated output tokens for the batch."""
        return sum(f.output_tokens for f in self.files)

    @property
    def total_tokens(self) -> int:
        """Total estimated tokens (input + output) for the batch."""
        return self.total_input_tokens + self.total_output_tokens

    @property
    def total_image_count(self) -> int:
        """Total number of images that will be processed."""
        return sum(f.image_count for f in self.files)

    @property
    def files_using_llm(self) -> List[FileTokenEstimate]:
        """Files that will use LLM tokens."""
        return [f for f in self.files if f.total_tokens > 0]

    @property
    def files_skipped(self) -> List[FileTokenEstimate]:
        """Files that won't use LLM tokens."""
        return [f for f in self.files if f.total_tokens == 0]

    @property
    def cached_files(self) -> List[FileTokenEstimate]:
        """Files that are cached and won't use tokens."""
        return [f for f in self.files if f.category == FileCategory.CACHED]

    @property
    def resumed_files(self) -> List[FileTokenEstimate]:
        """Files that have existing output and won't use tokens."""
        return [f for f in self.files if f.category == FileCategory.RESUMED]

    def to_dict(self) -> Dict:
        """Convert to dictionary for serialization."""
        return {
            "summary": {
                "total_files": len(self.files),
                "files_using_llm": len(self.files_using_llm),
                "files_skipped": len(self.files_skipped),
                "cached_files": len(self.cached_files),
                "resumed_files": len(self.resumed_files),
                "total_image_count": self.total_image_count,
                "total_input_tokens": self.total_input_tokens,
                "total_output_tokens": self.total_output_tokens,
                "total_tokens": self.total_tokens,
            },
            "files": [f.to_dict() for f in self.files],
        }

    def __str__(self) -> str:
        """Return a human-readable summary."""
        lines = []
        lines.append("=" * 60)
        lines.append("TOKEN ESTIMATION SUMMARY")
        lines.append("=" * 60)
        lines.append(f"Total files: {len(self.files)}")
        lines.append(f"  Files using LLM: {len(self.files_using_llm)}")
        lines.append(f"  Files not using LLM: {len(self.files_skipped)}")
        if self.cached_files:
            lines.append(f"    (cached: {len(self.cached_files)})")
        if self.resumed_files:
            lines.append(f"    (already converted: {len(self.resumed_files)})")
        lines.append("")
        lines.append(f"Total images to process: {self.total_image_count}")
        lines.append("")
        lines.append("ESTIMATED TOKEN USAGE:")
        lines.append(f"  Input tokens:  {self.total_input_tokens:,}")
        lines.append(f"  Output tokens: {self.total_output_tokens:,}")
        lines.append(f"  TOTAL TOKENS:  {self.total_tokens:,}")

        if self.files_using_llm:
            lines.append("")
            lines.append("Files using LLM tokens:")
            # Sort by total tokens descending to show most expensive first
            sorted_files = sorted(self.files_using_llm, key=lambda f: f.total_tokens, reverse=True)
            for f in sorted_files[:20]:  # Show first 20
                # Truncate long paths
                display_path = f.source_path
                if len(display_path) > 50:
                    display_path = "..." + display_path[-47:]
                lines.append(f"  {display_path}: {f.total_tokens:,} tokens ({f.image_count} image(s))")
            if len(sorted_files) > 20:
                lines.append(f"  ... and {len(sorted_files) - 20} more files")

        lines.append("=" * 60)
        return "\n".join(lines)


def _estimate_image_tokens(file_path: Union[str, Path]) -> int:
    """
    Estimate the number of input tokens for an image file.

    Based on OpenAI's vision model token calculation:
    - Images are scaled to fit within 2048x2048
    - Then divided into 512x512 tiles
    - Each tile costs ~170 tokens (high detail)
    - Plus 85 base tokens

    For simplicity, we estimate based on file size as a proxy for image complexity.
    Larger files typically contain more detail and result in more tiles.

    Args:
        file_path: Path to the image file.

    Returns:
        Estimated input tokens for the image.
    """
    try:
        file_size = os.path.getsize(file_path)
    except OSError:
        # Default estimate if we can't read file size
        return BASE_IMAGE_TOKENS + TOKENS_PER_TILE * 4  # Assume 4 tiles

    # Estimate image dimensions based on file size
    # This is a rough heuristic: larger files typically mean larger/more detailed images
    # Average JPEG compression: ~0.5 bytes per pixel
    # Average PNG compression: ~2-3 bytes per pixel
    # We'll use a middle ground of ~1 byte per pixel
    estimated_pixels = file_size
    estimated_dimension = int(math.sqrt(estimated_pixels))

    # Cap at max dimension
    if estimated_dimension > MAX_IMAGE_DIMENSION:
        estimated_dimension = MAX_IMAGE_DIMENSION

    # Calculate number of tiles
    # Images are divided into 512x512 tiles
    tiles_per_side = max(1, math.ceil(estimated_dimension / TILE_SIZE))
    total_tiles = tiles_per_side * tiles_per_side

    # Calculate tokens
    return BASE_IMAGE_TOKENS + (TOKENS_PER_TILE * total_tiles)


def _estimate_pptx_image_count(file_path: Union[str, Path]) -> int:
    """
    Estimate the number of images in a PowerPoint file.

    This is a rough estimate based on file size, as reading the actual
    PPTX file would be slow for estimation purposes.

    The estimation uses a logarithmic scale for larger files to avoid
    overestimating. Large PPTX files often contain:
    - Embedded videos (very large, but not processed by LLM)
    - Charts and SmartArt (rendered as images but often small)
    - Text-heavy slides with templates
    - Master slide backgrounds (shared across slides)

    Therefore, file size does not scale linearly with image count.

    Args:
        file_path: Path to the PPTX file.

    Returns:
        Estimated number of images in the presentation.
    """
    try:
        file_size = os.path.getsize(file_path)
    except OSError:
        return 0

    size_mb = file_size / (1024 * 1024)

    # Estimation heuristic with logarithmic scaling for large files:
    #
    # Small files (<0.5MB): Likely no embedded images, just text/templates
    # Medium files (0.5-2MB): A few images, typical for basic presentations
    # Large files (2-10MB): More images, but also likely charts/SmartArt
    # Very large files (10-50MB): Often contain videos or many high-res images,
    #   but we cap growth to avoid massive overestimates
    # Huge files (>50MB): Likely dominated by video/audio, not more images
    #
    # Formula uses sqrt for diminishing returns on larger files.
    # This prevents a 50MB file from showing 75 images (old formula)
    # and instead shows ~15-20 images which is more realistic.

    if size_mb < 0.5:
        return 0  # Very small, likely no images
    elif size_mb < 1:
        return 1
    elif size_mb < 2:
        # Linear growth for small-medium files: 1-3 images
        return max(1, int(size_mb * 1.5))
    elif size_mb < 10:
        # Moderate growth: 3-10 images
        # Base of 2 images + sqrt scaling
        return max(2, int(2 + math.sqrt(size_mb) * 2))
    elif size_mb < 50:
        # Logarithmic growth for large files: 10-20 images
        # Large files often have videos, charts, not just photos
        return max(8, int(5 + math.log2(size_mb) * 3))
    else:
        # Cap for very large files: ~20-25 images max
        # Files >50MB are usually dominated by video/audio content
        return max(15, min(25, int(10 + math.log2(size_mb) * 2.5)))


def estimate_file_tokens(
    file_path: Union[str, Path],
    *,
    cache: Optional["ConversionCache"] = None,
    is_resumed: bool = False,
) -> FileTokenEstimate:
    """
    Estimate the LLM tokens that will be used to convert a file.

    Args:
        file_path: Path to the file to estimate.
        cache: Optional cache to check if file is already cached.
        is_resumed: Whether the file's output already exists (resume mode).

    Returns:
        FileTokenEstimate with the estimated token usage.
    """
    file_path = Path(file_path)
    source_str = str(file_path)
    extension = file_path.suffix.lower()

    # Check file size
    try:
        file_size = os.path.getsize(file_path)
    except OSError:
        file_size = 0

    # Check if file is resumed (output already exists)
    if is_resumed:
        return FileTokenEstimate(
            source_path=source_str,
            category=FileCategory.RESUMED,
            file_size_bytes=file_size,
            skip_reason="Output file already exists",
        )

    # Check if file is cached
    if cache is not None:
        try:
            if cache.has(source_str):
                return FileTokenEstimate(
                    source_path=source_str,
                    category=FileCategory.CACHED,
                    file_size_bytes=file_size,
                    skip_reason="File is cached",
                )
        except Exception:
            pass  # Cache check failed, proceed with estimation

    # Estimate based on file type
    if extension in IMAGE_EXTENSIONS:
        # Standalone image file
        image_tokens = _estimate_image_tokens(file_path)
        return FileTokenEstimate(
            source_path=source_str,
            category=FileCategory.IMAGE,
            input_tokens=image_tokens + DEFAULT_PROMPT_TOKENS,
            output_tokens=DEFAULT_OUTPUT_TOKENS,
            image_count=1,
            file_size_bytes=file_size,
        )

    elif extension in PPTX_EXTENSIONS:
        # PowerPoint file with potential embedded images
        image_count = _estimate_pptx_image_count(file_path)
        if image_count > 0:
            # Each image in PPTX gets processed similar to standalone images
            # Estimate average tokens per embedded image (smaller than standalone)
            avg_image_tokens = BASE_IMAGE_TOKENS + TOKENS_PER_TILE * 2  # Assume 2 tiles avg
            total_input = (avg_image_tokens + DEFAULT_PROMPT_TOKENS) * image_count
            total_output = DEFAULT_OUTPUT_TOKENS * image_count

            return FileTokenEstimate(
                source_path=source_str,
                category=FileCategory.PPTX,
                input_tokens=total_input,
                output_tokens=total_output,
                image_count=image_count,
                file_size_bytes=file_size,
            )
        else:
            return FileTokenEstimate(
                source_path=source_str,
                category=FileCategory.NO_LLM,
                file_size_bytes=file_size,
                skip_reason="PowerPoint has no estimated images",
            )

    else:
        # File type that doesn't use LLM
        return FileTokenEstimate(
            source_path=source_str,
            category=FileCategory.NO_LLM,
            file_size_bytes=file_size,
            skip_reason="File type does not use LLM",
        )


def estimate_batch_tokens(
    files: List[Union[str, Path]],
    *,
    cache: Optional["ConversionCache"] = None,
    resumed_files: Optional[Dict[str, Path]] = None,
) -> BatchTokenEstimate:
    """
    Estimate the LLM tokens that will be used for a batch conversion.

    Args:
        files: List of file paths to estimate.
        cache: Optional cache to check if files are already cached.
        resumed_files: Optional dict mapping source paths to existing output paths.

    Returns:
        BatchTokenEstimate with per-file and total estimates.
    """
    resumed_files = resumed_files or {}

    batch_estimate = BatchTokenEstimate()

    for file_path in files:
        file_str = str(file_path)
        is_resumed = file_str in resumed_files

        estimate = estimate_file_tokens(
            file_path,
            cache=cache,
            is_resumed=is_resumed,
        )
        batch_estimate.files.append(estimate)

    return batch_estimate
