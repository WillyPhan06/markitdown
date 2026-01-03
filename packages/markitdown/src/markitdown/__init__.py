# SPDX-FileCopyrightText: 2024-present Adam Fourney <adamfo@microsoft.com>
#
# SPDX-License-Identifier: MIT

from .__about__ import __version__
from ._markitdown import (
    MarkItDown,
    PRIORITY_SPECIFIC_FILE_FORMAT,
    PRIORITY_GENERIC_FILE_FORMAT,
)
from ._base_converter import DocumentConverterResult, DocumentConverter
from ._stream_info import StreamInfo
from ._exceptions import (
    MarkItDownException,
    MissingDependencyException,
    FailedConversionAttempt,
    FileConversionException,
    UnsupportedFormatException,
)
from ._conversion_quality import (
    ConversionQuality,
    ConversionWarning,
    FormattingLossType,
    WarningSeverity,
)
from ._batch import (
    BatchConversionResult,
    BatchItemResult,
    BatchItemStatus,
    convert_batch,
    convert_directory,
    write_batch_results,
)
from ._cache import ConversionCache
from ._document_metadata import DocumentMetadata
from ._token_estimator import (
    estimate_batch_tokens,
    estimate_file_tokens,
    BatchTokenEstimate,
    FileTokenEstimate,
    FileCategory,
)

__all__ = [
    "__version__",
    "MarkItDown",
    "DocumentConverter",
    "DocumentConverterResult",
    "MarkItDownException",
    "MissingDependencyException",
    "FailedConversionAttempt",
    "FileConversionException",
    "UnsupportedFormatException",
    "StreamInfo",
    "PRIORITY_SPECIFIC_FILE_FORMAT",
    "PRIORITY_GENERIC_FILE_FORMAT",
    "ConversionQuality",
    "ConversionWarning",
    "FormattingLossType",
    "WarningSeverity",
    # Batch conversion
    "BatchConversionResult",
    "BatchItemResult",
    "BatchItemStatus",
    "convert_batch",
    "convert_directory",
    "write_batch_results",
    # Caching
    "ConversionCache",
    # Document metadata
    "DocumentMetadata",
    # Token estimation
    "estimate_batch_tokens",
    "estimate_file_tokens",
    "BatchTokenEstimate",
    "FileTokenEstimate",
    "FileCategory",
]
