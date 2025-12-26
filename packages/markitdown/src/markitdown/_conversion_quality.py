# SPDX-FileCopyrightText: 2024-present Adam Fourney <adamfo@microsoft.com>
#
# SPDX-License-Identifier: MIT

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional


class FormattingLossType(Enum):
    """Types of formatting or content that may be lost during conversion."""

    # Tables and data structures
    TABLE = "table"
    TABLE_FORMATTING = "table_formatting"  # e.g., merged cells, cell styling
    SPREADSHEET_FORMULA = "spreadsheet_formula"

    # Media content
    IMAGE = "image"
    IMAGE_DESCRIPTION = "image_description"  # Alt text or caption missing
    AUDIO = "audio"
    VIDEO = "video"
    EMBEDDED_OBJECT = "embedded_object"

    # Text formatting
    FONT_STYLE = "font_style"  # Bold, italic, underline that couldn't be preserved
    TEXT_COLOR = "text_color"
    HIGHLIGHT = "highlight"
    HEADING_LEVEL = "heading_level"

    # Document structure
    HEADER_FOOTER = "header_footer"
    PAGE_BREAK = "page_break"
    FOOTNOTE = "footnote"
    ENDNOTE = "endnote"
    COMMENT = "comment"
    TOC = "table_of_contents"

    # Advanced content
    MATH_EQUATION = "math_equation"
    CHART = "chart"
    DIAGRAM = "diagram"
    SMART_ART = "smart_art"

    # Links and references
    HYPERLINK = "hyperlink"
    CROSS_REFERENCE = "cross_reference"
    BOOKMARK = "bookmark"

    # Other
    CUSTOM_STYLE = "custom_style"
    MACRO = "macro"
    FORM_FIELD = "form_field"
    TRACKED_CHANGES = "tracked_changes"


class WarningSeverity(Enum):
    """Severity levels for conversion warnings."""

    INFO = "info"  # Informational, no data loss expected
    LOW = "low"  # Minor formatting loss, content preserved
    MEDIUM = "medium"  # Some content may be degraded or simplified
    HIGH = "high"  # Significant content or formatting loss


@dataclass
class ConversionWarning:
    """Represents a warning about potential issues during conversion."""

    message: str
    severity: WarningSeverity = WarningSeverity.LOW
    formatting_type: Optional[FormattingLossType] = None
    element_count: Optional[int] = None  # Number of affected elements
    details: Optional[Dict[str, Any]] = None

    def __str__(self) -> str:
        parts = [f"[{self.severity.value.upper()}] {self.message}"]
        if self.element_count is not None:
            parts.append(f" (affects {self.element_count} element(s))")
        return "".join(parts)


@dataclass
class ConversionQuality:
    """
    Metadata about the quality of a document conversion.

    This class provides information about:
    - Overall confidence in the conversion quality
    - Specific warnings about potential issues
    - Details about what formatting or content may have been lost
    - Information about the conversion process itself
    """

    # Confidence score from 0.0 to 1.0
    # 1.0 = perfect conversion expected
    # 0.0 = conversion likely has significant issues
    confidence: float = 1.0

    # List of warnings about potential issues
    warnings: List[ConversionWarning] = field(default_factory=list)

    # Summary of formatting types that were lost or degraded
    formatting_loss: List[FormattingLossType] = field(default_factory=list)

    # Converter-specific metrics
    metrics: Dict[str, Any] = field(default_factory=dict)

    # The converter type that was used
    converter_used: Optional[str] = None

    # Whether optional features were available
    optional_features_used: Dict[str, bool] = field(default_factory=dict)

    # Whether the conversion was complete or partial
    is_partial: bool = False

    # If partial, what percentage was converted (0-100)
    completion_percentage: Optional[float] = None

    def add_warning(
        self,
        message: str,
        severity: WarningSeverity = WarningSeverity.LOW,
        formatting_type: Optional[FormattingLossType] = None,
        element_count: Optional[int] = None,
        details: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Add a warning to the quality report."""
        warning = ConversionWarning(
            message=message,
            severity=severity,
            formatting_type=formatting_type,
            element_count=element_count,
            details=details,
        )
        self.warnings.append(warning)

        # Also track the formatting type if provided
        if formatting_type is not None and formatting_type not in self.formatting_loss:
            self.formatting_loss.append(formatting_type)

    def add_formatting_loss(self, loss_type: FormattingLossType) -> None:
        """Record that a type of formatting was lost."""
        if loss_type not in self.formatting_loss:
            self.formatting_loss.append(loss_type)

    def set_metric(self, key: str, value: Any) -> None:
        """Set a converter-specific metric."""
        self.metrics[key] = value

    def set_optional_feature(self, feature: str, used: bool) -> None:
        """Record whether an optional feature was available/used."""
        self.optional_features_used[feature] = used

    @property
    def has_warnings(self) -> bool:
        """Check if there are any warnings."""
        return len(self.warnings) > 0

    @property
    def highest_severity(self) -> Optional[WarningSeverity]:
        """Get the highest severity level among all warnings."""
        if not self.warnings:
            return None

        severity_order = [
            WarningSeverity.INFO,
            WarningSeverity.LOW,
            WarningSeverity.MEDIUM,
            WarningSeverity.HIGH,
        ]
        max_severity = WarningSeverity.INFO
        for warning in self.warnings:
            if severity_order.index(warning.severity) > severity_order.index(
                max_severity
            ):
                max_severity = warning.severity
        return max_severity

    def get_warnings_by_severity(
        self, severity: WarningSeverity
    ) -> List[ConversionWarning]:
        """Get all warnings of a specific severity."""
        return [w for w in self.warnings if w.severity == severity]

    def to_dict(self) -> Dict[str, Any]:
        """Convert to a dictionary for serialization."""
        return {
            "confidence": self.confidence,
            "warnings": [
                {
                    "message": w.message,
                    "severity": w.severity.value,
                    "formatting_type": w.formatting_type.value
                    if w.formatting_type
                    else None,
                    "element_count": w.element_count,
                    "details": w.details,
                }
                for w in self.warnings
            ],
            "formatting_loss": [f.value for f in self.formatting_loss],
            "metrics": self.metrics,
            "converter_used": self.converter_used,
            "optional_features_used": self.optional_features_used,
            "is_partial": self.is_partial,
            "completion_percentage": self.completion_percentage,
        }

    def __str__(self) -> str:
        """Return a human-readable summary."""
        lines = []
        lines.append(f"Conversion Quality: {self.confidence:.0%} confidence")

        if self.converter_used:
            lines.append(f"Converter: {self.converter_used}")

        if self.is_partial:
            if self.completion_percentage is not None:
                lines.append(
                    f"Status: Partial conversion ({self.completion_percentage:.0f}% complete)"
                )
            else:
                lines.append("Status: Partial conversion")
        else:
            lines.append("Status: Complete")

        if self.formatting_loss:
            loss_names = [f.value for f in self.formatting_loss]
            lines.append(f"Formatting loss: {', '.join(loss_names)}")

        if self.optional_features_used:
            used = [k for k, v in self.optional_features_used.items() if v]
            not_used = [k for k, v in self.optional_features_used.items() if not v]
            if used:
                lines.append(f"Optional features used: {', '.join(used)}")
            if not_used:
                lines.append(f"Optional features not available: {', '.join(not_used)}")

        if self.warnings:
            lines.append(f"\nWarnings ({len(self.warnings)}):")
            for warning in self.warnings:
                lines.append(f"  - {warning}")

        return "\n".join(lines)
