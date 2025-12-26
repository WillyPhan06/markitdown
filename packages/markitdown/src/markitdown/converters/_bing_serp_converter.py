import re
import base64
import binascii
from urllib.parse import parse_qs, urlparse
from typing import Any, BinaryIO
from bs4 import BeautifulSoup

from .._base_converter import DocumentConverter, DocumentConverterResult
from .._stream_info import StreamInfo
from ._markdownify import _CustomMarkdownify
from .._conversion_quality import (
    ConversionQuality,
    FormattingLossType,
    WarningSeverity,
)

ACCEPTED_MIME_TYPE_PREFIXES = [
    "text/html",
    "application/xhtml",
]

ACCEPTED_FILE_EXTENSIONS = [
    ".html",
    ".htm",
]


class BingSerpConverter(DocumentConverter):
    """
    Handle Bing results pages (only the organic search results).
    NOTE: It is better to use the Bing API
    """

    def accepts(
        self,
        file_stream: BinaryIO,
        stream_info: StreamInfo,
        **kwargs: Any,  # Options to pass to the converter
    ) -> bool:
        """
        Make sure we're dealing with HTML content *from* Bing.
        """

        url = stream_info.url or ""
        mimetype = (stream_info.mimetype or "").lower()
        extension = (stream_info.extension or "").lower()

        if not re.search(r"^https://www\.bing\.com/search\?q=", url):
            # Not a Bing SERP URL
            return False

        if extension in ACCEPTED_FILE_EXTENSIONS:
            return True

        for prefix in ACCEPTED_MIME_TYPE_PREFIXES:
            if mimetype.startswith(prefix):
                return True

        # Not HTML content
        return False

    def convert(
        self,
        file_stream: BinaryIO,
        stream_info: StreamInfo,
        **kwargs: Any,  # Options to pass to the converter
    ) -> DocumentConverterResult:
        assert stream_info.url is not None

        # Quality tracking
        quality = ConversionQuality(confidence=0.8)
        results_detected = 0
        results_extracted = 0
        results_with_url = 0
        url_decode_failures = 0

        # Parse the query parameters
        parsed_params = parse_qs(urlparse(stream_info.url).query)
        query = parsed_params.get("q", [""])[0]

        # Parse the stream
        encoding = "utf-8" if stream_info.charset is None else stream_info.charset
        soup = BeautifulSoup(file_stream, "html.parser", from_encoding=encoding)

        # Clean up some formatting
        for tptt in soup.find_all(class_="tptt"):
            if hasattr(tptt, "string") and tptt.string:
                tptt.string += " "
        for slug in soup.find_all(class_="algoSlug_icon"):
            slug.extract()

        # Parse the algorithmic results
        _markdownify = _CustomMarkdownify(**kwargs)
        results = list()
        all_results = soup.find_all(class_="b_algo")
        results_detected = len(all_results)

        for result in all_results:
            if not hasattr(result, "find_all"):
                continue

            # Rewrite redirect urls
            has_valid_url = False
            for a in result.find_all("a", href=True):
                parsed_href = urlparse(a["href"])
                qs = parse_qs(parsed_href.query)

                # The destination is contained in the u parameter,
                # but appears to be base64 encoded, with some prefix
                if "u" in qs:
                    u = (
                        qs["u"][0][2:].strip() + "=="
                    )  # Python 3 doesn't care about extra padding

                    try:
                        # RFC 4648 / Base64URL" variant, which uses "-" and "_"
                        a["href"] = base64.b64decode(u, altchars="-_").decode("utf-8")
                        has_valid_url = True
                    except UnicodeDecodeError:
                        url_decode_failures += 1
                    except binascii.Error:
                        url_decode_failures += 1
                else:
                    # Direct URL, no decoding needed
                    has_valid_url = True

            if has_valid_url:
                results_with_url += 1

            # Convert to markdown
            md_result = _markdownify.convert_soup(result).strip()
            lines = [line.strip() for line in re.split(r"\n+", md_result)]
            result_text = "\n".join([line for line in lines if len(line) > 0])
            if result_text:
                results.append(result_text)
                results_extracted += 1

        webpage_text = (
            f"## A Bing search for '{query}' found the following results:\n\n"
            + "\n\n".join(results)
        )

        # Build quality report
        quality.set_metric("search_query", query)
        quality.set_metric("results_detected", results_detected)
        quality.set_metric("results_extracted", results_extracted)
        quality.set_metric("results_with_url", results_with_url)

        if results_detected == 0:
            quality.add_warning(
                "No search results found on the page.",
                severity=WarningSeverity.HIGH,
            )
            quality.confidence = 0.4
        elif results_extracted < results_detected:
            skipped = results_detected - results_extracted
            quality.add_warning(
                f"{skipped} search result(s) could not be extracted.",
                severity=WarningSeverity.MEDIUM,
                element_count=skipped,
            )

        if url_decode_failures > 0:
            quality.add_warning(
                f"{url_decode_failures} URL(s) could not be decoded from Bing redirect format.",
                severity=WarningSeverity.LOW,
                formatting_type=FormattingLossType.HYPERLINK,
                element_count=url_decode_failures,
            )

        # SERP-specific notes
        quality.add_warning(
            "Only organic search results are extracted. Ads, featured snippets, and other elements are excluded.",
            severity=WarningSeverity.INFO,
        )

        quality.add_warning(
            "Rich result features (images, ratings, etc.) may not be fully preserved.",
            severity=WarningSeverity.INFO,
            formatting_type=FormattingLossType.EMBEDDED_OBJECT,
        )

        return DocumentConverterResult(
            markdown=webpage_text,
            title=None if soup.title is None else soup.title.string,
            quality=quality,
        )
