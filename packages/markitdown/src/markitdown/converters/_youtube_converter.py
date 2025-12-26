import json
import time
import re
import bs4
from typing import Any, BinaryIO, Dict, List, Union
from urllib.parse import parse_qs, urlparse, unquote

from .._base_converter import DocumentConverter, DocumentConverterResult
from .._stream_info import StreamInfo
from .._conversion_quality import (
    ConversionQuality,
    FormattingLossType,
    WarningSeverity,
)

# Optional YouTube transcription support
try:
    # Suppress some warnings on library import
    import warnings

    with warnings.catch_warnings():
        warnings.filterwarnings("ignore", category=SyntaxWarning)
        # Patch submitted upstream to fix the SyntaxWarning
        from youtube_transcript_api import YouTubeTranscriptApi

    IS_YOUTUBE_TRANSCRIPT_CAPABLE = True
except ModuleNotFoundError:
    IS_YOUTUBE_TRANSCRIPT_CAPABLE = False


ACCEPTED_MIME_TYPE_PREFIXES = [
    "text/html",
    "application/xhtml",
]

ACCEPTED_FILE_EXTENSIONS = [
    ".html",
    ".htm",
]


class YouTubeConverter(DocumentConverter):
    """Handle YouTube specially, focusing on the video title, description, and transcript."""

    def accepts(
        self,
        file_stream: BinaryIO,
        stream_info: StreamInfo,
        **kwargs: Any,  # Options to pass to the converter
    ) -> bool:
        """
        Make sure we're dealing with HTML content *from* YouTube.
        """
        url = stream_info.url or ""
        mimetype = (stream_info.mimetype or "").lower()
        extension = (stream_info.extension or "").lower()

        url = unquote(url)
        url = url.replace(r"\?", "?").replace(r"\=", "=")

        if not url.startswith("https://www.youtube.com/watch?"):
            # Not a YouTube URL
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
        # Quality tracking
        quality = ConversionQuality(confidence=0.8)
        has_title = False
        has_description = False
        has_transcript = False
        has_views = False
        has_runtime = False
        transcript_error = None

        # Parse the stream
        encoding = "utf-8" if stream_info.charset is None else stream_info.charset
        soup = bs4.BeautifulSoup(file_stream, "html.parser", from_encoding=encoding)

        # Read the meta tags
        metadata: Dict[str, str] = {}

        if soup.title and soup.title.string:
            metadata["title"] = soup.title.string

        for meta in soup(["meta"]):
            if not isinstance(meta, bs4.Tag):
                continue

            for a in meta.attrs:
                if a in ["itemprop", "property", "name"]:
                    key = str(meta.get(a, ""))
                    content = str(meta.get("content", ""))
                    if key and content:  # Only add non-empty content
                        metadata[key] = content
                    break

        # Try reading the description
        try:
            for script in soup(["script"]):
                if not isinstance(script, bs4.Tag):
                    continue
                if not script.string:  # Skip empty scripts
                    continue
                content = script.string
                if "ytInitialData" in content:
                    match = re.search(r"var ytInitialData = ({.*?});", content)
                    if match:
                        data = json.loads(match.group(1))
                        attrdesc = self._findKey(data, "attributedDescriptionBodyText")
                        if attrdesc and isinstance(attrdesc, dict):
                            metadata["description"] = str(attrdesc.get("content", ""))
                    break
        except Exception as e:
            quality.add_warning(
                f"Error extracting full description: {e}",
                severity=WarningSeverity.LOW,
            )

        # Start preparing the page
        webpage_text = "# YouTube\n"

        title = self._get(metadata, ["title", "og:title", "name"])  # type: ignore
        assert isinstance(title, str)

        if title:
            has_title = True
            webpage_text += f"\n## {title}\n"

        stats = ""
        views = self._get(metadata, ["interactionCount"])  # type: ignore
        if views:
            has_views = True
            stats += f"- **Views:** {views}\n"

        keywords = self._get(metadata, ["keywords"])  # type: ignore
        if keywords:
            stats += f"- **Keywords:** {keywords}\n"

        runtime = self._get(metadata, ["duration"])  # type: ignore
        if runtime:
            has_runtime = True
            stats += f"- **Runtime:** {runtime}\n"

        if len(stats) > 0:
            webpage_text += f"\n### Video Metadata\n{stats}\n"

        description = self._get(metadata, ["description", "og:description"])  # type: ignore
        if description:
            has_description = True
            webpage_text += f"\n### Description\n{description}\n"

        # Track transcript availability
        quality.set_optional_feature("youtube_transcript_api", IS_YOUTUBE_TRANSCRIPT_CAPABLE)

        if IS_YOUTUBE_TRANSCRIPT_CAPABLE:
            ytt_api = YouTubeTranscriptApi()
            transcript_text = ""
            parsed_url = urlparse(stream_info.url)  # type: ignore
            params = parse_qs(parsed_url.query)  # type: ignore
            if "v" in params and params["v"][0]:
                video_id = str(params["v"][0])
                quality.set_metric("video_id", video_id)

                try:
                    transcript_list = ytt_api.list(video_id)
                    languages = ["en"]
                    for transcript in transcript_list:
                        languages.append(transcript.language_code)
                        break

                    youtube_transcript_languages = kwargs.get(
                        "youtube_transcript_languages", languages
                    )
                    # Retry the transcript fetching operation
                    transcript = self._retry_operation(
                        lambda: ytt_api.fetch(
                            video_id, languages=youtube_transcript_languages
                        ),
                        retries=3,  # Retry 3 times
                        delay=2,  # 2 seconds delay between retries
                    )

                    if transcript:
                        transcript_text = " ".join(
                            [part.text for part in transcript]
                        )  # type: ignore
                except Exception as e:
                    transcript_error = str(e)
                    # Try translation fallback
                    try:
                        if 'transcript_list' in dir() and 'languages' in dir():
                            transcript = (
                                transcript_list.find_transcript(languages)
                                .translate(youtube_transcript_languages[0])
                                .fetch()
                            )
                            transcript_text = " ".join([part.text for part in transcript])
                            transcript_error = None  # Cleared since fallback worked
                    except Exception:
                        pass

                if transcript_text:
                    has_transcript = True
                    webpage_text += f"\n### Transcript\n{transcript_text}\n"
                    quality.set_metric("transcript_length", len(transcript_text))
        else:
            quality.add_warning(
                "youtube_transcript_api not installed. Transcript extraction is disabled.",
                severity=WarningSeverity.MEDIUM,
            )

        title = title if title else (soup.title.string if soup.title else "")
        assert isinstance(title, str)

        # Build quality report
        quality.set_metric("has_title", has_title)
        quality.set_metric("has_description", has_description)
        quality.set_metric("has_transcript", has_transcript)
        quality.set_metric("has_views", has_views)
        quality.set_metric("has_runtime", has_runtime)

        if transcript_error:
            quality.set_metric("transcript_error", transcript_error)
            quality.add_warning(
                f"Transcript could not be fetched: {transcript_error}",
                severity=WarningSeverity.MEDIUM,
            )

        if not has_title:
            quality.add_warning(
                "Video title could not be extracted.",
                severity=WarningSeverity.MEDIUM,
            )

        if not has_description:
            quality.add_warning(
                "Video description could not be extracted.",
                severity=WarningSeverity.LOW,
            )

        if not has_transcript:
            if IS_YOUTUBE_TRANSCRIPT_CAPABLE and not transcript_error:
                quality.add_warning(
                    "No transcript available for this video.",
                    severity=WarningSeverity.INFO,
                )
            quality.confidence = max(0.5, quality.confidence - 0.2)
        else:
            quality.confidence = 0.9

        # YouTube-specific notes
        quality.add_warning(
            "Video content itself cannot be converted to markdown. Only metadata and transcript are extracted.",
            severity=WarningSeverity.INFO,
            formatting_type=FormattingLossType.VIDEO,
        )

        quality.add_warning(
            "Comments and related videos are not extracted.",
            severity=WarningSeverity.INFO,
        )

        return DocumentConverterResult(
            markdown=webpage_text,
            title=title,
            quality=quality,
        )

    def _get(
        self,
        metadata: Dict[str, str],
        keys: List[str],
        default: Union[str, None] = None,
    ) -> Union[str, None]:
        """Get first non-empty value from metadata matching given keys."""
        for k in keys:
            if k in metadata:
                return metadata[k]
        return default

    def _findKey(self, json: Any, key: str) -> Union[str, None]:  # TODO: Fix json type
        """Recursively search for a key in nested dictionary/list structures."""
        if isinstance(json, list):
            for elm in json:
                ret = self._findKey(elm, key)
                if ret is not None:
                    return ret
        elif isinstance(json, dict):
            for k, v in json.items():
                if k == key:
                    return json[k]
                if result := self._findKey(v, key):
                    return result
        return None

    def _retry_operation(self, operation, retries=3, delay=2):
        """Retries the operation if it fails."""
        attempt = 0
        while attempt < retries:
            try:
                return operation()  # Attempt the operation
            except Exception as e:
                print(f"Attempt {attempt + 1} failed: {e}")
                if attempt < retries - 1:
                    time.sleep(delay)  # Wait before retrying
                attempt += 1
        # If all attempts fail, raise the last exception
        raise Exception(f"Operation failed after {retries} attempts.")
