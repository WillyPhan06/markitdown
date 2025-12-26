from typing import Any, BinaryIO

from ._exiftool import exiftool_metadata
from ._transcribe_audio import transcribe_audio
from .._base_converter import DocumentConverter, DocumentConverterResult
from .._stream_info import StreamInfo
from .._exceptions import MissingDependencyException
from .._conversion_quality import (
    ConversionQuality,
    FormattingLossType,
    WarningSeverity,
)

ACCEPTED_MIME_TYPE_PREFIXES = [
    "audio/x-wav",
    "audio/mpeg",
    "video/mp4",
]

ACCEPTED_FILE_EXTENSIONS = [
    ".wav",
    ".mp3",
    ".m4a",
    ".mp4",
]


class AudioConverter(DocumentConverter):
    """
    Converts audio files to markdown via extraction of metadata (if `exiftool` is installed), and speech transcription (if `speech_recognition` is installed).
    """

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
        md_content = ""

        # Quality tracking
        quality = ConversionQuality(confidence=0.8)
        metadata_extracted = False
        transcription_succeeded = False
        transcription_attempted = False
        transcription_error = None

        # Add metadata
        metadata = exiftool_metadata(
            file_stream, exiftool_path=kwargs.get("exiftool_path")
        )

        exiftool_available = kwargs.get("exiftool_path") is not None or metadata
        quality.set_optional_feature("exiftool_metadata", exiftool_available)

        metadata_fields_found = []
        if metadata:
            metadata_extracted = True
            for f in [
                "Title",
                "Artist",
                "Author",
                "Band",
                "Album",
                "Genre",
                "Track",
                "DateTimeOriginal",
                "CreateDate",
                # "Duration", -- Wrong values when read from memory
                "NumChannels",
                "SampleRate",
                "AvgBytesPerSec",
                "BitsPerSample",
            ]:
                if f in metadata:
                    md_content += f"{f}: {metadata[f]}\n"
                    metadata_fields_found.append(f)

            quality.set_metric("metadata_fields", metadata_fields_found)
        else:
            quality.add_warning(
                "No metadata could be extracted from the audio file (exiftool may not be available).",
                severity=WarningSeverity.LOW,
            )

        # Figure out the audio format for transcription
        if stream_info.extension == ".wav" or stream_info.mimetype == "audio/x-wav":
            audio_format = "wav"
        elif stream_info.extension == ".mp3" or stream_info.mimetype == "audio/mpeg":
            audio_format = "mp3"
        elif (
            stream_info.extension in [".mp4", ".m4a"]
            or stream_info.mimetype == "video/mp4"
        ):
            audio_format = "mp4"
        else:
            audio_format = None

        quality.set_metric("audio_format", audio_format)

        # Transcribe
        if audio_format:
            transcription_attempted = True
            try:
                transcript = transcribe_audio(file_stream, audio_format=audio_format)
                if transcript:
                    transcription_succeeded = True
                    md_content += "\n\n### Audio Transcript:\n" + transcript
                    quality.set_metric("transcript_length", len(transcript))
                else:
                    quality.add_warning(
                        "Transcription returned empty result.",
                        severity=WarningSeverity.MEDIUM,
                    )
            except MissingDependencyException as e:
                transcription_error = "missing_dependency"
                quality.add_warning(
                    "Speech recognition library not installed. Transcription not available.",
                    severity=WarningSeverity.MEDIUM,
                )
                quality.set_optional_feature("speech_recognition", False)
            except Exception as e:
                transcription_error = str(e)
                quality.add_warning(
                    f"Transcription failed: {e}",
                    severity=WarningSeverity.MEDIUM,
                )
        else:
            quality.add_warning(
                "Audio format could not be determined. Transcription skipped.",
                severity=WarningSeverity.LOW,
            )

        # Record transcription status
        quality.set_metric("transcription_attempted", transcription_attempted)
        quality.set_metric("transcription_succeeded", transcription_succeeded)
        quality.set_metric("has_metadata", metadata_extracted)

        if transcription_error:
            quality.set_metric("transcription_error", transcription_error)

        # Set optional feature status for transcription
        if transcription_succeeded:
            quality.set_optional_feature("speech_recognition", True)

        # Adjust confidence based on results
        if not metadata_extracted and not transcription_succeeded:
            quality.confidence = 0.3
            quality.add_warning(
                "No metadata or transcription could be extracted. Output may be minimal.",
                severity=WarningSeverity.HIGH,
            )
        elif not transcription_succeeded and transcription_attempted:
            quality.confidence = 0.6
        elif transcription_succeeded:
            quality.confidence = 0.9

        # Note about audio conversion limitations
        quality.add_warning(
            "Audio waveform and timing information are not preserved.",
            severity=WarningSeverity.INFO,
            formatting_type=FormattingLossType.AUDIO,
        )

        # Return the result
        return DocumentConverterResult(markdown=md_content.strip(), quality=quality)
