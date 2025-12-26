from typing import BinaryIO, Any, Union
import base64
import mimetypes
from ._exiftool import exiftool_metadata
from .._base_converter import DocumentConverter, DocumentConverterResult
from .._stream_info import StreamInfo
from .._conversion_quality import (
    ConversionQuality,
    FormattingLossType,
    WarningSeverity,
)

ACCEPTED_MIME_TYPE_PREFIXES = [
    "image/jpeg",
    "image/png",
]

ACCEPTED_FILE_EXTENSIONS = [".jpg", ".jpeg", ".png"]


class ImageConverter(DocumentConverter):
    """
    Converts images to markdown via extraction of metadata (if `exiftool` is installed), and description via a multimodal LLM (if an llm_client is configured).
    """

    def accepts(
        self,
        file_stream: BinaryIO,
        stream_info: StreamInfo,
        **kwargs: Any,
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
        quality = ConversionQuality(confidence=0.9)
        metadata_extracted = False
        llm_description_generated = False

        # Add metadata
        metadata = exiftool_metadata(
            file_stream, exiftool_path=kwargs.get("exiftool_path")
        )

        exiftool_available = kwargs.get("exiftool_path") is not None or metadata
        quality.set_optional_feature("exiftool_metadata", exiftool_available)

        if metadata:
            metadata_extracted = True
            metadata_fields_found = []
            for f in [
                "ImageSize",
                "Title",
                "Caption",
                "Description",
                "Keywords",
                "Artist",
                "Author",
                "DateTimeOriginal",
                "CreateDate",
                "GPSPosition",
            ]:
                if f in metadata:
                    md_content += f"{f}: {metadata[f]}\n"
                    metadata_fields_found.append(f)

            quality.set_metric("metadata_fields", metadata_fields_found)
        else:
            quality.add_warning(
                "No metadata could be extracted from the image (exiftool may not be available).",
                severity=WarningSeverity.LOW,
            )

        # Try describing the image with GPT
        llm_client = kwargs.get("llm_client")
        llm_model = kwargs.get("llm_model")
        llm_available = llm_client is not None and llm_model is not None
        quality.set_optional_feature("llm_description", llm_available)

        if llm_available:
            llm_description = self._get_llm_description(
                file_stream,
                stream_info,
                client=llm_client,
                model=llm_model,
                prompt=kwargs.get("llm_prompt"),
            )

            if llm_description is not None:
                md_content += "\n# Description:\n" + llm_description.strip() + "\n"
                llm_description_generated = True
            else:
                quality.add_warning(
                    "Failed to generate LLM description for the image.",
                    severity=WarningSeverity.LOW,
                    formatting_type=FormattingLossType.IMAGE_DESCRIPTION,
                )
        else:
            quality.add_warning(
                "LLM client not configured. Image content description not available.",
                severity=WarningSeverity.MEDIUM,
                formatting_type=FormattingLossType.IMAGE_DESCRIPTION,
            )

        # Adjust confidence based on what was extracted
        if not metadata_extracted and not llm_description_generated:
            quality.confidence = 0.3
            quality.add_warning(
                "No metadata or description could be extracted. Output may be empty.",
                severity=WarningSeverity.HIGH,
            )
        elif not llm_description_generated:
            quality.confidence = 0.6

        quality.set_metric("has_metadata", metadata_extracted)
        quality.set_metric("has_llm_description", llm_description_generated)

        return DocumentConverterResult(markdown=md_content, quality=quality)

    def _get_llm_description(
        self,
        file_stream: BinaryIO,
        stream_info: StreamInfo,
        *,
        client,
        model,
        prompt=None,
    ) -> Union[None, str]:
        if prompt is None or prompt.strip() == "":
            prompt = "Write a detailed caption for this image."

        # Get the content type
        content_type = stream_info.mimetype
        if not content_type:
            content_type, _ = mimetypes.guess_type(
                "_dummy" + (stream_info.extension or "")
            )
        if not content_type:
            content_type = "application/octet-stream"

        # Convert to base64
        cur_pos = file_stream.tell()
        try:
            base64_image = base64.b64encode(file_stream.read()).decode("utf-8")
        except Exception as e:
            return None
        finally:
            file_stream.seek(cur_pos)

        # Prepare the data-uri
        data_uri = f"data:{content_type};base64,{base64_image}"

        # Prepare the OpenAI API request
        messages = [
            {
                "role": "user",
                "content": [
                    {"type": "text", "text": prompt},
                    {
                        "type": "image_url",
                        "image_url": {
                            "url": data_uri,
                        },
                    },
                ],
            }
        ]

        # Call the OpenAI API
        response = client.chat.completions.create(model=model, messages=messages)
        return response.choices[0].message.content
