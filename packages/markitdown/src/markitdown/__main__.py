# SPDX-FileCopyrightText: 2024-present Adam Fourney <adamfo@microsoft.com>
#
# SPDX-License-Identifier: MIT
import argparse
import json
import os
import sys
import codecs
import threading
from pathlib import Path
from textwrap import dedent
from importlib.metadata import entry_points
from .__about__ import __version__
from ._markitdown import MarkItDown, StreamInfo, DocumentConverterResult
from ._batch import BatchConversionResult, BatchItemResult, BatchItemStatus


def main():
    parser = argparse.ArgumentParser(
        description="Convert various file formats to markdown.",
        prog="markitdown",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        usage=dedent(
            """
            SYNTAX:

                markitdown <OPTIONAL: FILENAME>
                If FILENAME is empty, markitdown reads from stdin.

            SINGLE FILE EXAMPLES:

                markitdown example.pdf

                OR

                cat example.pdf | markitdown

                OR

                markitdown < example.pdf

                OR to save to a file use

                markitdown example.pdf -o example.md

                OR

                markitdown example.pdf > example.md

            BATCH/DIRECTORY EXAMPLES:

                # Convert all files in a directory
                markitdown --batch /path/to/documents -o /path/to/output

                # Convert specific file types only
                markitdown --batch /path/to/documents --include "*.pdf" --include "*.docx"

                # Convert multiple specific files
                markitdown --batch file1.pdf file2.docx file3.xlsx -o /path/to/output

                # Non-recursive directory scan
                markitdown --batch /path/to/documents --no-recursive

                # Show progress during batch conversion
                markitdown --batch /path/to/documents --progress

            CACHING EXAMPLES:

                # Enable caching to skip unchanged files on re-runs
                markitdown --batch /path/to/documents --cache --progress

                # Use a custom cache directory
                markitdown --batch /path/to/documents --cache-dir /tmp/my-cache

                # Clear the cache
                markitdown --clear-cache

            QUALITY MANIFEST EXAMPLES:

                # Export per-file quality metrics to a JSON manifest
                markitdown --batch /path/to/documents -o /output --export-manifest quality.json

                # Combine with progress and summary for full visibility
                markitdown --batch /path/to/documents --progress --summary --export-manifest report.json
            """
        ).strip(),
    )

    parser.add_argument(
        "-v",
        "--version",
        action="version",
        version=f"%(prog)s {__version__}",
        help="show the version number and exit",
    )

    parser.add_argument(
        "-o",
        "--output",
        help="Output file name. If not provided, output is written to stdout.",
    )

    parser.add_argument(
        "-x",
        "--extension",
        help="Provide a hint about the file extension (e.g., when reading from stdin).",
    )

    parser.add_argument(
        "-m",
        "--mime-type",
        help="Provide a hint about the file's MIME type.",
    )

    parser.add_argument(
        "-c",
        "--charset",
        help="Provide a hint about the file's charset (e.g, UTF-8).",
    )

    parser.add_argument(
        "-d",
        "--use-docintel",
        action="store_true",
        help="Use Document Intelligence to extract text instead of offline conversion. Requires a valid Document Intelligence Endpoint.",
    )

    parser.add_argument(
        "-e",
        "--endpoint",
        type=str,
        help="Document Intelligence Endpoint. Required if using Document Intelligence.",
    )

    parser.add_argument(
        "-p",
        "--use-plugins",
        action="store_true",
        help="Use 3rd-party plugins to convert files. Use --list-plugins to see installed plugins.",
    )

    parser.add_argument(
        "--list-plugins",
        action="store_true",
        help="List installed 3rd-party plugins. Plugins are loaded when using the -p or --use-plugin option.",
    )

    parser.add_argument(
        "--keep-data-uris",
        action="store_true",
        help="Keep data URIs (like base64-encoded images) in the output. By default, data URIs are truncated.",
    )

    parser.add_argument(
        "-q",
        "--quality",
        action="store_true",
        help="Show conversion quality information after the output.",
    )

    parser.add_argument(
        "--quality-json",
        action="store_true",
        help="Output conversion quality information as JSON to stderr.",
    )

    parser.add_argument(
        "--export-manifest",
        type=str,
        metavar="FILE",
        help=(
            "Export quality metrics for each file to a JSON manifest file. "
            "The manifest includes the source file path, conversion status, "
            "and detailed quality information (confidence, warnings, formatting losses) "
            "for each file individually. This makes it easy to review quality metrics "
            "per file after batch conversions. Only applies in batch mode."
        ),
    )

    # Batch conversion arguments
    parser.add_argument(
        "-b",
        "--batch",
        action="store_true",
        help=(
            "Enable batch mode for converting multiple files at once. "
            "Accepts any combination of: (1) a single directory to convert all files within, "
            "(2) multiple individual files, or (3) a mix of directories and files. "
            "Examples: '--batch /docs' converts all files in /docs; "
            "'--batch file1.pdf file2.docx' converts those two files; "
            "'--batch /docs report.pdf /other' converts files from both directories plus the PDF."
        ),
    )

    parser.add_argument(
        "--include",
        action="append",
        metavar="PATTERN",
        help=(
            "Only include files matching this glob pattern. Can be specified multiple times "
            "to include multiple patterns (files matching ANY pattern are included). "
            "Examples: '--include \"*.pdf\"' for PDFs only; "
            "'--include \"*.pdf\" --include \"*.docx\"' for both PDFs and Word docs; "
            "'--include \"report_*\"' for files starting with 'report_'."
        ),
    )

    parser.add_argument(
        "--exclude",
        action="append",
        metavar="PATTERN",
        help=(
            "Exclude files matching this glob pattern. Can be specified multiple times "
            "to exclude multiple patterns (files matching ANY pattern are excluded). "
            "Exclusions are applied after inclusions. "
            "Examples: '--exclude \"*.tmp\"' to skip temp files; "
            "'--exclude \".*\" --exclude \"~*\"' to skip hidden and backup files; "
            "'--exclude \"draft_*\"' to skip files starting with 'draft_'."
        ),
    )

    parser.add_argument(
        "--no-recursive",
        action="store_true",
        help=(
            "Do not search subdirectories recursively. When set, only files in the "
            "immediate directory are processed, not files in nested subdirectories. "
            "Only applies when a directory is provided to --batch."
        ),
    )

    parser.add_argument(
        "--parallel",
        type=int,
        metavar="N",
        help=(
            "Number of parallel worker threads for batch conversion. "
            "Default: 'auto' which uses min(32, cpu_count + 4) workers as determined "
            "by Python's ThreadPoolExecutor, optimized for I/O-bound tasks. "
            "Set to 1 for strictly sequential processing (useful for debugging or "
            "when processing order matters). Higher values may speed up conversion "
            "of many small files but won't help with a few large files."
        ),
    )

    parser.add_argument(
        "--progress",
        action="store_true",
        help=(
            "Show real-time progress updates to stderr as each file completes. "
            "Each line shows: [count] status_icon filepath (confidence%%). "
            "Status icons: ✓=success, ✗=failed, ?=unsupported format, ○=skipped. "
            "Output is flushed immediately so progress appears in real-time even "
            "when stderr is piped. Thread-safe when used with --parallel."
        ),
    )

    parser.add_argument(
        "--summary",
        action="store_true",
        help=(
            "Show a summary report after batch conversion completes, including "
            "total/successful/failed counts and overall quality metrics. "
            "Automatically enabled when --quality is used in batch mode."
        ),
    )

    # Cache arguments
    parser.add_argument(
        "--cache",
        action="store_true",
        help=(
            "Enable caching for batch conversions. When enabled, conversion results "
            "are cached based on file content hash (SHA-256). Subsequent batch runs "
            "will skip files that haven't changed and use cached results instead. "
            "This can significantly speed up repeated batch conversions when only "
            "some files have changed. Cache is stored in ~/.cache/markitdown by default."
        ),
    )

    parser.add_argument(
        "--cache-dir",
        type=str,
        metavar="PATH",
        help=(
            "Directory to store cache files. Defaults to ~/.cache/markitdown. "
            "Implies --cache if specified."
        ),
    )

    parser.add_argument(
        "--clear-cache",
        action="store_true",
        help=(
            "Clear all cached conversion results and exit. Use this to free up "
            "disk space or to force re-conversion of all files on next batch run."
        ),
    )

    parser.add_argument("filename", nargs="*")
    args = parser.parse_args()

    # Parse the extension hint
    extension_hint = args.extension
    if extension_hint is not None:
        extension_hint = extension_hint.strip().lower()
        if len(extension_hint) > 0:
            if not extension_hint.startswith("."):
                extension_hint = "." + extension_hint
        else:
            extension_hint = None

    # Parse the mime type
    mime_type_hint = args.mime_type
    if mime_type_hint is not None:
        mime_type_hint = mime_type_hint.strip()
        if len(mime_type_hint) > 0:
            if mime_type_hint.count("/") != 1:
                _exit_with_error(f"Invalid MIME type: {mime_type_hint}")
        else:
            mime_type_hint = None

    # Parse the charset
    charset_hint = args.charset
    if charset_hint is not None:
        charset_hint = charset_hint.strip()
        if len(charset_hint) > 0:
            try:
                charset_hint = codecs.lookup(charset_hint).name
            except LookupError:
                _exit_with_error(f"Invalid charset: {charset_hint}")
        else:
            charset_hint = None

    stream_info = None
    if (
        extension_hint is not None
        or mime_type_hint is not None
        or charset_hint is not None
    ):
        stream_info = StreamInfo(
            extension=extension_hint, mimetype=mime_type_hint, charset=charset_hint
        )

    if args.list_plugins:
        # List installed plugins, then exit
        print("Installed MarkItDown 3rd-party Plugins:\n")
        plugin_entry_points = list(entry_points(group="markitdown.plugin"))
        if len(plugin_entry_points) == 0:
            print("  * No 3rd-party plugins installed.")
            print(
                "\nFind plugins by searching for the hashtag #markitdown-plugin on GitHub.\n"
            )
        else:
            for entry_point in plugin_entry_points:
                print(f"  * {entry_point.name:<16}\t(package: {entry_point.value})")
            print(
                "\nUse the -p (or --use-plugins) option to enable 3rd-party plugins.\n"
            )
        sys.exit(0)

    if args.clear_cache:
        # Clear cache and exit
        from ._cache import ConversionCache

        cache_dir = Path(args.cache_dir) if args.cache_dir else None
        cache = ConversionCache(cache_dir)
        count = cache.clear()
        print(f"Cleared {count} cached conversion(s) from {cache.cache_dir}")
        sys.exit(0)

    # Validate --export-manifest requires --batch
    if args.export_manifest and not args.batch:
        _exit_with_error(
            "--export-manifest can only be used with --batch mode. "
            "Use --quality-json for single file quality output."
        )

    if args.use_docintel:
        if args.endpoint is None:
            _exit_with_error(
                "Document Intelligence Endpoint is required when using Document Intelligence."
            )
        elif not args.filename:
            _exit_with_error("Filename is required when using Document Intelligence.")

        markitdown = MarkItDown(
            enable_plugins=args.use_plugins, docintel_endpoint=args.endpoint
        )
    else:
        markitdown = MarkItDown(enable_plugins=args.use_plugins)

    # Handle batch mode
    if args.batch:
        _handle_batch_conversion(args, markitdown, stream_info)
        return

    # Handle single file mode
    if not args.filename:
        result = markitdown.convert_stream(
            sys.stdin.buffer,
            stream_info=stream_info,
            keep_data_uris=args.keep_data_uris,
        )
    else:
        # Single file - use first filename
        filename = args.filename[0] if args.filename else None
        if filename is None:
            result = markitdown.convert_stream(
                sys.stdin.buffer,
                stream_info=stream_info,
                keep_data_uris=args.keep_data_uris,
            )
        else:
            result = markitdown.convert(
                filename, stream_info=stream_info, keep_data_uris=args.keep_data_uris
            )

    _handle_output(args, result)


def _handle_batch_conversion(args, markitdown: MarkItDown, stream_info):
    """
    Handle batch conversion mode for the CLI.

    This function orchestrates the batch conversion process, handling all the complexity
    of converting multiple files while providing real-time feedback and proper output handling.

    The function performs the following steps:

    1. INPUT VALIDATION:
       Ensures at least one file or directory was provided to process.

    2. PROGRESS CALLBACK SETUP (if --progress flag is set):
       Creates a thread-safe progress callback that prints real-time updates to stderr.
       Thread safety is critical here because when --parallel is used with multiple workers,
       multiple threads may call the callback simultaneously. Without the lock, output could
       be interleaved/corrupted (e.g., two progress lines mixed together on one line).

       The callback uses:
       - A threading.Lock to ensure only one thread prints at a time
       - A mutable list [0] instead of an int for the counter (closures can't rebind
         immutable variables in enclosing scope without 'nonlocal', but can mutate list contents)
       - flush=True to ensure output appears immediately, not buffered

       Status icons provide at-a-glance understanding:
       - ✓ (SUCCESS): File converted successfully
       - ✗ (FAILED): Conversion threw an exception
       - ? (UNSUPPORTED): No converter could handle this file type
       - ○ (SKIPPED): File was skipped (e.g., filtered out)

    3. INPUT MODE DETECTION:
       Determines whether the user provided:
       - A single directory: Use convert_directory() which has built-in recursive scanning
         and pattern matching optimized for directory trees
       - Multiple items (files and/or directories): Expand any directories manually, then
         use convert_batch() on the resulting file list

       Why two modes? convert_directory() is optimized for the common case of "convert
       everything in this folder" and preserves directory structure in output. The mixed
       mode handles the flexibility of "convert these specific things" where users might
       combine folders and individual files.

    4. FILE FILTERING (for mixed mode):
       When directories are mixed with files, we manually expand directories and then
       apply --include/--exclude patterns. This uses fnmatch for glob-style matching
       against just the filename (not the full path) for consistency with how users
       typically think about file patterns.

       Include patterns act as a whitelist: if specified, ONLY matching files are processed.
       Exclude patterns act as a blacklist: matching files are removed from the list.
       Exclusions are applied AFTER inclusions, so you can do things like
       --include "*.doc*" --exclude "*_draft*" to get all Word docs except drafts.

    5. CONVERSION EXECUTION:
       Calls either convert_directory() or convert_batch() with appropriate parameters.
       Both methods handle parallel execution internally via ThreadPoolExecutor.

    6. OUTPUT HANDLING:
       Three output modes based on --output flag:
       - No output flag: Print all markdown to stdout with file headers (useful for piping)
       - Output ends in .json: Write complete results as JSON (includes all metadata)
       - Output is a directory: Write each converted file as separate .md file

       For directory output, preserve_structure=True only when input was a single directory,
       so the output mirrors the input's folder hierarchy.

    7. QUALITY/SUMMARY REPORTING:
       If --quality-json: Output full results as JSON to stderr (machine-readable)
       If --quality or --summary: Output human-readable summary to stderr

    Args:
        args: Parsed argparse namespace containing all CLI arguments
        markitdown: Configured MarkItDown instance to use for conversions
        stream_info: Optional StreamInfo with hints about file types (rarely used in batch mode)
    """
    from ._batch import write_batch_results

    # Step 1: Input validation
    if not args.filename:
        _exit_with_error("Batch mode requires at least one file or directory path.")

    # Set up cache if enabled (--cache or --cache-dir implies caching)
    cache = None
    if args.cache or args.cache_dir:
        from ._cache import ConversionCache

        cache_dir = Path(args.cache_dir) if args.cache_dir else None
        cache = ConversionCache(cache_dir)
        if args.progress:
            print(f"Cache enabled: {cache.cache_dir}", file=sys.stderr)

    # Step 2: Set up thread-safe progress callback
    # We use a threading.Lock to prevent interleaved output when multiple worker threads
    # complete conversions at nearly the same time. Without this, parallel execution could
    # produce garbled output like "[   1] ✓ file1.pdf[   2] ✓ file2.docx" on a single line.
    progress_callback = None
    processed_count = [0]  # Use list for mutable closure (can't rebind int in nested function)
    progress_lock = threading.Lock()  # Ensures atomic counter increment + print operations

    if args.progress:

        def progress_callback(item: BatchItemResult):
            # Acquire lock before modifying shared state or printing
            # This ensures the counter increment and print are atomic together
            with progress_lock:
                processed_count[0] += 1

                # Map status to visual icon for quick scanning of output
                status_icon = {
                    BatchItemStatus.SUCCESS: "✓",
                    BatchItemStatus.CACHED: "⚡",  # Lightning bolt for cached (fast)
                    BatchItemStatus.FAILED: "✗",
                    BatchItemStatus.SKIPPED: "○",
                    BatchItemStatus.UNSUPPORTED: "?",
                }.get(item.status, "?")

                # Truncate long paths to keep output readable
                # Show the end of the path (most informative part) with "..." prefix
                display_path = item.source_path
                if len(display_path) > 60:
                    display_path = "..." + display_path[-57:]

                # Show confidence percentage for successful conversions
                # This gives users immediate feedback on conversion quality
                confidence_str = ""
                if item.quality and item.status in (BatchItemStatus.SUCCESS, BatchItemStatus.CACHED):
                    confidence_str = f" ({item.quality.confidence:.0%})"
                if item.status == BatchItemStatus.CACHED:
                    confidence_str += " [cached]"

                # Print to stderr (not stdout) so it doesn't mix with converted content
                # flush=True ensures immediate output even when stderr is piped/redirected
                print(
                    f"[{processed_count[0]:4d}] {status_icon} {display_path}{confidence_str}",
                    file=sys.stderr,
                    flush=True,
                )

    # Step 3: Determine input mode - single directory vs mixed files/directories
    sources = args.filename
    is_single_directory = len(sources) == 1 and Path(sources[0]).is_dir()

    if is_single_directory:
        # Single directory mode: use optimized convert_directory() method
        # This handles recursive scanning and pattern matching internally
        directory = sources[0]
        result = markitdown.convert_directory(
            directory,
            recursive=not args.no_recursive,
            include_patterns=args.include,
            exclude_patterns=args.exclude,
            stream_info=stream_info,
            max_workers=args.parallel,
            on_progress=progress_callback,
            keep_data_uris=args.keep_data_uris if hasattr(args, "keep_data_uris") else False,
            cache=cache,
        )
    else:
        # Mixed mode: user provided multiple files and/or directories
        # We need to manually expand directories and apply filtering

        # Step 4a: Expand any directories in the input list
        files_to_convert = []
        for source in sources:
            source_path = Path(source)
            if source_path.is_dir():
                # Expand directory contents using glob
                # "**/*" for recursive, "*" for non-recursive
                pattern = "**/*" if not args.no_recursive else "*"
                for file_path in source_path.glob(pattern):
                    if file_path.is_file():
                        files_to_convert.append(str(file_path))
            else:
                # Individual file - add directly
                files_to_convert.append(source)

        # Step 4b: Apply include/exclude pattern filtering
        if args.include or args.exclude:
            import fnmatch

            filtered_files = []
            for file_path in files_to_convert:
                # Match against filename only, not full path
                # This is more intuitive: "*.pdf" matches any PDF regardless of directory
                filename = Path(file_path).name

                # Include patterns act as whitelist - file must match at least one
                if args.include:
                    matched = any(
                        fnmatch.fnmatch(filename, pat) for pat in args.include
                    )
                    if not matched:
                        continue

                # Exclude patterns act as blacklist - file must not match any
                if args.exclude:
                    excluded = any(
                        fnmatch.fnmatch(filename, pat) for pat in args.exclude
                    )
                    if excluded:
                        continue

                filtered_files.append(file_path)

            files_to_convert = filtered_files

        # Step 5: Execute batch conversion
        result = markitdown.convert_batch(
            files_to_convert,
            stream_info=stream_info,
            max_workers=args.parallel,
            on_progress=progress_callback,
            keep_data_uris=args.keep_data_uris if hasattr(args, "keep_data_uris") else False,
            cache=cache,
        )

    # Step 6: Handle output based on --output flag
    if args.output:
        output_path = Path(args.output)
        if output_path.suffix == ".json":
            # JSON output: write complete results including all metadata
            # Useful for programmatic processing of batch results
            with open(output_path, "w", encoding="utf-8") as f:
                json.dump(result.to_dict(), f, indent=2)
            print(f"Results written to {output_path}", file=sys.stderr)
        else:
            # Directory output: write each converted file as separate .md file
            # preserve_structure keeps input directory hierarchy in output
            output_mapping = write_batch_results(
                result,
                output_path,
                preserve_structure=is_single_directory,
            )
            print(
                f"Wrote {len(output_mapping)} files to {output_path}", file=sys.stderr
            )
    else:
        # No output specified: print all markdown to stdout
        # Separate each file with headers so output is parseable
        for item in result.successful_items:
            print(f"\n{'='*60}")
            print(f"FILE: {item.source_path}")
            print(f"{'='*60}\n")
            if item.markdown:
                # Handle encoding issues gracefully - replace unrepresentable chars
                print(
                    item.markdown.encode(sys.stdout.encoding, errors="replace").decode(
                        sys.stdout.encoding
                    )
                )

    # Step 7: Output quality/summary information if requested
    if args.quality_json:
        # Machine-readable JSON output to stderr
        quality_dict = result.to_dict()
        print(json.dumps(quality_dict, indent=2), file=sys.stderr)
    elif args.quality or args.summary:
        # Human-readable summary to stderr
        print("\n" + str(result), file=sys.stderr)
        print("\nOVERALL QUALITY:", file=sys.stderr)
        print(str(result.overall_quality), file=sys.stderr)

    # Step 8: Export manifest file if requested
    if args.export_manifest:
        manifest = _build_quality_manifest(result)
        manifest_path = Path(args.export_manifest)
        manifest_path.parent.mkdir(parents=True, exist_ok=True)
        with open(manifest_path, "w", encoding="utf-8") as f:
            json.dump(manifest, f, indent=2)
        print(f"Quality manifest written to {manifest_path}", file=sys.stderr)


def _build_quality_manifest(result: BatchConversionResult) -> dict:
    """
    Build a quality manifest dictionary from batch conversion results.

    The manifest provides a clear mapping between each source file and its
    quality metrics, making it easy to review conversion quality per file.

    Structure:
    {
        "summary": {
            "total_files": int,
            "successful": int,
            "failed": int,
            "unsupported": int,
            "average_confidence": float | null  # null if no successful conversions
        },
        "files": [
            {
                "source_path": str,
                "status": str,
                "quality": {
                    "confidence": float,
                    "converter_used": str,
                    "warnings": [...],
                    "formatting_loss": [...],
                    ...
                } | null,
                "error": str | null
            },
            ...
        ]
    }
    """
    # Build summary
    # Use None for average_confidence when no files were successfully converted
    # to clearly indicate no quality data is available (vs 0.0 which could mean low quality)
    avg_confidence = None
    successful_items = result.successful_items
    if successful_items:
        items_with_quality = [item for item in successful_items if item.quality]
        if items_with_quality:
            total_confidence = sum(item.quality.confidence for item in items_with_quality)
            avg_confidence = total_confidence / len(items_with_quality)

    manifest = {
        "summary": {
            "total_files": result.total_count,
            "successful": result.success_count,
            "cached": result.cached_count,
            "failed": result.failed_count,
            "unsupported": result.unsupported_count,
            "skipped": result.skipped_count,
            "completion_percentage": result.completion_percentage,
            "average_confidence": avg_confidence,
        },
        "files": [],
    }

    # Build per-file entries
    for item in result.items:
        file_entry = {
            "source_path": item.source_path,
            "status": item.status.value,
        }

        # Add quality info if available
        if item.quality:
            file_entry["quality"] = item.quality.to_dict()
        else:
            file_entry["quality"] = None

        # Add error info if present
        if item.error:
            file_entry["error"] = item.error
            file_entry["error_type"] = item.error_type

        manifest["files"].append(file_entry)

    return manifest


def _handle_output(args, result: DocumentConverterResult):
    """Handle output to stdout or file"""
    if args.output:
        with open(args.output, "w", encoding="utf-8") as f:
            f.write(result.markdown)
    else:
        # Handle stdout encoding errors more gracefully
        print(
            result.markdown.encode(sys.stdout.encoding, errors="replace").decode(
                sys.stdout.encoding
            )
        )

    # Output quality information if requested
    if args.quality_json:
        quality_dict = result.quality.to_dict()
        print(json.dumps(quality_dict, indent=2), file=sys.stderr)
    elif args.quality:
        print("\n" + "=" * 60, file=sys.stderr)
        print("CONVERSION QUALITY REPORT", file=sys.stderr)
        print("=" * 60, file=sys.stderr)
        print(str(result.quality), file=sys.stderr)
        print("=" * 60, file=sys.stderr)


def _exit_with_error(message: str):
    print(message)
    sys.exit(1)


if __name__ == "__main__":
    main()
