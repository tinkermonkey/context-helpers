"""Document → markdown conversion for the filesystem collector.

Converts binary document formats to markdown on the mac, so the existing
NDJSON/commit-ack delivery pipeline carries them like any text file. MS Office
and PDF go straight through MarkItDown; Apple iWork formats (.pages, .numbers,
.key) are proprietary protobuf internally — no cross-platform converter reads
them — but both their zip and directory-bundle forms embed a QuickLook preview
PDF, which we extract and convert instead.

Run as a module for subprocess isolation (``python -m …filesystem.convert
<path>``): MarkItDown conversion is CPU-bound, can hang on pathological inputs,
and pulls large parsers into memory — a subprocess gives the collector a hard
timeout and keeps parser crashes/leaks out of the server process. Markdown is
written to stdout; errors go to stderr with a non-zero exit.
"""

from __future__ import annotations

import sys
import tempfile
import zipfile
from pathlib import Path

# iWork formats: converted via their embedded QuickLook preview PDF.
IWORK_EXTENSIONS = {".pages", ".numbers", ".key"}

# Formats MarkItDown converts directly.
MARKITDOWN_EXTENSIONS = {".pdf", ".docx", ".xlsx", ".pptx", ".xls"}

CONVERTIBLE_EXTENSIONS = IWORK_EXTENSIONS | MARKITDOWN_EXTENSIONS

# Candidate preview locations inside an iWork bundle (zip member names or
# bundle-relative paths). Modern iWork always writes QuickLook/Preview.pdf
# unless "include preview" was disabled at save time.
_IWORK_PREVIEW_NAMES = (
    "QuickLook/Preview.pdf",
    "preview.pdf",
    "Preview.pdf",
)


class ConversionError(Exception):
    """Raised when a document cannot be converted to markdown."""


def _markitdown_convert(path: Path) -> str:
    try:
        from markitdown import MarkItDown
    except ImportError as e:  # pragma: no cover - guarded by caller in-process
        raise ConversionError(f"markitdown not installed: {e}") from e

    try:
        result = MarkItDown(enable_plugins=False).convert(str(path))
    except Exception as e:
        raise ConversionError(f"markitdown failed: {type(e).__name__}: {e}") from e

    text = (result.text_content or "").strip()
    # pdfminer occasionally yields the literal string "None" for unparseable
    # input instead of raising; treat it as empty rather than delivering junk.
    if not text or text == "None":
        raise ConversionError("markitdown produced empty output")
    return text


def _extract_iwork_preview(path: Path) -> bytes:
    """Return the embedded preview PDF bytes from an iWork document.

    Handles both on-disk forms: a single zip file and a directory bundle.
    """
    if path.is_dir():
        for name in _IWORK_PREVIEW_NAMES:
            candidate = path / name
            if candidate.is_file():
                return candidate.read_bytes()
        raise ConversionError(
            "no QuickLook preview in iWork bundle (saved without preview?)"
        )

    try:
        with zipfile.ZipFile(path) as zf:
            members = set(zf.namelist())
            for name in _IWORK_PREVIEW_NAMES:
                if name in members:
                    return zf.read(name)
    except zipfile.BadZipFile as e:
        raise ConversionError(f"not a zip-format iWork file: {e}") from e
    raise ConversionError(
        "no QuickLook preview in iWork file (saved without preview?)"
    )


def convert_to_markdown(path: Path) -> str:
    """Convert *path* to markdown text.

    Raises:
        ConversionError: if the format is unsupported, the file has no usable
            content, or the underlying converter fails.
    """
    ext = path.suffix.lower()
    if ext in IWORK_EXTENSIONS:
        pdf_bytes = _extract_iwork_preview(path)
        with tempfile.NamedTemporaryFile(suffix=".pdf") as tmp:
            tmp.write(pdf_bytes)
            tmp.flush()
            return _markitdown_convert(Path(tmp.name))
    if ext in MARKITDOWN_EXTENSIONS:
        return _markitdown_convert(path)
    raise ConversionError(f"unsupported extension: {ext}")


def main(argv: list[str]) -> int:
    if len(argv) != 2:
        print("usage: python -m context_helpers.collectors.filesystem.convert <path>", file=sys.stderr)
        return 2
    try:
        markdown = convert_to_markdown(Path(argv[1]))
    except ConversionError as e:
        print(str(e), file=sys.stderr)
        return 1
    sys.stdout.write(markdown)
    return 0


if __name__ == "__main__":  # pragma: no cover - exercised via subprocess in tests
    sys.exit(main(sys.argv))
