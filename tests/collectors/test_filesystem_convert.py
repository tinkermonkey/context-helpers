"""Tests for document → markdown conversion in the filesystem collector.

Uses real minimal fixtures (hand-assembled PDF, minimal DOCX zip, iWork zip and
directory bundles with embedded preview PDFs) so the MarkItDown chain is
exercised end-to-end, not mocked.
"""

import io
import subprocess
import sys
import zipfile
from pathlib import Path

import pytest

import context_helpers.collectors.base as base_mod
import context_helpers.collectors.filesystem.collector as collector_mod
from context_helpers.collectors.filesystem.collector import FilesystemCollector
from context_helpers.collectors.filesystem.convert import (
    ConversionError,
    convert_to_markdown,
)
from context_helpers.config import FilesystemConfig, FilesystemRoot

markitdown = pytest.importorskip("markitdown", reason="[documents] extra not installed")


# ---------------------------------------------------------------------------
# Fixture builders (real formats, minimal content)
# ---------------------------------------------------------------------------

def minimal_pdf(text: str = "Quarterly report for Linkage Labs") -> bytes:
    """Hand-assembled single-page PDF with a correct xref table."""
    objects = [
        b"<< /Type /Catalog /Pages 2 0 R >>",
        b"<< /Type /Pages /Kids [3 0 R] /Count 1 >>",
        b"<< /Type /Page /Parent 2 0 R /MediaBox [0 0 612 792] "
        b"/Contents 4 0 R /Resources << /Font << /F1 5 0 R >> >> >>",
        None,
        b"<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>",
    ]
    stream = f"BT /F1 12 Tf 72 720 Td ({text}) Tj ET".encode()
    objects[3] = (
        b"<< /Length " + str(len(stream)).encode() + b" >>\nstream\n" + stream + b"\nendstream"
    )
    out = io.BytesIO()
    out.write(b"%PDF-1.4\n")
    offsets = []
    for i, obj in enumerate(objects, start=1):
        offsets.append(out.tell())
        out.write(f"{i} 0 obj\n".encode() + obj + b"\nendobj\n")
    xref_pos = out.tell()
    out.write(f"xref\n0 {len(objects) + 1}\n".encode())
    out.write(b"0000000000 65535 f \n")
    for off in offsets:
        out.write(f"{off:010d} 00000 n \n".encode())
    out.write(
        f"trailer\n<< /Size {len(objects) + 1} /Root 1 0 R >>\nstartxref\n{xref_pos}\n%%EOF\n".encode()
    )
    return out.getvalue()


def minimal_docx(text: str = "Meeting notes from the design review") -> bytes:
    doc_xml = (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<w:document xmlns:w="http://schemas.openxmlformats.org/wordprocessingml/2006/main">'
        f"<w:body><w:p><w:r><w:t>{text}</w:t></w:r></w:p></w:body></w:document>"
    )
    content_types = (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types">'
        '<Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/>'
        '<Default Extension="xml" ContentType="application/xml"/>'
        '<Override PartName="/word/document.xml" ContentType='
        '"application/vnd.openxmlformats-officedocument.wordprocessingml.document.main+xml"/>'
        "</Types>"
    )
    rels = (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">'
        '<Relationship Id="rId1" Type='
        '"http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument" '
        'Target="word/document.xml"/></Relationships>'
    )
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("[Content_Types].xml", content_types)
        zf.writestr("_rels/.rels", rels)
        zf.writestr("word/document.xml", doc_xml)
    return buf.getvalue()


def pages_zip(preview_text: str = "Pages preview text content") -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("Index/Document.iwa", b"\x00\x01proprietary")
        zf.writestr("QuickLook/Preview.pdf", minimal_pdf(preview_text))
    return buf.getvalue()


def make_pages_bundle(parent: Path, name: str, preview_text: str) -> Path:
    """Create an iWork *directory bundle* with an embedded preview PDF."""
    bundle = parent / name
    (bundle / "QuickLook").mkdir(parents=True)
    (bundle / "QuickLook" / "Preview.pdf").write_bytes(minimal_pdf(preview_text))
    (bundle / "Index.zip").write_bytes(b"\x00\x01proprietary")
    return bundle


@pytest.fixture(autouse=True)
def _isolate_state(tmp_path, monkeypatch):
    state = tmp_path.parent / f"{tmp_path.name}_state"
    state.mkdir(exist_ok=True)
    monkeypatch.setattr(collector_mod, "_STATE_DIR", state)
    monkeypatch.setattr(base_mod, "_CURSORS_DIR", state / "cursors")
    yield


def _collector(root: Path, **overrides) -> FilesystemCollector:
    cfg = FilesystemConfig(
        enabled=True,
        roots=[FilesystemRoot(path=str(root))],
        extensions=overrides.pop("extensions", []),
        max_file_size_mb=overrides.pop("max_file_size_mb", 5.0),
        page_size=overrides.pop("page_size", 50),
        exclude_globs=[],
        **overrides,
    )
    c = FilesystemCollector(cfg)
    c.scan_now()
    return c


def _fetch_all(c: FilesystemCollector, limit: int = 50):
    items, has_more, page_max = c.fetch_page(after=None, limit=limit)
    return items, has_more, page_max


# ---------------------------------------------------------------------------
# convert_to_markdown (in-process)
# ---------------------------------------------------------------------------

class TestConvertToMarkdown:
    def test_pdf(self, tmp_path):
        f = tmp_path / "report.pdf"
        f.write_bytes(minimal_pdf("Quarterly report for Linkage Labs"))
        assert "Quarterly report for Linkage Labs" in convert_to_markdown(f)

    def test_docx(self, tmp_path):
        f = tmp_path / "notes.docx"
        f.write_bytes(minimal_docx("Meeting notes from the design review"))
        assert "Meeting notes from the design review" in convert_to_markdown(f)

    def test_pages_zip_via_embedded_preview(self, tmp_path):
        f = tmp_path / "letter.pages"
        f.write_bytes(pages_zip("Dear valued customer"))
        assert "Dear valued customer" in convert_to_markdown(f)

    def test_pages_directory_bundle(self, tmp_path):
        bundle = make_pages_bundle(tmp_path, "budget.numbers", "Household budget overview")
        assert "Household budget overview" in convert_to_markdown(bundle)

    def test_iwork_without_preview_raises(self, tmp_path):
        f = tmp_path / "nopreview.pages"
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w") as zf:
            zf.writestr("Index/Document.iwa", b"\x00\x01")
        f.write_bytes(buf.getvalue())
        with pytest.raises(ConversionError, match="preview"):
            convert_to_markdown(f)

    def test_unsupported_extension_raises(self, tmp_path):
        f = tmp_path / "x.xyz"
        f.write_text("hi")
        with pytest.raises(ConversionError, match="unsupported"):
            convert_to_markdown(f)

    def test_unreadable_file_raises(self, tmp_path):
        # (MarkItDown falls back to plain-text passthrough for corrupt PDFs and
        # DOCX alike, so content corruption doesn't reliably raise — but an
        # unreadable path always does.)
        with pytest.raises(ConversionError):
            convert_to_markdown(tmp_path / "vanished.docx")


class TestSubprocessEntrypoint:
    def test_module_main_outputs_markdown(self, tmp_path):
        f = tmp_path / "doc.pdf"
        f.write_bytes(minimal_pdf("Subprocess conversion works"))
        result = subprocess.run(
            [sys.executable, "-m", "context_helpers.collectors.filesystem.convert", str(f)],
            capture_output=True,
            text=True,
            timeout=120,
        )
        assert result.returncode == 0
        assert "Subprocess conversion works" in result.stdout

    def test_module_main_error_exit(self, tmp_path):
        result = subprocess.run(
            [sys.executable, "-m", "context_helpers.collectors.filesystem.convert",
             str(tmp_path / "vanished.docx")],
            capture_output=True,
            text=True,
            timeout=120,
        )
        assert result.returncode == 1
        assert result.stderr.strip()


# ---------------------------------------------------------------------------
# Collector integration
# ---------------------------------------------------------------------------

class TestCollectorConversion:
    def test_pdf_delivered_as_markdown(self, tmp_path):
        (tmp_path / "report.pdf").write_bytes(minimal_pdf("Quarterly report for Linkage Labs"))
        c = _collector(tmp_path)
        items, _, _ = _fetch_all(c)
        docs = [i for i in items if not i.get("op")]
        assert len(docs) == 1
        assert docs[0]["source_id"].endswith("report.pdf")
        assert "Quarterly report for Linkage Labs" in docs[0]["markdown"]

    def test_iwork_bundle_dir_delivered_once_not_descended(self, tmp_path):
        make_pages_bundle(tmp_path, "budget.numbers", "Household budget overview")
        c = _collector(tmp_path)
        items, _, _ = _fetch_all(c)
        docs = [i for i in items if not i.get("op")]
        assert len(docs) == 1
        assert docs[0]["source_id"].endswith("budget.numbers")
        assert "Household budget overview" in docs[0]["markdown"]
        # Bundle internals must not be indexed as separate sources
        assert not any("QuickLook" in i.get("source_id", "") for i in items)

    def test_allowlist_gates_convertibles(self, tmp_path):
        (tmp_path / "report.pdf").write_bytes(minimal_pdf())
        (tmp_path / "notes.md").write_text("# hello")
        c = _collector(tmp_path, extensions=[".md"])
        items, _, _ = _fetch_all(c)
        ids = [i["source_id"] for i in items if not i.get("op")]
        assert any(s.endswith("notes.md") for s in ids)
        assert not any(s.endswith("report.pdf") for s in ids)

    def test_convert_documents_false_skips_binaries(self, tmp_path):
        (tmp_path / "report.pdf").write_bytes(minimal_pdf())
        c = _collector(tmp_path, convert_documents=False)
        items, _, _ = _fetch_all(c)
        assert not [i for i in items if not i.get("op")]

    def test_conversion_cached_and_reused(self, tmp_path, monkeypatch):
        (tmp_path / "report.pdf").write_bytes(minimal_pdf("Cache me once"))
        c = _collector(tmp_path)
        items, _, page_max = _fetch_all(c)
        assert "Cache me once" in items[0]["markdown"]

        # Second serve from cursor 0 must come from the cache — no subprocess.
        def _boom(path):
            raise AssertionError("conversion subprocess must not run on cache hit")

        monkeypatch.setattr(c, "_convert_document", _boom)
        items2, _, _ = _fetch_all(c)
        assert "Cache me once" in items2[0]["markdown"]

    def test_cache_invalidated_on_file_change(self, tmp_path):
        f = tmp_path / "report.pdf"
        f.write_bytes(minimal_pdf("Version one"))
        c = _collector(tmp_path)
        items, _, _ = _fetch_all(c)
        assert "Version one" in items[0]["markdown"]

        f.write_bytes(minimal_pdf("Version two"))
        import os
        os.utime(f, (f.stat().st_mtime + 5, f.stat().st_mtime + 5))
        c.scan_now()
        items2, _, _ = _fetch_all(c)
        docs = [i for i in items2 if not i.get("op") and "Version" in i.get("markdown", "")]
        assert any("Version two" in d["markdown"] for d in docs)

    def test_conversion_failure_requeues_transiently(self, tmp_path, monkeypatch):
        f = tmp_path / "broken.pdf"
        f.write_bytes(b"%PDF-1.4 not really")
        c = _collector(tmp_path)
        monkeypatch.setattr(c, "_convert_document", lambda path: (None, "boom"))
        items, _, page_max = _fetch_all(c)
        assert not [i for i in items if not i.get("op")]
        # Failure requeued with a fresh seq (transient policy) → still has changes
        assert c._index.has_changes(page_max)
        row = c._index.lookup(f"{c._roots[0][0]}/broken.pdf")
        assert row.failures == 1

    def test_page_conversion_budget_ends_page_early(self, tmp_path, monkeypatch):
        (tmp_path / "a.pdf").write_bytes(minimal_pdf("Document A"))
        (tmp_path / "b.pdf").write_bytes(minimal_pdf("Document B"))
        c = _collector(tmp_path)
        c._config.convert_budget_sec = 0.05

        real = c._convert_document

        def _slow(path):
            import time as _t
            _t.sleep(0.1)
            return real(path)

        monkeypatch.setattr(c, "_convert_document", _slow)
        items, has_more, page_max = _fetch_all(c)
        docs = [i for i in items if not i.get("op")]
        # First conversion consumes the whole budget → page ends with one doc
        assert len(docs) == 1
        assert has_more is True

        # Next page picks up the remaining document from the un-advanced cursor
        items2, _, _ = c.fetch_page(after=page_max, limit=50)
        docs2 = [i for i in items2 if not i.get("op")]
        assert len(docs2) == 1
        texts = (docs[0]["markdown"] + docs2[0]["markdown"])
        assert "Document A" in texts and "Document B" in texts
