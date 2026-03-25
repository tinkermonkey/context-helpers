"""FastAPI router for the filesystem collector endpoints."""

from __future__ import annotations

import json as _json
import logging
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Iterator

from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import FileResponse, StreamingResponse
from pydantic import BaseModel

if TYPE_CHECKING:
    from context_helpers.collectors.filesystem.collector import FilesystemCollector

logger = logging.getLogger(__name__)


def _to_normalized_content(doc: dict) -> dict:
    """Map a flat document dict to the NormalizedContent wire format."""
    return {
        "markdown": doc["markdown"],
        "source_id": doc["source_id"],
        "structural_hints": {
            "has_headings": doc.get("has_headings", False),
            "has_lists": doc.get("has_lists", False),
            "has_tables": doc.get("has_tables", False),
            "natural_boundaries": [],
            "file_path": None,
            "modified_at": doc.get("modified_at"),
            "file_size_bytes": doc.get("file_size_bytes"),
            "extra_metadata": None,
        },
        "normalizer_version": "1.0.0",
    }


def _ndjson_stream(
    page_iter: Iterator[dict],
    collector: "FilesystemCollector",
    fallback_cursor: "datetime | None",
) -> Iterator[str]:
    """Consume iter_page() output and yield NDJSON lines.

    Content items are serialised as NormalizedContent-shaped objects.
    The final ``__meta__`` sentinel becomes the closing meta line.

    After the meta line is yielded the helper cursor is advanced so that
    restarts resume from the right position.  If iter_page found no files
    (next_cursor is None), the fallback_cursor (the caller's source_ref) is
    adopted — this prevents has_changes_since from looping forever when the
    library is already past everything the helper has on disk.
    """
    for item in page_iter:
        if item.get("__meta__"):
            yield _json.dumps({
                "has_more": item["has_more"],
                "next_cursor": item["next_cursor"],
            }) + "\n"
            # Advance helper cursor to keep it in sync with the library.
            cursor_to_write: "datetime | None" = None
            raw = item.get("next_cursor")
            if raw:
                try:
                    cursor_to_write = datetime.fromisoformat(raw)
                    if cursor_to_write.tzinfo is None:
                        cursor_to_write = cursor_to_write.replace(tzinfo=timezone.utc)
                except ValueError:
                    pass
            if cursor_to_write is None:
                cursor_to_write = fallback_cursor
            if cursor_to_write is not None:
                current = collector.get_cursor()
                if current is None or cursor_to_write > current:
                    collector._save_cursor(cursor_to_write)
            return
        yield _json.dumps(_to_normalized_content(item)) + "\n"


class FetchRequest(BaseModel):
    source_ref: str = ""
    page_size: int | None = None
    extensions: list[str] | None = None
    max_size_mb: float | None = None
    stream: bool = False


def make_filesystem_router(collector: "FilesystemCollector") -> APIRouter:
    """Build and return the filesystem router bound to a collector instance."""
    router = APIRouter()

    @router.get("/filesystem/documents")
    def get_documents(
        since: str | None = Query(default=None, description="ISO 8601 timestamp for incremental fetch"),
        extensions: str | None = Query(default=None, description="Comma-separated file extensions, e.g. .md,.txt"),
        max_size_mb: float | None = Query(default=None, description="Maximum file size in MB; overrides server config when set"),
    ) -> list[dict]:
        """Return documents from the configured local directory.

        Matches the API contract expected by FilesystemHelperAdapter.
        """
        if extensions:
            ext_list = [e.strip() if e.strip().startswith(".") else f".{e.strip()}" for e in extensions.split(",")]
        else:
            ext_list = None
        return collector.fetch_documents(since=since, extensions=ext_list, max_size_mb=max_size_mb)

    @router.get("/filesystem/file")
    def get_file(
        path: str = Query(..., description="Relative path to file within the configured directory"),
    ):
        """Serve a single file by its relative path.

        Supports HTTP Range requests (byte-range streaming) for large files —
        FastAPI's FileResponse sets Accept-Ranges: bytes and honours the
        Range header natively via Starlette.

        This endpoint intentionally has no max_file_size_mb guard; it is
        designed for files too large to inline in fetch_page().

        Returns 403 if the resolved path escapes the configured directory.
        Returns 404 if the file does not exist.
        """
        # collector._directory is already fully resolved (see FilesystemCollector.__init__),
        # so resolving the joined path and comparing is sufficient to catch both
        # "../" traversal sequences and in-directory symlinks whose targets lie outside
        # the configured directory.  Files reachable only via such outward symlinks are
        # intentionally blocked here even though rglob() would follow them; serving
        # arbitrary external paths from a path hint would be a confused-deputy attack.
        resolved = (collector._directory / path).resolve()
        try:
            resolved.relative_to(collector._directory)
        except ValueError:
            raise HTTPException(status_code=403, detail="Access denied: path outside configured directory")

        if not resolved.exists() or not resolved.is_file():
            raise HTTPException(status_code=404, detail="File not found")

        return FileResponse(str(resolved))

    @router.post("/filesystem/fetch")
    def fetch_paged(body: FetchRequest):
        """Fetch a bounded page of documents for the push-trigger pipeline.

        Request body:
          source_ref: ISO 8601 cursor string (empty = start from beginning)
          page_size:  Override default page size from config
          extensions: Override extension filter
          max_size_mb: Override per-file size cap
          stream:     When True, return NDJSON stream instead of JSON object

        JSON response (stream=False):
          normalized_contents: List of NormalizedContent-shaped items
          has_more: True if additional pages remain beyond this page
          next_cursor: ISO 8601 timestamp of last item, or null

        NDJSON response (stream=True):
          One NormalizedContent JSON object per line, then a final meta line:
          {"has_more": bool, "next_cursor": str | null}
          Content-Type: application/x-ndjson
        """
        after: datetime | None = None
        if body.source_ref:
            try:
                after = datetime.fromisoformat(body.source_ref)
                if after.tzinfo is None:
                    after = after.replace(tzinfo=timezone.utc)
            except ValueError:
                pass  # invalid cursor → start from beginning

        limit = body.page_size if body.page_size is not None else collector._config.page_size

        if body.stream:
            # Stash is bypassed for streaming — iter_page() reads lazily from disk
            page_iter = collector.iter_page(
                after=after,
                limit=limit,
                extensions=body.extensions,
                max_size_mb=body.max_size_mb,
            )
            return StreamingResponse(
                _ndjson_stream(page_iter, collector, after),
                media_type="application/x-ndjson",
            )

        # Non-streaming path.
        # If the caller provides an explicit cursor (source_ref), use it directly —
        # the library is authoritative for its own pagination position, and the stash
        # (filled by the push trigger from the helper's local cursor) may be at a
        # different position.  Mixing the two would deliver data from the wrong offset.
        # Only use the stash when no source_ref is given (push-trigger pre-fill flow).
        if not body.source_ref and collector.has_pending():
            items = collector.consume_stash()
            has_more = collector.has_more()
        else:
            # The library is driving with its own cursor — the push trigger's pre-filled
            # stash (if any) was built from the helper's cursor and is now irrelevant.
            # Discard it so has_pending() returns False on the next poll cycle and the
            # push trigger doesn't loop indefinitely serving stale stash data.
            collector.discard_stash()
            items, has_more = collector.fetch_page(
                after=after,
                limit=limit,
                extensions=body.extensions,
                max_size_mb=body.max_size_mb,
            )
            # Persist cursor so the helper and library stay in sync — if the library
            # resets its cursor, the helper can resume from the right position.
            # If fetch_page returned nothing but the library supplied a cursor, adopt
            # that cursor: the library is already past everything we have, and without
            # writing it the helper cursor stays None → has_changes_since keeps
            # returning True → push trigger fires forever with no useful work.
            cursor_to_write = collector._page_cursor or after
            if cursor_to_write is not None:
                current = collector.get_cursor()
                if current is None or cursor_to_write > current:
                    collector._save_cursor(cursor_to_write)

        # Use the collector's page_cursor (which includes permanently-skipped file
        # timestamps) so the adapter's cursor always advances past skipped files,
        # not just past the last delivered item.
        page_cur = collector._page_cursor
        if page_cur is not None:
            next_cursor: str | None = page_cur.isoformat()
        elif items:
            next_cursor = max(
                item["modified_at"] for item in items if item.get("modified_at")
            )
        else:
            next_cursor = None

        return {
            "normalized_contents": [_to_normalized_content(doc) for doc in items],
            "has_more": has_more,
            "next_cursor": next_cursor,
        }

    return router
