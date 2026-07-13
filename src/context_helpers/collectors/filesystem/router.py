"""FastAPI router for the filesystem collector endpoints.

The wire contract is unchanged in shape from the previous version, with two
semantic changes:

* ``source_ref`` / ``next_cursor`` are an opaque integer change-sequence (the
  index cursor), not an ISO timestamp. Consumers treat them as opaque strings.
* Deleted files are delivered as tombstone lines ``{"op": "delete", ...}`` so the
  consumer can retire their chunks.
"""

from __future__ import annotations

import json as _json
import logging
from typing import TYPE_CHECKING, Iterator

from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import FileResponse, StreamingResponse
from pydantic import BaseModel

if TYPE_CHECKING:
    from context_helpers.collectors.filesystem.collector import FilesystemCollector

logger = logging.getLogger(__name__)


def _to_normalized_content(doc: dict) -> dict:
    """Map a flat content doc to the NormalizedContent wire format."""
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


def _to_tombstone(doc: dict) -> dict:
    """Map a tombstone marker to its wire line."""
    return {"op": "delete", "source_id": doc["source_id"], "modified_at": doc.get("modified_at")}


def _parse_cursor(source_ref: str) -> int | None:
    if not source_ref:
        return None
    try:
        return int(source_ref)
    except ValueError:
        return None  # unparseable cursor → start from the beginning


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
        """Return present documents from the configured roots (direct API)."""
        if extensions:
            ext_list = [e.strip() if e.strip().startswith(".") else f".{e.strip()}" for e in extensions.split(",")]
        else:
            ext_list = None
        return collector.fetch_documents(since=since, extensions=ext_list, max_size_mb=max_size_mb)

    @router.get("/filesystem/file")
    def get_file(
        path: str = Query(..., description="source_id (<label>/<relative-path>) of an indexed file"),
    ):
        """Serve a single file by its source_id, honouring HTTP Range requests.

        The source_id is ``<root-label>/<relative-path>``; the label selects the
        configured root and the remainder is resolved within it. Returns 403 if
        the resolved path escapes its root, 404 if it does not exist.
        """
        roots = collector.roots_map()
        label, _, rel = path.partition("/")
        base = roots.get(label)
        if base is None or not rel:
            raise HTTPException(status_code=404, detail="Unknown root or empty path")

        resolved = (base / rel).resolve()
        try:
            resolved.relative_to(base)
        except ValueError:
            raise HTTPException(status_code=403, detail="Access denied: path outside configured root")
        if not resolved.exists() or not resolved.is_file():
            raise HTTPException(status_code=404, detail="File not found")
        return FileResponse(str(resolved))

    @router.post("/filesystem/fetch")
    def fetch_paged(body: FetchRequest):
        """Fetch one bounded page for the ingestion pipeline.

        ``source_ref`` is the integer index cursor (empty = from the start).
        With ``stream: true`` the response is NDJSON: one NormalizedContent or
        tombstone object per line, then ``{"has_more": bool, "next_cursor": str}``.
        Otherwise a JSON object with ``normalized_contents``, ``deletions``,
        ``has_more`` and ``next_cursor``.

        The delivery cursor advances after the page is served. Under ``?ack=true``
        the advance is *staged* and only persisted when the consumer confirms via
        ``POST /collectors/filesystem/ack``, so a page whose ingestion fails is
        re-served rather than skipped.
        """
        after = _parse_cursor(body.source_ref)
        limit = body.page_size if body.page_size is not None else collector._config.page_size

        if body.stream:
            page_iter = collector.iter_page(
                after=after, limit=limit, extensions=body.extensions, max_size_mb=body.max_size_mb,
            )
            return StreamingResponse(
                _ndjson_stream(page_iter, collector),
                media_type="application/x-ndjson",
            )

        items, has_more, page_max = collector.fetch_page(
            after=after, limit=limit, extensions=body.extensions, max_size_mb=body.max_size_mb,
        )
        # page_max is THIS request's max examined seq, returned by fetch_page —
        # never read from shared collector state, so overlapping requests each
        # commit exactly the page they served.
        collector.commit_page(page_max)

        contents = [_to_normalized_content(d) for d in items if not d.get("__deleted__")]
        deletions = [_to_tombstone(d) for d in items if d.get("__deleted__")]
        return {
            "normalized_contents": contents,
            "deletions": deletions,
            "has_more": has_more,
            "next_cursor": str(page_max),
        }

    return router


def _ndjson_stream(page_iter: Iterator[dict], collector: "FilesystemCollector") -> Iterator[str]:
    """Yield NDJSON lines from iter_page() output, then commit the page cursor.

    The page's max seq arrives in THIS iterator's meta sentinel (a per-request
    value) — never from shared collector state — so overlapping requests each
    commit exactly the page they streamed. If the sentinel never arrives (the
    stream aborted mid-page), nothing is committed and the page is re-served.
    """
    page_max: int | None = None
    for item in page_iter:
        if item.get("__meta__"):
            try:
                page_max = int(item["next_cursor"])
            except (TypeError, ValueError):
                page_max = None
            yield _json.dumps({"has_more": item["has_more"], "next_cursor": item["next_cursor"]}) + "\n"
            break
        if item.get("__deleted__"):
            yield _json.dumps(_to_tombstone(item)) + "\n"
        else:
            yield _json.dumps(_to_normalized_content(item)) + "\n"
    if page_max is not None:
        collector.commit_page(page_max)
