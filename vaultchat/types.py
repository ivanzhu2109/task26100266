"""Public data types for the VaultChat search library."""
from __future__ import annotations

from dataclasses import dataclass, field

PAGE_SIZE = 50

# Cursor position meaning "before any message" (scan goes backwards in time).
INF_POS = (1 << 62) - 1


@dataclass(frozen=True)
class SearchHit:
    """One visible search result, shown with its frozen version body."""

    msg_id: int
    channel_id: str
    sent_at: int
    body: str


@dataclass(frozen=True)
class SearchPage:
    """A page of results. ``cursor`` is opaque to callers; pass it back to
    ``next_page`` (or rely on the session's internal position)."""

    session_id: str
    hits: tuple[SearchHit, ...]
    has_more: bool
    cursor: tuple[int, int] | None  # (sent_at, msg_id) of last returned hit
