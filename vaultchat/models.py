"""Data types exposed to callers."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import List, Optional, Tuple


@dataclass(frozen=True)
class Message:
    """A single search result item, showing the frozen version's body."""

    msg_id: int
    channel_id: int
    sent_at: int
    body: str


@dataclass(frozen=True)
class Page:
    """One page of search results."""

    session_id: int
    items: Tuple[Message, ...]
    cursor: Optional[int]
    has_more: bool
