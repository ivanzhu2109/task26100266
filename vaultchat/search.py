"""Search session management and paging (internal).

A session freezes, at creation time:
  * the global write sequence number (freeze_seq) -- later publishes and
    edits never enter the session;
  * the user's channel set -- later joins of other channels do not expand
    the session;
  * a match bitmap over dense message ids, computed from the terms of the
    version that is current at freeze time.

Paging re-evaluates, under the store lock (one complete state per call):
  * current membership intersected with the frozen channel set;
  * current recall state.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, FrozenSet, List, Optional, Set, Tuple

from .models import Message, Page

if TYPE_CHECKING:  # pragma: no cover
    from .store import VaultChatStore

_FETCH_CHUNK = 512


@dataclass
class _Session:
    """Frozen search session state (in-memory only)."""

    session_id: int
    user_id: int
    terms: Tuple[str, ...]
    freeze_seq: int
    channels: FrozenSet[int]
    bitmap: bytearray
    cursor_sent_at: Optional[int] = None
    cursor_msg_id: Optional[int] = None


def _bitmap_for_term(store: "VaultChatStore", term: str,
                     nbytes: int) -> bytearray:
    bm = bytearray(nbytes)
    rows = store._conn.execute(
        "SELECT dense FROM current_terms WHERE term = ?", (term,))
    for (dense,) in rows:
        bm[dense >> 3] |= 1 << (dense & 7)
    return bm


def _bitmap_and(a: bytearray, b: bytearray) -> bytearray:
    merged = int.from_bytes(a, "little") & int.from_bytes(b, "little")
    return bytearray(merged.to_bytes(len(a), "little"))


def start_search(store: "VaultChatStore", user_id: int,
                 terms: Tuple[str, ...], limit: int) -> Page:
    """Create a frozen session and return its first page."""
    freeze_seq = store._seq
    channels = frozenset(
        r[0]
        for r in store._conn.execute(
            "SELECT channel_id FROM membership WHERE user_id = ?",
            (user_id,))
    )
    n = len(store._msgid_of)
    nbytes = (n + 7) // 8
    if terms and n:
        bitmap = _bitmap_for_term(store, terms[0], nbytes)
        for term in terms[1:]:
            bitmap = _bitmap_and(bitmap, _bitmap_for_term(store, term, nbytes))
    else:
        bitmap = bytearray(nbytes)
    session = _Session(
        session_id=store._next_session_id,
        user_id=user_id,
        terms=terms,
        freeze_seq=freeze_seq,
        channels=channels,
        bitmap=bitmap,
    )
    store._next_session_id += 1
    store._sessions[session.session_id] = session
    return _page(store, session, limit)


def fetch_page(store: "VaultChatStore", session_id: int,
               limit: int) -> Page:
    session = store._sessions.get(session_id)
    if session is None:
        raise KeyError(f"unknown session_id {session_id}")
    return _page(store, session, limit)


def _visible_channels(store: "VaultChatStore", session: _Session) -> Set[int]:
    if not session.channels:
        return set()
    current = {
        r[0]
        for r in store._conn.execute(
            "SELECT channel_id FROM membership WHERE user_id = ?",
            (session.user_id,))
    }
    return current & session.channels


def _frozen_body(store: "VaultChatStore", msg_id: int,
                 freeze_seq: int) -> str:
    row = store._conn.execute(
        "SELECT body FROM versions WHERE msg_id = ? AND seq <= ?"
        " ORDER BY version DESC LIMIT 1",
        (msg_id, freeze_seq),
    ).fetchone()
    return row[0] if row else ""


def _page(store: "VaultChatStore", session: _Session, limit: int) -> Page:
    limit = max(1, min(limit, 50))
    items: List[Message] = []
    # Note: an empty page never permanently exhausts the session. The
    # cursor only advances past returned items, so if visibility is later
    # restored (e.g. the user re-joins a frozen channel), messages that
    # were never passed can appear again.
    visible = _visible_channels(store, session)
    bitmap = session.bitmap
    bitmap_bits = len(bitmap) * 8
    sql = (
        "SELECT msg_id, dense, channel_id, sent_at, recalled"
        " FROM messages"
    )
    params: list = []
    if session.cursor_sent_at is not None:
        sql += (
            " WHERE (sent_at < ? OR (sent_at = ? AND msg_id < ?))"
        )
        params.extend(
            [session.cursor_sent_at, session.cursor_sent_at,
             session.cursor_msg_id]
        )
    sql += " ORDER BY sent_at DESC, msg_id DESC"
    cur = store._conn.execute(sql, params)
    done = False
    while len(items) < limit and not done:
        rows = cur.fetchmany(_FETCH_CHUNK)
        if not rows:
            done = True
            break
        for msg_id, dense, channel_id, sent_at, recalled in rows:
            if recalled:
                continue
            if channel_id not in visible:
                continue
            if dense >= bitmap_bits:
                continue  # published after the freeze
            if not (bitmap[dense >> 3] >> (dense & 7)) & 1:
                continue
            items.append(Message(
                msg_id=msg_id,
                channel_id=channel_id,
                sent_at=sent_at,
                body="",  # filled below
            ))
            if len(items) >= limit:
                break
    reached_end = done or len(items) < limit
    if items:
        last = items[-1]
        session.cursor_sent_at = last.sent_at
        session.cursor_msg_id = last.msg_id
        items = [
            Message(
                msg_id=it.msg_id,
                channel_id=it.channel_id,
                sent_at=it.sent_at,
                body=_frozen_body(store, it.msg_id, session.freeze_seq),
            )
            for it in items
        ]
    cursor = None
    if session.cursor_sent_at is not None:
        cursor = (session.cursor_sent_at << 64) | (
            session.cursor_msg_id & 0xFFFFFFFFFFFFFFFF)
    return Page(
        session_id=session.session_id,
        items=tuple(items),
        cursor=cursor,
        has_more=not reached_end,
    )
