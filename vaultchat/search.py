"""Search sessions and cursor-based paging.

A session freezes, at open time: the global seq S, the user's channel set,
and the query terms. Matching always uses the version that was latest at S
(via term_intervals covering S); display uses that version's body. Recall
and membership, by contrast, are re-evaluated on every page inside a single
read transaction, so each page reflects exactly one complete current state.

Cursor semantics: the cursor is a global (sent_at, msg_id) watermark that
only advances to the last *returned* hit. Messages the watermark has passed
are never re-emitted; messages beyond it in a frozen channel may still
appear if the user re-joins that channel. ``has_more`` is therefore decided
against *all frozen channels* ignoring current recall/membership, so a
session is only exhausted when no snapshot messages remain at all.
"""
from __future__ import annotations

import heapq
import itertools
import threading
import uuid
from dataclasses import dataclass, field

from vaultchat.db import Database
from vaultchat.types import INF_POS, PAGE_SIZE, SearchHit, SearchPage

_CHUNK = 512

_CANDIDATE_SQL = (
    "SELECT id, sent_at FROM messages"
    " WHERE channel_id = ? AND recalled = 0 AND created_seq <= ?"
    " AND (sent_at < ? OR (sent_at = ? AND id < ?))"
    " ORDER BY sent_at DESC, id DESC LIMIT ?"
)
_TERM_SQL = (
    "SELECT 1 FROM term_intervals"
    " WHERE msg_id = ? AND term = ? AND start_seq <= ? AND end_seq > ? LIMIT 1"
)
_BODY_SQL = (
    "SELECT body FROM versions WHERE msg_id = ? AND seq <= ?"
    " ORDER BY version DESC LIMIT 1"
)
_MEMBER_SQL = "SELECT channel_id FROM memberships WHERE user_id = ?"
_SEQ_SQL = "SELECT value FROM meta WHERE key = 'seq'"
_MORE_SQL_HEAD = (
    "SELECT 1 FROM messages WHERE recalled = 0 AND created_seq <= ?"
    " AND (sent_at < ? OR (sent_at = ? AND id < ?))"
    " AND channel_id IN ("
)


@dataclass
class _Session:
    user_id: str
    terms: tuple[str, ...]
    channels: frozenset  # frozen at open
    seq: int  # frozen at open
    cursor: tuple[int, int] = (INF_POS, INF_POS)  # (sent_at, msg_id)
    done: bool = False
    lock: threading.Lock = field(default_factory=threading.Lock)


class _ChannelScan:
    """Chunked backwards scan of one channel's currently-visible messages."""

    __slots__ = ("_conn", "channel", "_seq", "_pos", "_buf", "_exhausted")

    def __init__(self, conn, channel: str, seq: int, pos: tuple[int, int]):
        self._conn = conn
        self.channel = channel
        self._seq = seq
        self._pos = pos
        self._buf: list[tuple[int, int]] = []
        self._exhausted = False

    def next(self) -> tuple[int, int] | None:
        if not self._buf:
            if self._exhausted:
                return None
            rows = self._conn.execute(
                _CANDIDATE_SQL,
                (self.channel, self._seq, self._pos[0], self._pos[0],
                 self._pos[1], _CHUNK),
            ).fetchall()
            if len(rows) < _CHUNK:
                self._exhausted = True
            if not rows:
                return None
            self._buf = list(rows)
        msg_id, sent_at = self._buf.pop(0)
        self._pos = (sent_at, msg_id)
        return msg_id, sent_at


class SessionManager:
    def __init__(self, db: Database):
        self._db = db
        self._sessions: dict[str, _Session] = {}
        self._sessions_lock = threading.Lock()

    # -- public API ------------------------------------------------------

    def open_search(self, user_id: str, terms: list[str]) -> SearchPage:
        unique_terms = tuple(dict.fromkeys(terms))
        if not unique_terms:
            raise ValueError("query must contain at least one term")
        conn = self._db.read_conn()
        conn.execute("BEGIN")
        try:
            seq = conn.execute(_SEQ_SQL).fetchone()[0]
            channels = frozenset(
                r[0] for r in conn.execute(_MEMBER_SQL, (user_id,))
            )
            session = _Session(
                user_id=user_id, terms=unique_terms, channels=channels, seq=seq
            )
            session_id = uuid.uuid4().hex
            with self._sessions_lock:
                self._sessions[session_id] = session
            hits, has_more = self._fill_page(conn, session)
            conn.commit()
        except Exception:
            conn.rollback()
            with self._sessions_lock:
                self._sessions.pop(session_id, None)
            raise
        return self._make_page(session_id, hits, has_more)

    def next_page(self, session_id: str) -> SearchPage:
        session = self._get(session_id)
        with session.lock:
            if session.done:
                return self._make_page(session_id, [], False)
            conn = self._db.read_conn()
            conn.execute("BEGIN")
            try:
                hits, has_more = self._fill_page(conn, session)
                conn.commit()
            except Exception:
                conn.rollback()
                raise
            session.done = not has_more
            return self._make_page(session_id, hits, has_more)

    def close_search(self, session_id: str) -> None:
        with self._sessions_lock:
            self._sessions.pop(session_id, None)

    # -- internals ---------------------------------------------------------

    def _get(self, session_id: str) -> _Session:
        with self._sessions_lock:
            session = self._sessions.get(session_id)
        if session is None:
            raise KeyError(f"unknown search session {session_id}")
        return session

    def _fill_page(
        self, conn, session: _Session
    ) -> tuple[list[SearchHit], bool]:
        """Collect up to PAGE_SIZE hits after the session cursor that match
        the frozen terms and are visible in *this* transaction's snapshot.
        Advances the session cursor to the last returned hit."""
        member_channels = {
            r[0] for r in conn.execute(_MEMBER_SQL, (session.user_id,))
        }
        active = sorted(session.channels & member_channels)

        hits: list[SearchHit] = []
        if active:
            pos = session.cursor
            scans = [_ChannelScan(conn, ch, session.seq, pos) for ch in active]
            counter = itertools.count()
            heap: list[tuple[int, int, int, int, _ChannelScan]] = []
            for idx, scan in enumerate(scans):
                row = scan.next()
                if row is not None:
                    msg_id, sent_at = row
                    heapq.heappush(
                        heap, (-sent_at, -msg_id, idx, next(counter), scan)
                    )

            term_checks = [(term, conn.cursor()) for term in session.terms]
            body_cur = conn.cursor()
            while heap and len(hits) < PAGE_SIZE:
                neg_sent, neg_id, idx, _, scan = heapq.heappop(heap)
                msg_id, sent_at = -neg_id, -neg_sent
                if self._matches(term_checks, msg_id, session.seq):
                    body = body_cur.execute(
                        _BODY_SQL, (msg_id, session.seq)
                    ).fetchone()[0]
                    hits.append(
                        SearchHit(
                            msg_id=msg_id,
                            channel_id=scan.channel,
                            sent_at=sent_at,
                            body=body,
                        )
                    )
                    pos = (sent_at, msg_id)
                row = scan.next()
                if row is not None:
                    nid, nsent = row
                    heapq.heappush(
                        heap, (-nsent, -nid, idx, next(counter), scan)
                    )
            session.cursor = pos

        has_more = self._has_more(conn, session)
        return hits, has_more

    @staticmethod
    def _has_more(conn, session: _Session) -> bool:
        """True while any non-recalled snapshot message (ignoring current
        membership) still sits beyond the cursor in a frozen channel.
        Recalled messages never come back, so they cannot yield future
        hits; left channels can be re-joined, so they still count."""
        if not session.channels:
            return False
        sent_at, msg_id = session.cursor
        placeholders = ",".join("?" for _ in session.channels)
        sql = _MORE_SQL_HEAD + placeholders + ") LIMIT 1"
        params = (session.seq, sent_at, sent_at, msg_id, *sorted(session.channels))
        return conn.execute(sql, params).fetchone() is not None

    @staticmethod
    def _matches(term_checks, msg_id: int, seq: int) -> bool:
        for term, cur in term_checks:
            row = cur.execute(_TERM_SQL, (msg_id, term, seq, seq)).fetchone()
            if row is None:
                return False
        return True

    @staticmethod
    def _make_page(
        session_id: str, hits: list[SearchHit], has_more: bool
    ) -> SearchPage:
        cursor = (hits[-1].sent_at, hits[-1].msg_id) if hits else None
        return SearchPage(
            session_id=session_id,
            hits=tuple(hits),
            has_more=has_more,
            cursor=cursor,
        )
