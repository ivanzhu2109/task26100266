"""SQLite-backed storage: message state writes and membership changes."""

from __future__ import annotations

import sqlite3
import threading
from array import array
from typing import Dict, Iterable, List, Optional, Set

from .models import Message, Page

_SCHEMA = """
CREATE TABLE IF NOT EXISTS meta (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    seq INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS messages (
    msg_id INTEGER PRIMARY KEY,
    dense INTEGER NOT NULL UNIQUE,
    channel_id INTEGER NOT NULL,
    sent_at INTEGER NOT NULL,
    created_seq INTEGER NOT NULL,
    recalled INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS idx_messages_order ON messages(sent_at, msg_id);
CREATE TABLE IF NOT EXISTS versions (
    msg_id INTEGER NOT NULL,
    version INTEGER NOT NULL,
    seq INTEGER NOT NULL,
    body TEXT NOT NULL,
    PRIMARY KEY (msg_id, version)
) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS current_terms (
    term TEXT NOT NULL,
    dense INTEGER NOT NULL,
    PRIMARY KEY (term, dense)
) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS membership (
    user_id INTEGER NOT NULL,
    channel_id INTEGER NOT NULL,
    PRIMARY KEY (user_id, channel_id)
);
"""


class VaultChatStore:
    """Main entry point used by upper layers.

    Thread-safe for single-process multi-threaded use: every public
    operation runs under one re-entrant lock, so each search page is
    assembled against one complete, consistent state.
    """

    def __init__(self, db_path: str = ":memory:") -> None:
        self._lock = threading.RLock()
        self._conn = sqlite3.connect(db_path, check_same_thread=False)
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA synchronous=NORMAL")
        self._conn.executescript(_SCHEMA)
        row = self._conn.execute("SELECT seq FROM meta WHERE id = 1").fetchone()
        if row is None:
            self._conn.execute("INSERT INTO meta(id, seq) VALUES (1, 0)")
            self._conn.commit()
            self._seq = 0
        else:
            self._seq = int(row[0])
        # In-memory global indexes (shared by all sessions; part of the
        # data baseline, rebuilt from SQLite on open).
        self._dense_of: Dict[int, int] = {}
        self._msgid_of: List[int] = []
        self._channel_of: array = array("q")
        for msg_id, dense, channel_id in self._conn.execute(
            "SELECT msg_id, dense, channel_id FROM messages ORDER BY dense"
        ):
            self._dense_of[msg_id] = dense
            self._msgid_of.append(msg_id)
            self._channel_of.append(channel_id)
        self._sessions: Dict[int, "_Session"] = {}
        self._next_session_id = 1

    # ---- internal helpers ----

    def _bump_seq(self) -> int:
        self._seq += 1
        self._conn.execute("UPDATE meta SET seq = ? WHERE id = 1", (self._seq,))
        return self._seq

    # ---- 写入消息状态 ----

    def publish(self, msg_id: int, channel_id: int, sent_at: int,
                body: str, terms: Iterable[str]) -> None:
        """Publish a new message (caller guarantees validity/uniqueness)."""
        with self._lock:
            seq = self._bump_seq()
            dense = len(self._msgid_of)
            self._conn.execute(
                "INSERT INTO messages(msg_id, dense, channel_id, sent_at,"
                " created_seq, recalled) VALUES (?,?,?,?,?,0)",
                (msg_id, dense, channel_id, sent_at, seq),
            )
            self._conn.execute(
                "INSERT INTO versions(msg_id, version, seq, body)"
                " VALUES (?,1,?,?)",
                (msg_id, seq, body),
            )
            term_list = sorted(set(terms))
            self._conn.executemany(
                "INSERT INTO current_terms(term, dense) VALUES (?,?)",
                [(t, dense) for t in term_list],
            )
            self._conn.commit()
            self._dense_of[msg_id] = dense
            self._msgid_of.append(msg_id)
            self._channel_of.append(channel_id)

    def edit(self, msg_id: int, body: str, terms: Iterable[str]) -> None:
        """Create a new version of an existing message."""
        with self._lock:
            row = self._conn.execute(
                "SELECT dense, COALESCE((SELECT MAX(version) FROM versions"
                " WHERE msg_id = ?), 0) FROM messages WHERE msg_id = ?",
                (msg_id, msg_id),
            ).fetchone()
            if row is None:
                raise KeyError(f"unknown msg_id {msg_id}")
            dense, version = int(row[0]), int(row[1]) + 1
            seq = self._bump_seq()
            self._conn.execute(
                "INSERT INTO versions(msg_id, version, seq, body)"
                " VALUES (?,?,?,?)",
                (msg_id, version, seq, body),
            )
            self._conn.execute(
                "DELETE FROM current_terms WHERE dense = ?", (dense,))
            term_list = sorted(set(terms))
            self._conn.executemany(
                "INSERT INTO current_terms(term, dense) VALUES (?,?)",
                [(t, dense) for t in term_list],
            )
            self._conn.commit()

    def bulk_publish(
        self,
        items: Iterable["tuple[int, int, int, str, Iterable[str]]"],
    ) -> None:
        """Import many messages in one transaction.

        Each item is (msg_id, channel_id, sent_at, body, terms). Messages
        imported late keep their original sent_at but never enter search
        sessions that were opened before the import.
        """
        with self._lock:
            for msg_id, channel_id, sent_at, body, terms in items:
                seq = self._bump_seq()
                dense = len(self._msgid_of)
                self._conn.execute(
                    "INSERT INTO messages(msg_id, dense, channel_id,"
                    " sent_at, created_seq, recalled)"
                    " VALUES (?,?,?,?,?,0)",
                    (msg_id, dense, channel_id, sent_at, seq),
                )
                self._conn.execute(
                    "INSERT INTO versions(msg_id, version, seq, body)"
                    " VALUES (?,1,?,?)",
                    (msg_id, seq, body),
                )
                term_list = sorted(set(terms))
                self._conn.executemany(
                    "INSERT INTO current_terms(term, dense) VALUES (?,?)",
                    [(t, dense) for t in term_list],
                )
                self._dense_of[msg_id] = dense
                self._msgid_of.append(msg_id)
                self._channel_of.append(channel_id)
            self._conn.commit()

    def recall(self, msg_id: int) -> None:
        """Recall a message. Irrevocable."""
        with self._lock:
            self._bump_seq()
            cur = self._conn.execute(
                "UPDATE messages SET recalled = 1 WHERE msg_id = ?",
                (msg_id,))
            if cur.rowcount == 0:
                raise KeyError(f"unknown msg_id {msg_id}")
            self._conn.commit()

    # ---- 变更成员关系 ----

    def add_member(self, user_id: int, channel_id: int) -> None:
        with self._lock:
            self._bump_seq()
            self._conn.execute(
                "INSERT OR IGNORE INTO membership(user_id, channel_id)"
                " VALUES (?,?)",
                (user_id, channel_id),
            )
            self._conn.commit()

    def remove_member(self, user_id: int, channel_id: int) -> None:
        with self._lock:
            self._bump_seq()
            self._conn.execute(
                "DELETE FROM membership WHERE user_id = ? AND channel_id = ?",
                (user_id, channel_id),
            )
            self._conn.commit()

    # ---- 开启搜索 / 继续翻页 ----

    def search(self, user_id: int, terms: Iterable[str],
               limit: int = 50) -> Page:
        from .search import start_search
        with self._lock:
            return start_search(self, user_id, tuple(sorted(set(terms))), limit)

    def next_page(self, session_id: int, limit: int = 50) -> Page:
        from .search import fetch_page
        with self._lock:
            return fetch_page(self, session_id, limit)

    def close(self) -> None:
        with self._lock:
            self._conn.close()
