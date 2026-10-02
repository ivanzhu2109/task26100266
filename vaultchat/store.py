"""Write-side operations: messages, versions, recalls, membership.

Every public method is one committed write transaction under the database
write lock, so a paging call that starts after the method returns is
guaranteed to observe its effects.
"""
from __future__ import annotations

from typing import Iterable, Sequence

from vaultchat.db import Database, END_SEQ_INF


class Store:
    def __init__(self, db: Database):
        self._db = db

    # -- internal helpers (caller holds write_lock) ---------------------

    @staticmethod
    def _next_seq(conn) -> int:
        conn.execute("UPDATE meta SET value = value + 1 WHERE key = 'seq'")
        return conn.execute("SELECT value FROM meta WHERE key = 'seq'").fetchone()[0]

    def _insert_message(
        self,
        conn,
        msg_id: int,
        channel_id: str,
        sent_at: int,
        body: str,
        terms: Sequence[str],
    ) -> None:
        seq = self._next_seq(conn)
        conn.execute(
            "INSERT INTO messages(id, channel_id, sent_at, created_seq, recalled)"
            " VALUES (?, ?, ?, ?, 0)",
            (msg_id, channel_id, sent_at, seq),
        )
        conn.execute(
            "INSERT INTO versions(msg_id, version, seq, body) VALUES (?, 1, ?, ?)",
            (msg_id, seq, body),
        )
        conn.executemany(
            "INSERT INTO term_intervals(term, msg_id, start_seq, end_seq)"
            " VALUES (?, ?, ?, ?)",
            [(t, msg_id, seq, END_SEQ_INF) for t in set(terms)],
        )

    # -- message state ---------------------------------------------------

    def post_message(
        self, msg_id: int, channel_id: str, sent_at: int, body: str, terms: Sequence[str]
    ) -> None:
        with self._db.write_lock:
            conn = self._db.write_conn()
            self._insert_message(conn, msg_id, channel_id, sent_at, body, terms)
            conn.commit()

    def post_messages(
        self,
        batch: Iterable[tuple[int, str, int, str, Sequence[str]]],
    ) -> None:
        """Bulk import (one transaction). Each message still gets its own
        global seq, so late-imported messages never enter older sessions."""
        with self._db.write_lock:
            conn = self._db.write_conn()
            for msg_id, channel_id, sent_at, body, terms in batch:
                self._insert_message(conn, msg_id, channel_id, sent_at, body, terms)
            conn.commit()

    def edit_message(self, msg_id: int, body: str, terms: Sequence[str]) -> None:
        with self._db.write_lock:
            conn = self._db.write_conn()
            row = conn.execute(
                "SELECT MAX(version) FROM versions WHERE msg_id = ?", (msg_id,)
            ).fetchone()
            if row is None or row[0] is None:
                raise KeyError(f"unknown message {msg_id}")
            version = row[0] + 1
            seq = self._next_seq(conn)
            conn.execute(
                "INSERT INTO versions(msg_id, version, seq, body) VALUES (?, ?, ?, ?)",
                (msg_id, version, seq, body),
            )
            current = {
                r[0]
                for r in conn.execute(
                    "SELECT term FROM term_intervals"
                    " WHERE msg_id = ? AND end_seq = ?",
                    (msg_id, END_SEQ_INF),
                )
            }
            wanted = set(terms)
            conn.executemany(
                "UPDATE term_intervals SET end_seq = ?"
                " WHERE msg_id = ? AND term = ? AND end_seq = ?",
                [(seq, msg_id, t, END_SEQ_INF) for t in current - wanted],
            )
            conn.executemany(
                "INSERT INTO term_intervals(term, msg_id, start_seq, end_seq)"
                " VALUES (?, ?, ?, ?)",
                [(t, msg_id, seq, END_SEQ_INF) for t in wanted - current],
            )
            conn.commit()

    def recall_message(self, msg_id: int) -> None:
        with self._db.write_lock:
            conn = self._db.write_conn()
            conn.execute("UPDATE messages SET recalled = 1 WHERE id = ?", (msg_id,))
            conn.commit()

    # -- membership --------------------------------------------------------

    def join_channel(self, user_id: str, channel_id: str) -> None:
        with self._db.write_lock:
            conn = self._db.write_conn()
            conn.execute(
                "INSERT OR IGNORE INTO memberships(user_id, channel_id) VALUES (?, ?)",
                (user_id, channel_id),
            )
            conn.commit()

    def leave_channel(self, user_id: str, channel_id: str) -> None:
        with self._db.write_lock:
            conn = self._db.write_conn()
            conn.execute(
                "DELETE FROM memberships WHERE user_id = ? AND channel_id = ?",
                (user_id, channel_id),
            )
            conn.commit()
