"""SQLite schema and connection helpers.

Storage layout
--------------
meta(key, value)                -- global monotonic sequence ("seq")
messages(id, channel_id, sent_at, created_seq, recalled)
versions(msg_id, version, seq, body)   -- immutable version history
term_intervals(term, msg_id, start_seq, end_seq)
    -- term is present in the message version that is current for any
       global seq in [start_seq, end_seq); END_SEQ_INF means "still open".
memberships(user_id, channel_id)

A search session opened at global seq S therefore sees, for every message,
exactly the version whose intervals cover S; messages with created_seq > S
(late imports) and versions with seq > S (later edits) are invisible to it.
"""
from __future__ import annotations

import sqlite3
import threading

END_SEQ_INF = (1 << 62) - 1

SCHEMA = """
CREATE TABLE IF NOT EXISTS meta (
    key   TEXT PRIMARY KEY,
    value INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS messages (
    id          INTEGER PRIMARY KEY,
    channel_id  TEXT NOT NULL,
    sent_at     INTEGER NOT NULL,
    created_seq INTEGER NOT NULL,
    recalled    INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS idx_msg_channel_time
    ON messages (channel_id, sent_at, id);
CREATE TABLE IF NOT EXISTS versions (
    msg_id  INTEGER NOT NULL,
    version INTEGER NOT NULL,
    seq     INTEGER NOT NULL,
    body    TEXT NOT NULL,
    PRIMARY KEY (msg_id, version)
);
CREATE INDEX IF NOT EXISTS idx_versions_msg_seq
    ON versions (msg_id, seq);
CREATE TABLE IF NOT EXISTS term_intervals (
    term      TEXT NOT NULL,
    msg_id    INTEGER NOT NULL,
    start_seq INTEGER NOT NULL,
    end_seq   INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_ti_msg_term
    ON term_intervals (msg_id, term, start_seq);
CREATE TABLE IF NOT EXISTS memberships (
    user_id    TEXT NOT NULL,
    channel_id TEXT NOT NULL,
    PRIMARY KEY (user_id, channel_id)
);
"""


def _connect(path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(path, check_same_thread=False)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA busy_timeout=5000")
    return conn


class Database:
    """Owns one write connection (serialized by a lock) plus thread-local
    read connections. Read transactions give each paging call a single
    consistent snapshot of recall/membership state."""

    def __init__(self, path: str):
        self.path = path
        self._write_conn = _connect(path)
        self._write_conn.executescript(SCHEMA)
        self._write_conn.execute(
            "INSERT OR IGNORE INTO meta(key, value) VALUES ('seq', 0)"
        )
        self._write_conn.commit()
        self.write_lock = threading.Lock()
        self._local = threading.local()

    def write_conn(self) -> sqlite3.Connection:
        """Caller must hold ``write_lock``."""
        return self._write_conn

    def read_conn(self) -> sqlite3.Connection:
        conn = getattr(self._local, "conn", None)
        if conn is None:
            conn = _connect(self.path)
            self._local.conn = conn
        return conn

    def close(self) -> None:
        self._write_conn.close()
        conn = getattr(self._local, "conn", None)
        if conn is not None:
            conn.close()
            self._local.conn = None
