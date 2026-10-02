"""Public facade combining write operations and search sessions.

Thread-safe for single-process multi-threaded use: writers are serialized
by a lock, readers use per-thread connections with snapshot transactions.
"""
from __future__ import annotations

from typing import Iterable, Sequence

from vaultchat.db import Database
from vaultchat.search import SessionManager
from vaultchat.store import Store
from vaultchat.types import SearchPage

__all__ = ["VaultChatSearch"]


class VaultChatSearch:
    """Entry point used by the upper layer.

    Capabilities:
      * write message state:  post_message / post_messages / edit_message /
        recall_message
      * change membership:    join_channel / leave_channel
      * open a search:        open_search  (first page, freezes the session)
      * keep paging:          next_page    (cursor carried by the session)
    """

    def __init__(self, db_path: str):
        self._db = Database(db_path)
        self._store = Store(self._db)
        self._sessions = SessionManager(self._db)

    # -- write message state --------------------------------------------

    def post_message(
        self, msg_id: int, channel_id: str, sent_at: int, body: str, terms: Sequence[str]
    ) -> None:
        self._store.post_message(msg_id, channel_id, sent_at, body, terms)

    def post_messages(
        self, batch: Iterable[tuple[int, str, int, str, Sequence[str]]]
    ) -> None:
        self._store.post_messages(batch)

    def edit_message(self, msg_id: int, body: str, terms: Sequence[str]) -> None:
        self._store.edit_message(msg_id, body, terms)

    def recall_message(self, msg_id: int) -> None:
        self._store.recall_message(msg_id)

    # -- membership --------------------------------------------------------

    def join_channel(self, user_id: str, channel_id: str) -> None:
        self._store.join_channel(user_id, channel_id)

    def leave_channel(self, user_id: str, channel_id: str) -> None:
        self._store.leave_channel(user_id, channel_id)

    # -- search --------------------------------------------------------------

    def open_search(self, user_id: str, terms: Sequence[str]) -> SearchPage:
        return self._sessions.open_search(user_id, list(terms))

    def next_page(self, session_id: str) -> SearchPage:
        return self._sessions.next_page(session_id)

    def close_search(self, session_id: str) -> None:
        self._sessions.close_search(session_id)

    # -- lifecycle -----------------------------------------------------------

    def checkpoint(self) -> None:
        """Fold the WAL back into the main database file (maintenance)."""
        with self._db.write_lock:
            self._db.write_conn().execute("PRAGMA wal_checkpoint(TRUNCATE)")

    def close(self) -> None:
        self._db.close()

    def __enter__(self) -> "VaultChatSearch":
        return self

    def __exit__(self, *exc) -> None:
        self.close()
