"""Concurrency test: membership changes, recalls and paging interleave
across threads. Every returned page must be explainable by one complete
state; in particular nothing that lost access *before* the page call
started may be returned.

Invariant checked per page (with logical clocks): a hit is definitely
invalid iff it was recalled before the call started, or its channel was
continuously hidden (left, never re-joined) for the whole call interval.
"""
from __future__ import annotations

import os
import random
import sys
import tempfile
import threading
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from vaultchat import VaultChatSearch


class _Shared:
    def __init__(self):
        self.lock = threading.Lock()
        self.counter = 0
        self.recalled = set()
        self.member_events = []  # (counter, 'join'|'leave', channel)

    def record_recall(self, mid):
        with self.lock:
            self.counter += 1
            self.recalled.add(mid)

    def record_membership(self, kind, ch):
        with self.lock:
            self.counter += 1
            self.member_events.append((self.counter, kind, ch))

    def do_recall(self, vc, mid):
        # DB commit and logical-clock record happen in one critical
        # section, so the clock order matches the DB commit order.
        with self.lock:
            vc.recall_message(mid)
            self.counter += 1
            self.recalled.add(mid)

    def do_membership(self, vc, kind, ch, user):
        with self.lock:
            if kind == "leave":
                vc.leave_channel(user, ch)
            else:
                vc.join_channel(user, ch)
            self.counter += 1
            self.member_events.append((self.counter, kind, ch))

    def snapshot_start(self):
        with self.lock:
            return self.counter, set(self.recalled)

    def snapshot_end(self):
        with self.lock:
            return self.counter, list(self.member_events)


class TestInterleavedPaging(unittest.TestCase):
    def test_recall_and_membership_vs_paging(self):
        tmp = tempfile.TemporaryDirectory()
        try:
            vc = VaultChatSearch(os.path.join(tmp.name, "t.db"))
            user = "u"
            channels = ["c1", "c2", "c3"]
            n = 3000
            for ch in channels:
                vc.join_channel(user, ch)
            vc.post_messages(
                [
                    (i, channels[i % len(channels)], i, f"body-{i}", ["x"])
                    for i in range(1, n + 1)
                ]
            )

            page = vc.open_search(user, ["x"])
            session_id = page.session_id
            self.assertEqual(len(page.hits), 50)

            shared = _Shared()
            stop = threading.Event()
            errors = []

            def writer(seed):
                rng = random.Random(seed)
                try:
                    while not stop.is_set():
                        if rng.random() < 0.5:
                            shared.do_recall(vc, rng.randint(1, n))
                        else:
                            ch = rng.choice(channels)
                            kind = "leave" if rng.random() < 0.5 else "join"
                            shared.do_membership(vc, kind, ch, user)
                except Exception as exc:  # pragma: no cover
                    errors.append(exc)

            threads = [threading.Thread(target=writer, args=(s,)) for s in (1, 2, 3)]
            for t in threads:
                t.start()

            seen = set()
            prev_key = None
            pages = 0
            try:
                current = page
                while True:
                    pages += 1
                    for h in current.hits:
                        key = (h.sent_at, h.msg_id)
                        self.assertNotIn(h.msg_id, seen, "duplicate hit")
                        seen.add(h.msg_id)
                        if prev_key is not None:
                            self.assertLess(key, prev_key, "order violation")
                        prev_key = key
                    if not current.has_more:
                        break
                    t0, recalled_t0 = shared.snapshot_start()
                    current = vc.next_page(session_id)
                    t1, events = shared.snapshot_end()

                    hidden_throughout = set()
                    for ch in channels:
                        before = [k for (c, k, cc) in events
                                  if cc == ch and c <= t0]
                        hidden_at_t0 = bool(before) and before[-1] == "leave"
                        joined_during = any(
                            k == "join"
                            for (c, k, cc) in events
                            if cc == ch and t0 < c <= t1
                        )
                        if hidden_at_t0 and not joined_during:
                            hidden_throughout.add(ch)

                    for h in current.hits:
                        self.assertNotIn(
                            h.msg_id, recalled_t0,
                            "returned a message recalled before the call",
                        )
                        self.assertNotIn(
                            h.channel_id, hidden_throughout,
                            "returned a message from a channel hidden for the "
                            "whole call interval",
                        )
            finally:
                stop.set()
                for t in threads:
                    t.join()
                vc.close()
            self.assertFalse(errors)
            self.assertGreater(len(seen), 100)
            self.assertGreater(pages, 5)
        finally:
            tmp.cleanup()


if __name__ == "__main__":
    unittest.main()
