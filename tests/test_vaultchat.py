"""Correctness tests for the VaultChat search library."""
from __future__ import annotations

import os
import random
import sys
import tempfile
import threading
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from vaultchat import VaultChatSearch
from vaultchat.types import INF_POS, PAGE_SIZE


class VaultChatTestCase(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.db_path = os.path.join(self._tmp.name, "test.db")
        self.vc = VaultChatSearch(self.db_path)

    def tearDown(self):
        self.vc.close()
        self._tmp.cleanup()

    # -- helpers -----------------------------------------------------------

    def post(self, msg_id, channel="c1", sent_at=0, terms=("t",), body=None):
        self.vc.post_message(
            msg_id, channel, sent_at, body or f"body-{msg_id}", list(terms)
        )

    def drain(self, session_id, first_page):
        """Collect all hits from a session until has_more is False."""
        hits = list(first_page.hits)
        page = first_page
        guard = 0
        while page.has_more:
            guard += 1
            self.assertLess(guard, 10000, "paging did not terminate")
            page = self.vc.next_page(session_id)
            hits.extend(page.hits)
        return hits


class TestOrdering(VaultChatTestCase):
    def test_sent_at_desc_then_id_desc(self):
        self.vc.join_channel("u", "c1")
        # Same sent_at for a large group; ordering must fall back to id desc.
        for i in range(1, 121):
            self.post(i, sent_at=1000 if i <= 60 else 500)
        page = self.vc.open_search("u", ["t"])
        self.assertTrue(page.has_more)
        self.assertEqual(len(page.hits), PAGE_SIZE)
        self.assertEqual([h.msg_id for h in page.hits], list(range(60, 10, -1)))
        rest = self.drain(page.session_id, self._empty_page(page.session_id))
        all_hits = list(page.hits) + rest
        got = [(h.sent_at, h.msg_id) for h in all_hits]
        want = sorted(((h.sent_at, h.msg_id) for h in all_hits), reverse=True)
        self.assertEqual(got, want)
        self.assertEqual(len({h.msg_id for h in all_hits}), len(all_hits))
        self.assertEqual(len(all_hits), 120)

    def _empty_page(self, session_id):
        class P:
            hits = ()
            has_more = True
        return P()


class TestFrozenSnapshot(VaultChatTestCase):
    def test_edit_after_open_keeps_old_version(self):
        self.vc.join_channel("u", "c1")
        self.post(1, sent_at=10, terms=("alpha", "common"), body="v1-alpha")
        self.post(2, sent_at=20, terms=("beta", "common"), body="v1-beta")
        page = self.vc.open_search("u", ["common"])
        # Edit after the session opened: msg 2 drops 'common', msg 1 changes body.
        self.vc.edit_message(2, "v2-no-common", ["other"])
        self.vc.edit_message(1, "v1-alpha-edited", ["alpha", "common"])
        hits = self.drain(page.session_id, page)
        self.assertEqual([h.msg_id for h in hits], [2, 1])
        self.assertEqual(hits[0].body, "v1-beta")   # frozen body
        self.assertEqual(hits[1].body, "v1-alpha")  # frozen body
        # A new session sees the edited state instead.
        page2 = self.vc.open_search("u", ["common"])
        hits2 = self.drain(page2.session_id, page2)
        self.assertEqual([(h.msg_id, h.body) for h in hits2],
                         [(1, "v1-alpha-edited")])

    def test_late_import_never_enters_old_session(self):
        self.vc.join_channel("u", "c1")
        self.post(1, sent_at=100, terms=("x",))
        page = self.vc.open_search("u", ["x"])
        # Late import with a very old sent_at must not enter the old session.
        self.post(2, sent_at=1, terms=("x",))
        self.post(3, sent_at=10**9, terms=("x",))
        hits = self.drain(page.session_id, page)
        self.assertEqual([h.msg_id for h in hits], [1])
        page2 = self.vc.open_search("u", ["x"])
        hits2 = self.drain(page2.session_id, page2)
        self.assertEqual([h.msg_id for h in hits2], [3, 1, 2])


class TestVisibility(VaultChatTestCase):
    def test_recall_hides_and_page_backfills(self):
        self.vc.join_channel("u", "c1")
        for i in range(1, 201):
            self.post(i, sent_at=i, terms=("x",))
        page = self.vc.open_search("u", ["x"])
        self.assertEqual(len(page.hits), PAGE_SIZE)
        # Recall a large consecutive run; pages must still be full.
        for i in range(100, 161):
            self.vc.recall_message(i)
        hits = self.drain(page.session_id, self._empty(page.session_id))
        got = [h.msg_id for h in hits]
        self.assertNotIn(100, got)
        self.assertNotIn(160, got)
        # Visible after cursor: ids 51..99 minus none = 49, then 1..49.
        self.assertEqual(got, list(range(99, 50, -1)) + list(range(50, 0, -1)))

    def test_leave_and_rejoin(self):
        self.vc.join_channel("u", "c1")
        self.vc.join_channel("u", "c2")
        for i in range(1, 101):
            self.post(i, channel="c1", sent_at=i, terms=("x",))
        for i in range(101, 201):
            self.post(i, channel="c2", sent_at=i - 100, terms=("x",))
        page = self.vc.open_search("u", ["x"])
        # First page: 50 hits, ids 100..51 (c1) merged with 200..151 (c2),
        # same sent_at -> higher id first, so c2 wins every tie.
        self.assertEqual([h.msg_id for h in page.hits[:3]], [200, 100, 199])
        self.vc.leave_channel("u", "c2")
        page2 = self.vc.next_page(page.session_id)
        self.assertTrue(all(h.channel_id == "c1" for h in page2.hits))
        self.vc.join_channel("u", "c2")
        page3 = self.vc.next_page(page.session_id)
        self.assertTrue(page3.hits)
        cursor = page2.cursor
        for h in page3.hits:
            self.assertLess((h.sent_at, h.msg_id), cursor)
        # Messages from c2 that the cursor had passed are not re-emitted.
        passed = {h.msg_id for h in page.hits} | {h.msg_id for h in page2.hits}
        self.assertFalse(passed & {h.msg_id for h in page3.hits})

    def test_leave_all_channels_then_rejoin_continues(self):
        self.vc.join_channel("u", "c1")
        for i in range(1, 121):
            self.post(i, sent_at=i, terms=("x",))
        page = self.vc.open_search("u", ["x"])
        self.assertEqual(len(page.hits), PAGE_SIZE)
        self.vc.leave_channel("u", "c1")
        empty = self.vc.next_page(page.session_id)
        self.assertEqual(empty.hits, ())
        self.assertTrue(empty.has_more)  # session not exhausted
        self.vc.join_channel("u", "c1")
        resumed = self.vc.next_page(page.session_id)
        self.assertEqual([h.msg_id for h in resumed.hits],
                         list(range(70, 20, -1)))

    def test_recall_before_page_call_takes_effect(self):
        self.vc.join_channel("u", "c1")
        for i in range(1, 61):
            self.post(i, sent_at=i, terms=("x",))
        page = self.vc.open_search("u", ["x"])
        self.assertEqual(len(page.hits), PAGE_SIZE)
        # Recall completes strictly before the next page call: must apply.
        for i in range(11, 61):
            self.vc.recall_message(i)
        page2 = self.vc.next_page(page.session_id)
        self.assertEqual([h.msg_id for h in page2.hits], list(range(10, 0, -1)))
        self.assertFalse(page2.has_more)

    def test_new_channel_does_not_expand_old_session(self):
        self.vc.join_channel("u", "c1")
        self.post(1, channel="c1", sent_at=1, terms=("x",))
        page = self.vc.open_search("u", ["x"])
        self.vc.join_channel("u", "c9")
        self.post(2, channel="c9", sent_at=2, terms=("x",))
        # c9 was not in the frozen channel set; also msg 2 is a late import.
        hits = self.drain(page.session_id, page)
        self.assertEqual([h.msg_id for h in hits], [1])

    def _empty(self, session_id):
        class P:
            hits = ()
            has_more = True
        return P()


class TestPageFill(VaultChatTestCase):
    def test_invisible_run_does_not_shorten_page(self):
        self.vc.join_channel("u", "c1")
        n = 400
        for i in range(1, n + 1):
            self.post(i, sent_at=i, terms=("x",))
        page = self.vc.open_search("u", ["x"])
        # Recall a big consecutive block right after the first page.
        for i in range(250, 351):
            self.vc.recall_message(i)
        self.vc.leave_channel("u", "c1")  # also hide via membership
        self.vc.join_channel("u", "c1")
        page2 = self.vc.next_page(page.session_id)
        # 50 visible matches remain after the cursor (ids 1..249 minus
        # recalled 250..350 -> 1..249 all visible): page must be full.
        self.assertEqual(len(page2.hits), PAGE_SIZE)
        self.assertEqual([h.msg_id for h in page2.hits],
                         list(range(249, 199, -1)))


if __name__ == "__main__":
    unittest.main()
