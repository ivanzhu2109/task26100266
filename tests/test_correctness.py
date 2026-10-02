"""Correctness tests: VaultChatStore vs. an independent snapshot model."""

import random
import sys
import unittest
from collections import defaultdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from vaultchat import VaultChatStore


class Model:
    """Independent reference implementing the frozen-snapshot semantics."""

    def __init__(self):
        self.seq = 0
        self.info = {}          # msg_id -> (channel_id, sent_at)
        self.versions = {}      # msg_id -> [(seq, body, frozenset(terms))]
        self.recalled = set()
        self.members = defaultdict(set)   # user_id -> {channel_id}
        self.sessions = {}      # sid -> dict

    def _bump(self):
        self.seq += 1

    def publish(self, msg_id, channel_id, sent_at, body, terms):
        self._bump()
        self.info[msg_id] = (channel_id, sent_at)
        self.versions[msg_id] = [(self.seq, body, frozenset(terms))]

    def edit(self, msg_id, body, terms):
        self._bump()
        self.versions[msg_id].append((self.seq, body, frozenset(terms)))

    def recall(self, msg_id):
        self._bump()
        self.recalled.add(msg_id)

    def add_member(self, user_id, channel_id):
        self._bump()
        self.members[user_id].add(channel_id)

    def remove_member(self, user_id, channel_id):
        self._bump()
        self.members[user_id].discard(channel_id)

    def search(self, user_id, terms):
        freeze = self.seq
        frozen = {}
        for mid, vers in self.versions.items():
            vs = [v for v in vers if v[0] <= freeze]
            if vs:
                frozen[mid] = vs[-1]  # (seq, body, terms) at freeze
        sid = len(self.sessions) + 1
        self.sessions[sid] = {
            "user": user_id,
            "terms": frozenset(terms),
            "channels": set(self.members[user_id]),
            "frozen": frozen,
            "cursor": None,
        }
        return sid

    def page(self, sid, limit=50):
        s = self.sessions[sid]
        visible_channels = s["channels"] & self.members[s["user"]]
        out = []
        cands = []
        for mid, (_, body, terms) in s["frozen"].items():
            channel_id, sent_at = self.info[mid]
            if mid in self.recalled:
                continue
            if channel_id not in visible_channels:
                continue
            if not s["terms"] <= terms:
                continue
            if s["cursor"] is not None:
                cs, cm = s["cursor"]
                if (sent_at, mid) >= (cs, cm):
                    continue
            cands.append((sent_at, mid, channel_id, body))
        cands.sort(key=lambda t: (t[0], t[1]), reverse=True)
        for sent_at, mid, channel_id, body in cands[:limit]:
            out.append((mid, channel_id, sent_at, body))
        if out:
            s["cursor"] = (out[-1][2], out[-1][0])
        return out, len(out) == limit


def page_tuples(page):
    return [(m.msg_id, m.channel_id, m.sent_at, m.body) for m in page.items]


class CorrectnessTest(unittest.TestCase):
    def setUp(self):
        self.store = VaultChatStore()
        self.model = Model()

    def tearDown(self):
        self.store.close()

    def both(self, op, *args):
        getattr(self.store, op)(*args)
        getattr(self.model, op)(*args)

    def run_session(self, user_id, terms, ops_between_pages=(), limit=50):
        """Open a session on both, fully paginate, compare every page."""
        sp = self.store.search(user_id, terms, limit=limit)
        msid = self.model.search(user_id, terms)
        store_all, model_all = [], []
        first = True
        while True:
            if not first:
                sp = self.store.next_page(sp.session_id, limit=limit)
            first = False
            mitems, m_more = self.model.page(msid, limit=limit)
            store_all.extend(page_tuples(sp))
            model_all.extend(mitems)
            self.assertEqual(page_tuples(sp), mitems)
            self.assertEqual(sp.has_more, m_more)
            if not sp.has_more:
                break
            for op, args in ops_between_pages:
                self.both(op, *args)
        ids = [t[0] for t in store_all]
        self.assertEqual(len(ids), len(set(ids)), "duplicate msg across pages")
        return store_all, model_all

    def test_order_and_tie_break(self):
        self.both("add_member", 1, 7)
        for i in range(200):
            self.both("publish", 100 + i, 7, 9999, f"common body {i}",
                      ["common"])
        got, _ = self.run_session(1, ["common"])
        self.assertEqual([t[0] for t in got], list(range(299, 99, -1)))

    def test_same_sent_at_large_bulk(self):
        self.both("add_member", 1, 7)
        for i in range(500):
            self.both("publish", 1000 + i, 7, 12345, "bulk", ["bulk"])
        got, _ = self.run_session(1, ["bulk"])
        self.assertEqual([t[0] for t in got], list(range(1499, 999, -1)))

    def test_late_publish_never_enters_session(self):
        self.both("add_member", 1, 7)
        for i in range(60):
            self.both("publish", 100 + i, 7, 5000 - i, "v", ["t"])
        sp = self.store.search(1, ["t"])
        msid = self.model.search(1, ["t"])
        # Late import with very early sent_at (would sort first).
        self.both("publish", 9000, 7, 1, "late", ["t"])
        self.both("publish", 9001, 7, 99999, "late", ["t"])
        first = True
        seen = []
        while True:
            if not first:
                sp = self.store.next_page(sp.session_id)
            first = False
            mitems, m_more = self.model.page(msid)
            self.assertEqual(page_tuples(sp), mitems)
            seen.extend(m.msg_id for m in sp.items)
            if not sp.has_more:
                break
        self.assertNotIn(9000, seen)
        self.assertNotIn(9001, seen)
        self.assertEqual(len(seen), 60)

    def test_edit_mid_search_freezes_body_and_terms(self):
        self.both("add_member", 1, 7)
        for i in range(80):
            self.both("publish", 100 + i, 7, 5000 - i, f"original {i}",
                      ["keep"])
        sp = self.store.search(1, ["keep"])
        msid = self.model.search(1, ["keep"])
        # Edit some not-yet-returned messages: change body and terms.
        self.both("edit", 140, "edited body", ["other"])
        self.both("edit", 150, "edited body", ["keep", "extra"])
        seen_bodies = {}
        first = True
        while True:
            if not first:
                sp = self.store.next_page(sp.session_id)
            first = False
            mitems, m_more = self.model.page(msid)
            self.assertEqual(page_tuples(sp), mitems)
            for m in sp.items:
                seen_bodies[m.msg_id] = m.body
            if not sp.has_more:
                break
        # 140 no longer matches (term removed after freeze still matches
        # the frozen version! it stays, showing the frozen body).
        self.assertEqual(seen_bodies[140], "original 40")
        self.assertEqual(seen_bodies[150], "original 50")
        self.assertEqual(len(seen_bodies), 80)

    def test_leave_and_rejoin_channel(self):
        self.both("add_member", 1, 7)
        for i in range(120):
            self.both("publish", 100 + i, 7, 5000 - i, "x", ["t"])
        sp = self.store.search(1, ["t"])
        msid = self.model.search(1, ["t"])
        self.assertEqual(len(sp.items), 50)
        self.assertEqual(page_tuples(sp), self.model.page(msid)[0])
        # Leave: everything hidden, pages come back empty but not "more".
        self.both("remove_member", 1, 7)
        sp = self.store.next_page(sp.session_id)
        mitems, _ = self.model.page(msid)
        self.assertEqual(page_tuples(sp), mitems)
        self.assertEqual(len(sp.items), 0)
        self.assertFalse(sp.has_more)
        # Rejoin: messages after the cursor reappear; passed ones are not
        # resent. The first page returned msg_ids 100..149, so the cursor
        # sits at msg_id 149 and 150..199 must now appear.
        self.both("add_member", 1, 7)
        # Session was exhausted while invisible; per spec, re-joining
        # should reveal not-yet-passed messages. The store keeps paging
        # from the cursor even after an empty page.
        sp = self.store.next_page(sp.session_id)
        mitems, m_more = self.model.page(msid)
        self.assertEqual(page_tuples(sp), mitems)
        self.assertEqual([m.msg_id for m in sp.items],
                         list(range(150, 200)))

    def test_recall_hides_and_pages_refill(self):
        self.both("add_member", 1, 7)
        for i in range(200):
            self.both("publish", 100 + i, 7, 5000 - i, "x", ["t"])
        sp = self.store.search(1, ["t"])
        msid = self.model.search(1, ["t"])
        self.assertEqual(len(sp.items), 50)
        self.model.page(msid)
        # Recall a large contiguous run of not-yet-returned messages.
        for mid in range(150, 190):
            self.both("recall", mid)
        sp = self.store.next_page(sp.session_id)
        mitems, m_more = self.model.page(msid)
        self.assertEqual(page_tuples(sp), mitems)
        # Page must be refilled past the invisible run: still 50 items.
        self.assertEqual(len(sp.items), 50)
        self.assertTrue(all(m.msg_id < 150 or m.msg_id >= 190
                            for m in sp.items))
        # Recalled-before-call items must never appear.
        self.assertTrue(all(m.msg_id not in range(150, 190)
                            for m in sp.items))

    def test_revoked_before_page_call_must_apply(self):
        self.both("add_member", 1, 7)
        self.both("add_member", 1, 8)
        for i in range(30):
            self.both("publish", 100 + i, 7, 5000 - i, "x", ["t"])
            self.both("publish", 200 + i, 8, 5000 - i, "x", ["t"])
        sp = self.store.search(1, ["t"], limit=10)
        msid = self.model.search(1, ["t"])
        self.model.page(msid, limit=10)
        # Revoke channel 8 and recall one message *before* the page call.
        self.both("remove_member", 1, 8)
        self.both("recall", 111)
        sp = self.store.next_page(sp.session_id, limit=10)
        mitems, _ = self.model.page(msid, limit=10)
        self.assertEqual(page_tuples(sp), mitems)
        self.assertTrue(all(m.channel_id != 8 for m in sp.items))
        self.assertNotIn(111, [m.msg_id for m in sp.items])

    def test_new_channels_do_not_expand_session(self):
        self.both("add_member", 1, 7)
        for i in range(60):
            self.both("publish", 100 + i, 7, 5000 - i, "x", ["t"])
        sp = self.store.search(1, ["t"])
        msid = self.model.search(1, ["t"])
        # Join a new channel with matching messages after the freeze.
        self.both("add_member", 1, 8)
        for i in range(60):
            self.both("publish", 200 + i, 8, 6000 - i, "x", ["t"])
        first = True
        seen = []
        while True:
            if not first:
                sp = self.store.next_page(sp.session_id)
            first = False
            mitems, m_more = self.model.page(msid)
            self.assertEqual(page_tuples(sp), mitems)
            seen.extend(m.msg_id for m in sp.items)
            if not sp.has_more:
                break
        self.assertTrue(all(mid < 200 for mid in seen))

    def test_differential_random(self):
        return self._differential_random()

    def test_edge_cases(self):
        self.both("add_member", 1, 7)
        for i in range(60):
            self.both("publish", 100 + i, 7, 5000 - i, "x", ["t"])
        # Empty term set matches nothing.
        page = self.store.search(1, [])
        self.assertEqual(len(page.items), 0)
        self.assertFalse(page.has_more)
        # Term nobody has matches nothing.
        page = self.store.search(1, ["absent"])
        self.assertEqual(len(page.items), 0)
        # limit is clamped to 50.
        page = self.store.search(1, ["t"], limit=500)
        self.assertEqual(len(page.items), 50)
        page = self.store.search(1, ["t"], limit=10)
        self.assertEqual(len(page.items), 10)
        # Unknown session id raises.
        with self.assertRaises(KeyError):
            self.store.next_page(99999)
        # User with no membership anywhere gets nothing.
        page = self.store.search(777, ["t"])
        self.assertEqual(len(page.items), 0)
        self.assertFalse(page.has_more)

    def _differential_random(self):
        rng = random.Random(20261002)
        users = [1, 2, 3]
        channels = [10, 11, 12]
        msg_counter = [1000]
        live_msgs = []
        sessions = []  # (store_sid, model_sid)

        def new_msg(sent_at=None):
            mid = msg_counter[0]
            msg_counter[0] += 1
            ch = rng.choice(channels)
            sa = sent_at if sent_at is not None else rng.randint(0, 10000)
            words = rng.sample(["a", "b", "c", "d", "e"], k=rng.randint(1, 3))
            body = " ".join(words) + f" #{mid}"
            self.both("publish", mid, ch, sa, body, words)
            live_msgs.append(mid)
            return mid

        for u in users:
            for ch in channels:
                if rng.random() < 0.7:
                    self.both("add_member", u, ch)
        for _ in range(300):
            new_msg()

        for step in range(400):
            r = rng.random()
            if r < 0.30:
                new_msg()
            elif r < 0.45 and live_msgs:
                mid = rng.choice(live_msgs)
                words = rng.sample(["a", "b", "c", "d", "e"],
                                   k=rng.randint(1, 3))
                self.both("edit", mid, "edited " + " ".join(words), words)
            elif r < 0.55 and live_msgs:
                mid = rng.choice(live_msgs)
                if mid not in self.model.recalled:
                    self.both("recall", mid)
            elif r < 0.70:
                u, ch = rng.choice(users), rng.choice(channels)
                if rng.random() < 0.5:
                    self.both("add_member", u, ch)
                else:
                    self.both("remove_member", u, ch)
            elif r < 0.80:
                u = rng.choice(users)
                n = rng.randint(1, 2)
                terms = rng.sample(["a", "b", "c", "d", "e"], k=n)
                sp = self.store.search(u, terms)
                msid = self.model.search(u, terms)
                sessions.append((sp.session_id, msid, sp))
                # compare first page immediately
                mitems, m_more = self.model.page(msid)
                self.assertEqual(page_tuples(sp), mitems,
                                 f"first page mismatch at step {step}")
                self.assertEqual(sp.has_more, m_more)
            elif sessions:
                idx = rng.randrange(len(sessions))
                ssid, msid, last = sessions[idx]
                sp = self.store.next_page(ssid)
                mitems, m_more = self.model.page(msid)
                self.assertEqual(page_tuples(sp), mitems,
                                 f"page mismatch at step {step}")
                self.assertEqual(sp.has_more, m_more)
                sessions[idx] = (ssid, msid, sp)


if __name__ == "__main__":
    unittest.main()
