"""Randomized differential test: the library is compared against a direct
in-memory model that computes, for every page, the expected result from the
frozen first-screen snapshot plus the current recall/membership state."""
from __future__ import annotations

import os
import random
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from vaultchat import VaultChatSearch
from vaultchat.types import INF_POS, PAGE_SIZE


class Model:
    """Brute-force reference implementation of the business rules."""

    def __init__(self):
        self.messages = {}  # id -> [channel, sent_at, created_seq, recalled]
        self.versions = {}  # id -> [(seq, body, terms)]
        self.members = set()  # (user, channel)
        self.seq = 0

    def post(self, mid, ch, sent_at, body, terms):
        self.seq += 1
        self.messages[mid] = [ch, sent_at, self.seq, 0]
        self.versions[mid] = [(self.seq, body, frozenset(terms))]

    def edit(self, mid, body, terms):
        self.seq += 1
        self.versions[mid].append((self.seq, body, frozenset(terms)))

    def recall(self, mid):
        self.messages[mid][3] = 1

    def join(self, user, ch):
        self.members.add((user, ch))

    def leave(self, user, ch):
        self.members.discard((user, ch))


class ModelSession:
    def __init__(self, model, user, terms):
        self.model = model
        self.user = user
        self.terms = tuple(terms)
        self.seq = model.seq
        self.channels = frozenset(c for (u, c) in model.members if u == user)
        self.cursor = (INF_POS, INF_POS)
        self.done = False

    def page(self):
        if self.done:
            return [], False
        active = self.channels & {
            c for (u, c) in self.model.members if u == self.user
        }
        cands = []
        for mid, (ch, sent_at, cseq, recalled) in self.model.messages.items():
            if cseq > self.seq or recalled or ch not in active:
                continue
            vseq, body, terms = max(
                v for v in self.model.versions[mid] if v[0] <= self.seq
            )
            if all(t in terms for t in self.terms):
                cands.append((sent_at, mid, ch, body))
        cands.sort(key=lambda x: (-x[0], -x[1]))
        out = []
        for sent_at, mid, ch, body in cands:
            if (sent_at, mid) >= self.cursor:
                continue
            out.append((mid, ch, sent_at, body))
            if len(out) == PAGE_SIZE:
                break
        if out:
            self.cursor = (out[-1][2], out[-1][0])
        has_more = any(
            cseq <= self.seq
            and not recalled
            and ch in self.channels
            and (sent_at, mid) < self.cursor
            for mid, (ch, sent_at, cseq, recalled) in self.model.messages.items()
        )
        self.done = not has_more
        return out, has_more


class TestRandomizedVsModel(unittest.TestCase):
    def test_random_interleaving(self):
        rng = random.Random(20261002)
        tmp = tempfile.TemporaryDirectory()
        vc = VaultChatSearch(os.path.join(tmp.name, "m.db"))
        model = Model()
        users = ["alice", "bob"]
        channels = ["c1", "c2", "c3", "c4"]
        vocab = [f"w{i}" for i in range(20)]
        next_id = [1]
        sessions = []  # (session_id, ModelSession)

        def rand_terms():
            return rng.sample(vocab, rng.randint(1, 3))

        def do_post():
            mid = next_id[0]
            next_id[0] += 1
            ch = rng.choice(channels)
            sent_at = rng.randint(1, 200)  # heavy timestamp collisions
            terms = rand_terms()
            body = f"body-{mid}-{'-'.join(terms)}"
            vc.post_message(mid, ch, sent_at, body, terms)
            model.post(mid, ch, sent_at, body, terms)

        def do_edit():
            if not model.messages:
                return
            mid = rng.choice(list(model.messages))
            terms = rand_terms()
            body = f"edit-{mid}-{rng.randint(0, 10**6)}"
            vc.edit_message(mid, body, terms)
            model.edit(mid, body, terms)

        def do_recall():
            if not model.messages:
                return
            mid = rng.choice(list(model.messages))
            vc.recall_message(mid)
            model.recall(mid)

        def do_membership():
            user, ch = rng.choice(users), rng.choice(channels)
            if (user, ch) in model.members and rng.random() < 0.5:
                vc.leave_channel(user, ch)
                model.leave(user, ch)
            else:
                vc.join_channel(user, ch)
                model.join(user, ch)

        def do_open():
            user = rng.choice(users)
            terms = rand_terms()
            page = vc.open_search(user, terms)
            ms = ModelSession(model, user, terms)
            want, want_more = ms.page()
            self._check_page(page, want, want_more)
            sessions.append((page.session_id, ms))

        def do_page():
            if not sessions:
                return
            idx = rng.randrange(len(sessions))
            sid, ms = sessions[idx]
            page = vc.next_page(sid)
            want, want_more = ms.page()
            self._check_page(page, want, want_more)
            if ms.done:
                sessions.pop(idx)

        # Seed some state, then interleave everything.
        for u in users:
            for ch in channels:
                if rng.random() < 0.7:
                    vc.join_channel(u, ch)
                    model.join(u, ch)
        for _ in range(150):
            do_post()
        ops = [do_post] * 8 + [do_edit] * 4 + [do_recall] * 3 + \
              [do_membership] * 4 + [do_open] * 2 + [do_page] * 12
        for _ in range(1500):
            rng.choice(ops)()
        # Drain any remaining sessions. No further ops will happen, so an
        # empty page means nothing more can become visible; stop there.
        for sid, ms in sessions:
            while not ms.done:
                page = vc.next_page(sid)
                want, want_more = ms.page()
                self._check_page(page, want, want_more)
                if not page.hits:
                    break
        vc.close()
        tmp.cleanup()

    def _check_page(self, page, want, want_more):
        got = [(h.msg_id, h.channel_id, h.sent_at, h.body) for h in page.hits]
        self.assertEqual(got, want)
        self.assertEqual(page.has_more, want_more)


if __name__ == "__main__":
    unittest.main()
