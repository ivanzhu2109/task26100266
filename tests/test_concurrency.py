"""Threading stress: writes, membership changes and paging interleaved."""

import random
import sys
import threading
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from vaultchat import VaultChatStore


class ConcurrencyTest(unittest.TestCase):
    def test_interleaved_load(self):
        store = VaultChatStore()
        n_channels = 5
        n_users = 4
        for u in range(n_users):
            for ch in range(n_channels):
                store.add_member(u, ch)
        for i in range(2000):
            store.publish(1_000_000 + i, i % n_channels, 10_000 - i,
                          f"body {i} alpha", ["alpha", f"g{i % 4}"])

        errors = []
        stop = threading.Event()
        msg_counter = [2_000_000]
        counter_lock = threading.Lock()

        def writer():
            rng = random.Random(1)
            try:
                while not stop.is_set():
                    r = rng.random()
                    if r < 0.5:
                        with counter_lock:
                            mid = msg_counter[0]
                            msg_counter[0] += 1
                        store.publish(mid, rng.randrange(n_channels),
                                      rng.randint(0, 10_000),
                                      f"late {mid} alpha", ["alpha"])
                    elif r < 0.75:
                        store.edit(1_000_000 + rng.randrange(2000),
                                   "edited alpha", ["alpha", "edited"])
                    elif r < 0.9:
                        store.recall(1_000_000 + rng.randrange(2000))
                    else:
                        u, ch = rng.randrange(n_users), rng.randrange(n_channels)
                        if rng.random() < 0.5:
                            store.add_member(u, ch)
                        else:
                            store.remove_member(u, ch)
            except Exception as exc:  # noqa: BLE001
                errors.append(exc)

        def pager(user):
            rng = random.Random(user)
            try:
                while not stop.is_set():
                    page = store.search(user, ["alpha"])
                    seen = set()
                    prev_key = None
                    for _ in range(20):
                        for m in page.items:
                            key = (m.sent_at, m.msg_id)
                            if prev_key is not None:
                                assert key < prev_key, "order violated"
                            prev_key = key
                            assert m.msg_id not in seen, "dup in session"
                            seen.add(m.msg_id)
                        if not page.has_more:
                            break
                        page = store.next_page(page.session_id)
            except Exception as exc:  # noqa: BLE001
                errors.append(exc)

        threads = ([threading.Thread(target=pager, args=(u,))
                    for u in range(n_users)]
                   + [threading.Thread(target=writer) for _ in range(2)])
        for t in threads:
            t.start()
        import time
        time.sleep(3)
        stop.set()
        for t in threads:
            t.join(timeout=30)
        store.close()
        self.assertEqual(errors, [])


if __name__ == "__main__":
    unittest.main()
