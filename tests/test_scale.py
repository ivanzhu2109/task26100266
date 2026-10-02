"""Scale test: 300k messages, 200 active sessions, >=100k hits each.

Reports first-page and paging latency (p95) plus the resident-memory and
on-disk delta attributable to the 200 sessions. Run directly:

    python3 tests/test_scale.py
"""

import os
import random
import resource
import statistics
import sys
import tempfile
import time
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from vaultchat import VaultChatStore

N_MESSAGES = int(os.environ.get("SCALE_MESSAGES", "300_000"))
N_SESSIONS = int(os.environ.get("SCALE_SESSIONS", "200"))
N_CHANNELS = 20
PAGE_LIMIT_MS = 300
MEMORY_LIMIT_MIB = 32


def current_rss_bytes():
    rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    # ru_maxrss is bytes on macOS, kilobytes on Linux.
    return rss if sys.platform == "darwin" else rss * 1024


def p95(values):
    ordered = sorted(values)
    idx = min(len(ordered) - 1, int(0.95 * len(ordered)))
    return ordered[idx]


def make_body(rng, msg_no):
    words = ["lorem", "ipsum", "dolor", "sit", "amet", "consectetur",
             "adipiscing", "elit", "sed", "do", "eiusmod", "tempor"]
    text = " ".join(rng.choice(words) for _ in range(28))
    return f"{text} message {msg_no}"  # ~200 chars


# Shared vocabulary: each shared term appears in a message with
# probability SHARED_P, so any pair of shared terms co-occurs in roughly
# SHARED_P**2 * N_MESSAGES messages (~0.49 * 300k = 147k >= 100k).
# 30 shared terms give C(30,2) = 435 distinct two-term queries.
SHARED = [f"topic{i:02d}" for i in range(30)]
SHARED_P = 0.7
FILLERS = [f"word{i:04d}" for i in range(500)]


class ScaleTest(unittest.TestCase):
    def test_scale(self):
        tmpdir = tempfile.mkdtemp(prefix="vaultchat-scale-")
        db_path = os.path.join(tmpdir, "scale.db")
        store = VaultChatStore(db_path)
        rng = random.Random(42)

        n_users = 50
        for u in range(n_users):
            for ch in range(N_CHANNELS):
                store.add_member(u, ch)

        t0 = time.perf_counter()

        def gen():
            for i in range(N_MESSAGES):
                terms = [t for t in SHARED if rng.random() < SHARED_P]
                terms.extend(rng.sample(FILLERS, 3))
                yield (1_000_000 + i, i % N_CHANNELS,
                       2_000_000_000 - rng.randrange(1_000_000_000),
                       make_body(rng, i), terms)

        store.bulk_publish(gen())
        load_s = time.perf_counter() - t0

        rss_baseline = current_rss_bytes()
        disk_baseline = os.path.getsize(db_path)

        # ---- open N_SESSIONS search sessions (first page each) ----
        first_page_ms = []
        sessions = []
        pairs = [(a, b) for i, a in enumerate(SHARED) for b in SHARED[i + 1:]]
        for k in range(N_SESSIONS):
            user = k % n_users
            terms = list(pairs[k % len(pairs)])
            t0 = time.perf_counter()
            page = store.search(user, terms)
            first_page_ms.append((time.perf_counter() - t0) * 1000)
            self.assertEqual(len(page.items), 50)
            sessions.append(page.session_id)

        # Each session must have at least 100k initial hits.
        for sid in sessions[:5]:
            bitmap = store._sessions[sid].bitmap
            hits = sum(bin(b).count("1") for b in bitmap)
            self.assertGreaterEqual(hits, 100_000)
            print(f"session {sid}: {hits} initial hits")

        rss_after_sessions = current_rss_bytes()

        # ---- interleave writes with paging ----
        paging_ms = []
        live = list(range(1_000_000, 1_000_000 + N_MESSAGES))
        for round_no in range(3):
            for sid in sessions:
                t0 = time.perf_counter()
                page = store.next_page(sid)
                paging_ms.append((time.perf_counter() - t0) * 1000)
                self.assertLessEqual(len(page.items), 50)
            # background activity: publishes, edits, recalls
            base = 5_000_000 + round_no * 10_000
            store.bulk_publish(
                (base + i, i % N_CHANNELS, 1_500_000_000 - i,
                 make_body(rng, i),
                 [t for t in SHARED if rng.random() < SHARED_P])
                for i in range(200))
            for i in range(100):
                store.edit(live[rng.randrange(len(live))],
                           make_body(rng, i), ["edited"] + SHARED[:20])
            for i in range(100):
                store.recall(live[rng.randrange(len(live))])

        rss_final = current_rss_bytes()
        disk_final = os.path.getsize(db_path)
        wal = db_path + "-wal"
        if os.path.exists(wal):
            disk_final += os.path.getsize(wal)
        store.close()

        session_mem_mib = (rss_after_sessions - rss_baseline) / 2**20
        final_mem_mib = (rss_final - rss_baseline) / 2**20
        disk_growth_mib = (disk_final - disk_baseline) / 2**20

        print("\n===== VaultChat scale report =====")
        print(f"messages:            {N_MESSAGES}")
        print(f"sessions:            {N_SESSIONS}")
        print(f"data load:           {load_s:.1f}s")
        print(f"baseline RSS:        {rss_baseline / 2**20:.1f} MiB")
        print(f"baseline db file:    {disk_baseline / 2**20:.1f} MiB")
        print(f"first-page p50/p95:  "
              f"{statistics.median(first_page_ms):.1f} / "
              f"{p95(first_page_ms):.1f} ms")
        print(f"paging p50/p95:      "
              f"{statistics.median(paging_ms):.1f} / "
              f"{p95(paging_ms):.1f} ms")
        print(f"session RSS delta:   {session_mem_mib:.1f} MiB "
              f"(limit {MEMORY_LIMIT_MIB} MiB)")
        print(f"final RSS delta:     {final_mem_mib:.1f} MiB")
        print(f"disk growth:         {disk_growth_mib:.1f} MiB")
        print("==================================")

        self.assertLessEqual(p95(first_page_ms), PAGE_LIMIT_MS,
                             "first-page p95 exceeds limit")
        self.assertLessEqual(p95(paging_ms), PAGE_LIMIT_MS,
                             "paging p95 exceeds limit")
        self.assertLessEqual(session_mem_mib, MEMORY_LIMIT_MIB,
                             "session memory delta exceeds limit")


if __name__ == "__main__":
    unittest.main(argv=[sys.argv[0]], verbosity=2)
