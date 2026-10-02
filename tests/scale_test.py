"""Scale / performance test for the VaultChat search library.

Default scale (per requirements): 300k messages, ~200-char bodies, 200
active search sessions each with >= 100k initial hits. Reports first-page
and paging latency (p50/p95/max) and the resident-memory + on-disk delta
attributable to the 200 sessions (must be <= 32 MiB).

Run:  python3 tests/scale_test.py           # full scale
      VC_SCALE_QUICK=1 python3 tests/scale_test.py   # smoke scale
"""
from __future__ import annotations

import os
import random
import resource
import statistics
import sys
import tempfile
import threading
import time
import tracemalloc

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from vaultchat import VaultChatSearch

QUICK = os.environ.get("VC_SCALE_QUICK") == "1"
N_MESSAGES = int(os.environ.get("VC_SCALE_MESSAGES", 30_000 if QUICK else 300_000))
N_SESSIONS = int(os.environ.get("VC_SCALE_SESSIONS", 20 if QUICK else 200))
PAGES_PER_SESSION = int(os.environ.get("VC_SCALE_PAGES", 3 if QUICK else 5))
MIN_INITIAL_HITS = 100_000
N_CHANNELS = 20
N_COMMON = 20
COMMON_PER_MSG = 14
USER = "scale-user"
MIB = 1024 * 1024


def rss_bytes() -> int:
    # Peak resident set size (bytes on Linux, bytes on macOS >= 10.6?).
    return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss


def db_size_bytes(path: str) -> int:
    total = 0
    for suffix in ("", "-wal", "-shm"):
        p = path + suffix
        if os.path.exists(p):
            total += os.path.getsize(p)
    return total


def percentile(values, pct):
    values = sorted(values)
    k = max(0, min(len(values) - 1, int(round(pct / 100 * (len(values) - 1)))))
    return values[k]


def gen_common_terms(i: int) -> list[str]:
    # Deterministic spread: message i holds common terms (i + 3k) % 16,
    # k = 0..9. Any single common term appears in 10/16 of messages; any
    # pair with difference in R-R co-occurs in ~6/16 of messages.
    return [f"ct{(i + 3 * k) % N_COMMON}" for k in range(COMMON_PER_MSG)]


def main() -> int:
    tmp = tempfile.TemporaryDirectory()
    db_path = os.path.join(tmp.name, "scale.db")
    vc = VaultChatSearch(db_path)
    for c in range(N_CHANNELS):
        vc.join_channel(USER, f"ch{c}")

    # -- load messages ----------------------------------------------------
    print(f"[load] inserting {N_MESSAGES:,} messages ...", flush=True)
    t0 = time.perf_counter()
    rng = random.Random(42)
    batch = []
    rare_pool = [f"rare{j}" for j in range(1000)]
    for i in range(N_MESSAGES):
        terms = gen_common_terms(i)
        terms += [rare_pool[i % 1000], rare_pool[(i * 7) % 1000]]
        body = (f"message {i} " + " ".join(terms) + " " + "filler word " * 14)[:220]
        batch.append((i + 1, f"ch{i % N_CHANNELS}", i // 3, body, terms))
        if len(batch) == 20_000:
            vc.post_messages(batch)
            batch = []
    if batch:
        vc.post_messages(batch)
    print(f"[load] done in {time.perf_counter() - t0:.1f}s", flush=True)

    # -- pick queries with >= MIN_INITIAL_HITS hits -------------------------
    # Message i holds common terms (i mod 20 + 3k) mod 20, k = 0..13, so its
    # term set depends only on i mod 20. A query's hit count is therefore
    # exactly: (# residue classes whose set covers the query) / 20 * N.
    print("[queries] selecting heavy queries ...", flush=True)
    class_sets = [
        frozenset((r + 3 * k) % N_COMMON for k in range(COMMON_PER_MSG))
        for r in range(N_COMMON)
    ]

    def hit_count(combo) -> float:
        wanted = set(combo)
        covered = sum(1 for cs in class_sets if wanted <= cs)
        return covered / N_COMMON * N_MESSAGES

    queries: list[list[str]] = []
    for width in (1, 2, 3):
        for combo in __import__("itertools").combinations(range(N_COMMON), width):
            if hit_count(combo) >= MIN_INITIAL_HITS:
                queries.append([f"ct{c}" for c in combo])
    print(f"[queries] {len(queries)} heavy queries available", flush=True)
    assert len(queries) >= N_SESSIONS, "not enough heavy queries"
    rng.shuffle(queries)
    queries = queries[:N_SESSIONS]

    vc.checkpoint()
    # -- memory baseline ------------------------------------------------------
    rss_before = rss_bytes()
    disk_before = db_size_bytes(db_path)
    tracemalloc.start()
    trace_before = tracemalloc.take_snapshot()

    # -- open sessions (first page) ------------------------------------------
    print(f"[sessions] opening {N_SESSIONS} sessions ...", flush=True)
    open_lat = []
    session_ids = []
    t0 = time.perf_counter()
    for q in queries:
        s = time.perf_counter()
        page = vc.open_search(USER, q)
        open_lat.append((time.perf_counter() - s) * 1000)
        assert len(page.hits) == 50
        session_ids.append(page.session_id)
    print(f"[sessions] opened in {time.perf_counter() - t0:.1f}s", flush=True)

    trace_after_open = tracemalloc.take_snapshot()
    rss_after_open = rss_bytes()
    disk_after_open = db_size_bytes(db_path)

    # -- background writer while paging ---------------------------------------
    stop = threading.Event()

    def writer():
        w_rng = random.Random(9)
        n = 0
        while not stop.is_set():
            mid = w_rng.randint(1, N_MESSAGES)
            action = n % 3
            if action == 0:
                vc.recall_message(mid)
            elif action == 1:
                vc.edit_message(mid, f"edited {mid}", ["ct0", "edited"])
            else:
                vc.post_message(
                    N_MESSAGES + 1 + n, f"ch{n % N_CHANNELS}", 0,
                    "late import", ["ct0"],
                )
            n += 1

    wt = threading.Thread(target=writer)
    wt.start()

    # -- page through all sessions ---------------------------------------------
    print("[paging] running ...", flush=True)
    page_lat = []
    for sid in session_ids:
        for _ in range(PAGES_PER_SESSION):
            s = time.perf_counter()
            page = vc.next_page(sid)
            page_lat.append((time.perf_counter() - s) * 1000)
    stop.set()
    wt.join()
    vc.checkpoint()

    trace_after_paging = tracemalloc.take_snapshot()
    rss_after = rss_bytes()
    disk_after = db_size_bytes(db_path)
    tracemalloc.stop()

    # -- report -------------------------------------------------------------------
    def fmt(lat):
        return (f"p50={percentile(lat, 50):7.2f}ms "
                f"p95={percentile(lat, 95):7.2f}ms "
                f"max={max(lat):7.2f}ms")

    py_mem_open = sum(
        st.size_diff for st in trace_after_open.compare_to(trace_before, "filename")
    )
    py_mem_total = sum(
        st.size_diff for st in trace_after_paging.compare_to(trace_before, "filename")
    )
    print("\n===== scale test report =====")
    print(f"messages={N_MESSAGES:,} sessions={N_SESSIONS} "
          f"pages/session={PAGES_PER_SESSION}")
    print(f"first page : {fmt(open_lat)}")
    print(f"paging     : {fmt(page_lat)}  (n={len(page_lat)})")
    print(f"session python-mem delta (open)   : {py_mem_open / MIB:.2f} MiB")
    print(f"session python-mem delta (total)  : {py_mem_total / MIB:.2f} MiB")
    print(f"RSS peak delta (open)             : "
          f"{(rss_after_open - rss_before) / MIB:.2f} MiB")
    print(f"RSS peak delta (total)            : "
          f"{(rss_after - rss_before) / MIB:.2f} MiB")
    print(f"on-disk delta (sessions)          : "
          f"{(disk_after_open - disk_before) / MIB:.2f} MiB")
    print(f"db size (data, not session state) : {disk_after / MIB:.1f} MiB")

    ok = True
    p95_open = percentile(open_lat, 95)
    p95_page = percentile(page_lat, 95)
    if p95_open > 300 or p95_page > 300:
        print("FAIL: p95 latency above 300ms")
        ok = False
    session_delta = max(py_mem_total, rss_after_open - rss_before) + \
        max(0, disk_after_open - disk_before)
    if session_delta > 32 * MIB:
        print(f"FAIL: session state delta {session_delta / MIB:.1f} MiB > 32 MiB")
        ok = False
    print("RESULT:", "PASS" if ok else "FAIL")

    vc.close()
    tmp.cleanup()
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
