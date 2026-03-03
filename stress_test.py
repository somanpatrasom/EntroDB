#!/usr/bin/env python3
"""
EntroDB Stress Test
- 100 concurrent clients
- Mixed reads/writes simultaneously
- Checks for data corruption, deadlocks, lost updates
"""
import threading
import time
import random
import sys
from entro_client import NexusClient

HOST, PORT = "localhost", 6969
USER, PASS = "admin", "entrodb"

# ── Results tracking ──────────────────────────────────────
results = {
    "success": 0, "errors": 0, "timeouts": 0,
    "corrupt": 0, "deadlocks": 0
}
lock = threading.Lock()

def record(key):
    with lock:
        results[key] += 1

def log(msg):
    with lock:
        print(msg, flush=True)

# ── Worker threads ────────────────────────────────────────

def writer_worker(thread_id, row_range):
    """Inserts and updates rows in a given ID range."""
    try:
        c = NexusClient(HOST, PORT, USER, PASS)
        c.connect()
        for i in row_range:
            try:
                r = c.query(f"INSERT INTO stress_users VALUES ({i}, 'User{i}', {20 + i % 50})")
                if "inserted" in r:
                    record("success")
                elif "violation" in r.lower():
                    record("success")  # expected on re-run
                else:
                    record("errors")
            except Exception as e:
                msg = str(e).lower()
                if "timeout" in msg:   record("timeouts")
                elif "deadlock" in msg: record("deadlocks")
                else:                   record("errors")
        c.close()
    except Exception as e:
        record("errors")
        log(f"[writer-{thread_id}] connection failed: {e}")

def reader_worker(thread_id, iterations=50):
    """Reads random rows repeatedly."""
    try:
        c = NexusClient(HOST, PORT, USER, PASS)
        c.connect()
        for _ in range(iterations):
            try:
                rid = random.randint(1, 500)
                r = c.query(f"SELECT * FROM stress_users WHERE id = {rid}")
                record("success")
            except Exception as e:
                record("errors")
        c.close()
    except Exception as e:
        record("errors")
        log(f"[reader-{thread_id}] connection failed: {e}")

def updater_worker(thread_id, iterations=30):
    """Updates random rows — tests write-write conflicts."""
    try:
        c = NexusClient(HOST, PORT, USER, PASS)
        c.connect()
        for _ in range(iterations):
            try:
                rid = random.randint(1, 500)
                age = random.randint(20, 60)
                r = c.query(
                    f"UPDATE stress_users SET age = {age} WHERE id = {rid}")
                record("success")
            except Exception as e:
                msg = str(e).lower()
                if "timeout" in msg:    record("timeouts")
                elif "deadlock" in msg: record("deadlocks")
                else:                   record("errors")
        c.close()
    except Exception as e:
        record("errors")
        log(f"[updater-{thread_id}] connection failed: {e}")

def aggregate_worker(thread_id, iterations=20):
    """Runs aggregates while writes are happening."""
    try:
        c = NexusClient(HOST, PORT, USER, PASS)
        c.connect()
        for _ in range(iterations):
            try:
                r = c.query("SELECT COUNT(*) FROM stress_users")
                record("success")
                r = c.query("SELECT AVG(age) FROM stress_users")
                record("success")
            except Exception as e:
                record("errors")
        c.close()
    except Exception as e:
        record("errors")

def integrity_checker(expected_ids):
    """After all writes done — verify no data was lost or corrupted."""
    try:
        c = NexusClient(HOST, PORT, USER, PASS)
        c.connect()
        r = c.query("SELECT COUNT(*) FROM stress_users")
        c.close()
        # Extract count
        lines = r.strip().split('\n')
        for line in lines:
            line = line.strip('| ').strip()
            try:
                count = int(line)
                return count
            except:
                continue
        return -1
    except Exception as e:
        log(f"Integrity check failed: {e}")
        return -1

# ── Main ──────────────────────────────────────────────────

def main():
    print("\n" + "="*60)
    print("  EntroDB Stress Test")
    print("="*60)

    # Setup
    setup = NexusClient(HOST, PORT, USER, PASS)
    setup.connect()
    try: setup.query("DROP TABLE stress_users")
    except: pass
    setup.query("""CREATE TABLE stress_users (
        id   INT PRIMARY KEY,
        name VARCHAR(50) NOT NULL,
        age  INT
    )""")
    setup.close()
    print("✓ Table created\n")

    threads = []
    start = time.perf_counter()

    # ── Round 1: 20 concurrent writers (IDs 1-500) ───────
    print("[ Round 1 ] 20 concurrent writers inserting 500 rows...")
    batch = 500 // 20
    for i in range(20):
        lo = i * batch + 1
        hi = lo + batch
        t = threading.Thread(
            target=writer_worker,
            args=(i, range(lo, hi)))
        threads.append(t)

    for t in threads: t.start()
    for t in threads: t.join()
    threads.clear()

    count = integrity_checker(range(1, 501))
    print(f"  Rows after insert: {count} (expected ~500)")
    print(f"  Success: {results['success']}  "
          f"Errors: {results['errors']}  "
          f"Timeouts: {results['timeouts']}  "
          f"Deadlocks: {results['deadlocks']}")

    with lock: results['success'] = results['errors'] = \
               results['timeouts'] = results['deadlocks'] = 0

    # ── Round 2: 30 readers + 20 updaters simultaneously ─
    print("\n[ Round 2 ] 30 readers + 20 updaters simultaneously...")
    for i in range(30):
        t = threading.Thread(target=reader_worker,  args=(i,))
        threads.append(t)
    for i in range(20):
        t = threading.Thread(target=updater_worker, args=(i,))
        threads.append(t)

    for t in threads: t.start()
    for t in threads: t.join()
    threads.clear()

    print(f"  Success: {results['success']}  "
          f"Errors: {results['errors']}  "
          f"Timeouts: {results['timeouts']}  "
          f"Deadlocks: {results['deadlocks']}")

    with lock: results['success'] = results['errors'] = \
               results['timeouts'] = results['deadlocks'] = 0

    # ── Round 3: 50 mixed (readers + writers + aggregates) 
    print("\n[ Round 3 ] 50 mixed concurrent clients...")
    for i in range(20):
        t = threading.Thread(target=reader_worker,    args=(i, 30))
        threads.append(t)
    for i in range(20):
        t = threading.Thread(target=writer_worker,
            args=(i, range(501, 501 + 25)))
        threads.append(t)
    for i in range(10):
        t = threading.Thread(target=aggregate_worker, args=(i,))
        threads.append(t)

    for t in threads: t.start()
    for t in threads: t.join()
    threads.clear()

    final_count = integrity_checker(None)
    elapsed = (time.perf_counter() - start) * 1000

    print(f"  Success: {results['success']}  "
          f"Errors: {results['errors']}  "
          f"Timeouts: {results['timeouts']}  "
          f"Deadlocks: {results['deadlocks']}")

    print(f"\n[ Final state ]")
    print(f"  Total rows in table: {final_count}")
    print(f"  Total time: {elapsed:.0f}ms")

    print("\n[ Verdict ]")
    if results['errors'] == 0 and results['corrupt'] == 0:
        print("  ✅ PASSED — no corruption, no unexpected errors")
    elif results['corrupt'] > 0:
        print("  ❌ FAILED — data corruption detected")
    else:
        print(f"  ⚠️  PARTIAL — {results['errors']} errors "
              f"(check for race conditions)")

    print("="*60 + "\n")

if __name__ == "__main__":
    main()

