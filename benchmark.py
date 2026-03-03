#!/usr/bin/env python3
"""
EntroDB Benchmark
Measures: INSERT throughput, index lookup vs full scan, JOIN performance
"""
import time
import sys
sys.path.insert(0, '.')
from entro_client import NexusClient

HOST, PORT = "localhost", 6969
USER, PASS = "admin", "entrodb"

def connect():
    c = NexusClient(HOST, PORT, USER, PASS)
    c.connect()
    return c

def run(c, sql):
    return c.query(sql)

def bench(label, fn, iterations=1):
    start = time.perf_counter()
    for _ in range(iterations):
        fn()
    elapsed = (time.perf_counter() - start) * 1000
    per_op  = elapsed / iterations
    print(f"  {label:<45} {per_op:>8.2f} ms/op   ({iterations} ops, {elapsed:.0f}ms total)")
    return per_op

print("\n" + "="*70)
print("  EntroDB Benchmark")
print("="*70)

c = connect()

# ── Setup ─────────────────────────────────────────────────────────────
run(c, "DROP TABLE benchmark_orders")  
run(c, "DROP TABLE benchmark_users")

run(c, """CREATE TABLE benchmark_users (
    id INT PRIMARY KEY,
    name VARCHAR(50) NOT NULL,
    age INT
)""")

run(c, """CREATE TABLE benchmark_orders (
    id INT PRIMARY KEY,
    user_id INT NOT NULL,
    amount INT
)""")

# ── INSERT benchmark ──────────────────────────────────────────────────
print("\n[ INSERT ]")
N = 1000

start = time.perf_counter()
for i in range(1, N + 1):
    run(c, f"INSERT INTO benchmark_users VALUES ({i}, 'User{i}', {20 + i % 50})")
elapsed = (time.perf_counter() - start) * 1000
print(f"  {'1000 x INSERT INTO benchmark_users':<45} {elapsed/N:>8.2f} ms/op   ({N} ops, {elapsed:.0f}ms total)")

start = time.perf_counter()
for i in range(1, N + 1):
    run(c, f"INSERT INTO benchmark_orders VALUES ({i}, {(i % 100) + 1}, {10 + i % 200})")
elapsed = (time.perf_counter() - start) * 1000
print(f"  {'1000 x INSERT INTO benchmark_orders':<45} {elapsed/N:>8.2f} ms/op   ({N} ops, {elapsed:.0f}ms total)")

# ── SELECT: index vs full scan ────────────────────────────────────────
print("\n[ SELECT — Primary Key Index Lookup (O log n) ]")
bench("SELECT WHERE id=500 (PK index, 1000 rows)",
      lambda: run(c, "SELECT * FROM benchmark_users WHERE id = 500"), 50)
bench("SELECT WHERE id=1 (PK index, 1000 rows)",
      lambda: run(c, "SELECT * FROM benchmark_users WHERE id = 1"), 50)
bench("SELECT WHERE id=999 (PK index, 1000 rows)",
      lambda: run(c, "SELECT * FROM benchmark_users WHERE id = 999"), 50)

print("\n[ SELECT — Full Scan (no index on age) ]")
bench("SELECT WHERE age=35 (full scan, 1000 rows)",
      lambda: run(c, "SELECT * FROM benchmark_users WHERE age = 35"), 20)
bench("SELECT WHERE age>40 (full scan, 1000 rows)",
      lambda: run(c, "SELECT * FROM benchmark_users WHERE age > 40"), 20)

# ── Aggregate benchmark ───────────────────────────────────────────────
print("\n[ AGGREGATES ]")
bench("COUNT(*) over 1000 rows",
      lambda: run(c, "SELECT COUNT(*) FROM benchmark_users"), 20)
bench("AVG(age) over 1000 rows",
      lambda: run(c, "SELECT AVG(age) FROM benchmark_users"), 20)
bench("SUM(amount) GROUP BY user_id (1000 rows)",
      lambda: run(c, "SELECT user_id, SUM(amount) FROM benchmark_orders GROUP BY user_id"), 10)

# ── JOIN benchmark ────────────────────────────────────────────────────
print("\n[ JOIN ]")
bench("INNER JOIN users x orders (index on users.id)",
      lambda: run(c, """SELECT benchmark_users.name, benchmark_orders.amount
                        FROM benchmark_users
                        INNER JOIN benchmark_orders
                        ON benchmark_users.id = benchmark_orders.user_id
                        WHERE benchmark_users.id = 5"""), 20)

# ── CREATE INDEX then re-benchmark ────────────────────────────────────
print("\n[ CREATE INDEX on age ]")
run(c, "CREATE INDEX idx_bench_age ON benchmark_users (age)")
bench("SELECT WHERE age=35 (index, 1000 rows)",
      lambda: run(c, "SELECT * FROM benchmark_users WHERE age = 35"), 20)

print("\n[ SUMMARY ]")
print(f"  Dataset: {N} users, {N} orders")
print(f"  Index type: B+ Tree (order=128)")
print(f"  Buffer pool: 256 frames (1MB)")
print("="*70 + "\n")

c.close()
