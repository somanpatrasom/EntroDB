# EntroDB

A relational database management system built from scratch in Java.
EntroDB implements core database internals — storage engine, buffer pool, B+ Tree indexes, WAL-based crash recovery, SQL parser, and a TCP server — without using any external database libraries.

---

## Architecture

```
┌─────────────────────────────────────────────────────────┐
│                    TCP Server :6969                      │
│            Auth · Sessions · Wire Protocol               │
├─────────────────────────────────────────────────────────┤
│                    SQL Parser                            │
│         Lexer → Tokens → AST → Statement                │
├─────────────────────────────────────────────────────────┤
│                  Query Executor                          │
│   SELECT · INSERT · UPDATE · DELETE · JOIN · Aggregates  │
├──────────────────┬──────────────────────────────────────┤
│  Constraint      │         Index Manager                 │
│  Checker         │     B+ Tree (order=128)               │
│  PK · NN · UQ    │     O(log n) lookups                  │
├──────────────────┴──────────────────────────────────────┤
│               Transaction Manager                        │
│        2-Phase Locking · ACID · Lock Manager             │
├─────────────────────────────────────────────────────────┤
│                   WAL Manager                            │
│      Write-Ahead Log · REDO/UNDO · pageLSN Recovery      │
├─────────────────────────────────────────────────────────┤
│                 Buffer Pool Manager                      │
│           LRU Eviction · 256 Frames · 1MB                │
├─────────────────────────────────────────────────────────┤
│                   Disk Manager                           │
│        Slotted Pages · 4KB · NIO FileChannel             │
│              data/*.ndb · data/*.idx                     │
└─────────────────────────────────────────────────────────┘
```

---

## Features

| Layer | Feature |
|---|---|
| **Storage** | Slotted page format (4KB pages), NIO FileChannel I/O, one `.ndb` file per table |
| **Buffer Pool** | LRU replacement, 256 frames (1MB), write-through caching |
| **WAL** | Write-Ahead Logging, fsync on every write, pageLSN-based REDO/UNDO |
| **Recovery** | Crash recovery on startup — committed txns redone, incomplete txns undone |
| **Transactions** | ACID guarantees, 2-Phase Locking, row-level + table-level locks |
| **Indexes** | B+ Tree (order=128), auto-created on PRIMARY KEY, `CREATE INDEX` on any column |
| **Constraints** | `PRIMARY KEY`, `NOT NULL`, `UNIQUE`, `CHECK` — enforced before every write |
| **SQL** | `SELECT`, `INSERT`, `UPDATE`, `DELETE`, `CREATE TABLE`, `DROP TABLE` |
| **JOIN** | `INNER JOIN`, `LEFT JOIN`, index-accelerated probe on right table |
| **Aggregates** | `COUNT`, `SUM`, `AVG`, `MIN`, `MAX`, `GROUP BY` |
| **Auth** | SHA-256 password hashing, per-user credentials in `data/users.auth` |
| **Server** | TCP server on port 6969, 50 concurrent clients, custom wire protocol |
| **Dashboard** | Live admin web UI on port 6970 — query stats, buffer pool, table info |
| **Client** | Python client library + CLI |

---

## Benchmark

Measured on a local machine with 1000-row dataset over TCP loopback.

```
[ INSERT ]
  1000 x INSERT INTO benchmark_users        12.01 ms/op
  1000 x INSERT INTO benchmark_orders       10.90 ms/op

[ SELECT — Primary Key Index Lookup (B+ Tree, O log n) ]
  WHERE id = 500  (1000 rows)                7.88 ms/op
  WHERE id = 1    (1000 rows)                6.56 ms/op
  WHERE id = 999  (1000 rows)                6.38 ms/op

[ SELECT — Full Scan (no index) ]
  WHERE age = 35  (1000 rows)                6.64 ms/op
  WHERE age > 40  (1000 rows)                7.55 ms/op

[ AGGREGATES ]
  COUNT(*) over 1000 rows                    7.44 ms/op
  AVG(age) over 1000 rows                    8.40 ms/op
  SUM(amount) GROUP BY user_id               9.38 ms/op

[ JOIN ]
  INNER JOIN with index probe                8.04 ms/op

[ After CREATE INDEX on age ]
  WHERE age = 35  (1000 rows, indexed)       7.30 ms/op

Dataset: 1000 rows · B+ Tree order=128 · Buffer pool 256 frames (1MB)
All measurements include round-trip TCP latency.
```

---

## Quick Start

### Requirements
- Java 17+
- Maven 3.6+

### Build
```bash
git clone https://github.com/yourusername/EntroDB
cd EntroDB
mvn clean package -q
```

### Run (server mode)
```bash
rm -rf data/
java -jar target/EntroDB-1.0.0.jar --server
```

Server starts on port **6969**. Admin dashboard at **http://localhost:6970**.

Default credentials: `admin` / `entrodb`

### Connect (Python client)
```bash
python3 entro_client.py localhost 6969 admin entrodb
```

### Connect (custom port)
```bash
java -jar target/EntroDB-1.0.0.jar --server 7000
```

---

## SQL Reference

### DDL
```sql
-- Create table
CREATE TABLE users (
    id   INT          PRIMARY KEY,
    name VARCHAR(50)  NOT NULL,
    age  INT
);

-- Create index on any column
CREATE INDEX idx_age ON users (age);

-- Unique index
CREATE UNIQUE INDEX idx_name ON users (name);

-- Show indexes
SHOW INDEXES ON users;

-- Drop
DROP INDEX idx_age ON users;
DROP TABLE users;
SHOW TABLES;
```

### DML
```sql
-- Insert
INSERT INTO users VALUES (1, 'Alice', 30);
INSERT INTO users (id, name) VALUES (2, 'Bob');

-- Select
SELECT * FROM users;
SELECT name, age FROM users WHERE age > 25;
SELECT * FROM users WHERE id = 1;          -- uses B+ Tree index

-- Update
UPDATE users SET age = 31 WHERE id = 1;

-- Delete
DELETE FROM users WHERE id = 2;
```

### Aggregates
```sql
SELECT COUNT(*) FROM users;
SELECT SUM(age), AVG(age), MIN(age), MAX(age) FROM users;
SELECT age, COUNT(*) FROM users GROUP BY age;
```

### JOIN
```sql
-- INNER JOIN (index-accelerated on right table PK)
SELECT users.name, orders.amount
FROM users INNER JOIN orders ON users.id = orders.user_id;

-- LEFT JOIN (NULL for unmatched rows)
SELECT users.name, orders.amount
FROM users LEFT JOIN orders ON users.id = orders.user_id;

-- JOIN with WHERE
SELECT users.name, orders.amount
FROM users JOIN orders ON users.id = orders.user_id
WHERE orders.amount > 50;
```

### Constraints
```sql
-- PRIMARY KEY — enforces uniqueness + NOT NULL
-- NOT NULL    — rejects null on insert/update
-- UNIQUE      — rejects duplicate values

-- Violations return clear error messages:
-- ERROR: PRIMARY KEY violation on table 'users': duplicate value '1' in column 'id'
-- ERROR: NOT NULL violation on table 'users': column 'name' cannot be null
-- ERROR: UNIQUE violation on table 'users': duplicate value 'Alice' in column 'name'
```

---

## Python Client

```python
from entro_client import NexusClient

db = NexusClient("localhost", 6969, "admin", "entrodb")
db.connect()

result = db.query("SELECT * FROM users WHERE id = 1")
print(result)

db.close()
```

---

## Project Structure

```
src/main/java/com/entrodb/
├── EntroDB.java              # Entry point
├── storage/
│   ├── Page.java             # Slotted page (4KB), pageLSN header
│   ├── DiskManager.java      # File I/O, one .ndb per table
│   └── PageId.java           # (tableId, pageNum) identifier
├── buffer/
│   ├── BufferPoolManager.java # LRU cache, pin/unpin
│   └── LRUReplacer.java       # LRU eviction policy
├── catalog/
│   ├── CatalogManager.java    # Persistent schema store
│   ├── TableSchema.java       # Column definitions + constraints
│   ├── Column.java            # Column metadata
│   ├── Constraint.java        # PK, NOT NULL, UNIQUE, CHECK
│   ├── ConstraintChecker.java # Pre-write validation
│   └── ConstraintViolationException.java
├── transaction/
│   ├── TransactionManager.java # BEGIN/COMMIT/ABORT, recovery
│   ├── WALManager.java         # WAL file I/O
│   ├── WALRecord.java          # Log record format
│   ├── Transaction.java        # Transaction state
│   └── LockManager.java        # 2PL, row + table locks
├── index/
│   ├── BPlusTree.java          # B+ Tree (order=128)
│   ├── BPlusTreeNode.java      # Internal + leaf nodes
│   ├── IndexManager.java       # Named index registry
│   └── RID.java                # Record ID (pageNum:slot)
├── sql/
│   ├── Lexer.java              # Tokenizer
│   ├── Parser.java             # Recursive descent parser
│   ├── Token.java              # Token types
│   └── ast/                   # AST node classes
├── executor/
│   ├── QueryExecutor.java      # Statement execution engine
│   └── ResultSet.java          # Query results + pretty-print
├── server/
│   ├── EntroServer.java        # TCP server, thread pool
│   ├── ClientHandler.java      # Per-client thread
│   ├── EntroProtocol.java      # Wire protocol
│   ├── SessionContext.java     # Session state
│   ├── AuthManager.java        # SHA-256 auth
│   ├── DashboardServer.java    # HTTP admin dashboard
│   └── StatsCollector.java     # Live stats aggregation
├── cli/
│   └── REPL.java               # Interactive CLI mode
└── util/
    └── ByteUtils.java          # Row serialization/deserialization
```

---

## Data Files

```
data/
├── catalog.ncat       # Table schemas and constraints
├── users.auth         # SHA-256 hashed credentials
├── indexes.reg        # Named index registry
├── nexus.wal          # Write-Ahead Log
├── tablename.ndb      # Table data pages
└── tablename_col.idx  # B+ Tree index files
```

---

## Implementation Notes

**Why slotted pages?** Variable-length records (VARCHAR) need a directory to track where each record starts. The slot array grows from the front, records grow from the back, and free space sits in the middle.

**Why pageLSN?** Write-through caching means pages may already reflect committed changes before a crash. Storing the last applied LSN in each page header lets recovery skip redundant REDOs — preventing duplicate rows on restart.

**Why 2-Phase Locking?** 2PL guarantees serializability — concurrent transactions see a consistent view of the database. The growing phase acquires locks, the shrinking phase (commit/abort) releases them all at once.

**Why B+ Tree order=128?** Each node holds up to 255 keys before splitting. For 1 million rows, tree height stays at 3 — meaning any lookup touches at most 3 nodes regardless of dataset size.

---

## Limitations

- No query optimizer — always uses nested loop join and full scan unless an index exists on the WHERE column
- No MVCC — uses 2PL locking instead of multi-version concurrency control
- No subqueries or CTEs
- No foreign key constraints
- Single WAL file (no log rotation for very large datasets)

---

## License

MIT
