# Slotwatch

A lightweight Change Data Capture (CDC) pipeline written in Go. Slotwatch taps into PostgreSQL's Write-Ahead Log (WAL) and streams every INSERT, UPDATE, and DELETE to Kafka in real time — with a live dashboard to monitor the pipeline as it runs.

> Built from scratch in Go. No JVM, no Debezium, no Zookeeper.

![Go](https://img.shields.io/badge/Go-1.22+-00ADD8?style=flat&logo=go)
![Kafka](https://img.shields.io/badge/Kafka-3.7_KRaft-231F20?style=flat&logo=apache-kafka)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-16-336791?style=flat&logo=postgresql)
![Next.js](https://img.shields.io/badge/Next.js-14-000000?style=flat&logo=next.js)

---

## What is CDC and why does it matter?

Most applications keep multiple systems in sync with their database — a search index, a cache, an analytics pipeline. The naive approach is to poll ("check for changes every 5 seconds") or dual-write (write to Postgres *and* Elasticsearch at the same time). Both break down in production: polling is slow and hammers the DB, and dual-writes go out of sync the moment one write fails.

**Change Data Capture** solves this at the source. PostgreSQL already maintains a detailed log of every change it makes — the Write-Ahead Log (WAL) — for crash recovery. Slotwatch connects to that log as a logical replication client and turns each change into a structured event that downstream systems can consume reliably, in order, the moment it happens.

```
Your app writes to Postgres
         ↓
  Postgres writes to WAL  ←── already happens, always
         ↓
   Slotwatch reads WAL
         ↓
  Publishes to Kafka topic
    ┌────┴────┬──────────┐
    ↓         ↓          ↓
Elasticsearch  Redis    Analytics
  (search)   (cache)  (dashboard)
```

No polling. No dual writes. No sync bugs.

---

## Architecture

```
┌─────────────────────────────────────────────────────────┐
│                     Docker Compose                       │
│                                                          │
│  ┌──────────┐  WAL stream   ┌──────────────────────┐   │
│  │ Postgres │ ────────────► │   Slotwatch (Go)     │   │
│  │  :5432   │  (replication │                      │   │
│  └──────────┘   protocol)   │  WAL Reader          │   │
│       ▲                     │     ↓ channel        │   │
│       │ LSN checkpoint       │  Decoder             │   │
│       │ (SQL connection)     │     ↓ channel        │   │
│       └─────────────────────│  Publisher           │   │
│                             │     ↓ on Kafka ack   │   │
│  ┌──────────┐               │  LSN Tracker         │   │
│  │  Kafka   │ ◄──────────── │                      │   │
│  │  :9092   │ Debezium JSON │  HTTP API  :8080     │   │
│  │  KRaft   │               └──────────────────────┘   │
│  └──────────┘                        ▲                  │
│                                      │ proxy            │
│  ┌─────────────────────┐             │                  │
│  │ Next.js Dashboard   │ ────────────┘                  │
│  │  :3000              │                                │
│  └─────────────────────┘                                │
└─────────────────────────────────────────────────────────┘
```

**Four goroutines, connected by channels:**

| Component | What it does |
|-----------|-------------|
| **WAL Reader** | Opens a replication connection to Postgres. Streams binary WAL messages. Sends periodic heartbeats so Postgres can reclaim old WAL. |
| **Decoder** | Parses `pgoutput` binary protocol into structured `ChangeEvent` structs. |
| **Publisher** | Serializes events to Debezium-compatible JSON and produces to Kafka. Waits for Kafka ack before signalling success. |
| **LSN Tracker** | Only after Kafka acks does it write the current WAL position (LSN) to Postgres. This is the crash-safety guarantee. |

**Why channels?** Each stage runs at its own pace. If Kafka slows down, the channel fills up and the WAL reader naturally throttles — no explicit rate limiting code needed. This is backpressure.

---

## Crash Safety

The LSN (Log Sequence Number) is Postgres's bookmark in the WAL. Slotwatch stores the last confirmed LSN in a `slotwatch_checkpoints` table in your own Postgres instance. On restart, it resumes from exactly that position.

The order is: **publish → Kafka ack → save LSN**. If Slotwatch crashes between publish and save, it re-publishes on restart. This means **at-least-once delivery** — a small number of duplicates are possible after a crash, but events are never silently dropped.

---

## Kafka Output Format

Events are published in [Debezium-compatible](https://debezium.io/documentation/reference/stable/connectors/postgresql.html) JSON format (payload only, no schema envelope). This means existing Kafka consumers that work with Debezium work with Slotwatch without changes.

**Topic naming:** `<prefix>.<schema>.<table>` — e.g. `postgres.public.orders`

**Message key:** Primary key value of the changed row — guarantees all changes for the same row land on the same Kafka partition (preserving per-row order).

```json
{
  "op": "c",
  "before": null,
  "after": { "id": "42", "user": "anupam", "total": "350" },
  "source": {
    "connector": "slotwatch",
    "schema": "public",
    "table": "orders",
    "lsn": 27263488,
    "ts_ms": 1749398400000
  },
  "ts_ms": 1749398400000
}
```

Operation codes: `c` = INSERT, `u` = UPDATE, `d` = DELETE

---

## Quick Start

**Prerequisites:** Docker and Docker Compose

```bash
# 1. Clone and configure
git clone https://github.com/anupam/slotwatch
cd slotwatch
cp .env.example .env
# Edit .env — set POSTGRES_PASSWORD to anything you like

# 2. Set up Postgres (run once after `docker compose up postgres`)
docker compose up postgres -d
docker compose exec postgres psql -U postgres -d myapp -c "
  CREATE USER slotwatch REPLICATION LOGIN PASSWORD 'secret';
  GRANT CONNECT ON DATABASE myapp TO slotwatch;
  GRANT USAGE ON SCHEMA public TO slotwatch;
  GRANT SELECT ON ALL TABLES IN SCHEMA public TO slotwatch;
  CREATE TABLE orders (id SERIAL PRIMARY KEY, item TEXT, qty INT);
  CREATE PUBLICATION slotwatch_pub FOR TABLE public.orders;
"

# 3. Start everything
docker compose up --build

# Dashboard → http://localhost:3000
# API       → http://localhost:8080
```

---

## Postgres Setup (existing database)

If you're connecting Slotwatch to an existing Postgres database, you need to:

**1. Enable logical replication** (requires a Postgres restart):
```sql
-- In postgresql.conf:
wal_level = logical
```

**2. Create a dedicated user and publication:**
```sql
CREATE USER slotwatch REPLICATION LOGIN PASSWORD 'yourpassword';
GRANT CONNECT ON DATABASE yourdb TO slotwatch;
GRANT USAGE ON SCHEMA public TO slotwatch;
GRANT SELECT ON TABLE public.orders, public.users TO slotwatch;

-- List every table you want to capture
CREATE PUBLICATION slotwatch_pub FOR TABLE public.orders, public.users;
```

**3. Update `slotwatch.yml`** with your connection details and table list.

Adding a new table later: add it to the Postgres publication + add it to the config. No Slotwatch restart needed.

---

## Configuration

Copy `slotwatch.yml.example` to `slotwatch.yml`. Secrets can be injected via environment variables using `${VAR_NAME}` syntax — the value in the YAML file is used as a fallback if the env var is not set.

```yaml
postgres:
  host: localhost
  port: 5432
  user: slotwatch
  password: ${SLOTWATCH_PG_PASSWORD}   # set in .env
  database: myapp
  replication_slot: slotwatch_slot     # created automatically on first run
  publication: slotwatch_pub           # you create this once (see setup above)

kafka:
  brokers:
    - kafka:9092
  topic_prefix: postgres               # topics: postgres.public.orders

pipeline:
  tables:
    - public.orders
    - public.users
  wal_lag_warn_threshold_bytes: 10485760  # 10MB — shows warning banner in dashboard

api:
  port: 8080
```

---

## API Reference

| Method | Endpoint | Description |
|--------|----------|-------------|
| `GET` | `/health` | Liveness check — returns `{"status":"ok"}` |
| `GET` | `/status` | Pipeline state, WAL lag, connection health |
| `GET` | `/metrics` | Prometheus metrics (scrape with Grafana etc.) |
| `GET` | `/events/recent` | Last 50 captured events (in-memory) |
| `POST` | `/pipeline/pause` | Pause without losing WAL position |
| `POST` | `/pipeline/resume` | Resume from same position |

**`/status` response:**
```json
{
  "pipeline": "running",
  "current_lsn": "0/1A3F200",
  "last_event_at": "2026-06-08T17:30:00Z",
  "tables_tracked": ["public.orders", "public.users"],
  "kafka_connected": true,
  "postgres_connected": true,
  "wal_lag_bytes": 0
}
```

**`wal_lag_bytes`** = `pg_current_wal_lsn() - confirmed_flush_lsn` from `pg_replication_slots`. Zero means fully caught up with Postgres.

**Prometheus metrics:**
```
slotwatch_events_published_total{table="orders"} 14823
slotwatch_wal_lag_bytes 0
slotwatch_kafka_errors_total 0
slotwatch_pipeline_up 1
```

**Pause/Resume** — useful during downstream maintenance. The replication connection stays open so Postgres doesn't accumulate WAL. WAL position is preserved exactly. Resume picks up from the same point.

---

## Dashboard

The Next.js dashboard polls the API every 5 seconds and shows:

- Pipeline status (running / paused / error)
- Event count, WAL lag, current LSN
- Warning banner when WAL lag exceeds the configured threshold
- Connection error banner after 3 consecutive API failures (so stale data is never silently shown)
- Live table of the last 50 captured events with operation type, table, LSN, and timestamp
- Pause / Resume button

---

## Development

```bash
make build    # compile → bin/slotwatch
make test     # run unit tests
make lint     # go vet
make up       # docker compose up --build
make down     # docker compose down
```

**Running unit tests** (no external dependencies needed):
```bash
go test ./... -v
```

**Running integration tests** (requires a running Postgres):
```bash
SLOTWATCH_TEST_PG_DSN=postgres://slotwatch:secret@localhost:5432/myapp \
  go test ./internal/checkpoint/... -v
```

---

## Project Structure

```
├── cmd/slotwatch/main.go       # entrypoint
├── internal/
│   ├── config/                 # YAML + env var config loading
│   ├── types/                  # shared ChangeEvent type
│   ├── replication/            # Postgres WAL reader
│   ├── decoder/                # pgoutput binary → ChangeEvent
│   ├── publisher/              # Kafka producer + Debezium serialization
│   ├── checkpoint/             # LSN persistence in Postgres
│   ├── pipeline/               # goroutine orchestration, pause/resume
│   ├── ringbuffer/             # fixed-size in-memory event buffer
│   ├── metrics/                # Prometheus metric definitions
│   └── api/                    # HTTP server and handlers
├── dashboard/                  # Next.js 14 frontend
├── docker-compose.yml
├── Dockerfile
└── slotwatch.yml.example
```

---

## Why This Exists

Debezium is the dominant CDC tool, but it runs on the JVM and requires Kafka Connect infrastructure. Most Go shops don't want a JVM dependency just to get CDC working.

Slotwatch is a self-contained Go binary that does the same core job — read the WAL, publish to Kafka — without any JVM, Kafka Connect, or Zookeeper. The codebase is small enough to read and understand in an afternoon.

---

## License

MIT
