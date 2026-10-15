# RMQ QA Utility — Web Edition

A browser-based RabbitMQ management tool with SQL Server audit logging,
dual-mode event monitoring, port scanning, and full CRUD operations.
Mirrors the official RabbitMQ Management UI with extra QA features.

---

## Project Structure

```
rmq-qa-tool/
├── app.py                  ← Flask web server (main entry point)
├── rmq_client.py           ← RabbitMQ HTTP API wrapper
├── db.py                   ← SQL Server database layer (pyodbc)
├── audit.py                ← Dual-mode activity monitor
├── logger.py               ← Structured logging (DEBUG/ERROR/JSON)
├── port_scanner.py         ← Parallel RabbitMQ port scanner
├── config.json             ← ALL settings (edit this first)
├── start.bat               ← Windows one-click launcher
├── requirements.txt        ← Python dependencies
├── setup_database.sql      ← Run in SSMS to create DB + tables
├── templates/
│   └── index.html          ← Single-page web UI
├── static/                 ← CSS/JS assets (if separated later)
├── logs/                   ← Rotating log files written here
└── archives/               ← CSV audit archives written here
```

---

## Quick Start

### Step 1 — Install Python dependencies

```bat
pip install -r requirements.txt
```

Requires Python 3.10+ and the **ODBC Driver 17 for SQL Server** installed.
Download from: https://aka.ms/downloadmsodbcsql

---

### Step 2 — Create the SQL Server database

Open `setup_database.sql` in **SSMS** and run it.
This creates:
- Database: `RMQ_Events`
- Tables: `audit_log`, `rmq_events`, `rmq_snapshots`


---

### Step 3 — Edit config.json

Open `config.json` and update at minimum:

```json
"rabbitmq": {
    "host":     "your-rabbitmq-server",
    "port":     15672,
    "username": "guest",
    "password": "your-password"
},

"database": {
    "host":     "your-sql-server",
    "name":     "RMQ_Events",
    "username": "sa",
    "password": "your-db-password"
}
```

---

### Step 4 — Run

**Windows (double-click):**
```
start.bat
```

**Command line:**
```bat
python app.py
```

**Browser opens automatically at:** http://localhost:8080

---

## Config Reference

### `rabbitmq` section

| Key | Default | Description |
|-----|---------|-------------|
| `host` | `localhost` | RabbitMQ server hostname or IP |
| `port` | `15672` | Management API port |
| `username` | `guest` | RabbitMQ username |
| `password` | `guest` | RabbitMQ password |
| `vhost` | `/` | Default virtual host |
| `ssl` | `false` | Use HTTPS |
| `verify_ssl` | `false` | Verify SSL certificate |
| `timeout` | `10` | HTTP timeout in seconds |

---

### `database` section (SQL Server / SSMS)

| Key | Default | Description |
|-----|---------|-------------|
| `driver` | `ODBC Driver 17 for SQL Server` | ODBC driver name |
| `host` | `localhost` | SQL Server hostname |
| `port` | `1433` | SQL Server port |
| `name` | `RMQ_Events` | Database name |
| `username` | `sa` | SQL login username |
| `password` | — | SQL login password |
| `trusted_connection` | `false` | Use Windows auth instead |
| `encrypt` | `false` | Encrypt connection |
| `trust_server_certificate` | `true` | Skip cert validation |
| `auto_create_tables` | `true` | Auto-create tables on startup |

**Windows Authentication (no username/password):**
```json
"trusted_connection": true
```

---

### `logging` section

| Key | Default | Description |
|-----|---------|-------------|
| `level` | `DEBUG` | `DEBUG`, `INFO`, `WARNING`, `ERROR` |
| `file` | `logs/rmq_qa.log` | Log file path |
| `max_bytes` | `10485760` | Max log file size (10 MB) |
| `backup_count` | `5` | Number of rotated files to keep |
| `console` | `true` | Print logs to console |
| `json_format` | `false` | Emit JSON log lines |
| `log_api_calls` | `true` | Log every RabbitMQ API call |

---

### `audit` section

| Key | Default | Description |
|-----|---------|-------------|
| `retention_days` | `90` | Days to keep audit entries in DB |
| `archive_to_csv` | `true` | Archive expired entries to CSV |
| `archive_folder` | `archives` | Folder for CSV archives |
| `event_exchange` | `true` | Use `amq.rabbitmq.event` if available |
| `poll_interval_seconds` | `10` | Diff-polling interval |
| `plugin_poll_interval_seconds` | `3` | Event exchange polling interval |
| `store_raw_event` | `true` | Store full JSON payload in `rmq_events` table |

---

### `ports` section

| Key | Default | Description |
|-----|---------|-------------|
| `scan_on_connect` | `true` | Scan ports on every connect |
| `rescan_interval_seconds` | `60` | Background re-scan interval |
| `timeout_seconds` | `1` | Per-port socket timeout |
| `monitor` | (12 ports) | Port number → service name map |

---

### `server` section

| Key | Default | Description |
|-----|---------|-------------|
| `host` | `0.0.0.0` | Bind address (use `127.0.0.1` for local only) |
| `port` | `8080` | Web UI port |
| `debug` | `true` | Flask debug mode (set `false` in production) |
| `auto_open_browser` | `true` | Open browser on startup |
| `secret_key` | *(change this)* | Flask session secret — change in production |

---

## UI Tabs

| Tab | What it does |
|-----|-------------|
| **Overview** | Cluster stats, node health/memory bars, top queues, message rates |
| **Connections** | Live connections, close individual connections |
| **Channels** | Active channels with consumer and prefetch info |
| **Exchanges** | List, create, delete exchanges |
| **Queues** | List, create, purge, delete, preview messages, bulk operations |
| **Users** | List, create (with tags + vhost permissions), delete |
| **Virtual Hosts** | List, create, delete |
| **Policies** | List, create, delete — with JSON definition editor |
| **Shovels** | List and create dynamic shovels |
| **Port Scanner** | Scan all 12 RabbitMQ ports, see open/closed + latency |
| **Audit Log** | Full DB-backed audit: filter, search, paginate, export CSV, purge |

---

## Audit Log — How It Works

### Mode 1 (always active) — Diff Polling
- Polls the RabbitMQ HTTP API every 10 seconds
- Compares snapshots of queues, exchanges, users, vhosts, policies, bindings
- Detects creates, deletes, and purges
- Works with zero RabbitMQ plugin requirements

### Mode 2 (auto-enabled if plugin present) — Event Exchange
- Subscribes to `amq.rabbitmq.event` exchange
- Real-time — fires within 3 seconds
- Captures the exact user who performed each action
- Requires: `rabbitmq-plugins enable rabbitmq_event_exchange`

Both modes write to the **SQL Server `audit_log` table** simultaneously.

---

## SQL Server Tables

### `audit_log` — Main audit trail
Every action (from this app or detected externally) is stored here.

### `rmq_events` — Raw event exchange messages
Full JSON payloads from `amq.rabbitmq.event` (Mode 2 only).

### `rmq_snapshots` — State snapshots
Periodic full-state captures for diff comparison.

---

## Useful SSMS Queries

```sql
-- Latest 50 audit entries
SELECT TOP 50 * FROM audit_log ORDER BY timestamp DESC;

-- All deletes in the last 7 days
SELECT * FROM audit_log
WHERE action LIKE '%Delete%'
  AND timestamp >= DATEADD(DAY, -7, GETDATE())
ORDER BY timestamp DESC;

-- Actions by user
SELECT [user], COUNT(*) AS actions
FROM audit_log
GROUP BY [user]
ORDER BY actions DESC;
```

---

## Requirements

- Python 3.10+
- ODBC Driver 17 for SQL Server (https://aka.ms/downloadmsodbcsql)
- SQL Server 2016+ (or Azure SQL)
- RabbitMQ 3.8+ with Management Plugin enabled
