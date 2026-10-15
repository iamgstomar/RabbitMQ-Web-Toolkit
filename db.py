"""
db.py  —  Microsoft SQL Server database layer for RMQ QA Utility
Uses pyodbc (via SQLAlchemy) to store audit events, raw RMQ events,
and periodic snapshots.  Auto-creates tables on first run.
"""

import json
import os
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional
from contextlib import contextmanager

import pyodbc
import logger as log

_log = log.get_logger("db")

# Mirrors audit.py's EXCLUDED_ACTIONS. Kept as a plain list (not imported)
# to avoid a circular import between db.py and audit.py. Filtering here too
# means rows already written before this fix (Connect/Disconnect/Connection
# Opened-Closed/etc.) stop showing up without needing a manual cleanup.
EXCLUDED_ACTIONS = [
    "Connect", "Disconnect",
    "Connection Opened", "Connection Closed",
    "Channel Opened", "Channel Closed",
    "Consumer Added", "Consumer Removed",
    "Alarm Triggered", "Alarm Cleared",
]


# ── Connection pool ──────────────────────────────────────────────────────────

class Database:
    """Thin connection manager over pyodbc for SQL Server."""

    def __init__(self, cfg: dict):
        self.cfg       = cfg
        self._conn_str = self._build_conn_str(cfg)
        self._pool: List[pyodbc.Connection] = []
        self._pool_size = cfg.get("pool_size", 10)

        if cfg.get("auto_create_tables", True):
            self._ensure_tables()

        _log.info("Database initialised — server=%s  db=%s",
                  cfg.get("host"), cfg.get("name"))

    # ── Connection string ────────────────────────────────────────────────────

    @staticmethod
    def _build_conn_str(cfg: dict) -> str:
        driver    = cfg.get("driver", "ODBC Driver 17 for SQL Server")
        host      = cfg.get("host",   "localhost")
        port      = cfg.get("port",   1433)
        db        = cfg.get("name",   "RMQ_Events")
        user      = cfg.get("username", "sa")
        pwd       = cfg.get("password", "")
        trusted   = cfg.get("trusted_connection", False)
        encrypt   = "yes" if cfg.get("encrypt", False) else "no"
        trust_srv = "yes" if cfg.get("trust_server_certificate", True) else "no"

        if trusted:
            return (
                f"DRIVER={{{driver}}};"
                f"SERVER={host},{port};"
                f"DATABASE={db};"
                f"Trusted_Connection=yes;"
                f"Encrypt={encrypt};"
                f"TrustServerCertificate={trust_srv};"
            )
        return (
            f"DRIVER={{{driver}}};"
            f"SERVER={host},{port};"
            f"DATABASE={db};"
            f"UID={user};"
            f"PWD={pwd};"
            f"Encrypt={encrypt};"
            f"TrustServerCertificate={trust_srv};"
        )

    # ── Connection context manager ────────────────────────────────────────────

    @contextmanager
    def connection(self):
        """Yield a pyodbc connection; commit on success, rollback on error."""
        conn = None
        try:
            conn = pyodbc.connect(self._conn_str, timeout=30)
            conn.autocommit = False
            yield conn
            conn.commit()
        except Exception as exc:
            if conn:
                try:
                    conn.rollback()
                except Exception:
                    pass
            _log.exception("DB error: %s", exc)
            raise
        finally:
            if conn:
                try:
                    conn.close()
                except Exception:
                    pass

    # ── Table bootstrap ───────────────────────────────────────────────────────

    def _ensure_tables(self):
        """Create all required tables if they don't already exist."""
        schema  = self.cfg.get("schema", "dbo")
        t_audit = self.cfg.get("table_audit",     "audit_log")
        t_event = self.cfg.get("table_events",    "rmq_events")
        t_snap  = self.cfg.get("table_snapshots", "rmq_snapshots")

        ddl_audit = f"""
        IF NOT EXISTS (
            SELECT 1 FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_SCHEMA='{schema}' AND TABLE_NAME='{t_audit}'
        )
        BEGIN
            CREATE TABLE [{schema}].[{t_audit}] (
                [id]          BIGINT IDENTITY(1,1) PRIMARY KEY,
                [timestamp]   DATETIME2   NOT NULL DEFAULT GETDATE(),
                [user]        NVARCHAR(200) NULL,
                [host]        NVARCHAR(300) NULL,
                [action]      NVARCHAR(200) NULL,
                [detail]      NVARCHAR(MAX) NULL,
                [status]      NVARCHAR(100) NULL,
                [source]      NVARCHAR(200) NULL,
                [vhost]       NVARCHAR(300) NULL,
                [created_at]  DATETIME2 NOT NULL DEFAULT GETDATE()
            );
            CREATE INDEX [IX_{t_audit}_timestamp] ON [{schema}].[{t_audit}] ([timestamp] DESC);
            CREATE INDEX [IX_{t_audit}_user]      ON [{schema}].[{t_audit}] ([user]);
            CREATE INDEX [IX_{t_audit}_action]    ON [{schema}].[{t_audit}] ([action]);
        END
        """

        ddl_event = f"""
        IF NOT EXISTS (
            SELECT 1 FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_SCHEMA='{schema}' AND TABLE_NAME='{t_event}'
        )
        BEGIN
            CREATE TABLE [{schema}].[{t_event}] (
                [id]           BIGINT IDENTITY(1,1) PRIMARY KEY,
                [timestamp]    DATETIME2   NOT NULL DEFAULT GETDATE(),
                [routing_key]  NVARCHAR(300) NULL,
                [user]         NVARCHAR(200) NULL,
                [vhost]        NVARCHAR(300) NULL,
                [resource]     NVARCHAR(500) NULL,
                [action]       NVARCHAR(200) NULL,
                [source]       NVARCHAR(200) NULL,
                [raw_event]    NVARCHAR(MAX) NULL,
                [dedup_key]    NVARCHAR(500) NULL,
                [created_at]   DATETIME2 NOT NULL DEFAULT GETDATE()
            );
            CREATE INDEX [IX_{t_event}_timestamp]   ON [{schema}].[{t_event}] ([timestamp] DESC);
            CREATE INDEX [IX_{t_event}_routing_key] ON [{schema}].[{t_event}] ([routing_key]);
            CREATE UNIQUE INDEX [IX_{t_event}_dedup] ON [{schema}].[{t_event}] ([dedup_key])
                WHERE [dedup_key] IS NOT NULL;
        END
        """

        ddl_snap = f"""
        IF NOT EXISTS (
            SELECT 1 FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_SCHEMA='{schema}' AND TABLE_NAME='{t_snap}'
        )
        BEGIN
            CREATE TABLE [{schema}].[{t_snap}] (
                [id]          BIGINT IDENTITY(1,1) PRIMARY KEY,
                [timestamp]   DATETIME2   NOT NULL DEFAULT GETDATE(),
                [snap_type]   NVARCHAR(50)  NULL,
                [host]        NVARCHAR(300) NULL,
                [data]        NVARCHAR(MAX) NULL,
                [created_at]  DATETIME2 NOT NULL DEFAULT GETDATE()
            );
            CREATE INDEX [IX_{t_snap}_timestamp] ON [{schema}].[{t_snap}] ([timestamp] DESC);
            CREATE INDEX [IX_{t_snap}_type]      ON [{schema}].[{t_snap}] ([snap_type]);
        END
        """

        try:
            with self.connection() as conn:
                cur = conn.cursor()
                for ddl in [ddl_audit, ddl_event, ddl_snap]:
                    cur.execute(ddl)
            _log.info("Tables verified/created: %s, %s, %s", t_audit, t_event, t_snap)
        except Exception as exc:
            _log.error("Failed to create tables: %s", exc)
            raise

    # ── Audit log operations ──────────────────────────────────────────────────

    def insert_audit(self, entry: dict) -> Optional[int]:
        """Insert one audit entry. Returns the new row ID or None on error."""
        schema  = self.cfg.get("schema", "dbo")
        table   = self.cfg.get("table_audit", "audit_log")
        sql = f"""
            INSERT INTO [{schema}].[{table}]
                ([timestamp],[user],[host],[action],[detail],[status],[source],[vhost])
            OUTPUT INSERTED.id
            VALUES (?,?,?,?,?,?,?,?)
        """
        try:
            with self.connection() as conn:
                cur  = conn.cursor()
                ts   = entry.get("timestamp", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
                row  = cur.execute(sql, (
                    ts,
                    entry.get("user",   ""),
                    entry.get("host",   ""),
                    entry.get("action", ""),
                    entry.get("detail", "")[:4000],
                    entry.get("status", ""),
                    entry.get("source", ""),
                    entry.get("vhost",  ""),
                )).fetchone()
                _log.debug("Inserted audit entry id=%s action=%s",
                           row[0] if row else "?", entry.get("action"))
                return row[0] if row else None
        except Exception as exc:
            _log.error("insert_audit failed: %s", exc)
            return None

    def get_audit_log(
        self,
        limit:  int  = 500,
        offset: int  = 0,
        user:   str  = None,
        action: str  = None,
        source: str  = None,
        status: str  = None,
        from_date: str = None,
        to_date:   str = None,
        search:    str = None,
    ) -> List[dict]:
        """Fetch audit entries with optional filtering, newest first."""
        schema = self.cfg.get("schema", "dbo")
        table  = self.cfg.get("table_audit", "audit_log")

        where  = ["1=1", f"[action] NOT IN ({','.join('?' for _ in EXCLUDED_ACTIONS)})"]
        params = list(EXCLUDED_ACTIONS)

        if user:
            where.append("[user] = ?");           params.append(user)
        if action:
            where.append("[action] = ?");         params.append(action)
        if source:
            where.append("[source] LIKE ?");      params.append(f"%{source}%")
        if status:
            where.append("[status] LIKE ?");      params.append(f"%{status}%")
        if from_date:
            where.append("[timestamp] >= ?");     params.append(from_date)
        if to_date:
            where.append("[timestamp] <= ?");     params.append(to_date + " 23:59:59")
        if search:
            where.append(
                "([action] LIKE ? OR [detail] LIKE ? OR [user] LIKE ?)")
            params += [f"%{search}%", f"%{search}%", f"%{search}%"]

        sql = f"""
            SELECT [id],[timestamp],[user],[host],[action],
                   [detail],[status],[source],[vhost]
            FROM [{schema}].[{table}]
            WHERE {" AND ".join(where)}
            ORDER BY [timestamp] DESC
            OFFSET ? ROWS FETCH NEXT ? ROWS ONLY

        """

        params += [offset, limit]

        try:
            with self.connection() as conn:
                cur  = conn.cursor()
                rows = cur.execute(sql, params).fetchall()
                cols = [d[0] for d in cur.description]
                return [dict(zip(cols, row)) for row in rows]
        except Exception as exc:
            _log.error("get_audit_log failed: %s", exc)
            return []

    def count_audit(self, **filters) -> int:
        """Return total count matching the same filters as get_audit_log."""
        schema = self.cfg.get("schema", "dbo")
        table  = self.cfg.get("table_audit", "audit_log")
        where  = ["1=1", f"[action] NOT IN ({','.join('?' for _ in EXCLUDED_ACTIONS)})"]
        params = list(EXCLUDED_ACTIONS)

        if filters.get("user"):
            where.append("[user] = ?");           params.append(filters["user"])
        if filters.get("action"):
            where.append("[action] = ?");         params.append(filters["action"])
        if filters.get("source"):
            where.append("[source] LIKE ?");      params.append(f"%{filters['source']}%")
        if filters.get("from_date"):
            where.append("[timestamp] >= ?");     params.append(filters["from_date"])
        if filters.get("to_date"):
            where.append("[timestamp] <= ?");     params.append(filters["to_date"] + " 23:59:59")
        if filters.get("search"):
            s = filters["search"]
            where.append("([action] LIKE ? OR [detail] LIKE ? OR [user] LIKE ?)")
            params += [f"%{s}%", f"%{s}%", f"%{s}%"]

        sql = f"SELECT COUNT(*) FROM [{schema}].[{table}] WHERE {' AND '.join(where)}"
        try:
            with self.connection() as conn:
                row = conn.cursor().execute(sql, params).fetchone()
                return row[0] if row else 0
        except Exception:
            return 0

    def purge_old_audit(self, retention_days: int = 90) -> int:
        """Delete audit entries older than retention_days. Returns deleted count."""
        schema  = self.cfg.get("schema", "dbo")
        table   = self.cfg.get("table_audit", "audit_log")
        cutoff  = (datetime.now() - timedelta(days=retention_days)).strftime("%Y-%m-%d")
        sql     = f"DELETE FROM [{schema}].[{table}] WHERE [timestamp] < ?"
        try:
            with self.connection() as conn:
                cur = conn.cursor()
                cur.execute(sql, (cutoff,))
                count = cur.rowcount
                _log.info("Purged %d audit entries older than %s", count, cutoff)
                return count
        except Exception as exc:
            _log.error("purge_old_audit failed: %s", exc)
            return 0

    # ── Raw event log operations ───────────────────────────────────────────────

    def insert_event(self, event: dict, raw_payload: dict = None) -> Optional[int]:
        """Store a raw RabbitMQ event exchange message."""
        schema = self.cfg.get("schema", "dbo")
        table  = self.cfg.get("table_events", "rmq_events")
        sql = f"""
            INSERT INTO [{schema}].[{table}]
                ([timestamp],[routing_key],[user],[vhost],[resource],
                 [action],[source],[raw_event],[dedup_key])
            OUTPUT INSERTED.id
            VALUES (?,?,?,?,?,?,?,?,?)
        """
        dedup = event.get("dedup_key")
        try:
            with self.connection() as conn:
                cur = conn.cursor()
                row = cur.execute(sql, (
                    event.get("timestamp", datetime.now().strftime("%Y-%m-%d %H:%M:%S")),
                    event.get("routing_key", ""),
                    event.get("user",        ""),
                    event.get("vhost",       ""),
                    event.get("resource",    ""),
                    event.get("action",      ""),
                    event.get("source",      ""),
                    json.dumps(raw_payload, default=str) if raw_payload else None,
                    dedup[:500] if dedup else None,
                )).fetchone()
                return row[0] if row else None
        except pyodbc.IntegrityError:
            _log.debug("Duplicate event skipped: %s", dedup)
            return None
        except Exception as exc:
            _log.error("insert_event failed: %s", exc)
            return None

    # ── Snapshot operations ───────────────────────────────────────────────────

    def save_snapshot(self, snap_type: str, host: str, data: dict) -> None:
        """Persist a state snapshot (queues, exchanges, users, etc.)."""
        schema = self.cfg.get("schema", "dbo")
        table  = self.cfg.get("table_snapshots", "rmq_snapshots")
        sql = f"""
            INSERT INTO [{schema}].[{table}] ([snap_type],[host],[data])
            VALUES (?,?,?)
        """
        try:
            with self.connection() as conn:
                conn.cursor().execute(
                    sql, (snap_type, host, json.dumps(data, default=str)))
            _log.debug("Saved snapshot type=%s host=%s", snap_type, host)
        except Exception as exc:
            _log.error("save_snapshot failed: %s", exc)

    def get_latest_snapshot(self, snap_type: str, host: str) -> Optional[dict]:
        """Return the most recent snapshot for a given type and host."""
        schema = self.cfg.get("schema", "dbo")
        table  = self.cfg.get("table_snapshots", "rmq_snapshots")
        sql = f"""
            SELECT TOP 1 [data],[timestamp]
            FROM [{schema}].[{table}]
            WHERE [snap_type]=? AND [host]=?
            ORDER BY [timestamp] DESC
        """
        try:
            with self.connection() as conn:
                row = conn.cursor().execute(sql, (snap_type, host)).fetchone()
                if row:
                    return json.loads(row[0])
        except Exception as exc:
            _log.error("get_latest_snapshot failed: %s", exc)
        return None

    # ── Distinct value helpers (for filter dropdowns) ─────────────────────────

    def distinct_users(self) -> List[str]:
        schema = self.cfg.get("schema", "dbo")
        table  = self.cfg.get("table_audit", "audit_log")
        try:
            with self.connection() as conn:
                rows = conn.cursor().execute(
                    f"SELECT DISTINCT [user] FROM [{schema}].[{table}] "
                    f"WHERE [user] IS NOT NULL ORDER BY [user]"
                ).fetchall()
                return [r[0] for r in rows if r[0]]
        except Exception:
            return []

    def distinct_actions(self) -> List[str]:
        schema = self.cfg.get("schema", "dbo")
        table  = self.cfg.get("table_audit", "audit_log")
        excl   = ','.join('?' for _ in EXCLUDED_ACTIONS)
        try:
            with self.connection() as conn:
                rows = conn.cursor().execute(
                    f"SELECT DISTINCT [action] FROM [{schema}].[{table}] "
                    f"WHERE [action] IS NOT NULL AND [action] NOT IN ({excl}) "
                    f"ORDER BY [action]",
                    EXCLUDED_ACTIONS,
                ).fetchall()
                return [r[0] for r in rows if r[0]]
        except Exception:
            return []

    # ── Health check ──────────────────────────────────────────────────────────

    def ping(self) -> bool:
        """Return True if the database is reachable."""
        try:
            with self.connection() as conn:
                conn.cursor().execute("SELECT 1")
            return True
        except Exception as exc:
            _log.error("DB ping failed: %s", exc)
            return False
