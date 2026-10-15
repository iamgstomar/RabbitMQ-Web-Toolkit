"""
audit.py  —  Dual-mode RabbitMQ activity monitor
Mode 1: HTTP diff-polling (no plugins needed)
Mode 2: amq.rabbitmq.event exchange subscription (real-time, richer data)
Both modes write to SQL Server via db.py.
"""

import base64
import json
import time
import threading
from datetime import datetime
from typing import Any, Dict, Optional, Set

import logger as log
from db import Database

_log = log.get_logger("audit")

# ── Routing key → human action name ─────────────────────────────────────────
EVENT_ACTION_MAP: Dict[str, str] = {
    "queue.created":          "Create Queue",
    "queue.deleted":          "Delete Queue",
    "queue.purged":           "Purge Queue",
    "exchange.created":       "Create Exchange",
    "exchange.deleted":       "Delete Exchange",
    "binding.created":        "Create Binding",
    "binding.deleted":        "Delete Binding",
    "user.created":           "Create User",
    "user.deleted":           "Delete User",
    "user.password.changed":  "Change Password",
    "user.tags.set":          "Set User Tags",
    "permission.created":     "Grant Permission",
    "permission.deleted":     "Revoke Permission",
    "vhost.created":          "Create VHost",
    "vhost.deleted":          "Delete VHost",
    "policy.created":         "Create Policy",
    "policy.deleted":         "Delete Policy",
    "connection.created":     "Connection Opened",
    "connection.closed":      "Connection Closed",
    "channel.created":        "Channel Opened",
    "channel.closed":         "Channel Closed",
    "consumer.created":       "Consumer Added",
    "consumer.deleted":       "Consumer Removed",
    "parameter.set":          "Set Parameter",
    "parameter.cleared":      "Clear Parameter",
    "alarm.set":              "Alarm Triggered",
    "alarm.cleared":          "Alarm Cleared",
}

# Events too noisy to log individually
SKIP_EVENTS: Set[str] = {
    "channel.created", "channel.closed",
    "consumer.created", "consumer.deleted",
    "alarm.set", "alarm.cleared",
}


class AuditEngine:
    """
    Background thread manager for RabbitMQ activity monitoring.
    Call start() after connecting, stop() on disconnect.
    """

    def __init__(self, rmq_client, db: Database, cfg: dict,
                 app_user: str = "", app_host: str = ""):
        self.rmq       = rmq_client
        self.db        = db
        self.cfg       = cfg.get("audit", {})
        self.app_user  = app_user
        self.app_host  = app_host

        self._active          = False
        self._plugin_active   = False
        self._event_queue     = None
        self._snapshot        = {}
        self._first_diff_done = False
        self._dedup_keys: Set[str] = set()

        self._poll_interval   = self.cfg.get("poll_interval_seconds",        10)
        self._plugin_interval = self.cfg.get("plugin_poll_interval_seconds",  3)
        self._dedup_window    = self.cfg.get("dedup_window_minutes",           1)
        self._dedup_max       = self.cfg.get("dedup_cache_size",             500)
        self._store_raw       = self.cfg.get("store_raw_event",             True)

        self._diff_thread   = None
        self._plugin_thread = None

    # ── Public API ────────────────────────────────────────────────────────────

    def start(self):
        """Initialise both monitoring modes in a background thread."""
        self._active = True
        t = threading.Thread(target=self._init, daemon=True, name="audit-init")
        t.start()
        _log.info("AuditEngine starting — host=%s user=%s", self.app_host, self.app_user)

    def stop(self):
        """Stop all monitoring and clean up the event queue."""
        _log.info("AuditEngine stopping")
        self._active        = False
        self._plugin_active = False
        self._snapshot      = {}
        self._dedup_keys    = set()
        self._first_diff_done = False
        self._cleanup_event_queue()

    def record(self, action: str, detail: str = "", status: str = "✅ Success",
               vhost: str = "", source: str = "RMQ QA Utility"):
        """Write an audit entry for an action taken within this app."""
        entry = {
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "user":      self.app_user,
            "host":      self.app_host,
            "action":    action,
            "detail":    detail[:self.cfg.get("max_detail_length", 1000)],
            "status":    status,
            "source":    source,
            "vhost":     vhost,
        }
        self.db.insert_audit(entry)
        _log.debug("Audit: [%s] %s — %s", action, detail[:80], status)

    # ── Initialisation ────────────────────────────────────────────────────────

    def _init(self):
        """Take initial snapshot, then try to set up event exchange plugin."""
        # Mode 1: baseline snapshot
        try:
            self._snapshot = self._fetch_snapshot()
            _log.info("Initial snapshot taken")
        except Exception as exc:
            _log.error("Snapshot failed: %s", exc)
            self._snapshot = {}

        # Mode 2: check for amq.rabbitmq.event
        if self.cfg.get("event_exchange", True):
            try:
                exchanges = self.rmq.exchanges()
                if any(e.get("name") == "amq.rabbitmq.event" for e in exchanges):
                    self._setup_event_queue()
                else:
                    _log.info("amq.rabbitmq.event not found — diff-only mode")
            except Exception as exc:
                _log.warning("Event exchange check failed: %s", exc)

        # Schedule both loops
        self._schedule_diff()
        if self._plugin_active:
            self._schedule_plugin()

    def _setup_event_queue(self):
        """Create a temp queue bound to amq.rabbitmq.event."""
        try:
            q_name = f"rmq-qa-audit-{int(time.time())}"
            self.rmq.create_temp_queue("/", q_name)
            self.rmq.bind_to_event_exchange("/", q_name)
            self._event_queue   = q_name
            self._plugin_active = True
            _log.info("Event exchange queue created: %s — dual-mode active", q_name)
        except Exception as exc:
            _log.warning("Could not set up event queue: %s", exc)

    def _cleanup_event_queue(self):
        if self._event_queue:
            try:
                self.rmq.delete_temp_queue("/", self._event_queue)
                _log.debug("Cleaned up event queue: %s", self._event_queue)
            except Exception:
                pass
            self._event_queue = None

    # ── Diff polling (Mode 1) ─────────────────────────────────────────────────

    def _schedule_diff(self):
        if not self._active:
            return
        t = threading.Timer(self._poll_interval, self._run_diff)
        t.daemon = True
        t.start()

    def _run_diff(self):
        if not self._active:
            return
        try:
            new_snap = self._fetch_snapshot()
            if self._first_diff_done:
                self._apply_diff(self._snapshot, new_snap)
            else:
                self._first_diff_done = True
                _log.debug("First diff skipped (baseline established)")
            self._snapshot = new_snap
        except Exception as exc:
            _log.error("Diff poll error: %s", exc)
        finally:
            self._schedule_diff()

    def _fetch_snapshot(self) -> dict:
        snap: Dict[str, Any] = {}
        if not self.rmq or not self.rmq.connected:
            return snap

        def _try(fn):
            try:    return fn()
            except Exception as exc:
                _log.debug("Snapshot fetch error: %s", exc)
                return None

        # Queues
        qs = _try(self.rmq.queues)
        snap["queues"] = {
            f"{q['vhost']}/{q['name']}": {
                "messages": q.get("messages", 0),
                "state":    q.get("state",    ""),
            }
            for q in (qs or [])
            if not q.get("name", "").startswith("rmq-qa-audit-")
        }

        # Exchanges (user-defined)
        exs = _try(self.rmq.exchanges)
        snap["exchanges"] = {
            f"{e['vhost']}/{e['name']}"
            for e in (exs or [])
            if e.get("name") and not e["name"].startswith("amq.")
        }

        # Users
        us = _try(self.rmq.users)
        snap["users"] = {
            u["name"]: u.get("tags", "")
            for u in (us or [])
        }

        # VHosts
        vhs = _try(self.rmq.vhosts)
        snap["vhosts"] = {v["name"] for v in (vhs or [])}

        # Policies
        ps = _try(self.rmq.policies)
        snap["policies"] = {
            f"{p['vhost']}/{p['name']}" for p in (ps or [])
        }

        # Connections
        conns = _try(self.rmq.connections)
        snap["connections"] = {c["name"] for c in (conns or [])}
        snap["active_users"] = list({
            c.get("user", "") for c in (conns or [])
            if c.get("user") and c.get("user") != "unknown"
        })

        # Bindings
        bs = _try(self.rmq.bindings)
        snap["bindings"] = {
            f"{b['vhost']}/{b['source']}->{b['destination']}/{b.get('routing_key','')}"
            for b in (bs or [])
            if b.get("source")
            and b.get("source") != "amq.rabbitmq.event"
            and not str(b.get("destination", "")).startswith("rmq-qa-audit-")
        }

        _log.debug("Snapshot: %d queues, %d exchanges, %d users",
                   len(snap["queues"]), len(snap["exchanges"]), len(snap["users"]))
        return snap

    def _apply_diff(self, old: dict, new: dict):
        """Detect changes and write audit entries for each difference."""
        who, src = self._attribute_change(new)

        def _ext(action, detail, vhost=""):
            self._record_detected(action, detail, src, who, vhost)

        # Queues
        old_q, new_q = old.get("queues", {}), new.get("queues", {})
        for key in set(new_q) - set(old_q):
            vh, name = key.split("/", 1)
            _ext("Create Queue", f"Queue: {name} | VHost: {vh}", vh)
        for key in set(old_q) - set(new_q):
            vh, name = key.split("/", 1)
            _ext("Delete Queue", f"Queue: {name} | VHost: {vh}", vh)
        for key in set(old_q) & set(new_q):
            if old_q[key].get("messages", 0) > 0 and new_q[key].get("messages", 0) == 0:
                vh, name = key.split("/", 1)
                _ext("Purge Queue",
                     f"Queue: {name} | VHost: {vh} | Was: {old_q[key]['messages']:,} msgs", vh)

        # Exchanges
        old_e, new_e = old.get("exchanges", set()), new.get("exchanges", set())
        for key in new_e - old_e:
            vh, name = key.split("/", 1)
            _ext("Create Exchange", f"Exchange: {name} | VHost: {vh}", vh)
        for key in old_e - new_e:
            vh, name = key.split("/", 1)
            _ext("Delete Exchange", f"Exchange: {name} | VHost: {vh}", vh)

        # Users
        old_u, new_u = old.get("users", {}), new.get("users", {})
        for name in set(new_u) - set(old_u):
            _ext("Create User", f"User: {name} | Tags: {new_u[name] or '(none)'}")
        for name in set(old_u) - set(new_u):
            _ext("Delete User", f"User: {name}")
        for name in set(old_u) & set(new_u):
            if str(old_u[name]) != str(new_u[name]):
                _ext("Change User Tags",
                     f"User: {name} | {old_u[name] or '(none)'} → {new_u[name] or '(none)'}")

        # VHosts
        old_v, new_v = old.get("vhosts", set()), new.get("vhosts", set())
        for name in new_v - old_v:
            _ext("Create VHost", f"VHost: {name}", name)
        for name in old_v - new_v:
            _ext("Delete VHost", f"VHost: {name}", name)

        # Policies
        old_p, new_p = old.get("policies", set()), new.get("policies", set())
        for key in new_p - old_p:
            vh, name = key.split("/", 1)
            _ext("Create Policy", f"Policy: {name} | VHost: {vh}", vh)
        for key in old_p - new_p:
            vh, name = key.split("/", 1)
            _ext("Delete Policy", f"Policy: {name} | VHost: {vh}", vh)

        # Connections
        old_c, new_c = old.get("connections", set()), new.get("connections", set())
        for name in new_c - old_c:
            _ext("Connection Opened", f"Conn: {name}")
        for name in old_c - new_c:
            _ext("Connection Closed", f"Conn: {name}")

        # Bindings
        old_b, new_b = old.get("bindings", set()), new.get("bindings", set())
        for key in new_b - old_b:
            _ext("Create Binding", key.split("/", 1)[-1][:200])
        for key in old_b - new_b:
            _ext("Delete Binding", key.split("/", 1)[-1][:200])

    def _attribute_change(self, new_snap: dict):
        """Try to identify which user made a change."""
        try:
            active = [u for u in new_snap.get("active_users", [])
                      if u != self.app_user]
            if len(active) == 1:
                return active[0], "RabbitMQ UI / External"
            elif len(active) > 1:
                return ", ".join(sorted(active)), "RabbitMQ UI / External"
            elif self.app_user:
                return self.app_user, f"RMQ QA Utility (via {self.app_user})"
        except Exception:
            pass
        return "unknown", "External (Detected)"

    def _record_detected(self, action: str, detail: str, source: str,
                         who: str, vhost: str = ""):
        dedup_key = (f"{action}:{detail[:40]}:"
                     f"{datetime.now().strftime('%Y-%m-%d %H:%M')}")
        if dedup_key in self._dedup_keys:
            return
        self._dedup_keys.add(dedup_key)
        if len(self._dedup_keys) > self._dedup_max:
            # Keep last half
            self._dedup_keys = set(list(self._dedup_keys)[-(self._dedup_max // 2):])

        entry = {
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "user":      who,
            "host":      self.app_host,
            "action":    action,
            "detail":    detail[:self.cfg.get("max_detail_length", 1000)],
            "status":    "✅ Detected",
            "source":    source,
            "vhost":     vhost,
        }
        self.db.insert_audit(entry)
        _log.info("Detected: [%s] %s — by %s (%s)", action, detail[:60], who, source)

    # ── Plugin polling (Mode 2) ───────────────────────────────────────────────

    def _schedule_plugin(self):
        if not self._active or not self._plugin_active:
            return
        t = threading.Timer(self._plugin_interval, self._run_plugin_poll)
        t.daemon = True
        t.start()

    def _run_plugin_poll(self):
        if not self._active or not self._plugin_active:
            return
        try:
            msgs = self.rmq.poll_queue("/", self._event_queue)
            for msg in msgs:
                self._process_event_msg(msg)
        except Exception as exc:
            _log.warning("Plugin poll error: %s", exc)
        finally:
            self._schedule_plugin()

    def _process_event_msg(self, msg: dict):
        """Parse a single amq.rabbitmq.event message and write to DB."""
        try:
            routing_key = msg.get("routing_key", "")
            if routing_key in SKIP_EVENTS:
                return
            if self._event_queue and self._event_queue in str(msg):
                return

            props       = msg.get("properties", {}) or {}
            raw_headers = props.get("headers",   {}) or {}
            payload_raw = msg.get("payload",      "")
            payload_enc = msg.get("payload_encoding", "string")

            # Decode payload
            data = {}
            try:
                raw = payload_raw
                if payload_enc == "base64" and raw:
                    raw = base64.b64decode(raw).decode("utf-8", errors="replace")
                if isinstance(raw, str) and raw.strip().startswith("{"):
                    data = json.loads(raw.strip())
                elif isinstance(raw, dict):
                    data = raw
            except Exception as exc:
                _log.debug("Payload decode error: %s", exc)

            # Decode AMQP typed header values
            def _hval(v):
                if v is None:                          return ""
                if isinstance(v, (str, int, float)):   return str(v)
                if isinstance(v, bytes):               return v.decode("utf-8", errors="replace")
                if isinstance(v, dict):
                    for t in ("longstr","signedint","long","float","bool"):
                        if t in v: return str(v[t])
                    return str(next(iter(v.values()), ""))
                return str(v)

            headers = {k: _hval(v) for k, v in raw_headers.items()}

            # Who
            who = (headers.get("user_who_performed_action", "").strip()
                   or data.get("user_who_performed_action", "")
                   or headers.get("user", "").strip()
                   or data.get("user", ""))
            if not who and routing_key.startswith("user."):
                who = data.get("name", headers.get("name", ""))
            who = str(who).strip() if who else "unknown"

            name  = str(data.get("name",  headers.get("name",  ""))).strip()
            vhost = str(data.get("vhost", headers.get("vhost", ""))).strip()

            parts = []
            if name:  parts.append(f"Name: {name}")
            if vhost: parts.append(f"VHost: {vhost}")
            if "connection" in routing_key:
                peer = data.get("peer_host") or headers.get("peer_host", "")
                if peer:
                    parts.append(f"From: {peer}:{data.get('peer_port','?')}")
            elif "permission" in routing_key:
                parts.append(f"Config:{data.get('configure','')} "
                             f"Write:{data.get('write','')} "
                             f"Read:{data.get('read','')}")
            elif routing_key == "user.tags.set":
                parts.append(f"Tags: {data.get('tags','')}")

            action = EVENT_ACTION_MAP.get(routing_key, routing_key)
            detail = " | ".join(parts) if parts else routing_key

            # Dedup
            ts_min   = datetime.now().strftime("%Y-%m-%d %H:%M")
            dedup    = f"{routing_key}:{name}:{vhost}:{ts_min}"
            if dedup in self._dedup_keys:
                return
            self._dedup_keys.add(dedup)
            if len(self._dedup_keys) > self._dedup_max:
                self._dedup_keys = set(list(self._dedup_keys)[-(self._dedup_max // 2):])

            source = ("RMQ QA Utility"
                      if (who and who == self.app_user)
                      else "RabbitMQ UI / External")

            # Write to both audit_log and rmq_events tables
            entry = {
                "timestamp":   datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "user":        who,
                "host":        self.app_host,
                "action":      action,
                "detail":      detail[:self.cfg.get("max_detail_length", 1000)],
                "status":      "✅ Success",
                "source":      source,
                "vhost":       vhost,
            }
            self.db.insert_audit(entry)

            raw_event_entry = {
                "timestamp":   entry["timestamp"],
                "routing_key": routing_key,
                "user":        who,
                "vhost":       vhost,
                "resource":    name,
                "action":      action,
                "source":      source,
                "dedup_key":   dedup[:500],
            }
            self.db.insert_event(
                raw_event_entry,
                raw_payload=data if self._store_raw else None)

            _log.info("Event: [%s] %s — by %s", action, detail[:80], who)

        except Exception as exc:
            _log.error("process_event_msg error: %s", exc)
