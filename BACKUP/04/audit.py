"""
REQUIRES rmq_client.py additions (if not already present):

  def get_queue(self, vhost, name):
      return self.get(f"/queues/{quote(vhost, safe='')}/{quote(name, safe='')}")

  def create_temp_queue(self, vhost, name, arguments=None):
      body = {"auto_delete": True, "durable": False, "arguments": arguments or {}}
      self.put(f"/queues/{quote(vhost, safe='')}/{quote(name, safe='')}", body)

  def poll_queue(self, vhost, name, count=200):
      return self.post(f"/queues/{quote(vhost, safe='')}/{quote(name, safe='')}/get",
                       {"count": count, "ackmode": "ack_requeue_false", "encoding": "auto"}) or []

audit.py  —  Dual-mode RabbitMQ activity monitor
Mode 1: HTTP diff-polling (no plugins needed)
Mode 2: amq.rabbitmq.event exchange subscription (real-time, richer data, correct attribution)
Both modes write to SQL Server via db.py.

FIXES IN THIS VERSION
─────────────────────
1. VHost blank in Detail
   Root cause: default vhost "/" caused key "//QueueName"; split("/",1) gave
   vh="" and name="/QueueName". Fixed by _split_vhost_key() which handles the
   leading slash correctly for the default vhost.

2. User always "unknown" in diff mode
   Root cause: _attribute_change() only checked active connections at the moment
   of the next diff poll. If the user closed their browser tab before the 10-second
   poll fired, no connection existed → "unknown". Fixed by maintaining a
   _recent_users cache that remembers every user seen in a connection within the
   last N minutes, and using that for attribution instead of only live connections.

3. Source always "External (Detected)"
   Root cause: diff mode has no reliable per-event attribution — it only knows
   something changed, not who did it. Source label is now more informative:
   shows "RabbitMQ UI / External" with the attributed user when known, and
   "External (Detected)" only when truly unattributable. In plugin (Mode 2) the
   user_who_performed_action header is used directly — that is always correct.

4. Plugin mode attribution fallback ordering bug
   Root cause: "user_who_performed_action" was checked first in headers, but
   for some event types RabbitMQ puts it only in the payload. Fixed by checking
   both headers and payload with correct priority.

5. db.insert_event() missing null guard → crash when db=None
   Fixed with if self.db is not None guard + try/except.

6. _check_queue_depth() fetching ALL queues every 3 s
   Fixed with single-queue GET /queues/<vhost>/<name> via rmq.get_queue().

7. _file_append() unconditional double-write in record() and _record_detected()
   Fixed: file is written only when db_ok is False (true fallback behaviour).

8. Duplicate audit entries when plugin mode is active
   Root cause: diff polling and the event-exchange plugin both ran independently
   and both wrote audit rows for the same real-world change, using two different
   dedup-key formats that never matched each other. Fixed: once plugin mode is
   confirmed working, diff polling continues (to keep _recent_users / snapshot
   state warm) but stops writing audit rows itself — the plugin is the single
   source of truth for audit entries whenever it's available.

9. Connect / Disconnect / Connection Opened / Connection Closed noise
   These are app-session events, not RabbitMQ resource changes performed by a
   user, so they are no longer written to the audit log at all (see
   EXCLUDED_ACTIONS below). db.py also filters them out of historical queries
   so old rows already in the table stop showing up too.
"""

import base64
import os
import json
import time
import threading
from collections import deque
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Set

import logger as log
from db import Database

_log = log.get_logger("audit")

# ── Routing key → human action name ──────────────────────────────────────────
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

# Events too noisy to log individually, or not a real user action.
# (connection/channel/consumer/alarm events are session/system noise, not
#  someone deliberately creating, deleting, or changing a RabbitMQ resource)
SKIP_EVENTS: Set[str] = {
    "connection.created", "connection.closed",
    "channel.created", "channel.closed",
    "consumer.created", "consumer.deleted",
    "alarm.set", "alarm.cleared",
}

# Action names that should NEVER be written to the audit log, and should be
# excluded from historical queries even if older rows already have them
# (db.py applies the same list when reading). These are app-session events
# (Connect/Disconnect) or RabbitMQ plumbing events (Connection Opened/Closed,
# Channel Opened/Closed, Consumer Added/Removed, Alarm Triggered/Cleared) —
# not a deliberate action a user took against a RabbitMQ resource.
EXCLUDED_ACTIONS: Set[str] = {
    "Connect", "Disconnect",
    "Connection Opened", "Connection Closed",
    "Channel Opened", "Channel Closed",
    "Consumer Added", "Consumer Removed",
    "Alarm Triggered", "Alarm Cleared",
}

# Default vhost display name
DEFAULT_VHOST = "/"


def _normalise_vhost(vhost: str) -> str:
    """Return '/' when vhost is empty/None — RabbitMQ's default vhost."""
    if not vhost or str(vhost).strip() in ("", "%2F"):
        return DEFAULT_VHOST
    return str(vhost).strip()


def _split_vhost_key(key: str):
    """
    Split a snapshot key of the form '<vhost>/<resource_name>' correctly.

    The default vhost is '/' which makes keys like '//QueueName'.
    A naive split('/') would give vh='' and name='/QueueName'.
    This function handles that case explicitly.

    Examples:
        '//MyQueue'         → ('/', 'MyQueue')
        '/production/MyQ'   → ('/production', 'MyQ')   [custom vhost]
        'staging/MyQ'       → ('staging', 'MyQ')
    """
    if key.startswith("//"):
        # Default vhost '/' followed by resource name
        return DEFAULT_VHOST, key[2:]
    if "/" in key:
        vh, name = key.split("/", 1)
        return _normalise_vhost(vh), name
    return DEFAULT_VHOST, key


class _RecentUserCache:
    """
    Remembers every (user, host) pair seen in active connections within a
    rolling time window. This solves the 'unknown user' problem in diff mode:
    even if a user closes their browser tab before the next poll fires, we
    still know they were recently active and can attribute the change to them.
    """

    def __init__(self, window_minutes: int = 5, max_size: int = 200):
        self._window   = timedelta(minutes=window_minutes)
        self._max_size = max_size
        # deque of (datetime, user, host) tuples
        self._entries: deque = deque()
        self._lock = threading.Lock()

    def update(self, connections: list, app_user: str):
        """Feed current connection list into the cache."""
        now = datetime.now()
        with self._lock:
            for c in connections:
                user = (c.get("user") or "").strip()
                if user and user != "unknown" and user != app_user:
                    self._entries.append((now, user, c.get("peer_host", "")))
            # Prune expired entries
            cutoff = now - self._window
            while self._entries and self._entries[0][0] < cutoff:
                self._entries.popleft()
            # Trim to max size
            while len(self._entries) > self._max_size:
                self._entries.popleft()

    def recent_users(self) -> List[str]:
        """Return distinct non-app users seen within the window, newest first."""
        cutoff = datetime.now() - self._window
        with self._lock:
            seen, result = set(), []
            for ts, user, _ in reversed(self._entries):
                if ts < cutoff:
                    break
                if user not in seen:
                    seen.add(user)
                    result.append(user)
        return result


class AuditEngine:
    """
    Background thread manager for RabbitMQ activity monitoring.
    Call start() after connecting, stop() on disconnect.
    """

    def __init__(self, rmq_client, db: Database, cfg: dict,
                 app_user: str = "", app_host: str = ""):
        self.rmq      = rmq_client
        self.db       = db
        self.cfg      = cfg.get("audit", {})
        self.app_user = app_user
        self.app_host = app_host

        self._active          = False
        self._plugin_active   = False
        self._event_queue: Optional[str] = None
        self._snapshot        = {}
        self._first_diff_done = False
        self._dedup_keys: Set[str] = set()

        self._poll_interval   = self.cfg.get("poll_interval_seconds",        10)
        self._plugin_interval = self.cfg.get("plugin_poll_interval_seconds",  3)
        self._dedup_max       = self.cfg.get("dedup_cache_size",             500)
        self._store_raw       = self.cfg.get("store_raw_event",             True)

        # FIX 2: rolling user cache for diff-mode attribution
        self._recent_users = _RecentUserCache(
            window_minutes=self.cfg.get("attribution_window_minutes", 5),
            max_size=200,
        )

        self._diff_thread   = None
        self._plugin_thread = None

    # ── Public API ─────────────────────────────────────────────────────────────

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
               vhost: str = "", source: str = "PuffinMQ"):
        """Write an audit entry — tries DB first, falls back to local JSON file.

        Entries in EXCLUDED_ACTIONS (app-session events like Connect/Disconnect)
        are intentionally never written — the audit log should only contain
        deliberate actions a user took against a RabbitMQ resource.
        """
        if action in EXCLUDED_ACTIONS:
            _log.debug("Skipping excluded action from audit log: %s", action)
            return

        entry = {
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "user":      self.app_user,
            "host":      self.app_host,
            "action":    action,
            "detail":    detail[:self.cfg.get("max_detail_length", 1000)],
            "status":    status,
            "source":    source,
            "vhost":     _normalise_vhost(vhost),
        }
        # FIX 7: Only write to file when DB insert fails (true fallback)
        db_ok = False
        if self.db is not None:
            try:
                result = self.db.insert_audit(entry)
                db_ok  = result is not None
            except Exception as exc:
                _log.warning("DB insert_audit failed, writing to file: %s", exc)

        if not db_ok:
            self._file_append(entry)

        _log.debug("Audit: [%s] %s — %s (db=%s)", action, detail[:80], status, db_ok)

    @staticmethod
    def _file_append(entry: dict, path: str = "rmq_audit_log.json") -> None:
        """Append one audit entry to the local JSON backup file."""
        try:
            entries = []
            if os.path.exists(path):
                with open(path, "r", encoding="utf-8") as f:
                    entries = json.load(f)
            entries.append(entry)
            if len(entries) > 5000:
                entries = entries[-5000:]
            with open(path, "w", encoding="utf-8") as f:
                json.dump(entries, f, indent=2, default=str)
        except Exception as exc:
            _log.warning("audit file append failed: %s", exc)

    # ── Initialisation ─────────────────────────────────────────────────────────

    def _init(self):
        """Take initial snapshot, then try to set up event exchange plugin."""
        try:
            self._snapshot = self._fetch_snapshot()
            _log.info("Initial snapshot taken")
        except Exception as exc:
            _log.error("Snapshot failed: %s", exc)
            self._snapshot = {}

        if self.cfg.get("event_exchange", True):
            try:
                exchanges = self.rmq.exchanges()
                if any(e.get("name") == "amq.rabbitmq.event" for e in exchanges):
                    self._setup_event_queue()
                else:
                    _log.info("amq.rabbitmq.event not found — diff-only mode")
            except Exception as exc:
                _log.warning("Event exchange check failed: %s", exc)

        self._schedule_diff()
        if self._plugin_active:
            self._schedule_plugin()

    def _cleanup_orphan_audit_queues(self):
        """Delete leftover rmq-qa-audit-* queues from previous sessions."""
        try:
            all_queues = self.rmq.queues()
            orphans = [
                q for q in all_queues
                if q.get("name", "").startswith("rmq-qa-audit-")
                and q.get("name") != self._event_queue
            ]
            for q in orphans:
                try:
                    self.rmq.delete_temp_queue(q.get("vhost", "/"), q["name"])
                    _log.info("Cleaned up orphan audit queue: %s", q["name"])
                except Exception as exc:
                    _log.warning("Could not delete orphan queue %s: %s", q["name"], exc)
        except Exception as exc:
            _log.warning("Orphan audit queue cleanup failed: %s", exc)

    def _setup_event_queue(self):
        """Create a temp queue bound to amq.rabbitmq.event."""
        try:
            self._cleanup_orphan_audit_queues()

            q_name       = f"rmq-qa-audit-{int(time.time())}"
            max_length   = self.cfg.get("event_queue_max_length",   5000)
            msg_ttl_ms   = self.cfg.get("event_queue_msg_ttl_ms",   300_000)
            queue_exp_ms = self.cfg.get("event_queue_expires_ms",   1_800_000)

            queue_args = {
                "x-max-length":  max_length,
                "x-overflow":    "drop-head",
                "x-message-ttl": msg_ttl_ms,
                "x-expires":     queue_exp_ms,
            }
            self.rmq.create_temp_queue("/", q_name, arguments=queue_args)
            self.rmq.bind_to_event_exchange("/", q_name)
            self._event_queue   = q_name
            self._plugin_active = True
            _log.info(
                "Event queue created: %s (max=%d msgs, ttl=%ds, expires=%ds)",
                q_name, max_length, msg_ttl_ms // 1000, queue_exp_ms // 1000,
            )
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

    # ── Diff polling (Mode 1) ──────────────────────────────────────────────────

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
                # FIX 8: when the event-exchange plugin is active, it is the
                # single source of truth for audit entries. Diff polling still
                # runs (to keep the snapshot / recent-user cache warm as a
                # fallback) but no longer writes its own audit rows, which is
                # what caused every real change to be logged twice.
                if not self._plugin_active:
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

        # Queues — key as "<vhost>/<name>" using normalised vhost
        qs = _try(self.rmq.queues)
        snap["queues"] = {}
        for q in (qs or []):
            if q.get("name", "").startswith("rmq-qa-audit-"):
                continue
            vh   = _normalise_vhost(q.get("vhost", ""))
            name = q.get("name", "")
            # FIX 1: Use normalised vhost so default '/' produces key '//Name'
            #         which _split_vhost_key() handles correctly.
            key  = f"{vh}/{name}"
            snap["queues"][key] = {
                "messages": q.get("messages", 0),
                "state":    q.get("state",    ""),
            }

        # Exchanges (user-defined)
        exs = _try(self.rmq.exchanges)
        snap["exchanges"] = set()
        for e in (exs or []):
            if e.get("name") and not e["name"].startswith("amq."):
                vh  = _normalise_vhost(e.get("vhost", ""))
                key = f"{vh}/{e['name']}"
                snap["exchanges"].add(key)

        # Users
        us = _try(self.rmq.users)
        snap["users"] = {
            u["name"]: u.get("tags", "")
            for u in (us or [])
        }

        # VHosts
        vhs = _try(self.rmq.vhosts)
        snap["vhosts"] = {_normalise_vhost(v["name"]) for v in (vhs or [])}

        # Policies
        ps = _try(self.rmq.policies)
        snap["policies"] = set()
        for p in (ps or []):
            vh  = _normalise_vhost(p.get("vhost", ""))
            key = f"{vh}/{p['name']}"
            snap["policies"].add(key)

        # Connections — also feed the recent-user cache here (FIX 2)
        # (no longer diffed into audit entries — see EXCLUDED_ACTIONS — but
        #  still needed to keep _recent_users warm for attribution)
        conns = _try(self.rmq.connections) or []
        snap["connections"] = {c["name"] for c in conns}
        snap["active_users"] = list({
            c.get("user", "") for c in conns
            if c.get("user") and c.get("user") != "unknown"
        })
        # Update rolling cache with this snapshot's connections
        self._recent_users.update(conns, self.app_user)

        # Bindings
        bs = _try(self.rmq.bindings)
        snap["bindings"] = set()
        for b in (bs or []):
            if (not b.get("source")
                    or b.get("source") == "amq.rabbitmq.event"
                    or str(b.get("destination", "")).startswith("rmq-qa-audit-")):
                continue
            vh  = _normalise_vhost(b.get("vhost", ""))
            key = f"{vh}/{b['source']}->{b['destination']}/{b.get('routing_key','')}"
            snap["bindings"].add(key)

        _log.debug("Snapshot: %d queues, %d exchanges, %d users",
                   len(snap["queues"]), len(snap["exchanges"]), len(snap["users"]))
        return snap

    def _apply_diff(self, old: dict, new: dict):
        """Detect changes and write audit entries for each difference.

        Only called when the event-exchange plugin is NOT active (see FIX 8
        in _run_diff). Connections are intentionally not diffed into audit
        entries — see EXCLUDED_ACTIONS.
        """
        who, src = self._attribute_change(new)

        def _ext(action, detail, vhost=""):
            self._record_detected(action, detail, src, who, vhost)

        # Queues — FIX 1: use _split_vhost_key() everywhere
        old_q, new_q = old.get("queues", {}), new.get("queues", {})
        for key in set(new_q) - set(old_q):
            vh, name = _split_vhost_key(key)
            _ext("Create Queue", f"Queue: {name} | VHost: {vh}", vh)
        for key in set(old_q) - set(new_q):
            vh, name = _split_vhost_key(key)
            _ext("Delete Queue", f"Queue: {name} | VHost: {vh}", vh)
        for key in set(old_q) & set(new_q):
            if old_q[key].get("messages", 0) > 0 and new_q[key].get("messages", 0) == 0:
                vh, name = _split_vhost_key(key)
                _ext("Purge Queue",
                     f"Queue: {name} | VHost: {vh} | Was: {old_q[key]['messages']:,} msgs", vh)

        # Exchanges
        old_e, new_e = old.get("exchanges", set()), new.get("exchanges", set())
        for key in new_e - old_e:
            vh, name = _split_vhost_key(key)
            _ext("Create Exchange", f"Exchange: {name} | VHost: {vh}", vh)
        for key in old_e - new_e:
            vh, name = _split_vhost_key(key)
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
            vh, name = _split_vhost_key(key)
            _ext("Create Policy", f"Policy: {name} | VHost: {vh}", vh)
        for key in old_p - new_p:
            vh, name = _split_vhost_key(key)
            _ext("Delete Policy", f"Policy: {name} | VHost: {vh}", vh)

        # Bindings
        old_b, new_b = old.get("bindings", set()), new.get("bindings", set())
        for key in new_b - old_b:
            _ext("Create Binding", _split_vhost_key(key)[1][:200])
        for key in old_b - new_b:
            _ext("Delete Binding", _split_vhost_key(key)[1][:200])

    def _attribute_change(self, new_snap: dict):
        """
        Identify which user most likely made a detected change.

        FIX 2 — Attribution strategy (best → fallback):
        1. Live connections: another user is actively connected right now.
        2. Recent cache: a non-app user was seen connected within the last N
           minutes (default 5). Covers the case where they closed their browser
           tab between diff polls.
        3. Only app user connected / nobody connected → "External (Detected)".

        In all cases where a real user is identified the source is labelled
        "RabbitMQ UI / External" (the change came from outside the QA Utility).
        """
        try:
            live_others = [
                u for u in new_snap.get("active_users", [])
                if u != self.app_user
            ]
            if live_others:
                who = ", ".join(sorted(set(live_others)))
                return who, "RabbitMQ UI / External"

            # Fall back to recently-seen cache
            recent = self._recent_users.recent_users()
            if recent:
                who = ", ".join(recent[:3])   # cap at 3 names for readability
                return who, "RabbitMQ UI / External"

        except Exception:
            pass

        return "unknown", "External (Detected)"

    def _record_detected(self, action: str, detail: str, source: str,
                         who: str, vhost: str = ""):
        if action in EXCLUDED_ACTIONS:
            return

        dedup_key = (f"{action}:{detail[:40]}:"
                     f"{datetime.now().strftime('%Y-%m-%d %H:%M')}")
        if dedup_key in self._dedup_keys:
            return
        self._dedup_keys.add(dedup_key)
        if len(self._dedup_keys) > self._dedup_max:
            self._dedup_keys = set(list(self._dedup_keys)[-(self._dedup_max // 2):])

        entry = {
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "user":      who,
            "host":      self.app_host,
            "action":    action,
            "detail":    detail[:self.cfg.get("max_detail_length", 1000)],
            "status":    "✅ Detected",
            "source":    source,
            "vhost":     _normalise_vhost(vhost),
        }
        # FIX 7: file only on DB failure
        db_ok = False
        if self.db is not None:
            try:
                self.db.insert_audit(entry)
                db_ok = True
            except Exception as exc:
                _log.warning("DB insert failed in _record_detected: %s", exc)

        if not db_ok:
            self._file_append(entry)

        _log.info("Detected: [%s] %s — by %s (%s)", action, detail[:60], who, source)

    # ── Plugin polling (Mode 2) ────────────────────────────────────────────────

    def _check_queue_depth(self):
        """
        FIX 6: Fetch only the single audit queue instead of all queues.
        Requires rmq_client.get_queue(vhost, name).
        """
        if not self._event_queue:
            return
        try:
            max_len = self.cfg.get("event_queue_max_length", 5000)
            q       = self.rmq.get_queue("/", self._event_queue)
            if q:
                depth = q.get("messages", 0)
                pct   = depth / max_len * 100
                if pct >= 80:
                    _log.warning(
                        "Audit event queue depth HIGH: %d/%d (%.0f%%) — "
                        "consider reducing plugin_poll_interval_seconds",
                        depth, max_len, pct,
                    )
                else:
                    _log.debug("Audit queue depth: %d/%d (%.0f%%)", depth, max_len, pct)
        except Exception:
            pass

    def _schedule_plugin(self):
        if not self._active or not self._plugin_active:
            return
        t = threading.Timer(self._plugin_interval, self._run_plugin_poll)
        t.daemon = True
        t.start()

    def _run_plugin_poll(self):
        """Drain ALL pending messages from the audit event queue in one poll cycle."""
        if not self._active or not self._plugin_active:
            return
        try:
            batch_size    = self.cfg.get("event_poll_batch_size", 200)
            total_drained = 0
            while True:
                msgs = self.rmq.poll_queue("/", self._event_queue, count=batch_size) or []
                if not msgs:
                    break
                for msg in msgs:
                    self._process_event_msg(msg)
                total_drained += len(msgs)
                if len(msgs) < batch_size:
                    break
            if total_drained:
                _log.debug("Plugin poll: drained %d event(s) to DB", total_drained)
            self._check_queue_depth()
        except Exception as exc:
            _log.warning("Plugin poll error: %s", exc)
        finally:
            self._schedule_plugin()

    def _process_event_msg(self, msg: dict):
        """
        Parse a single amq.rabbitmq.event message and write to DB.

        FIX 4: Attribution — check both payload AND headers with correct priority:
          1. user_who_performed_action in payload  (most reliable, set by RabbitMQ)
          2. user_who_performed_action in headers
          3. user in payload
          4. user in headers
          5. name field (for user.* events where the subject IS the user)
        """
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
                    for t in ("longstr", "signedint", "long", "float", "bool"):
                        if t in v: return str(v[t])
                    return str(next(iter(v.values()), ""))
                return str(v)

            headers = {k: _hval(v) for k, v in raw_headers.items()}

            # FIX 4: Correct attribution priority — payload first, then headers
            def _pick(*candidates) -> str:
                for c in candidates:
                    val = str(c).strip() if c else ""
                    if val and val.lower() not in ("none", "unknown", ""):
                        return val
                return ""

            who = _pick(
                data.get("user_who_performed_action"),
                headers.get("user_who_performed_action"),
                data.get("user"),
                headers.get("user"),
            )
            # For user.* events the subject of the action may be the only name present
            if not who and routing_key.startswith("user."):
                who = _pick(data.get("name"), headers.get("name"))
            who = who or "unknown"

            # Resource name and vhost — FIX 1: normalise vhost
            name  = str(data.get("name",  headers.get("name",  ""))).strip()
            vhost = _normalise_vhost(data.get("vhost") or headers.get("vhost") or "")

            # Build detail string
            parts = []
            if name:  parts.append(f"Name: {name}")
            if vhost: parts.append(f"VHost: {vhost}")
            if "connection" in routing_key:
                peer = data.get("peer_host") or headers.get("peer_host", "")
                if peer:
                    parts.append(f"From: {peer}:{data.get('peer_port', '?')}")
            elif "permission" in routing_key:
                parts.append(
                    f"Config: {data.get('configure','')} | "
                    f"Write: {data.get('write','')} | "
                    f"Read: {data.get('read','')}"
                )
            elif routing_key == "user.tags.set":
                parts.append(f"Tags: {data.get('tags', '')}")

            action = EVENT_ACTION_MAP.get(routing_key, routing_key)
            if action in EXCLUDED_ACTIONS:
                return
            detail = " | ".join(parts) if parts else routing_key

            # Dedup
            ts_min = datetime.now().strftime("%Y-%m-%d %H:%M")
            dedup  = f"{routing_key}:{name}:{vhost}:{ts_min}"
            if dedup in self._dedup_keys:
                return
            self._dedup_keys.add(dedup)
            if len(self._dedup_keys) > self._dedup_max:
                self._dedup_keys = set(list(self._dedup_keys)[-(self._dedup_max // 2):])

            source = ("PuffinMQ"
                      if who and who == self.app_user
                      else "RabbitMQ UI / External")

            # Write to audit_log table
            entry = {
                "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "user":      who,
                "host":      self.app_host,
                "action":    action,
                "detail":    detail[:self.cfg.get("max_detail_length", 1000)],
                "status":    "✅ Success",
                "source":    source,
                "vhost":     vhost,
            }
            db_ok = False
            if self.db is not None:
                try:
                    self.db.insert_audit(entry)
                    db_ok = True
                except Exception as exc:
                    _log.warning("DB insert_audit failed in event msg: %s", exc)

            if not db_ok:
                self._file_append(entry)

            # FIX 5: Guard insert_event with null check + try/except
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
            if self.db is not None:
                try:
                    self.db.insert_event(
                        raw_event_entry,
                        raw_payload=data if self._store_raw else None,
                    )
                except Exception as exc:
                    _log.warning("DB insert_event failed in event msg: %s", exc)

            _log.info("Event: [%s] %s — by %s", action, detail[:80], who)

        except Exception as exc:
            _log.error("process_event_msg error: %s", exc)
