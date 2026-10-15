"""
app.py  —  RMQ QA Console  —  Flask web server
All DELETE/GET routes that need vhost+name use query-string params
to avoid Flask 404s caused by %2F (encoded slash) in path segments.
"""

import json
import os
import csv
import io
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
import webbrowser
from datetime import datetime
from functools import wraps
from urllib.parse import quote
from zipfile import ZipFile

from flask import Flask, jsonify, request, render_template, Response, send_file

import logger as log
from rmq_client   import RabbitMQClient
from db           import Database
from audit        import AuditEngine
from port_scanner import PortScanner

# ── Bootstrap ────────────────────────────────────────────────────────────────

def _load_config(path: str = "config.json") -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

CFG = _load_config()
log.configure(CFG.get("logging", {}))
_log = log.get_logger("app")

app = Flask(__name__)
app.secret_key = CFG["server"].get("secret_key", "change_me")

_rmq:     RabbitMQClient = None
_db:      Database       = None
_audit:   AuditEngine    = None
_scanner: PortScanner    = PortScanner(CFG)
_lock     = threading.Lock()

# Global backup progress tracker
_backup_progress = {
    "active": False,
    "queues_total": 0,
    "queues_done": 0,
    "current_queue": "",
    "messages_fetched": 0,
    "status": ""
}

# Temporary storage for backup data
# NOTE: for large queues we avoid keeping the entire backup in RAM.
_backup_data = None
_backup_disk_path = None  # folder or file path for current backup



def _apply_sort(data: list) -> list:
    """Sort a list of dicts by ?sort=<key>&sort_reverse=true, mirroring RabbitMQ Management API behaviour.

    - ``sort``         – dot-notation key path, e.g. ``name``, ``messages``, ``message_stats.publish_details.rate``
    - ``sort_reverse`` – any truthy string (``true``, ``1``, ``yes``) reverses the order
    Rows where the key is absent are placed at the end (they sort as None / smallest).
    """
    sort_key     = request.args.get("sort", "").strip()
    sort_reverse = request.args.get("sort_reverse", "").strip().lower() in ("true", "1", "yes")

    if not sort_key or not isinstance(data, list):
        return data

    def _get(row: dict, path: str):
        """Resolve a dot-notation path against a nested dict; return None if missing."""
        val = row
        for part in path.split("."):
            if not isinstance(val, dict):
                return None
            val = val.get(part)
        return val

    def _sort_key(row):
        val = _get(row, sort_key)
        # (False, val) → present values sort before absent ones
        if val is None:
            return (True, "")
        if isinstance(val, (int, float)):
            return (False, val)
        return (False, str(val).lower())

    return sorted(data, key=_sort_key, reverse=sort_reverse)


def _fetch_queue_messages_chunk(vhost: str, queue_name: str, chunk_size: int = 10000,
                                 ackmode: str = "ack_requeue_true") -> list:
    """Fetch one chunk of messages from a queue.

    ack_requeue_true = fetch without consuming (non-destructive, FAST - no manual requeue needed).
    """
    from urllib.parse import quote as _q
    msgs = _rmq.post(
        f"/queues/{_q(vhost, safe='')}/{_q(queue_name, safe='')}/get",
        {
            "count": chunk_size,
            "ackmode": ackmode,
            "encoding": "auto"
        }
    )
    if not msgs:
        return []
    if isinstance(msgs, dict):
        return [msgs]
    if isinstance(msgs, list):
        return msgs
    return []

def _drain_queue_non_destructive(vhost: str, queue_name: str,
                                  max_messages: int = 1000000,
                                  chunk_size: int = 10000) -> list:
    """Fast non-destructive backup: fetch in chunks, no manual requeue needed.

    Uses ack_requeue_true mode so queue contents should remain.

    Safety: in some broker/client combinations the Management API can return
    the same message again and again across chunks within a single backup.
    To prevent duplicating the same message N times into the backup output,
    we stop if a chunk adds zero *new* unique message signatures.
    """
    all_messages: list = []
    seen_signatures: set = set()

    def _sig(m: dict) -> str:
        # Prefer message_id when present; fall back to payload+properties.
        props = m.get("properties") or {}
        mid = None
        if isinstance(props, dict):
            mid = props.get("message_id")
        payload = m.get("payload", "")
        # payload can be big; keep signature bounded.
        if mid is not None and payload is not None:
            return f"mid={mid}|p={str(payload)[:200]}"
        return f"p={str(payload)[:200]}|rk={m.get('routing_key','')}"

    while len(all_messages) < max_messages:
        chunk = _fetch_queue_messages_chunk(
            vhost, queue_name, chunk_size=chunk_size, ackmode="ack_requeue_true"
        )
        if not chunk:
            break

        new_added = 0
        for m in chunk:
            s = _sig(m if isinstance(m, dict) else {})
            if s in seen_signatures:
                continue
            seen_signatures.add(s)
            all_messages.append(m)
            new_added += 1

            if len(all_messages) >= max_messages:
                break

        _backup_progress["messages_fetched"] = len(all_messages)

        # If we didn't get any new unique messages from the next chunk,
        # the API is likely repeating the same items; stop to avoid
        # duplicating them in the backup file.
        if new_added == 0:
            break

    return all_messages[:max_messages]




def _get_db() -> Database:
    """Return DB instance, or None if DB is not available."""
    global _db
    if _db is None:
        try:
            _db = Database(CFG["database"])
        except Exception as exc:
            _log.warning("DB not available (audit will use local JSON file): %s", exc)
            return None
    return _db


# ── Decorators ────────────────────────────────────────────────────────────────

def require_connection(f):
    @wraps(f)
    def wrapper(*args, **kwargs):
        if _rmq is None or not _rmq.connected:
            return jsonify({"error": "Not connected to RabbitMQ"}), 503
        return f(*args, **kwargs)
    return wrapper


def api_error(exc: Exception, status: int = 500):
    msg = str(exc)
    _log.error("API error: %s", msg)
    return jsonify({"error": msg}), status


# ── Index ─────────────────────────────────────────────────────────────────────

@app.route("/")
def index():
    rmq_cfg = CFG.get("rabbitmq", {})
    return render_template("index.html",
                           title=CFG["ui"].get("page_title", "PuffinMQ"),
                           theme=CFG["ui"].get("theme", "dark"),
                           rmq_host=rmq_cfg.get("host", "localhost"),
                           rmq_port=rmq_cfg.get("port", 15672))


# ── Connection ────────────────────────────────────────────────────────────────

@app.route("/api/connect", methods=["POST"])
def api_connect():
    global _rmq, _audit
    body = request.get_json(force=True)
    c = dict(CFG)
    c["rabbitmq"] = dict(CFG.get("rabbitmq", {}))
    for key in ("host", "port", "username", "password", "vhost"):
        if body.get(key):
            c["rabbitmq"][key] = body[key]

    # ── Step 1: RabbitMQ connection ─────────────────────────────────────────
    # This is the part the UI actually depends on. If this succeeds, the
    # user is "connected" as far as the app is concerned — everything below
    # (audit engine, port scanner) is best-effort and must NOT be able to
    # fail this request.
    try:
        if _audit:
            try:
                _audit.stop()
            except Exception as exc:
                _log.warning("Error stopping previous audit engine: %s", exc)
        client   = RabbitMQClient(c)
        overview = client.connect()
        with _lock:
            _rmq = client
    except Exception as exc:
        return api_error(exc, 400)

    host = f"{c['rabbitmq']['host']}:{c['rabbitmq']['port']}"
    user = c["rabbitmq"]["username"]
    _log.info("Connected: user=%s host=%s", user, host)

    # ── Step 2: Audit engine (best-effort, non-fatal) ───────────────────────
    audit_warning = None
    try:
        db = _get_db()
        _audit = AuditEngine(_rmq, db, CFG, app_user=user, app_host=host)
        _audit.start()
        # NOTE: Connect/Disconnect are app-session events, not a user action
        # against a RabbitMQ resource — they're intentionally not written to
        # the audit log (see EXCLUDED_ACTIONS in audit.py). Logged here instead.
    except Exception as exc:
        _audit = None
        audit_warning = str(exc)
        _log.error("Audit engine failed to start (RabbitMQ connection still OK): %s", exc)

    # ── Step 3: Port scanner (best-effort, non-fatal) ───────────────────────
    if CFG["ports"].get("scan_on_connect", True):
        try:
            threading.Thread(target=_scanner.start_background,
                             args=(c["rabbitmq"]["host"],), daemon=True).start()
        except Exception as exc:
            _log.warning("Port scanner failed to start: %s", exc)

    resp = {"ok": True, "overview": overview, "host": host, "user": user}
    if audit_warning:
        resp["warning"] = f"Connected to RabbitMQ, but audit logging is unavailable: {audit_warning}"
    return jsonify(resp)


@app.route("/api/disconnect", methods=["POST"])
def api_disconnect():
    global _rmq, _audit
    try:
        if _audit:
            _log.info("Disconnecting: user=%s", _rmq.username if _rmq else "?")
            _audit.stop()
            _audit = None
        if _rmq:
            _rmq.disconnect()
            _rmq = None
        _scanner.stop()
        return jsonify({"ok": True})
    except Exception as exc:
        return api_error(exc)


@app.route("/api/status")
def api_status():
    db_ok = False
    try:
        db = _get_db()
        if db is not None:
            db_ok = db.ping()
    except Exception:
        pass
    return jsonify({
        "connected": _rmq is not None and _rmq.connected,
        "host":      f"{_rmq.host}:{_rmq.port}" if _rmq else None,
        "user":      _rmq.username if _rmq else None,
        "db_ok":     db_ok,
    })


# ── Overview ──────────────────────────────────────────────────────────────────

@app.route("/api/overview")
@require_connection
def api_overview():
    try:
        return jsonify(_rmq.overview())
    except Exception as exc:
        return api_error(exc)


@app.route("/api/nodes")
@require_connection
def api_nodes():
    try:
        return jsonify(_apply_sort(_rmq.nodes()))
    except Exception as exc:
        return api_error(exc)


# ── Queues ────────────────────────────────────────────────────────────────────
# All queue routes that need vhost+name use ?vhost=...&name=... query params
# to avoid Flask 404 caused by %2F in URL path segments.

@app.route("/api/queues", methods=["GET"])
@require_connection
def api_queues():
    try:
        return jsonify(_apply_sort(_rmq.queues()))
    except Exception as exc:
        return api_error(exc)


@app.route("/api/queues", methods=["POST"])
@require_connection
def api_create_queue():
    body  = request.get_json(force=True)
    vhost = body.pop("vhost", "/")
    name  = body.pop("name", "")
    try:
        _rmq.create_queue(name, vhost, body)
        _audit and _audit.record("Create Queue", f"Name: {name} | VHost: {vhost}")
        return jsonify({"ok": True})
    except Exception as exc:
        return api_error(exc)


@app.route("/api/queues/delete", methods=["DELETE"])
@require_connection
def api_delete_queue():
    # ?vhost=/&name=my-queue
    vhost = request.args.get("vhost", "/")
    name  = request.args.get("name",  "")
    try:
        _rmq.delete_queue(name, vhost)
        _audit and _audit.record("Delete Queue", f"Name: {name} | VHost: {vhost}")
        return jsonify({"ok": True})
    except Exception as exc:
        return api_error(exc)


@app.route("/api/queues/purge", methods=["DELETE"])
@require_connection
def api_purge_queue():
    # ?vhost=/&name=my-queue
    vhost = request.args.get("vhost", "/")
    name  = request.args.get("name",  "")
    try:
        _rmq.purge_queue(name, vhost)
        _audit and _audit.record("Purge Queue", f"Name: {name} | VHost: {vhost}")
        return jsonify({"ok": True})
    except Exception as exc:
        return api_error(exc)


@app.route("/api/queues/messages", methods=["GET"])
@require_connection
def api_get_messages():
    # ?vhost=/&name=my-queue&count=10
    vhost    = request.args.get("vhost", "/")
    name     = request.args.get("name",  "")
    count    = int(request.args.get("count", 10))
    ack_mode = request.args.get("ack_mode", "ack_requeue_true")
    try:
        msgs = _rmq.get_messages(name, vhost, count, ack_mode)
        return jsonify(msgs)
    except Exception as exc:
        return api_error(exc)


@app.route("/api/queues/publish", methods=["POST"])
@require_connection
def api_publish_message():
    # ?vhost=/&name=my-queue
    vhost = request.args.get("vhost", "/")
    name  = request.args.get("name",  "")
    body  = request.get_json(force=True)
    try:
        from urllib.parse import quote as _q
        payload = {
            "properties":       {},
            "routing_key":      name,
            "payload":          body.get("payload", ""),
            "payload_encoding": "string",
        }
        _rmq.post(f"/exchanges/{_q(vhost, safe='')}//publish", payload)
        _audit and _audit.record("Publish Message", f"Queue: {name} | VHost: {vhost}")
        return jsonify({"ok": True})
    except Exception as exc:
        return api_error(exc)


@app.route("/api/queues/bulk-delete", methods=["POST"])
@require_connection
def api_bulk_delete_queues():
    body   = request.get_json(force=True)
    queues = body.get("queues", [])
    errors = []
    for q in queues:
        try:
            _rmq.delete_queue(q["name"], q["vhost"])
            _audit and _audit.record("Delete Queue", f"Name: {q['name']} | VHost: {q['vhost']}")
        except Exception as exc:
            errors.append({"queue": q["name"], "error": str(exc)})
    return jsonify({"ok": True, "deleted": len(queues) - len(errors), "errors": errors})


@app.route("/api/queues/bulk-purge", methods=["POST"])
@require_connection
def api_bulk_purge_queues():
    body   = request.get_json(force=True)
    queues = body.get("queues", [])
    errors = []
    for q in queues:
        try:
            _rmq.purge_queue(q["name"], q["vhost"])
            _audit and _audit.record("Purge Queue", f"Name: {q['name']} | VHost: {q['vhost']}")
        except Exception as exc:
            errors.append({"queue": q["name"], "error": str(exc)})
    return jsonify({"ok": True, "purged": len(queues) - len(errors), "errors": errors})


# ── Message Backup & Restore ──────────────────────────────────────────────────

@app.route("/api/queues/backup-progress", methods=["GET"])
def api_backup_progress():
    """Get current backup/restore progress."""
    global _backup_progress
    return jsonify(_backup_progress)



@app.route("/api/queues/backup", methods=["POST"])
@require_connection
def api_backup_messages():

    """Export messages from queue as downloadable file (JSON/CSV).

    Fix: this endpoint must not cap exports at 10,000 messages.
    It now fetches in chunks (non-destructive via ack_requeue_true)
    until the queue is empty or the requested `count` is reached.
    """
    vhost = request.args.get("vhost", "/")
    name  = request.args.get("name",  "")
    count = int(request.args.get("count", 10000))
    fmt   = request.args.get("format", "json")

    try:
        # Chunked non-destructive drain.
        # If count is huge, caller can cap via `count` query param.
        msgs = _drain_queue_non_destructive(
            vhost=vhost,
            queue_name=name,
            max_messages=count,
            chunk_size=int(CFG.get("audit", {}).get("backup_chunk_size", 10000)),
        )

        if not msgs:
            return jsonify({"error": "No messages found in queue"}), 400

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        safe_name = name.replace("/", "_")
        safe_vhost = vhost.replace("/", "_") if vhost != "/" else "root"
        filename = f"{safe_vhost}_{safe_name}_{timestamp}.{fmt}"

        # IMPORTANT: do not modify/normalize the message objects themselves.
        # Export exactly as fetched from RabbitMQ GET /queues/.../get.
        # (No export-time dedupe; duplicates must be investigated upstream.)
        exported = msgs

        # Only export the message payload itself (as "original msg")
        # to match what is already available in the queue (decoded JSON object).
        # If payload is not JSON, keep it as-is.
        def _maybe_decode_payload(p):
            if isinstance(p, str):
                s = p.strip()
                if s.startswith("{") and s.endswith("}"):
                    try:
                        return json.loads(s)
                    except Exception:
                        return p
            return p

        payload_only = [_maybe_decode_payload(m.get("payload", "")) for m in exported]

        # Filter out empty payloads (empty strings or None) to avoid
        # spurious entries like "" or null in the backup file.
        payload_only = [p for p in payload_only if p is not None and p != ""]

        # Use compact JSON separators (no spaces) to mirror the exact
        # format that RabbitMQ stores messages in the queue.
        _compact = {"ensure_ascii": False, "separators": (",", ":")}

        if fmt == "csv":
            output = io.StringIO()
            writer = csv.DictWriter(output, fieldnames=["payload"])
            writer.writeheader()
            for p in payload_only:
                writer.writerow({"payload": json.dumps(p, **_compact) if isinstance(p, (dict, list)) else (p or "")})
            content = output.getvalue()
            mimetype = "text/csv"
        elif len(payload_only) == 1:
            # Single message → output just the object (no array wrapper)
            content = json.dumps(payload_only[0], **_compact)
            mimetype = "application/json"
        else:
            # Multiple messages → compact array
            content = json.dumps(payload_only, **_compact)
            mimetype = "application/json"

        _audit and _audit.record(
            "Backup Queue",
            f"Queue: {name} | VHost: {vhost} | Count: {len(msgs)} | Format: {fmt}",
        )

        return Response(

            content,
            mimetype=mimetype,
            headers={"Content-Disposition": f"attachment; filename={filename}"}
        )


    except Exception as exc:
        _log.error("Backup error: %s", exc)
        return api_error(exc)


def _sanitize_amqp_properties(properties):
    """
    Clean up AMQP message properties as returned by RabbitMQ's
    GET /queues/<vhost>/<name>/get endpoint, so they can be safely
    re-submitted to POST /exchanges/.../publish.

    Why this is needed: when fetching messages (for backup/restore), RabbitMQ's
    management API can return header values wrapped in AMQP wire-format type
    markers for non-string types, e.g.:
        "headers": {"x-retry-count": {"signedint": 3}}
    Sending that wrapped shape straight back to the publish endpoint causes
    RabbitMQ's management plugin to throw an unhandled exception server-side —
    a 500 Internal Server Error — instead of a clean validation error. This
    unwraps those values back to plain JSON types so publish succeeds.
    """
    if not isinstance(properties, dict):
        return {}

    allowed_keys = {
        "content_type", "content_encoding", "headers", "delivery_mode",
        "priority", "correlation_id", "reply_to", "expiration", "message_id",
        "timestamp", "type", "user_id", "app_id", "cluster_id",
    }
    clean = {k: v for k, v in properties.items() if k in allowed_keys and v is not None}

    def _unwrap(v):
        if isinstance(v, dict):
            for t in ("longstr", "signedint", "long", "short", "byte",
                      "float", "double", "bool", "decimal", "timestamp"):
                if t in v:
                    return v[t]
            # Unknown/nested wrapper shape — drop rather than risk another 500
            return None
        return v

    headers = clean.get("headers")
    if isinstance(headers, dict):
        safe_headers = {}
        for hk, hv in headers.items():
            uv = _unwrap(hv)
            if uv is not None:
                safe_headers[hk] = uv
        clean["headers"] = safe_headers
    elif "headers" in clean:
        clean.pop("headers", None)

    # RabbitMQ/AMQP requires "expiration" to be a string, not a number
    if "expiration" in clean:
        clean["expiration"] = str(clean["expiration"])

    return clean


def _publish_with_property_fallback(vhost, queue_name, payload_text, payload_enc, properties, _q):
    """
    Publish one message via the default exchange, with a safety-net retry:
    if RabbitMQ's management API still 500s even after _sanitize_amqp_properties
    (e.g. an unanticipated header shape), retry once with headers stripped
    entirely rather than losing the whole message.
    """
    # Preserve the original payload_encoding from the backup (e.g. "base64" for
    # binary messages). Hardcoding "string" here corrupts any non-text payload.
    payload_enc = properties.pop("payload_encoding", "string") if isinstance(properties, dict) else "string"
    publish_payload = {
        "properties": properties,
        "routing_key": queue_name,
        "payload": payload_text,
        "payload_encoding": payload_enc,
    }
    try:
        return _rmq.post(
            f"/exchanges/{_q(vhost, safe='')}/{_q('amq.default', safe='')}/publish",
            publish_payload,
        )
    except Exception:
        if properties.get("headers"):
            stripped = dict(properties)
            stripped.pop("headers", None)
            publish_payload["properties"] = stripped
            return _rmq.post(
                f"/exchanges/{_q(vhost, safe='')}/{_q('amq.default', safe='')}/publish",
                publish_payload,
            )


@app.route("/api/queues/restore", methods=["POST"])
@require_connection
def api_restore_messages():
    """Restore (re-publish) messages from uploaded backup file.

    Runs as async job and exposes progress via /api/queues/restore-progress.
    """
    from urllib.parse import quote as _q

    global _backup_progress

    vhost = request.args.get("vhost", "/")
    queue_name = request.args.get("name", "")

    try:
        if "file" not in request.files:
            return jsonify({"error": "No file provided"}), 400

        file = request.files["file"]
        if not file.filename:
            return jsonify({"error": "Empty filename"}), 400

        content = file.read().decode("utf-8")
        msgs_to_restore = []

        if file.filename.endswith(".json"):
            msgs_to_restore = json.loads(content)
        elif file.filename.endswith(".csv"):
            reader = csv.DictReader(io.StringIO(content))
            for row in reader:
                msgs_to_restore.append({
                    "payload": row.get("payload", ""),
                    "properties": json.loads(row.get("properties", "{}")),
                    "routing_key": row.get("routing_key", ""),
                })
        else:
            return jsonify({"error": "Unsupported file format. Use .json or .csv"}), 400

        if not msgs_to_restore:
            return jsonify({"error": "No messages in backup file"}), 400

        # Start async worker
        job_messages = msgs_to_restore
        total = len(job_messages)

        _backup_progress["active"] = True
        _backup_progress["queues_total"] = 1
        _backup_progress["queues_done"] = 0
        _backup_progress["current_queue"] = queue_name
        _backup_progress["messages_fetched"] = 0
        _backup_progress["status"] = "Action is under process, please wait (restore started)..."

        def restore_worker():
            global _backup_progress
            published_count = 0
            errors = []
            
            # Publish messages in batches of 50 for speed without overwhelming the API
            batch_size = 50
            for batch_start in range(0, total, batch_size):
                batch_end = min(batch_start + batch_size, total)
                batch = job_messages[batch_start:batch_end]
                
                for msg in batch:
                    try:
                        payload_text = msg.get("payload", "")
                        payload_enc  = msg.get("payload_encoding", "string")
                        # IMPORTANT: always route by the target queue name, not the
                        # message's original routing_key.
                        properties = _sanitize_amqp_properties(msg.get("properties", {}))

                        result = _publish_with_property_fallback(
                            vhost, queue_name, payload_text, payload_enc, properties, _q
                        )
                        # RabbitMQ's publish response includes "routed": true/false —
                        # a 200 OK only means the API accepted the request, not that
                        # the message actually landed in a queue. Treat routed=false
                        # as a failure so it shows up in errors instead of silently
                        # inflating the success count.
                        if isinstance(result, dict) and result.get("routed") is False:
                            raise Exception(
                                f"Not routed to any queue (routing_key '{queue_name}' "
                                f"had no matching queue/binding)"
                            )
                        published_count += 1
                    except Exception as e:
                        errors.append({"msg_idx": batch_start + batch.index(msg), "error": str(e)})
                        _log.warning("Restore publish failed for %s: %s", queue_name, e)
                
                # Update progress after each batch
                _backup_progress["messages_fetched"] = published_count
                _backup_progress["status"] = (
                    f"Action is under process, please wait... "
                    f"Restoring {queue_name}: {published_count}/{total}"
                )

            _backup_progress["queues_done"] = 1
            _backup_progress["active"] = False
            _backup_progress["errors"] = errors if errors else None
            _backup_progress["status"] = (
                f"Restore complete for {queue_name}. Restored {published_count}/{total}."
                + (f" {len(errors)} failed." if errors else "")
            )

            _audit and _audit.record(
                "Restore Queue",
                f"Queue: {queue_name} | VHost: {vhost} | Restored: {published_count} | Total: {total}",
            )

        threading.Thread(target=restore_worker, daemon=True).start()

        return jsonify({
            "ok": True,
            "message": "Restore started",
            "status_endpoint": "/api/queues/backup-progress",
            "total": total,
        })

    except Exception as exc:
        return api_error(exc)



@app.route("/api/queues/backup-multiple", methods=["POST"])
@require_connection
def api_backup_multiple_queues():
    """Start backup of multiple queues. Returns immediately with task ID. Uses progress endpoint to check status."""
    global _backup_progress, _backup_data
    
    body = request.get_json(force=True)
    queues = body.get("queues", [])
    
    if not queues:
        return jsonify({"error": "No queues provided"}), 400
    
    # Start backup in background thread
    def backup_worker():
        global _backup_progress, _backup_data
        from urllib.parse import quote as _q
        from zipfile import ZipFile
        
        try:
            _backup_progress["active"] = True
            _backup_progress["queues_total"] = len(queues)
            _backup_progress["queues_done"] = 0
            _backup_progress["status"] = "Starting backup..."
            _backup_progress["messages_fetched"] = 0
            
            backups = {}
            total_messages = 0
            
            for idx, q in enumerate(queues):
                vhost = q.get("vhost", "/")
                name = q.get("name", "")
                queue_key = f"{vhost}||{name}"  # Use vhost+name as key to avoid collisions
                
                _backup_progress["current_queue"] = name
                _backup_progress["status"] = f"Fetching messages from {name}..."
                
                try:
                    # Non-destructive drain + immediate requeue
                    msgs = _drain_queue_non_destructive(vhost, name)
                    backups[queue_key] = msgs

                    total_messages += len(msgs)
                    _backup_progress["messages_fetched"] = total_messages
                    _backup_progress["status"] = f"Fetched {len(msgs)} messages from {name}"
                    
                    _log.info(f"Backed up queue {name}: {len(msgs)} messages")
                except Exception as exc:
                    _log.warning(f"Failed to backup queue {name}: {exc}")
                    backups[queue_key] = []
                    _backup_progress["status"] = f"Error backing up {name}: {str(exc)}"
                
                _backup_progress["queues_done"] = idx + 1
            
            # Create backup data (JSON or ZIP)
            if len(queues) == 1:
                q = queues[0]
                queue_key = f"{q.get('vhost', '/')}||{q.get('name', '')}"
                msgs = backups[queue_key]
                _backup_data = {
                    "type": "json",
                    "filename": f"{q['name'].replace('/', '_')}.json",
                    "content": json.dumps(msgs, ensure_ascii=False, separators=(",", ":")),
                    "mimetype": "application/json"
                }
            else:
                # Create ZIP file in memory
                zip_buffer = io.BytesIO()
                with ZipFile(zip_buffer, 'w') as zf:
                    for q in queues:
                        queue_key = f"{q.get('vhost', '/')}||{q.get('name', '')}"
                        msgs = backups[queue_key]
                        content = json.dumps(msgs, ensure_ascii=False, separators=(",", ":"))
                        # Use consistent naming format: {safe_vhost}_{safe_name}.json
                        safe_vhost = q.get('vhost', '/').replace("/", "_") if q.get('vhost', '/') != "/" else "root"
                        safe_name = q.get('name', '').replace("/", "_")
                        filename = f"{safe_vhost}_{safe_name}.json"
                        zf.writestr(filename, content)
                
                zip_buffer.seek(0)
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                _backup_data = {
                    "type": "zip",
                    "filename": f"backup_{timestamp}.zip",
                    "content": zip_buffer.getvalue(),
                    "mimetype": "application/zip"
                }
            
            # Record audit
            _audit and _audit.record("Backup Queues", f"Count: {len(queues)} | Total Messages: {total_messages} | Queues: {', '.join([q['name'] for q in queues])}")
            
            _backup_progress["status"] = f"Backup complete! {total_messages} messages ready for download"
            _backup_progress["active"] = False
            
        except Exception as exc:
            _log.error(f"Backup worker error: {exc}")
            _backup_progress["status"] = f"Backup failed: {str(exc)}"
            _backup_progress["active"] = False
    
    # Start thread
    thread = threading.Thread(target=backup_worker, daemon=True)
    thread.start()
    
    return jsonify({
        "ok": True,
        "message": "Backup started",
        "queues": len(queues),
        "status_endpoint": "/api/queues/backup-progress"
    })


@app.route("/api/queues/backup-download", methods=["GET"])
def api_backup_download():
    """Download the backup file that was created."""
    global _backup_data
    
    if _backup_data is None:
        return jsonify({"error": "No backup data available. Start a backup first."}), 400
    
    if _backup_progress["active"]:
        return jsonify({"error": "Backup is still in progress. Check /api/queues/backup-progress"}), 400
    
    return Response(
        _backup_data["content"],
        mimetype=_backup_data["mimetype"],
        headers={"Content-Disposition": f"attachment; filename={_backup_data['filename']}"}
    )


@app.route("/api/queues/restore-from-data", methods=["POST"])
@require_connection
def api_restore_from_data():
    """Restore messages from JSON data array.

    Runs as an async background job and reports progress via
    /api/queues/backup-progress — same pattern as the file-upload restore
    endpoint. This was previously synchronous, which meant large message
    counts (100s+) routinely exceeded the frontend's 60s fetch timeout,
    causing the request to be aborted client-side with no feedback even
    though the backend might still have been publishing in the background.
    """
    from urllib.parse import quote as _q

    global _backup_progress

    body = request.get_json(force=True)
    vhost = body.get("vhost", "/")
    queue_name = body.get("name", "")
    messages = body.get("messages", [])

    if not messages:
        return jsonify({"error": "No messages provided"}), 400

    job_messages = messages
    total = len(job_messages)

    _backup_progress["active"] = True
    _backup_progress["queues_total"] = 1
    _backup_progress["queues_done"] = 0
    _backup_progress["current_queue"] = queue_name
    _backup_progress["messages_fetched"] = 0
    _backup_progress["published"] = 0
    _backup_progress["total"] = total
    _backup_progress["errors"] = None
    _backup_progress["status"] = "Action is under process, please wait (restore started)..."

    def restore_from_data_worker():
        global _backup_progress
        published_count = 0
        errors = []
        progress_lock = threading.Lock()

        def _publish_one(msg):
            payload_text = msg.get("payload", "")
            payload_enc  = msg.get("payload_encoding", "string")
            properties = _sanitize_amqp_properties(msg.get("properties", {}))

            result = _publish_with_property_fallback(
                vhost, queue_name, payload_text, payload_enc, properties, _q
            )
            if isinstance(result, dict) and result.get("routed") is False:
                raise Exception(
                    f"Not routed to any queue (routing_key '{queue_name}' "
                    f"had no matching queue/binding)"
                )

        # Publish concurrently — these are independent HTTP round-trips to the
        # RabbitMQ Management API. Now that the actual failure cause (malformed
        # AMQP header values triggering RabbitMQ 500s) is fixed, concurrency is
        # restored to a healthy level for speed. If you still see errors under
        # load, lowering this is a reasonable knob to turn.
        max_workers = min(15, max(1, total))
        with ThreadPoolExecutor(max_workers=max_workers) as pool:
            future_to_msg = {pool.submit(_publish_one, msg): msg for msg in job_messages}
            for future in as_completed(future_to_msg):
                msg = future_to_msg[future]
                try:
                    future.result()
                    with progress_lock:
                        published_count += 1
                except Exception as exc:
                    with progress_lock:
                        errors.append({"msg": str(msg.get("payload", ""))[:50], "error": str(exc)})
                    _log.warning("Restore publish failed for %s: %s", queue_name, exc)

                with progress_lock:
                    _backup_progress["messages_fetched"] = published_count
                    _backup_progress["published"] = published_count
                    _backup_progress["status"] = (
                        f"Action is under process, please wait... "
                        f"Restoring {queue_name}: {published_count}/{total}"
                    )

        _backup_progress["queues_done"] = 1
        _backup_progress["active"] = False
        _backup_progress["errors"] = errors if errors else None
        _backup_progress["status"] = (
            f"Restore complete for {queue_name}. Restored {published_count}/{total}."
        )

        _audit and _audit.record(
            "Restore Queue",
            f"Queue: {queue_name} | VHost: {vhost} | Restored: {published_count} | Total: {total}",
        )

    threading.Thread(target=restore_from_data_worker, daemon=True).start()

    return jsonify({
        "ok": True,
        "message": "Restore started",
        "total": total,
    })


# ── Exchanges ─────────────────────────────────────────────────────────────────

@app.route("/api/exchanges", methods=["GET"])
@require_connection
def api_exchanges():
    try:
        return jsonify(_apply_sort(_rmq.exchanges()))
    except Exception as exc:
        return api_error(exc)


@app.route("/api/exchanges", methods=["POST"])
@require_connection
def api_create_exchange():
    body  = request.get_json(force=True)
    vhost = body.pop("vhost", "/")
    name  = body.pop("name", "")
    try:
        _rmq.create_exchange(name, vhost, body)
        _audit and _audit.record("Create Exchange",
                                 f"Name: {name} | Type: {body.get('type')} | VHost: {vhost}")
        return jsonify({"ok": True})
    except Exception as exc:
        return api_error(exc)


@app.route("/api/exchanges/delete", methods=["DELETE"])
@require_connection
def api_delete_exchange():
    # ?vhost=/&name=my-exchange
    vhost = request.args.get("vhost", "/")
    name  = request.args.get("name",  "")
    try:
        _rmq.delete_exchange(name, vhost)
        _audit and _audit.record("Delete Exchange", f"Name: {name} | VHost: {vhost}")
        return jsonify({"ok": True})
    except Exception as exc:
        return api_error(exc)


# ── Bindings ──────────────────────────────────────────────────────────────────

@app.route("/api/bindings", methods=["GET"])
@require_connection
def api_bindings():
    try:
        return jsonify(_apply_sort(_rmq.bindings()))
    except Exception as exc:
        return api_error(exc)


@app.route("/api/bindings", methods=["POST"])
@require_connection
def api_create_binding():
    body = request.get_json(force=True)
    try:
        _rmq.create_binding(
            body["vhost"], body["exchange"], body["queue"],
            {"routing_key": body.get("routing_key", ""),
             "arguments":   body.get("arguments", {})})
        _audit and _audit.record("Create Binding",
            f"Exchange: {body['exchange']} → Queue: {body['queue']} | "
            f"Key: {body.get('routing_key','(none)')}")
        return jsonify({"ok": True})
    except Exception as exc:
        return api_error(exc)


# ── Connections ───────────────────────────────────────────────────────────────

@app.route("/api/connections", methods=["GET"])
@require_connection
def api_connections():
    try:
        return jsonify(_apply_sort(_rmq.connections()))
    except Exception as exc:
        return api_error(exc)


@app.route("/api/connections/close", methods=["DELETE"])
@require_connection
def api_close_connection():
    # ?name=127.0.0.1%3A12345
    name   = request.args.get("name", "")
    reason = request.args.get("reason", "Closed by admin")
    try:
        _rmq.close_connection(name, reason)
        _audit and _audit.record("Close Connection", f"Conn: {name} | Reason: {reason}")
        return jsonify({"ok": True})
    except Exception as exc:
        return api_error(exc)


# ── Channels ──────────────────────────────────────────────────────────────────

@app.route("/api/channels", methods=["GET"])
@require_connection
def api_channels():
    try:
        return jsonify(_apply_sort(_rmq.channels()))
    except Exception as exc:
        return api_error(exc)


# ── Users ─────────────────────────────────────────────────────────────────────

@app.route("/api/users", methods=["GET"])
@require_connection
def api_users():
    try:
        return jsonify(_apply_sort(_rmq.users()))
    except Exception as exc:
        return api_error(exc)


@app.route("/api/users", methods=["POST"])
@require_connection
def api_create_user():
    body     = request.get_json(force=True)
    username = body.get("username", "")
    try:
        _rmq.create_user(username, {
            "password": body.get("password", ""),
            "tags":     body.get("tags", ""),
        })
        for perm in body.get("permissions", []):
            _rmq.set_permissions(perm["vhost"], username, {
                "configure": perm.get("configure", ".*"),
                "write":     perm.get("write",     ".*"),
                "read":      perm.get("read",      ".*"),
            })
        _audit and _audit.record("Create User",
                                 f"User: {username} | Tags: {body.get('tags','(none)')}")
        return jsonify({"ok": True})
    except Exception as exc:
        return api_error(exc)


@app.route("/api/users/delete", methods=["DELETE"])
@require_connection
def api_delete_user():
    # ?name=myuser
    username = request.args.get("name", "")
    try:
        _rmq.delete_user(username)
        _audit and _audit.record("Delete User", f"User: {username}")
        return jsonify({"ok": True})
    except Exception as exc:
        return api_error(exc)


@app.route("/api/permissions", methods=["GET"])
@require_connection
def api_permissions():
    try:
        return jsonify(_apply_sort(_rmq.permissions()))
    except Exception as exc:
        return api_error(exc)


@app.route("/api/whoami", methods=["GET"])
@require_connection
def api_whoami():
    try:
        return jsonify(_rmq.whoami())
    except Exception as exc:
        return api_error(exc)


# ── Virtual Hosts ─────────────────────────────────────────────────────────────

@app.route("/api/vhosts", methods=["GET"])
@require_connection
def api_vhosts():
    try:
        return jsonify(_apply_sort(_rmq.vhosts()))
    except Exception as exc:
        return api_error(exc)


@app.route("/api/vhosts", methods=["POST"])
@require_connection
def api_create_vhost():
    body = request.get_json(force=True)
    name = body.get("name", "")
    try:
        _rmq.create_vhost(name)
        try:
            me = _rmq.whoami().get("name", "")
            if me:
                _rmq.set_permissions(name, me,
                    {"configure": ".*", "write": ".*", "read": ".*"})
        except Exception:
            pass
        _audit and _audit.record("Create VHost", f"VHost: {name}")
        return jsonify({"ok": True})
    except Exception as exc:
        return api_error(exc)


@app.route("/api/vhosts/delete", methods=["DELETE"])
@require_connection
def api_delete_vhost():
    # ?name=my-vhost
    name = request.args.get("name", "")
    try:
        _rmq.delete_vhost(name)
        _audit and _audit.record("Delete VHost", f"VHost: {name}")
        return jsonify({"ok": True})
    except Exception as exc:
        return api_error(exc)


# ── Policies ──────────────────────────────────────────────────────────────────

@app.route("/api/policies", methods=["GET"])
@require_connection
def api_policies():
    try:
        return jsonify(_apply_sort(_rmq.policies()))
    except Exception as exc:
        return api_error(exc)


@app.route("/api/policies", methods=["POST"])
@require_connection
def api_create_policy():
    body  = request.get_json(force=True)
    vhost = body.pop("vhost", "/")
    name  = body.pop("name", "")
    try:
        _rmq.create_policy(vhost, name, body)
        _audit and _audit.record("Create Policy",
            f"Name: {name} | VHost: {vhost} | Pattern: {body.get('pattern','')}")
        return jsonify({"ok": True})
    except Exception as exc:
        return api_error(exc)


@app.route("/api/policies/delete", methods=["DELETE"])
@require_connection
def api_delete_policy():
    # ?vhost=/&name=my-policy
    vhost = request.args.get("vhost", "/")
    name  = request.args.get("name",  "")
    try:
        _rmq.delete_policy(vhost, name)
        _audit and _audit.record("Delete Policy", f"Name: {name} | VHost: {vhost}")
        return jsonify({"ok": True})
    except Exception as exc:
        return api_error(exc)


# ── Feature Flags ─────────────────────────────────────────────────────────────

@app.route("/api/feature-flags", methods=["GET"])
@require_connection
def api_feature_flags():
    try:
        return jsonify(_rmq.feature_flags())
    except Exception as exc:
        return api_error(exc)


# ── Shovels ───────────────────────────────────────────────────────────────────

@app.route("/api/shovels", methods=["GET"])
@require_connection
def api_shovels():
    try:
        return jsonify(_rmq.shovels())
    except Exception as exc:
        return api_error(exc)


@app.route("/api/shovels", methods=["POST"])
@require_connection
def api_create_shovel():
    body  = request.get_json(force=True)
    vhost = body.get("vhost", "/")
    name  = body.get("name",  "")
    try:
        _rmq.create_shovel(vhost, name, {"value": body.get("value", {})})
        _audit and _audit.record("Create Shovel", f"Name: {name} | VHost: {vhost}")
        return jsonify({"ok": True})
    except Exception as exc:
        return api_error(exc)


# ── Definitions ───────────────────────────────────────────────────────────────

@app.route("/api/definitions", methods=["GET"])
@require_connection
def api_definitions():
    try:
        return jsonify(_rmq.definitions())
    except Exception as exc:
        return api_error(exc)


@app.route("/api/definitions", methods=["POST"])
@require_connection
def api_upload_definitions():
    try:
        _rmq.upload_definitions(request.get_json(force=True))
        _audit and _audit.record("Upload Definitions", "Definitions uploaded")
        return jsonify({"ok": True})
    except Exception as exc:
        return api_error(exc)


@app.route("/api/definitions/export", methods=["GET"])
@require_connection
def api_export_definitions():
    try:
        data = _rmq.definitions()
        return Response(
            json.dumps(data, indent=2),
            mimetype="application/json",
            headers={"Content-Disposition":
                f'attachment; filename="rabbitmq-definitions-{datetime.now():%Y%m%d-%H%M%S}.json"'})
    except Exception as exc:
        return api_error(exc)


# ── Port Scanner ──────────────────────────────────────────────────────────────

@app.route("/api/ports", methods=["GET"])
def api_ports():
    return jsonify(_scanner.latest())


@app.route("/api/ports/scan", methods=["POST"])
def api_scan_ports():
    host = (_rmq.host if _rmq else
            request.get_json(force=True).get("host", "localhost"))
    try:
        return jsonify(_scanner.scan(host))
    except Exception as exc:
        return api_error(exc)


# ── Audit Log ─────────────────────────────────────────────────────────────────

# ── Audit helpers — JSON file fallback when DB not available ─────────────────
_AUDIT_FILE = "rmq_audit_log.json"

def _audit_file_load() -> list:
    """Load audit entries from local JSON fallback file."""
    try:
        if os.path.exists(_AUDIT_FILE):
            with open(_AUDIT_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
    except Exception as exc:
        _log.warning("audit file load failed: %s", exc)
    return []

def _audit_file_save(entries: list) -> None:
    """Save audit entries to local JSON fallback file."""
    try:
        with open(_AUDIT_FILE, "w", encoding="utf-8") as f:
            json.dump(entries, f, indent=2, default=str)
    except Exception as exc:
        _log.warning("audit file save failed: %s", exc)

def _audit_file_append(entry: dict) -> None:
    """Append one entry to the local JSON fallback file."""
    entries = _audit_file_load()
    entries.append(entry)
    # Keep last 5000 entries in file
    if len(entries) > 5000:
        entries = entries[-5000:]
    _audit_file_save(entries)

def _filter_entries(entries: list, user=None, action=None, source=None,
                    status=None, from_date=None, to_date=None, search=None) -> list:
    """Apply filters to a list of audit entry dicts."""
    out = []
    for e in entries:
        if user      and e.get("user",   "") != user:                    continue
        if action    and e.get("action", "") != action:                  continue
        if source    and source not in (e.get("source", "") or ""):      continue
        if status    and status not in (e.get("status", "") or ""):      continue
        if from_date and (e.get("timestamp","") or "")[:10] < from_date: continue
        if to_date   and (e.get("timestamp","") or "")[:10] > to_date:   continue
        if search:
            hay = json.dumps(e).lower()
            if search.lower() not in hay:                                continue
        out.append(e)
    return out


@app.route("/api/audit", methods=["GET"])
def api_audit():
    try:
        limit     = int(request.args.get("limit",  100))
        offset    = int(request.args.get("offset",   0))
        user      = request.args.get("user")      or None
        action    = request.args.get("action")    or None
        source    = request.args.get("source")    or None
        status    = request.args.get("status")    or None
        from_date = request.args.get("from")      or None
        to_date   = request.args.get("to")        or None
        search    = request.args.get("search")    or None

        db = _get_db()

        # ── DB path ───────────────────────────────────────────────────────────
        if db is not None:
            try:
                params = dict(limit=limit, offset=offset, user=user, action=action,
                              source=source, status=status, from_date=from_date,
                              to_date=to_date, search=search)
                entries = db.get_audit_log(**params)
                total   = db.count_audit(**{k: v for k, v in params.items()
                                            if k not in ("limit", "offset")})
                # Serialise datetime objects
                for e in entries:
                    for k, v in e.items():
                        if isinstance(v, datetime):
                            e[k] = v.strftime("%Y-%m-%d %H:%M:%S")
                return jsonify({"entries": entries, "total": total, "source": "db"})
            except Exception as exc:
                _log.error("DB audit read failed, falling back to file: %s", exc)

        # ── File fallback path ────────────────────────────────────────────────
        all_entries = _audit_file_load()
        # Newest first
        all_entries = list(reversed(all_entries))
        filtered    = _filter_entries(all_entries, user=user, action=action,
                                      source=source, status=status,
                                      from_date=from_date, to_date=to_date,
                                      search=search)
        total   = len(filtered)
        page    = filtered[offset: offset + limit]
        # Add row id if missing
        for i, e in enumerate(page):
            if "id" not in e:
                e["id"] = offset + i + 1
        return jsonify({"entries": page, "total": total, "source": "file"})

    except Exception as exc:
        _log.exception("api_audit error")
        return api_error(exc)


@app.route("/api/audit/filters", methods=["GET"])
def api_audit_filters():
    try:
        db = _get_db()
        audit_users = []
        if db is not None:
            try:
                audit_users = db.distinct_users()
            except Exception as exc:
                _log.error("DB audit filters (users) failed: %s", exc)
        else:
            entries = _audit_file_load()
            audit_users = sorted(set(e.get("user", "") for e in entries if e.get("user")))

        # Merge in every RabbitMQ user that currently exists, not just users
        # who happen to have triggered a logged change so far — otherwise the
        # dropdown silently hides anyone who hasn't done anything yet.
        rmq_users = []
        if _rmq is not None and _rmq.connected:
            try:
                rmq_users = [u.get("name") for u in (_rmq.users() or []) if u.get("name")]
            except Exception as exc:
                _log.warning("Could not fetch live RabbitMQ users for filter list: %s", exc)

        users = sorted({u for u in (audit_users + rmq_users) if u and u.lower() != "unknown"})

        if db is not None:
            try:
                return jsonify({"users": users, "actions": db.distinct_actions()})
            except Exception as exc:
                _log.error("DB audit filters (actions) failed, using file: %s", exc)

        entries = _audit_file_load()
        actions = sorted(set(e.get("action", "") for e in entries if e.get("action")))
        return jsonify({"users": users, "actions": actions})
    except Exception as exc:
        _log.exception("api_audit_filters error")
        return api_error(exc)


@app.route("/api/audit/export", methods=["GET"])
def api_audit_export():
    try:
        user      = request.args.get("user")   or None
        action    = request.args.get("action") or None
        from_date = request.args.get("from")   or None
        to_date   = request.args.get("to")     or None
        search    = request.args.get("search") or None

        db = _get_db()
        entries = []

        if db is not None:
            try:
                entries = db.get_audit_log(limit=10000, offset=0, user=user,
                                           action=action, from_date=from_date,
                                           to_date=to_date, search=search)
            except Exception as exc:
                _log.error("DB export failed, using file: %s", exc)

        if not entries:
            all_e   = list(reversed(_audit_file_load()))
            entries = _filter_entries(all_e, user=user, action=action,
                                      from_date=from_date, to_date=to_date,
                                      search=search)

        output = io.StringIO()
        writer = csv.DictWriter(output,
            fieldnames=["id","timestamp","user","host","action",
                        "detail","status","source","vhost"],
            extrasaction="ignore")
        writer.writeheader()
        for i, e in enumerate(entries):
            row = {k: (v.strftime("%Y-%m-%d %H:%M:%S") if isinstance(v, datetime) else (v or ""))
                   for k, v in e.items()}
            row.setdefault("id", i + 1)
            writer.writerow(row)
        return Response(output.getvalue(), mimetype="text/csv",
            headers={"Content-Disposition":
                f'attachment; filename="audit-log-{datetime.now():%Y%m%d-%H%M%S}.csv"'})
    except Exception as exc:
        _log.exception("api_audit_export error")
        return api_error(exc)


@app.route("/api/audit/purge", methods=["DELETE"])
def api_audit_purge():
    try:
        days    = int(request.args.get("days", CFG["audit"].get("retention_days", 90)))
        deleted = 0
        db      = _get_db()
        if db is not None:
            try:
                deleted = db.purge_old_audit(days)
            except Exception as exc:
                _log.error("DB purge failed: %s", exc)

        # Also purge from local file
        from datetime import timedelta
        cutoff  = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")
        entries = _audit_file_load()
        before  = len(entries)
        entries = [e for e in entries if (e.get("timestamp","") or "") >= cutoff]
        _audit_file_save(entries)
        deleted += before - len(entries)

        _audit and _audit.record("Audit Log Purge",
                                 f"Deleted {deleted} entries older than {days} days")
        return jsonify({"ok": True, "deleted": deleted})
    except Exception as exc:
        _log.exception("api_audit_purge error")
        return api_error(exc)


# ── Config ────────────────────────────────────────────────────────────────────

@app.route("/api/config", methods=["GET"])
def api_config():
    safe = {
        "rabbitmq": {k: v for k, v in CFG.get("rabbitmq", {}).items() if k != "password"},
        "database": {k: v for k, v in CFG.get("database", {}).items() if k != "password"},
        "logging":  CFG.get("logging",  {}),
        "audit":    CFG.get("audit",    {}),
        "ports":    CFG.get("ports",    {}),
        "server":   {k: v for k, v in CFG.get("server", {}).items() if k != "secret_key"},
        "ui":       CFG.get("ui",       {}),
    }
    return jsonify(safe)


# ── Aliveness ─────────────────────────────────────────────────────────────────

@app.route("/api/aliveness", methods=["GET"])
@require_connection
def api_aliveness():
    vhost = request.args.get("vhost", "/")
    try:
        return jsonify(_rmq.aliveness_test(vhost))
    except Exception as exc:
        return api_error(exc)


# ── Startup ───────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    srv = CFG.get("server", {})
    host  = srv.get("host",  "0.0.0.0")
    port  = srv.get("port",  8080)
    debug = srv.get("debug", True)

    _log.info("Starting PuffinMQ on http://%s:%d", host, port)

    if srv.get("auto_open_browser", True):
        def _open():
            import time; time.sleep(1.5)
            webbrowser.open(f"http://127.0.0.1:{port}")
        threading.Thread(target=_open, daemon=True).start()

    app.run(host=host, port=port, debug=debug, use_reloader=False)
