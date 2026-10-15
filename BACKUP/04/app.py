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
    
    Uses ack_requeue_true mode which means messages stay in queue.
    Returns a list with all fetched messages.
    """
    all_messages = []
    from urllib.parse import quote as _q

    while len(all_messages) < max_messages:
        chunk = _fetch_queue_messages_chunk(
            vhost, queue_name, chunk_size=chunk_size, ackmode="ack_requeue_true"
        )
        if not chunk:
            break

        all_messages.extend(chunk)
        _backup_progress["messages_fetched"] = len(all_messages)

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
    return render_template("index.html",
                           title=CFG["ui"].get("page_title", "PuffinMQ"),
                           theme=CFG["ui"].get("theme", "dark"))


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
    try:
        if _audit:
            _audit.stop()
        client   = RabbitMQClient(c)
        overview = client.connect()
        with _lock:
            _rmq = client
        db   = _get_db()
        host = f"{c['rabbitmq']['host']}:{c['rabbitmq']['port']}"
        user = c["rabbitmq"]["username"]
        _audit = AuditEngine(_rmq, db, CFG, app_user=user, app_host=host)
        _audit.start()
        # NOTE: Connect/Disconnect are app-session events, not a user action
        # against a RabbitMQ resource — they're intentionally not written to
        # the audit log (see EXCLUDED_ACTIONS in audit.py). Logged here instead.
        if CFG["ports"].get("scan_on_connect", True):
            threading.Thread(target=_scanner.start_background,
                             args=(c["rabbitmq"]["host"],), daemon=True).start()
        _log.info("Connected: user=%s host=%s", user, host)
        return jsonify({"ok": True, "overview": overview, "host": host, "user": user})
    except Exception as exc:
        return api_error(exc, 400)


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

    """Export messages from queue as downloadable file (JSON/CSV)."""
    from urllib.parse import quote as _q
    vhost = request.args.get("vhost", "/")
    name  = request.args.get("name",  "")
    count = int(request.args.get("count", 10000))  # Default to get all (10000)
    fmt   = request.args.get("format", "json")  # json or csv
    
    try:
        # Get messages from queue using RabbitMQ Management API directly
        # This avoids double-encoding issues by handling the API call directly
        msgs = _rmq.post(
            f"/queues/{_q(vhost, safe='')}/{_q(name, safe='')}/get",
            {
                "count": min(count, 10000),  # Get up to 10000 messages
                "ackmode": "ack_requeue_true",  # Don't consume messages
                "encoding": "auto"
            }
        )
        
        # Handle response format
        if not msgs:
            msgs = []
        elif isinstance(msgs, dict):
            msgs = [msgs]
        elif not isinstance(msgs, list):
            msgs = []
        
        if not msgs:
            return jsonify({"error": "No messages found in queue"}), 400
        
        # Create filename with timestamp
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        safe_name = name.replace("/", "_")
        safe_vhost = vhost.replace("/", "_") if vhost != "/" else "root"
        filename = f"{safe_vhost}_{safe_name}_{timestamp}.{fmt}"
        
        if fmt == "csv":
            # Export as CSV
            output = io.StringIO()
            writer = csv.DictWriter(output, fieldnames=["payload", "properties", "routing_key"])
            writer.writeheader()
            for msg in msgs:
                writer.writerow({
                    "payload": msg.get("payload", ""),
                    "properties": json.dumps(msg.get("properties", {})),
                    "routing_key": msg.get("routing_key", "")
                })
            content = output.getvalue()
            mimetype = "text/csv"
        else:
            # Export as JSON
            content = json.dumps(msgs, indent=2, default=str)
            mimetype = "application/json"
        
        # Record audit
        _audit and _audit.record("Backup Queue", f"Queue: {name} | VHost: {vhost} | Count: {len(msgs)} | Format: {fmt}")
        
        # Return as downloadable file
        return Response(
            content,
            mimetype=mimetype,
            headers={"Content-Disposition": f"attachment; filename={filename}"}
        )
    except Exception as exc:
        _log.error("Backup error: %s", exc)
        return api_error(exc)

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
                        routing_key = msg.get("routing_key", "") or queue_name
                        properties = msg.get("properties", {}) or {}

                        publish_payload = {
                            "properties": properties,
                            "routing_key": routing_key,
                            "payload": payload_text,
                            "payload_encoding": "string",
                        }

                        _rmq.post(
                            f"/exchanges/{_q(vhost, safe='')}/{_q('amq.default', safe='')}/publish",
                            publish_payload,
                        )
                        published_count += 1
                    except Exception as e:
                        errors.append({"msg_idx": batch_start + batch.index(msg), "error": str(e)})
                
                # Update progress after each batch
                _backup_progress["messages_fetched"] = published_count
                _backup_progress["status"] = (
                    f"Action is under process, please wait... "
                    f"Restoring {queue_name}: {published_count}/{total}"
                )

            _backup_progress["queues_done"] = 1
            _backup_progress["active"] = False
            _backup_progress["status"] = (
                f"Restore complete for {queue_name}. Restored {published_count}/{total}."
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
                    "content": json.dumps(msgs, indent=2, default=str),
                    "mimetype": "application/json"
                }
            else:
                # Create ZIP file in memory
                zip_buffer = io.BytesIO()
                with ZipFile(zip_buffer, 'w') as zf:
                    for q in queues:
                        queue_key = f"{q.get('vhost', '/')}||{q.get('name', '')}"
                        msgs = backups[queue_key]
                        content = json.dumps(msgs, indent=2, default=str)
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
    """Restore messages from JSON data array."""
    from urllib.parse import quote as _q
    
    body = request.get_json(force=True)
    vhost = body.get("vhost", "/")
    queue_name = body.get("name", "")
    messages = body.get("messages", [])
    
    if not messages:
        return jsonify({"error": "No messages provided"}), 400
    
    try:
        published_count = 0
        errors = []
        
        for msg in messages:
            try:
                # Extract message data
                payload_text = msg.get("payload", "")
                routing_key = msg.get("routing_key", "")
                properties = msg.get("properties", {})
                
                # Prepare the message payload for RabbitMQ API
                publish_payload = {
                    "properties": properties,
                    "routing_key": routing_key or queue_name,
                    "payload": payload_text,
                    "payload_encoding": "string",
                }
                
                # Publish via default exchange
                _rmq.post(f"/exchanges/{_q(vhost, safe='')}/{_q('amq.default', safe='')}/publish", publish_payload)
                published_count += 1
            except Exception as exc:
                errors.append({"msg": str(msg.get("payload", ""))[:50], "error": str(exc)})
        
        _audit and _audit.record("Restore Queue", f"Queue: {queue_name} | VHost: {vhost} | Restored: {published_count}")
        
        return jsonify({
            "ok": True,
            "published": published_count,
            "total": len(messages),
            "errors": errors if errors else None
        })
    except Exception as exc:
        return api_error(exc)


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
