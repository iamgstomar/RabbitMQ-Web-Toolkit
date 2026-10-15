"""
app.py  —  RMQ QA Utility  —  Flask web server
Exposes a JSON API consumed by the single-page frontend.
Mirrors the official RabbitMQ Management UI feature set.
"""

import json
import os
import csv
import io
import threading
import webbrowser
from datetime import datetime
from functools import wraps
from typing import Any

from flask import (
    Flask, jsonify, request, render_template,
    session, redirect, url_for, Response
)

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

# ── Global singletons (created per connection, not at startup) ───────────────
_rmq:     RabbitMQClient = None
_db:      Database       = None
_audit:   AuditEngine    = None
_scanner: PortScanner    = PortScanner(CFG)
_lock     = threading.Lock()


def _get_db() -> Database:
    global _db
    if _db is None:
        _db = Database(CFG["database"])
    return _db


def _get_rmq() -> RabbitMQClient:
    global _rmq
    return _rmq


# ── Decorators ────────────────────────────────────────────────────────────────

def require_connection(f):
    """Return 503 if not connected to RabbitMQ."""
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
                           title=CFG["ui"].get("page_title", "RMQ QA Utility"),
                           theme=CFG["ui"].get("theme", "dark"))


# ── Connection ────────────────────────────────────────────────────────────────

@app.route("/api/connect", methods=["POST"])
def api_connect():
    global _rmq, _audit
    body = request.get_json(force=True)

    # Override config with request params if supplied
    c = dict(CFG)
    c["rabbitmq"] = dict(CFG.get("rabbitmq", {}))
    for key in ("host", "port", "username", "password", "vhost"):
        if body.get(key):
            c["rabbitmq"][key] = body[key]

    try:
        # Stop previous audit engine if any
        if _audit:
            _audit.stop()

        client = RabbitMQClient(c)
        overview = client.connect()

        with _lock:
            _rmq = client

        db   = _get_db()
        host = f"{c['rabbitmq']['host']}:{c['rabbitmq']['port']}"
        user = c["rabbitmq"]["username"]

        # Start audit engine
        _audit = AuditEngine(_rmq, db, CFG, app_user=user, app_host=host)
        _audit.start()
        _audit.record("Connect", f"Host: {c['rabbitmq']['scheme'] if hasattr(_rmq,'scheme') else 'http'}://{host}")

        # Start / refresh port scanner
        if CFG["ports"].get("scan_on_connect", True):
            threading.Thread(
                target=_scanner.start_background,
                args=(c["rabbitmq"]["host"],),
                daemon=True).start()

        _log.info("Connected: user=%s host=%s", user, host)
        return jsonify({
            "ok":       True,
            "overview": overview,
            "host":     host,
            "user":     user,
        })
    except Exception as exc:
        return api_error(exc, 400)


@app.route("/api/disconnect", methods=["POST"])
def api_disconnect():
    global _rmq, _audit
    try:
        if _audit:
            _audit.record("Disconnect", status="✅ Success")
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
    rmq = _get_rmq()
    db_ok = False
    try:
        db_ok = _get_db().ping()
    except Exception:
        pass
    return jsonify({
        "connected": rmq is not None and rmq.connected,
        "host":      f"{rmq.host}:{rmq.port}" if rmq else None,
        "user":      rmq.username if rmq else None,
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
        return jsonify(_rmq.nodes())
    except Exception as exc:
        return api_error(exc)


# ── Queues ────────────────────────────────────────────────────────────────────

@app.route("/api/queues")
@require_connection
def api_queues():
    try:
        return jsonify(_rmq.queues())
    except Exception as exc:
        return api_error(exc)


@app.route("/api/queues", methods=["POST"])
@require_connection
def api_create_queue():
    body = request.get_json(force=True)
    try:
        vhost = body.pop("vhost", "/")
        name  = body.pop("name")
        _rmq.create_queue(name, vhost, body)
        _audit and _audit.record(
            "Create Queue", f"Name: {name} | VHost: {vhost}")
        _log.info("Queue created: %s/%s", vhost, name)
        return jsonify({"ok": True})
    except Exception as exc:
        return api_error(exc)


@app.route("/api/queues/<vhost>/<name>", methods=["DELETE"])
@require_connection
def api_delete_queue(vhost, name):
    try:
        _rmq.delete_queue(name, vhost)
        _audit and _audit.record(
            "Delete Queue", f"Name: {name} | VHost: {vhost}", status="✅ Success")
        return jsonify({"ok": True})
    except Exception as exc:
        return api_error(exc)


@app.route("/api/queues/<vhost>/<name>/purge", methods=["DELETE"])
@require_connection
def api_purge_queue(vhost, name):
    try:
        _rmq.purge_queue(name, vhost)
        _audit and _audit.record(
            "Purge Queue", f"Name: {name} | VHost: {vhost}")
        return jsonify({"ok": True})
    except Exception as exc:
        return api_error(exc)


@app.route("/api/queues/<vhost>/<name>/messages", methods=["GET"])
@require_connection
def api_get_messages(vhost, name):
    try:
        count    = int(request.args.get("count", 10))
        ack_mode = request.args.get("ack_mode", "ack_requeue_true")
        msgs     = _rmq.get_messages(name, vhost, count, ack_mode)
        return jsonify(msgs)
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
            _audit and _audit.record(
                "Delete Queue", f"Name: {q['name']} | VHost: {q['vhost']}")
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
            _audit and _audit.record(
                "Purge Queue", f"Name: {q['name']} | VHost: {q['vhost']}")
        except Exception as exc:
            errors.append({"queue": q["name"], "error": str(exc)})
    return jsonify({"ok": True, "purged": len(queues) - len(errors), "errors": errors})


@app.route("/api/queues/<vhost>/<name>/publish", methods=["POST"])
@require_connection
def api_publish_message(vhost, name):
    body = request.get_json(force=True)
    try:
        payload = {
            "properties":       {},
            "routing_key":      name,
            "payload":          body.get("payload", ""),
            "payload_encoding": "string",
        }
        from urllib.parse import quote as _q
        _rmq.post(f"/exchanges/{_q(vhost, safe='')}//publish", payload)
        _audit and _audit.record(
            "Publish Message", f"Queue: {name} | VHost: {vhost}")
        return jsonify({"ok": True})
    except Exception as exc:
        return api_error(exc)


# ── Exchanges ─────────────────────────────────────────────────────────────────

@app.route("/api/exchanges")
@require_connection
def api_exchanges():
    try:
        return jsonify(_rmq.exchanges())
    except Exception as exc:
        return api_error(exc)


@app.route("/api/exchanges", methods=["POST"])
@require_connection
def api_create_exchange():
    body = request.get_json(force=True)
    try:
        vhost = body.pop("vhost", "/")
        name  = body.pop("name")
        _rmq.create_exchange(name, vhost, body)
        _audit and _audit.record(
            "Create Exchange", f"Name: {name} | Type: {body.get('type')} | VHost: {vhost}")
        return jsonify({"ok": True})
    except Exception as exc:
        return api_error(exc)


@app.route("/api/exchanges/<vhost>/<name>", methods=["DELETE"])
@require_connection
def api_delete_exchange(vhost, name):
    try:
        _rmq.delete_exchange(name, vhost)
        _audit and _audit.record(
            "Delete Exchange", f"Name: {name} | VHost: {vhost}")
        return jsonify({"ok": True})
    except Exception as exc:
        return api_error(exc)


# ── Bindings ──────────────────────────────────────────────────────────────────

@app.route("/api/bindings")
@require_connection
def api_bindings():
    try:
        return jsonify(_rmq.bindings())
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
        _audit and _audit.record(
            "Create Binding",
            f"Exchange: {body['exchange']} → Queue: {body['queue']} | "
            f"Key: {body.get('routing_key','(none)')}")
        return jsonify({"ok": True})
    except Exception as exc:
        return api_error(exc)


# ── Connections ───────────────────────────────────────────────────────────────

@app.route("/api/connections")
@require_connection
def api_connections():
    try:
        return jsonify(_rmq.connections())
    except Exception as exc:
        return api_error(exc)


@app.route("/api/connections/<name>", methods=["DELETE"])
@require_connection
def api_close_connection(name):
    reason = request.args.get("reason", "Closed by admin")
    try:
        _rmq.close_connection(name, reason)
        _audit and _audit.record("Close Connection", f"Conn: {name} | Reason: {reason}")
        return jsonify({"ok": True})
    except Exception as exc:
        return api_error(exc)


# ── Channels ──────────────────────────────────────────────────────────────────

@app.route("/api/channels")
@require_connection
def api_channels():
    try:
        return jsonify(_rmq.channels())
    except Exception as exc:
        return api_error(exc)


# ── Users ─────────────────────────────────────────────────────────────────────

@app.route("/api/users")
@require_connection
def api_users():
    try:
        return jsonify(_rmq.users())
    except Exception as exc:
        return api_error(exc)


@app.route("/api/users", methods=["POST"])
@require_connection
def api_create_user():
    body = request.get_json(force=True)
    try:
        username = body["username"]
        _rmq.create_user(username, {
            "password": body["password"],
            "tags":     body.get("tags", ""),
        })
        # Set vhost permissions
        for perm in body.get("permissions", []):
            _rmq.set_permissions(perm["vhost"], username, {
                "configure": perm.get("configure", ".*"),
                "write":     perm.get("write",     ".*"),
                "read":      perm.get("read",      ".*"),
            })
        _audit and _audit.record(
            "Create User",
            f"User: {username} | Tags: {body.get('tags','(none)')}")
        return jsonify({"ok": True})
    except Exception as exc:
        return api_error(exc)


@app.route("/api/users/<username>", methods=["DELETE"])
@require_connection
def api_delete_user(username):
    try:
        _rmq.delete_user(username)
        _audit and _audit.record("Delete User", f"User: {username}")
        return jsonify({"ok": True})
    except Exception as exc:
        return api_error(exc)


@app.route("/api/permissions")
@require_connection
def api_permissions():
    try:
        return jsonify(_rmq.permissions())
    except Exception as exc:
        return api_error(exc)


@app.route("/api/whoami")
@require_connection
def api_whoami():
    try:
        return jsonify(_rmq.whoami())
    except Exception as exc:
        return api_error(exc)


# ── Virtual Hosts ─────────────────────────────────────────────────────────────

@app.route("/api/vhosts")
@require_connection
def api_vhosts():
    try:
        return jsonify(_rmq.vhosts())
    except Exception as exc:
        return api_error(exc)


@app.route("/api/vhosts", methods=["POST"])
@require_connection
def api_create_vhost():
    body = request.get_json(force=True)
    name = body.get("name", "")
    try:
        _rmq.create_vhost(name)
        # Grant current user full access
        try:
            me = _rmq.whoami().get("name", "")
            if me:
                _rmq.set_permissions(
                    name, me,
                    {"configure": ".*", "write": ".*", "read": ".*"})
        except Exception:
            pass
        _audit and _audit.record("Create VHost", f"VHost: {name}")
        return jsonify({"ok": True})
    except Exception as exc:
        return api_error(exc)


@app.route("/api/vhosts/<name>", methods=["DELETE"])
@require_connection
def api_delete_vhost(name):
    try:
        _rmq.delete_vhost(name)
        _audit and _audit.record("Delete VHost", f"VHost: {name}")
        return jsonify({"ok": True})
    except Exception as exc:
        return api_error(exc)


# ── Policies ──────────────────────────────────────────────────────────────────

@app.route("/api/policies")
@require_connection
def api_policies():
    try:
        return jsonify(_rmq.policies())
    except Exception as exc:
        return api_error(exc)


@app.route("/api/policies", methods=["POST"])
@require_connection
def api_create_policy():
    body = request.get_json(force=True)
    try:
        vhost = body.pop("vhost", "/")
        name  = body.pop("name")
        _rmq.create_policy(vhost, name, body)
        _audit and _audit.record(
            "Create Policy",
            f"Name: {name} | VHost: {vhost} | Pattern: {body.get('pattern','')}")
        return jsonify({"ok": True})
    except Exception as exc:
        return api_error(exc)


@app.route("/api/policies/<vhost>/<name>", methods=["DELETE"])
@require_connection
def api_delete_policy(vhost, name):
    try:
        _rmq.delete_policy(vhost, name)
        _audit and _audit.record("Delete Policy", f"Name: {name} | VHost: {vhost}")
        return jsonify({"ok": True})
    except Exception as exc:
        return api_error(exc)


# ── Feature Flags ─────────────────────────────────────────────────────────────

@app.route("/api/feature-flags")
@require_connection
def api_feature_flags():
    try:
        return jsonify(_rmq.feature_flags())
    except Exception as exc:
        return api_error(exc)


# ── Shovels ───────────────────────────────────────────────────────────────────

@app.route("/api/shovels")
@require_connection
def api_shovels():
    try:
        return jsonify(_rmq.shovels())
    except Exception as exc:
        return api_error(exc)


@app.route("/api/shovels", methods=["POST"])
@require_connection
def api_create_shovel():
    body = request.get_json(force=True)
    try:
        vhost = body.get("vhost", "/")
        name  = body.get("name")
        _rmq.create_shovel(vhost, name, {"value": body.get("value", {})})
        _audit and _audit.record(
            "Create Shovel", f"Name: {name} | VHost: {vhost}")
        return jsonify({"ok": True})
    except Exception as exc:
        return api_error(exc)


# ── Definitions ───────────────────────────────────────────────────────────────

@app.route("/api/definitions")
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
        data = request.get_json(force=True)
        _rmq.upload_definitions(data)
        _audit and _audit.record("Upload Definitions", "Definitions uploaded")
        return jsonify({"ok": True})
    except Exception as exc:
        return api_error(exc)


@app.route("/api/definitions/export")
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

@app.route("/api/ports")
def api_ports():
    return jsonify(_scanner.latest())


@app.route("/api/ports/scan", methods=["POST"])
def api_scan_ports():
    rmq = _get_rmq()
    host = (rmq.host if rmq else
            request.get_json(force=True).get("host", "localhost"))
    try:
        results = _scanner.scan(host)
        return jsonify(results)
    except Exception as exc:
        return api_error(exc)


# ── Audit Log ─────────────────────────────────────────────────────────────────

@app.route("/api/audit")
def api_audit():
    try:
        db = _get_db()
        params = {
            "limit":     int(request.args.get("limit",  100)),
            "offset":    int(request.args.get("offset",   0)),
            "user":      request.args.get("user")   or None,
            "action":    request.args.get("action") or None,
            "source":    request.args.get("source") or None,
            "status":    request.args.get("status") or None,
            "from_date": request.args.get("from")   or None,
            "to_date":   request.args.get("to")     or None,
            "search":    request.args.get("search") or None,
        }
        entries = db.get_audit_log(**params)
        total   = db.count_audit(**{k: v for k, v in params.items()
                                    if k not in ("limit", "offset")})
        # Serialise datetime objects
        for e in entries:
            for k, v in e.items():
                if isinstance(v, datetime):
                    e[k] = v.strftime("%Y-%m-%d %H:%M:%S")
        return jsonify({"entries": entries, "total": total})
    except Exception as exc:
        return api_error(exc)


@app.route("/api/audit/filters")
def api_audit_filters():
    try:
        db = _get_db()
        return jsonify({
            "users":   db.distinct_users(),
            "actions": db.distinct_actions(),
        })
    except Exception as exc:
        return api_error(exc)


@app.route("/api/audit/export")
def api_audit_export():
    """Export filtered audit log as CSV."""
    try:
        db = _get_db()
        params = {
            "limit":     10000,
            "offset":    0,
            "user":      request.args.get("user")   or None,
            "action":    request.args.get("action") or None,
            "from_date": request.args.get("from")   or None,
            "to_date":   request.args.get("to")     or None,
            "search":    request.args.get("search") or None,
        }
        entries = db.get_audit_log(**params)

        output = io.StringIO()
        writer = csv.DictWriter(
            output,
            fieldnames=["id","timestamp","user","host",
                        "action","detail","status","source","vhost"])
        writer.writeheader()
        for e in entries:
            row = {k: (v.strftime("%Y-%m-%d %H:%M:%S") if isinstance(v, datetime) else v)
                   for k, v in e.items()}
            writer.writerow(row)

        filename = f"audit-log-{datetime.now():%Y%m%d-%H%M%S}.csv"
        return Response(
            output.getvalue(),
            mimetype="text/csv",
            headers={"Content-Disposition": f'attachment; filename="{filename}"'})
    except Exception as exc:
        return api_error(exc)


@app.route("/api/audit/purge", methods=["DELETE"])
def api_audit_purge():
    try:
        days    = int(request.args.get("days", CFG["audit"].get("retention_days", 90)))
        deleted = _get_db().purge_old_audit(days)
        _audit and _audit.record("Audit Log Purge", f"Deleted {deleted} entries older than {days} days")
        return jsonify({"ok": True, "deleted": deleted})
    except Exception as exc:
        return api_error(exc)


# ── Config (read-only for UI) ─────────────────────────────────────────────────

@app.route("/api/config")
def api_config():
    """Return safe subset of config (no passwords)."""
    safe = {
        "rabbitmq": {k: v for k, v in CFG.get("rabbitmq", {}).items()
                     if k not in ("password",)},
        "database": {k: v for k, v in CFG.get("database", {}).items()
                     if k not in ("password",)},
        "logging":  CFG.get("logging",  {}),
        "audit":    CFG.get("audit",    {}),
        "ports":    CFG.get("ports",    {}),
        "server":   {k: v for k, v in CFG.get("server",  {}).items()
                     if k not in ("secret_key",)},
        "ui":       CFG.get("ui",       {}),
    }
    return jsonify(safe)


# ── Aliveness test ────────────────────────────────────────────────────────────

@app.route("/api/aliveness/<vhost>")
@require_connection
def api_aliveness(vhost):
    try:
        return jsonify(_rmq.aliveness_test(vhost))
    except Exception as exc:
        return api_error(exc)


# ── Startup ───────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    srv_cfg = CFG.get("server", {})
    host    = srv_cfg.get("host",  "0.0.0.0")
    port    = srv_cfg.get("port",  8080)
    debug   = srv_cfg.get("debug", True)

    _log.info("Starting RMQ QA Utility on http://%s:%d", host, port)

    if srv_cfg.get("auto_open_browser", True):
        def _open():
            import time
            time.sleep(1.5)
            webbrowser.open(f"http://127.0.0.1:{port}")
        threading.Thread(target=_open, daemon=True).start()

    app.run(host=host, port=port, debug=debug, use_reloader=False)
