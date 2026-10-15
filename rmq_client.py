"""
rmq_client.py  —  RabbitMQ Management HTTP API wrapper
Handles auth, SSL, retries, and all API endpoints used by the web UI.
"""

import time
import requests
import urllib3
from urllib.parse import quote
from typing import Any, Dict, List, Optional

import logger as log

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

_log = log.get_logger("rmq_client")


class RabbitMQClient:
    """Thread-safe HTTP client for the RabbitMQ Management API."""

    def __init__(self, cfg: dict):
        rmq            = cfg.get("rabbitmq", {})
        self.host      = rmq.get("host",     "localhost")
        self.port      = rmq.get("port",     15672)
        self.username  = rmq.get("username", "guest")
        self.password  = rmq.get("password", "guest")
        self.vhost     = rmq.get("vhost",    "/")
        self.ssl       = rmq.get("ssl",      False)
        self.verify    = rmq.get("verify_ssl", False)
        self.timeout   = rmq.get("timeout",  10)
        self.schemes   = rmq.get("schemes",  ["http", "https"])

        self.base      = ""
        self.scheme    = ""
        self.auth      = (self.username, self.password)
        self.session   = requests.Session()
        # Larger connection pool so concurrent operations (e.g. bulk restore/publish,
        # which fire many parallel HTTP requests) don't serialize behind the
        # requests library's small default pool size (10).
        _adapter = requests.adapters.HTTPAdapter(pool_connections=50, pool_maxsize=50)
        self.session.mount("http://", _adapter)
        self.session.mount("https://", _adapter)
        self.connected = False

        _log.debug("RabbitMQClient created for %s:%s", self.host, self.port)

    # ── Connection ────────────────────────────────────────────────────────────

    def connect(self) -> dict:
        """
        Try each scheme until one succeeds.
        Returns the /overview payload on success.
        Raises ConnectionError on failure.
        """
        schemes = (["https", "http"] if self.port in (15671, 5671)
                   else self.schemes)

        last_err = None
        for scheme in schemes:
            base = f"{scheme}://{self.host}:{self.port}/api"
            _log.debug("Trying %s", base)
            try:
                r = self.session.get(
                    f"{base}/overview",
                    auth=self.auth,
                    verify=self.verify,
                    timeout=self.timeout,
                )
                if r.ok:
                    self.base      = base
                    self.scheme    = scheme
                    self.connected = True
                    _log.info("Connected via %s://%s:%s", scheme, self.host, self.port)
                    return r.json()
                last_err = f"HTTP {r.status_code}"
                _log.warning("Scheme %s returned %s", scheme, r.status_code)
            except Exception as exc:
                last_err = str(exc)
                _log.warning("Scheme %s error: %s", scheme, exc)

        self.connected = False
        raise ConnectionError(
            f"Cannot connect to {self.host}:{self.port} — {last_err}"
        )

    def disconnect(self) -> None:
        self.connected = False
        self.base      = ""
        _log.info("Disconnected from RabbitMQ")

    # ── Core HTTP verbs ───────────────────────────────────────────────────────

    def _url(self, path: str) -> str:
        return self.base + path

    def get(self, path: str) -> Any:
        _log.debug("GET %s", path)
        r = self.session.get(
            self._url(path), auth=self.auth,
            verify=self.verify, timeout=self.timeout)
        r.raise_for_status()
        return r.json()

    def put(self, path: str, body: dict) -> requests.Response:
        _log.debug("PUT %s  body=%s", path, body)
        r = self.session.put(
            self._url(path), auth=self.auth, json=body,
            verify=self.verify, timeout=self.timeout)
        if r.status_code not in (200, 201, 204):
            raise Exception(f"PUT {path} → HTTP {r.status_code}: {r.text[:300]}")
        return r

    def post(self, path: str, body: dict) -> Any:
        _log.debug("POST %s", path)
        r = self.session.post(
            self._url(path), auth=self.auth, json=body,
            verify=self.verify, timeout=self.timeout)
        if not r.ok:
            raise Exception(f"POST {path} → HTTP {r.status_code}: {r.text[:500]}")
        try:
            return r.json()
        except Exception:
            return {}

    def delete(self, path: str, headers: dict = None) -> requests.Response:
        _log.debug("DELETE %s", path)
        h = dict(headers) if headers else {}
        r = self.session.delete(
            self._url(path), auth=self.auth, headers=h,
            verify=self.verify, timeout=self.timeout)
        if r.status_code not in (200, 204):
            raise Exception(f"DELETE {path} → HTTP {r.status_code}: {r.text[:300]}")
        return r

    # ── Helpers ───────────────────────────────────────────────────────────────

    @staticmethod
    def _q(val: str) -> str:
        """URL-encode a path segment."""
        return quote(str(val), safe="")

    # ── Overview / Cluster ────────────────────────────────────────────────────

    def overview(self)              -> dict:  return self.get("/overview")
    def nodes(self)                 -> list:  return self.get("/nodes")
    def cluster_name(self)          -> dict:  return self.get("/cluster-name")
    def health_check(self)          -> dict:  return self.get("/health/checks/alarms")

    # ── Queues ────────────────────────────────────────────────────────────────

    def queues(self, vhost: str = None) -> list:
        path = f"/queues/{self._q(vhost)}" if vhost else "/queues"
        return self.get(path)

    def queue(self, name: str, vhost: str = "/") -> dict:
        return self.get(f"/queues/{self._q(vhost)}/{self._q(name)}")

    def create_queue(self, name: str, vhost: str, body: dict) -> requests.Response:
        return self.put(f"/queues/{self._q(vhost)}/{self._q(name)}", body)

    def delete_queue(self, name: str, vhost: str) -> requests.Response:
        return self.delete(f"/queues/{self._q(vhost)}/{self._q(name)}")

    def purge_queue(self, name: str, vhost: str) -> requests.Response:
        return self.delete(f"/queues/{self._q(vhost)}/{self._q(name)}/contents")

    def get_messages(self, name: str, vhost: str, count: int = 10,
                     ack_mode: str = "ack_requeue_true") -> list:
        return self.post(
            f"/queues/{self._q(vhost)}/{self._q(name)}/get",
            {"count": count, "ackmode": ack_mode, "encoding": "auto"})

    # ── Exchanges ─────────────────────────────────────────────────────────────

    def exchanges(self, vhost: str = None) -> list:
        path = f"/exchanges/{self._q(vhost)}" if vhost else "/exchanges"
        return self.get(path)

    def create_exchange(self, name: str, vhost: str, body: dict) -> requests.Response:
        return self.put(f"/exchanges/{self._q(vhost)}/{self._q(name)}", body)

    def delete_exchange(self, name: str, vhost: str) -> requests.Response:
        return self.delete(f"/exchanges/{self._q(vhost)}/{self._q(name)}")

    # ── Bindings ──────────────────────────────────────────────────────────────

    def bindings(self) -> list:
        return self.get("/bindings")

    def create_binding(self, vhost: str, exchange: str, queue: str,
                       body: dict) -> requests.Response:
        url = f"{self.base}/bindings/{self._q(vhost)}/e/{self._q(exchange)}/q/{self._q(queue)}"
        r = self.session.post(
            url, auth=self.auth, json=body,
            verify=self.verify, timeout=self.timeout)
        if r.status_code not in (200, 201, 204):
            raise Exception(f"HTTP {r.status_code}: {r.text[:300]}")
        return r

    # ── Connections / Channels ────────────────────────────────────────────────

    def connections(self)   -> list:  return self.get("/connections")
    def channels(self)      -> list:  return self.get("/channels")

    def close_connection(self, name: str, reason: str = "Closed by admin"):
        return self.delete(f"/connections/{self._q(name)}",
                           headers={"X-Reason": reason})

    # ── Users / Permissions ───────────────────────────────────────────────────

    def users(self)         -> list:  return self.get("/users")
    def whoami(self)        -> dict:  return self.get("/whoami")
    def permissions(self)   -> list:  return self.get("/permissions")

    def create_user(self, username: str, body: dict) -> requests.Response:
        return self.put(f"/users/{self._q(username)}", body)

    def delete_user(self, username: str) -> requests.Response:
        return self.delete(f"/users/{self._q(username)}")

    def set_permissions(self, vhost: str, username: str, body: dict):
        return self.put(
            f"/permissions/{self._q(vhost)}/{self._q(username)}", body)

    # ── Virtual Hosts ─────────────────────────────────────────────────────────

    def vhosts(self) -> list:
        return self.get("/vhosts")

    def create_vhost(self, name: str) -> requests.Response:
        return self.put(f"/vhosts/{self._q(name)}", {})

    def delete_vhost(self, name: str) -> requests.Response:
        return self.delete(f"/vhosts/{self._q(name)}")

    def aliveness_test(self, vhost: str = "/") -> dict:
        return self.get(f"/aliveness-test/{self._q(vhost)}")

    # ── Policies ──────────────────────────────────────────────────────────────

    def policies(self) -> list:
        return self.get("/policies")

    def create_policy(self, vhost: str, name: str, body: dict):
        return self.put(f"/policies/{self._q(vhost)}/{self._q(name)}", body)

    def delete_policy(self, vhost: str, name: str):
        return self.delete(f"/policies/{self._q(vhost)}/{self._q(name)}")

    # ── Feature flags / Plugins ───────────────────────────────────────────────

    def feature_flags(self) -> list:
        return self.get("/feature-flags")

    # ── Definitions ───────────────────────────────────────────────────────────

    def definitions(self) -> dict:
        return self.get("/definitions")

    def upload_definitions(self, data: dict):
        return self.post("/definitions", data)

    # ── Shovels ───────────────────────────────────────────────────────────────

    def shovels(self) -> list:
        try:
            return self.get("/parameters/shovel")
        except Exception:
            return []

    def create_shovel(self, vhost: str, name: str, body: dict):
        return self.put(f"/parameters/shovel/{self._q(vhost)}/{self._q(name)}", body)

    # ── Event queue (audit plugin) ────────────────────────────────────────────

    def create_temp_queue(self, vhost: str, name: str):
        return self.put(
            f"/queues/{self._q(vhost)}/{self._q(name)}",
            {"durable": False, "auto_delete": True, "arguments": {}})

    def bind_to_event_exchange(self, vhost: str, queue_name: str):
        url = (f"{self.base}/bindings/{self._q(vhost)}"
               f"/e/{self._q('amq.rabbitmq.event')}"
               f"/q/{self._q(queue_name)}")
        r = self.session.post(
            url, auth=self.auth,
            json={"routing_key": "#", "arguments": {}},
            verify=self.verify, timeout=self.timeout)
        return r

    def poll_queue(self, vhost: str, name: str, count: int = 100) -> list:
        try:
            return self.post(
                f"/queues/{self._q(vhost)}/{self._q(name)}/get",
                {"count": count,
                 "ack_mode": "ack_requeue_false",
                 "encoding": "auto"})
        except Exception:
            return []

    def delete_temp_queue(self, vhost: str, name: str):
        try:
            self.delete(f"/queues/{self._q(vhost)}/{self._q(name)}")
        except Exception:
            pass
