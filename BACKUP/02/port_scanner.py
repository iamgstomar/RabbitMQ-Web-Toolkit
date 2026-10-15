"""
port_scanner.py  —  Parallel RabbitMQ port scanner
Checks all configured ports and returns open/closed status.
Runs as a background thread for periodic re-checks.
"""

import socket
import threading
import time
from typing import Dict, List

import logger as log

_log = log.get_logger("port_scanner")


class PortScanner:
    """Scans RabbitMQ ports in parallel; stores latest results in memory."""

    def __init__(self, cfg: dict):
        ports_cfg      = cfg.get("ports", {})
        self.ports     = {
            int(k): v
            for k, v in ports_cfg.get("monitor", {}).items()
        }
        self.timeout   = ports_cfg.get("timeout_seconds",       1)
        self.rescan_s  = ports_cfg.get("rescan_interval_seconds", 60)

        self._results: Dict[int, dict] = {}
        self._host    = ""
        self._active  = False
        self._lock    = threading.Lock()

    # ── Public API ────────────────────────────────────────────────────────────

    def scan(self, host: str) -> List[dict]:
        """Perform a single parallel scan and return results list."""
        self._host = host
        results    = []
        threads    = []
        lock       = threading.Lock()

        def _check(port, service):
            status, latency = self._probe(host, port)
            with lock:
                results.append({
                    "port":    port,
                    "service": service,
                    "open":    status,
                    "latency": latency,
                    "checked": time.strftime("%Y-%m-%d %H:%M:%S"),
                })

        for port, service in self.ports.items():
            t = threading.Thread(target=_check, args=(port, service), daemon=True)
            threads.append(t)
            t.start()

        for t in threads:
            t.join(timeout=self.timeout + 0.5)

        results.sort(key=lambda r: r["port"])

        with self._lock:
            self._results = {r["port"]: r for r in results}

        open_count = sum(1 for r in results if r["open"])
        _log.info("Port scan complete — host=%s  %d/%d open",
                  host, open_count, len(results))
        return results

    def latest(self) -> List[dict]:
        """Return cached results from the last scan."""
        with self._lock:
            return sorted(self._results.values(), key=lambda r: r["port"])

    def start_background(self, host: str):
        """Start periodic background re-scans."""
        self._host   = host
        self._active = True
        self.scan(host)   # immediate first scan
        t = threading.Thread(target=self._loop, daemon=True, name="port-scanner")
        t.start()
        _log.info("Background port scanner started — interval=%ds", self.rescan_s)

    def stop(self):
        """Stop background scanning."""
        self._active = False
        _log.info("Port scanner stopped")

    # ── Internal ──────────────────────────────────────────────────────────────

    def _probe(self, host: str, port: int):
        """Return (is_open: bool, latency_ms: float)."""
        t0 = time.monotonic()
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.settimeout(self.timeout)
            result  = s.connect_ex((host, port))
            s.close()
            latency = (time.monotonic() - t0) * 1000
            return result == 0, round(latency, 1)
        except Exception as exc:
            latency = (time.monotonic() - t0) * 1000
            _log.debug("Port probe %s:%d — %s", host, port, exc)
            return False, round(latency, 1)

    def _loop(self):
        """Periodic re-scan loop."""
        while self._active:
            time.sleep(self.rescan_s)
            if self._active and self._host:
                try:
                    self.scan(self._host)
                except Exception as exc:
                    _log.error("Background scan error: %s", exc)
