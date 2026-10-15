"""
logger.py  —  Structured logging for RMQ QA Utility
Supports DEBUG / INFO / WARNING / ERROR levels, rotating file handler,
optional JSON format, and per-module named loggers.
"""

import logging
import logging.handlers
import json
import os
import sys
from datetime import datetime


# ── Master registry so every module gets the same configured logger ─────────
_loggers: dict = {}
_config:  dict = {}


class JsonFormatter(logging.Formatter):
    """Emit each log record as a single JSON line — useful for log aggregators."""

    def format(self, record: logging.LogRecord) -> str:
        payload = {
            "ts":      datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z",
            "level":   record.levelname,
            "logger":  record.name,
            "msg":     record.getMessage(),
            "file":    f"{record.filename}:{record.lineno}",
        }
        if record.exc_info:
            payload["exc"] = self.formatException(record.exc_info)
        return json.dumps(payload, ensure_ascii=False)


class ColorConsoleFormatter(logging.Formatter):
    """ANSI-coloured console output — only applied when stdout is a TTY."""

    COLORS = {
        "DEBUG":    "\033[36m",   # cyan
        "INFO":     "\033[32m",   # green
        "WARNING":  "\033[33m",   # yellow
        "ERROR":    "\033[31m",   # red
        "CRITICAL": "\033[35m",   # magenta
    }
    RESET = "\033[0m"

    def format(self, record: logging.LogRecord) -> str:
        color  = self.COLORS.get(record.levelname, "")
        reset  = self.RESET if color else ""
        record.levelname = f"{color}{record.levelname:<8}{reset}"
        return super().format(record)


def configure(cfg: dict) -> None:
    """
    Call once at startup with the 'logging' section from config.json.
    Sets up root handlers; all get_logger() calls after this are pre-configured.
    """
    global _config
    _config = cfg

    level_name = cfg.get("level", "DEBUG").upper()
    level      = getattr(logging, level_name, logging.DEBUG)

    root = logging.getLogger("rmq_qa")
    root.setLevel(level)

    # Avoid adding duplicate handlers if configure() is called more than once
    if root.handlers:
        root.handlers.clear()

    fmt      = cfg.get("format",      "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s")
    date_fmt = cfg.get("date_format", "%Y-%m-%d %H:%M:%S")
    use_json = cfg.get("json_format", False)

    # ── Rotating file handler ──────────────────────────────────────────────
    log_file = cfg.get("file", "logs/rmq_qa.log")
    os.makedirs(os.path.dirname(log_file) if os.path.dirname(log_file) else ".", exist_ok=True)

    file_handler = logging.handlers.RotatingFileHandler(
        log_file,
        maxBytes=cfg.get("max_bytes",    10 * 1024 * 1024),  # 10 MB
        backupCount=cfg.get("backup_count", 5),
        encoding="utf-8",
    )
    file_handler.setLevel(level)
    file_handler.setFormatter(
        JsonFormatter() if use_json
        else logging.Formatter(fmt, datefmt=date_fmt)
    )
    root.addHandler(file_handler)

    # ── Console handler ────────────────────────────────────────────────────
    if cfg.get("console", True):
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(level)
        if sys.stdout.isatty() and not use_json:
            console_handler.setFormatter(
                ColorConsoleFormatter(fmt, datefmt=date_fmt))
        else:
            console_handler.setFormatter(
                JsonFormatter() if use_json
                else logging.Formatter(fmt, datefmt=date_fmt)
            )
        root.addHandler(console_handler)

    root.info(
        "Logging configured — level=%s  file=%s  json=%s",
        level_name, log_file, use_json
    )


def get_logger(name: str) -> logging.Logger:
    """
    Return a named child logger under 'rmq_qa'.
    All loggers share the root handlers configured by configure().
    """
    full_name = f"rmq_qa.{name}"
    if full_name not in _loggers:
        _loggers[full_name] = logging.getLogger(full_name)
    return _loggers[full_name]


# ── Convenience wrappers ─────────────────────────────────────────────────────

def debug(msg: str, *args, logger: str = "app", **kw) -> None:
    get_logger(logger).debug(msg, *args, **kw)


def info(msg: str, *args, logger: str = "app", **kw) -> None:
    get_logger(logger).info(msg, *args, **kw)


def warning(msg: str, *args, logger: str = "app", **kw) -> None:
    get_logger(logger).warning(msg, *args, **kw)


def error(msg: str, *args, logger: str = "app", **kw) -> None:
    get_logger(logger).error(msg, *args, **kw)


def exception(msg: str, *args, logger: str = "app", **kw) -> None:
    get_logger(logger).exception(msg, *args, **kw)
