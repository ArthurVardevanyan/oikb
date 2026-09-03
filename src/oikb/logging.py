"""Structured JSON log formatter for production deployments."""

from __future__ import annotations

import json
import logging
import time
from typing import Any


class JsonFormatter(logging.Formatter):
    """Emit log records as single-line JSON objects.

    Output format (one JSON object per line):
        {"ts":"2025-05-21T06:00:01Z","level":"INFO","logger":"oikb.daemon","msg":"Synced ..."}

    Compatible with Datadog, Splunk, ELK, CloudWatch, Loki, and any
    JSON-aware log aggregator.
    """

    def format(self, record: logging.LogRecord) -> str:
        entry: dict[str, Any] = {
            "ts": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(record.created)),
            "level": record.levelname,
            "logger": record.name,
            "msg": record.getMessage(),
        }

        if record.exc_info and record.exc_info[1] is not None:
            entry["exception"] = self.formatException(record.exc_info)

        # Merge any extra fields passed via `log.info("msg", extra={...})`.
        for key in ("source", "kb_id", "status", "duration_ms",
                     "files_added", "files_modified", "files_deleted"):
            val = getattr(record, key, None)
            if val is not None:
                entry[key] = val

        return json.dumps(entry, default=str, ensure_ascii=False)


def configure_logging(log_format: str = "text", log_level: str = "INFO") -> None:
    """Configure root logging for the daemon.

    Args:
        log_format: "text" for human-readable, "json" for structured JSON lines.
        log_level:  Standard Python log level name.
    """
    root = logging.getLogger()
    root.setLevel(getattr(logging, log_level.upper(), logging.INFO))

    # Suppress noisy INFO-level httpx request logs (404s, etc.).
    # Actual errors (WARNING+) still surface.
    logging.getLogger("httpx").setLevel(logging.WARNING)

    # Remove any existing handlers (uvicorn may add its own).
    root.handlers.clear()

    handler = logging.StreamHandler()

    if log_format == "json":
        handler.setFormatter(JsonFormatter())
    else:
        handler.setFormatter(
            logging.Formatter("%(asctime)s %(levelname)-8s %(name)s  %(message)s")
        )

    root.addHandler(handler)
