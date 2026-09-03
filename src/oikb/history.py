"""Sync history tracking via SQLite."""

from __future__ import annotations

import queue
import sqlite3
import time
import uuid
from contextlib import contextmanager
from pathlib import Path
from typing import Any

from oikb.config import CONFIG_DIR


_DEFAULT_DB = CONFIG_DIR / "history.db" if CONFIG_DIR.exists() else Path.home() / ".oikb" / "history.db"

_SCHEMA = """\
CREATE TABLE IF NOT EXISTS sync_log (
    id             TEXT PRIMARY KEY,
    source         TEXT NOT NULL,
    kb_id          TEXT NOT NULL,
    status         TEXT NOT NULL,
    started_at     REAL NOT NULL,
    finished_at    REAL,
    duration_ms    INTEGER,
    files_added    INTEGER DEFAULT 0,
    files_modified INTEGER DEFAULT 0,
    files_deleted  INTEGER DEFAULT 0,
    unmodified     INTEGER DEFAULT 0,
    error_message  TEXT,
    created_at     REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_sync_log_kb_id  ON sync_log(kb_id);
CREATE INDEX IF NOT EXISTS idx_sync_log_source ON sync_log(source);
CREATE INDEX IF NOT EXISTS idx_sync_log_status ON sync_log(status);

CREATE TABLE IF NOT EXISTS file_failures (
    kb_id      TEXT NOT NULL,
    path       TEXT NOT NULL,
    filename   TEXT NOT NULL,
    checksum   TEXT NOT NULL,
    file_id    TEXT,
    error      TEXT,
    kind       TEXT NOT NULL,
    attempts   INTEGER DEFAULT 1,
    first_seen REAL NOT NULL,
    last_seen  REAL NOT NULL,
    PRIMARY KEY (kb_id, path, filename, checksum)
);
CREATE INDEX IF NOT EXISTS idx_file_failures_kind ON file_failures(kind);
"""


class SyncHistory:
    """Lightweight sync history backed by a local SQLite database."""

    def __init__(self, db_path: Path | None = None, pool_size: int = 5):
        self.db_path = db_path or _DEFAULT_DB
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        
        self.pool_size = pool_size
        self._pool = queue.Queue(maxsize=pool_size)
        self._all_conns = []
        
        # Safely initialize and close the schema connection
        init_conn = sqlite3.connect(str(self.db_path), timeout=30.0)
        try:
            init_conn.execute("PRAGMA journal_mode=WAL")
            init_conn.executescript(_SCHEMA)
        finally:
            init_conn.close()
            
        # Populate the pool with bounded connections
        for _ in range(pool_size):
            conn = sqlite3.connect(
                str(self.db_path),
                check_same_thread=False,
                timeout=30.0,
            )
            conn.row_factory = sqlite3.Row
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA synchronous=NORMAL")  # Faster writes in WAL mode
            self._pool.put(conn)
            self._all_conns.append(conn)

    @contextmanager
    def _get_conn(self):
        """Borrow a connection from the pool."""
        try:
            conn = self._pool.get(timeout=30.0)
        except queue.Empty:
            raise RuntimeError("Database connection pool exhausted")
        try:
            yield conn
        finally:
            self._pool.put(conn)

    def log(
        self,
        source: str,
        kb_id: str,
        status: str,
        started_at: float,
        files_added: int = 0,
        files_modified: int = 0,
        files_deleted: int = 0,
        unmodified: int = 0,
        error: str | None = None,
    ) -> None:
        """Record a sync result."""
        now = time.time()
        duration_ms = int((now - started_at) * 1000)
        
        if not self._all_conns:
            return
            
        with self._get_conn() as conn:
            conn.execute(
                """INSERT INTO sync_log
                   (id, source, kb_id, status, started_at, finished_at,
                    duration_ms, files_added, files_modified, files_deleted,
                    unmodified, error_message, created_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    str(uuid.uuid4()),
                    source,
                    kb_id,
                    status,
                    started_at,
                    now,
                    duration_ms,
                    files_added,
                    files_modified,
                    files_deleted,
                    unmodified,
                    error,
                    now,
                ),
            )
            conn.commit()

    def query(
        self,
        limit: int = 20,
        kb_id: str | None = None,
        errors_only: bool = False,
    ) -> list[dict[str, Any]]:
        """Retrieve recent sync log entries."""
        if not self._all_conns:
            return []
            
        sql = "SELECT * FROM sync_log WHERE 1=1"
        params: list[Any] = []

        if kb_id:
            sql += " AND kb_id = ?"
            params.append(kb_id)
        if errors_only:
            sql += " AND status IN ('error', 'partial')"

        sql += " ORDER BY started_at DESC LIMIT ?"
        params.append(limit)

        with self._get_conn() as conn:
            rows = conn.execute(sql, params).fetchall()
            return [dict(row) for row in rows]

    def last_sync(self, source: str) -> dict[str, Any] | None:
        """Get the most recent sync entry for a source."""
        if not self._all_conns:
            return None
            
        with self._get_conn() as conn:
            row = conn.execute(
                "SELECT * FROM sync_log WHERE source = ? ORDER BY started_at DESC LIMIT 1",
                (source,),
            ).fetchone()
            return dict(row) if row else None

    def clear(self, older_than_days: int = 30) -> int:
        """Prune entries older than N days. Returns count deleted."""
        if not self._all_conns:
            return 0
            
        cutoff = time.time() - (older_than_days * 86400)
        with self._get_conn() as conn:
            cursor = conn.execute(
                "DELETE FROM sync_log WHERE created_at < ?", (cutoff,)
            )
            conn.commit()
            return cursor.rowcount

    def close(self) -> None:
        """Close all connections in the pool."""
        # Empty the pool so subsequent calls fail fast
        while not self._pool.empty():
            try:
                self._pool.get_nowait()
            except queue.Empty:
                break

        # Close all tracked connections
        for conn in self._all_conns:
            try:
                conn.close()
            except Exception:
                pass
        self._all_conns.clear()

    def record_failure(self, kb_id: str, path: str, filename: str,
                       checksum: str, file_id: str, error: str, kind: str) -> None:
        """Record or update a file failure (upsert)."""
        if not self._all_conns:
            return
        now = time.time()
        with self._get_conn() as conn:
            conn.execute(
                """INSERT INTO file_failures
                   (kb_id, path, filename, checksum, file_id, error, kind,
                    attempts, first_seen, last_seen)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                   ON CONFLICT(kb_id, path, filename, checksum) DO UPDATE SET
                       file_id = excluded.file_id,
                       error = excluded.error,
                       kind = excluded.kind,
                       attempts = file_failures.attempts + 1,
                       last_seen = excluded.last_seen
                """,
                (kb_id, path, filename, checksum, file_id, error, kind,
                 now, now, now),
            )
            conn.commit()

    def get_failures(self, kb_id: str) -> dict[tuple[str, str, str], dict[str, Any]]:
        """Return failures for a KB, keyed by (path, filename, checksum)."""
        if not self._all_conns:
            return {}
        with self._get_conn() as conn:
            rows = conn.execute(
                "SELECT * FROM file_failures WHERE kb_id = ?", (kb_id,),
            ).fetchall()
            return {
                (row["path"], row["filename"], row["checksum"]): dict(row)
                for row in rows
            }

    def clear_failure(self, kb_id: str, path: str, filename: str,
                      checksum: str) -> None:
        """Remove a failure record (used when linkage later succeeds)."""
        if not self._all_conns:
            return
        with self._get_conn() as conn:
            conn.execute(
                "DELETE FROM file_failures WHERE kb_id = ? AND path = ? AND filename = ? AND checksum = ?",
                (kb_id, path, filename, checksum),
            )
            conn.commit()
