"""DuckDB write helpers — singleton connection with threading lock."""

import json
import threading
import duckdb
from datetime import datetime, timezone
from pathlib import Path

from config.settings import settings
from models import Observation, TextObservation

_lock = threading.Lock()
_connection: duckdb.DuckDBPyConnection | None = None


def get_connection() -> duckdb.DuckDBPyConnection:
    """Return the module-level singleton DuckDB connection (creates on first call)."""
    global _connection
    if _connection is None:
        db_dir = Path(settings.db_path).parent
        db_dir.mkdir(parents=True, exist_ok=True)
        _connection = duckdb.connect(settings.db_path)
    return _connection


def _execute(sql: str, params: list | None = None):
    """Execute a single SQL statement under the write lock."""
    with _lock:
        con = get_connection()
        if params:
            return con.execute(sql, params)
        return con.execute(sql)


def _executemany(sql: str, param_list: list[tuple]):
    """Execute a parameterized statement for many rows under the write lock."""
    with _lock:
        con = get_connection()
        con.executemany(sql, param_list)


def batch_store_observations(records: list[Observation]) -> int:
    """Batch-insert observations with deduplication (INSERT OR IGNORE)."""
    if not records:
        return 0
    _executemany(
        """INSERT OR IGNORE INTO observations
           (installation_id, source, collected_at, observed_date,
            metric_name, metric_value, metadata, metadata_hash)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
        [r.to_row_tuple() for r in records],
    )
    return len(records)


def batch_store_text_observations(records: list[TextObservation]) -> int:
    """Batch-insert text observations with deduplication on (installation_id, source, url)."""
    if not records:
        return 0
    _executemany(
        """INSERT OR IGNORE INTO text_observations
           (installation_id, source, collected_at, observed_date,
            title, body, url, relevance_score, metadata)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        [r.to_row_tuple() for r in records],
    )
    return len(records)


def log_collection_run(source: str, installation_id: str | None = None) -> int:
    """Start a collection run log entry. Returns the run ID."""
    result = _execute(
        """INSERT INTO collection_runs (source, installation_id, started_at, status)
           VALUES (?, ?, ?, 'running')
           RETURNING id""",
        [source, installation_id, datetime.now(timezone.utc)],
    ).fetchone()
    return result[0]


def complete_collection_run(
    run_id: int,
    records: int = 0,
    records_failed: int = 0,
    error: str | None = None,
):
    """Mark a collection run as complete or errored."""
    status = "error" if error else "success"
    _execute(
        """UPDATE collection_runs
           SET completed_at = ?, status = ?, records_collected = ?,
               records_failed = ?, error_message = ?
           WHERE id = ?""",
        [datetime.now(timezone.utc), status, records, records_failed, error, run_id],
    )


def close_connection():
    """Close the singleton connection (for clean shutdown / tests)."""
    global _connection
    with _lock:
        if _connection is not None:
            _connection.close()
            _connection = None
