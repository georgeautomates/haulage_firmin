import sqlite3
from datetime import datetime
from pathlib import Path

from firmin.utils.logger import get_logger

logger = get_logger(__name__)


class DedupStore:
    def __init__(self, db_path: str = "firmin.db"):
        self.db_path = db_path
        # For in-memory DBs, hold a single persistent connection
        if db_path == ":memory:":
            self._mem_conn = sqlite3.connect(":memory:")
            self._mem_conn.row_factory = sqlite3.Row
        else:
            self._mem_conn = None
        self._init_db()

    def _connect(self) -> sqlite3.Connection:
        if self._mem_conn is not None:
            return self._mem_conn
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def _exec(self, fn):
        conn = self._connect()
        if self._mem_conn is not None:
            return fn(conn)
        with conn:
            return fn(conn)

    def _init_db(self) -> None:
        self._exec(lambda conn: conn.executescript("""
            CREATE TABLE IF NOT EXISTS processed_emails (
                message_id   TEXT PRIMARY KEY,
                processed_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS processed_orders (
                job_number   TEXT PRIMARY KEY,
                message_id   TEXT NOT NULL,
                processed_at TEXT NOT NULL
            );
        """))
        logger.debug("Dedup store initialised at %s", self.db_path)

    def email_seen(self, message_id: str) -> bool:
        row = self._exec(lambda conn: conn.execute(
            "SELECT 1 FROM processed_emails WHERE message_id = ?", (message_id,)
        ).fetchone())
        return row is not None

    def mark_email_seen(self, message_id: str) -> None:
        self._exec(lambda conn: conn.execute(
            "INSERT OR IGNORE INTO processed_emails (message_id, processed_at) VALUES (?, ?)",
            (message_id, datetime.utcnow().isoformat()),
        ))
        logger.debug("Marked email as seen: %s", message_id)

    def order_seen(self, job_number: str) -> bool:
        row = self._exec(lambda conn: conn.execute(
            "SELECT 1 FROM processed_orders WHERE job_number = ?", (job_number,)
        ).fetchone())
        return row is not None

    def mark_order_seen(self, job_number: str, message_id: str) -> None:
        self._exec(lambda conn: conn.execute(
            "INSERT OR IGNORE INTO processed_orders (job_number, message_id, processed_at) VALUES (?, ?, ?)",
            (job_number, message_id, datetime.utcnow().isoformat()),
        ))
        logger.debug("Marked order as seen: %s", job_number)
