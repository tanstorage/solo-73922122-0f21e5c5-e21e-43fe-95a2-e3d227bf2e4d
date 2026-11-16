"""SQLite replica storage.

Layout:
  stations / readings  - the published local copy of the source tables
  meta                 - durable sync state (snapshot_done, last_applied_lsn, ...)

Concurrency model: exactly one writer (the sync worker thread); API handlers
read through short-lived connections. WAL mode lets readers proceed while the
writer holds a transaction, so a source transaction becomes visible atomically
only when its SQLite transaction commits.
"""
import sqlite3
import threading

from . import config

_init_lock = threading.Lock()

SCHEMA = """
CREATE TABLE IF NOT EXISTS meta (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS stations (
    id         INTEGER PRIMARY KEY,
    correction INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS readings (
    id         INTEGER PRIMARY KEY,
    station_id INTEGER NOT NULL,
    value      INTEGER NOT NULL
);
"""


def connect():
    conn = sqlite3.connect(config.SQLITE_PATH, timeout=30)
    conn.execute("PRAGMA busy_timeout = 30000")
    return conn


def init_db():
    with _init_lock:
        conn = connect()
        try:
            conn.execute("PRAGMA journal_mode = WAL")
            conn.executescript(SCHEMA)
            conn.commit()
        finally:
            conn.close()


def get_meta(key, default=None):
    conn = connect()
    try:
        row = conn.execute("SELECT value FROM meta WHERE key = ?", (key,)).fetchone()
        return row[0] if row else default
    finally:
        conn.close()


def set_meta(key, value):
    conn = connect()
    try:
        upsert_meta(conn, key, value)
        conn.commit()
    finally:
        conn.close()


def upsert_meta(conn, key, value):
    conn.execute(
        "INSERT INTO meta(key, value) VALUES(?, ?) "
        "ON CONFLICT(key) DO UPDATE SET value = excluded.value",
        (key, str(value)),
    )


def published():
    """The replica is only open for queries after a complete snapshot."""
    return get_meta("snapshot_done") == "1"


def clear_replica():
    """Local reset: drop all replica rows and all durable sync state."""
    conn = connect()
    try:
        conn.execute("DELETE FROM stations")
        conn.execute("DELETE FROM readings")
        conn.execute("DELETE FROM meta")
        conn.commit()
    finally:
        conn.close()


def fetch_dump():
    """Both tables read inside ONE SQLite transaction.

    Gives callers a single consistent cross-table view of the replica: the
    pair of result sets always reflects a whole number of applied source
    transactions, never a half-applied one.
    """
    conn = connect()
    try:
        conn.execute("BEGIN")
        stations = conn.execute(
            "SELECT id, correction FROM stations ORDER BY id"
        ).fetchall()
        readings = conn.execute(
            "SELECT id, station_id, value FROM readings ORDER BY id"
        ).fetchall()
        conn.commit()
        return stations, readings
    finally:
        conn.close()


def fetch_stations():
    conn = connect()
    try:
        return conn.execute(
            "SELECT id, correction FROM stations ORDER BY id"
        ).fetchall()
    finally:
        conn.close()


def fetch_readings(station_id=None, limit=1000):
    conn = connect()
    try:
        if station_id is None:
            return conn.execute(
                "SELECT id, station_id, value FROM readings ORDER BY id LIMIT ?",
                (limit,),
            ).fetchall()
        return conn.execute(
            "SELECT id, station_id, value FROM readings "
            "WHERE station_id = ? ORDER BY id LIMIT ?",
            (station_id, limit),
        ).fetchall()
    finally:
        conn.close()
