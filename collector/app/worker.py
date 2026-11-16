"""Sync worker: consistent initial snapshot + logical decoding CDC into SQLite.

Correctness outline
-------------------
Handoff snapshot -> stream without gaps or duplicates:
  The replication slot is created with SNAPSHOT 'export', so the exported
  snapshot corresponds exactly to the slot's consistent point. Everything
  committed before that point is visible in the snapshot; everything committed
  after it arrives through the slot. The snapshot copy and the stream
  therefore tile the timeline with no gap and no overlap.

Exactly-once effect across crashes:
  Each source transaction is applied together with the new last_applied_lsn in
  ONE SQLite transaction. Feedback to the slot is only sent after that commit.
  If the process dies between the SQLite commit and the feedback, the slot
  re-delivers the transaction after restart; it is then skipped because its
  commit LSN <= persisted last_applied_lsn (and the upserts are idempotent
  anyway). Newer values are never reverted because changes are applied
  strictly in source commit order on a single slot / single consumer.

Cross-table atomicity:
  All row changes of one source transaction (both tables) are buffered and
  applied in a single SQLite transaction, so readers only see complete source
  transactions.
"""
import logging
import re
import select
import threading
import time
from datetime import datetime, timezone

import psycopg2
from psycopg2.extras import LogicalReplicationConnection, REPLICATION_LOGICAL

from . import config, store
from .decoder import is_begin, is_commit, parse_change

log = logging.getLogger("collector.worker")

SNAPSHOT_TABLES = (
    ("stations", "id, correction"),
    ("readings", "id, station_id, value"),
)
COLUMNS = {
    "stations": ("id", "correction"),
    "readings": ("id", "station_id", "value"),
}
UPSERT_SQL = {
    "stations": "INSERT INTO stations(id, correction) VALUES(?, ?) "
                "ON CONFLICT(id) DO UPDATE SET correction = excluded.correction",
    "readings": "INSERT INTO readings(id, station_id, value) VALUES(?, ?, ?) "
                "ON CONFLICT(id) DO UPDATE SET station_id = excluded.station_id, "
                "value = excluded.value",
}
DELETE_SQL = {
    "stations": "DELETE FROM stations WHERE id = ?",
    "readings": "DELETE FROM readings WHERE id = ?",
}

SLOT_NAME_RE = re.compile(r"^[a-z_][a-z0-9_]*$")
SNAPSHOT_NAME_RE = re.compile(r"^[0-9A-Fa-f-]+$")


def utcnow():
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def lsn_to_int(lsn):
    hi, lo = lsn.split("/")
    return (int(hi, 16) << 32) + int(lo, 16)


class StopStream(Exception):
    """Raised inside the consumer to unwind streaming when a stop is requested."""


class FatalSyncError(Exception):
    """Unrecoverable state (e.g. slot vanished on the source); needs a reset."""


class Status:
    """Volatile runtime status. Durable facts live in SQLite meta."""

    def __init__(self):
        self._lock = threading.Lock()
        self.reset()

    def reset(self):
        with self._lock:
            self.phase = "idle"  # idle | snapshot | streaming | disconnected | error
            self.message = ""
            self.connected = False
            self.rows_copied = 0
            self.total_rows = 0
            self.applied_txns = 0
            self.applied_changes = 0
            self.last_commit_lsn = None
            self.last_commit_at = None
            self.started_at = None

    def update(self, **kw):
        with self._lock:
            for k, v in kw.items():
                setattr(self, k, v)

    def txn_applied(self, n_changes, lsn):
        with self._lock:
            self.applied_txns += 1
            self.applied_changes += n_changes
            self.last_commit_lsn = lsn
            self.last_commit_at = utcnow()

    def snapshot(self):
        with self._lock:
            return {
                "phase": self.phase,
                "message": self.message,
                "connected": self.connected,
                "rows_copied": self.rows_copied,
                "total_rows": self.total_rows,
                "applied_txns": self.applied_txns,
                "applied_changes": self.applied_changes,
                "last_commit_lsn": self.last_commit_lsn,
                "last_commit_at": self.last_commit_at,
                "started_at": self.started_at,
            }


STATUS = Status()


def apply_change(db, change):
    table, op, old, new = change
    if op == "DELETE":
        db.execute(DELETE_SQL[table], (old["id"],))
        return
    if op == "UPDATE" and old and old.get("id") is not None and old["id"] != new["id"]:
        db.execute(DELETE_SQL[table], (old["id"],))  # primary key changed
    cols = COLUMNS[table]
    db.execute(UPSERT_SQL[table], tuple(new[c] for c in cols))


class ChangeConsumer:
    """Applies the decoded stream; one SQLite transaction per source transaction."""

    def __init__(self, worker, repl_cursor, last_applied_lsn):
        self.worker = worker
        self.cursor = repl_cursor
        self.last_applied_lsn = last_applied_lsn
        self.buffer = []

    def __call__(self, msg):
        if self.worker.stop_event.is_set():
            raise StopStream()
        text = self._decode_payload(msg.payload)
        if text is None:  # primary keepalive message
            STATUS.update(connected=True)
            self.cursor.send_feedback(flush_lsn=self.last_applied_lsn, force=True)
            return
        STATUS.update(connected=True)
        for line in text.splitlines():
            self._handle_line(line.strip(), msg)

    @staticmethod
    def _decode_payload(payload):
        if isinstance(payload, str):
            return payload
        try:
            return bytes(payload).decode("utf-8")
        except (UnicodeDecodeError, ValueError):
            return None

    def _handle_line(self, line, msg):
        if not line:
            return
        if is_begin(line):
            self.buffer = []
        elif is_commit(line):
            self._commit(msg.data_start)
        else:
            change = parse_change(line)
            if change and change[0] in UPSERT_SQL:
                self.buffer.append(change)

    def _commit(self, commit_lsn):
        changes, self.buffer = self.buffer, []
        if commit_lsn and commit_lsn <= self.last_applied_lsn:
            # Already persisted before a crash/restart: skip the changes but let
            # the slot advance so it does not re-deliver forever.
            self.cursor.send_feedback(flush_lsn=self.last_applied_lsn, force=True)
            return
        db = store.connect()
        try:
            for change in changes:
                apply_change(db, change)
            # Persisted in the SAME transaction as the data -> never confirm a
            # position whose changes are not durable in the replica.
            store.upsert_meta(db, "last_applied_lsn", commit_lsn)
            db.commit()
        except Exception:
            db.rollback()
            raise
        finally:
            db.close()
        self.last_applied_lsn = commit_lsn
        # Only now tell the source it may recycle WAL up to here.
        self.cursor.send_feedback(flush_lsn=commit_lsn, force=True)
        STATUS.txn_applied(len(changes), commit_lsn)


class Worker:
    """The single collector pipeline. Runs in one background thread."""

    def __init__(self):
        self._lock = threading.Lock()
        self._thread = None
        self.stop_event = threading.Event()

    # ------------------------------------------------------------------ API
    def start(self):
        with self._lock:
            if self._thread and self._thread.is_alive():
                return False, "sync already running"
            self.stop_event.clear()
            self._thread = threading.Thread(
                target=self._run, name="sync-worker", daemon=True
            )
            self._thread.start()
            return True, "sync started"

    def stop(self, timeout=30):
        thread = self._thread
        if not thread:
            return True
        self.stop_event.set()
        thread.join(timeout=timeout)
        if thread.is_alive():
            log.error("worker thread did not stop within %ss", timeout)
            return False
        return True

    def running(self):
        return bool(self._thread and self._thread.is_alive())

    def reset_local(self):
        """Stop the worker, drop the slot (best effort), wipe the local replica."""
        if not self.stop():
            raise RuntimeError("sync worker did not stop; not resetting")
        try:
            self._drop_slot_if_exists()
        except Exception as exc:
            log.warning("could not drop slot (source offline?): %s", exc)
        store.clear_replica()
        STATUS.reset()

    # -------------------------------------------------------------- pipeline
    def _run(self):
        store.set_meta("sync_requested", "1")
        STATUS.update(started_at=utcnow())
        log.info("sync worker started")
        while not self.stop_event.is_set():
            try:
                if not store.published():
                    self._rebuild_snapshot()
                self._stream()
            except StopStream:
                break
            except FatalSyncError as exc:
                log.error("fatal sync error: %s", exc)
                STATUS.update(phase="error", connected=False, message=str(exc))
                break
            except Exception as exc:  # connection drop, source restart, ...
                log.warning("sync interrupted: %s", exc)
                STATUS.update(phase="disconnected", connected=False, message=str(exc))
                self.stop_event.wait(config.RECONNECT_DELAY_S)
        STATUS.update(connected=False)
        if STATUS.snapshot()["phase"] != "error":
            STATUS.update(phase="idle", message="worker stopped")
        log.info("sync worker stopped")

    def _rebuild_snapshot(self):
        """Full rebuild: fresh slot + exported snapshot, atomic publish."""
        if not SLOT_NAME_RE.match(config.SLOT_NAME):
            raise FatalSyncError(f"invalid slot name: {config.SLOT_NAME!r}")
        STATUS.update(
            phase="snapshot", message="preparing snapshot", rows_copied=0, total_rows=0
        )
        self._drop_slot_if_exists()

        # Close the replica while rebuilding (contract: not open until complete).
        db = store.connect()
        try:
            db.execute("DELETE FROM stations")
            db.execute("DELETE FROM readings")
            store.upsert_meta(db, "snapshot_done", "0")
            store.upsert_meta(db, "last_applied_lsn", "0")
            db.commit()
        finally:
            db.close()

        repl = psycopg2.connect(
            config.PG_DSN,
            connection_factory=LogicalReplicationConnection,
            connect_timeout=config.CONNECT_TIMEOUT_S,
        )
        src = None
        snap_db = None
        try:
            # 1. Create the slot and export a snapshot at its consistent point.
            #    The replication connection must stay open while the snapshot
            #    is in use.
            rcur = repl.cursor()
            rcur.execute(
                f"CREATE_REPLICATION_SLOT {config.SLOT_NAME} "
                "LOGICAL test_decoding (SNAPSHOT 'export')"
            )
            _slot, consistent_point, snapshot_name, _plugin = rcur.fetchone()
            if not snapshot_name or not SNAPSHOT_NAME_RE.match(snapshot_name):
                raise FatalSyncError(f"unexpected exported snapshot {snapshot_name!r}")
            base_lsn = lsn_to_int(consistent_point)
            log.info(
                "slot %s created at %s, exported snapshot %s",
                config.SLOT_NAME, consistent_point, snapshot_name,
            )

            # 2. Copy both tables inside the exported snapshot.
            src = psycopg2.connect(config.PG_DSN, connect_timeout=config.CONNECT_TIMEOUT_S)
            src.set_session(isolation_level="REPEATABLE READ", readonly=True)
            scur = src.cursor()
            scur.execute(f"SET TRANSACTION SNAPSHOT '{snapshot_name}'")

            total = 0
            for table, _cols in SNAPSHOT_TABLES:
                scur.execute(f"SELECT count(*) FROM {table}")
                total += scur.fetchone()[0]
            STATUS.update(total_rows=total, message="copying snapshot")

            # 3. Single SQLite transaction: readers see the replica only once
            #    the whole snapshot (plus its meta marker) is committed.
            snap_db = store.connect()
            copied = 0
            try:
                for table, cols in SNAPSHOT_TABLES:
                    scur.execute(f"SELECT {cols} FROM {table} ORDER BY id")
                    for row in scur.fetchall():
                        if self.stop_event.is_set():
                            raise StopStream()
                        snap_db.execute(UPSERT_SQL[table], tuple(row))
                        copied += 1
                        if copied % 50 == 0:
                            STATUS.update(rows_copied=copied)
                        if config.SNAPSHOT_ROW_DELAY_MS:
                            time.sleep(config.SNAPSHOT_ROW_DELAY_MS / 1000.0)
                store.upsert_meta(snap_db, "snapshot_done", "1")
                store.upsert_meta(snap_db, "last_applied_lsn", base_lsn)
                store.upsert_meta(snap_db, "slot_name", config.SLOT_NAME)
                store.upsert_meta(snap_db, "published_at", utcnow())
                snap_db.commit()
            except Exception:
                snap_db.rollback()
                raise
            STATUS.update(rows_copied=copied, message="snapshot committed")
            log.info("snapshot committed: %d rows, base lsn %d", copied, base_lsn)
        finally:
            for res in (src, repl):
                if res is not None:
                    try:
                        res.close()
                    except Exception:
                        pass
            if snap_db is not None:
                snap_db.close()

    def _stream(self):
        last_applied = int(store.get_meta("last_applied_lsn", "0"))
        repl = psycopg2.connect(
            config.PG_DSN,
            connection_factory=LogicalReplicationConnection,
            connect_timeout=config.CONNECT_TIMEOUT_S,
        )
        try:
            cur = repl.cursor()
            consumer = ChangeConsumer(self, cur, last_applied)
            try:
                cur.start_replication(
                    slot_name=config.SLOT_NAME,
                    slot_type=REPLICATION_LOGICAL,
                    decode=False,
                    status_interval=config.STATUS_INTERVAL_S,
                )
            except Exception as exc:
                msg = str(exc)
                if (
                    "does not exist" in msg
                    or "logical decoding requires" in msg
                    or "wal_level" in msg
                ):
                    raise FatalSyncError(msg.strip()) from exc
                raise
            STATUS.update(
                phase="streaming", connected=True,
                message="receiving logical changes",
            )
            log.info(
                "streaming from slot %s (last applied lsn=%d)",
                config.SLOT_NAME, last_applied,
            )
            self._consume_loop(cur, consumer)
        finally:
            try:
                repl.close()
            except Exception:
                pass

    def _consume_loop(self, cur, consumer):
        """Non-blocking consume loop.

        Uses read_message() (non-blocking) + select() so the stop request is
        honored within ~1s even when the stream is idle, and so a dropped
        connection surfaces as an exception promptly.
        """
        conn = cur.connection
        while not self.stop_event.is_set():
            msg = cur.read_message()  # raises on connection failure
            if msg is None:
                # Nothing to read: wait for readability, then advertise our
                # durable position so the slot can recycle WAL.
                select.select([conn], [], [], 1.0)
                cur.send_feedback(flush_lsn=consumer.last_applied_lsn)
                continue
            consumer(msg)

    def _drop_slot_if_exists(self):
        conn = psycopg2.connect(
            config.PG_DSN, connect_timeout=config.CONNECT_TIMEOUT_S
        )
        conn.autocommit = True
        try:
            cur = conn.cursor()
            cur.execute(
                "SELECT active FROM pg_replication_slots WHERE slot_name = %s",
                (config.SLOT_NAME,),
            )
            row = cur.fetchone()
            if row:
                if row[0]:
                    raise FatalSyncError(
                        f"replication slot {config.SLOT_NAME} is active on another "
                        "connection - is a second collector running?"
                    )
                cur.execute("SELECT pg_drop_replication_slot(%s)", (config.SLOT_NAME,))
                log.info("dropped existing slot %s", config.SLOT_NAME)
        finally:
            conn.close()


WORKER = Worker()
