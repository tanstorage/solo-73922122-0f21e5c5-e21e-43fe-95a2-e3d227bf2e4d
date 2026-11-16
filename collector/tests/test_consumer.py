import os
import tempfile
import threading
import unittest

from app import config, store
from app.worker import ChangeConsumer


class FakeCursor:
    def __init__(self):
        self.feedback = []

    def send_feedback(self, **kw):
        self.feedback.append(kw)


class FakeMsg:
    def __init__(self, payload, lsn):
        self.payload = payload
        self.data_start = lsn


class FakeWorker:
    def __init__(self):
        self.stop_event = threading.Event()


class ConsumerTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.old_path = config.SQLITE_PATH
        config.SQLITE_PATH = os.path.join(self.tmp, "t.db")
        store.init_db()
        store.set_meta("snapshot_done", "1")
        store.set_meta("last_applied_lsn", "100")
        self.cursor = FakeCursor()
        self.consumer = ChangeConsumer(FakeWorker(), self.cursor, 100)

    def tearDown(self):
        config.SQLITE_PATH = self.old_path

    def feed(self, text, lsn):
        self.consumer(FakeMsg(text, lsn))

    def test_commit_applies_both_tables_atomically(self):
        self.feed(
            "BEGIN 1\n"
            "table public.stations: INSERT: id[integer]:1 correction[integer]:5\n"
            "table public.readings: INSERT: id[integer]:9 station_id[integer]:1 "
            "value[integer]:-3\n"
            "COMMIT 1",
            lsn=150,
        )
        self.assertEqual(store.fetch_stations(), [(1, 5)])
        self.assertEqual(store.fetch_readings(), [(9, 1, -3)])
        self.assertEqual(store.get_meta("last_applied_lsn"), "150")
        # feedback only after the SQLite commit, at the commit LSN
        self.assertEqual(self.cursor.feedback[-1]["flush_lsn"], 150)

    def test_redelivered_commit_is_skipped(self):
        self.feed("BEGIN 1\n"
                  "table public.stations: INSERT: id[integer]:1 correction[integer]:5\n"
                  "COMMIT 1", lsn=150)
        # crash before feedback -> slot re-delivers the same transaction
        consumer2 = ChangeConsumer(FakeWorker(), self.cursor, 150)
        consumer2(FakeMsg("BEGIN 1\n"
                          "table public.stations: INSERT: id[integer]:1 "
                          "correction[integer]:5\nCOMMIT 1", 150))
        self.assertEqual(store.fetch_stations(), [(1, 5)])
        self.assertEqual(store.get_meta("last_applied_lsn"), "150")

    def test_upsert_idempotent_and_delete(self):
        self.feed("BEGIN 1\ntable public.stations: INSERT: id[integer]:1 "
                  "correction[integer]:5\nCOMMIT 1", lsn=150)
        self.feed("BEGIN 2\ntable public.stations: UPDATE: id[integer]:1 "
                  "correction[integer]:8\nCOMMIT 2", lsn=160)
        self.feed("BEGIN 3\ntable public.stations: DELETE: id[integer]:1\n"
                  "COMMIT 3", lsn=170)
        self.assertEqual(store.fetch_stations(), [])

    def test_pk_change_update(self):
        self.feed("BEGIN 1\ntable public.stations: INSERT: id[integer]:1 "
                  "correction[integer]:5\nCOMMIT 1", lsn=150)
        self.feed("BEGIN 2\ntable public.stations: UPDATE: old-key: id[integer]:1 "
                  "new-tuple: id[integer]:2 correction[integer]:9\nCOMMIT 2", lsn=160)
        self.assertEqual(store.fetch_stations(), [(2, 9)])

    def test_keepalive_bytes_ignored(self):
        self.consumer(FakeMsg(b"\x00\x01\x02\x03binary\xff", 0))
        self.assertEqual(store.get_meta("last_applied_lsn"), "100")


if __name__ == "__main__":
    unittest.main()
