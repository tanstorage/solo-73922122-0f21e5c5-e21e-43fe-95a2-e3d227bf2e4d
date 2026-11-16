import unittest

from app.decoder import is_begin, is_commit, parse_change, parse_fields


class DecoderTest(unittest.TestCase):
    def test_insert(self):
        ch = parse_change(
            "table public.stations: INSERT: id[integer]:1 correction[integer]:-5"
        )
        self.assertEqual(ch, ("stations", "INSERT", None, {"id": 1, "correction": -5}))

    def test_update(self):
        ch = parse_change(
            "table public.readings: UPDATE: id[integer]:2 station_id[integer]:3 "
            "value[integer]:42"
        )
        self.assertEqual(
            ch, ("readings", "UPDATE", None, {"id": 2, "station_id": 3, "value": 42})
        )

    def test_update_old_key(self):
        ch = parse_change(
            "table public.stations: UPDATE: old-key: id[integer]:1 "
            "new-tuple: id[integer]:5 correction[integer]:9"
        )
        self.assertEqual(
            ch, ("stations", "UPDATE", {"id": 1}, {"id": 5, "correction": 9})
        )

    def test_delete(self):
        ch = parse_change("table public.readings: DELETE: id[integer]:7")
        self.assertEqual(ch, ("readings", "DELETE", {"id": 7}, None))

    def test_quoted_text_and_null(self):
        self.assertEqual(
            parse_fields("a[text]:'it''s' b[integer]:3 c[integer]:NULL"),
            {"a": "it's", "b": 3, "c": None},
        )

    def test_begin_commit(self):
        self.assertTrue(is_begin("BEGIN 529"))
        self.assertTrue(is_begin("BEGIN 529;"))
        self.assertTrue(is_commit("COMMIT 529"))
        self.assertTrue(is_commit("COMMIT 529;"))
        self.assertFalse(is_commit("BEGIN 529"))
        self.assertFalse(is_begin("table public.t: INSERT: id[integer]:1"))

    def test_non_change_lines(self):
        self.assertIsNone(parse_change("some garbage"))
        self.assertIsNone(parse_change("BEGIN 1"))
        self.assertIsNone(
            parse_change("table public.stations: TRUNCATE: (no-flags)")
        )


if __name__ == "__main__":
    unittest.main()
