"""Parser for PostgreSQL test_decoding output.

test_decoding emits one text message per transaction command, e.g.:

    BEGIN 529
    table public.stations: INSERT: id[integer]:1 correction[integer]:-3
    table public.readings: UPDATE: id[integer]:7 station_id[integer]:2 value[integer]:40
    table public.readings: DELETE: id[integer]:9
    table public.stations: UPDATE: old-key: id[integer]:1 new-tuple: id[integer]:4 correction[integer]:8
    COMMIT 529

Only row-level INSERT/UPDATE/DELETE are relevant here (the source never does
DDL or TRUNCATE per the data contract).
"""
import re

BEGIN_RE = re.compile(r"^BEGIN\b")
COMMIT_RE = re.compile(r"^COMMIT\b")
CHANGE_RE = re.compile(
    r"^table\s+(?P<schema>[A-Za-z_][\w$]*)\.(?P<table>[A-Za-z_][\w$]*):\s+"
    r"(?P<op>INSERT|UPDATE|DELETE|TRUNCATE):(?P<rest>.*)$"
)
# name[type]:value  where value is NULL, a quoted string, or a bare token
FIELD_RE = re.compile(r"([A-Za-z_][\w$]*)\[[^\]]*\]:(NULL|'(?:[^']|'')*'|[^\s]+)")

OLD_KEY_PREFIX = "old-key:"
NEW_TUPLE_MARKER = " new-tuple: "


def parse_fields(text):
    """Parse a `name[type]:value ...` sequence into a dict."""
    fields = {}
    for name, raw in FIELD_RE.findall(text):
        if raw == "NULL":
            fields[name] = None
        elif raw.startswith("'"):
            fields[name] = raw[1:-1].replace("''", "'")
        else:
            try:
                fields[name] = int(raw)
            except ValueError:
                fields[name] = raw
    return fields


def parse_change(line):
    """Parse one change line.

    Returns (table, op, old, new) where old/new are column dicts (or None),
    or None if the line is not a supported row change.
    """
    m = CHANGE_RE.match(line.strip())
    if not m:
        return None
    table = m.group("table")
    op = m.group("op")
    rest = m.group("rest").strip()
    if op == "INSERT":
        return (table, "INSERT", None, parse_fields(rest))
    if op == "DELETE":
        # REPLICA IDENTITY DEFAULT: only the key columns of the old row
        return (table, "DELETE", parse_fields(rest), None)
    if op == "UPDATE":
        if rest.startswith(OLD_KEY_PREFIX) and NEW_TUPLE_MARKER in rest:
            # primary key changed: old-key + new-tuple
            head, _, tail = rest[len(OLD_KEY_PREFIX):].partition(NEW_TUPLE_MARKER)
            return (table, "UPDATE", parse_fields(head), parse_fields(tail))
        # full new row image (key columns included)
        return (table, "UPDATE", None, parse_fields(rest))
    return None  # TRUNCATE etc. are out of contract


def is_begin(line):
    return bool(BEGIN_RE.match(line.strip()))


def is_commit(line):
    return bool(COMMIT_RE.match(line.strip()))
