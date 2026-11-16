"""Runtime configuration (environment variables)."""
import os

# Source PostgreSQL DSN (normal, non-replication connection string).
PG_DSN = os.environ.get(
    "PG_DSN", "postgresql://postgres:postgres@localhost:5432/appdb"
)

# Local replica database file.
SQLITE_PATH = os.environ.get("SQLITE_PATH", "./replica.db")

# Logical replication slot name on the source.
SLOT_NAME = os.environ.get("SLOT_NAME", "collector_slot")

# Artificial per-row delay during the initial snapshot copy (demo aid, 0 in prod).
SNAPSHOT_ROW_DELAY_MS = int(os.environ.get("SNAPSHOT_ROW_DELAY_MS", "0"))

# Delay between reconnect attempts after a connection failure.
RECONNECT_DELAY_S = float(os.environ.get("RECONNECT_DELAY_S", "2"))

# How often the server sends keepalives while streaming (also bounds stop latency).
STATUS_INTERVAL_S = int(os.environ.get("STATUS_INTERVAL_S", "2"))

# Connect timeout for source connections.
CONNECT_TIMEOUT_S = int(os.environ.get("CONNECT_TIMEOUT_S", "5"))
