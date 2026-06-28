import psycopg
from psycopg.types.json import Jsonb


DDL = """
CREATE TABLE IF NOT EXISTS checkpoint (
    singleton boolean PRIMARY KEY DEFAULT true CHECK (singleton),
    cursor bigint NOT NULL CHECK (cursor >= 0)
);
INSERT INTO checkpoint VALUES (true, 0) ON CONFLICT DO NOTHING;
CREATE TABLE IF NOT EXISTS records (
    id text PRIMARY KEY,
    version bigint NOT NULL CHECK (version > 0),
    value jsonb
);
CREATE TABLE IF NOT EXISTS applied_events (
    seq bigint PRIMARY KEY,
    record_id text NOT NULL
);
"""


def connect(dsn):
    return psycopg.connect(dsn, autocommit=True)


def initialize(conn):
    with conn.transaction():
        conn.execute(DDL)


def cursor(conn, lock=False):
    suffix = " FOR UPDATE" if lock else ""
    return conn.execute("SELECT cursor FROM checkpoint WHERE singleton" + suffix).fetchone()[0]


def validate_item(item):
    if (not isinstance(item, dict) or not isinstance(item.get("id"), str)
            or not item["id"] or type(item.get("seq")) is not int or item["seq"] <= 0
            or "value" not in item
            or (item["value"] is not None and not isinstance(item["value"], dict))):
        raise ValueError("invalid record")


def upsert(conn, item):
    conn.execute("""INSERT INTO records VALUES (%s, %s, %s)
        ON CONFLICT (id) DO UPDATE SET version = EXCLUDED.version, value = EXCLUDED.value
        WHERE records.version < EXCLUDED.version""",
                 (item["id"], item["seq"], Jsonb(item["value"]) if item["value"] is not None else None))


def state(conn):
    return dict(conn.execute("SELECT id, value FROM records WHERE value IS NOT NULL").fetchall())


def apply_page(conn, expected, page, after_write=None):
    items = page["items"]
    if not isinstance(items, list) or type(page["next"]) is not int:
        raise ValueError("invalid page")
    for index, item in enumerate(items, expected + 1):
        validate_item(item)
        if item["seq"] != index:
            raise ValueError("change feed must be contiguous and ordered")
    if page["next"] != expected + len(items):
        raise ValueError("cursor does not match page")
    with conn.transaction():
        if cursor(conn, lock=True) != expected:
            return False
        for item in items:
            conn.execute("INSERT INTO applied_events VALUES (%s, %s) ON CONFLICT DO NOTHING",
                         (item["seq"], item["id"]))
            upsert(conn, item)
            if after_write:
                after_write(item)
        conn.execute("UPDATE checkpoint SET cursor = %s", (page["next"],))
    return True
