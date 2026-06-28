from .source import fetch
from .store import apply_page, cursor, validate_item
from psycopg.types.json import Jsonb


def sync(conn, base, limit=7, after_write=None):
    if not 1 <= limit <= 100:
        raise ValueError("page size must be between 1 and 100")
    while True:
        start = cursor(conn)
        page = fetch(base, "changes", start, limit)
        if type(page.get("done")) is not bool:
            raise ValueError("missing completion flag")
        if not page["items"] and not page["done"]:
            raise ValueError("non-progressing change feed")
        if apply_page(conn, start, page, after_write) and page["done"]:
            return page["next"]


def reconcile(conn, base, limit=7):
    """Stage a consistent snapshot and atomically replace records and checkpoint."""
    if not 1 <= limit <= 100:
        raise ValueError("page size must be between 1 and 100")
    with conn.transaction():
        start = cursor(conn, lock=True)
        conn.execute("CREATE TEMP TABLE snapshot_stage (LIKE records INCLUDING ALL) ON COMMIT DROP")
        offset, watermark = 0, None
        while True:
            page = fetch(base, "snapshot", offset, limit)
            current = page["watermark"]
            if type(current) is not int or current < start:
                raise ValueError("snapshot is older than checkpoint")
            if watermark is not None and watermark != current:
                raise ValueError("snapshot watermark changed")
            watermark = current
            if type(page["done"]) is not bool or page["next"] != offset + len(page["items"]):
                raise ValueError("invalid snapshot pagination")
            for item in page["items"]:
                validate_item(item)
                if item["value"] is None or item["seq"] > watermark:
                    raise ValueError("invalid snapshot record")
                conn.execute("INSERT INTO snapshot_stage VALUES (%s, %s, %s)",
                             (item["id"], item["seq"], Jsonb(item["value"])))
            if page["done"]:
                break
            if page["next"] <= offset:
                raise ValueError("non-progressing snapshot")
            offset = page["next"]
        conn.execute("DELETE FROM records")
        conn.execute("INSERT INTO records SELECT * FROM snapshot_stage")
        conn.execute("UPDATE checkpoint SET cursor = %s", (watermark,))
    return watermark
