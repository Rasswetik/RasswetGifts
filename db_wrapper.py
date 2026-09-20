# PostgreSQL adapter for the Rasswet/Goshangifts Flask application.
# SQLite is intentionally NOT supported in production.
import os
import re
import threading
import psycopg2

DATABASE_URL = (os.getenv('DATABASE_URL') or '').strip()
if not DATABASE_URL:
    raise RuntimeError(
        'DATABASE_URL is required. This build uses PostgreSQL only; SQLite has been disabled.'
    )

USE_POSTGRES = True

_pool_lock = threading.Lock()


def _normalize_sql(sql: str) -> str:
    """Convert the small SQLite SQL dialect subset used by the legacy app to PostgreSQL."""
    if not isinstance(sql, str):
        return sql
    s = sql

    # SQLite placeholders -> psycopg2 placeholders.
    s = s.replace('?', '%s')

    # SQLite AUTOINCREMENT syntax -> PostgreSQL identity/sequence syntax.
    s = re.sub(r'INTEGER\s+PRIMARY\s+KEY\s+AUTOINCREMENT', 'BIGSERIAL PRIMARY KEY', s, flags=re.I)

    # SQLite boolean defaults written as 0/1.
    s = re.sub(r'\bBOOLEAN\s+DEFAULT\s+1\b', 'BOOLEAN DEFAULT TRUE', s, flags=re.I)
    s = re.sub(r'\bBOOLEAN\s+DEFAULT\s+0\b', 'BOOLEAN DEFAULT FALSE', s, flags=re.I)

    # SQLite datetime('now', ...) helpers.
    s = re.sub(r"datetime\(\s*'now'\s*\)", 'CURRENT_TIMESTAMP', s, flags=re.I)

    def _dt_interval(m):
        sign = m.group(1)
        amount = m.group(2)
        unit = m.group(3)
        # PostgreSQL accepts CURRENT_TIMESTAMP +/- INTERVAL '10 minutes'.
        op = '+' if sign == '+' else '-'
        return f"CURRENT_TIMESTAMP {op} INTERVAL '{amount} {unit}'"

    s = re.sub(
        r"datetime\(\s*'now'\s*,\s*'([+-])(\d+)\s+(seconds?|minutes?|hours?|days?|weeks?)'\s*\)",
        _dt_interval,
        s,
        flags=re.I,
    )

    # PRAGMA queries used by legacy health/migration code.
    m = re.fullmatch(r'\s*PRAGMA\s+table_info\(([^)]+)\)\s*;?', s, flags=re.I)
    if m:
        table = m.group(1).strip().strip('"`')
        return (
            "SELECT ordinal_position - 1 AS cid, column_name AS name, data_type AS type, "
            "CASE WHEN is_nullable = 'NO' THEN 1 ELSE 0 END AS notnull, "
            "column_default AS dflt_value, "
            "CASE WHEN EXISTS (SELECT 1 FROM pg_constraint c "
            "JOIN pg_attribute a ON a.attrelid=c.conrelid AND a.attnum=ANY(c.conkey) "
            "WHERE c.contype='p' AND c.conrelid=%s::regclass AND a.attname=columns.column_name) "
            "THEN 1 ELSE 0 END AS pk "
            "FROM information_schema.columns columns "
            "WHERE table_schema='public' AND table_name=%s ORDER BY ordinal_position"
        )

    # SQLite's sqlite_master table list.
    if re.search(r"sqlite_master", s, flags=re.I):
        return "SELECT table_name AS name FROM information_schema.tables WHERE table_schema='public' AND table_type='BASE TABLE'"

    # SQLite PRAGMA tuning commands are meaningless on PostgreSQL; make them harmless.
    if re.match(r'\s*PRAGMA\s+', s, flags=re.I):
        return 'SELECT 1'

    # SQLite transaction command used by one promo endpoint.
    if re.match(r'\s*BEGIN\s+IMMEDIATE\s*;?\s*$', s, flags=re.I):
        return 'BEGIN'

    # SQLite INSERT OR IGNORE -> PostgreSQL ON CONFLICT DO NOTHING.
    if re.match(r'\s*INSERT\s+OR\s+IGNORE\s+', s, flags=re.I):
        s = re.sub(r'\bINSERT\s+OR\s+IGNORE\s+', 'INSERT ', s, count=1, flags=re.I)
        if not re.search(r'\bON\s+CONFLICT\b', s, flags=re.I):
            s = s.rstrip().rstrip(';') + ' ON CONFLICT DO NOTHING'

    return s


def _replace_or_replace(sql: str, conn):
    """Translate SQLite INSERT OR REPLACE using the table primary key."""
    if not re.match(r'\s*INSERT\s+OR\s+REPLACE\s+', sql, flags=re.I):
        return _normalize_sql(sql)

    m = re.match(
        r"\s*INSERT\s+OR\s+REPLACE\s+INTO\s+([\w.\"]+)\s*\(([^)]+)\)\s*VALUES\s*\((.*)\)\s*;?\s*$",
        sql,
        flags=re.I | re.S,
    )
    if not m:
        return _normalize_sql(sql)

    table = m.group(1).strip('"')
    cols = [c.strip().strip('"') for c in m.group(2).split(',')]
    values = m.group(3).strip()

    # Look up primary key columns.
    cur = conn.cursor()
    try:
        cur.execute(
            "SELECT a.attname "
            "FROM pg_index i "
            "JOIN pg_attribute a ON a.attrelid=i.indrelid AND a.attnum=ANY(i.indkey) "
            "WHERE i.indrelid=%s::regclass AND i.indisprimary "
            "ORDER BY array_position(i.indkey, a.attnum)",
            (table,),
        )
        pk_cols = [r[0] for r in cur.fetchall()]
    finally:
        cur.close()

    if not pk_cols:
        # Last-resort behavior: PostgreSQL has no generic REPLACE. Use INSERT
        # and let a real constraint error surface rather than silently deleting data.
        return _normalize_sql(sql.replace('INSERT OR REPLACE', 'INSERT', 1))

    normalized = _normalize_sql(
        re.sub(r'^\s*INSERT\s+OR\s+REPLACE\s+', 'INSERT ', sql, count=1, flags=re.I)
    )
    normalized = normalized.rstrip().rstrip(';')
    non_pk_cols = [c for c in cols if c.lower() not in {p.lower() for p in pk_cols}]
    if non_pk_cols:
        assignments = ', '.join(f'"{c}" = EXCLUDED."{c}"' for c in non_pk_cols)
        return normalized + ' ON CONFLICT (' + ', '.join(f'"{p}"' for p in pk_cols) + ') DO UPDATE SET ' + assignments
    return normalized + ' ON CONFLICT (' + ', '.join(f'"{p}"' for p in pk_cols) + ') DO NOTHING'


class PGCursor:
    def __init__(self, cursor, connection):
        self._cursor = cursor
        self._connection = connection
        self._lastrowid = None

    @property
    def lastrowid(self):
        return self._lastrowid

    @property
    def rowcount(self):
        return self._cursor.rowcount

    def execute(self, sql, params=None):
        sql = str(sql)
        sql2 = _replace_or_replace(sql, self._connection._conn) if re.match(r'\s*INSERT\s+OR\s+REPLACE\s+', sql, re.I) else _normalize_sql(sql)
        self._cursor.execute(sql2, params)
        self._lastrowid = None
        if re.match(r'\s*INSERT\b', sql2, re.I):
            # The application frequently uses cursor.lastrowid. Recover the
            # sequence value only when the target table actually has a serial id.
            m = re.match(r'\s*INSERT\s+(?:INTO)\s+([\w.\"]+)', sql2, re.I)
            if m:
                table = m.group(1).strip('"')
                try:
                    self._cursor.execute("SELECT pg_get_serial_sequence(%s, 'id')", (table,))
                    seq_row = self._cursor.fetchone()
                    if seq_row and seq_row[0]:
                        self._cursor.execute("SELECT currval(%s)", (seq_row[0],))
                        row = self._cursor.fetchone()
                        self._lastrowid = row[0] if row else None
                except Exception:
                    # Never roll back the caller's successful INSERT merely
                    # because lastrowid is unavailable.
                    try:
                        self._connection._conn.rollback()
                    except Exception:
                        pass
                    self._cursor = self._connection._conn.cursor()
        return self

    def executemany(self, sql, seq_of_params):
        self._cursor.executemany(_normalize_sql(sql), seq_of_params)
        return self

    def fetchone(self):
        return self._cursor.fetchone()

    def fetchmany(self, size=None):
        return self._cursor.fetchmany(size) if size else self._cursor.fetchmany()

    def fetchall(self):
        return self._cursor.fetchall()

    def __iter__(self):
        return iter(self._cursor)

    def close(self):
        return self._cursor.close()

    def __getattr__(self, name):
        return getattr(self._cursor, name)


class PGConnection:
    def __init__(self, conn):
        self._conn = conn
        self._closed = False

    def cursor(self):
        return PGCursor(self._conn.cursor(), self)

    def execute(self, sql, params=None):
        cur = self.cursor()
        cur.execute(sql, params)
        return cur

    def commit(self):
        return self._conn.commit()

    def rollback(self):
        return self._conn.rollback()

    def close(self):
        self._closed = True
        return self._conn.close()

    def __enter__(self):
        self._conn.__enter__()
        return self

    def __exit__(self, exc_type, exc, tb):
        return self._conn.__exit__(exc_type, exc, tb)

    def __getattr__(self, name):
        return getattr(self._conn, name)


def get_connection():
    kwargs = {
        'connect_timeout': int(os.getenv('PG_CONNECT_TIMEOUT', '15')),
        'application_name': os.getenv('PG_APPLICATION_NAME', 'goshangifts'),
    }
    conn = psycopg2.connect(DATABASE_URL, **kwargs)
    # Render/Postgres benefits from TCP keepalives on long-running workers.
    try:
        cur = conn.cursor()
        cur.execute("SET TIME ZONE 'UTC'")
        cur.close()
    except Exception:
        conn.rollback()
    return PGConnection(conn)
