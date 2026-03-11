"""
Database abstraction layer — supports both SQLite and PostgreSQL.

When DATABASE_URL is set (PostgreSQL), all connections go through psycopg2
with automatic query translation (? → %s, SQLite syntax → PG syntax).
Otherwise falls back to SQLite in data/raswet_gifts.db.
"""
import os
import re
import sqlite3
import logging
import time

logger = logging.getLogger(__name__)

DATABASE_URL = os.environ.get('DATABASE_URL', '')
USE_POSTGRES = bool(DATABASE_URL)

# ⚠️ ВАЖНО: Логируем какая БД используется при импорте модуля
if USE_POSTGRES:
    print(f"🐘 DATABASE MODE: PostgreSQL (DATABASE_URL set)")
    logger.info(f"🐘 Using PostgreSQL database")
else:
    print("⚠️ DATABASE MODE: SQLite (WARNING: Data will be LOST on redeploy!)")
    print("⚠️ Set DATABASE_URL environment variable for persistent storage!")
    logger.warning("⚠️ Using SQLite - data will be lost on redeploy!")

# ── PostgreSQL support via psycopg2 ──────────────────────────────
if USE_POSTGRES:
    try:
        import psycopg2
        import psycopg2.extras
        import psycopg2.pool
        _pg_pool = None
        print("✅ psycopg2 loaded successfully")
    except ImportError:
        print("❌ psycopg2 not installed, falling back to SQLite!")
        logger.error("psycopg2 not installed, falling back to SQLite")
        USE_POSTGRES = False


def _translate_query(sql):
    """Convert SQLite-flavoured SQL to PostgreSQL-compatible SQL."""
    if not USE_POSTGRES:
        return sql
    # ? → %s parameter markers
    out = sql.replace('?', '%s')
    # id INTEGER PRIMARY KEY AUTOINCREMENT → id SERIAL PRIMARY KEY
    out = re.sub(
        r'\bid\s+INTEGER\s+PRIMARY\s+KEY\s+AUTOINCREMENT\b',
        'id SERIAL PRIMARY KEY',
        out, flags=re.IGNORECASE
    )
    # Remaining id INTEGER PRIMARY KEY (no autoincrement) → id BIGINT PRIMARY KEY
    # These tables receive explicit IDs (e.g. users.id = telegram user_id)
    out = re.sub(
        r'\bid\s+INTEGER\s+PRIMARY\s+KEY\b',
        'id BIGINT PRIMARY KEY',
        out, flags=re.IGNORECASE
    )
    # datetime('now') → NOW()
    out = re.sub(r"datetime\(\s*'now'\s*\)", 'NOW()', out, flags=re.IGNORECASE)
    # date('now') → CURRENT_DATE
    out = re.sub(r"date\(\s*'now'\s*\)", 'CURRENT_DATE', out, flags=re.IGNORECASE)
    # date('now','start of month') → date_trunc('month', CURRENT_TIMESTAMP)
    out = re.sub(
        r"date\(\s*'now'\s*,\s*'start of month'\s*\)",
        "date_trunc('month', CURRENT_TIMESTAMP)",
        out, flags=re.IGNORECASE
    )
    # INSERT OR IGNORE → INSERT ... ON CONFLICT DO NOTHING
    _is_insert_or_ignore = bool(re.search(r'\bINSERT\s+OR\s+IGNORE\b', out, re.IGNORECASE))
    # INSERT OR REPLACE → upsert with ON CONFLICT DO UPDATE
    _replace_match = re.match(
        r'\s*INSERT\s+OR\s+REPLACE\s+INTO\s+(\w+)\s*\(([^)]+)\)\s*VALUES\s*\((.+)\)\s*;?\s*$',
        out, re.IGNORECASE | re.DOTALL
    )
    if _replace_match:
        _UPSERT_CONFLICT = {
            'case_limits': 'case_id',
            'crash_customizations': 'item_type, item_id',
            'levels': 'level',
        }
        tbl = _replace_match.group(1).lower()
        cols_str = _replace_match.group(2)
        vals_str = _replace_match.group(3)
        conflict_col = _UPSERT_CONFLICT.get(tbl)
        if conflict_col:
            cols = [c.strip() for c in cols_str.split(',')]
            conflict_set = {c.strip() for c in conflict_col.split(',')}
            update_cols = [c for c in cols if c not in conflict_set and c != 'id']
            set_clause = ', '.join(f"{c} = EXCLUDED.{c}" for c in update_cols)
            out = f"INSERT INTO {tbl} ({cols_str}) VALUES ({vals_str}) ON CONFLICT ({conflict_col}) DO UPDATE SET {set_clause}"
        else:
            out = re.sub(r'\bINSERT\s+OR\s+REPLACE\b', 'INSERT', out, flags=re.IGNORECASE)
    else:
        out = re.sub(r'\bINSERT\s+OR\s+REPLACE\b', 'INSERT', out, flags=re.IGNORECASE)
    out = re.sub(r'\bINSERT\s+OR\s+IGNORE\b', 'INSERT', out, flags=re.IGNORECASE)
    # Remove PRAGMA statements entirely
    if re.match(r'^\s*PRAGMA\b', out, re.IGNORECASE):
        return ''
    # BEGIN IMMEDIATE → BEGIN (PG doesn't support IMMEDIATE)
    out = re.sub(r'\bBEGIN\s+IMMEDIATE\b', 'BEGIN', out, flags=re.IGNORECASE)
    # sqlite_master table listing → pg equivalent
    if 'sqlite_master' in out.lower():
        out = re.sub(
            r"SELECT\s+name\s+FROM\s+sqlite_master\s+WHERE\s+type\s*=\s*'table'",
            "SELECT tablename AS name FROM pg_tables WHERE schemaname = 'public'",
            out, flags=re.IGNORECASE
        )
    # DATETIME (SQLite) -> TIMESTAMP (Postgres)
    out = re.sub(r'\bDATETIME\b', 'TIMESTAMP', out, flags=re.IGNORECASE)
    # BOOLEAN defaults in SQLite are often written as 0/1 — convert to TRUE/FALSE for Postgres
    out = re.sub(r'\bBOOLEAN\b\s+DEFAULT\s+0\b', 'BOOLEAN DEFAULT FALSE', out, flags=re.IGNORECASE)
    out = re.sub(r'\bBOOLEAN\b\s+DEFAULT\s+1\b', 'BOOLEAN DEFAULT TRUE', out, flags=re.IGNORECASE)
    # SQLite uses "value" as string literal; PG treats "value" as identifier.
    # Convert double-quoted values in SET/WHERE clauses to single quotes.
    # Match = "word" patterns (status = "crashed", etc.)
    out = re.sub(r'''=\s*"([^"]*)"''', r"= '\1'", out)
    # Append ON CONFLICT DO NOTHING for INSERT OR IGNORE queries
    if _is_insert_or_ignore:
        out = out.rstrip().rstrip(';') + ' ON CONFLICT DO NOTHING'
    return out


class PgCursorWrapper:
    """Wraps a psycopg2 cursor so it auto-translates queries."""
    def __init__(self, real_cursor):
        self._cur = real_cursor

    def execute(self, sql, params=None):
        translated = _translate_query(sql)
        if not translated.strip():
            return  # PRAGMA → skip
        if params:
            # psycopg2 wants tuples
            if isinstance(params, list):
                params = tuple(params)
            self._cur.execute(translated, params)
        else:
            self._cur.execute(translated)

    def executemany(self, sql, seq_of_params):
        translated = _translate_query(sql)
        if not translated.strip():
            return
        self._cur.executemany(translated, seq_of_params)

    def fetchone(self):
        return self._cur.fetchone()

    def fetchall(self):
        return self._cur.fetchall()

    def fetchmany(self, size=None):
        if size is not None:
            return self._cur.fetchmany(size)
        return self._cur.fetchmany()

    @property
    def lastrowid(self):
        # PostgreSQL doesn't have lastrowid the same way
        # Use RETURNING id instead in queries; but for compat:
        try:
            self._cur.execute("SELECT lastval()")
            return self._cur.fetchone()[0]
        except Exception:
            return None

    @property
    def rowcount(self):
        return self._cur.rowcount

    @property
    def description(self):
        return self._cur.description

    def close(self):
        self._cur.close()


class PgConnectionWrapper:
    """Wraps a psycopg2 connection to look like sqlite3.Connection."""
    def __init__(self, pg_conn):
        self._conn = pg_conn

    def cursor(self):
        return PgCursorWrapper(self._conn.cursor())

    def execute(self, sql, params=None):
        cur = self.cursor()
        cur.execute(sql, params)
        return cur

    def commit(self):
        self._conn.commit()

    def rollback(self):
        self._conn.rollback()

    def close(self):
        if USE_POSTGRES and _pg_pool:
            _pg_pool.putconn(self._conn)
        else:
            self._conn.close()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()


def _init_pg_pool():
    """Initialize the PostgreSQL connection pool (called once)."""
    global _pg_pool
    if _pg_pool is not None:
        return
    try:
        _pg_pool = psycopg2.pool.ThreadedConnectionPool(
            minconn=2, maxconn=30,
            dsn=DATABASE_URL
        )
        logger.info("✅ PostgreSQL connection pool initialized (max=30)")
    except Exception as e:
        logger.error(f"❌ Failed to init PG pool: {e}")
        raise


def get_pg_connection():
    """Get a PostgreSQL connection from the pool, wrapped for compatibility."""
    if _pg_pool is None:
        _init_pg_pool()
    raw = _pg_pool.getconn()
    raw.autocommit = False
    return PgConnectionWrapper(raw)


# ── Public API ───────────────────────────────────────────────────

def get_connection(db_path=None, timeout=30):
    """
    Drop-in replacement for sqlite3.connect() / get_db_connection().
    When DATABASE_URL is set → PostgreSQL; otherwise → SQLite.
    """
    if USE_POSTGRES:
        return get_pg_connection()
    # SQLite fallback
    return sqlite3.connect(db_path or 'data/raswet_gifts.db',
                           timeout=timeout, check_same_thread=False,
                           isolation_level='DEFERRED')


def create_tables_pg(conn):
    """Create all tables in PostgreSQL using PG-compatible DDL.
    Called once during startup when USE_POSTGRES is True."""
    cur = conn.cursor()
    # The _create_all_tables in app.py will be called through the wrapper,
    # which auto-translates SQLite DDL → PG DDL via _translate_query().
    # This is just a safety check for PG-specific setup.
    cur.execute("""
        SELECT tablename FROM pg_tables 
        WHERE schemaname = 'public'
    """)
    tables = {r[0] for r in cur.fetchall()}
    conn.commit()
    logger.info(f"PostgreSQL tables found: {len(tables)}")
    return tables
