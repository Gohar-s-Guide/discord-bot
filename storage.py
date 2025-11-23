import os
import sqlite3
from typing import List, Dict, Optional, Any

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "."))
DB_PATH = os.path.join(ROOT, "data.db")


def _conn():
    return sqlite3.connect(DB_PATH)


def init_db() -> None:
    """Create database and tables if they don't exist."""
    conn = _conn()
    cur = conn.cursor()
    # guild config
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS guilds (
            guild_id TEXT PRIMARY KEY,
            pairing INTEGER,
            partner_log INTEGER
        )
        """
    )
    # key/value per-guild items table: item can be 'pairing', 'partner_log', or other keys
    # guild_items is a simple key/value table where each row is an item and
    # main/test hold integer values. We intentionally do NOT store guild_id as
    # a column; instead the special row with item='id' will hold the two guild ids
    # (main and test) when you populate them.
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS guild_items (
            item TEXT PRIMARY KEY,
            main INTEGER DEFAULT 0,
            test INTEGER DEFAULT 0
        )
        """
    )
    # subjects: global subjects table (no guild_id) - if an old subjects table exists with guild_id
    # migrate by preserving ids so pings.subject_id references remain valid
    cur.execute("PRAGMA table_info(subjects)")
    cols = [r[1] for r in cur.fetchall()]
    if not cols:
        # subjects table doesn't exist; create new global subjects table
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS subjects (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                subject TEXT,
                main_channel INTEGER DEFAULT 0,
                test_channel INTEGER DEFAULT 0,
                category TEXT DEFAULT NULL,
                message TEXT DEFAULT NULL,
                footer TEXT DEFAULT NULL,
                cooldown INTEGER DEFAULT 600
            )
            """
        )
    elif "guild_id" in cols:
        # migration: create new subjects_new without guild_id, copy rows preserving id
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS subjects_new (
                id INTEGER PRIMARY KEY,
                subject TEXT,
                main_channel INTEGER DEFAULT 0,
                test_channel INTEGER DEFAULT 0,
                category TEXT DEFAULT NULL,
                message TEXT DEFAULT NULL,
                footer TEXT DEFAULT NULL,
                cooldown INTEGER DEFAULT 600
            )
            """
        )
        # determine which columns exist in the old table and copy safely
        old_cols = cols
        select_cols = []
        for c in ("id", "subject", "main_channel", "test_channel", "category", "message", "footer", "cooldown"):
            if c in old_cols:
                select_cols.append(c)
            else:
                select_cols.append(f"NULL AS {c}")
        select_sql = ", ".join(select_cols)
        cur.execute(f"INSERT OR IGNORE INTO subjects_new ({', '.join([ 'id','subject','main_channel','test_channel','category','message','footer','cooldown'])}) SELECT {select_sql} FROM subjects")
        # replace old table
        cur.execute("DROP TABLE subjects")
        cur.execute("ALTER TABLE subjects_new RENAME TO subjects")
    else:
        # subjects exists and already is in the desired shape; nothing to do
        pass
    # pings: store separate main_role/test_role for main vs test environments
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS pings (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            subject_id INTEGER,
            ping_value TEXT,
            name TEXT,
            main_role INTEGER DEFAULT 0,
            test_role INTEGER DEFAULT 0,
            last_time REAL DEFAULT 0
        )
        """
    )
    # aliases
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS aliases (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ping_id INTEGER,
            alias TEXT
        )
        """
    )
    # per-guild cog enable/disable
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS guild_cogs (
            guild_id TEXT,
            cog TEXT,
            enabled INTEGER DEFAULT 1,
            PRIMARY KEY (guild_id, cog)
        )
        """
    )
    conn.commit()
    conn.close()


def _ensure_column(table: str, column: str, definition: str) -> None:
    """Ensure a column exists on a table; add it if missing."""
    conn = _conn()
    cur = conn.cursor()
    try:
        cur.execute(f"PRAGMA table_info({table})")
        cols = [r[1] for r in cur.fetchall()]
        if column not in cols:
            cur.execute(f"ALTER TABLE {table} ADD COLUMN {definition}")
            conn.commit()
    finally:
        conn.close()


# Ensure older DBs get the new columns if they were created with previous schema
def _migrate_additional_columns() -> None:
    # subjects: add main_channel, test_channel if missing
    _ensure_column("subjects", "main_channel", "main_channel INTEGER DEFAULT 0")
    _ensure_column("subjects", "test_channel", "test_channel INTEGER DEFAULT 0")
    # pings: add main_role, test_role if missing
    _ensure_column("pings", "main_role", "main_role INTEGER DEFAULT 0")
    _ensure_column("pings", "test_role", "test_role INTEGER DEFAULT 0")

    # Ensure there's a single 'id' item row so you can populate main/test guild IDs later.
    conn = _conn()
    cur = conn.cursor()
    try:
        cur.execute("SELECT 1 FROM guild_items WHERE item = ?", ("id",))
        if not cur.fetchone():
            cur.execute("INSERT INTO guild_items (item, main, test) VALUES (?, 0, 0)", ("id",))
            conn.commit()
    except Exception:
        # ignore if something goes wrong; this is non-critical
        pass
    finally:
        conn.close()


def set_cog_enabled(guild_id: int, cog: str, enabled: bool) -> None:
    conn = _conn()
    cur = conn.cursor()
    cur.execute("INSERT OR REPLACE INTO guild_cogs (guild_id, cog, enabled) VALUES (?, ?, ?)", (str(guild_id), cog, 1 if enabled else 0))
    conn.commit()
    conn.close()


def is_cog_enabled(guild_id: int, cog: str) -> bool:
    conn = _conn()
    cur = conn.cursor()
    cur.execute("SELECT enabled FROM guild_cogs WHERE guild_id = ? AND cog = ?", (str(guild_id), cog))
    row = cur.fetchone()
    conn.close()
    if row is None:
        # default to enabled
        return True
    return bool(row[0])


def get_guild_cogs(guild_id: int) -> Dict[str, int]:
    conn = _conn()
    cur = conn.cursor()
    cur.execute("SELECT cog, enabled FROM guild_cogs WHERE guild_id = ?", (str(guild_id),))
    rows = cur.fetchall()
    conn.close()
    return {r[0]: r[1] for r in rows}


# Run migration helper
# Ensure DB exists and then run migration helper to add missing columns on older DBs
init_db()
_migrate_additional_columns()


def get_guild_config(guild_id: int) -> Optional[Dict[str, Any]]:
    conn = _conn()
    cur = conn.cursor()
    # In the new schema guild_items is global (no guild_id). Read pairing/partner_log from items.
    cur.execute("SELECT main FROM guild_items WHERE item = ?", ("pairing",))
    r1 = cur.fetchone()
    cur.execute("SELECT main FROM guild_items WHERE item = ?", ("partner_log",))
    r2 = cur.fetchone()
    if r1 or r2:
        pairing = int(r1[0]) if r1 and r1[0] else None
        partner_log = int(r2[0]) if r2 and r2[0] else None
        conn.close()
        return {"pairing": pairing, "partner_log": partner_log}

    # Fallback to legacy guilds table for older DBs
    cur.execute("SELECT pairing, partner_log FROM guilds WHERE guild_id = ?", (str(guild_id),))
    row = cur.fetchone()
    conn.close()
    if not row:
        return None
    return {"pairing": row[0], "partner_log": row[1]}


def set_guild_config(guild_id: int, pairing: Optional[int] = None, partner_log: Optional[int] = None) -> None:
    conn = _conn()
    cur = conn.cursor()
    # Write into the new guild_items table per-item to avoid overwriting other item values
    if pairing is not None:
        # upsert pairing row (global item)
        cur.execute("SELECT 1 FROM guild_items WHERE item = ?", ("pairing",))
        if cur.fetchone():
            cur.execute("UPDATE guild_items SET main = ? WHERE item = ?", (int(pairing), "pairing"))
        else:
            cur.execute("INSERT INTO guild_items (item, main) VALUES (?, ?)", ("pairing", int(pairing)))

    if partner_log is not None:
        cur.execute("SELECT 1 FROM guild_items WHERE item = ?", ("partner_log",))
        if cur.fetchone():
            cur.execute("UPDATE guild_items SET main = ? WHERE item = ?", (int(partner_log), "partner_log"))
        else:
            cur.execute("INSERT INTO guild_items (item, main) VALUES (?, ?)", ("partner_log", int(partner_log)))

    conn.commit()
    conn.close()


def get_subjects_for_guild(guild_id: Optional[int]) -> List[Dict[str, Any]]:
    """Return list of subject dicts in the same shape previously used by the JSON format.

    Subjects are global in the new schema (no guild_id). This function ignores the
    guild_id argument and returns all subjects; callers should filter by channel/guild
    membership if they need per-guild views.
    """
    conn = _conn()
    cur = conn.cursor()
    cur.execute("SELECT id, subject, main_channel, test_channel, category, message, footer, cooldown FROM subjects ORDER BY id")
    subs = []
    for sid, subject, main_channel, test_channel, category, message, footer, cooldown in cur.fetchall():
        cur.execute("SELECT id, ping_value, name, main_role, test_role, last_time FROM pings WHERE subject_id = ? ORDER BY id", (sid,))
        pings_rows = cur.fetchall()
        pings = []
        names = []
        roles = []
        roles_test = []
        times = []
        ping_ids = []
        for pid, ping_value, name, main_role, test_role, last_time in pings_rows:
            pings.append(ping_value)
            names.append(name)
            roles.append(main_role or 0)
            roles_test.append(test_role or 0)
            times.append(last_time or 0)
            ping_ids.append(pid)

        # aliases per ping
        aliases_list = []
        for pid in ping_ids:
            cur.execute("SELECT alias FROM aliases WHERE ping_id = ? ORDER BY id", (pid,))
            alias_rows = [r[0] for r in cur.fetchall()]
            aliases_list.append(alias_rows)

        subs.append({
            "subject": subject,
            "pings": pings,
            "names": names,
            "roles": roles,            # main roles by default
            "roles_test": roles_test,  # parallel test roles
            "times": times,
            "channel": main_channel or 0,
            "main_channel": main_channel or 0,
            "test_channel": test_channel or 0,
            "aliases": aliases_list,
            "message": message,
            "footer": footer,
            "cooldown": cooldown,
            "category": category,
        })
    conn.close()
    return subs


def create_subject(guild_id: int, subject: str) -> None:
    # subjects are global now; guild_id is ignored
    conn = _conn()
    cur = conn.cursor()
    cur.execute("INSERT INTO subjects (subject) VALUES (?)", (subject,))
    conn.commit()
    conn.close()


def add_ping(guild_id: int, subject: str, ping: str, name: str, role: int) -> bool:
    # guild_id ignored because subjects are global
    conn = _conn()
    cur = conn.cursor()
    cur.execute("SELECT id FROM subjects WHERE subject = ? ORDER BY id LIMIT 1", (subject,))
    row = cur.fetchone()
    if not row:
        conn.close()
        return False
    sid = row[0]
    # insert into main_role by default; test_role left as default 0
    cur.execute(
        "INSERT INTO pings (subject_id, ping_value, name, main_role, last_time) VALUES (?, ?, ?, ?, 0)",
        (sid, ping, name, int(role)),
    )
    conn.commit()
    conn.close()
    return True


def update_ping_time(guild_id: int, subject: str, ping_value: str, timestamp: float) -> None:
    conn = _conn()
    cur = conn.cursor()
    # find subject id
    cur.execute("SELECT id FROM subjects WHERE subject = ? ORDER BY id LIMIT 1", (subject,))
    row = cur.fetchone()
    if not row:
        conn.close()
        return
    sid = row[0]
    cur.execute("UPDATE pings SET last_time = ? WHERE subject_id = ? AND ping_value = ?", (float(timestamp), sid, ping_value))
    conn.commit()
    conn.close()


def get_all_pings() -> List[Dict[str, str]]:
    conn = _conn()
    cur = conn.cursor()
    cur.execute("SELECT name, ping_value FROM pings")
    rows = cur.fetchall()
    conn.close()
    return [{"name": r[0], "value": r[1]} for r in rows]


def get_all_subjects() -> List[str]:
    conn = _conn()
    cur = conn.cursor()
    cur.execute("SELECT DISTINCT subject FROM subjects")
    rows = cur.fetchall()
    conn.close()
    return [r[0] for r in rows]


# initialize DB on import (already called above before migrations)
