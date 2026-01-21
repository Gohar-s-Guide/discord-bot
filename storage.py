import os
import json 
import datetime
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
    # key/value items table: item can be 'pairing', 'partner_log', or other keys
    # Stored globally in `items` (no guild-specific columns).
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS items (
            item TEXT PRIMARY KEY,
            main INTEGER DEFAULT 0,
            test INTEGER DEFAULT 0
        )
        """
    )
    # subjects: global subjects table (no guild_id)
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
    # queue table persists waiting user IDs in order
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS partner_queue (
            pos INTEGER PRIMARY KEY,
            user_id INTEGER
        )
        """
    )
    # active sessions persisted across restarts
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS sessions (
            text_channel_id INTEGER PRIMARY KEY,
            members TEXT,
            created_at TEXT,
            messages TEXT
        )
        """
    )
    # global cogs enable/disable table (one row per cog)
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS cogs (
            cog TEXT PRIMARY KEY,
            enabled INTEGER DEFAULT 1
        )
        """
    )
    conn.commit()
    conn.close()





def set_cog_enabled(cog: str, enabled: bool) -> None:
    conn = _conn()
    cur = conn.cursor()
    # upsert into global `cogs` table
    cur.execute("SELECT 1 FROM cogs WHERE cog = ?", (cog,))
    if cur.fetchone():
        cur.execute("UPDATE cogs SET enabled = ? WHERE cog = ?", (1 if enabled else 0, cog))
    else:
        cur.execute("INSERT INTO cogs (cog, enabled) VALUES (?, ?)", (cog, 1 if enabled else 0))
    conn.commit()
    conn.close()


def save_queue(queue: List[int]) -> None:
    conn = _conn()
    cur = conn.cursor()
    cur.execute("DELETE FROM partner_queue")
    for i, uid in enumerate(queue):
        cur.execute("INSERT INTO partner_queue (pos, user_id) VALUES (?, ?)", (i, int(uid)))
    conn.commit()
    conn.close()


def load_queue() -> List[int]:
    conn = _conn()
    cur = conn.cursor()
    cur.execute("SELECT user_id FROM partner_queue ORDER BY pos")
    rows = cur.fetchall()
    conn.close()
    return [r[0] for r in rows]


def save_session(session: Dict) -> None:
    """Persist a session dict. session must include 'text_channel_id', 'members', 'created_at', 'messages'."""
    conn = _conn()
    cur = conn.cursor()
    text_id = int(session.get("text_channel_id"))
    members = json.dumps(session.get("members", []))
    created_at = None
    ca = session.get("created_at")
    if isinstance(ca, datetime.datetime):
        created_at = ca.isoformat()
    elif isinstance(ca, str):
        created_at = ca
    # Ensure messages are JSON-serializable: convert any datetime to ISO strings
    msgs_list = []
    for m in session.get("messages", []):
        mm = dict(m)
        ca = mm.get("created_at")
        if isinstance(ca, datetime.datetime):
            mm["created_at"] = ca.isoformat()
        mm_str = mm
        msgs_list.append(mm_str)
    msgs = json.dumps(msgs_list)
    cur.execute("SELECT 1 FROM sessions WHERE text_channel_id = ?", (text_id,))
    if cur.fetchone():
        cur.execute("UPDATE sessions SET members = ?, created_at = ?, messages = ? WHERE text_channel_id = ?", (members, created_at, msgs, text_id))
    else:
        cur.execute("INSERT INTO sessions (text_channel_id, members, created_at, messages) VALUES (?, ?, ?, ?)", (text_id, members, created_at, msgs))
    conn.commit()
    conn.close()


def delete_session(text_channel_id: int) -> None:
    conn = _conn()
    cur = conn.cursor()
    cur.execute("DELETE FROM sessions WHERE text_channel_id = ?", (int(text_channel_id),))
    conn.commit()
    conn.close()


def load_sessions() -> List[Dict]:
    conn = _conn()
    cur = conn.cursor()
    cur.execute("SELECT text_channel_id, members, created_at, messages FROM sessions")
    rows = cur.fetchall()
    sessions = []
    for text_id, members_json, created_at, messages_json in rows:
        try:
            members = json.loads(members_json) if members_json else []
        except Exception:
            members = []
        try:
            messages = json.loads(messages_json) if messages_json else []
        except Exception:
            messages = []
        sessions.append({
            "text_channel_id": int(text_id),
            "members": members,
            "created_at": created_at,
            "messages": messages,
        })
    conn.close()
    return sessions


def is_cog_enabled(cog: str) -> bool:
    conn = _conn()
    cur = conn.cursor()
    cur.execute("SELECT enabled FROM cogs WHERE cog = ?", (cog,))
    row = cur.fetchone()
    conn.close()
    if row is None:
        # default to enabled
        return True
    return bool(row[0])


def get_cogs() -> Dict[str, int]:
    conn = _conn()
    cur = conn.cursor()
    cur.execute("SELECT cog, enabled FROM cogs")
    rows = cur.fetchall()
    conn.close()
    return {r[0]: r[1] for r in rows}


# Run migration helper
# Ensure DB exists and then run migration helper to add missing columns on older DBs
init_db()


def get_guild_config() -> Optional[Dict[str, Any]]:
    """Return global configuration values (pairing and partner_log).

    This function no longer accepts a `guild_id` — configuration is stored
    globally in `items` and is not guild-specific.
    """
    conn = _conn()
    cur = conn.cursor()
    # Configuration values are stored globally in `items` (no per-guild rows).
    cur.execute("SELECT main FROM items WHERE item = ?", ("pairing",))
    r1 = cur.fetchone()
    cur.execute("SELECT main FROM items WHERE item = ?", ("partner_log",))
    r2 = cur.fetchone()

    pairing = int(r1[0]) if r1 and r1[0] else None
    partner_log = int(r2[0]) if r2 and r2[0] else None
    conn.close()

    # If neither value is configured, return None to indicate no config present.
    if pairing is None and partner_log is None:
        return None
    return {"pairing": pairing, "partner_log": partner_log}


def set_guild_config(pairing: Optional[int] = None, partner_log: Optional[int] = None) -> None:
    """Set global configuration values for pairing and partner_log.

    The previous `guild_id` parameter has been removed — configuration is
    global and stored in `items`.
    """
    conn = _conn()
    cur = conn.cursor()
    # Write into the items table per-item to avoid overwriting other item values
    if pairing is not None:
        # upsert pairing row (global item)
        cur.execute("SELECT 1 FROM items WHERE item = ?", ("pairing",))
        if cur.fetchone():
            cur.execute("UPDATE items SET main = ? WHERE item = ?", (int(pairing), "pairing"))
        else:
            cur.execute("INSERT INTO items (item, main) VALUES (?, ?)", ("pairing", int(pairing)))

    if partner_log is not None:
        cur.execute("SELECT 1 FROM items WHERE item = ?", ("partner_log",))
        if cur.fetchone():
            cur.execute("UPDATE items SET main = ? WHERE item = ?", (int(partner_log), "partner_log"))
        else:
            cur.execute("INSERT INTO items (item, main) VALUES (?, ?)", ("partner_log", int(partner_log)))

    conn.commit()
    conn.close()


def get_subjects() -> List[Dict[str, Any]]:
    """Return list of subject dicts in the same shape previously used by the JSON format.

    Subjects are global in the schema. This function returns all subjects; callers
    should filter by channel/guild membership if they need per-guild views.
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


def create_subject(subject: str) -> None:
    # subjects are global
    conn = _conn()
    cur = conn.cursor()
    cur.execute("INSERT INTO subjects (subject) VALUES (?)", (subject,))
    conn.commit()
    conn.close()


def add_ping(subject: str, ping: str, name: str, role: int) -> bool:
    # subjects are global
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


def update_ping_time(subject: str, ping_value: str, timestamp: float) -> None:
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
