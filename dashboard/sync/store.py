"""Private durable mapping, not a copy of or writer to Hermes state.db."""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path


class Store:
    def __init__(self, path):
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        self.db = sqlite3.connect(path)
        path.chmod(0o600)
        self.db.row_factory = sqlite3.Row
        self.db.executescript("""
            PRAGMA journal_mode=WAL;
            CREATE TABLE IF NOT EXISTS links (
                session TEXT PRIMARY KEY, thread TEXT UNIQUE NOT NULL,
                title TEXT NOT NULL, cursor TEXT NOT NULL, enabled INTEGER NOT NULL DEFAULT 1
            );
            CREATE TABLE IF NOT EXISTS projection (
                session TEXT NOT NULL, address TEXT NOT NULL, ids TEXT NOT NULL,
                body TEXT NOT NULL, owned INTEGER NOT NULL DEFAULT 1,
                PRIMARY KEY(session, address)
            );
            CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS inbox (
                thread TEXT NOT NULL, message TEXT NOT NULL, status TEXT NOT NULL,
                row_id INTEGER, PRIMARY KEY(thread, message)
            );
        """)

    def links(self):
        return [dict(row) for row in self.db.execute("SELECT * FROM links WHERE enabled=1")]

    def known(self, *, session=None, thread=None):
        # Includes unlinked rows: an explicit !unlink must not be undone by auto-linking.
        row = self.db.execute(
            "SELECT 1 FROM links WHERE session=? OR thread=?",
            (session, None if thread is None else str(thread)),
        ).fetchone()
        return row is not None

    def meta(self, key, default):
        row = self.db.execute("SELECT value FROM meta WHERE key=?", (key,)).fetchone()
        if row:
            return row["value"]
        self.db.execute("INSERT INTO meta VALUES(?,?)", (key, str(default)))
        self.db.commit()
        return str(default)

    def link(self, session, thread, title, cursor):
        self.db.execute(
            "INSERT INTO links(session,thread,title,cursor) VALUES(?,?,?,?)",
            (session, str(thread), title, str(cursor)),
        )
        self.db.commit()

    def update(self, session, **values):
        if set(values) - {"cursor", "title", "enabled"}:
            raise ValueError("Invalid link update")
        fields = ",".join(f"{name}=?" for name in values)
        self.db.execute(f"UPDATE links SET {fields} WHERE session=?", (*values.values(), session))
        self.db.commit()

    def get_projection(self, session):
        return {
            row["address"]: {**dict(row), "ids": json.loads(row["ids"])}
            for row in self.db.execute("SELECT * FROM projection WHERE session=?", (session,))
        }

    def project(self, session, address, ids, body, owned=True):
        self.db.execute(
            "INSERT OR REPLACE INTO projection VALUES(?,?,?,?,?)",
            (session, address, json.dumps(ids), body, int(owned)),
        )
        self.db.commit()

    def forget(self, session, address):
        self.db.execute("DELETE FROM projection WHERE session=? AND address=?", (session, address))
        self.db.commit()

    def pin(self, session):
        """The pin state last mirrored for this session: True, False, or None if never mirrored."""
        mirrored = self.get_projection(session).get("pin")
        if mirrored is None:
            return None
        return mirrored["body"] == "1"

    def set_pin(self, session, pinned):
        self.project(session, "pin", [], "1" if pinned else "0")

    def inbox_status(self, thread, message):
        row = self.db.execute(
            "SELECT * FROM inbox WHERE thread=? AND message=?", (str(thread), str(message))
        ).fetchone()
        return dict(row) if row else None

    def received(self, thread, message, status, row_id=None):
        self.db.execute(
            "INSERT OR REPLACE INTO inbox VALUES(?,?,?,?)", (str(thread), str(message), status, row_id)
        )
        self.db.commit()

    def close(self):
        self.db.close()
