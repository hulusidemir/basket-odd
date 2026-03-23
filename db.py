import sqlite3
from contextlib import contextmanager


class Database:
    def __init__(self, db_path: str):
        self.db_path = db_path

    @contextmanager
    def _conn(self):
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        try:
            yield conn
            conn.commit()
        finally:
            conn.close()

    def init(self):
        with self._conn() as conn:
            conn.executescript("""
                CREATE TABLE IF NOT EXISTS opening_lines (
                    match_id    TEXT PRIMARY KEY,
                    match_name  TEXT NOT NULL,
                    opening     REAL NOT NULL,
                    created_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );

                CREATE TABLE IF NOT EXISTS alerts (
                    id          INTEGER PRIMARY KEY AUTOINCREMENT,
                    match_id    TEXT NOT NULL,
                    match_name  TEXT NOT NULL,
                    tournament  TEXT NOT NULL DEFAULT '',
                    status      TEXT NOT NULL DEFAULT '',
                    opening     REAL NOT NULL,
                    live        REAL NOT NULL,
                    direction   TEXT NOT NULL,
                    diff        REAL NOT NULL,
                    url         TEXT NOT NULL DEFAULT '',
                    bet_placed  INTEGER NOT NULL DEFAULT 0,
                    ignored     INTEGER NOT NULL DEFAULT 0,
                    alerted_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """)
            # Migrate: add new columns if they don't exist yet
            try:
                conn.execute("ALTER TABLE alerts ADD COLUMN tournament TEXT NOT NULL DEFAULT ''")
            except Exception:
                pass
            try:
                conn.execute("ALTER TABLE alerts ADD COLUMN status TEXT NOT NULL DEFAULT ''")
            except Exception:
                pass
            try:
                conn.execute("ALTER TABLE alerts ADD COLUMN url TEXT NOT NULL DEFAULT ''")
            except Exception:
                pass
            try:
                conn.execute("ALTER TABLE alerts ADD COLUMN bet_placed INTEGER NOT NULL DEFAULT 0")
            except Exception:
                pass
            try:
                conn.execute("ALTER TABLE alerts ADD COLUMN ignored INTEGER NOT NULL DEFAULT 0")
            except Exception:
                pass

    # ---------- opening line ----------

    def get_opening(self, match_id: str) -> float | None:
        with self._conn() as conn:
            row = conn.execute(
                "SELECT opening FROM opening_lines WHERE match_id = ?",
                (match_id,),
            ).fetchone()
        return row["opening"] if row else None

    def save_opening(self, match_id: str, match_name: str, opening: float):
        with self._conn() as conn:
            conn.execute(
                "INSERT OR IGNORE INTO opening_lines (match_id, match_name, opening) VALUES (?, ?, ?)",
                (match_id, match_name, opening),
            )

    def delete_opening(self, match_id: str):
        """Call after a match ends to clean up stale records."""
        with self._conn() as conn:
            conn.execute("DELETE FROM opening_lines WHERE match_id = ?", (match_id,))

    # ---------- alerts ----------

    def was_alerted_recently(self, match_id: str, direction: str, cooldown_minutes: int) -> bool:
        with self._conn() as conn:
            row = conn.execute(
                """
                SELECT 1 FROM alerts
                WHERE match_id = ? AND direction = ?
                  AND alerted_at > datetime('now', ? || ' minutes')
                LIMIT 1
                """,
                (match_id, direction, f"-{cooldown_minutes}"),
            ).fetchone()
        return row is not None

    def save_alert(
        self,
        match_id: str,
        match_name: str,
        opening: float,
        live: float,
        direction: str,
        diff: float,
        tournament: str = "",
        status: str = "",
        url: str = "",
    ):
        with self._conn() as conn:
            conn.execute(
                """
                INSERT INTO alerts (match_id, match_name, opening, live, direction, diff, tournament, status, url)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (match_id, match_name, opening, live, direction, diff, tournament, status, url),
            )

    def recent_alerts(self, limit: int = 200) -> list:
        with self._conn() as conn:
            rows = conn.execute(
                "SELECT * FROM alerts ORDER BY alerted_at DESC LIMIT ?", (limit,)
            ).fetchall()
        return [dict(r) for r in rows]

    def set_bet_placed(self, alert_id: int, value: bool):
        with self._conn() as conn:
            conn.execute(
                "UPDATE alerts SET bet_placed = ? WHERE id = ?",
                (1 if value else 0, alert_id),
            )

    def set_ignored(self, alert_id: int, value: bool):
        with self._conn() as conn:
            conn.execute(
                "UPDATE alerts SET ignored = ? WHERE id = ?",
                (1 if value else 0, alert_id),
            )

    def get_alert(self, alert_id: int) -> dict | None:
        with self._conn() as conn:
            row = conn.execute("SELECT * FROM alerts WHERE id = ?", (alert_id,)).fetchone()
        return dict(row) if row else None

    def delete_alert(self, alert_id: int) -> bool:
        with self._conn() as conn:
            cursor = conn.execute("DELETE FROM alerts WHERE id = ?", (alert_id,))
        return cursor.rowcount > 0
