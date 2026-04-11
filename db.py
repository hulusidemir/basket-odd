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
                    score       TEXT NOT NULL DEFAULT '',
                    ai_analysis TEXT NOT NULL DEFAULT '',
                    signal_count INTEGER NOT NULL DEFAULT 1,
                    bet_placed  INTEGER NOT NULL DEFAULT 0,
                    ignored     INTEGER NOT NULL DEFAULT 0,
                    followed    INTEGER NOT NULL DEFAULT 0,
                    quality_grade TEXT NOT NULL DEFAULT '',
                    quality_score REAL NOT NULL DEFAULT 0,
                    quality_setup TEXT NOT NULL DEFAULT '',
                    quality_summary TEXT NOT NULL DEFAULT '',
                    quality_reasons TEXT NOT NULL DEFAULT '',
                    alerted_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );

                CREATE TABLE IF NOT EXISTS match_actions (
                    match_id    TEXT PRIMARY KEY,
                    bet_placed  INTEGER NOT NULL DEFAULT 0,
                    ignored     INTEGER NOT NULL DEFAULT 0,
                    followed    INTEGER NOT NULL DEFAULT 0
                );

                CREATE TABLE IF NOT EXISTS finished_matches (
                    id              INTEGER PRIMARY KEY AUTOINCREMENT,
                    source_alert_id INTEGER NOT NULL UNIQUE,
                    match_id        TEXT NOT NULL,
                    match_name      TEXT NOT NULL,
                    tournament      TEXT NOT NULL DEFAULT '',
                    status          TEXT NOT NULL DEFAULT '',
                    final_status    TEXT NOT NULL DEFAULT '',
                    opening         REAL NOT NULL,
                    live            REAL NOT NULL,
                    direction       TEXT NOT NULL,
                    diff            REAL NOT NULL,
                    url             TEXT NOT NULL DEFAULT '',
                    bet_placed      INTEGER NOT NULL DEFAULT 0,
                    ignored         INTEGER NOT NULL DEFAULT 0,
                    followed        INTEGER NOT NULL DEFAULT 0,
                    alerted_at      TIMESTAMP,
                    score           TEXT NOT NULL DEFAULT '',
                    signal_count    INTEGER NOT NULL DEFAULT 1,
                    quality_grade   TEXT NOT NULL DEFAULT '',
                    quality_score   REAL NOT NULL DEFAULT 0,
                    quality_setup   TEXT NOT NULL DEFAULT '',
                    quality_summary TEXT NOT NULL DEFAULT '',
                    quality_reasons TEXT NOT NULL DEFAULT '',
                    final_score     TEXT NOT NULL DEFAULT '',
                    final_total     REAL,
                    result          TEXT NOT NULL DEFAULT '',
                    finished_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );

                CREATE INDEX IF NOT EXISTS idx_finished_matches_match_id
                ON finished_matches(match_id);

                CREATE INDEX IF NOT EXISTS idx_finished_matches_finished_at
                ON finished_matches(finished_at DESC);
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
            try:
                conn.execute("ALTER TABLE alerts ADD COLUMN followed INTEGER NOT NULL DEFAULT 0")
            except Exception:
                pass
            try:
                conn.execute("ALTER TABLE alerts ADD COLUMN score TEXT NOT NULL DEFAULT ''")
            except Exception:
                pass
            try:
                conn.execute("ALTER TABLE alerts ADD COLUMN ai_analysis TEXT NOT NULL DEFAULT ''")
            except Exception:
                pass
            try:
                conn.execute("ALTER TABLE alerts ADD COLUMN signal_count INTEGER NOT NULL DEFAULT 1")
            except Exception:
                pass
            try:
                conn.execute("ALTER TABLE alerts ADD COLUMN quality_grade TEXT NOT NULL DEFAULT ''")
            except Exception:
                pass
            try:
                conn.execute("ALTER TABLE alerts ADD COLUMN quality_score REAL NOT NULL DEFAULT 0")
            except Exception:
                pass
            try:
                conn.execute("ALTER TABLE alerts ADD COLUMN quality_setup TEXT NOT NULL DEFAULT ''")
            except Exception:
                pass
            try:
                conn.execute("ALTER TABLE alerts ADD COLUMN quality_summary TEXT NOT NULL DEFAULT ''")
            except Exception:
                pass
            try:
                conn.execute("ALTER TABLE alerts ADD COLUMN quality_reasons TEXT NOT NULL DEFAULT ''")
            except Exception:
                pass
            try:
                conn.execute("ALTER TABLE finished_matches ADD COLUMN final_status TEXT NOT NULL DEFAULT ''")
            except Exception:
                pass
            try:
                conn.execute("ALTER TABLE finished_matches ADD COLUMN final_score TEXT NOT NULL DEFAULT ''")
            except Exception:
                pass
            try:
                conn.execute("ALTER TABLE finished_matches ADD COLUMN final_total REAL")
            except Exception:
                pass
            try:
                conn.execute("ALTER TABLE finished_matches ADD COLUMN result TEXT NOT NULL DEFAULT ''")
            except Exception:
                pass
            try:
                conn.execute("ALTER TABLE finished_matches ADD COLUMN quality_grade TEXT NOT NULL DEFAULT ''")
            except Exception:
                pass
            try:
                conn.execute("ALTER TABLE finished_matches ADD COLUMN quality_score REAL NOT NULL DEFAULT 0")
            except Exception:
                pass
            try:
                conn.execute("ALTER TABLE finished_matches ADD COLUMN quality_setup TEXT NOT NULL DEFAULT ''")
            except Exception:
                pass
            try:
                conn.execute("ALTER TABLE finished_matches ADD COLUMN quality_summary TEXT NOT NULL DEFAULT ''")
            except Exception:
                pass
            try:
                conn.execute("ALTER TABLE finished_matches ADD COLUMN quality_reasons TEXT NOT NULL DEFAULT ''")
            except Exception:
                pass
            # Ensure match_actions table exists for action inheritance
            conn.execute("""
                CREATE TABLE IF NOT EXISTS match_actions (
                    match_id    TEXT PRIMARY KEY,
                    bet_placed  INTEGER NOT NULL DEFAULT 0,
                    ignored     INTEGER NOT NULL DEFAULT 0,
                    followed    INTEGER NOT NULL DEFAULT 0
                )
            """)

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

    def count_match_alerts(self, match_id: str) -> int:
        """Count how many alerts have been sent for this match (any direction)."""
        with self._conn() as conn:
            row = conn.execute(
                "SELECT COUNT(*) as cnt FROM alerts WHERE match_id = ?",
                (match_id,),
            ).fetchone()
        return row["cnt"] if row else 0

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
        score: str = "",
        signal_count: int = 1,
        quality_grade: str = "",
        quality_score: float = 0.0,
        quality_setup: str = "",
        quality_summary: str = "",
        quality_reasons: str = "",
    ) -> int:
        with self._conn() as conn:
            # Inherit match-level actions if previously set
            action = conn.execute(
                "SELECT bet_placed, ignored, followed FROM match_actions WHERE match_id = ?",
                (match_id,),
            ).fetchone()
            bet = action["bet_placed"] if action else 0
            ign = action["ignored"] if action else 0
            fol = action["followed"] if action else 0
            cursor = conn.execute(
                """
                INSERT INTO alerts (
                    match_id, match_name, opening, live, direction, diff, tournament, status, url, score,
                    signal_count, quality_grade, quality_score, quality_setup, quality_summary, quality_reasons,
                    bet_placed, ignored, followed
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    match_id, match_name, opening, live, direction, diff, tournament, status, url, score,
                    signal_count, quality_grade, quality_score, quality_setup, quality_summary, quality_reasons,
                    bet, ign, fol,
                ),
            )
            return cursor.lastrowid

    def recent_alerts(self, limit: int = 200) -> list:
        with self._conn() as conn:
            rows = conn.execute(
                "SELECT * FROM alerts ORDER BY alerted_at DESC LIMIT ?", (limit,)
            ).fetchall()
        return [dict(r) for r in rows]

    def set_match_statuses(
        self,
        match_id: str,
        *,
        bet_placed: bool | None = None,
        ignored: bool | None = None,
        followed: bool | None = None,
    ) -> int:
        """Update one or more status flags for all alerts of the same match."""
        updates = []
        params = []

        if bet_placed is not None:
            updates.append("bet_placed = ?")
            params.append(1 if bet_placed else 0)
        if ignored is not None:
            updates.append("ignored = ?")
            params.append(1 if ignored else 0)
        if followed is not None:
            updates.append("followed = ?")
            params.append(1 if followed else 0)

        if not updates:
            return 0

        params.append(match_id)
        with self._conn() as conn:
            cursor = conn.execute(
                f"UPDATE alerts SET {', '.join(updates)} WHERE match_id = ?",
                tuple(params),
            )
            # Persist match-level actions for future alerts
            ma_cols = ["match_id"]
            ma_vals = [match_id]
            ma_updates = []
            if bet_placed is not None:
                ma_cols.append("bet_placed")
                ma_vals.append(1 if bet_placed else 0)
                ma_updates.append("bet_placed = excluded.bet_placed")
            if ignored is not None:
                ma_cols.append("ignored")
                ma_vals.append(1 if ignored else 0)
                ma_updates.append("ignored = excluded.ignored")
            if followed is not None:
                ma_cols.append("followed")
                ma_vals.append(1 if followed else 0)
                ma_updates.append("followed = excluded.followed")
            if ma_updates:
                placeholders = ", ".join("?" for _ in ma_vals)
                conn.execute(
                    f"INSERT INTO match_actions ({', '.join(ma_cols)}) VALUES ({placeholders}) "
                    f"ON CONFLICT(match_id) DO UPDATE SET {', '.join(ma_updates)}",
                    tuple(ma_vals),
                )
        return cursor.rowcount

    def get_alert(self, alert_id: int) -> dict | None:
        with self._conn() as conn:
            row = conn.execute("SELECT * FROM alerts WHERE id = ?", (alert_id,)).fetchone()
        return dict(row) if row else None

    def update_analysis(self, alert_id: int, analysis: str):
        with self._conn() as conn:
            conn.execute(
                "UPDATE alerts SET ai_analysis = ? WHERE id = ?",
                (analysis, alert_id),
            )

    def get_match_analysis_text(self, match_id: str) -> str | None:
        """Return the latest non-empty AI analysis for a match, if any."""
        with self._conn() as conn:
            row = conn.execute(
                "SELECT ai_analysis FROM alerts WHERE match_id = ? AND ai_analysis != '' ORDER BY alerted_at DESC LIMIT 1",
                (match_id,),
            ).fetchone()
        return row["ai_analysis"] if row else None

    def delete_alert(self, alert_id: int) -> bool:
        with self._conn() as conn:
            cursor = conn.execute("DELETE FROM alerts WHERE id = ?",(alert_id,))
        return cursor.rowcount > 0

    def delete_match_data(self, match_id: str) -> int:
        """Delete all alert/state records belonging to the same match."""
        with self._conn() as conn:
            cursor = conn.execute("DELETE FROM alerts WHERE match_id = ?", (match_id,))
            conn.execute("DELETE FROM match_actions WHERE match_id = ?", (match_id,))
            conn.execute("DELETE FROM opening_lines WHERE match_id = ?", (match_id,))
        return cursor.rowcount

    def clear_all(self):
        with self._conn() as conn:
            conn.execute("DELETE FROM alerts")
            conn.execute("DELETE FROM match_actions")
            conn.execute("DELETE FROM opening_lines")

    # ---------- finished matches ----------

    def recent_finished_matches(self, limit: int = 500) -> list:
        with self._conn() as conn:
            rows = conn.execute(
                "SELECT * FROM finished_matches ORDER BY finished_at DESC, id DESC LIMIT ?",
                (limit,),
            ).fetchall()
        return [dict(r) for r in rows]

    def delete_finished_match(self, finished_match_id: int) -> bool:
        with self._conn() as conn:
            cursor = conn.execute(
                "DELETE FROM finished_matches WHERE id = ?",
                (finished_match_id,),
            )
        return cursor.rowcount > 0

    def clear_finished_matches(self) -> int:
        with self._conn() as conn:
            cursor = conn.execute("DELETE FROM finished_matches")
        return cursor.rowcount

    def get_tracked_live_matches(self, limit: int = 200) -> list:
        """
        Return one latest alert row per match where at least one signal
        has not been copied into finished_matches yet.
        """
        with self._conn() as conn:
            rows = conn.execute(
                """
                SELECT a.match_id, a.match_name, a.tournament, a.url, a.status, a.score, a.alerted_at
                FROM alerts a
                INNER JOIN (
                    SELECT match_id, MAX(id) AS latest_alert_id
                    FROM alerts
                    WHERE url != ''
                    GROUP BY match_id
                ) latest ON latest.latest_alert_id = a.id
                WHERE EXISTS (
                    SELECT 1
                    FROM alerts pending
                    LEFT JOIN finished_matches fm ON fm.source_alert_id = pending.id
                    WHERE pending.match_id = a.match_id
                      AND fm.id IS NULL
                )
                ORDER BY a.alerted_at DESC, a.id DESC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()
        return [dict(r) for r in rows]

    def get_pending_alerts_for_match(self, match_id: str) -> list:
        with self._conn() as conn:
            rows = conn.execute(
                """
                SELECT a.*
                FROM alerts a
                LEFT JOIN finished_matches fm ON fm.source_alert_id = a.id
                WHERE a.match_id = ?
                  AND fm.id IS NULL
                ORDER BY a.alerted_at ASC, a.id ASC
                """,
                (match_id,),
            ).fetchall()
        return [dict(r) for r in rows]

    def archive_finished_alert(
        self,
        alert: dict,
        *,
        final_status: str,
        final_score: str,
        final_total: float | None,
        result: str,
    ) -> int:
        with self._conn() as conn:
            cursor = conn.execute(
                """
                INSERT OR IGNORE INTO finished_matches (
                    source_alert_id, match_id, match_name, tournament, status, final_status,
                    opening, live, direction, diff, url, bet_placed, ignored, followed,
                    alerted_at, score, signal_count, quality_grade, quality_score, quality_setup, quality_summary, quality_reasons,
                    final_score, final_total, result
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    alert["id"],
                    alert["match_id"],
                    alert["match_name"],
                    alert.get("tournament", ""),
                    alert.get("status", ""),
                    final_status,
                    alert["opening"],
                    alert["live"],
                    alert["direction"],
                    alert["diff"],
                    alert.get("url", ""),
                    alert.get("bet_placed", 0),
                    alert.get("ignored", 0),
                    alert.get("followed", 0),
                    alert.get("alerted_at"),
                    alert.get("score", ""),
                    alert.get("signal_count", 1),
                    alert.get("quality_grade", ""),
                    alert.get("quality_score", 0),
                    alert.get("quality_setup", ""),
                    alert.get("quality_summary", ""),
                    alert.get("quality_reasons", ""),
                    final_score,
                    final_total,
                    result,
                ),
            )
        return cursor.lastrowid if cursor.rowcount > 0 else 0
