import json
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
                    prematch    REAL,
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
                    counter_direction TEXT NOT NULL DEFAULT '',
                    counter_level TEXT NOT NULL DEFAULT '',
                    counter_score REAL NOT NULL DEFAULT 0,
                    counter_note TEXT NOT NULL DEFAULT '',
                    counter_reasons TEXT NOT NULL DEFAULT '',
                    deleted_at  TIMESTAMP,
                    result      TEXT NOT NULL DEFAULT '',
                    alerted_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );

                CREATE TABLE IF NOT EXISTS match_actions (
                    match_id    TEXT PRIMARY KEY,
                    bet_placed  INTEGER NOT NULL DEFAULT 0,
                    ignored     INTEGER NOT NULL DEFAULT 0,
                    followed    INTEGER NOT NULL DEFAULT 0,
                    deleted_at  TIMESTAMP
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
                    prematch        REAL,
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
                    counter_direction TEXT NOT NULL DEFAULT '',
                    counter_level   TEXT NOT NULL DEFAULT '',
                    counter_score   REAL NOT NULL DEFAULT 0,
                    counter_note    TEXT NOT NULL DEFAULT '',
                    counter_reasons TEXT NOT NULL DEFAULT '',
                    final_score     TEXT NOT NULL DEFAULT '',
                    final_total     REAL,
                    result          TEXT NOT NULL DEFAULT '',
                    finished_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );

                CREATE INDEX IF NOT EXISTS idx_finished_matches_match_id
                ON finished_matches(match_id);

                CREATE INDEX IF NOT EXISTS idx_finished_matches_finished_at
                ON finished_matches(finished_at DESC);

                CREATE TABLE IF NOT EXISTS saved_bet_slips (
                    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
                    name                TEXT NOT NULL,
                    requested_max_count INTEGER NOT NULL DEFAULT 1,
                    selected_count      INTEGER NOT NULL DEFAULT 0,
                    eligible_count      INTEGER NOT NULL DEFAULT 0,
                    message             TEXT NOT NULL DEFAULT '',
                    payload_json        TEXT NOT NULL,
                    created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );

                CREATE INDEX IF NOT EXISTS idx_saved_bet_slips_created_at
                ON saved_bet_slips(created_at DESC, id DESC);
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
                conn.execute("ALTER TABLE alerts ADD COLUMN counter_direction TEXT NOT NULL DEFAULT ''")
            except Exception:
                pass
            try:
                conn.execute("ALTER TABLE alerts ADD COLUMN counter_level TEXT NOT NULL DEFAULT ''")
            except Exception:
                pass
            try:
                conn.execute("ALTER TABLE alerts ADD COLUMN counter_score REAL NOT NULL DEFAULT 0")
            except Exception:
                pass
            try:
                conn.execute("ALTER TABLE alerts ADD COLUMN counter_note TEXT NOT NULL DEFAULT ''")
            except Exception:
                pass
            try:
                conn.execute("ALTER TABLE alerts ADD COLUMN counter_reasons TEXT NOT NULL DEFAULT ''")
            except Exception:
                pass
            try:
                conn.execute("ALTER TABLE alerts ADD COLUMN deleted_at TIMESTAMP")
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
            try:
                conn.execute("ALTER TABLE finished_matches ADD COLUMN counter_direction TEXT NOT NULL DEFAULT ''")
            except Exception:
                pass
            try:
                conn.execute("ALTER TABLE finished_matches ADD COLUMN counter_level TEXT NOT NULL DEFAULT ''")
            except Exception:
                pass
            try:
                conn.execute("ALTER TABLE finished_matches ADD COLUMN counter_score REAL NOT NULL DEFAULT 0")
            except Exception:
                pass
            try:
                conn.execute("ALTER TABLE finished_matches ADD COLUMN counter_note TEXT NOT NULL DEFAULT ''")
            except Exception:
                pass
            try:
                conn.execute("ALTER TABLE finished_matches ADD COLUMN counter_reasons TEXT NOT NULL DEFAULT ''")
            except Exception:
                pass
            # Professional evaluation columns
            try:
                conn.execute("ALTER TABLE finished_matches ADD COLUMN margin REAL")
            except Exception:
                pass
            try:
                conn.execute("ALTER TABLE finished_matches ADD COLUMN signal_timing_grade TEXT NOT NULL DEFAULT ''")
            except Exception:
                pass
            try:
                conn.execute("ALTER TABLE finished_matches ADD COLUMN market_read_correct INTEGER")
            except Exception:
                pass
            try:
                conn.execute("ALTER TABLE finished_matches ADD COLUMN projection_accuracy REAL")
            except Exception:
                pass
            try:
                conn.execute("ALTER TABLE finished_matches ADD COLUMN quality_accuracy TEXT NOT NULL DEFAULT ''")
            except Exception:
                pass
            try:
                conn.execute("ALTER TABLE finished_matches ADD COLUMN counter_triggered INTEGER")
            except Exception:
                pass
            try:
                conn.execute("ALTER TABLE finished_matches ADD COLUMN verdict TEXT NOT NULL DEFAULT ''")
            except Exception:
                pass
            try:
                conn.execute("ALTER TABLE finished_matches ADD COLUMN lesson TEXT NOT NULL DEFAULT ''")
            except Exception:
                pass
            # Ensure match_actions table exists for action inheritance
            conn.execute("""
                CREATE TABLE IF NOT EXISTS match_actions (
                    match_id    TEXT PRIMARY KEY,
                    bet_placed  INTEGER NOT NULL DEFAULT 0,
                    ignored     INTEGER NOT NULL DEFAULT 0,
                    followed    INTEGER NOT NULL DEFAULT 0,
                    deleted_at  TIMESTAMP
                )
            """)
            try:
                conn.execute("ALTER TABLE match_actions ADD COLUMN deleted_at TIMESTAMP")
            except Exception:
                pass
            try:
                conn.execute("ALTER TABLE alerts ADD COLUMN prematch REAL")
            except Exception:
                pass
            try:
                conn.execute("ALTER TABLE finished_matches ADD COLUMN prematch REAL")
            except Exception:
                pass
            try:
                conn.execute("ALTER TABLE alerts ADD COLUMN result TEXT NOT NULL DEFAULT ''")
            except Exception:
                pass
            conn.execute("CREATE INDEX IF NOT EXISTS idx_alerts_deleted_at ON alerts(deleted_at)")

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

    def is_match_deleted(self, match_id: str) -> bool:
        with self._conn() as conn:
            row = conn.execute(
                """
                SELECT 1 FROM match_actions
                WHERE match_id = ?
                  AND deleted_at IS NOT NULL
                  AND deleted_at != ''
                LIMIT 1
                """,
                (match_id,),
            ).fetchone()
        return row is not None

    def count_match_alerts(self, match_id: str) -> int:
        """Count how many active (non-deleted) alerts have been sent for this match."""
        with self._conn() as conn:
            row = conn.execute(
                "SELECT COUNT(*) as cnt FROM alerts WHERE match_id = ? AND (deleted_at IS NULL OR deleted_at = '')",
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
        counter_direction: str = "",
        counter_level: str = "",
        counter_score: float = 0.0,
        counter_note: str = "",
        counter_reasons: str = "",
        prematch: float | None = None,
    ) -> int:
        with self._conn() as conn:
            # Inherit match-level actions if previously set
            action = conn.execute(
                "SELECT bet_placed, ignored, followed, deleted_at FROM match_actions WHERE match_id = ?",
                (match_id,),
            ).fetchone()
            bet = action["bet_placed"] if action else 0
            ign = action["ignored"] if action else 0
            fol = action["followed"] if action else 0
            deleted_at = action["deleted_at"] if action else None
            cursor = conn.execute(
                """
                INSERT INTO alerts (
                    match_id, match_name, opening, prematch, live, direction, diff, tournament, status, url, score,
                    signal_count, quality_grade, quality_score, quality_setup, quality_summary, quality_reasons,
                    counter_direction, counter_level, counter_score, counter_note, counter_reasons,
                    bet_placed, ignored, followed, deleted_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    match_id, match_name, opening, prematch, live, direction, diff, tournament, status, url, score,
                    signal_count, quality_grade, quality_score, quality_setup, quality_summary, quality_reasons,
                    counter_direction, counter_level, counter_score, counter_note, counter_reasons,
                    bet, ign, fol, deleted_at,
                ),
            )
            return cursor.lastrowid

    def recent_alerts(self, limit: int = 200) -> list:
        with self._conn() as conn:
            rows = conn.execute(
                """
                SELECT * FROM alerts
                WHERE deleted_at IS NULL OR deleted_at = ''
                ORDER BY alerted_at DESC
                LIMIT ?
                """,
                (limit,),
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
                f"UPDATE alerts SET {', '.join(updates)} WHERE match_id = ? AND (deleted_at IS NULL OR deleted_at = '')",
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
            row = conn.execute(
                "SELECT * FROM alerts WHERE id = ? AND (deleted_at IS NULL OR deleted_at = '')",
                (alert_id,),
            ).fetchone()
        return dict(row) if row else None

    def delete_alert(self, alert_id: int) -> bool:
        with self._conn() as conn:
            cursor = conn.execute("DELETE FROM alerts WHERE id = ?",(alert_id,))
        return cursor.rowcount > 0

    def delete_match_data(self, match_id: str) -> int:
        """Move all active alert/state records for the same match into deleted matches."""
        with self._conn() as conn:
            cursor = conn.execute(
                """
                UPDATE alerts
                SET deleted_at = CURRENT_TIMESTAMP
                WHERE match_id = ?
                  AND (deleted_at IS NULL OR deleted_at = '')
                """,
                (match_id,),
            )
            conn.execute(
                """
                INSERT INTO match_actions (match_id, deleted_at)
                VALUES (?, CURRENT_TIMESTAMP)
                ON CONFLICT(match_id) DO UPDATE SET deleted_at = excluded.deleted_at
                """,
                (match_id,),
            )
            conn.execute("DELETE FROM opening_lines WHERE match_id = ?", (match_id,))
        return cursor.rowcount

    def clear_all(self):
        """Move every active alert to deleted matches, preserving the rows for review."""
        with self._conn() as conn:
            rows = conn.execute(
                """
                SELECT DISTINCT match_id
                FROM alerts
                WHERE deleted_at IS NULL OR deleted_at = ''
                """
            ).fetchall()
            match_ids = [row["match_id"] for row in rows]
            cursor = conn.execute(
                """
                UPDATE alerts
                SET deleted_at = CURRENT_TIMESTAMP
                WHERE deleted_at IS NULL OR deleted_at = ''
                """
            )
            for match_id in match_ids:
                conn.execute(
                    """
                    INSERT INTO match_actions (match_id, deleted_at)
                    VALUES (?, CURRENT_TIMESTAMP)
                    ON CONFLICT(match_id) DO UPDATE SET deleted_at = excluded.deleted_at
                    """,
                    (match_id,),
                )
            conn.execute("DELETE FROM opening_lines")
        return cursor.rowcount

    # ---------- deleted matches ----------

    def recent_deleted_alerts(self, limit: int | None = 1000) -> list:
        with self._conn() as conn:
            if limit is None:
                rows = conn.execute(
                    """
                    SELECT * FROM alerts
                    WHERE deleted_at IS NOT NULL AND deleted_at != ''
                    ORDER BY deleted_at DESC, alerted_at DESC, id DESC
                    """
                ).fetchall()
            else:
                rows = conn.execute(
                    """
                    SELECT * FROM alerts
                    WHERE deleted_at IS NOT NULL AND deleted_at != ''
                    ORDER BY deleted_at DESC, alerted_at DESC, id DESC
                    LIMIT ?
                    """,
                    (limit,),
                ).fetchall()
        return [dict(r) for r in rows]

    def purge_deleted_matches(self) -> int:
        with self._conn() as conn:
            rows = conn.execute(
                """
                SELECT DISTINCT match_id
                FROM alerts
                WHERE deleted_at IS NOT NULL AND deleted_at != ''
                """
            ).fetchall()
            match_ids = [row["match_id"] for row in rows]
            if match_ids:
                placeholders = ", ".join("?" for _ in match_ids)
                cursor = conn.execute(
                    f"DELETE FROM alerts WHERE match_id IN ({placeholders})",
                    tuple(match_ids),
                )
                conn.execute(
                    f"DELETE FROM match_actions WHERE match_id IN ({placeholders})",
                    tuple(match_ids),
                )
                conn.execute(
                    f"DELETE FROM opening_lines WHERE match_id IN ({placeholders})",
                    tuple(match_ids),
                )
                conn.execute(
                    f"DELETE FROM finished_matches WHERE match_id IN ({placeholders})",
                    tuple(match_ids),
                )
            else:
                cursor = conn.execute(
                    "DELETE FROM alerts WHERE deleted_at IS NOT NULL AND deleted_at != ''"
                )
        return cursor.rowcount

    def update_deleted_alert_result(self, alert_id: int, result: str) -> bool:
        with self._conn() as conn:
            cursor = conn.execute(
                "UPDATE alerts SET result = ? WHERE id = ? AND deleted_at IS NOT NULL AND deleted_at != ''",
                (result, alert_id),
            )
        return cursor.rowcount > 0

    # ---------- saved bet slips ----------

    def save_bet_slip(self, name: str, payload: dict) -> int:
        with self._conn() as conn:
            cursor = conn.execute(
                """
                INSERT INTO saved_bet_slips (
                    name, requested_max_count, selected_count, eligible_count, message, payload_json
                )
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    name,
                    int(payload.get("requested_max_count") or 1),
                    int(payload.get("selected_count") or 0),
                    int(payload.get("eligible_count") or 0),
                    str(payload.get("message") or ""),
                    json.dumps(payload, ensure_ascii=False),
                ),
            )
        return cursor.lastrowid

    def list_saved_bet_slips(self, limit: int = 50) -> list:
        with self._conn() as conn:
            rows = conn.execute(
                """
                SELECT id, name, requested_max_count, selected_count, eligible_count, message, payload_json, created_at
                FROM saved_bet_slips
                ORDER BY created_at DESC, id DESC
                LIMIT ?
                """,
                (max(1, limit),),
            ).fetchall()

        items = []
        for row in rows:
            item = dict(row)
            try:
                payload = json.loads(item.get("payload_json") or "{}")
            except Exception:
                payload = {}
            item["payload"] = payload if isinstance(payload, dict) else {}
            item.pop("payload_json", None)
            items.append(item)
        return items

    def get_saved_bet_slip(self, slip_id: int) -> dict | None:
        with self._conn() as conn:
            row = conn.execute(
                """
                SELECT id, name, requested_max_count, selected_count, eligible_count, message, payload_json, created_at
                FROM saved_bet_slips
                WHERE id = ?
                """,
                (slip_id,),
            ).fetchone()
        if not row:
            return None
        item = dict(row)
        try:
            payload = json.loads(item.get("payload_json") or "{}")
        except Exception:
            payload = {}
        item["payload"] = payload if isinstance(payload, dict) else {}
        item.pop("payload_json", None)
        return item

    def latest_finished_by_match_ids(self, match_ids: list[str]) -> dict[str, dict]:
        keys = [str(mid) for mid in match_ids if str(mid).strip()]
        if not keys:
            return {}

        placeholders = ", ".join("?" for _ in keys)
        with self._conn() as conn:
            rows = conn.execute(
                f"""
                SELECT id, match_id, final_status, final_score, final_total, result, finished_at
                FROM finished_matches
                WHERE match_id IN ({placeholders})
                ORDER BY finished_at DESC, id DESC
                """,
                tuple(keys),
            ).fetchall()

        by_match: dict[str, dict] = {}
        for row in rows:
            item = dict(row)
            match_id = str(item.get("match_id") or "")
            if not match_id or match_id in by_match:
                continue
            by_match[match_id] = item
        return by_match

    def latest_alerts_by_match_ids(self, match_ids: list[str]) -> dict[str, dict]:
        keys = [str(mid) for mid in match_ids if str(mid).strip()]
        if not keys:
            return {}

        placeholders = ", ".join("?" for _ in keys)
        with self._conn() as conn:
            rows = conn.execute(
                f"""
                SELECT id, match_id, status, score, alerted_at, live, opening, bet_placed, followed, ignored
                FROM alerts
                WHERE match_id IN ({placeholders})
                  AND (deleted_at IS NULL OR deleted_at = '')
                ORDER BY alerted_at DESC, id DESC
                """,
                tuple(keys),
            ).fetchall()

        by_match: dict[str, dict] = {}
        for row in rows:
            item = dict(row)
            match_id = str(item.get("match_id") or "")
            if not match_id or match_id in by_match:
                continue
            by_match[match_id] = item
        return by_match

    def get_saved_bet_match_ids(self, limit: int = 500) -> set[str]:
        rows = self.list_saved_bet_slips(limit=max(1, limit))
        match_ids: set[str] = set()
        for row in rows:
            payload = row.get("payload") if isinstance(row, dict) else {}
            slip = payload.get("slip") if isinstance(payload, dict) else []
            if not isinstance(slip, list):
                continue
            for leg in slip:
                if not isinstance(leg, dict):
                    continue
                match_id = str(leg.get("match_id") or "").strip()
                if match_id:
                    match_ids.add(match_id)
        return match_ids

    def delete_saved_bet_slip(self, slip_id: int) -> bool:
        with self._conn() as conn:
            cursor = conn.execute(
                "DELETE FROM saved_bet_slips WHERE id = ?",
                (slip_id,),
            )
        return cursor.rowcount > 0

    def update_saved_bet_slip_result(self, slip_id: int, match_id: str, result: str) -> bool:
        saved = self.get_saved_bet_slip(slip_id)
        if not saved:
            return False

        payload = saved.get("payload") if isinstance(saved, dict) else {}
        if not isinstance(payload, dict):
            return False

        slip = payload.get("slip")
        if not isinstance(slip, list):
            return False

        target_match_id = str(match_id or "").strip()
        updated = False
        normalized_slip: list[dict] = []

        for leg in slip:
            if not isinstance(leg, dict):
                continue
            item = dict(leg)
            if str(item.get("match_id") or "").strip() == target_match_id:
                item["result"] = result
                updated = True
            normalized_slip.append(item)

        if not updated:
            return False

        payload["slip"] = normalized_slip
        with self._conn() as conn:
            conn.execute(
                """
                UPDATE saved_bet_slips
                SET payload_json = ?, selected_count = ?, eligible_count = ?, requested_max_count = ?, message = ?
                WHERE id = ?
                """,
                (
                    json.dumps(payload, ensure_ascii=False),
                    int(payload.get("selected_count") or len(normalized_slip)),
                    int(payload.get("eligible_count") or len(normalized_slip)),
                    int(payload.get("requested_max_count") or len(normalized_slip) or 1),
                    str(payload.get("message") or ""),
                    slip_id,
                ),
            )
        return True

    # ---------- finished matches ----------

    def recent_finished_matches(self, limit: int | None = 500) -> list:
        with self._conn() as conn:
            if limit is None:
                rows = conn.execute(
                    "SELECT * FROM finished_matches ORDER BY finished_at DESC, id DESC"
                ).fetchall()
            else:
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

    def update_finished_match_result(self, finished_match_id: int, result: str) -> bool:
        with self._conn() as conn:
            cursor = conn.execute(
                "UPDATE finished_matches SET result = ? WHERE id = ?",
                (result, finished_match_id),
            )
        return cursor.rowcount > 0

    def get_tracked_deleted_matches(self, limit: int = 200) -> list:
        """
        Return one latest deleted alert row per match where at least one deleted
        signal has not been copied into finished_matches yet.
        """
        with self._conn() as conn:
            rows = conn.execute(
                """
                SELECT a.match_id, a.match_name, a.tournament, a.url, a.status, a.score, a.alerted_at, a.deleted_at
                FROM alerts a
                INNER JOIN (
                    SELECT match_id, MAX(id) AS latest_alert_id
                    FROM alerts
                    WHERE url != ''
                      AND deleted_at IS NOT NULL
                      AND deleted_at != ''
                    GROUP BY match_id
                ) latest ON latest.latest_alert_id = a.id
                WHERE EXISTS (
                    SELECT 1
                    FROM alerts pending
                    LEFT JOIN finished_matches fm ON fm.source_alert_id = pending.id
                    WHERE pending.match_id = a.match_id
                      AND pending.deleted_at IS NOT NULL
                      AND pending.deleted_at != ''
                      AND fm.id IS NULL
                )
                ORDER BY a.deleted_at DESC, a.alerted_at DESC, a.id DESC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()
        return [dict(r) for r in rows]

    def get_pending_deleted_alerts_for_match(self, match_id: str) -> list:
        with self._conn() as conn:
            rows = conn.execute(
                """
                SELECT a.*
                FROM alerts a
                LEFT JOIN finished_matches fm ON fm.source_alert_id = a.id
                WHERE a.match_id = ?
                  AND a.deleted_at IS NOT NULL
                  AND a.deleted_at != ''
                  AND fm.id IS NULL
                ORDER BY a.alerted_at ASC, a.id ASC
                """,
                (match_id,),
            ).fetchall()
        return [dict(r) for r in rows]

    def get_tracked_live_matches(self, limit: int = 200) -> list:
        """Backward-compatible alias for the deleted-match finished queue."""
        return self.get_tracked_deleted_matches(limit=limit)

    def get_pending_alerts_for_match(self, match_id: str) -> list:
        """Backward-compatible alias for deleted alerts waiting for FT archive."""
        return self.get_pending_deleted_alerts_for_match(match_id)

    def archive_finished_alert(
        self,
        alert: dict,
        *,
        final_status: str,
        final_score: str,
        final_total: float | None,
        result: str,
        evaluation: dict | None = None,
    ) -> int:
        eval_data = evaluation or {}
        with self._conn() as conn:
            cursor = conn.execute(
                """
                INSERT OR IGNORE INTO finished_matches (
                    source_alert_id, match_id, match_name, tournament, status, final_status,
                    opening, prematch, live, direction, diff, url, bet_placed, ignored, followed,
                    alerted_at, score, signal_count, quality_grade, quality_score, quality_setup, quality_summary, quality_reasons,
                    counter_direction, counter_level, counter_score, counter_note, counter_reasons,
                    final_score, final_total, result,
                    margin, signal_timing_grade, market_read_correct, projection_accuracy,
                    quality_accuracy, counter_triggered, verdict, lesson
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    alert["id"],
                    alert["match_id"],
                    alert["match_name"],
                    alert.get("tournament", ""),
                    alert.get("status", ""),
                    final_status,
                    alert["opening"],
                    alert.get("prematch"),
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
                    alert.get("counter_direction", ""),
                    alert.get("counter_level", ""),
                    alert.get("counter_score", 0),
                    alert.get("counter_note", ""),
                    alert.get("counter_reasons", ""),
                    final_score,
                    final_total,
                    result,
                    eval_data.get("margin"),
                    eval_data.get("signal_timing_grade", ""),
                    eval_data.get("market_read_correct"),
                    eval_data.get("projection_accuracy"),
                    eval_data.get("quality_accuracy", ""),
                    eval_data.get("counter_triggered"),
                    eval_data.get("verdict", ""),
                    eval_data.get("lesson", ""),
                ),
            )
        return cursor.lastrowid if cursor.rowcount > 0 else 0
