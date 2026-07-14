import json
import logging
import os
import sqlite3
from contextlib import contextmanager
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError


logger = logging.getLogger(__name__)


def _run_compatible_migration(
    conn: sqlite3.Connection,
    statement: str,
    *,
    ignored_errors: tuple[str, ...],
) -> None:
    """Ignore only known schema-compatibility outcomes, never I/O/lock errors."""
    try:
        conn.execute(statement)
    except sqlite3.OperationalError as exc:
        message = str(exc).lower()
        if any(expected in message for expected in ignored_errors):
            return
        raise


class Database:
    def __init__(self, db_path: str):
        self.db_path = db_path

    @contextmanager
    def _conn(self):
        conn = sqlite3.connect(self.db_path, timeout=30)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA busy_timeout=30000")
        conn.execute("PRAGMA foreign_keys=ON")
        conn.execute("PRAGMA synchronous=NORMAL")
        try:
            yield conn
            conn.commit()
        finally:
            conn.close()

    def init(self):
        with self._conn() as conn:
            # Journal mode is persistent for the database file; setting it once
            # at schema initialization avoids a lock-taking pragma per query.
            conn.execute("PRAGMA journal_mode=WAL")
            conn.executescript("""
                CREATE TABLE IF NOT EXISTS alerts (
                    id           INTEGER PRIMARY KEY AUTOINCREMENT,
                    match_id     TEXT NOT NULL,
                    match_name   TEXT NOT NULL,
                    tournament   TEXT NOT NULL DEFAULT '',
                    status       TEXT NOT NULL DEFAULT '',
                    opening      REAL NOT NULL,
                    prematch     REAL,
                    live         REAL NOT NULL,
                    direction    TEXT NOT NULL,
                    diff         REAL NOT NULL,
                    url          TEXT NOT NULL DEFAULT '',
                    score        TEXT NOT NULL DEFAULT '',
                    final_status TEXT NOT NULL DEFAULT '',
                    final_score  TEXT NOT NULL DEFAULT '',
                    signal_count INTEGER NOT NULL DEFAULT 1,
                    ai_analysis  TEXT NOT NULL DEFAULT '',
                    display_snapshot TEXT NOT NULL DEFAULT '',
                    telegram_status TEXT NOT NULL DEFAULT 'not_required',
                    telegram_retry_count INTEGER NOT NULL DEFAULT 0,
                    telegram_last_error TEXT NOT NULL DEFAULT '',
                    telegram_message_ids TEXT NOT NULL DEFAULT '',
                    bet_placed   INTEGER NOT NULL DEFAULT 0,
                    ignored      INTEGER NOT NULL DEFAULT 0,
                    followed     INTEGER NOT NULL DEFAULT 0,
                    deleted_at   TIMESTAMP,
                    result       TEXT NOT NULL DEFAULT '',
                    result_source TEXT NOT NULL DEFAULT '',
                    settled_at   TIMESTAMP,
                    alerted_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );

                CREATE TABLE IF NOT EXISTS match_actions (
                    match_id    TEXT PRIMARY KEY,
                    bet_placed  INTEGER NOT NULL DEFAULT 0,
                    ignored     INTEGER NOT NULL DEFAULT 0,
                    followed    INTEGER NOT NULL DEFAULT 0,
                    note        TEXT NOT NULL DEFAULT '',
                    deleted_at  TIMESTAMP
                );

                CREATE TABLE IF NOT EXISTS upcoming_matches (
                    match_id       TEXT PRIMARY KEY,
                    match_name     TEXT NOT NULL,
                    home_team      TEXT NOT NULL DEFAULT '',
                    away_team      TEXT NOT NULL DEFAULT '',
                    tournament     TEXT NOT NULL DEFAULT '',
                    kickoff        TEXT NOT NULL DEFAULT '',
                    opening_total  REAL,
                    prematch_total REAL,
                    expected_total REAL,
                    direction      TEXT NOT NULL DEFAULT '',
                    diff           REAL,
                    url            TEXT NOT NULL DEFAULT '',
                    payload_json   TEXT NOT NULL DEFAULT '',
                    fetched_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );

                CREATE INDEX IF NOT EXISTS idx_upcoming_matches_fetched_at
                ON upcoming_matches(fetched_at DESC);

                CREATE TABLE IF NOT EXISTS upcoming_match_actions (
                    match_id    TEXT PRIMARY KEY,
                    bet_placed  INTEGER NOT NULL DEFAULT 0,
                    ignored     INTEGER NOT NULL DEFAULT 0,
                    followed    INTEGER NOT NULL DEFAULT 0,
                    deleted_at  TIMESTAMP,
                    updated_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );

                CREATE TABLE IF NOT EXISTS saved_match_lists (
                    id           INTEGER PRIMARY KEY AUTOINCREMENT,
                    name         TEXT NOT NULL,
                    match_count  INTEGER NOT NULL DEFAULT 0,
                    matches_json TEXT NOT NULL DEFAULT '[]',
                    created_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );

                CREATE INDEX IF NOT EXISTS idx_saved_match_lists_created_at
                ON saved_match_lists(created_at DESC, id DESC);

                CREATE TABLE IF NOT EXISTS signal_lists (
                    id               INTEGER PRIMARY KEY AUTOINCREMENT,
                    list_type        TEXT NOT NULL,
                    scope            TEXT NOT NULL,
                    value            TEXT NOT NULL,
                    normalized_value TEXT NOT NULL,
                    created_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(list_type, scope, normalized_value)
                );

                CREATE INDEX IF NOT EXISTS idx_signal_lists_lookup
                ON signal_lists(list_type, scope, normalized_value);

                CREATE TABLE IF NOT EXISTS signal_trials (
                    id               INTEGER PRIMARY KEY AUTOINCREMENT,
                    trial_key        TEXT NOT NULL UNIQUE,
                    alert_id         INTEGER,
                    match_id         TEXT NOT NULL,
                    policy_id        TEXT NOT NULL,
                    strategy_id      TEXT NOT NULL,
                    strategy_version INTEGER NOT NULL,
                    evidence_epoch   TEXT NOT NULL,
                    evaluated_at     TEXT NOT NULL,
                    gate_json        TEXT NOT NULL,
                    direction        TEXT NOT NULL,
                    live_line        REAL NOT NULL,
                    result           TEXT NOT NULL DEFAULT '',
                    result_source    TEXT NOT NULL DEFAULT '',
                    final_score      TEXT NOT NULL DEFAULT '',
                    final_status     TEXT NOT NULL DEFAULT '',
                    settled_at       TIMESTAMP,
                    created_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(policy_id, strategy_id, strategy_version, evidence_epoch, match_id)
                );

                CREATE INDEX IF NOT EXISTS idx_signal_trials_evidence
                ON signal_trials(policy_id, strategy_id, strategy_version, evidence_epoch, evaluated_at);

                CREATE INDEX IF NOT EXISTS idx_signal_trials_pending
                ON signal_trials(result, match_id);

                CREATE INDEX IF NOT EXISTS idx_signal_trials_match_result
                ON signal_trials(match_id, result);

                CREATE INDEX IF NOT EXISTS idx_signal_trials_alert
                ON signal_trials(alert_id);

            """)
            # Backward-compatible migrations for older DB files. New installs
            # get the clean schema above; old installs keep their extra quality_*
            # columns as ignored dead data — we don't try to DROP them because
            # SQLite support varies by version.
            for alter in (
                "ALTER TABLE alerts ADD COLUMN tournament TEXT NOT NULL DEFAULT ''",
                "ALTER TABLE alerts ADD COLUMN status TEXT NOT NULL DEFAULT ''",
                "ALTER TABLE alerts ADD COLUMN url TEXT NOT NULL DEFAULT ''",
                "ALTER TABLE alerts ADD COLUMN final_status TEXT NOT NULL DEFAULT ''",
                "ALTER TABLE alerts ADD COLUMN final_score TEXT NOT NULL DEFAULT ''",
                "ALTER TABLE alerts ADD COLUMN bet_placed INTEGER NOT NULL DEFAULT 0",
                "ALTER TABLE alerts ADD COLUMN ignored INTEGER NOT NULL DEFAULT 0",
                "ALTER TABLE alerts ADD COLUMN followed INTEGER NOT NULL DEFAULT 0",
                "ALTER TABLE alerts ADD COLUMN score TEXT NOT NULL DEFAULT ''",
                "ALTER TABLE alerts ADD COLUMN signal_count INTEGER NOT NULL DEFAULT 1",
                "ALTER TABLE alerts ADD COLUMN ai_analysis TEXT NOT NULL DEFAULT ''",
                "ALTER TABLE alerts ADD COLUMN display_snapshot TEXT NOT NULL DEFAULT ''",
                "ALTER TABLE alerts ADD COLUMN telegram_status TEXT NOT NULL DEFAULT 'not_required'",
                "ALTER TABLE alerts ADD COLUMN telegram_retry_count INTEGER NOT NULL DEFAULT 0",
                "ALTER TABLE alerts ADD COLUMN telegram_last_error TEXT NOT NULL DEFAULT ''",
                "ALTER TABLE alerts ADD COLUMN telegram_message_ids TEXT NOT NULL DEFAULT ''",
                "ALTER TABLE alerts ADD COLUMN deleted_at TIMESTAMP",
                "ALTER TABLE alerts ADD COLUMN prematch REAL",
                "ALTER TABLE alerts ADD COLUMN result TEXT NOT NULL DEFAULT ''",
                "ALTER TABLE alerts ADD COLUMN result_source TEXT NOT NULL DEFAULT ''",
                "ALTER TABLE alerts ADD COLUMN settled_at TIMESTAMP",
                "ALTER TABLE alerts ADD COLUMN alert_period INTEGER",
                "ALTER TABLE alerts ADD COLUMN alert_moment TEXT NOT NULL DEFAULT ''",
                "ALTER TABLE match_actions ADD COLUMN note TEXT NOT NULL DEFAULT ''",
                "ALTER TABLE match_actions ADD COLUMN deleted_at TIMESTAMP",
            ):
                _run_compatible_migration(
                    conn,
                    alter,
                    ignored_errors=("duplicate column name",),
                )
            conn.execute("CREATE INDEX IF NOT EXISTS idx_alerts_deleted_at ON alerts(deleted_at)")
            conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_alerts_match_period_state
                ON alerts(match_id, deleted_at, alert_period)
                """
            )
            conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_alerts_match_direction_state
                ON alerts(match_id, direction, deleted_at, alerted_at DESC)
                """
            )
            conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_alerts_deleted_result_match
                ON alerts(deleted_at, result, match_id)
                """
            )
            conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_alerts_telegram_delivery
                ON alerts(telegram_status, telegram_retry_count, deleted_at, alerted_at)
                """
            )
            duplicate_period = conn.execute(
                """
                SELECT match_id, alert_period, COUNT(*) AS duplicate_count
                FROM alerts
                WHERE (deleted_at IS NULL OR deleted_at = '')
                  AND alert_period > 0
                GROUP BY match_id, alert_period
                HAVING COUNT(*) > 1
                LIMIT 1
                """
            ).fetchone()
            if duplicate_period:
                # Do not delete or rewrite a user's legacy signals merely to
                # satisfy a new index. BEGIN IMMEDIATE + the period recheck in
                # save_alert still prevents any new duplicate. Once the legacy
                # conflict is archived/removed, the next init installs the
                # defensive unique index automatically.
                logger.warning(
                    "Legacy active period duplicate preserved; unique index "
                    "postponed for match=%s period=%s count=%s",
                    duplicate_period["match_id"],
                    duplicate_period["alert_period"],
                    duplicate_period["duplicate_count"],
                )
            else:
                conn.execute(
                    """
                    CREATE UNIQUE INDEX IF NOT EXISTS idx_alerts_active_match_period
                    ON alerts(match_id, alert_period)
                    WHERE (deleted_at IS NULL OR deleted_at = '')
                      AND alert_period > 0
                    """
                )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS signal_lists (
                    id               INTEGER PRIMARY KEY AUTOINCREMENT,
                    list_type        TEXT NOT NULL,
                    scope            TEXT NOT NULL,
                    value            TEXT NOT NULL,
                    normalized_value TEXT NOT NULL,
                    created_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(list_type, scope, normalized_value)
                )
                """
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_signal_lists_lookup ON signal_lists(list_type, scope, normalized_value)"
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS upcoming_matches (
                    match_id       TEXT PRIMARY KEY,
                    match_name     TEXT NOT NULL,
                    home_team      TEXT NOT NULL DEFAULT '',
                    away_team      TEXT NOT NULL DEFAULT '',
                    tournament     TEXT NOT NULL DEFAULT '',
                    kickoff        TEXT NOT NULL DEFAULT '',
                    opening_total  REAL,
                    prematch_total REAL,
                    expected_total REAL,
                    direction      TEXT NOT NULL DEFAULT '',
                    diff           REAL,
                    url            TEXT NOT NULL DEFAULT '',
                    payload_json   TEXT NOT NULL DEFAULT '',
                    fetched_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
                """
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_upcoming_matches_fetched_at ON upcoming_matches(fetched_at DESC)"
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS upcoming_match_actions (
                    match_id    TEXT PRIMARY KEY,
                    bet_placed  INTEGER NOT NULL DEFAULT 0,
                    ignored     INTEGER NOT NULL DEFAULT 0,
                    followed    INTEGER NOT NULL DEFAULT 0,
                    deleted_at  TIMESTAMP,
                    updated_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
                """
            )
            conn.execute(
                """
                UPDATE alerts
                SET alert_moment = TRIM(
                    CASE
                        WHEN COALESCE(status, '') != '' AND COALESCE(score, '') != ''
                            THEN status || ' | ' || score
                        WHEN COALESCE(status, '') != '' THEN status
                        WHEN COALESCE(score, '') != '' THEN score
                        ELSE ''
                    END
                )
                WHERE COALESCE(alert_moment, '') = ''
                  AND TRIM(COALESCE(result, '')) = ''
                """
            )

    # ---------- signal black/white lists ----------

    @staticmethod
    def normalize_signal_list_value(value: str) -> str:
        return (
            str(value or "").strip().lower()
            .replace("ı", "i").replace("ş", "s")
            .replace("ğ", "g").replace("ü", "u")
            .replace("ö", "o").replace("ç", "c")
        )

    @staticmethod
    def _clean_signal_list_type(value: str) -> str:
        text = str(value or "").strip().lower()
        return text if text in {"black", "white"} else ""

    @staticmethod
    def _clean_signal_list_scope(value: str) -> str:
        text = str(value or "").strip().lower()
        return text if text in {"team", "league"} else ""

    def add_signal_list_entry(self, list_type: str, scope: str, value: str) -> dict | None:
        clean_type = self._clean_signal_list_type(list_type)
        clean_scope = self._clean_signal_list_scope(scope)
        clean_value = str(value or "").strip()[:200]
        normalized = self.normalize_signal_list_value(clean_value)
        if not clean_type or not clean_scope or not normalized:
            return None

        with self._conn() as conn:
            conn.execute(
                """
                INSERT INTO signal_lists (list_type, scope, value, normalized_value)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(list_type, scope, normalized_value)
                DO UPDATE SET value = excluded.value
                """,
                (clean_type, clean_scope, clean_value, normalized),
            )
            row = conn.execute(
                """
                SELECT id, list_type, scope, value, normalized_value, created_at
                FROM signal_lists
                WHERE list_type = ? AND scope = ? AND normalized_value = ?
                """,
                (clean_type, clean_scope, normalized),
            ).fetchone()
        return dict(row) if row else None

    def list_signal_list_entries(self) -> list[dict]:
        with self._conn() as conn:
            rows = conn.execute(
                """
                SELECT id, list_type, scope, value, normalized_value, created_at
                FROM signal_lists
                ORDER BY list_type ASC, scope ASC, value COLLATE NOCASE ASC
                """
            ).fetchall()
        return [dict(r) for r in rows]

    def delete_signal_list_entry(self, entry_id: int) -> bool:
        with self._conn() as conn:
            cursor = conn.execute(
                "DELETE FROM signal_lists WHERE id = ?",
                (int(entry_id),),
            )
        return cursor.rowcount > 0

    # ---------- alerts ----------

    def was_alerted_in_period(self, match_id: str, period: int) -> bool:
        if period is None:
            return False
        with self._conn() as conn:
            row = conn.execute(
                """
                SELECT 1 FROM alerts
                WHERE match_id = ?
                  AND alert_period = ?
                  AND (deleted_at IS NULL OR deleted_at = '')
                LIMIT 1
                """,
                (match_id, int(period)),
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
        with self._conn() as conn:
            row = conn.execute(
                "SELECT COUNT(*) as cnt FROM alerts WHERE match_id = ? AND (deleted_at IS NULL OR deleted_at = '')",
                (match_id,),
            ).fetchone()
        return row["cnt"] if row else 0

    def latest_match_alert_in_direction(self, match_id: str, direction: str) -> dict | None:
        with self._conn() as conn:
            row = conn.execute(
                """
                SELECT * FROM alerts
                WHERE match_id = ?
                  AND direction = ?
                  AND (deleted_at IS NULL OR deleted_at = '')
                ORDER BY alerted_at DESC, id DESC
                LIMIT 1
                """,
                (match_id, direction),
            ).fetchone()
        return dict(row) if row else None

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
        prematch: float | None = None,
        ai_analysis: str = "",
        alert_period: int | None = None,
        alert_moment: str = "",
        telegram_required: bool = False,
    ) -> int:
        parsed_analysis = {}
        try:
            parsed_analysis = json.loads(ai_analysis or "{}")
        except (TypeError, ValueError, json.JSONDecodeError):
            parsed_analysis = {}
        if not isinstance(parsed_analysis, dict):
            parsed_analysis = {}

        with self._conn() as conn:
            conn.execute("BEGIN IMMEDIATE")
            action = conn.execute(
                "SELECT bet_placed, ignored, followed, deleted_at FROM match_actions WHERE match_id = ?",
                (match_id,),
            ).fetchone()
            if action and str(action["deleted_at"] or "").strip():
                raise RuntimeError("cannot save an alert for an archived match")

            active_count_row = conn.execute(
                """
                SELECT COUNT(*) AS cnt
                FROM alerts
                WHERE match_id = ?
                  AND (deleted_at IS NULL OR deleted_at = '')
                """,
                (match_id,),
            ).fetchone()
            active_count = int(active_count_row["cnt"] if active_count_row else 0)
            if active_count != max(0, int(signal_count or 1) - 1):
                raise RuntimeError("active alert state changed before insert")

            if alert_period is not None and int(alert_period) > 0:
                period_row = conn.execute(
                    """
                    SELECT 1
                    FROM alerts
                    WHERE match_id = ?
                      AND alert_period = ?
                      AND (deleted_at IS NULL OR deleted_at = '')
                    LIMIT 1
                    """,
                    (match_id, int(alert_period)),
                ).fetchone()
                if period_row:
                    raise RuntimeError("active alert already exists for this period")

            bet = action["bet_placed"] if action else 0
            ign = action["ignored"] if action else 0
            fol = action["followed"] if action else 0
            cursor = conn.execute(
                """
                INSERT INTO alerts (
                    match_id, match_name, opening, prematch, live, direction, diff,
                    tournament, status, url, score, signal_count, ai_analysis,
                    bet_placed, ignored, followed, alert_period, alert_moment,
                    telegram_status
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    match_id, match_name, opening, prematch, live, direction, diff,
                    tournament, status, url, score, signal_count, ai_analysis,
                    bet, ign, fol, alert_period, alert_moment,
                    "pending" if telegram_required else "not_required",
                ),
            )
            alert_id = int(cursor.lastrowid)

            gate = parsed_analysis.get("signal_gate")
            if isinstance(gate, dict) and gate.get("trial_eligible"):
                trial_key = str(gate.get("trial_key") or "").strip()
                if trial_key:
                    conn.execute(
                        """
                        INSERT OR IGNORE INTO signal_trials (
                            trial_key, alert_id, match_id, policy_id, strategy_id,
                            strategy_version, evidence_epoch, evaluated_at, gate_json,
                            direction, live_line
                        )
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            trial_key,
                            alert_id,
                            match_id,
                            str(gate.get("policy_id") or ""),
                            str(gate.get("strategy_id") or ""),
                            int(gate.get("strategy_version") or 0),
                            str(gate.get("evidence_epoch") or ""),
                            str(gate.get("evaluated_at") or ""),
                            json.dumps(gate, ensure_ascii=False, sort_keys=True),
                            direction,
                            float(live),
                        ),
                    )
            return alert_id

    def signal_trial_rows(
        self,
        *,
        policy_id: str | None = None,
        strategy_id: str | None = None,
        strategy_version: int | None = None,
        evidence_epoch: str | None = None,
        limit: int | None = None,
    ) -> list[dict]:
        """Return a bounded durable evidence-ledger shape when requested."""
        clauses: list[str] = []
        params: list = []
        for column, value in (
            ("policy_id", policy_id),
            ("strategy_id", strategy_id),
            ("strategy_version", strategy_version),
            ("evidence_epoch", evidence_epoch),
        ):
            if value is None:
                continue
            clauses.append(f"{column} = ?")
            params.append(value)
        where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        bounded = limit is not None
        order = "DESC" if bounded else "ASC"
        limit_sql = " LIMIT ?" if bounded else ""
        if bounded:
            params.append(max(1, int(limit or 1)))
        with self._conn() as conn:
            rows = conn.execute(
                f"""
                SELECT id, match_id, result, result_source, settled_at, gate_json
                FROM signal_trials
                {where}
                ORDER BY evaluated_at {order}, id {order}
                {limit_sql}
                """,
                tuple(params),
            ).fetchall()
        if bounded:
            rows = list(reversed(rows))
        items = []
        for row in rows:
            try:
                gate = json.loads(row["gate_json"] or "{}")
            except (TypeError, ValueError, json.JSONDecodeError) as exc:
                raise RuntimeError(f"corrupt signal trial gate payload: id={row['id']}") from exc
            if not isinstance(gate, dict):
                raise RuntimeError(f"invalid signal trial gate payload: id={row['id']}")
            items.append({
                "id": int(row["id"]),
                "match_id": row["match_id"],
                "result": row["result"],
                "result_source": row["result_source"],
                "settled_at": row["settled_at"],
                "ai_analysis": json.dumps(
                    {"signal_gate": gate},
                    ensure_ascii=False,
                ),
            })
        return items

    def signal_trials_for_match(
        self,
        match_id: str,
        *,
        unresolved_only: bool = True,
    ) -> list[dict]:
        where = "AND TRIM(COALESCE(result, '')) = ''" if unresolved_only else ""
        with self._conn() as conn:
            rows = conn.execute(
                f"""
                SELECT id, alert_id, match_id, direction, live_line, result,
                       result_source, final_score, final_status, settled_at
                FROM signal_trials
                WHERE match_id = ?
                  {where}
                ORDER BY evaluated_at ASC, id ASC
                """,
                (str(match_id or "").strip(),),
            ).fetchall()
        return [dict(row) for row in rows]

    def update_signal_trial_final_result(
        self,
        trial_id: int,
        *,
        result: str,
        final_score: str,
        final_status: str,
        force: bool = False,
    ) -> bool:
        unresolved_only = "" if force else "AND TRIM(COALESCE(result, '')) = ''"
        with self._conn() as conn:
            cursor = conn.execute(
                f"""
                UPDATE signal_trials
                SET result = ?,
                    result_source = 'automatic_final_score',
                    final_score = ?,
                    final_status = ?,
                    settled_at = CURRENT_TIMESTAMP
                WHERE id = ?
                  {unresolved_only}
                """,
                (result, final_score, final_status, int(trial_id)),
            )
        return cursor.rowcount > 0

    def pending_telegram_alerts(self, limit: int = 20, max_retries: int = 8) -> list[dict]:
        with self._conn() as conn:
            rows = conn.execute(
                """
                SELECT *
                FROM alerts
                WHERE telegram_status IN ('pending', 'retry')
                  AND telegram_retry_count < ?
                  AND (deleted_at IS NULL OR deleted_at = '')
                ORDER BY alerted_at ASC, id ASC
                LIMIT ?
                """,
                (max(1, int(max_retries)), max(1, int(limit))),
            ).fetchall()
        return [dict(row) for row in rows]

    def mark_telegram_delivery_sent(self, alert_id: int, message_ids: dict) -> bool:
        with self._conn() as conn:
            cursor = conn.execute(
                """
                UPDATE alerts
                SET telegram_status = 'sent',
                    telegram_last_error = '',
                    telegram_message_ids = ?
                WHERE id = ?
                  AND telegram_status IN ('pending', 'retry')
                """,
                (json.dumps(message_ids or {}, ensure_ascii=False), int(alert_id)),
            )
        return cursor.rowcount > 0

    def mark_telegram_delivery_failed(
        self,
        alert_id: int,
        error: str,
        *,
        message_ids: dict | None = None,
        max_retries: int = 8,
    ) -> bool:
        clean_error = str(error or "delivery failed").strip()[:500]
        serialized_ids = (
            json.dumps(message_ids, ensure_ascii=False)
            if isinstance(message_ids, dict)
            else None
        )
        with self._conn() as conn:
            cursor = conn.execute(
                """
                UPDATE alerts
                SET telegram_status = CASE
                        WHEN telegram_retry_count + 1 >= ? THEN 'failed'
                        ELSE 'retry'
                    END,
                    telegram_retry_count = telegram_retry_count + 1,
                    telegram_last_error = ?,
                    telegram_message_ids = CASE
                        WHEN ? IS NOT NULL THEN ?
                        ELSE telegram_message_ids
                    END
                WHERE id = ?
                  AND telegram_status IN ('pending', 'retry')
                """,
                (
                    max(1, int(max_retries)),
                    clean_error,
                    serialized_ids,
                    serialized_ids,
                    int(alert_id),
                ),
            )
        return cursor.rowcount > 0

    def update_alert_live_snapshot(
        self,
        alert_id: int,
        *,
        status: str = "",
        score: str = "",
        ai_analysis: str = "",
    ) -> bool:
        with self._conn() as conn:
            cursor = conn.execute(
                """
                UPDATE alerts
                SET status = CASE WHEN ? != '' THEN ? ELSE status END,
                    score = CASE WHEN ? != '' THEN ? ELSE score END,
                    ai_analysis = CASE WHEN ? != '' THEN ? ELSE ai_analysis END
                WHERE id = ?
                  AND (deleted_at IS NULL OR deleted_at = '')
                """,
                (
                    status, status,
                    score, score,
                    ai_analysis, ai_analysis,
                    int(alert_id),
                ),
            )
        return cursor.rowcount > 0

    def recent_alerts(self, limit: int = 200) -> list:
        with self._conn() as conn:
            rows = conn.execute(
                """
                SELECT a.*, COALESCE(ma.note, '') AS note
                FROM alerts a
                LEFT JOIN match_actions ma ON ma.match_id = a.match_id
                WHERE (a.deleted_at IS NULL OR a.deleted_at = '')
                  AND NOT (
                    COALESCE(a.alert_period, 0) = -1
                    AND COALESCE(a.alert_moment, '') = 'Gelecek Maç'
                  )
                  AND COALESCE(a.status, '') != 'Gelecek'
                ORDER BY a.alerted_at DESC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()
        return [dict(r) for r in rows]

    def active_alerts(self, limit: int | None = None) -> list:
        with self._conn() as conn:
            sql = """
                SELECT a.*, COALESCE(ma.note, '') AS note
                FROM alerts a
                LEFT JOIN match_actions ma ON ma.match_id = a.match_id
                WHERE (a.deleted_at IS NULL OR a.deleted_at = '')
                  AND NOT (
                    COALESCE(a.alert_period, 0) = -1
                    AND COALESCE(a.alert_moment, '') = 'Gelecek Maç'
                  )
                  AND COALESCE(a.status, '') != 'Gelecek'
                ORDER BY a.alerted_at ASC, a.id ASC
            """
            if limit is None:
                rows = conn.execute(sql).fetchall()
            else:
                rows = conn.execute(f"{sql} LIMIT ?", (limit,)).fetchall()
        return [dict(r) for r in rows]

    def all_active_alerts(self, limit: int | None = None) -> list:
        """Return every active alert, including rows hidden from the live list."""
        with self._conn() as conn:
            sql = """
                SELECT a.*, COALESCE(ma.note, '') AS note
                FROM alerts a
                LEFT JOIN match_actions ma ON ma.match_id = a.match_id
                WHERE (a.deleted_at IS NULL OR a.deleted_at = '')
                ORDER BY a.alerted_at ASC, a.id ASC
            """
            if limit is None:
                rows = conn.execute(sql).fetchall()
            else:
                rows = conn.execute(f"{sql} LIMIT ?", (limit,)).fetchall()
        return [dict(r) for r in rows]

    def active_alerts_for_match(self, match_id: str) -> list:
        with self._conn() as conn:
            rows = conn.execute(
                """
                SELECT a.*, COALESCE(ma.note, '') AS note
                FROM alerts a
                LEFT JOIN match_actions ma ON ma.match_id = a.match_id
                WHERE a.match_id = ?
                  AND (a.deleted_at IS NULL OR a.deleted_at = '')
                ORDER BY a.alerted_at ASC, a.id ASC
                """,
                (match_id,),
            ).fetchall()
        return [dict(r) for r in rows]

    def update_alert_ai_analysis(self, alert_id: int, ai_analysis: str, *, active_only: bool = True) -> bool:
        where = "id = ?"
        if active_only:
            where += " AND (deleted_at IS NULL OR deleted_at = '')"
        with self._conn() as conn:
            cursor = conn.execute(
                f"UPDATE alerts SET ai_analysis = ? WHERE {where}",
                (ai_analysis, int(alert_id)),
            )
        return cursor.rowcount > 0

    def save_active_alert_display_snapshots(self, snapshots: dict[int, dict]) -> int:
        if not snapshots:
            return 0
        updated = 0
        with self._conn() as conn:
            for alert_id, payload in snapshots.items():
                cursor = conn.execute(
                    """
                    UPDATE alerts
                    SET display_snapshot = ?
                    WHERE id = ?
                      AND (deleted_at IS NULL OR deleted_at = '')
                    """,
                    (json.dumps(payload, ensure_ascii=False), int(alert_id)),
                )
                updated += cursor.rowcount
        return updated

    @staticmethod
    def _serialized_display_snapshots(snapshots: dict[int, dict]) -> dict[int, str]:
        """Validate and serialize dashboard snapshots before opening a write transaction."""
        serialized: dict[int, str] = {}
        for raw_alert_id, payload in (snapshots or {}).items():
            alert_id = int(raw_alert_id or 0)
            if alert_id <= 0 or not isinstance(payload, dict):
                raise ValueError("invalid live dashboard snapshot")
            payload_id = int(payload.get("id") or alert_id)
            if payload_id != alert_id:
                raise ValueError("live dashboard snapshot id mismatch")
            serialized[alert_id] = json.dumps(payload, ensure_ascii=False)
        return serialized

    def archive_match_with_display_snapshots(
        self,
        match_id: str,
        snapshots: dict[int, dict],
    ) -> int:
        """Persist every live row and soft-delete the match atomically.

        The exact active-id set is checked while holding a write lock. A signal
        inserted between dashboard rendering and archiving therefore aborts the
        operation instead of being moved without its live dashboard state.
        """
        clean_match_id = str(match_id or "").strip()
        if not clean_match_id:
            raise ValueError("match_id is required")
        serialized = self._serialized_display_snapshots(snapshots)
        if not serialized:
            raise RuntimeError("active alert is missing its live dashboard snapshot")

        with self._conn() as conn:
            conn.execute("BEGIN IMMEDIATE")
            rows = conn.execute(
                """
                SELECT id
                FROM alerts
                WHERE match_id = ?
                  AND (deleted_at IS NULL OR deleted_at = '')
                ORDER BY id
                """,
                (clean_match_id,),
            ).fetchall()
            active_ids = {int(row["id"]) for row in rows}
            if not active_ids:
                return 0
            if active_ids != set(serialized):
                raise RuntimeError(
                    "active alerts changed while capturing live dashboard snapshots"
                )

            for alert_id in sorted(active_ids):
                cursor = conn.execute(
                    """
                    UPDATE alerts
                    SET display_snapshot = ?
                    WHERE id = ?
                      AND match_id = ?
                      AND (deleted_at IS NULL OR deleted_at = '')
                    """,
                    (serialized[alert_id], alert_id, clean_match_id),
                )
                if cursor.rowcount != 1:
                    raise RuntimeError("could not persist live dashboard snapshot")

            cursor = conn.execute(
                """
                UPDATE alerts
                SET deleted_at = CURRENT_TIMESTAMP,
                    telegram_status = CASE
                        WHEN telegram_status IN ('pending', 'retry') THEN 'cancelled'
                        ELSE telegram_status
                    END
                WHERE match_id = ?
                  AND (deleted_at IS NULL OR deleted_at = '')
                """,
                (clean_match_id,),
            )
            if cursor.rowcount != len(active_ids):
                raise RuntimeError("active alerts changed during archive transaction")
            conn.execute(
                """
                INSERT INTO match_actions (match_id, deleted_at)
                VALUES (?, CURRENT_TIMESTAMP)
                ON CONFLICT(match_id) DO UPDATE SET deleted_at = excluded.deleted_at
                """,
                (clean_match_id,),
            )
        return cursor.rowcount

    def archive_all_with_display_snapshots(self, snapshots: dict[int, dict]) -> int:
        """Atomically snapshot and soft-delete the complete active alert set."""
        serialized = self._serialized_display_snapshots(snapshots)
        with self._conn() as conn:
            conn.execute("BEGIN IMMEDIATE")
            rows = conn.execute(
                """
                SELECT id, match_id
                FROM alerts
                WHERE deleted_at IS NULL OR deleted_at = ''
                ORDER BY id
                """
            ).fetchall()
            active_ids = {int(row["id"]) for row in rows}
            if not active_ids:
                return 0
            if active_ids != set(serialized):
                raise RuntimeError(
                    "active alerts changed while capturing live dashboard snapshots"
                )

            for alert_id in sorted(active_ids):
                cursor = conn.execute(
                    """
                    UPDATE alerts
                    SET display_snapshot = ?
                    WHERE id = ?
                      AND (deleted_at IS NULL OR deleted_at = '')
                    """,
                    (serialized[alert_id], alert_id),
                )
                if cursor.rowcount != 1:
                    raise RuntimeError("could not persist live dashboard snapshot")

            cursor = conn.execute(
                """
                UPDATE alerts
                SET deleted_at = CURRENT_TIMESTAMP,
                    telegram_status = CASE
                        WHEN telegram_status IN ('pending', 'retry') THEN 'cancelled'
                        ELSE telegram_status
                    END
                WHERE deleted_at IS NULL OR deleted_at = ''
                """
            )
            if cursor.rowcount != len(active_ids):
                raise RuntimeError("active alerts changed during archive transaction")
            match_ids = {str(row["match_id"]) for row in rows}
            for archived_match_id in match_ids:
                conn.execute(
                    """
                    INSERT INTO match_actions (match_id, deleted_at)
                    VALUES (?, CURRENT_TIMESTAMP)
                    ON CONFLICT(match_id) DO UPDATE SET deleted_at = excluded.deleted_at
                    """,
                    (archived_match_id,),
                )
        return cursor.rowcount

    def update_match_note(self, match_id: str, note: str) -> int:
        clean_note = str(note or "").strip()[:240]
        with self._conn() as conn:
            row = conn.execute(
                "SELECT COUNT(*) AS cnt FROM alerts WHERE match_id = ?",
                (match_id,),
            ).fetchone()
            conn.execute(
                """
                INSERT INTO match_actions (match_id, note)
                VALUES (?, ?)
                ON CONFLICT(match_id) DO UPDATE SET note = excluded.note
                """,
                (match_id, clean_note),
            )
        return int(row["cnt"] if row else 0)

    def set_match_statuses(
        self,
        match_id: str,
        *,
        bet_placed: bool | None = None,
        ignored: bool | None = None,
        followed: bool | None = None,
    ) -> int:
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
        """Permanently remove an already archived alert only.

        Keeping the historic method name preserves the dashboard API contract,
        while the predicate prevents an active signal id from being hard-deleted.
        """
        with self._conn() as conn:
            row = conn.execute(
                """
                SELECT a.match_id,
                       EXISTS (
                           SELECT 1
                           FROM signal_trials trial
                           WHERE trial.match_id = a.match_id
                             AND TRIM(COALESCE(trial.result, '')) = ''
                       ) AS protected_trial
                FROM alerts a
                WHERE a.id = ?
                  AND a.deleted_at IS NOT NULL
                  AND a.deleted_at != ''
                """,
                (alert_id,),
            ).fetchone()
            if not row or bool(row["protected_trial"]):
                return False
            cursor = conn.execute(
                """
                DELETE FROM alerts
                WHERE id = ?
                  AND deleted_at IS NOT NULL
                  AND deleted_at != ''
                """,
                (alert_id,),
            )
            if cursor.rowcount:
                conn.execute(
                    """
                    DELETE FROM match_actions
                    WHERE match_id = ?
                      AND NOT EXISTS (
                          SELECT 1 FROM alerts WHERE match_id = ?
                      )
                    """,
                    (row["match_id"], row["match_id"]),
                )
        return cursor.rowcount > 0

    def is_deleted_alert_protected(self, alert_id: int) -> bool:
        """Return whether hard deletion would orphan prospective evidence."""
        with self._conn() as conn:
            row = conn.execute(
                """
                SELECT 1
                FROM alerts a
                WHERE a.id = ?
                  AND a.deleted_at IS NOT NULL
                  AND a.deleted_at != ''
                  AND EXISTS (
                      SELECT 1
                      FROM signal_trials trial
                      WHERE trial.match_id = a.match_id
                        AND TRIM(COALESCE(trial.result, '')) = ''
                  )
                LIMIT 1
                """,
                (int(alert_id),),
            ).fetchone()
        return row is not None

    def get_active_matches_with_urls(self) -> list:
        with self._conn() as conn:
            rows = conn.execute(
                """
                SELECT a.match_id, a.match_name, a.url, a.status
                FROM alerts a
                INNER JOIN (
                    SELECT match_id, MAX(id) AS latest_id
                FROM alerts
                WHERE (deleted_at IS NULL OR deleted_at = '')
                  AND url != ''
                  AND NOT (
                    COALESCE(alert_period, 0) = -1
                    AND COALESCE(alert_moment, '') = 'Gelecek Maç'
                  )
                  AND COALESCE(status, '') != 'Gelecek'
                GROUP BY match_id
                ) latest ON latest.latest_id = a.id
                ORDER BY a.alerted_at DESC, a.id DESC
                """
            ).fetchall()
        return [dict(r) for r in rows]

    def delete_match_data(self, match_id: str, *, require_display_snapshot: bool = False) -> int:
        with self._conn() as conn:
            if require_display_snapshot:
                conn.execute("BEGIN IMMEDIATE")
                missing = conn.execute(
                    """
                    SELECT COUNT(*) AS cnt
                    FROM alerts
                    WHERE match_id = ?
                      AND (deleted_at IS NULL OR deleted_at = '')
                      AND TRIM(COALESCE(display_snapshot, '')) = ''
                    """,
                    (match_id,),
                ).fetchone()
                if int(missing["cnt"] if missing else 0) > 0:
                    raise RuntimeError("active alert is missing its live dashboard snapshot")
            cursor = conn.execute(
                """
                UPDATE alerts
                SET deleted_at = CURRENT_TIMESTAMP,
                    telegram_status = CASE
                        WHEN telegram_status IN ('pending', 'retry') THEN 'cancelled'
                        ELSE telegram_status
                    END
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
        return cursor.rowcount

    def clear_all(self, *, require_display_snapshot: bool = False):
        with self._conn() as conn:
            if require_display_snapshot:
                conn.execute("BEGIN IMMEDIATE")
                missing = conn.execute(
                    """
                    SELECT COUNT(*) AS cnt
                    FROM alerts
                    WHERE (deleted_at IS NULL OR deleted_at = '')
                      AND TRIM(COALESCE(display_snapshot, '')) = ''
                    """
                ).fetchone()
                if int(missing["cnt"] if missing else 0) > 0:
                    raise RuntimeError("active alert is missing its live dashboard snapshot")
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
                SET deleted_at = CURRENT_TIMESTAMP,
                    telegram_status = CASE
                        WHEN telegram_status IN ('pending', 'retry') THEN 'cancelled'
                        ELSE telegram_status
                    END
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
        return cursor.rowcount

    # ---------- deleted matches ----------

    def recent_deleted_alerts(self, limit: int | None = 1000) -> list:
        with self._conn() as conn:
            if limit is None:
                rows = conn.execute(
                    """
                    SELECT a.*, COALESCE(ma.note, '') AS note
                    FROM alerts a
                    LEFT JOIN match_actions ma ON ma.match_id = a.match_id
                    WHERE a.deleted_at IS NOT NULL AND a.deleted_at != ''
                    ORDER BY a.deleted_at DESC, a.alerted_at DESC, a.id DESC
                    """
                ).fetchall()
            else:
                rows = conn.execute(
                    """
                    SELECT a.*, COALESCE(ma.note, '') AS note
                    FROM alerts a
                    LEFT JOIN match_actions ma ON ma.match_id = a.match_id
                    WHERE a.deleted_at IS NOT NULL AND a.deleted_at != ''
                    ORDER BY a.deleted_at DESC, a.alerted_at DESC, a.id DESC
                    LIMIT ?
                    """,
                    (limit,),
                ).fetchall()
        return [dict(r) for r in rows]

    def purge_deleted_matches(self) -> dict:
        with self._conn() as conn:
            protected_row = conn.execute(
                """
                SELECT COUNT(*) AS cnt
                FROM alerts a
                WHERE a.deleted_at IS NOT NULL
                  AND a.deleted_at != ''
                  AND EXISTS (
                      SELECT 1
                      FROM signal_trials trial
                      WHERE trial.match_id = a.match_id
                        AND TRIM(COALESCE(trial.result, '')) = ''
                  )
                """
            ).fetchone()
            protected_count = int(protected_row["cnt"] if protected_row else 0)
            rows = conn.execute(
                """
                SELECT DISTINCT match_id
                FROM alerts a
                WHERE a.deleted_at IS NOT NULL
                  AND a.deleted_at != ''
                  AND NOT EXISTS (
                      SELECT 1
                      FROM signal_trials trial
                      WHERE trial.match_id = a.match_id
                        AND TRIM(COALESCE(trial.result, '')) = ''
                  )
                """
            ).fetchall()
            match_ids = [row["match_id"] for row in rows]
            cursor = conn.execute(
                """
                DELETE FROM alerts
                WHERE deleted_at IS NOT NULL
                  AND deleted_at != ''
                  AND NOT EXISTS (
                      SELECT 1
                      FROM signal_trials trial
                      WHERE trial.match_id = alerts.match_id
                        AND TRIM(COALESCE(trial.result, '')) = ''
                  )
                """
            )
            if match_ids:
                placeholders = ", ".join("?" for _ in match_ids)
                # A match id can be reused after an archive is cleared. Keep
                # its action row whenever a current alert still references it.
                conn.execute(
                    f"""
                    DELETE FROM match_actions
                    WHERE match_id IN ({placeholders})
                      AND NOT EXISTS (
                          SELECT 1
                          FROM alerts a
                          WHERE a.match_id = match_actions.match_id
                      )
                    """,
                    tuple(match_ids),
                )
        return {
            "deleted_count": int(cursor.rowcount or 0),
            "protected_count": protected_count,
        }

    def get_deleted_alert_by_id(self, alert_id: int) -> dict | None:
        with self._conn() as conn:
            row = conn.execute(
                """
                SELECT * FROM alerts
                WHERE id = ?
                  AND deleted_at IS NOT NULL
                  AND deleted_at != ''
                """,
                (alert_id,),
            ).fetchone()
        return dict(row) if row else None

    def update_deleted_alert_result(self, alert_id: int, result: str) -> bool:
        with self._conn() as conn:
            cursor = conn.execute(
                """
                UPDATE alerts
                SET result = ?,
                    result_source = CASE WHEN ? != '' THEN 'manual' ELSE '' END,
                    settled_at = CASE WHEN ? != '' THEN CURRENT_TIMESTAMP ELSE NULL END
                WHERE id = ?
                  AND deleted_at IS NOT NULL
                  AND deleted_at != ''
                """,
                (result, result, result, alert_id),
            )
        return cursor.rowcount > 0

    def update_deleted_alert_final_result(
        self,
        alert_id: int,
        *,
        result: str,
        final_score: str,
        final_status: str,
        force: bool = False,
    ) -> bool:
        unresolved_only = "" if force else "AND TRIM(COALESCE(result, '')) = ''"
        with self._conn() as conn:
            cursor = conn.execute(
                f"""
                UPDATE alerts
                SET result = ?,
                    final_score = CASE WHEN ? != '' THEN ? ELSE final_score END,
                    final_status = CASE WHEN ? != '' THEN ? ELSE final_status END,
                    result_source = 'automatic_final_score',
                    settled_at = CURRENT_TIMESTAMP
                WHERE id = ?
                  AND deleted_at IS NOT NULL
                  AND deleted_at != ''
                  {unresolved_only}
                """,
                (
                    result,
                    final_score, final_score,
                    final_status, final_status,
                    alert_id,
                ),
            )
            if cursor.rowcount and not force:
                conn.execute(
                    """
                    UPDATE signal_trials
                    SET result = ?,
                        result_source = 'automatic_final_score',
                        final_score = ?,
                        final_status = ?,
                        settled_at = CURRENT_TIMESTAMP
                    WHERE alert_id = ?
                      AND TRIM(COALESCE(result, '')) = ''
                    """,
                    (result, final_score, final_status, alert_id),
                )
        return cursor.rowcount > 0

    def update_deleted_match_final_observation(
        self,
        match_id: str,
        *,
        final_score: str,
        final_status: str,
    ) -> int:
        """Store final facts without changing a user's manual result label."""
        with self._conn() as conn:
            cursor = conn.execute(
                """
                UPDATE alerts
                SET final_score = CASE WHEN ? != '' THEN ? ELSE final_score END,
                    final_status = CASE WHEN ? != '' THEN ? ELSE final_status END
                WHERE match_id = ?
                  AND deleted_at IS NOT NULL
                  AND deleted_at != ''
                """,
                (
                    final_score, final_score,
                    final_status, final_status,
                    str(match_id or "").strip(),
                ),
            )
        return cursor.rowcount

    def mark_deleted_match_in_progress(self, match_id: str) -> int:
        with self._conn() as conn:
            cursor = conn.execute(
                """
                UPDATE alerts
                SET final_status = 'Devam Ediyor',
                    final_score = ''
                WHERE match_id = ?
                  AND deleted_at IS NOT NULL
                  AND deleted_at != ''
                  AND TRIM(COALESCE(result, '')) = ''
                """,
                (match_id,),
            )
        return cursor.rowcount

    # ---------- upcoming matches ----------

    def _upcoming_date_window(self) -> tuple[str, str]:
        timezone_id = os.getenv("AISCORE_TIMEZONE", "Europe/Istanbul")
        try:
            today = datetime.now(ZoneInfo(timezone_id)).date()
        except ZoneInfoNotFoundError:
            today = date.today()
        try:
            days_ahead = max(0, int(os.getenv("UPCOMING_DAYS_AHEAD", "0") or 0))
        except ValueError:
            days_ahead = 0
        return today.isoformat(), (today + timedelta(days=days_ahead)).isoformat()

    def save_upcoming_matches_and_signals(
        self,
        matches: list[dict],
        *,
        seen_match_ids: list[str] | set[str] | None = None,
        reconcile: bool = False,
    ) -> dict:
        """Upsert parsed upcoming rows without treating parse misses as deletions.

        ``seen_match_ids`` represents the authoritative listing generation,
        while ``matches`` contains only successfully parsed details. Reconcile
        is intentionally opt-in so a partial/failed scrape cannot erase valid
        rows or the user's match actions.
        """
        saved_matches = 0
        saved_signals = 0
        enriched: list[dict] = []
        saved_match_ids: list[str] = []

        with self._conn() as conn:
            for raw in matches or []:
                if not isinstance(raw, dict):
                    continue

                row = dict(raw)
                match_id = str(row.get("match_id") or "").strip()
                match_name = str(row.get("match_name") or "").strip()
                if not match_id or not match_name:
                    enriched.append(row)
                    continue

                opening = self._to_float(row.get("opening_total"))
                prematch = self._to_float(row.get("prematch_total"))
                expected = self._to_float(row.get("expected_total"))
                direction = ""
                diff = None
                if opening is not None and expected is not None:
                    if expected > opening:
                        direction = "ÜST"
                        diff = round(expected - opening, 2)
                    elif expected < opening:
                        direction = "ALT"
                        diff = round(opening - expected, 2)

                row["signal_direction"] = direction
                row["signal_diff"] = diff

                conn.execute(
                    """
                    INSERT INTO upcoming_matches (
                        match_id, match_name, home_team, away_team, tournament, kickoff,
                        opening_total, prematch_total, expected_total, direction, diff,
                        url, payload_json, fetched_at
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                    ON CONFLICT(match_id) DO UPDATE SET
                        match_name = excluded.match_name,
                        home_team = excluded.home_team,
                        away_team = excluded.away_team,
                        tournament = excluded.tournament,
                        kickoff = excluded.kickoff,
                        opening_total = excluded.opening_total,
                        prematch_total = excluded.prematch_total,
                        expected_total = excluded.expected_total,
                        direction = excluded.direction,
                        diff = excluded.diff,
                        url = excluded.url,
                        payload_json = excluded.payload_json,
                        fetched_at = excluded.fetched_at
                    """,
                    (
                        match_id,
                        match_name,
                        str(row.get("home_team") or ""),
                        str(row.get("away_team") or ""),
                        str(row.get("tournament") or ""),
                        str(row.get("kickoff") or ""),
                        opening,
                        prematch,
                        expected,
                        direction,
                        diff,
                        str(row.get("url") or ""),
                        json.dumps(row, ensure_ascii=False),
                    ),
                )
                saved_matches += 1
                saved_match_ids.append(match_id)

                if direction and diff is not None:
                    saved_signals += 1

                enriched.append(row)

            start_date, end_date = self._upcoming_date_window()
            expired_rows = conn.execute(
                """
                SELECT match_id
                FROM upcoming_matches
                WHERE kickoff IS NOT NULL
                  AND kickoff != ''
                  AND length(kickoff) >= 10
                  AND (substr(kickoff, 1, 10) < ? OR substr(kickoff, 1, 10) > ?)
                """,
                (start_date, end_date),
            ).fetchall()
            expired_ids = [
                str(row["match_id"])
                for row in expired_rows
                if str(row["match_id"] or "").strip()
            ]
            if expired_ids:
                placeholders = ", ".join("?" for _ in expired_ids)
                conn.execute(
                    f"DELETE FROM upcoming_matches WHERE match_id IN ({placeholders})",
                    tuple(expired_ids),
                )
                conn.execute(
                    f"DELETE FROM upcoming_match_actions WHERE match_id IN ({placeholders})",
                    tuple(expired_ids),
                )

            missing_ids: list[str] = []
            authoritative_ids = {
                str(match_id).strip()
                for match_id in (seen_match_ids or [])
                if str(match_id or "").strip()
            }
            if reconcile and authoritative_ids:
                placeholders = ", ".join("?" for _ in authoritative_ids)
                rows = conn.execute(
                    f"""
                    SELECT match_id
                    FROM upcoming_matches
                    WHERE (kickoff IS NULL OR kickoff = '' OR length(kickoff) < 10
                           OR substr(kickoff, 1, 10) BETWEEN ? AND ?)
                      AND match_id NOT IN ({placeholders})
                    """,
                    (start_date, end_date, *sorted(authoritative_ids)),
                ).fetchall()
                missing_ids = [
                    str(row["match_id"])
                    for row in rows
                    if str(row["match_id"] or "").strip()
                ]
                if missing_ids:
                    missing_placeholders = ", ".join("?" for _ in missing_ids)
                    conn.execute(
                        f"DELETE FROM upcoming_matches WHERE match_id IN ({missing_placeholders})",
                        tuple(missing_ids),
                    )
                    # Keep upcoming_match_actions: a transient listing miss or a
                    # later reappearance must not erase the user's decision.

            legacy_ids = list({*saved_match_ids, *expired_ids, *missing_ids})
            if legacy_ids:
                placeholders = ", ".join("?" for _ in legacy_ids)
                conn.execute(
                    f"""
                    DELETE FROM alerts
                    WHERE match_id IN ({placeholders})
                      AND alert_period = -1
                      AND alert_moment = 'Gelecek Maç'
                    """,
                    tuple(legacy_ids),
                )

        return {
            "matches": enriched,
            "saved_matches": saved_matches,
            "saved_signals": saved_signals,
            "reconciled": bool(reconcile and authoritative_ids),
            "listing_seen": len(authoritative_ids),
            "removed_missing": len(missing_ids),
            "removed_expired": len(expired_ids),
        }

    def list_upcoming_matches(self, limit: int = 200) -> list[dict]:
        start_date, end_date = self._upcoming_date_window()
        with self._conn() as conn:
            rows = conn.execute(
                """
                SELECT match_id, match_name, home_team, away_team, tournament, kickoff,
                       opening_total, prematch_total, expected_total, direction, diff,
                       url, payload_json, fetched_at
                FROM upcoming_matches
                WHERE kickoff IS NULL
                   OR kickoff = ''
                   OR length(kickoff) < 10
                   OR substr(kickoff, 1, 10) BETWEEN ? AND ?
                ORDER BY
                    kickoff ASC,
                    CASE WHEN diff IS NULL THEN 1 ELSE 0 END,
                    diff DESC,
                    fetched_at DESC
                LIMIT ?
                """,
                (start_date, end_date, max(1, int(limit or 200))),
            ).fetchall()

        items: list[dict] = []
        for row in rows:
            item = dict(row)
            try:
                payload = json.loads(item.get("payload_json") or "{}")
            except Exception:
                payload = {}
            if isinstance(payload, dict):
                item.update({k: v for k, v in payload.items() if k not in {"match_id"}})
            item["match_id"] = row["match_id"]
            item["signal_direction"] = item.get("signal_direction") or item.get("direction") or ""
            item["signal_diff"] = item.get("signal_diff")
            if item["signal_diff"] is None:
                item["signal_diff"] = item.get("diff")
            item.pop("payload_json", None)
            items.append(item)

        action_by_match = self.upcoming_match_action_statuses([item["match_id"] for item in items])
        for item in items:
            match_id = str(item.get("match_id") or "")
            action = action_by_match.get(match_id) or {}
            item["alert_id"] = None
            for key in ("bet_placed", "followed", "ignored"):
                item[key] = int(action.get(key) or 0)
        return items

    def upcoming_match_action_statuses(self, match_ids: list[str]) -> dict[str, dict]:
        keys = [str(mid) for mid in match_ids if str(mid).strip()]
        if not keys:
            return {}
        placeholders = ", ".join("?" for _ in keys)
        with self._conn() as conn:
            rows = conn.execute(
                f"""
                SELECT match_id, bet_placed, ignored, followed, deleted_at
                FROM upcoming_match_actions
                WHERE match_id IN ({placeholders})
                """,
                tuple(keys),
            ).fetchall()
        return {str(row["match_id"]): dict(row) for row in rows}

    def get_upcoming_match_action_status(self, match_id: str) -> dict:
        key = str(match_id or "").strip()
        if not key:
            return {"bet_placed": 0, "ignored": 0, "followed": 0}
        action = self.upcoming_match_action_statuses([key]).get(key) or {}
        return {
            "match_id": key,
            "bet_placed": int(action.get("bet_placed") or 0),
            "ignored": int(action.get("ignored") or 0),
            "followed": int(action.get("followed") or 0),
        }

    def is_upcoming_followed(self, match_id: str) -> bool:
        key = str(match_id or "").strip()
        if not key:
            return False
        with self._conn() as conn:
            row = conn.execute(
                "SELECT followed FROM upcoming_match_actions WHERE match_id = ?",
                (key,),
            ).fetchone()
        return bool(row and int(row["followed"] or 0))

    def upcoming_followed_match_ids(self, match_ids: list[str] | None = None) -> set[str]:
        with self._conn() as conn:
            if match_ids:
                keys = [str(mid) for mid in match_ids if str(mid).strip()]
                if not keys:
                    return set()
                placeholders = ", ".join("?" for _ in keys)
                rows = conn.execute(
                    f"SELECT match_id FROM upcoming_match_actions "
                    f"WHERE followed = 1 AND match_id IN ({placeholders})",
                    tuple(keys),
                ).fetchall()
            else:
                rows = conn.execute(
                    "SELECT match_id FROM upcoming_match_actions WHERE followed = 1"
                ).fetchall()
        return {str(row["match_id"]) for row in rows}

    def set_upcoming_match_statuses(
        self,
        match_id: str,
        *,
        bet_placed: bool | None = None,
        ignored: bool | None = None,
        followed: bool | None = None,
    ) -> int:
        key = str(match_id or "").strip()
        if not key:
            return 0

        cols = ["match_id"]
        vals = [key]
        updates = []
        if bet_placed is not None:
            cols.append("bet_placed")
            vals.append(1 if bet_placed else 0)
            updates.append("bet_placed = excluded.bet_placed")
        if ignored is not None:
            cols.append("ignored")
            vals.append(1 if ignored else 0)
            updates.append("ignored = excluded.ignored")
        if followed is not None:
            cols.append("followed")
            vals.append(1 if followed else 0)
            updates.append("followed = excluded.followed")
        if not updates:
            return 0

        cols.append("updated_at")
        vals.append(datetime.now().isoformat(sep=" ", timespec="seconds"))
        updates.append("updated_at = excluded.updated_at")
        placeholders = ", ".join("?" for _ in vals)
        with self._conn() as conn:
            cursor = conn.execute(
                f"""
                INSERT INTO upcoming_match_actions ({', '.join(cols)})
                VALUES ({placeholders})
                ON CONFLICT(match_id) DO UPDATE SET {', '.join(updates)}
                """,
                tuple(vals),
            )
        return int(cursor.rowcount or 0)

    def delete_upcoming_match_data(self, match_id: str) -> int:
        key = str(match_id or "").strip()
        if not key:
            return 0
        with self._conn() as conn:
            upcoming_count = conn.execute("DELETE FROM upcoming_matches WHERE match_id = ?", (key,)).rowcount
            conn.execute("DELETE FROM upcoming_match_actions WHERE match_id = ?", (key,))
            conn.execute(
                """
                DELETE FROM alerts
                WHERE match_id = ?
                  AND alert_period = -1
                  AND alert_moment = 'Gelecek Maç'
                """,
                (key,),
            )
        return int(upcoming_count or 0)

    def clear_upcoming_matches(self) -> dict:
        with self._conn() as conn:
            rows = conn.execute("SELECT match_id FROM upcoming_matches").fetchall()
            match_ids = [str(row["match_id"]) for row in rows if str(row["match_id"] or "").strip()]
            upcoming_count = conn.execute("DELETE FROM upcoming_matches").rowcount
            action_count = conn.execute("DELETE FROM upcoming_match_actions").rowcount

            alert_count = 0
            if match_ids:
                placeholders = ", ".join("?" for _ in match_ids)
                alert_count = conn.execute(
                    f"""
                    DELETE FROM alerts
                    WHERE match_id IN ({placeholders})
                      AND alert_period = -1
                      AND alert_moment = 'Gelecek Maç'
                    """,
                    tuple(match_ids),
                ).rowcount

        return {
            "deleted_upcoming": int(upcoming_count or 0),
            "deleted_actions": int(action_count or 0),
            "deleted_alerts": int(alert_count or 0),
        }

    @staticmethod
    def _to_float(value) -> float | None:
        if value is None or value == "":
            return None
        try:
            return float(value)
        except (TypeError, ValueError):
            return None

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

    # ---------- saved match lists ----------

    def save_match_list(self, name: str, matches: list) -> int:
        clean_name = str(name or "").strip()[:200] or "Liste"
        with self._conn() as conn:
            cursor = conn.execute(
                """
                INSERT INTO saved_match_lists (name, match_count, matches_json)
                VALUES (?, ?, ?)
                """,
                (clean_name, len(matches), json.dumps(matches, ensure_ascii=False)),
            )
        return cursor.lastrowid

    def list_saved_match_lists(self, limit: int = 100) -> list:
        with self._conn() as conn:
            rows = conn.execute(
                """
                SELECT id, name, match_count, created_at
                FROM saved_match_lists
                ORDER BY created_at DESC, id DESC
                LIMIT ?
                """,
                (max(1, limit),),
            ).fetchall()
        return [dict(r) for r in rows]

    def get_saved_match_list(self, list_id: int) -> dict | None:
        with self._conn() as conn:
            row = conn.execute(
                """
                SELECT id, name, match_count, matches_json, created_at
                FROM saved_match_lists
                WHERE id = ?
                """,
                (list_id,),
            ).fetchone()
        if not row:
            return None
        item = dict(row)
        try:
            item["matches"] = json.loads(item.pop("matches_json") or "[]")
        except Exception:
            item["matches"] = []
        return item

    def delete_saved_match_list(self, list_id: int) -> bool:
        with self._conn() as conn:
            cursor = conn.execute(
                "DELETE FROM saved_match_lists WHERE id = ?",
                (list_id,),
            )
        return cursor.rowcount > 0

    # ---------- deleted match result checks ----------

    def get_deleted_matches_for_result_check(self, limit: int | None = 200) -> list:
        with self._conn() as conn:
            sql = """
                SELECT a.match_id, a.match_name, a.tournament, a.url,
                       CASE WHEN TRIM(COALESCE(a.final_status, '')) != ''
                            THEN a.final_status ELSE a.status END AS status,
                       CASE WHEN TRIM(COALESCE(a.final_score, '')) != ''
                            THEN a.final_score ELSE a.score END AS score,
                       a.alerted_at, a.deleted_at
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
                    WHERE pending.match_id = a.match_id
                      AND pending.deleted_at IS NOT NULL
                      AND pending.deleted_at != ''
                      AND TRIM(COALESCE(pending.result, '')) = ''
                )
                   OR EXISTS (
                    SELECT 1
                    FROM signal_trials pending_trial
                    WHERE pending_trial.match_id = a.match_id
                      AND TRIM(COALESCE(pending_trial.result, '')) = ''
                )
                ORDER BY a.deleted_at DESC, a.alerted_at DESC, a.id DESC
                """
            if limit is None:
                rows = conn.execute(sql).fetchall()
            else:
                rows = conn.execute(f"{sql} LIMIT ?", (limit,)).fetchall()
        return [dict(r) for r in rows]

    def get_deleted_match_for_result_check_by_alert_id(self, alert_id: int) -> dict | None:
        with self._conn() as conn:
            row = conn.execute(
                """
                SELECT match_id
                FROM alerts
                WHERE id = ?
                  AND deleted_at IS NOT NULL
                  AND deleted_at != ''
                """,
                (alert_id,),
            ).fetchone()
            if not row:
                return None

            match = conn.execute(
                """
                SELECT match_id, match_name, tournament, url,
                       CASE WHEN TRIM(COALESCE(final_status, '')) != ''
                            THEN final_status ELSE status END AS status,
                       CASE WHEN TRIM(COALESCE(final_score, '')) != ''
                            THEN final_score ELSE score END AS score,
                       alerted_at, deleted_at
                FROM alerts
                WHERE match_id = ?
                  AND url != ''
                  AND deleted_at IS NOT NULL
                  AND deleted_at != ''
                ORDER BY id DESC
                LIMIT 1
                """,
                (row["match_id"],),
            ).fetchone()
        return dict(match) if match else None

    def get_deleted_alerts_for_result_check(self, match_id: str) -> list:
        with self._conn() as conn:
            rows = conn.execute(
                """
                SELECT *
                FROM alerts
                WHERE match_id = ?
                  AND deleted_at IS NOT NULL
                  AND deleted_at != ''
                  AND TRIM(COALESCE(result, '')) = ''
                ORDER BY alerted_at ASC, id ASC
                """,
                (match_id,),
            ).fetchall()
        return [dict(r) for r in rows]

    def get_deleted_alerts_for_match(self, match_id: str) -> list:
        with self._conn() as conn:
            rows = conn.execute(
                """
                SELECT *
                FROM alerts
                WHERE match_id = ?
                  AND deleted_at IS NOT NULL
                  AND deleted_at != ''
                ORDER BY alerted_at ASC, id ASC
                """,
                (match_id,),
            ).fetchall()
        return [dict(r) for r in rows]
