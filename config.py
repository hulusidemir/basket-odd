import math
import os
from typing import TypeVar

from dotenv import load_dotenv

load_dotenv()


_Number = TypeVar("_Number", int, float)
_CONFIG_ERRORS: list[str] = []


def _number_env(name: str, default: _Number, cast: type[_Number]) -> _Number:
    raw = os.getenv(name)
    if raw is None or not raw.strip():
        return default
    try:
        return cast(raw.strip())
    except (TypeError, ValueError):
        _CONFIG_ERRORS.append(f"{name} geçerli bir {cast.__name__} olmalı")
        return default


def _int_env(name: str, default: int) -> int:
    return _number_env(name, default, int)


def _float_env(name: str, default: float) -> float:
    return _number_env(name, default, float)


def _bool_env(name: str, default: bool) -> bool:
    raw = os.getenv(name, "").strip().lower()
    if not raw:
        return default
    if raw in {"true", "1", "yes", "on"}:
        return True
    if raw in {"false", "0", "no", "off"}:
        return False
    _CONFIG_ERRORS.append(f"{name} true veya false olmalı")
    return default


class Config:
    PRIOR_EQUIV_MINUTES: float = _float_env("PRIOR_EQUIV_MINUTES", 10.0)
    MIN_VALID_FUTURE_PACES: int = _int_env("MIN_VALID_FUTURE_PACES", 2)
    MIN_EDGE_POINTS: float = _float_env("MIN_EDGE_POINTS", 4.0)
    MIN_EDGE_RATIO: float = _float_env("MIN_EDGE_RATIO", 0.02)
    BLOWOUT_MARGIN: int = _int_env("BLOWOUT_MARGIN", 20)
    BLOWOUT_EDGE_MULTIPLIER: float = _float_env("BLOWOUT_EDGE_MULTIPLIER", 1.5)
    EXTREME_BLOWOUT_MARGIN: int = _int_env("EXTREME_BLOWOUT_MARGIN", 30)
    EXTREME_BLOWOUT_EDGE_MULTIPLIER: float = _float_env("EXTREME_BLOWOUT_EDGE_MULTIPLIER", 2.0)
    LARGE_REPRICE_RATIO: float = _float_env("LARGE_REPRICE_RATIO", 0.15)
    LARGE_REPRICE_EDGE_MULTIPLIER: float = _float_env("LARGE_REPRICE_EDGE_MULTIPLIER", 1.25)
    MAX_FUTURE_BAND_WIDTH: float = _float_env("MAX_FUTURE_BAND_WIDTH", 2.0)
    UNDER_MAX_NEGATIVE_LINE_MOVE: float = _float_env("UNDER_MAX_NEGATIVE_LINE_MOVE", 10.0)
    OVER_MIN_PACE_MARGIN_RATIO: float = _float_env("OVER_MIN_PACE_MARGIN_RATIO", 0.15)
    OVER_Q2_MIN_PACE_MARGIN_RATIO: float = _float_env("OVER_Q2_MIN_PACE_MARGIN_RATIO", 0.20)
    OVER_MIN_FAIR_EDGE_POINTS: float = _float_env("OVER_MIN_FAIR_EDGE_POINTS", 8.0)
    REPEAT_SIGNAL_QUALITY_PENALTY: int = _int_env("REPEAT_SIGNAL_QUALITY_PENALTY", 10)
    TELEGRAM_TOKEN: str = os.getenv("TELEGRAM_TOKEN", "")
    TELEGRAM_CHAT_ID: str = os.getenv("TELEGRAM_CHAT_ID", "")
    THRESHOLD: float = _float_env("THRESHOLD", 10.0)
    THRESHOLD_MODE: str = os.getenv("THRESHOLD_MODE", "percent").strip().lower()
    THRESHOLD_PERCENT: float = _float_env("THRESHOLD_PERCENT", 7.0)
    DISABLE_Q4_SIGNALS: bool = _bool_env("DISABLE_Q4_SIGNALS", True)
    Q1_THRESHOLD_MULTIPLIER: float = _float_env("Q1_THRESHOLD_MULTIPLIER", 1.0)
    Q2_THRESHOLD_MULTIPLIER: float = _float_env("Q2_THRESHOLD_MULTIPLIER", 1.0)
    Q3_THRESHOLD_MULTIPLIER: float = _float_env("Q3_THRESHOLD_MULTIPLIER", 1.0)
    Q4_THRESHOLD_MULTIPLIER: float = _float_env("Q4_THRESHOLD_MULTIPLIER", 1.0)
    Q2_ALT_THRESHOLD_MULTIPLIER: float = _float_env("Q2_ALT_THRESHOLD_MULTIPLIER", 1.0)
    POLL_INTERVAL_MIN: int = _int_env("POLL_INTERVAL_MIN", 25)
    POLL_INTERVAL_MAX: int = _int_env("POLL_INTERVAL_MAX", 40)
    LIVE_POLL_SECONDS: float = _float_env("LIVE_POLL_SECONDS", 4.0)
    MAX_SIGNALS_PER_MATCH: int = _int_env("MAX_SIGNALS_PER_MATCH", 3)
    SAME_DIRECTION_MIN_LIVE_DELTA: float = _float_env("SAME_DIRECTION_MIN_LIVE_DELTA", 10.0)
    DB_PATH: str = os.getenv("DB_PATH", "basketball.db")
    LOG_LEVEL: str = os.getenv("LOG_LEVEL", "INFO")
    AISCORE_URL: str = os.getenv("AISCORE_URL", "https://m.aiscore.com/basketball")
    AISCORE_TIMEZONE: str = os.getenv("AISCORE_TIMEZONE", "Europe/Istanbul")
    UPCOMING_DAYS_AHEAD: int = _int_env("UPCOMING_DAYS_AHEAD", 0)
    MAX_MATCHES_PER_CYCLE: int = _int_env("MAX_MATCHES_PER_CYCLE", 80)
    PAGE_TIMEOUT_MS: int = _int_env("PAGE_TIMEOUT_MS", 30000)
    FINISHED_PAGE_TIMEOUT_MS: int = _int_env("FINISHED_PAGE_TIMEOUT_MS", 20000)
    FINISHED_RETRY_ATTEMPTS: int = _int_env("FINISHED_RETRY_ATTEMPTS", 1)
    AISCORE_CONCURRENCY: int = _int_env("AISCORE_CONCURRENCY", 4)
    LIVE_MATCH_TIMEOUT_SECONDS: float = _float_env("LIVE_MATCH_TIMEOUT_SECONDS", 90.0)
    LIVE_SCRAPE_TIMEOUT_SECONDS: float = _float_env("LIVE_SCRAPE_TIMEOUT_SECONDS", 180.0)
    MAX_LIVE_OBSERVATION_AGE_SECONDS: float = _float_env(
        "MAX_LIVE_OBSERVATION_AGE_SECONDS", 20.0
    )
    LIVE_LINE_STALE_SECONDS: float = _float_env("LIVE_LINE_STALE_SECONDS", 90.0)
    LIVE_LINE_STALE_SCORE_DELTA: int = _int_env("LIVE_LINE_STALE_SCORE_DELTA", 10)
    LIVE_LINE_STALE_GAME_MINUTES: float = _float_env(
        "LIVE_LINE_STALE_GAME_MINUTES", 2.0
    )
    UPCOMING_CONCURRENCY: int = _int_env("UPCOMING_CONCURRENCY", 2)
    BLACKLIST: list = [b.strip().lower() for b in os.getenv("BLACKLIST", "").split(",") if b.strip()]

    # Adaptive Signal Engine v4.1 Configs
    MIN_ELAPSED_MINUTES: float = _float_env("MIN_ELAPSED_MINUTES", 12.0)
    MIN_REMAINING_MINUTES: float = _float_env("MIN_REMAINING_MINUTES", 5.0)
    MIN_VALID_PACE_WINDOWS: int = _int_env("MIN_VALID_PACE_WINDOWS", 2)
    ANCHOR_TOLERANCE_MIN_PCT: float = _float_env("ANCHOR_TOLERANCE_MIN_PCT", 0.75)
    ANCHOR_TOLERANCE_MAX_PCT: float = _float_env("ANCHOR_TOLERANCE_MAX_PCT", 1.25)
    MIN_QUARTER_ELAPSED_SEC: int = _int_env("MIN_QUARTER_ELAPSED_SEC", 60)
    MIN_IMPLIED_PACE: float = _float_env("MIN_IMPLIED_PACE", 1.0)
    MAX_IMPLIED_PACE: float = _float_env("MAX_IMPLIED_PACE", 7.0)
    HEARTBEAT_SECONDS: int = _int_env("HEARTBEAT_SECONDS", 60)

    def validate(self):
        if _CONFIG_ERRORS:
            raise ValueError("; ".join(_CONFIG_ERRORS))
        if not self.TELEGRAM_TOKEN or self.TELEGRAM_TOKEN == "123456789:ABCdefGhIJKlmNOpqRSTuvWXyz":
            raise ValueError("TELEGRAM_TOKEN ayarlanmamış! .env dosyasını düzenleyin.")
        if not self.TELEGRAM_CHAT_ID or self.TELEGRAM_CHAT_ID == "987654321":
            raise ValueError("TELEGRAM_CHAT_ID ayarlanmamış! .env dosyasını düzenleyin.")
        if self.POLL_INTERVAL_MIN <= 0 or self.POLL_INTERVAL_MAX <= 0:
            raise ValueError("POLL_INTERVAL_MIN/MAX 0'dan büyük olmalı.")
        if not 1 <= self.LIVE_POLL_SECONDS <= 60:
            raise ValueError("LIVE_POLL_SECONDS 1 ile 60 arasında olmalı.")
        if self.POLL_INTERVAL_MIN > self.POLL_INTERVAL_MAX:
            raise ValueError("POLL_INTERVAL_MIN, POLL_INTERVAL_MAX'ten büyük olamaz.")
        if self.SAME_DIRECTION_MIN_LIVE_DELTA < 0:
            raise ValueError("SAME_DIRECTION_MIN_LIVE_DELTA negatif olamaz.")
        for name in ("UNDER_MAX_NEGATIVE_LINE_MOVE", "OVER_MIN_PACE_MARGIN_RATIO",
                     "OVER_Q2_MIN_PACE_MARGIN_RATIO", "OVER_MIN_FAIR_EDGE_POINTS"):
            value = getattr(self, name)
            if not math.isfinite(value) or value < 0:
                raise ValueError(f"{name} sonlu ve negatif olmayan bir sayı olmalı.")
        if not 0 <= self.REPEAT_SIGNAL_QUALITY_PENALTY <= 100:
            raise ValueError("REPEAT_SIGNAL_QUALITY_PENALTY 0 ile 100 arasında olmalı.")
        if not math.isfinite(self.THRESHOLD) or not 0 < self.THRESHOLD <= 100:
            raise ValueError("THRESHOLD 0 ile 100 arasında olmalı.")
        if self.THRESHOLD_MODE not in {"percent", "hybrid"}:
            raise ValueError("THRESHOLD_MODE percent veya hybrid olmalı.")
        if not math.isfinite(self.THRESHOLD_PERCENT) or not 0 < self.THRESHOLD_PERCENT <= 100:
            raise ValueError("THRESHOLD_PERCENT 0 ile 100 arasında olmalı.")
        for name in (
            "Q1_THRESHOLD_MULTIPLIER", "Q2_THRESHOLD_MULTIPLIER",
            "Q3_THRESHOLD_MULTIPLIER", "Q4_THRESHOLD_MULTIPLIER",
            "Q2_ALT_THRESHOLD_MULTIPLIER",
        ):
            value = getattr(self, name)
            if not math.isfinite(value) or value <= 0:
                raise ValueError(f"{name} sonlu ve 0'dan büyük olmalı.")
        if self.MAX_SIGNALS_PER_MATCH <= 0:
            raise ValueError("MAX_SIGNALS_PER_MATCH 0'dan büyük olmalı.")
        if self.MAX_MATCHES_PER_CYCLE <= 0:
            raise ValueError("MAX_MATCHES_PER_CYCLE 0'dan büyük olmalı.")
        if self.PAGE_TIMEOUT_MS < 5000 or self.PAGE_TIMEOUT_MS > 120000:
            raise ValueError("PAGE_TIMEOUT_MS 5000 ile 120000 arasında olmalı.")
        if self.FINISHED_PAGE_TIMEOUT_MS < 5000 or self.FINISHED_PAGE_TIMEOUT_MS > 60000:
            raise ValueError("FINISHED_PAGE_TIMEOUT_MS 5000 ile 60000 arasında olmalı.")
        if not 0 <= self.FINISHED_RETRY_ATTEMPTS <= 2:
            raise ValueError("FINISHED_RETRY_ATTEMPTS 0 ile 2 arasında olmalı.")
        if self.UPCOMING_DAYS_AHEAD < 0 or self.UPCOMING_DAYS_AHEAD > 14:
            raise ValueError("UPCOMING_DAYS_AHEAD 0 ile 14 arasında olmalı.")
        if not 1 <= self.AISCORE_CONCURRENCY <= 8:
            raise ValueError("AISCORE_CONCURRENCY 1 ile 8 arasında olmalı.")
        if not 10 <= self.LIVE_MATCH_TIMEOUT_SECONDS <= 300:
            raise ValueError("LIVE_MATCH_TIMEOUT_SECONDS 10 ile 300 arasında olmalı.")
        if not 30 <= self.LIVE_SCRAPE_TIMEOUT_SECONDS <= 900:
            raise ValueError("LIVE_SCRAPE_TIMEOUT_SECONDS 30 ile 900 arasında olmalı.")
        if self.LIVE_SCRAPE_TIMEOUT_SECONDS <= self.LIVE_MATCH_TIMEOUT_SECONDS:
            raise ValueError(
                "LIVE_SCRAPE_TIMEOUT_SECONDS, LIVE_MATCH_TIMEOUT_SECONDS değerinden büyük olmalı."
            )
        if not 1 <= self.MAX_LIVE_OBSERVATION_AGE_SECONDS <= 120:
            raise ValueError("MAX_LIVE_OBSERVATION_AGE_SECONDS 1 ile 120 arasında olmalı.")
        if not 10 <= self.LIVE_LINE_STALE_SECONDS <= 600:
            raise ValueError("LIVE_LINE_STALE_SECONDS 10 ile 600 arasında olmalı.")
        if not 1 <= self.LIVE_LINE_STALE_SCORE_DELTA <= 50:
            raise ValueError("LIVE_LINE_STALE_SCORE_DELTA 1 ile 50 arasında olmalı.")
        if not 0.5 <= self.LIVE_LINE_STALE_GAME_MINUTES <= 10:
            raise ValueError("LIVE_LINE_STALE_GAME_MINUTES 0.5 ile 10 arasında olmalı.")
        if not 1 <= self.UPCOMING_CONCURRENCY <= 8:
            raise ValueError("UPCOMING_CONCURRENCY 1 ile 8 arasında olmalı.")
