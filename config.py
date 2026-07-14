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


class Config:
    TELEGRAM_TOKEN: str = os.getenv("TELEGRAM_TOKEN", "")
    TELEGRAM_CHAT_ID: str = os.getenv("TELEGRAM_CHAT_ID", "")
    THRESHOLD: float = _float_env("THRESHOLD", 10.0)
    POLL_INTERVAL_MIN: int = _int_env("POLL_INTERVAL_MIN", 25)
    POLL_INTERVAL_MAX: int = _int_env("POLL_INTERVAL_MAX", 40)
    MAX_SIGNALS_PER_MATCH: int = _int_env("MAX_SIGNALS_PER_MATCH", 3)
    SAME_DIRECTION_MIN_LIVE_DELTA: float = _float_env("SAME_DIRECTION_MIN_LIVE_DELTA", 10.0)
    DB_PATH: str = os.getenv("DB_PATH", "basketball.db")
    LOG_LEVEL: str = os.getenv("LOG_LEVEL", "INFO")
    AISCORE_URL: str = os.getenv("AISCORE_URL", "https://www.aiscore.com/basketball")
    AISCORE_TIMEZONE: str = os.getenv("AISCORE_TIMEZONE", "Europe/Istanbul")
    UPCOMING_DAYS_AHEAD: int = _int_env("UPCOMING_DAYS_AHEAD", 0)
    MAX_MATCHES_PER_CYCLE: int = _int_env("MAX_MATCHES_PER_CYCLE", 80)
    PAGE_TIMEOUT_MS: int = _int_env("PAGE_TIMEOUT_MS", 30000)
    AISCORE_CONCURRENCY: int = _int_env("AISCORE_CONCURRENCY", 2)
    UPCOMING_CONCURRENCY: int = _int_env("UPCOMING_CONCURRENCY", 2)
    BLACKLIST: list = [b.strip().lower() for b in os.getenv("BLACKLIST", "").split(",") if b.strip()]

    def validate(self):
        if _CONFIG_ERRORS:
            raise ValueError("; ".join(_CONFIG_ERRORS))
        if not self.TELEGRAM_TOKEN or self.TELEGRAM_TOKEN == "123456789:ABCdefGhIJKlmNOpqRSTuvWXyz":
            raise ValueError("TELEGRAM_TOKEN ayarlanmamış! .env dosyasını düzenleyin.")
        if not self.TELEGRAM_CHAT_ID or self.TELEGRAM_CHAT_ID == "987654321":
            raise ValueError("TELEGRAM_CHAT_ID ayarlanmamış! .env dosyasını düzenleyin.")
        if self.POLL_INTERVAL_MIN <= 0 or self.POLL_INTERVAL_MAX <= 0:
            raise ValueError("POLL_INTERVAL_MIN/MAX 0'dan büyük olmalı.")
        if self.POLL_INTERVAL_MIN > self.POLL_INTERVAL_MAX:
            raise ValueError("POLL_INTERVAL_MIN, POLL_INTERVAL_MAX'ten büyük olamaz.")
        if self.SAME_DIRECTION_MIN_LIVE_DELTA < 0:
            raise ValueError("SAME_DIRECTION_MIN_LIVE_DELTA negatif olamaz.")
        if self.THRESHOLD <= 0 or self.THRESHOLD > 100:
            raise ValueError("THRESHOLD 0 ile 100 arasında olmalı.")
        if self.MAX_SIGNALS_PER_MATCH <= 0:
            raise ValueError("MAX_SIGNALS_PER_MATCH 0'dan büyük olmalı.")
        if self.MAX_MATCHES_PER_CYCLE <= 0:
            raise ValueError("MAX_MATCHES_PER_CYCLE 0'dan büyük olmalı.")
        if self.PAGE_TIMEOUT_MS < 5000 or self.PAGE_TIMEOUT_MS > 120000:
            raise ValueError("PAGE_TIMEOUT_MS 5000 ile 120000 arasında olmalı.")
        if self.UPCOMING_DAYS_AHEAD < 0 or self.UPCOMING_DAYS_AHEAD > 14:
            raise ValueError("UPCOMING_DAYS_AHEAD 0 ile 14 arasında olmalı.")
        if not 1 <= self.AISCORE_CONCURRENCY <= 8:
            raise ValueError("AISCORE_CONCURRENCY 1 ile 8 arasında olmalı.")
        if not 1 <= self.UPCOMING_CONCURRENCY <= 8:
            raise ValueError("UPCOMING_CONCURRENCY 1 ile 8 arasında olmalı.")
