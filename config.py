import os
from dotenv import load_dotenv

load_dotenv()


class Config:
    TELEGRAM_TOKEN: str = os.getenv("TELEGRAM_TOKEN", "")
    TELEGRAM_CHAT_ID: str = os.getenv("TELEGRAM_CHAT_ID", "")
    THRESHOLD: float = float(os.getenv("THRESHOLD", "10"))
    POLL_INTERVAL_MIN: int = int(os.getenv("POLL_INTERVAL_MIN", "25"))
    POLL_INTERVAL_MAX: int = int(os.getenv("POLL_INTERVAL_MAX", "40"))
    ALERT_COOLDOWN_MINUTES: int = int(os.getenv("ALERT_COOLDOWN_MINUTES", "10"))
    DB_PATH: str = os.getenv("DB_PATH", "basketball.db")
    LOG_LEVEL: str = os.getenv("LOG_LEVEL", "INFO")
    AISCORE_URL: str = os.getenv("AISCORE_URL", "https://www.aiscore.com/basketball")
    MAX_MATCHES_PER_CYCLE: int = int(os.getenv("MAX_MATCHES_PER_CYCLE", "80"))
    PAGE_TIMEOUT_MS: int = int(os.getenv("PAGE_TIMEOUT_MS", "30000"))
    FINISHED_MATCH_POLL_SECONDS: int = int(os.getenv("FINISHED_MATCH_POLL_SECONDS", "120"))
    FINISHED_MATCH_BATCH_SIZE: int = int(os.getenv("FINISHED_MATCH_BATCH_SIZE", "40"))
    BLACKLIST: list = [b.strip().lower() for b in os.getenv("BLACKLIST", "").split(",") if b.strip()]
    MINIMUM_CONFIDENCE_FOR_SIGNAL: float = float(os.getenv("MINIMUM_CONFIDENCE_FOR_SIGNAL", "62"))
    MINIMUM_SUPPORTING_SOURCES: int = int(os.getenv("MINIMUM_SUPPORTING_SOURCES", "2"))
    MINIMUM_PROJECTION_EDGE: float = float(os.getenv("MINIMUM_PROJECTION_EDGE", "4"))
    CONTINUATION_CONFIDENCE_THRESHOLD: float = float(os.getenv("CONTINUATION_CONFIDENCE_THRESHOLD", "58"))
    CONTRARIAN_CONFIDENCE_THRESHOLD: float = float(os.getenv("CONTRARIAN_CONFIDENCE_THRESHOLD", "68"))
    BLOWOUT_MARGIN_THRESHOLD: int = int(os.getenv("BLOWOUT_MARGIN_THRESHOLD", "15"))
    LATE_GAME_MINUTES_THRESHOLD: float = float(os.getenv("LATE_GAME_MINUTES_THRESHOLD", "6"))
    FOUL_GAME_SCORE_DIFF_THRESHOLD: int = int(os.getenv("FOUL_GAME_SCORE_DIFF_THRESHOLD", "8"))
    MIN_RECENT_PACE_SAMPLE_MINUTES: float = float(os.getenv("MIN_RECENT_PACE_SAMPLE_MINUTES", "1.0"))

    def validate(self):
        if not self.TELEGRAM_TOKEN or self.TELEGRAM_TOKEN == "123456789:ABCdefGhIJKlmNOpqRSTuvWXyz":
            raise ValueError("TELEGRAM_TOKEN ayarlanmamış! .env dosyasını düzenleyin.")
        if not self.TELEGRAM_CHAT_ID or self.TELEGRAM_CHAT_ID == "987654321":
            raise ValueError("TELEGRAM_CHAT_ID ayarlanmamış! .env dosyasını düzenleyin.")
        if self.POLL_INTERVAL_MIN <= 0 or self.POLL_INTERVAL_MAX <= 0:
            raise ValueError("POLL_INTERVAL_MIN/MAX 0'dan büyük olmalı.")
        if self.POLL_INTERVAL_MIN > self.POLL_INTERVAL_MAX:
            raise ValueError("POLL_INTERVAL_MIN, POLL_INTERVAL_MAX'ten büyük olamaz.")
