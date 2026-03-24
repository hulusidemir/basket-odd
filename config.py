import os
from dotenv import load_dotenv

load_dotenv()


class Config:
    TELEGRAM_TOKEN: str = os.getenv("TELEGRAM_TOKEN", "")
    TELEGRAM_CHAT_ID: str = os.getenv("TELEGRAM_CHAT_ID", "")
    THRESHOLD: float = float(os.getenv("THRESHOLD", "10"))
    POLL_INTERVAL_MIN: int = int(os.getenv("POLL_INTERVAL_MIN", "25"))
    POLL_INTERVAL_MAX: int = int(os.getenv("POLL_INTERVAL_MAX", "40"))
    ALERT_COOLDOWN_MINUTES: int = int(os.getenv("ALERT_COOLDOWN_MINUTES", "15"))
    DB_PATH: str = os.getenv("DB_PATH", "basketball.db")
    LOG_LEVEL: str = os.getenv("LOG_LEVEL", "INFO")
    BROWSER_MODE: str = os.getenv("BROWSER_MODE", "opera")  # "opera" or "headless"
    OPERA_CDP_URL: str = os.getenv("OPERA_CDP_URL", "http://127.0.0.1:9222")
    OPERA_CDP_PORT: int = int(os.getenv("OPERA_CDP_PORT", "9222"))
    OPERA_BINARY: str = os.getenv("OPERA_BINARY", "")
    AISCORE_URL: str = os.getenv("AISCORE_URL", "https://www.aiscore.com/basketball")
    MAX_MATCHES_PER_CYCLE: int = int(os.getenv("MAX_MATCHES_PER_CYCLE", "80"))
    PAGE_TIMEOUT_MS: int = int(os.getenv("PAGE_TIMEOUT_MS", "30000"))
    GEMINI_API_KEY: str = os.getenv("GEMINI_API_KEY", "")
    GEMINI_MODEL: str = os.getenv("GEMINI_MODEL", "gemini-2.5-flash")

    def validate(self):
        if not self.TELEGRAM_TOKEN or self.TELEGRAM_TOKEN == "123456789:ABCdefGhIJKlmNOpqRSTuvWXyz":
            raise ValueError("TELEGRAM_TOKEN ayarlanmamış! .env dosyasını düzenleyin.")
        if not self.TELEGRAM_CHAT_ID or self.TELEGRAM_CHAT_ID == "987654321":
            raise ValueError("TELEGRAM_CHAT_ID ayarlanmamış! .env dosyasını düzenleyin.")
        if self.POLL_INTERVAL_MIN <= 0 or self.POLL_INTERVAL_MAX <= 0:
            raise ValueError("POLL_INTERVAL_MIN/MAX 0'dan büyük olmalı.")
        if self.POLL_INTERVAL_MIN > self.POLL_INTERVAL_MAX:
            raise ValueError("POLL_INTERVAL_MIN, POLL_INTERVAL_MAX'ten büyük olamaz.")
