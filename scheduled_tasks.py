"""scheduled_tasks.py — Saatlik otomatik kontroller.

Flask sunucusu çalışırken arka planda bir thread döner:
- Her saatin :00'ında "Bitenleri Kontrol Et" (dashboard butonuyla aynı)
- Her saatin :10'unda "Bitmişleri Kontrol Et" (silinen maçlar butonuyla aynı)
"""

from __future__ import annotations

import asyncio
import logging
import threading
import time
from datetime import datetime, timedelta

from config import Config
from db import Database
from finished_match_service import (
    run_active_match_finished_scan,
    run_deleted_match_result_cycle,
)

log = logging.getLogger("scheduled_tasks")

_started = False
_start_lock = threading.Lock()
_before_active_delete = None


def _next_at_minute(now: datetime, minute: int, second: int = 5) -> datetime:
    target = now.replace(minute=minute, second=second, microsecond=0)
    if target <= now:
        target += timedelta(hours=1)
    return target


def _run_safe(name: str, coro_factory) -> None:
    try:
        log.info("Scheduled task başladı: %s", name)
        result = asyncio.run(coro_factory())
        log.info("Scheduled task bitti: %s | %s", name, result)
    except Exception as exc:
        log.exception("Scheduled task başarısız (%s): %s", name, exc)


def _task_loop(task: str, minute: int) -> None:
    config = Config()
    db = Database(config.DB_PATH)
    db.init()

    while True:
        target = _next_at_minute(datetime.now(), minute)
        sleep_s = max(1.0, (target - datetime.now()).total_seconds())
        log.info(
            "Sıradaki otomatik kontrol: %s | %.0f sn sonra (%s)",
            task, sleep_s, target.strftime("%H:%M:%S"),
        )
        time.sleep(sleep_s)

        if task == "active":
            _run_safe(
                "active-match-finished-scan (saat başı)",
                lambda: run_active_match_finished_scan(
                    db,
                    config,
                    before_delete=_before_active_delete,
                ),
            )
        else:
            _run_safe(
                "deleted-match-result-cycle (saat :10)",
                lambda: run_deleted_match_result_cycle(db, config),
            )


def start(before_active_delete=None) -> None:
    global _before_active_delete, _started
    with _start_lock:
        if _started:
            return
        _started = True
        _before_active_delete = before_active_delete
    active_thread = threading.Thread(
        target=_task_loop,
        args=("active", 0),
        name="scheduled-active-finished",
        daemon=True,
    )
    result_thread = threading.Thread(
        target=_task_loop,
        args=("deleted", 10),
        name="scheduled-deleted-results",
        daemon=True,
    )
    active_thread.start()
    result_thread.start()
    log.info("Scheduled task threads başlatıldı (her saat :00 ve :10).")
