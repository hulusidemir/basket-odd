"""One observable active-match scan shared by the dashboard and scheduler."""

import asyncio
import copy
import logging
import threading
import uuid
from datetime import datetime, timezone

from finished_match_service import FinishedCheckBusy, run_active_match_finished_scan


logger = logging.getLogger(__name__)


def _now():
    return datetime.now(timezone.utc).isoformat()


class FinishedScanJobs:
    def __init__(self):
        self._lock = threading.Lock()
        self._thread = None
        self._state = {"state": "idle", "job_id": None}

    def status(self):
        with self._lock:
            return copy.deepcopy(self._state)

    def start(self, factory, source="manual"):
        with self._lock:
            if self._state["state"] == "running":
                return copy.deepcopy(self._state)
            job_id = uuid.uuid4().hex
            self._state = {
                "job_id": job_id, "state": "running", "source": source,
                "started_at": _now(), "finished_at": None,
                "progress": {"phase": "starting", "tracked_count": 0,
                             "processed_count": 0, "moved_count": 0},
            }
            self._thread = threading.Thread(
                target=self._run, args=(job_id, factory),
                name="active-finished-scan", daemon=True,
            )
            try:
                self._thread.start()
            except Exception:
                self._state.update(state="failed", finished_at=_now(),
                                   error="Tarama başlatılamadı. Tekrar deneyin.")
                raise
            return copy.deepcopy(self._state)

    def _progress(self, job_id, progress):
        with self._lock:
            if self._state["job_id"] == job_id and self._state["state"] == "running":
                self._state["progress"].update(copy.deepcopy(progress))

    def _run(self, job_id, factory):
        try:
            result = asyncio.run(factory(lambda progress: self._progress(job_id, progress)))
            failed = bool(result.get("busy") or (
                result.get("check_failed_count") and not result.get("checked_count")
            ) or (result.get("archive_failed_count") and not result.get("moved_count")))
            terminal = {"state": "failed" if failed else "completed", "result": result}
            if failed:
                terminal["error"] = result.get("message") or "Tarama tamamlanamadı."
            logger.info("Final scan completed: checked=%s moved=%s unavailable=%s",
                        result.get("checked_count", 0), result.get("moved_count", 0),
                        result.get("check_failed_count", 0))
        except FinishedCheckBusy as exc:
            terminal = {"state": "failed", "error": str(exc)}
        except TimeoutError:
            terminal = {"state": "failed", "error": "Final kontrolü zaman aşımına uğradı. Tekrar deneyin."}
        except Exception as exc:
            logger.error("Final scan failed (%s)", type(exc).__name__)
            terminal = {"state": "failed", "error": "Biten maçlar kontrol edilemedi. Tekrar deneyin."}
        with self._lock:
            if self._state["job_id"] == job_id:
                self._state.update(terminal, finished_at=_now())


active_scan_jobs = FinishedScanJobs()


def start_active_finished_scan(db, config, before_delete, source="manual"):
    return active_scan_jobs.start(
        lambda progress: run_active_match_finished_scan(
            db, config, before_delete=before_delete, on_progress=progress,
        ),
        source=source,
    )
