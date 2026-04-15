"""
signal_reliability.py — Shared reliability labels for alerts.
"""

from __future__ import annotations

import re

RELIABLE_LABEL = "GÜVENİLİR"
WATCH_LABEL = "GÜVENİLİR DEĞİL TAKİP ET"


def alert_reliability(direction: str, quality_grade: str, status: str, diff: float, counter_level: str) -> dict[str, str | bool]:
    stage = _stage_from_status(status)
    normalized_direction = str(direction or "").strip().upper()
    normalized_grade = str(quality_grade or "").strip().upper()
    normalized_counter = str(counter_level or "").strip().upper()
    diff_value = abs(float(diff or 0))

    is_reliable = (
        normalized_direction == "ALT"
        and normalized_grade in {"B", "A", "A+", "A++"}
        and stage in {"Q2", "Q3", "LIVE"}
        and diff_value >= 15
        and normalized_counter != "YÜKSEK"
    )

    return {
        "label": RELIABLE_LABEL if is_reliable else WATCH_LABEL,
        "code": "reliable" if is_reliable else "watch",
        "is_reliable": is_reliable,
    }


def _stage_from_status(status: str) -> str:
    status_clean = str(status or "").strip().upper()
    if status_clean.startswith("Q1"):
        return "Q1"
    if status_clean.startswith("Q2") or status_clean == "HT":
        return "Q2"
    if status_clean.startswith("Q3"):
        return "Q3"
    if status_clean.startswith("Q4"):
        return "Q4"
    if status_clean.startswith("LIVE") or re.search(r"^[1-4]\s*[-:\s]+\d{1,2}:\d{2}$", status_clean):
        return "LIVE"
    return "OTHER"
