"""
signal_reliability.py — Shared reliability labels for alerts.
"""

from __future__ import annotations


RELIABLE_LABEL = "GÜVENİLİR"
WATCH_LABEL = "İZLE"
WEAK_LABEL = "ZAYIF"


def alert_reliability(
    direction: str = "",
    quality_grade: str = "",
    status: str = "",
    diff: float = 0,
    threshold: float = 10.0,
) -> dict[str, str | bool]:
    normalized_grade = str(quality_grade or "").strip().upper()

    if normalized_grade == "A":
        return {"label": RELIABLE_LABEL, "code": "reliable", "is_reliable": True}
    if normalized_grade == "B":
        return {"label": WATCH_LABEL, "code": "watch", "is_reliable": False}
    return {"label": WEAK_LABEL, "code": "weak", "is_reliable": False}
