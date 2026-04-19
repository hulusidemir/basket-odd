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
    quality_score: float = 0,
    status: str = "",
    diff: float = 0,
    threshold: float = 10.0,
    opening: float | None = None,
    live: float | None = None,
) -> dict[str, str | bool]:
    normalized_grade = str(quality_grade or "").strip().upper()
    direction_key = str(direction or "").strip().upper().replace("UST", "ÜST")

    try:
        score = float(quality_score or 0)
    except (TypeError, ValueError):
        score = 0.0

    try:
        abs_diff = abs(float(diff or 0))
    except (TypeError, ValueError):
        abs_diff = 0.0

    line_delta = None
    if opening is not None and live is not None:
        try:
            line_delta = float(live) - float(opening)
        except (TypeError, ValueError):
            line_delta = None

    # Veriye göre en tehlikeli bölge: market çok ters yöne kaçmışken
    # sadece "A" harfine güvenip kontrarian oynamak.
    contrarian_extreme = False
    if line_delta is not None:
        contrarian_extreme = (
            (direction_key == "ALT" and line_delta >= 20)
            or (direction_key == "ÜST" and line_delta <= -20)
        )
    if contrarian_extreme:
        return {"label": WEAK_LABEL, "code": "weak", "is_reliable": False}

    status_key = str(status or "").strip().upper()
    early_game = status_key.startswith("Q1") or status_key.startswith("1Q")

    in_clean_diff_window = abs_diff >= max(float(threshold or 0), 10.0) and abs_diff < 20.0

    if (
        normalized_grade in {"A++", "A+"}
        or (
            normalized_grade == "A"
            and score >= 90
            and in_clean_diff_window
            and not early_game
        )
    ):
        return {"label": RELIABLE_LABEL, "code": "reliable", "is_reliable": True}

    if normalized_grade in {"A", "B"}:
        return {"label": WATCH_LABEL, "code": "watch", "is_reliable": False}
    return {"label": WEAK_LABEL, "code": "weak", "is_reliable": False}
