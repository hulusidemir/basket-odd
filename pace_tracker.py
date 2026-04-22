"""
pace_tracker.py — Bot döngüleri arasında maç başına çeyrek skorlarını hafızada
tutar, her çeyreğin hızını hesaplar ve anomali tespiti yapar.

Anomali: Bu çeyrekteki hız/dakika normalin ±%20 dışındaysa ortalamaya dönüş sinyali.
Kullanım: main.py ve live_matches_worker.py'de tek bir PaceTracker örneği paylaşılır.
"""
from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class _MatchState:
    # Tamamlanan çeyreklerin son kümülatif skoru  {1: 52, 2: 98, 3: 147}
    quarter_end_totals: dict[int, int] = field(default_factory=dict)
    last_period: int = 0
    last_total: int = 0


class PaceTracker:
    """
    Çeyrek geçişlerini izler ve hız anomalilerini tespit eder.
    Bot restart'ta sıfırlanır (in-memory); kalıcılık gerekmez.
    """

    def __init__(self) -> None:
        self._matches: dict[str, _MatchState] = {}

    # ── Public API ──────────────────────────────────────────────────────

    def update(
        self,
        match_id: str,
        period: int,
        total_pts: int,
        quarter_length: float,
    ) -> dict:
        """
        Her scraping döngüsünde çağrılır.

        period          : Mevcut çeyrek (1-4)
        total_pts       : İki takımın toplam skoru
        quarter_length  : Çeyrek süresi (dakika), NBA=12, FIBA=10

        Döndürür: pace analiz sözlüğü (quarter_paces, anomaly_direction, pace_note, ...)
        """
        state = self._matches.setdefault(match_id, _MatchState())

        # Yeni çeyreğe geçiş algıla → önceki çeyreğin bitiş skoru kaydedilir
        if period > state.last_period and state.last_period > 0:
            state.quarter_end_totals[state.last_period] = state.last_total

        state.last_period = period
        state.last_total = total_pts

        return _build_analysis(state, period, total_pts, quarter_length)

    def clear(self, match_id: str) -> None:
        self._matches.pop(match_id, None)

    def clear_all(self) -> None:
        self._matches.clear()


# ── Analiz ──────────────────────────────────────────────────────────────

def _build_analysis(
    state: _MatchState,
    current_period: int,
    current_total: int,
    quarter_length: float,
) -> dict:
    qet = state.quarter_end_totals  # {periyot: kümülatif_skor}

    # Tamamlanan çeyreklerin bireysel puanları ve dakika bazı hızları
    quarter_pts: dict[int, int] = {}
    quarter_paces: dict[int, float] = {}
    prev = 0
    for q in sorted(qet.keys()):
        pts = qet[q] - prev
        quarter_pts[q] = pts
        quarter_paces[q] = round(pts / quarter_length * 10, 1)  # puan/10dk
        prev = qet[q]

    if not quarter_paces:
        return _empty_result()

    avg_pace = sum(quarter_paces.values()) / len(quarter_paces)

    # Mevcut çeyreğin anlık hızı (tamamlanan çeyreklerin üstündeki artış)
    completed_total = prev
    current_q_pts = max(0, current_total - completed_total)
    current_q_pace: float | None = None
    if current_q_pts >= 4:
        # Çeyreğin ne kadarının geçtiğini bilmiyoruz — "tam dolmuş gibi" kaba hız
        current_q_pace = round(current_q_pts / quarter_length * 10, 1)

    anomaly_direction: str | None = None
    anomaly_pct: float | None = None
    pace_note = ""

    if avg_pace > 0 and current_q_pace is not None:
        # Mevcut çeyreğin hızını geçmiş ortalamaya kıyasla
        deviation = (current_q_pace - avg_pace) / avg_pace
        anomaly_pct = round(deviation * 100)
        if deviation >= 0.20:
            anomaly_direction = "ALT"
            pace_note = (
                f"Bu çeyrek anormal hızlı: {current_q_pace:.1f} puan/10dk "
                f"(maç ort. {avg_pace:.1f}). "
                f"Ortalamaya dönüş beklenir → ALT baskısı."
            )
        elif deviation <= -0.20:
            anomaly_direction = "ÜST"
            pace_note = (
                f"Bu çeyrek anormal yavaş: {current_q_pace:.1f} puan/10dk "
                f"(maç ort. {avg_pace:.1f}). "
                f"Hızlanma beklenir → ÜST baskısı."
            )
        else:
            pace_note = (
                f"Mevcut çeyrek hızı normal: {current_q_pace:.1f} puan/10dk "
                f"(maç ort. {avg_pace:.1f})."
            )
    elif len(quarter_paces) >= 2:
        # Mevcut çeyrekte henüz yeterli puan yok — son tamamlanan çeyreğe bak
        last_q = max(quarter_paces.keys())
        last_pace = quarter_paces[last_q]
        prev_qs = [v for k, v in quarter_paces.items() if k < last_q]
        if prev_qs:
            prev_avg = sum(prev_qs) / len(prev_qs)
            if prev_avg > 0:
                deviation = (last_pace - prev_avg) / prev_avg
                anomaly_pct = round(deviation * 100)
                if deviation >= 0.20:
                    anomaly_direction = "ALT"
                    pace_note = (
                        f"Son çeyrek ({last_q}.) çok hızlı geçti: {last_pace:.1f} puan/10dk "
                        f"(önceki ort. {prev_avg:.1f}). "
                        f"Sonraki çeyrekte yavaşlama beklenir → ALT."
                    )
                elif deviation <= -0.20:
                    anomaly_direction = "ÜST"
                    pace_note = (
                        f"Son çeyrek ({last_q}.) çok yavaş geçti: {last_pace:.1f} puan/10dk "
                        f"(önceki ort. {prev_avg:.1f}). "
                        f"Sonraki çeyrekte hızlanma beklenir → ÜST."
                    )

    return {
        "quarter_pts": quarter_pts,
        "quarter_paces": quarter_paces,
        "avg_pace_per_10": round(avg_pace, 1),
        "current_q_pace_per_10": current_q_pace,
        "anomaly_direction": anomaly_direction,
        "anomaly_pct": anomaly_pct,
        "pace_note": pace_note,
    }


def _empty_result() -> dict:
    return {
        "quarter_pts": {},
        "quarter_paces": {},
        "avg_pace_per_10": None,
        "current_q_pace_per_10": None,
        "anomaly_direction": None,
        "anomaly_pct": None,
        "pace_note": "",
    }
