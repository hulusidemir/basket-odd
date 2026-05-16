"""Sinyal skoru — 2026-05-15 analizinden çıkan stabil profillere göre 0-100.

Veriden çıkarıldı (n=2376, out-of-sample TRAIN/TEST stabilitesi doğrulandı):

  Ana motor — sinyal yönü ↔ anlık oynanan toplam (live_total) uyumu:
    - ALT: live_total ≤170 destek, ≥170 tuzak (%41.3)
    - ÜST: live_total ≥170 destek (%83.5), <150 kesin tuzak (%10.6-%32.4)

  Destekleyici — açılış bandı × live_total etkileşimi (10 stabil profil).
  Yardımcı — periyot, fair_edge işaret tutarlılığı, alignment paradoxu.

  Ek katmanlar (2026-05-15 v2): canlı bir Perry Lake Eagle ALT sinyalinde
  formülün overconfident kaldığı görüldükten sonra eklendi:
    F) ppm bandı (puan/dakika) — ALT ≥5.0 zayıflık, ÜST ≥5.5 güç
    G) Projection-live gap teri — proj canlı ile aynı yöne işaret etmiyorsa ceza
    H) Erken sinyal (P1) downweight — küçük örneklem aşırı güveni törpüle
    I) H2H quality sweet-spot — 70-89 ALT için anlamlı (analiz %80.9)
    J) Açılış uç bandları — ALT 190+ ve 200+ tuzak (%37-42), ÜST <140 baseline altı

Skor anlık hesaplanır; DB yazma yoktur, mevcut akışa müdahale yoktur.
"""

from __future__ import annotations

import re
from typing import Any


_SCORE_RE = re.compile(r"(\d{1,3})\s*[-–]\s*(\d{1,3})")


def _num(value: Any) -> float | None:
    try:
        if value is None or value == "":
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _parse_score(score: Any) -> tuple[int | None, int | None]:
    m = _SCORE_RE.search(str(score or ""))
    if not m:
        return None, None
    return int(m.group(1)), int(m.group(2))


def _label(score: int) -> str:
    if score >= 80:
        return "Güçlü"
    if score >= 65:
        return "İyi"
    if score >= 50:
        return "Orta"
    if score >= 35:
        return "Zayıf"
    return "Pas"


def compute_signal_score(alert: dict, analysis: dict | None = None) -> dict[str, Any]:
    """Bir sinyal için 0-100 arası skor üretir.

    Dönüş: {"score": int 0-100, "label": str}
    """
    direction = str(alert.get("direction") or "").strip().upper()
    if direction not in ("ALT", "ÜST"):
        return {"score": 50, "label": "—"}

    a = analysis if isinstance(analysis, dict) else {}
    opening = _num(alert.get("opening"))
    live = _num(alert.get("live"))
    period = int(alert.get("alert_period") or 0)
    fair_edge = _num(a.get("fair_edge"))
    projected = _num(a.get("projected_total"))
    h2h_q = _num(a.get("h2h_quality_score"))
    align = ((a.get("team_context") or {}).get("alignment_code") or "")
    proj_components = a.get("projection_components") or {}
    ppm = _num(proj_components.get("current_pace_per_min")) or _num(a.get("match_ppm"))
    elapsed = _num(a.get("elapsed_minutes"))

    home_s, away_s = _parse_score(alert.get("score"))
    live_total = (home_s + away_s) if home_s is not None else None

    # PPM elde yoksa anlık skor + elapsed_minutes'tan türet
    if ppm is None and live_total is not None and elapsed and elapsed > 0:
        ppm = live_total / elapsed

    # Başlangıç: yön baseline (analiz: ALT %57.5, ÜST %49.3)
    score = 57.0 if direction == "ALT" else 49.0

    # Erken sinyal flag'i (P1 + az puan = küçük örneklem, downweight)
    is_early = (period == 1) or (live_total is not None and live_total < 60)

    # ------------------------------------------------------------------
    # KATMAN A — Sinyal yönü × anlık toplam uyumu (ana motor)
    # ------------------------------------------------------------------
    layer_a = 0.0
    if live_total is not None:
        if direction == "ALT":
            if live_total < 120:
                layer_a = 28
            elif live_total < 150:
                layer_a = 26       # 120-149 → ALT bombası (%75.7)
            elif live_total < 170:
                layer_a = 14       # 150-169 → ALT iyi (%66)
            else:
                layer_a = -20      # 170+ → ALT tuzak (%41.3)
        else:
            if live_total < 120:
                layer_a = -35      # ÜST kesin tuzak (%10.6)
            elif live_total < 150:
                layer_a = -18      # ÜST zayıf (%32.4)
            elif live_total < 170:
                layer_a = 12       # ÜST hafif destek (%61)
            else:
                layer_a = 30       # ÜST güçlü (%83.5)
    # P1 + ALT live_total <120 küçük örneklemde overconfident — törpüle
    if is_early and direction == "ALT" and live_total is not None and live_total < 120:
        layer_a *= 0.45   # +28 → +12 civarı
    score += layer_a

    # ------------------------------------------------------------------
    # KATMAN B — Açılış bandı × anlık toplam (10 stabil profil)
    # ------------------------------------------------------------------
    if opening is not None and live_total is not None:
        if direction == "ALT":
            if 160 <= opening < 170 and 150 <= live_total < 170:
                score += 14      # TEST %94.4
            elif opening < 140 and 120 <= live_total < 150:
                score += 12      # TEST %80.0
            elif 150 <= opening < 160 and 150 <= live_total < 170:
                score += 8       # %76.6
            elif opening < 140 and 150 <= live_total < 170:
                score -= 20      # %11.4 (kesin tuzak)
            elif 150 <= opening < 160 and live_total >= 170:
                score -= 18      # %15.6 (tuzak)
            # Açılış uç bandları — Katman J:
            elif opening >= 190 and live_total >= 170:
                score -= 12      # 190+ %37, 200+ %42
        else:
            if 170 <= opening < 180 and live_total >= 170:
                score += 18      # TEST %96.4
            elif 180 <= opening < 190 and live_total >= 170:
                score += 16      # TEST %95.0
            elif 150 <= opening < 160 and 150 <= live_total < 170:
                score += 15      # %90.6
            elif opening < 140 and 120 <= live_total < 150:
                score += 14      # %84.6
            elif 160 <= opening < 170 and 150 <= live_total < 170:
                score += 12      # TEST %81.2
            elif opening < 140 and live_total < 120:
                score -= 25      # %13.6 (kesin tuzak)
            elif 160 <= opening < 170 and 120 <= live_total < 150:
                score -= 18      # %20.8
            elif 180 <= opening < 190 and 150 <= live_total < 170:
                score -= 15      # %23.3
            # ÜST < 140 baseline altı (%46.8) — küçük ceza
            elif opening is not None and opening < 140 and live_total is not None and live_total >= 150:
                score -= 3

    # ------------------------------------------------------------------
    # KATMAN C — Periyot × anlık toplam (stabil profil bonusu)
    # ------------------------------------------------------------------
    if live_total is not None and period in (2, 3):
        if direction == "ALT" and 120 <= live_total < 150:
            score += 6   # P2 %88.9 / P3 %85.4
        elif direction == "ÜST" and live_total >= 170:
            score += 8   # P2 %89.1 / P3 %83.6

    # ------------------------------------------------------------------
    # KATMAN D — fair_edge işaret tutarlılığı (yardımcı)
    # ------------------------------------------------------------------
    if fair_edge is not None:
        if direction == "ALT":
            if -10 <= fair_edge <= -2:
                score += 3
            elif fair_edge < -10:
                score -= 4   # aşırı destek = piyasa fiyatlamış, tuzak (%49)
            elif fair_edge > 3:
                score -= 3
        else:
            if 2 <= fair_edge <= 10:
                score += 3
            elif fair_edge > 10:
                score -= 1
            elif fair_edge < -2:
                score -= 4

    # ------------------------------------------------------------------
    # KATMAN E — Alignment paradoxu
    # ------------------------------------------------------------------
    if align == "against":
        score -= 6     # ALT %22.2, ÜST %0 (her iki yön için zayıf)
    elif direction == "ÜST" and align == "support":
        score -= 4     # ÜST + support paradoxal tuzak (%45.1)
    elif direction == "ÜST" and align == "neutral":
        score += 2     # ÜST + neutral baseline üstü (%54.2)
    elif direction == "ALT" and align == "support":
        score += 1     # ALT + support hafif iyi (%58.8)

    # ------------------------------------------------------------------
    # KATMAN F — PPM (puan/dakika) bandı
    # ------------------------------------------------------------------
    if ppm is not None:
        if direction == "ALT":
            if ppm < 3.5:
                score += 5     # %62.5
            elif ppm < 4.0:
                score += 1     # %58.6
            elif ppm < 4.5:
                score += 0
            elif ppm < 5.0:
                score += 4     # %61.4
            elif ppm < 5.5:
                score -= 5     # %52.2 (baseline altı)
            else:
                score -= 6     # %52.0 (hızlı tempo, ALT zayıf)
        else:
            if ppm < 3.5:
                score -= 2     # %47.5
            elif ppm < 4.0:
                score += 3     # %51.8
            elif ppm < 5.0:
                score += 0
            elif ppm < 5.5:
                score += 5     # %54.2 küçük örneklem
            else:
                score += 8     # %63.6 (hızlı tempo, ÜST güç)

    # ------------------------------------------------------------------
    # KATMAN G — Projection-live gap doğrulama
    # ------------------------------------------------------------------
    if projected is not None and live is not None:
        gap = projected - live   # + = sistem live'ın üstü tahmin ediyor (ÜST eğilimi)
        if direction == "ALT" and gap >= 10:
            score -= 6           # sistem ÜST diyor, ALT sinyali ters
        elif direction == "ALT" and gap >= 5:
            score -= 3
        elif direction == "ALT" and gap <= -10:
            score += 4           # sistem de ALT diyor
        elif direction == "ÜST" and gap <= -10:
            score -= 6           # sistem ALT diyor, ÜST sinyali ters
        elif direction == "ÜST" and gap <= -5:
            score -= 3
        elif direction == "ÜST" and gap >= 10:
            score += 4

    # ------------------------------------------------------------------
    # KATMAN H — Erken sinyal (P1) genel downweight
    # ------------------------------------------------------------------
    # Q1 sinyallerinde örneklem küçük + market yeterince hareket etmemiş,
    # skoru baseline'a doğru pull et (yüksekse düşür, düşükse yükselt).
    if is_early:
        baseline = 57 if direction == "ALT" else 49
        score = score * 0.75 + baseline * 0.25

    # ------------------------------------------------------------------
    # KATMAN I — H2H quality sweet-spot
    # ------------------------------------------------------------------
    if h2h_q is not None:
        if direction == "ALT" and 70 <= h2h_q < 90:
            score += 5     # %80.9 (n=47)
        elif direction == "ALT" and h2h_q < 40:
            score -= 2     # düşük kalite H2H, az veri

    # 0-100 clamp
    score = int(round(max(0.0, min(100.0, score))))
    return {"score": score, "label": _label(score)}
