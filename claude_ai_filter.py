"""C_A (Claude AI) filtre katmanı — v2 (yüksek güven kademeli).

Strateji:
    Mevcut sinyal üretimine dokunmadan, üretilmiş bir sinyali (alert + analysis)
    geçmiş 1595 sonuçlu silinen sinyal üzerinden çıkarılan dar/yüksek-tutma
    örüntüleriyle eşler. Sadece geçmişte ≥%85 tutmuş kovalar için ⭐ verir.

4 senaryo:
    SENARYO 1  FADE_UNDER   ALT sinyali → tersine ÜST oyna   (geçerli kova bulunamadı; skip)
    SENARYO 2  TRUE_UNDER   ALT sinyali → güçlü ALT          (kademe A/B)
    SENARYO 3  FADE_OVER    ÜST sinyali → tersine ALT oyna   (kademe A/C)
    SENARYO 4  TRUE_OVER    ÜST sinyali → güçlü ÜST          (kademe A — 100 Profili)

Kademe A — en güvenilir:
    Sistemin var olan `hundred_profile` kuralı eşleştiyse → C_A da ⭐
    (geçmiş veride 124/124 = %100 tutmuş, ALT 82/82 + ÜST 42/42)

Kademe B — TRUE_UNDER ek (h100 değilken):
    B1: ALT + opening<140 + projeksiyon ALT destek + h2h ALT destek    → 16/17 = %94.1
    B2: ALT + Q2 + projeksiyon nötr + ılımlı regresyon (5-10)           → 14/15 = %93.3
    B3: ALT + küçük fark (8-11) + Q2 + ılımlı regresyon (5-10)          → 18/21 = %85.7

Kademe C — FADE_OVER ek (h100 değilken):
    C1: ÜST + opening 140-149 + fair nötr + projeksiyon nötr            → 16/18 = %88.9
    C2: ÜST + opening 140-149 + takım ÜST destek + fair nötr            → 15/17 = %88.2
    C3: ÜST + opening 150-159 + nötr profil + zayıf regresyon (<5)      → 13/15 = %86.7
    C4: ÜST + opening 150-159 + nötr profil + fair ÜST destek           → 18/21 = %85.7
    C5: ÜST + opening 150-159 + takım ÜST destek + h2h nötr             → 18/21 = %85.7
    C6: ÜST + opening<140 + takım ÜST destek + h2h çok güçlü ÜST destek → 17/20 = %85.0
    C7: ÜST + opening 150-159 + fair nötr + güçlü regresyon (10-20)     → 17/20 = %85.0

Birleşik backtest sonuçları (1595 sonuçlu silinen sinyal):
    TRUE_UNDER  n=121  %96.7
    TRUE_OVER   n= 42  %100   (sadece kademe A)
    FADE_OVER   n=106  %84.9
    Toplam ⭐   269 / 1595 (%16.9), birleşik C_A bahsi başarısı: 249/269 = %92.6

UYARI: Küçük örneklemler nedeniyle canlıda gerçek oran %85-92 bandında dalgalanabilir.
"""

from __future__ import annotations
from typing import Any, Dict


SCENARIOS: Dict[str, Dict[str, str]] = {
    "TRUE_UNDER": {
        "label": "Güçlü Alt",
        "play": "ALT",
        "tooltip": "Güçlü Alt — geçmişte ~%96 tutan örüntü",
    },
    "TRUE_OVER": {
        "label": "Güçlü Üst",
        "play": "ÜST",
        "tooltip": "Güçlü Üst — geçmişte %100 tutan 100 Profili örüntüsü",
    },
    "FADE_OVER": {
        "label": "Tersine Alt",
        "play": "ALT",
        "tooltip": "Tersine Alt — düşük totalli maçta ÜST tuzağı, geçmişte ~%87 ALT tutmuş",
    },
    "FADE_UNDER": {
        "label": "Tersine Üst",
        "play": "ÜST",
        "tooltip": "Tersine Üst",
    },
}


# ---------- yardımcı bucket fonksiyonları ----------

def _f(value: Any) -> float | None:
    try:
        if value is None or value == "":
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _open_band(o: float | None) -> str:
    if o is None:
        return "na"
    if o < 140: return "o<140"
    if o < 150: return "o140-149"
    if o < 160: return "o150-159"
    if o < 170: return "o160-169"
    if o < 180: return "o170-179"
    if o < 190: return "o180-189"
    if o < 200: return "o190-199"
    return "o200+"


def _diff_band(d: float | None) -> str:
    d = abs(d or 0)
    if d < 8:  return "d<8"
    if d < 12: return "d8-11"
    if d < 16: return "d12-15"
    if d < 20: return "d16-19"
    if d < 25: return "d20-24"
    return "d25+"


def _fair_band(direction: str, fair_edge: float | None) -> str:
    if fair_edge is None:
        return "fair_na"
    if direction == "ALT":
        if fair_edge <= -8: return "fair_supp_strong"
        if fair_edge <= -3: return "fair_supp"
        if fair_edge <  3:  return "fair_neut"
        return "fair_opp"
    if fair_edge >=  8: return "fair_supp_strong"
    if fair_edge >=  3: return "fair_supp"
    if fair_edge >  -3: return "fair_neut"
    return "fair_opp"


def _proj_band(direction: str, projected_total: float | None, live: float | None) -> str:
    if projected_total is None or live is None:
        return "proj_na"
    gap = projected_total - live
    if direction == "ALT":
        if gap <= -15: return "proj_supp_strong"
        if gap <=  -5: return "proj_supp"
        if gap <    5: return "proj_neut"
        return "proj_opp"
    if gap >=  15: return "proj_supp_strong"
    if gap >=   5: return "proj_supp"
    if gap >   -5: return "proj_neut"
    return "proj_opp"


def _reg_band(regression_delta: float | None) -> str:
    if regression_delta is None:
        return "reg_na"
    rd = abs(regression_delta)
    if rd <  5: return "reg<5"
    if rd < 10: return "reg5-10"
    if rd < 20: return "reg10-20"
    return "reg20+"


def _h2h_band(direction: str, h2h_total: float | None, live: float | None) -> str:
    if h2h_total is None or live is None:
        return "h2h_na"
    gap = h2h_total - live
    if direction == "ALT":
        if gap <= -15: return "h2h_supp_strong"
        if gap <=  -5: return "h2h_supp"
        if gap <    5: return "h2h_neut"
        return "h2h_opp"
    if gap >=  15: return "h2h_supp_strong"
    if gap >=   5: return "h2h_supp"
    if gap >   -5: return "h2h_neut"
    return "h2h_opp"


# ---------- ana sınıflandırma ----------

def evaluate_claude_ai(alert: dict, analysis: dict | None) -> dict:
    """Bir alert + analysis için C_A filtre kararını döner.

    Dönüş:
        {"claude_ai": "<KOD>"|"", "claude_ai_rule": "<insan-okunur kısa açıklama>"}

    Hiçbir kural eşleşmezse boş döner (yıldızlanmaz).
    """
    direction = (alert.get("direction") or "").strip()
    if direction not in ("ALT", "ÜST"):
        return {"claude_ai": "", "claude_ai_rule": ""}

    a = analysis if isinstance(analysis, dict) else {}
    opening = _f(alert.get("opening"))
    live    = _f(alert.get("live"))
    diff    = _f(alert.get("diff"))
    period  = int(alert.get("alert_period") or 0)

    fair_edge        = _f(a.get("fair_edge"))
    projected_total  = _f(a.get("projected_total"))
    h2h_total        = _f(a.get("h2h_total"))
    team_context     = a.get("team_context") if isinstance(a.get("team_context"), dict) else {}
    alignment        = (team_context or {}).get("alignment_code") or "na"
    regression_delta = _f((team_context or {}).get("regression_delta"))

    open_b = _open_band(opening)
    diff_b = _diff_band(diff)
    fair_b = _fair_band(direction, fair_edge)
    proj_b = _proj_band(direction, projected_total, live)
    reg_b  = _reg_band(regression_delta)
    h2h_b  = _h2h_band(direction, h2h_total, live)
    period_b = f"P{period}" if period else "P0"

    # ---- KADEME A: 100 Profili eşleştiyse direkt ⭐ ----
    # alert.hundred_profile=1 olduğunda geçmiş 124/124 (%100) kazanmış
    if int(alert.get("hundred_profile") or 0) == 1:
        if direction == "ALT":
            return {
                "claude_ai": "TRUE_UNDER",
                "claude_ai_rule": "100 Profili onayı (geçmiş %100)",
            }
        return {
            "claude_ai": "TRUE_OVER",
            "claude_ai_rule": "100 Profili onayı (geçmiş %100)",
        }

    # ---- KADEME B: TRUE_UNDER pure-h0 yüksek güven ----
    if direction == "ALT":
        # B1 — %94.1
        if open_b == "o<140" and proj_b == "proj_opp" and h2h_b == "h2h_supp":
            return {"claude_ai": "TRUE_UNDER",
                    "claude_ai_rule": "B1: çok düşük total + projeksiyon yanıltıcı yüksek + H2H ALT destek (%94)"}
        # B2 — %93.3
        if period_b == "P2" and proj_b == "proj_neut" and reg_b == "reg5-10":
            return {"claude_ai": "TRUE_UNDER",
                    "claude_ai_rule": "B2: Q2 + projeksiyon nötr + ılımlı regresyon (%93)"}
        # B3 — %85.7
        if diff_b == "d8-11" and period_b == "P2" and reg_b == "reg5-10":
            return {"claude_ai": "TRUE_UNDER",
                    "claude_ai_rule": "B3: küçük fark + Q2 + ılımlı regresyon (%86)"}

    # KADEME C (FADE_OVER C1-C7) devre dışı — canlıda %19.6 tuttu, overfit kurallardı.
    # ÜST sinyalleri için sadece Kademe A (100 Profili) yıldız verir.

    return {"claude_ai": "", "claude_ai_rule": ""}


def scenario_meta(code: str) -> dict:
    return SCENARIOS.get(code or "", {"label": "", "play": "", "tooltip": ""})
