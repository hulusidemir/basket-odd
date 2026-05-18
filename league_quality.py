"""Lig kalite haritası — 2026-05-16 itibarıyla 2796 sonuçlu sinyalden çıkarıldı.

Her ligi yön bazlı (ALT / ÜST) 5 kategoriden birine atar:

    good      — n≥15, başarı ≥%60, train+test stabil   (yeşil 🟢)
    mid_good  — başarı ≥%55                            (açık yeşil)
    neutral   — başarı %48-%55 arası                   (gri)
    mid_bad   — başarı ≤%48                            (turuncu)
    bad       — n≥15, başarı ≤%45, train+test stabil   (kırmızı 🔴)
    unknown   — n<15 (yeterli veri yok)                (gri)

Kullanım: evaluate_league_quality(tournament, direction)
"""

from __future__ import annotations
from typing import Any

LEAGUE_QUALITY: dict[str, dict[str, str]] = {
    'Adriatic Basketball Association League': {'ALT': 'good', 'ÜST': 'unknown'},
    'Argentina Liga B': {'ALT': 'neutral', 'ÜST': 'bad'},
    'Argentinian Liga Nacional de Bosquetbol': {'ALT': 'unknown', 'ÜST': 'mid_bad'},
    'Australia Big V League': {'ALT': 'unknown', 'ÜST': 'bad'},
    "Australia Big V Women's League": {'ALT': 'unknown', 'ÜST': 'bad'},
    'Australia National Basketball League1 North': {'ALT': 'unknown', 'ÜST': 'bad'},
    'Australia National Basketball League1 West': {'ALT': 'bad', 'ÜST': 'unknown'},
    'B1 League': {'ALT': 'good', 'ÜST': 'unknown'},
    'BNXT': {'ALT': 'good', 'ÜST': 'unknown'},
    'Baloncesto Superior Nacional': {'ALT': 'mid_good', 'ÜST': 'mid_good'},
    'Basketball Bundesliga': {'ALT': 'good', 'ÜST': 'mid_good'},
    'Brazil Camp.Interclubes U19': {'ALT': 'good', 'ÜST': 'unknown'},
    'Chile LNB Segunda': {'ALT': 'mid_good', 'ÜST': 'unknown'},
    'China National Basketball League U21': {'ALT': 'mid_good', 'ÜST': 'bad'},
    'Chinese Basketball Association': {'ALT': 'bad', 'ÜST': 'mid_good'},
    'Dominican Republic Liga Nacional Basketball': {'ALT': 'bad', 'ÜST': 'mid_good'},
    'France Nationale 1': {'ALT': 'neutral', 'ÜST': 'unknown'},
    'Israel Basketball League': {'ALT': 'mid_bad', 'ÜST': 'good'},
    'Kazakhstan National League': {'ALT': 'mid_good', 'ÜST': 'unknown'},
    'Lietuvos Krepsinio Lyga': {'ALT': 'unknown', 'ÜST': 'bad'},
    'Liga Asociación de Clubs de Baloncesto': {'ALT': 'good', 'ÜST': 'unknown'},
    'Liga Nationala de Baschet Masculin': {'ALT': 'bad', 'ÜST': 'unknown'},
    'Liga de Baloncesto': {'ALT': 'bad', 'ÜST': 'unknown'},
    'Ligat HaAl': {'ALT': 'neutral', 'ÜST': 'unknown'},
    'Mexico CIBACOPA': {'ALT': 'mid_bad', 'ÜST': 'unknown'},
    'Mexico LNBPF Women': {'ALT': 'mid_bad', 'ÜST': 'unknown'},
    'NBL1 Eastern': {'ALT': 'bad', 'ÜST': 'unknown'},
    'National Basketball Association': {'ALT': 'unknown', 'ÜST': 'good'},
    'National Basketball League1 South': {'ALT': 'neutral', 'ÜST': 'good'},
    'National Women’s Basketball League1 East': {'ALT': 'neutral', 'ÜST': 'unknown'},
    'National Women’s Basketball League1 North': {'ALT': 'unknown', 'ÜST': 'bad'},
    'National Women’s Basketball League1 South': {'ALT': 'bad', 'ÜST': 'good'},
    'National Women’s Basketball League1 West': {'ALT': 'good', 'ÜST': 'unknown'},
    'Nemzeti Bajnokság I/A': {'ALT': 'mid_good', 'ÜST': 'neutral'},
    'New Zealand National Basketball League': {'ALT': 'unknown', 'ÜST': 'mid_bad'},
    'Novo Basquete Brasil': {'ALT': 'bad', 'ÜST': 'unknown'},
    'Paraguay Primera': {'ALT': 'unknown', 'ÜST': 'bad'},
    "Philippine Basketball Commissioner's Cup": {'ALT': 'mid_bad', 'ÜST': 'unknown'},
    'Philippines MPBL': {'ALT': 'mid_good', 'ÜST': 'good'},
    'Polska Liga Koszykowk': {'ALT': 'neutral', 'ÜST': 'unknown'},
    'Senegal Division 1': {'ALT': 'unknown', 'ÜST': 'mid_good'},
    'Serie B Basket': {'ALT': 'good', 'ÜST': 'bad'},
    'Turkish Basketball First League': {'ALT': 'unknown', 'ÜST': 'bad'},
    'Uruguay Liga Uruguaya': {'ALT': 'unknown', 'ÜST': 'bad'},
    "Women's National Basketball Association": {'ALT': 'bad', 'ÜST': 'mid_bad'},
}

# Rozet meta: etiket, ton (renk kategorisi), tooltip
_BADGE: dict[str, dict[str, str]] = {
    'good':     {'label': '🟢 İyi Lig',    'tone': 'success',  'tooltip': 'Bu ligde bu yön geçmişte %60+ tutmuş ve stabil.'},
    'mid_good': {'label': '🟢 Olumlu Lig', 'tone': 'positive', 'tooltip': 'Bu ligde bu yön geçmişte %55+ tutmuş.'},
    'neutral':  {'label': '⚪ Nötr Lig',   'tone': 'neutral',  'tooltip': 'Bu lig için bu yönde belirgin avantaj yok.'},
    'mid_bad':  {'label': '🟡 Zayıf Lig',  'tone': 'warning',  'tooltip': 'Bu ligde bu yön geçmişte %48 altı tutmuş, temkinli ol.'},
    'bad':      {'label': '🔴 Kötü Lig',   'tone': 'danger',   'tooltip': 'Bu ligde bu yön geçmişte %45 altı tutmuş, kaçın.'},
    'unknown':  {'label': '',              'tone': 'unknown',  'tooltip': ''},
}


def evaluate_league_quality(tournament: str | None, direction: str | None) -> dict[str, Any]:
    """Bir lig + yön için kalite rozetini döner.

    Dönüş:
        {
            "league_quality": "good"|"mid_good"|"neutral"|"mid_bad"|"bad"|"unknown",
            "league_quality_label": "🟢 İyi Lig",
            "league_quality_tone": "success",
            "league_quality_tooltip": "...",
        }
    """
    direction = (direction or "").strip()
    if direction not in ("ALT", "ÜST"):
        direction = ""
    entry = LEAGUE_QUALITY.get((tournament or "").strip(), {})
    quality = entry.get(direction, "unknown") if direction else "unknown"
    meta = _BADGE.get(quality, _BADGE['unknown'])
    return {
        "league_quality": quality,
        "league_quality_label": meta['label'],
        "league_quality_tone": meta['tone'],
        "league_quality_tooltip": meta['tooltip'],
    }
