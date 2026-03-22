"""
scraper.py — SofaScore public API üzerinden canlı basketbol toplam (over/under)
çizgilerini çeker.

SofaScore'un resmi API'si olmasa da halka açık uç noktaları kullanılıyor.
Site banlamaması için:
  • Gerçekçi User-Agent ve Referer başlıkları
  • Her istekte 0.5–1.5 saniyelik küçük gecikme
  • Session nesnesi (bağlantıyı yeniden kullanır)
"""

import logging
import random
import re
import time
from typing import Optional

import requests

logger = logging.getLogger(__name__)

# SofaScore halka açık uç noktaları
_BASE = "https://api.sofascore.com/api/v1"

_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Referer": "https://www.sofascore.com/",
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "tr-TR,tr;q=0.9,en-US;q=0.8,en;q=0.7",
    "Origin": "https://www.sofascore.com",
}

# Basketbol toplam (over/under) için bilinen SofaScore market type ID'leri
# 10: Totals (over/under), 18: Asian Handicap benzeri yapılar da olabilir
_TOTAL_MARKET_IDS = {10, 18, 226}

# Basketbol toplam çizgisi için makul aralık
_MIN_TOTAL = 120.0
_MAX_TOTAL = 380.0


class SofaScoreScraper:
    def __init__(self):
        self._session = requests.Session()
        self._session.headers.update(_HEADERS)

    def _get(self, url: str, timeout: int = 10) -> Optional[dict]:
        """Tek bir GET isteği yapar; hata varsa None döner."""
        try:
            time.sleep(random.uniform(0.5, 1.5))
            resp = self._session.get(url, timeout=timeout)
            if resp.status_code == 200:
                return resp.json()
            if resp.status_code == 429:
                logger.warning("Rate limit! 60 saniye bekleniyor...")
                time.sleep(60)
            else:
                logger.debug(f"HTTP {resp.status_code} — {url}")
        except requests.RequestException as e:
            logger.warning(f"İstek hatası: {e}")
        return None

    # ------------------------------------------------------------------ #
    #  Canlı maçları listele                                               #
    # ------------------------------------------------------------------ #

    def get_live_events(self) -> list[dict]:
        """
        Şu an canlı olan basketbol maçlarını döner.
        Her eleman: {id, homeTeam, awayTeam, tournament, status, ...}
        """
        data = self._get(f"{_BASE}/sport/basketball/events/live")
        if not data:
            logger.warning("Canlı basketbol maçları alınamadı.")
            return []
        events = data.get("events", [])
        logger.debug(f"{len(events)} canlı basketbol maçı bulundu.")
        return events

    # ------------------------------------------------------------------ #
    #  Maç odds verisi                                                     #
    # ------------------------------------------------------------------ #

    def get_event_odds(self, event_id: int) -> Optional[dict]:
        """
        Bir maçın tüm odds verilerini çeker.
        Birden fazla uç nokta dener; hangisi çalışırsa onu kullanır.
        """
        endpoints = [
            f"{_BASE}/event/{event_id}/odds/1/all/all",
            f"{_BASE}/event/{event_id}/odds/0/all/all",
        ]
        for url in endpoints:
            data = self._get(url)
            if data:
                return data
        return None

    # ------------------------------------------------------------------ #
    #  Total çizgisini ayrıştır                                           #
    # ------------------------------------------------------------------ #

    def extract_total(self, odds_data: dict) -> Optional[float]:
        """
        Odds yanıtından over/under çizgisini çıkarır.
        SofaScore'un yanıt yapısına göre birden fazla yol dener.
        """
        if not odds_data:
            return None

        # 1) markets → choices yolu
        for market in odds_data.get("markets", []):
            total = _parse_market(market)
            if total is not None:
                return total

        # 2) Flat list yolu (bazı yanıtlarda markets yerine doğrudan liste gelir)
        if isinstance(odds_data, list):
            for market in odds_data:
                total = _parse_market(market)
                if total is not None:
                    return total

        # 3) Eski format: "odds" anahtarı altındaki düz liste
        for item in odds_data.get("odds", []):
            total = _extract_total_from_flat_odds(item)
            if total is not None:
                return total

        return None

    # ------------------------------------------------------------------ #
    #  Ana metod: tüm canlı basketbol maçlarını toplam çizgisiyle döner  #
    # ------------------------------------------------------------------ #

    def get_live_basketball_totals(self) -> list[dict]:
        """
        Şu an canlı olan tüm basketbol maçlarını ve toplam çizgilerini döner.

        Dönüş formatı:
        [
          {
            "match_id": "12345678",
            "match_name": "Boston Celtics - LA Lakers",
            "tournament": "NBA",
            "live_total": 221.5,
            "status": "2nd Quarter"
          },
          ...
        ]
        """
        events = self.get_live_events()
        results = []

        for event in events:
            event_id = event.get("id")
            if not event_id:
                continue

            match_name = _build_match_name(event)
            tournament = _get_tournament_name(event)
            status = _get_status_text(event)

            # Odds verisini çek
            odds_data = self.get_event_odds(event_id)
            live_total = self.extract_total(odds_data) if odds_data else None

            if live_total is None:
                logger.debug(f"Toplam çizgisi bulunamadı: {match_name}")
                continue

            results.append(
                {
                    "match_id": str(event_id),
                    "match_name": match_name,
                    "tournament": tournament,
                    "live_total": live_total,
                    "status": status,
                }
            )

        logger.info(
            f"{len(events)} canlı maçtan {len(results)} tanesi için toplam çizgisi bulundu."
        )
        return results


# ------------------------------------------------------------------ #
#  Yardımcı fonksiyonlar                                              #
# ------------------------------------------------------------------ #


def _parse_market(market: dict) -> Optional[float]:
    """
    Tek bir market nesnesinden toplam çizgisini ayrıştırır.
    """
    market_id = market.get("marketId") or market.get("id")
    market_name = str(market.get("marketName") or market.get("name") or "").lower()

    is_totals = (
        market_id in _TOTAL_MARKET_IDS
        or "total" in market_name
        or "over" in market_name
        or "o/u" in market_name
    )

    if not is_totals:
        return None

    choices = market.get("choices") or market.get("outcomes") or []
    for choice in choices:
        choice_name = str(choice.get("name") or choice.get("choiceName") or "")
        # "Over 221.5" veya "221.5" gibi formatları dene
        total = _parse_total_from_string(choice_name)
        if total is not None:
            return total

    return None


def _extract_total_from_flat_odds(item: dict) -> Optional[float]:
    """
    Eski/düz odds nesnelerinden toplam çizgisi arar.
    """
    name = str(item.get("name") or item.get("handicap") or "").lower()
    if any(kw in name for kw in ["total", "over", "under", "o/u"]):
        val = item.get("handicap") or item.get("value") or item.get("line")
        if val is not None:
            try:
                total = float(str(val).replace(",", "."))
                if _MIN_TOTAL <= total <= _MAX_TOTAL:
                    return total
            except ValueError:
                pass
    return None


def _parse_total_from_string(s: str) -> Optional[float]:
    """
    Bir dizeden ilk sayıyı çıkarır ve basketbol aralığında mı kontrol eder.
    Örnek: "Over 221.5" → 221.5, "Under 218" → 218.0
    """
    match = re.search(r"\d{3}(?:[.,]\d)?", s)
    if match:
        try:
            val = float(match.group().replace(",", "."))
            if _MIN_TOTAL <= val <= _MAX_TOTAL:
                return val
        except ValueError:
            pass
    return None


def _build_match_name(event: dict) -> str:
    home = (event.get("homeTeam") or {}).get("name", "Ev Sahibi")
    away = (event.get("awayTeam") or {}).get("name", "Deplasman")
    return f"{home} - {away}"


def _get_tournament_name(event: dict) -> str:
    t = event.get("tournament") or {}
    return t.get("name") or t.get("uniqueTournament", {}).get("name") or "Bilinmiyor"


def _get_status_text(event: dict) -> str:
    status = event.get("status") or {}
    desc = status.get("description") or ""
    period = status.get("period") or ""
    if period:
        return f"{period}. Periyot"
    return desc or "Canlı"
