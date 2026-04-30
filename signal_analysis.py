import json
import math
import re
from statistics import mean

from projection import calculate_live_projection, game_clock, parse_score


def calculate_fair_line(
    *,
    prematch: float | None,
    pure_pace_projection: float | None,
    elapsed_minutes: float | None,
    total_game_minutes: int = 40,
    data_quality: float = 50.0,
) -> tuple[float | None, dict]:
    """
    İki bileşenli adil barem: açılış baremi + saf canlı tempo projeksiyonu
    (market_anchor IÇERMEYEN). H2H ve son form veriler buraya girmez — bookmaker'ın
    açılış çizgisi onları zaten büyük ölçüde fiyatlamıştır (double-counting önlenir).

    Mekanizma:
      - Sigmoidal time-decay: maç ilerledikçe live ağırlığı doğal S-eğrisiyle artar.
      - Data quality modülasyonu: 0-100 → 0.5-1.0 arası çarpan; düşük kaliteli
        canlı projeksiyonlarda açılış baremine geri sığınılır.
      - Minimum açılış sığınağı %15: maçın en sonunda bile bookmaker'ın çizgisi
        bir miktar ağırlığını korur (garbage-time / blowout savrulmalarına karşı).

    Profesyonel basket analisti mantığı:
      - Açılış baremi piyasanın ilk ana referans noktasıdır.
      - Saf canlı projeksiyon = pace_blend + shot/foul düzeltmeleri + script
        düzeltmesi (close-game / blowout). Tempo regresyonu içeride uygulanır.
      - Bu iki sinyal birbirinden bağımsız ve aralarındaki ağırlık dengelenmesi
        maçın kendi gelişimine göre yapılır.
    """
    if prematch is None:
        return (pure_pace_projection, {
            "prematch_weight": 0.0, "opening_weight": 0.0, "live_weight": 1.0,
            "progress": 0.0, "data_quality": data_quality, "anchor": "live_only",
        }) if pure_pace_projection is not None else (None, {
            "prematch_weight": 0.0, "opening_weight": 0.0, "live_weight": 0.0,
            "progress": 0.0, "data_quality": data_quality, "anchor": "none",
        })
    if pure_pace_projection is None or elapsed_minutes is None:
        return (float(prematch), {
            "prematch_weight": 1.0, "live_weight": 0.0,
            "opening_weight": 1.0, "progress": 0.0, "data_quality": data_quality, "anchor": "opening_only",
        })

    total = max(20, int(total_game_minutes or 40))
    progress = max(0.0, min(1.0, float(elapsed_minutes) / total))

    # Sigmoidal: progress 0.0→%3, 0.25→%15, 0.5→%50, 0.75→%85, 1.0→%97
    sigmoid_live = 1.0 / (1.0 + math.exp(-7.0 * (progress - 0.5)))

    # Data quality modülasyonu: kalite=30 → 0.65, kalite=70 → 0.85, kalite=100 → 1.0
    quality_factor = 0.5 + 0.5 * max(0.0, min(1.0, float(data_quality) / 100.0))
    live_weight = sigmoid_live * quality_factor

    # Minimum prematch sığınağı %15 — Q4 sonunda bile bookmaker anchor'ı korunur.
    live_weight = max(0.0, min(0.85, live_weight))
    prematch_weight = 1.0 - live_weight

    fair = float(prematch) * prematch_weight + float(pure_pace_projection) * live_weight
    return round(fair, 1), {
        "prematch_weight": round(prematch_weight, 3),
        "opening_weight": round(prematch_weight, 3),
        "live_weight": round(live_weight, 3),
        "progress": round(progress, 3),
        "data_quality": round(float(data_quality), 1),
        "anchor": "blended",
    }


def _safe_float(value) -> float | None:
    if value is None:
        return None
    match = re.search(r"-?\d+(?:\.\d+)?", str(value).replace(",", "."))
    if not match:
        return None
    try:
        return float(match.group(0))
    except ValueError:
        return None


def _safe_round(value, digits: int = 1):
    try:
        return round(float(value), digits)
    except (TypeError, ValueError):
        return None


def _fold(value) -> str:
    return (
        str(value or "").strip().lower()
        .replace("ı", "i").replace("ş", "s")
        .replace("ğ", "g").replace("ü", "u")
        .replace("ö", "o").replace("ç", "c")
    )


def _normalize_direction(value) -> str:
    text = str(value or "").strip().upper().replace("UST", "ÜST")
    return "ÜST" if text == "ÜST" else "ALT"


def _opposite_direction(direction: str) -> str:
    return "ALT" if _normalize_direction(direction) == "ÜST" else "ÜST"


def _clamp(value: float, low: float, high: float) -> float:
    return max(low, min(high, value))


def _split_match_name(match_name: str) -> tuple[str, str]:
    parts = [part.strip() for part in (match_name or "").split(" - ", 1)]
    if len(parts) == 2:
        return parts[0], parts[1]
    return match_name or "Home", "Away"


def _extract_h2h_metrics(body_text: str, match_name: str) -> dict:
    text = re.sub(r"\s+", " ", body_text or "").strip()
    home_team, away_team = _split_match_name(match_name)
    text_lower = text.lower()
    h2h_source = ""
    h2h_quality_notes: list[str] = []

    # Matches any unicode dash/separator between words
    _DASH = r"[-–—\u2012\u2015]"

    if not text:
        h2h_quality_notes.append("H2H sayfa metni boş.")
    if "no data" in text_lower:
        h2h_quality_notes.append("AiScore H2H sayfası No data döndürdü.")
    if "invalid date" in text_lower:
        h2h_quality_notes.append("AiScore H2H sayfası geçersiz/eksik maç sayfası döndürdü.")
    if re.search(r"\bpts\s+0(?:\.0)?\s*[-–—]\s*0(?:\.0)?\s+per\s+game\b", text, re.IGNORECASE):
        h2h_quality_notes.append("H2H istatistik satırı 0-0 döndü; yok sayıldı.")

    def team_metrics(team_name: str) -> dict:
        if not team_name:
            return {}
        escaped = re.escape(team_name)
        # Unicode dash + hyphen variants
        pts_sep = r"[-–—\u2012\u2015]"
        # Suffix tolerates "per game", "/game", "pts/game", "points/game"
        per_game = r"(?:per\s*game|/\s*game|pts?/game|points?/game)"
        patterns = [
            # AiScore: "Last 5, Team 90.5 points per match, 88.2 opponent points per game"
            (
                rf"Last\s*5[,\s]+{escaped}.{{0,200}}"
                rf"(\d+(?:\.\d+)?)\s*points?\s*per\s*match[,\s]*"
                rf"(\d+(?:\.\d+)?)\s*opponent\s*points?\s*per\s*game.{{0,200}}"
                rf"Total\s*points?\s*over%[:\s]*(\d+(?:\.\d+)?)%",
                3,
            ),
            # AiScore format 1: "Team 6 Away This league ... pts 79.4 – 94.4 per game"
            (
                rf"{escaped}\s+\d+\s+(?:Home|Away|All).{{0,150}}"
                rf"pts\s+(\d+(?:\.\d+)?)\s*{pts_sep}\s*(\d+(?:\.\d+)?)\s+{per_game}",
                2,
            ),
            # AiScore format 2: "H2H 6 Home — Team This league ... pts 96.2 – 88.0 per game"
            (
                rf"H2H\s+\d+\s+(?:Home|Away|All).{{0,12}}{escaped}.{{0,200}}"
                rf"pts\s+(\d+(?:\.\d+)?)\s*{pts_sep}\s*(\d+(?:\.\d+)?)\s+{per_game}",
                2,
            ),
            # AiScore format 3: "Team ... pts 96.2 – 88.0 per game" (any proximity)
            (
                rf"{escaped}.{{0,250}}"
                rf"pts\s+(\d+(?:\.\d+)?)\s*{pts_sep}\s*(\d+(?:\.\d+)?)\s+{per_game}",
                2,
            ),
            # Loose fallback: "TeamName ... 90.5 - 88.2 per game" (no 'pts' marker)
            (
                rf"{escaped}.{{0,180}}"
                rf"(\d{{2,3}}(?:\.\d+)?)\s*{pts_sep}\s*(\d{{2,3}}(?:\.\d+)?)\s+{per_game}",
                2,
            ),
        ]
        for pattern, groups in patterns:
            m = re.search(pattern, text, re.IGNORECASE)
            if not m:
                continue
            if groups == 3:
                ppg, oppg, over_pct = float(m.group(1)), float(m.group(2)), float(m.group(3))
                return {"team": team_name, "ppg": ppg, "oppg": oppg,
                        "avg_total": round(ppg + oppg, 1), "over_pct": over_pct}
            ppg, oppg = float(m.group(1)), float(m.group(2))
            return {"team": team_name, "ppg": ppg, "oppg": oppg,
                    "avg_total": round(ppg + oppg, 1), "over_pct": None}
        return {}

    def recent_team_metrics(team_name: str) -> dict:
        """Strictly parse team recent/last-5 blocks, not H2H aggregate rows."""
        if not team_name:
            return {}
        escaped = re.escape(team_name)
        pts_sep = r"[-–—\u2012\u2015]"
        per_game = r"(?:per\s*game|/\s*game|pts?/game|points?/game)"
        patterns = [
            rf"(?:Last\s*5|Last\s*Matches|Recent\s*Form).{{0,220}}{escaped}.{{0,220}}"
            rf"(\d{{2,3}}(?:\.\d+)?)\s*points?\s*per\s*match[,\s]*"
            rf"(\d{{2,3}}(?:\.\d+)?)\s*opponent\s*points?\s*per\s*game.{{0,160}}"
            rf"(?:Total\s*points?\s*over%[:\s]*(\d+(?:\.\d+)?)%)?",
            rf"(?:Last\s*5|Last\s*Matches|Recent\s*Form).{{0,260}}{escaped}.{{0,260}}"
            rf"pts\s+(\d{{2,3}}(?:\.\d+)?)\s*{pts_sep}\s*(\d{{2,3}}(?:\.\d+)?)\s+"
            rf"{per_game}",
            # AiScore H2H page format: "TeamName N [chars] Home/Away [chars] pts X – Y per game"
            # \b(?:[1-9]|[12]\d)\b matches game counts (1-29) but NOT basketball scores (50+)
            # This prevents false matches where a match-row score is followed by the next
            # section's Home/Away heading within 80 chars.
            rf"{escaped}.{{0,10}}\b(?:[1-9]|[12]\d)\b.{{0,80}}?(?:Home|Away|All).{{0,250}}?"
            rf"pts\s+(\d{{2,3}}(?:\.\d+)?)\s*{pts_sep}\s*(\d{{2,3}}(?:\.\d+)?)\s+"
            rf"{per_game}",
        ]
        for pattern in patterns:
            m = re.search(pattern, text, re.IGNORECASE)
            if not m:
                continue
            ppg, oppg = float(m.group(1)), float(m.group(2))
            if ppg <= 0 or oppg <= 0:
                continue
            over_pct = float(m.group(3)) if m.lastindex and m.lastindex >= 3 and m.group(3) else None
            return {
                "team": team_name,
                "ppg": ppg,
                "oppg": oppg,
                "avg_total": round(ppg + oppg, 1),
                "over_pct": over_pct,
            }
        return {}

    # Multiple date formats AiScore renders depending on locale / page section.
    _MONTH_LONG = (
        r"January|February|March|April|May|June|"
        r"July|August|September|October|November|December"
    )
    _MONTH_SHORT = r"Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Sept|Oct|Nov|Dec"
    _DOW = r"Mon|Tue|Wed|Thu|Fri|Sat|Sun"
    date_patterns = [
        # "Monday, January 14, 2025"
        rf"(?:Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday),\s+"
        rf"(?:{_MONTH_LONG})\s+\d{{1,2}},\s+\d{{4}}",
        # "January 14, 2025" (no weekday)
        rf"(?:{_MONTH_LONG})\s+\d{{1,2}},\s+\d{{4}}",
        # "Mon, Jan 14, 2025" / "Mon Jan 14 2025" / "Jan 14, 2025"
        rf"(?:(?:{_DOW})[,.]?\s+)?(?:{_MONTH_SHORT})\.?\s+\d{{1,2}}(?:,)?\s+\d{{4}}",
        # "14 January 2025" / "14 Jan 2025"
        rf"\d{{1,2}}\s+(?:{_MONTH_LONG}|{_MONTH_SHORT})\.?\s+\d{{4}}",
        # ISO "2025-01-14" or "2025/01/14"
        r"\d{4}[-/]\d{1,2}[-/]\d{1,2}",
        # "14/01/2025" or "14.01.2025"
        r"\d{1,2}[./]\d{1,2}[./]\d{2,4}",
    ]
    date_re = "(?:" + "|".join(date_patterns) + ")"

    def _team_in_chunk(chunk: str, team_name: str) -> bool:
        """Loose containment: case-insensitive + partial-on-spaces match."""
        if not chunk or not team_name:
            return False
        if team_name in chunk:
            return True
        low_chunk = chunk.lower()
        low_team = team_name.lower()
        if low_team in low_chunk:
            return True
        # Loose: every space-token in the team name appears in chunk
        tokens = [t for t in re.split(r"\s+", low_team) if len(t) >= 3]
        if tokens and all(t in low_chunk for t in tokens):
            return True
        return False

    def _team_pos(haystack: str, team_name: str) -> int:
        low_haystack = (haystack or "").lower()
        low_team = (team_name or "").lower()
        if not low_haystack or not low_team:
            return -1
        pos = low_haystack.find(low_team)
        if pos >= 0:
            return pos
        tokens = [t for t in re.split(r"\s+", low_team) if len(t) >= 3]
        positions = [low_haystack.find(t) for t in tokens]
        positions = [p for p in positions if p >= 0]
        return min(positions) if positions else -1

    def _score_row_for_team(chunk: str, team_name: str, opponent_name: str | None = None) -> dict | None:
        if not _team_in_chunk(chunk, team_name):
            return None
        score_matches = list(re.finditer(r"\b(\d{2,3})\b", chunk))
        score_nums = [m for m in score_matches if 40 <= int(m.group(1)) <= 180]
        if len(score_nums) < 2:
            return None
        # Prefer the first basketball-score pair after the date. Using the last
        # pair leaks the next row in AiScore's compact, single-line text.
        first, second = score_nums[0], score_nums[1]
        score_a, score_b = int(first.group(1)), int(second.group(1))
        before_scores = chunk[:first.start()]
        after_scores = chunk[second.end():]
        team_left = _team_pos(before_scores, team_name)
        team_right = _team_pos(after_scores, team_name)
        opp_left = _team_pos(before_scores, opponent_name or "")
        opp_right = _team_pos(after_scores, opponent_name or "")

        if team_left >= 0 and opp_left >= 0:
            is_first_team = team_left < opp_left
        elif team_right >= 0 and opp_right >= 0:
            # Less common layout: "80 76 Team A Team B".
            is_first_team = team_right < opp_right
        elif team_left >= 0 and team_right < 0:
            is_first_team = True
        elif team_right >= 0 and team_left < 0:
            is_first_team = False
        else:
            return None

        team_score = score_a if is_first_team else score_b
        opp_score = score_b if is_first_team else score_a
        if not (40 <= team_score <= 180 and 40 <= opp_score <= 180):
            return None
        return {"for": team_score, "against": opp_score, "total": team_score + opp_score}

    def _is_mutual_score_chunk(chunk: str, left_team: str, right_team: str) -> bool:
        score_matches = list(re.finditer(r"\b(\d{2,3})\b", chunk))
        score_nums = [m for m in score_matches if 40 <= int(m.group(1)) <= 180]
        if len(score_nums) < 2:
            return False
        first, second = score_nums[0], score_nums[1]
        before_scores = chunk[:first.start()]
        after_scores = chunk[second.end():]
        both_before = _team_pos(before_scores, left_team) >= 0 and _team_pos(before_scores, right_team) >= 0
        both_after = _team_pos(after_scores, left_team) >= 0 and _team_pos(after_scores, right_team) >= 0
        return both_before or both_after

    def _rows_from_date_range(range_text: str, team_name: str, opponent_name: str | None = None,
                              allow_mutual_rows: bool = True) -> list[dict]:
        rows: list[dict] = []
        seen_keys: set[tuple] = set()
        date_matches = list(re.finditer(date_re, range_text, re.IGNORECASE))
        for idx, match in enumerate(date_matches):
            start = match.end()
            end = date_matches[idx + 1].start() if idx + 1 < len(date_matches) else min(len(range_text), start + 320)
            chunk = range_text[start:end]
            if not allow_mutual_rows and opponent_name and _team_in_chunk(chunk, opponent_name):
                continue
            parsed = _score_row_for_team(chunk, team_name, opponent_name)
            if not parsed:
                continue
            key = (match.group(0), parsed["for"], parsed["against"])
            if key in seen_keys:
                continue
            seen_keys.add(key)
            rows.append({
                "date": match.group(0),
                **parsed,
            })
            if len(rows) >= 6:
                break
        return rows

    def _team_recent_sections(team_name: str) -> list[str]:
        if not text or not team_name:
            return []
        escaped = re.escape(team_name)
        section_start_patterns = [
            rf"(?:Last|Latest|Recent)\s+(?:\d+\s+)?(?:Matches|Games|Form).{{0,140}}?{escaped}",
            rf"{escaped}.{{0,40}}(?:Last|Latest|Recent)\s+(?:\d+\s+)?(?:Matches|Games|Form)",
            rf"{escaped}\s+\d{{1,2}}\s+(?:All|Home|Away|This\s+league)",
        ]
        boundary_patterns = [
            r"\bH2H\b",
            rf"{re.escape(home_team)}\s+\d{{1,2}}\s+(?:All|Home|Away|This\s+league)",
            rf"{re.escape(away_team)}\s+\d{{1,2}}\s+(?:All|Home|Away|This\s+league)",
            rf"(?:Last|Latest|Recent)\s+(?:\d+\s+)?(?:Matches|Games|Form).{{0,140}}?(?:{re.escape(home_team)}|{re.escape(away_team)})",
        ]
        sections: list[str] = []
        for pattern in section_start_patterns:
            for start_match in re.finditer(pattern, text, re.IGNORECASE):
                start = start_match.start()
                end = min(len(text), start + 2600)
                for boundary_pattern in boundary_patterns:
                    for boundary in re.finditer(boundary_pattern, text[start_match.end():end], re.IGNORECASE):
                        candidate_end = start_match.end() + boundary.start()
                        if candidate_end > start + 80:
                            end = min(end, candidate_end)
                            break
                section = text[start:end]
                if re.search(date_re, section, re.IGNORECASE):
                    sections.append(section)
        return sections

    def team_recent_form_from_match_rows(team_name: str, opponent_name: str | None = None) -> dict:
        """Takımın oynadığı son maçları çek (rakip kim olursa olsun).
        H2H bölümündeki karşılıklı maçları SF diye kullanma; önce takımın
        kendi son maç bölümünü, yoksa karşılıklı olmayan genel satırları dene."""
        if not text or not team_name:
            return {}
        section_rows: list[dict] = []
        for section in _team_recent_sections(team_name):
            rows = _rows_from_date_range(section, team_name, opponent_name, allow_mutual_rows=True)
            if len(rows) > len(section_rows):
                section_rows = rows

        rows = section_rows
        if len(rows) < 3:
            # Last-resort global scan, but do not let H2H rows masquerade as SF.
            rows = _rows_from_date_range(text, team_name, opponent_name, allow_mutual_rows=False)

        if len(rows) < 3:
            return {}
        rows = rows[:6]

        ppg = round(mean(g["for"] for g in rows), 1)
        oppg = round(mean(g["against"] for g in rows), 1)
        avg_total = round(ppg + oppg, 1)
        label = f"{round(ppg)}+{round(oppg)} = {round(avg_total)}"
        return {
            "team": team_name,
            "games": len(rows),
            "source_label": f"Son {len(rows)} maç",
            "ppg": ppg,
            "oppg": oppg,
            "avg_total": avg_total,
            "label": label,
            "scores": rows,
            "source": "match_rows",
        }

    def team_recent_form_with_fallbacks(team_name: str, opponent_name: str | None = None) -> dict:
        """Match-rows first; if we can't extract enough rows, fall back to
        the AiScore 'Last 5' stats line. H2H aggregate rows are not SF."""
        primary = team_recent_form_from_match_rows(team_name, opponent_name)
        if primary:
            return primary
        # Fallback 1: "Last 5 ... 90.5 points per match, 88.2 opponent points"
        recent = recent_team_metrics(team_name)
        if recent and recent.get("ppg") and recent.get("oppg"):
            ppg = round(float(recent["ppg"]), 1)
            oppg = round(float(recent["oppg"]), 1)
            avg_total = round(ppg + oppg, 1)
            return {
                "team": team_name,
                "games": 5,
                "source_label": "Son 5 maç",
                "ppg": ppg,
                "oppg": oppg,
                "avg_total": avg_total,
                "over_pct": recent.get("over_pct"),
                "label": f"{round(ppg)}+{round(oppg)} = {round(avg_total)}",
                "scores": [],
                "source": "last5_stats",
            }
        return {}

    def h2h_totals_from_match_rows() -> list[int]:
        if not text:
            return []
        totals: list[int] = []
        date_matches = list(re.finditer(date_re, text, re.IGNORECASE))
        seen: set[tuple] = set()
        for idx, match in enumerate(date_matches):
            start = match.end()
            end = date_matches[idx + 1].start() if idx + 1 < len(date_matches) else min(len(text), start + 320)
            chunk = text[start:end]
            if home_team and away_team and (not _team_in_chunk(chunk, home_team) or not _team_in_chunk(chunk, away_team)):
                continue
            if not _is_mutual_score_chunk(chunk, home_team, away_team):
                continue
            parsed = _score_row_for_team(chunk, home_team, away_team)
            if parsed:
                total = parsed["total"]
                key = (match.group(0), total)
                if 100 <= total <= 320 and key not in seen:
                    seen.add(key)
                    totals.append(total)
        return totals[:12]

    h2h_games = None
    h2h_avg_total = None
    h2h_over_pct = None

    # H2H over % — multiple phrasings
    over_match = (
        re.search(r"(?:Past\s+)?H2H\s+Results.{0,300}Total\s+Points?\s+Over%[:\s]*(\d+(?:\.\d+)?)%", text, re.IGNORECASE)
        or re.search(r"Total\s+Points?\s+Over%[:\s]*(\d+(?:\.\d+)?)%", text, re.IGNORECASE)
        or re.search(r"Over\s*(?:Under\s*)?%[:\s]*(\d+(?:\.\d+)?)%", text, re.IGNORECASE)
    )
    if over_match:
        h2h_over_pct = float(over_match.group(1))

    # H2H game count
    games_match = (
        re.search(r"Total\s+Matches[:\s]+(\d+)", text, re.IGNORECASE)
        or re.search(r"\bH2H\b[:\s]+(\d+)\b", text, re.IGNORECASE)
        or re.search(r"(?:head.to.head|h2h)\D{0,20}(\d{1,3})\s*(?:games?|matches?)", text, re.IGNORECASE)
    )
    if games_match:
        h2h_games = int(games_match.group(1))

    # H2H average total from past meetings
    # Handles: "H2H 6 Home — Team This league ... pts 96.2 – 88.0 per game"
    #       or: "H2H ... Home - Team ... pts X – Y per game"
    pts_sep = r"[-–—\u2012\u2015]"
    per_game = r"(?:per\s*game|/\s*game|pts?/game|points?/game)"
    if home_team:
        h2h_home_pattern = (
            rf"\bH2H\b.{{0,250}}Home.{{0,12}}{re.escape(home_team)}.{{0,250}}"
            rf"pts\s+(\d+(?:\.\d+)?)\s*{pts_sep}\s*(\d+(?:\.\d+)?)\s+{per_game}"
        )
        h2h_m = re.search(h2h_home_pattern, text, re.IGNORECASE)
        if h2h_m:
            left, right = float(h2h_m.group(1)), float(h2h_m.group(2))
            if left > 0 and right > 0:
                h2h_avg_total = round(left + right, 1)
                h2h_source = "stats_row"

    # Generic H2H average total fallback
    if h2h_avg_total is None:
        generic = (
            re.search(r"\bH2H\b.{0,60}avg(?:erage)?\D{0,10}(\d{3}(?:\.\d+)?)", text, re.IGNORECASE)
            or re.search(r"average\s+total[:\s]*(\d{3}(?:\.\d+)?)", text, re.IGNORECASE)
        )
        if generic:
            h2h_avg_total = float(generic.group(1))
            h2h_source = "average_text"

    h2h_row_totals = h2h_totals_from_match_rows()
    if h2h_avg_total is None and h2h_row_totals:
        h2h_avg_total = round(mean(h2h_row_totals), 1)
        h2h_source = "score_rows"
        if h2h_games is None:
            h2h_games = len(h2h_row_totals)
    if h2h_games is None and h2h_row_totals:
        h2h_games = len(h2h_row_totals)

    home_last6 = team_recent_form_with_fallbacks(home_team, away_team)
    away_last6 = team_recent_form_with_fallbacks(away_team, home_team)
    if (
        home_last6 and away_last6
        and home_last6.get("source") == "match_rows"
        and away_last6.get("source") == "match_rows"
        and home_last6.get("scores") == away_last6.get("scores")
    ):
        h2h_quality_notes.append("SF satırları iki takım için aynı okundu; H2H/SF karışmasını önlemek için son form kullanılmadı.")
        home_last6 = {}
        away_last6 = {}
    home = home_last6
    away = away_last6
    expected_total = None
    if home and away:
        # Cross-pair: home'un attığı + away'in yediği ≈ home skoru beklentisi (ve tersi).
        # Ham mean([home_avg, away_avg]) yerine bu karşılıklı eşleştirme daha doğrudur.
        home_expected = (home["ppg"] + away["oppg"]) / 2
        away_expected = (away["ppg"] + home["oppg"]) / 2
        expected_total = round(home_expected + away_expected, 1)
    elif home and home.get("avg_total") is not None:
        expected_total = round(float(home["avg_total"]), 1)
    elif away and away.get("avg_total") is not None:
        expected_total = round(float(away["avg_total"]), 1)

    def _games_count(profile: dict) -> int | None:
        games = profile.get("games") if profile else None
        try:
            return int(games) if games is not None else None
        except (TypeError, ValueError):
            return None

    home_games = _games_count(home_last6)
    away_games = _games_count(away_last6)
    if not home_last6:
        h2h_quality_notes.append(f"{home_team} için son 3-4-5 maç verisi alınamıyor; takım form ortalaması kullanılmadı.")
    elif home_games is not None and home_games < 6:
        h2h_quality_notes.append(f"{home_team} için son 6 yerine son {home_games} maç ortalaması kullanıldı.")
    if not away_last6:
        h2h_quality_notes.append(f"{away_team} için son 3-4-5 maç verisi alınamıyor; takım form ortalaması kullanılmadı.")
    elif away_games is not None and away_games < 6:
        h2h_quality_notes.append(f"{away_team} için son 6 yerine son {away_games} maç ortalaması kullanıldı.")
    if h2h_avg_total is None:
        h2h_quality_notes.append("H2H ortalama toplam çıkarılamadı.")

    quality_score = 100
    if not text:
        quality_score -= 60
    if h2h_avg_total is None:
        quality_score -= 35
    if expected_total is None:
        quality_score -= 20
    if h2h_games is not None and h2h_games < 3:
        quality_score -= 20
    if "no data" in text_lower or "invalid date" in text_lower:
        quality_score -= 25
    quality_score = max(0, min(100, quality_score))

    return {
        "home_last5": home,
        "away_last5": away,
        "home_last6": home_last6,
        "away_last6": away_last6,
        "expected_total": expected_total,
        "h2h_avg_total": h2h_avg_total,
        "h2h_over_pct": h2h_over_pct,
        "h2h_games": h2h_games,
        "h2h_source": h2h_source,
        "h2h_row_totals": h2h_row_totals,
        "h2h_body_chars": len(text),
        "h2h_quality_score": quality_score,
        "h2h_quality_notes": h2h_quality_notes,
    }


def _team_profile_label(avg_total: float | None, over_pct: float | None, games: int | None = None) -> str:
    if avg_total is None:
        return "veri yok"
    game_label = f"son {games} maç" if games else "son maçlar"
    label = f"{game_label} toplam ort {avg_total:.1f}"
    if over_pct is None:
        return label
    if over_pct >= 60:
        return f"{label} · over eğilimli"
    if over_pct <= 40:
        return f"{label} · under eğilimli"
    return f"{label} · dengeli"


def build_team_context(h2h_metrics: dict, live: float, direction: str) -> dict | None:
    home = h2h_metrics.get("home_last6") or h2h_metrics.get("home_last5") or {}
    away = h2h_metrics.get("away_last6") or h2h_metrics.get("away_last5") or {}
    if not home and not away and h2h_metrics.get("h2h_avg_total") is None and h2h_metrics.get("h2h_over_pct") is None:
        return None

    expected = h2h_metrics.get("expected_total")
    game_counts = sorted({
        int(games)
        for games in (home.get("games"), away.get("games"))
        if games is not None and int(games) >= 3
    }, reverse=True)
    form_label = (
        f"Son {'/'.join(str(g) for g in game_counts)} maç ortalaması"
        if game_counts else "Son form ortalaması"
    )
    regression_direction = None
    regression_note = None
    regression_delta = None
    if expected is not None:
        regression_delta = round(live - expected, 1)
        if regression_delta >= 4:
            regression_direction = "ALT"
            regression_note = f"{form_label} {expected:.1f}, canlı barem {live:.1f} (+{regression_delta:.1f}). Ortalamaya dönüş ALT tarafını destekler."
        elif regression_delta <= -4:
            regression_direction = "ÜST"
            regression_note = f"{form_label} {expected:.1f}, canlı barem {live:.1f} ({regression_delta:.1f}). Ortalamaya dönüş ÜST tarafını destekler."
        else:
            regression_note = f"{form_label} {expected:.1f}, canlı barem {live:.1f} ({regression_delta:+.1f}). Barem tarihsel profile yakın."

    def profile(raw: dict) -> dict | None:
        if not raw:
            return None
        return {
            "team": raw.get("team", "-"),
            "games": raw.get("games"),
            "source_label": raw.get("source_label"),
            "avg_total": raw.get("avg_total"),
            "ppg": raw.get("ppg"),
            "oppg": raw.get("oppg"),
            "over_pct": raw.get("over_pct"),
            "label": raw.get("label") or _team_profile_label(raw.get("avg_total"), raw.get("over_pct"), raw.get("games")),
            "scores": raw.get("scores") or [],
        }

    h2h_note = None
    h2h_games = h2h_metrics.get("h2h_games")
    h2h_over = h2h_metrics.get("h2h_over_pct")
    h2h_avg = h2h_metrics.get("h2h_avg_total")
    if h2h_over is not None:
        game_part = f"{int(h2h_games)} maç" if h2h_games else "geçmiş maçlar"
        if h2h_over >= 60:
            tilt = "ÜST tarafına eğilimli"
        elif h2h_over <= 40:
            tilt = "ALT tarafına eğilimli"
        else:
            tilt = "dengeli"
        h2h_note = f"Karşılaşma geçmişi ({game_part}): %{h2h_over:.0f} over — {tilt}."
    elif h2h_avg is not None:
        game_part = f"{int(h2h_games)} maç" if h2h_games else "geçmiş maçlar"
        h2h_note = f"Karşılaşma geçmişi ({game_part}): ortalama toplam {h2h_avg:.1f}."

    support_points = 0
    against_points = 0
    if regression_direction:
        if regression_direction == direction:
            support_points += 1
        else:
            against_points += 1
    if h2h_over is not None:
        if direction == "ÜST" and h2h_over >= 60:
            support_points += 1
        elif direction == "ALT" and h2h_over <= 40:
            support_points += 1
        elif direction == "ÜST" and h2h_over <= 40:
            against_points += 1
        elif direction == "ALT" and h2h_over >= 60:
            against_points += 1

    if support_points == 0 and against_points == 0:
        alignment, alignment_code = "Takım profili nötr", "neutral"
    elif support_points > against_points:
        alignment, alignment_code = f"Takım profili {direction} sinyalini destekliyor", "support"
    elif against_points > support_points:
        alignment, alignment_code = f"Takım profili {direction} sinyaline karşı", "against"
    else:
        alignment, alignment_code = "Takım profili karışık sinyal veriyor", "mixed"

    return {
        "expected_total": expected,
        "regression_delta": regression_delta,
        "regression_direction": regression_direction,
        "regression_note": regression_note,
        "home_profile": profile(home),
        "away_profile": profile(away),
        "home_last6_profile": profile(h2h_metrics.get("home_last6") or {}),
        "away_last6_profile": profile(h2h_metrics.get("away_last6") or {}),
        "h2h_games": h2h_games,
        "h2h_over_pct": h2h_over,
        "h2h_avg_total": h2h_avg,
        "h2h_note": h2h_note,
        "alignment": alignment,
        "alignment_code": alignment_code,
        "signal_direction": direction,
    }


def _market_total(match: dict, opening: float) -> float | None:
    odds = match.get("odds_snapshot") if isinstance(match.get("odds_snapshot"), dict) else {}
    for key in ("opening_total", "opening", "baseline"):
        parsed = _safe_float(match.get(key))
        if parsed is not None:
            return round(parsed, 1)
    for value in (
        odds.get("opening_median"),
        odds.get("prematch_median"),
    ):
        parsed = _safe_float(value)
        if parsed is not None:
            return round(parsed, 1)
    for key in ("prematch_total", "prematch"):
        value = match.get(key)
        parsed = _safe_float(value)
        if parsed is not None:
            return round(parsed, 1)
    return round(opening, 1)


# Eski çoklu-bileşenli (projection + market + team_recent + h2h) ağırlıklı adil
# barem hesaplama yardımcıları kaldırıldı. Yeni mimari:
#   calculate_fair_line(prematch, pure_pace_projection, elapsed_minutes, ...)
# H2H ve son form artık fair_line'a girmiyor — prematch çizgisi onları zaten fiyatlamıştır.


def _pace_note(
    projected_total: float | None,
    live: float,
    pace_data: dict | None = None,
    projection_components: dict | None = None,
    h2h_metrics: dict | None = None,
) -> str:
    """
    Maç hızını yorumlar. Regresyon uygulanmış projeksiyon kullanır:
    Q1/Q2'deki yüksek tempo doğrusal değil, ortalamaya dönüş varsayımıyla hesaplanır.
    pace_data sağlanmışsa çeyrek bazlı anomali notu da eklenir.
    """
    anomaly_note = (pace_data or {}).get("pace_note", "")

    projection_components = projection_components or {}
    h2h_metrics = h2h_metrics or {}

    if projected_total is None:
        base = "Maç içi projeksiyon hesaplanamadı; tempo yorumu için veri yetersiz."
        return f"{base} {anomaly_note}".strip() if anomaly_note else base

    gap = round(projected_total - live, 1)
    current_pace = projection_components.get("current_pace_per_min")
    sustainable_pace = projection_components.get("sustainable_pace_per_min")
    stats_adj = projection_components.get("stats_adjustment")
    script_adj = projection_components.get("script_adjustment")
    history_total = h2h_metrics.get("expected_total") or h2h_metrics.get("h2h_avg_total")
    history_gap = round(float(history_total) - live, 1) if history_total is not None else None

    sustainability = ""
    if current_pace is not None and sustainable_pace is not None:
        pace_gap = float(current_pace) - float(sustainable_pace)
        if pace_gap >= 0.35:
            sustainability = " Mevcut tempo tamamlanan çeyrek ortalamasının üstünde; sürdürülebilirlik için regresyon kontrolü gerekir."
        elif pace_gap <= -0.35:
            sustainability = " Mevcut tempo tamamlanan çeyrek ortalamasının altında; normalleşme/hızlanma ihtimali var."
        else:
            sustainability = " Mevcut tempo çeyrek ortalamasıyla uyumlu; ani regresyon sinyali zayıf."

    context_bits = []
    if stats_adj is not None and abs(float(stats_adj)) >= 2:
        context_bits.append(f"istatistik düzeltmesi {float(stats_adj):+.1f}")
    if script_adj is not None and abs(float(script_adj)) >= 2:
        context_bits.append(f"maç scripti {float(script_adj):+.1f}")
    if history_gap is not None and abs(history_gap) >= 6:
        side = "daha yüksek toplam profili" if history_gap > 0 else "daha düşük toplam profili"
        context_bits.append(f"takım/H2H {side} ({history_gap:+.1f})")
    context = f" Destekleyen/dengeleyen unsur: {', '.join(context_bits)}." if context_bits else ""

    if gap >= 6:
        base = (
            f"Canlı projeksiyon {projected_total:.1f}; baremin {gap:.1f} üstünde. "
            f"Bu otomatik ÜST demek değil; tempo sürdürülebilirliği, şut/faul trafiği ve takım profiliyle doğrulanmalı."
        )
    elif gap <= -6:
        base = (
            f"Canlı projeksiyon {projected_total:.1f}; baremin {abs(gap):.1f} altında. "
            f"Bu otomatik ALT demek değil; düşük tempo normalleşebilir veya maç scripti hızlanma yaratabilir."
        )
    else:
        base = f"Canlı projeksiyon {projected_total:.1f}; bareme yakın. Net tempo avantajı yok."

    note = f"{base}{sustainability}{context}"
    return f"{note} {anomaly_note}".strip() if anomaly_note else note


def _script_pace_adjustment(score: str, status: str, match_name: str, tournament: str) -> float:
    """Skor farkı ve periyot durumuna göre projeksiyona çarpan döndürür.
    1.0 = değişiklik yok. 0.85 = pace %15 düşer (blowout). 1.10 = pace %10 artar (faul oyunu)."""
    clock = game_clock(status, match_name, tournament)
    period = clock["period"]
    remaining_min = clock["remaining_min"]
    home_score, away_score = parse_score(score)
    if home_score is None or away_score is None or period is None:
        return 1.0
    gap = abs(home_score - away_score)
    if period >= 4 and remaining_min is not None and remaining_min <= 6:
        if gap >= 18:
            return 0.85
        if gap >= 12:
            return 0.92
        if gap <= 6:
            return 1.08
    elif period >= 3 and gap >= 20:
        return 0.93
    return 1.0


def _script_warning(score: str, status: str, match_name: str, tournament: str) -> str:
    clock = game_clock(status, match_name, tournament)
    period = clock["period"]
    remaining_min = clock["remaining_min"]
    home_score, away_score = parse_score(score)
    if home_score is None or away_score is None:
        return "Skor okunamadı; maç script uyarısı sınırlı."
    score_gap = abs(home_score - away_score)
    if score_gap >= 18 and period == 4 and remaining_min is not None and remaining_min <= 5:
        return f"Blowout/garbage time riski: fark {score_gap}, son bölümde tempo düşebilir."
    if score_gap >= 15:
        return f"Blowout riski: fark {score_gap}; önde olan takım tempoyu düşürebilir."
    if period == 4 and remaining_min is not None and remaining_min <= 6 and score_gap <= 8:
        return f"Yakın maç riski: fark {score_gap}; faul oyunu ve ekstra pozisyonlar maçı hızlandırabilir."
    return "Maç scriptinde belirgin ekstra risk yok."


def _script_signal(
    score: str,
    status: str,
    match_name: str,
    tournament: str,
    projected_gap: float | None = None,
) -> dict | None:
    clock = game_clock(status, match_name, tournament)
    period = clock["period"]
    remaining_min = clock["remaining_min"]
    home_score, away_score = parse_score(score)
    if home_score is None or away_score is None or period is None:
        return None

    gap = abs(home_score - away_score)
    if period >= 4 and remaining_min is not None and remaining_min <= 5 and gap >= 16:
        return {
            "direction": "ALT",
            "strength": 16,
            "confidence": 82,
            "reason": f"Q4 son bölüm blowout: fark {gap}; rotasyon ve hücum kalitesi düşebilir.",
            "pace_state": "tempo_drop",
        }
    if period >= 4 and remaining_min is not None and remaining_min <= 6 and gap <= 7:
        return {
            "direction": "ÜST",
            "strength": 14,
            "confidence": 76,
            "reason": f"Q4 yakın maç: fark {gap}; faul oyunu/erken hücumlar toplamı yukarı iter.",
            "pace_state": "tempo_rise",
        }
    if period >= 3 and gap >= 16:
        if projected_gap is not None and projected_gap >= 6 and not (period >= 4 and remaining_min is not None and remaining_min <= 6):
            return {
                "direction": "ÜST",
                "strength": 15,
                "confidence": 74,
                "reason": f"Maç kopuyor ama tempo yukarıda: fark {gap}, projeksiyon canlıdan {projected_gap:.1f} yüksek.",
                "pace_state": "runaway_rise",
            }
        return {
            "direction": "ALT",
            "strength": 11,
            "confidence": 68,
            "reason": f"Kopma riski: fark {gap}; öndeki takım set hücumuna ve süre eritmeye dönebilir.",
            "pace_state": "blowout_risk",
        }
    return None


def _period_key(status: str, alert_moment: str = "") -> str:
    source = alert_moment or status or ""
    s = _fold(source)
    if not s:
        return "period:unknown"
    if "devre arasi" in s or re.search(r"\bht\b", s):
        return "period:q2_ht"
    if "q1" in s or re.search(r"(^|\D)1(?:q|\.|c|st|\s*-)", s):
        return "period:q1"
    if "q2" in s or re.search(r"(^|\D)2(?:q|\.|c|nd|\s*-)", s):
        return "period:q2_ht"
    if "q3" in s or re.search(r"(^|\D)3(?:q|\.|c|rd|\s*-)", s):
        return "period:q3"
    if "q4" in s or re.search(r"(^|\D)4(?:q|\.|c|th|\s*-)", s):
        return "period:q4"
    return "period:unknown"


def _diff_key(diff) -> str:
    value = abs(_safe_float(diff) or 0)
    if value < 12:
        return "diff:10_12"
    if value < 16:
        return "diff:12_16"
    if value < 20:
        return "diff:16_20"
    return "diff:20_plus"


def _edge_key(fair_edge) -> str:
    edge = _safe_float(fair_edge)
    if edge is None:
        return "fair:none"
    if edge <= -6:
        return "fair:alt_strong"
    if edge < -2:
        return "fair:alt_mild"
    if edge < 2:
        return "fair:neutral"
    if edge < 6:
        return "fair:ust_mild"
    return "fair:ust_strong"


def _projected_gap_key(projected_total, live) -> str:
    projected = _safe_float(projected_total)
    live_total = _safe_float(live)
    if projected is None or live_total is None:
        return "projection:none"
    gap = projected - live_total
    if gap <= -8:
        return "projection:alt_strong"
    if gap < -3:
        return "projection:alt_mild"
    if gap < 3:
        return "projection:neutral"
    if gap < 8:
        return "projection:ust_mild"
    return "projection:ust_strong"


def _signal_count_key(value) -> str:
    try:
        count = int(value or 1)
    except (TypeError, ValueError):
        count = 1
    return "signal:first" if count <= 1 else "signal:repeat"


def _tournament_key(value) -> str | None:
    cleaned = re.sub(r"\s+", " ", str(value or "").strip().lower())
    if not cleaned:
        return None
    return f"tournament:{cleaned[:80]}"


def _backtest_keys_from_values(
    *,
    legacy_direction: str,
    diff,
    status: str = "",
    alert_moment: str = "",
    fair_edge=None,
    projected_total=None,
    live=None,
    signal_count=1,
    tournament: str = "",
) -> list[str]:
    keys = [
        "all",
        f"legacy:{_normalize_direction(legacy_direction)}",
        _period_key(status, alert_moment),
        _diff_key(diff),
        _edge_key(fair_edge),
        _projected_gap_key(projected_total, live),
        _signal_count_key(signal_count),
    ]
    t_key = _tournament_key(tournament)
    if t_key:
        keys.append(t_key)
    return keys


def build_backtest_profile(rows: list[dict] | None) -> dict:
    """
    Silinen ve sonucu belli sinyallerden fade edilebilir bir profil çıkarır.
    Her bucket için iki aday yön de skorlanır: aynı yöndeki başarılar + ters yöndeki
    başarısızlıklar, aday yönün kazanımı sayılır.
    """
    buckets: dict[str, dict[str, dict[str, int]]] = {}
    resolved = 0

    def add_bucket(key: str, row_direction: str, success: bool) -> None:
        bucket = buckets.setdefault(
            key,
            {
                "ALT": {"wins": 0, "total": 0},
                "ÜST": {"wins": 0, "total": 0},
            },
        )
        for candidate in ("ALT", "ÜST"):
            bucket[candidate]["total"] += 1
            candidate_won = (
                (row_direction == candidate and success)
                or (row_direction != candidate and not success)
            )
            if candidate_won:
                bucket[candidate]["wins"] += 1

    for row in rows or []:
        result = _fold(row.get("result"))
        if result not in {"basarili", "basarisiz"}:
            continue
        resolved += 1
        direction = _normalize_direction(row.get("direction"))
        success = result == "basarili"
        analysis = row.get("analysis")
        if not isinstance(analysis, dict):
            try:
                analysis = json.loads(row.get("ai_analysis") or "{}")
            except Exception:
                analysis = {}
        fair_edge = analysis.get("fair_edge", row.get("fair_edge"))
        projected = analysis.get("projected_total", row.get("projected"))
        keys = _backtest_keys_from_values(
            legacy_direction=analysis.get("legacy_direction") or direction,
            diff=row.get("diff"),
            status=row.get("status") or "",
            alert_moment=row.get("alert_moment") or "",
            fair_edge=fair_edge,
            projected_total=projected,
            live=row.get("live"),
            signal_count=row.get("signal_count") or 1,
            tournament=row.get("tournament") or "",
        )
        for key in keys:
            add_bucket(key, direction, success)

    return {"sample_size": resolved, "buckets": buckets}


def _bucket_rate(profile: dict | None, key: str, direction: str) -> dict | None:
    if not profile:
        return None
    bucket = (profile.get("buckets") or {}).get(key) or {}
    stats = bucket.get(_normalize_direction(direction))
    if not stats or int(stats.get("total") or 0) <= 0:
        return None
    total = int(stats.get("total") or 0)
    wins = int(stats.get("wins") or 0)
    return {"key": key, "wins": wins, "total": total, "rate": round((wins / total) * 100, 1)}


def _backtest_direction_scores(profile: dict | None, keys: list[str]) -> dict:
    weights = {
        "all": 0.8,
        "legacy": 1.1,
        "period": 0.9,
        "diff": 1.1,
        "fair": 1.35,
        "projection": 1.35,
        "signal": 0.75,
        "tournament": 0.85,
    }
    result = {}
    for direction in ("ALT", "ÜST"):
        weighted_sum = 0.0
        weight_sum = 0.0
        samples = 0
        used = []
        for key in keys:
            stats = _bucket_rate(profile, key, direction)
            if not stats:
                continue
            total = stats["total"]
            if total < 8 and not key.startswith("all"):
                continue
            prefix = key.split(":", 1)[0]
            weight = weights.get(prefix, 0.7) * _clamp(total / 60, 0.35, 1.4)
            weighted_sum += stats["rate"] * weight
            weight_sum += weight
            samples += total
            used.append(stats)
        rate = round(weighted_sum / weight_sum, 1) if weight_sum > 0 else None
        result[direction] = {"rate": rate, "samples": samples, "buckets": used[:7]}
    return result


def _classify_signal(decision: dict) -> dict:
    """
    Telegram gönderim filtresi + insan-okunur açıklama döndürür.
    Sayısal puan ya da sınıf etiketi üretmez (kullanıcı isteğiyle kaldırıldı).

    Filtre mantığı: Adil baremin canlıya göre net farkı (|fair_edge| >= 5) ve
    yön kararıyla uyumlu olduğunda Telegram'a uygun. Diğer durumlarda dashboard'da
    görünür kalır ama mesaj atılmaz.
    """
    scores = decision.get("signal_scores") or {}
    margin = abs(float(scores.get("ALT", 0) or 0) - float(scores.get("ÜST", 0) or 0))
    final_direction = _normalize_direction(decision.get("direction"))
    legacy_direction = _normalize_direction(decision.get("legacy_direction") or final_direction)
    fair_edge = _safe_float(decision.get("fair_edge"))
    projected_gap = _safe_float(decision.get("projected_gap"))

    fair_direction = None
    if fair_edge is not None and abs(fair_edge) >= 3:
        fair_direction = "ÜST" if fair_edge > 0 else "ALT"
    projection_direction = None
    if projected_gap is not None and abs(projected_gap) >= 4:
        projection_direction = "ÜST" if projected_gap > 0 else "ALT"

    fair_edge_abs = abs(fair_edge) if fair_edge is not None else 0.0
    fair_aligned = fair_direction == final_direction
    projection_aligned = projection_direction == final_direction
    flipped = final_direction != legacy_direction

    # Telegram'a sadece adil barem net şekilde sapmışsa ve yönle uyumluysa çık.
    telegram_eligible = (
        fair_edge_abs >= 5.0
        and fair_aligned
        and (projection_aligned or projection_direction is None)
        and margin >= 3.0
    )

    if telegram_eligible:
        reason = (
            f"Adil barem canlıdan {fair_edge_abs:.1f} puan {'yüksek' if fair_edge > 0 else 'düşük'}; "
            f"yön {final_direction} ile uyumlu."
        )
        if flipped:
            reason += f" Karar eski yönü çevirdi ({legacy_direction}→{final_direction})."
    else:
        if fair_edge is None:
            reason = "Adil barem hesaplanamadı; gönderim için yeterli güven yok."
        elif fair_edge_abs < 5.0:
            reason = (
                f"Adil barem canlıya çok yakın ({fair_edge_abs:.1f} puan); "
                "net değer alanı zayıf."
            )
        elif not fair_aligned:
            reason = (
                "Adil barem yönü ile karar yönü uyumsuz; sinyal güvenilir değil."
            )
        else:
            reason = (
                f"Taraf ayrışması zayıf ({margin:.1f}); gönderim için kademe eksik."
            )

    return {
        "telegram_eligible": telegram_eligible,
        "selection_reason": reason,
    }


def _vote(name: str, direction: str, strength: float, confidence: float, reason: str) -> dict:
    direction = _normalize_direction(direction)
    strength = round(_clamp(float(strength), 0, 30), 1)
    confidence = round(_clamp(float(confidence), 0, 100), 1)
    return {
        "name": name,
        "direction": direction,
        "strength": strength,
        "confidence": confidence,
        "score": round(strength * (confidence / 100), 1),
        "reason": reason,
    }


def _decision_from_components(
    *,
    legacy_direction: str,
    live: float,
    opening: float,
    diff: float,
    threshold: float,
    status: str,
    alert_moment: str = "",
    score: str = "",
    match_name: str = "",
    tournament: str = "",
    projected_total: float | None = None,
    fair_edge: float | None = None,
    history_total: float | None = None,
    h2h_over_pct: float | None = None,
    h2h_quality_score: float | None = None,
    pace_anomaly_direction: str | None = None,
    pace_anomaly_pct: float | None = None,
    projection_quality: float | None = None,
    signal_count: int = 1,
    backtest_profile: dict | None = None,
) -> dict:
    legacy_direction = _normalize_direction(legacy_direction)
    votes: list[dict] = []

    abs_diff = abs(float(diff or 0))
    # Piyasa-zekası ayarı: küçük-orta hareket fade edilebilir (sweet spot 11-16),
    # ama büyük hareket (>=22) genelde gerçek bilgi taşır — fade etmek tehlikeli.
    # Backtest verisi: diff 12-15 → %67 başarı; diff 25+ → %27 başarı.
    if abs_diff <= 16:
        legacy_strength = _clamp(8 + (abs_diff - float(threshold or 0)) * 0.6, 7, 13)
        legacy_confidence = 66
    elif abs_diff <= 20:
        legacy_strength = 6
        legacy_confidence = 50
    else:
        legacy_strength = 4
        legacy_confidence = 35
    votes.append(_vote(
        "Eski barem sinyali",
        legacy_direction,
        legacy_strength,
        legacy_confidence,
        f"Eski mantık: canlı-açılış farkı {diff:+.1f}, eşik {float(threshold or 0):.1f}."
        + (" Büyük hareket — güven düşük tutuldu." if abs_diff > 20 else ""),
    ))

    # Çok büyük hareketlerde (>=22) sadece "uyarı oyu" olarak çelişki ekle.
    # Bu oy yön kararını belirsizleştirip flip kararını gerçek
    # ayrışma varsa verir.
    if abs_diff >= 25:
        market_direction = _opposite_direction(legacy_direction)
        votes.append(_vote(
            "Piyasa direnci uyarısı",
            market_direction,
            _clamp(5 + (abs_diff - 25) * 0.3, 5, 10),
            45,
            f"Çizgi {abs_diff:.0f} puan kaydı; piyasa bu kadar net hareket ettiğinde "
            f"genelde haklı — fade güveni düşük tutuldu.",
        ))

    # Adil barem oyu — çok büyük edge'leri sınırla (genelde H2H/team_recent
    # parazitinden geliyor, gerçek değer değil).
    if fair_edge is not None:
        edge = float(fair_edge)
        edge_capped = _clamp(abs(edge), 0, 10)  # >10 üzerinde ek katkı yok
        if edge >= 3:
            confidence = 70 if abs(edge) >= 6 else 60
            if abs(edge) > 12:
                confidence -= 12  # aşırı edge şüpheli
            votes.append(_vote(
                "Adil barem değeri",
                "ÜST",
                _clamp(7 + edge_capped * 0.7, 7, 16),
                confidence,
                f"Adil barem canlıdan {edge:.1f} yüksek; piyasa aşağıda kalmış."
                + (" Edge çok büyük, ihtiyatla değerlendir." if abs(edge) > 12 else ""),
            ))
        elif edge <= -3:
            confidence = 70 if abs(edge) >= 6 else 60
            if abs(edge) > 12:
                confidence -= 12
            votes.append(_vote(
                "Adil barem değeri",
                "ALT",
                _clamp(7 + edge_capped * 0.7, 7, 16),
                confidence,
                f"Adil barem canlıdan {abs(edge):.1f} düşük; piyasa fazla şişmiş."
                + (" Edge çok büyük, ihtiyatla değerlendir." if abs(edge) > 12 else ""),
            ))

    projected_gap = None
    if projected_total is not None:
        projected_gap = round(float(projected_total) - float(live), 1)
        gap_capped = _clamp(abs(projected_gap), 0, 12)
        if projected_gap >= 4:
            votes.append(_vote(
                "Canlı tempo/projeksiyon",
                "ÜST",
                _clamp(6 + gap_capped * 0.6, 6, 16),
                70 if projected_gap >= 8 else 60,
                f"Regresyonlu projeksiyon canlı baremin {projected_gap:.1f} üstünde.",
            ))
        elif projected_gap <= -4:
            votes.append(_vote(
                "Canlı tempo/projeksiyon",
                "ALT",
                _clamp(6 + gap_capped * 0.6, 6, 16),
                70 if projected_gap <= -8 else 60,
                f"Regresyonlu projeksiyon canlı baremin {abs(projected_gap):.1f} altında.",
            ))

    if history_total is not None:
        history_gap = round(float(history_total) - float(live), 1)
        quality = _clamp((float(h2h_quality_score or 65) + 35) / 100, 0.45, 1.0)
        if history_gap >= 4:
            votes.append(_vote(
                "Takım/H2H regresyonu",
                "ÜST",
                _clamp((6 + history_gap * 0.55) * quality, 4, 16),
                58 + quality * 20,
                f"Tarihsel/son form toplamı canlıdan {history_gap:.1f} yüksek.",
            ))
        elif history_gap <= -4:
            votes.append(_vote(
                "Takım/H2H regresyonu",
                "ALT",
                _clamp((6 + abs(history_gap) * 0.55) * quality, 4, 16),
                58 + quality * 20,
                f"Tarihsel/son form toplamı canlıdan {abs(history_gap):.1f} düşük.",
            ))

    if h2h_over_pct is not None:
        over = float(h2h_over_pct)
        if over >= 62:
            votes.append(_vote("H2H over eğilimi", "ÜST", _clamp((over - 50) * 0.55, 5, 12), 58, f"H2H over oranı %{over:.0f}."))
        elif over <= 38:
            votes.append(_vote("H2H under eğilimi", "ALT", _clamp((50 - over) * 0.55, 5, 12), 58, f"H2H under tarafı güçlü; over oranı %{over:.0f}."))

    if pace_anomaly_direction:
        pct = abs(float(pace_anomaly_pct or 0))
        votes.append(_vote(
            "Çeyrek hız anomalisi",
            pace_anomaly_direction,
            _clamp(7 + pct * 0.15, 7, 15),
            64,
            f"Çeyrek hızı ortalamadan %{pct:.0f} saptı; regresyon {_normalize_direction(pace_anomaly_direction)} tarafında.",
        ))

    script_vote = _script_signal(score, status, match_name, tournament, projected_gap)
    if script_vote:
        votes.append(_vote(
            "Maç scripti",
            script_vote["direction"],
            script_vote["strength"],
            script_vote["confidence"],
            script_vote["reason"],
        ))

    backtest_keys = _backtest_keys_from_values(
        legacy_direction=legacy_direction,
        diff=diff,
        status=status,
        alert_moment=alert_moment,
        fair_edge=fair_edge,
        projected_total=projected_total,
        live=live,
        signal_count=signal_count,
        tournament=tournament,
    )
    backtest_scores = _backtest_direction_scores(backtest_profile, backtest_keys)

    totals = {"ALT": 0.0, "ÜST": 0.0}
    for item in votes:
        totals[item["direction"]] += float(item["score"])

    final_direction = "ALT" if totals["ALT"] > totals["ÜST"] else "ÜST"
    # Yön değişimleri tarihsel olarak güçlü; ama yönsüz/sıkı durumda
    # legacy'yi koru. Flip için net üstünlük gerekir.
    raw_margin = abs(totals["ALT"] - totals["ÜST"])
    if raw_margin < 2.5:
        final_direction = legacy_direction

    best_rate = backtest_scores.get(final_direction, {}).get("rate")
    fair_abs = abs(float(fair_edge)) if fair_edge is not None else 0.0
    projected_abs = abs(float(projected_gap)) if projected_gap is not None else 0.0
    flipped = final_direction != legacy_direction

    flip_reason = ""
    if flipped:
        flip_reason = (
            f"Eski sinyal {legacy_direction}, yeni karar {final_direction}: "
            f"tempo/adil barem/piyasa oyları yönü çevirdi."
        )

    return {
        "direction": final_direction,
        "legacy_direction": legacy_direction,
        "signal_scores": {key: round(value, 1) for key, value in totals.items()},
        "signal_votes": sorted(votes, key=lambda item: item["score"], reverse=True),
        "projection_quality": projection_quality,
        "fair_edge": fair_edge,
        "fair_edge_abs": round(fair_abs, 1),
        "projected_gap": projected_gap,
        "projected_gap_abs": round(projected_abs, 1),
        "signal_count": signal_count,
        "backtest": {
            "sample_size": (backtest_profile or {}).get("sample_size", 0),
            "keys": backtest_keys,
            "scores": backtest_scores,
            "chosen_rate": best_rate,
            "chosen_samples": backtest_scores.get(final_direction, {}).get("samples", 0),
        },
        "flip_reason": flip_reason,
    }


def enrich_analysis_with_backtest(
    alert: dict,
    analysis: dict | None,
    backtest_profile: dict | None = None,
    threshold: float = 0,
) -> dict:
    analysis = dict(analysis or {})
    opening = _safe_float(alert.get("opening_total", alert.get("opening")))
    live = _safe_float(alert.get("inplay_total", alert.get("live")))
    if opening is None or live is None:
        return analysis
    legacy_direction = analysis.get("legacy_direction") or alert.get("direction") or ("ALT" if live - opening > 0 else "ÜST")
    diff = _safe_float(alert.get("diff"))
    if diff is None:
        diff = abs(live - opening)
    decision = _decision_from_components(
        legacy_direction=legacy_direction,
        live=live,
        opening=opening,
        diff=diff,
        threshold=threshold,
        status=alert.get("status") or "",
        alert_moment=alert.get("alert_moment") or "",
        score=alert.get("score") or "",
        match_name=alert.get("match_name") or "",
        tournament=alert.get("tournament") or "",
        projected_total=analysis.get("projected_total"),
        fair_edge=analysis.get("fair_edge"),
        history_total=analysis.get("history_total"),
        h2h_over_pct=(analysis.get("team_context") or {}).get("h2h_over_pct") if isinstance(analysis.get("team_context"), dict) else None,
        h2h_quality_score=analysis.get("h2h_quality_score"),
        pace_anomaly_direction=analysis.get("pace_anomaly_direction"),
        pace_anomaly_pct=analysis.get("pace_anomaly_pct"),
        projection_quality=analysis.get("projection_quality"),
        signal_count=alert.get("signal_count") or 1,
        backtest_profile=backtest_profile,
    )
    selection = _classify_signal(decision)
    warnings = analysis.get("warnings") if isinstance(analysis.get("warnings"), list) else []
    selection_reason = selection.get("selection_reason")
    if selection_reason and selection_reason not in warnings:
        warnings = [selection_reason, *warnings]
    return {**analysis, **decision, **selection, "warnings": warnings}


def build_signal_analysis(
    match: dict,
    context: dict | None = None,
    threshold: float = 0,
    pace_data: dict | None = None,
    backtest_profile: dict | None = None,
) -> dict:
    context = context or {}
    opening = float(match.get("opening_total", match.get("opening")))
    live = float(match.get("inplay_total", match.get("live")))
    match_name = match.get("match_name", "")
    tournament = match.get("tournament", "")
    status = match.get("status", "")
    score = match.get("score", "")
    quarter_scores = match.get("quarter_scores") if isinstance(match.get("quarter_scores"), dict) else {}
    live_stats = match.get("live_stats") if isinstance(match.get("live_stats"), dict) else {}
    odds_snapshot = match.get("odds_snapshot") if isinstance(match.get("odds_snapshot"), dict) else {}

    legacy_direction = _normalize_direction(match.get("direction") or ("ALT" if live - opening > 0 else "ÜST"))
    direction = legacy_direction
    line_delta_open = round(live - opening, 1)
    h2h_body = (context.get("h2h") or {}).get("body_text", "") if isinstance(context, dict) else ""
    h2h_metrics = _extract_h2h_metrics(h2h_body, match_name)
    clock = game_clock(status, match_name, tournament)
    market_total = _market_total(match, opening)
    live_projection = calculate_live_projection(
        score,
        status,
        match_name,
        tournament,
        quarter_scores=quarter_scores,
        live_stats=live_stats.get("totals") if isinstance(live_stats, dict) else None,
        market_total=market_total,
        opening_total=opening,
    )
    raw_projected = live_projection.get("projected_total")
    pure_projected_raw = live_projection.get("pure_projected_total")
    projected_total = round(raw_projected, 1) if raw_projected is not None else None
    pure_projected_total = (
        round(pure_projected_raw, 1) if pure_projected_raw is not None else None
    )
    projection_quality = live_projection.get("data_quality")

    # Geçen süre — adil barem time-decay ağırlıkları için
    period = clock.get("period")
    remaining_min = clock.get("remaining_min")
    quarter_length = clock.get("quarter_length") or 10
    total_game_min = clock.get("total_game_min") or 40
    if period and remaining_min is not None:
        elapsed_minutes = (period - 1) * quarter_length + (quarter_length - remaining_min)
    else:
        elapsed_minutes = None

    # H2H ve son form verisini DB'de tutmaya devam ediyoruz (görünürlük için),
    # ancak adil barem hesabında SIFIR ağırlığa sahipler — açılış çizgisi
    # bu bilgileri zaten büyük ölçüde fiyatlamış (double-counting'i önler).
    team_recent_total = h2h_metrics.get("expected_total")
    h2h_total = h2h_metrics.get("h2h_avg_total")
    h2h_games = h2h_metrics.get("h2h_games")
    h2h_quality_score = h2h_metrics.get("h2h_quality_score")
    h2h_quality_notes = h2h_metrics.get("h2h_quality_notes") or []
    history_values = [value for value in (team_recent_total, h2h_total) if value is not None]
    history_total = round(mean(history_values), 1) if history_values else None

    # Şeffaflık için bileşenleri raporlamaya devam ediyoruz (H2H/form 0 weight).
    components = {
        "opening": market_total,
        "market": market_total,
        "live_projection": pure_projected_total,
        "projection": pure_projected_total,
        "team_recent": team_recent_total,
        "h2h": h2h_total,
    }
    fair_line, fair_meta = calculate_fair_line(
        prematch=market_total,
        pure_pace_projection=pure_projected_total,
        elapsed_minutes=elapsed_minutes,
        total_game_minutes=total_game_min,
        data_quality=projection_quality if projection_quality is not None else 50.0,
    )
    weights = {
        "opening": int(round(fair_meta.get("opening_weight", fair_meta.get("prematch_weight", 0)) * 100)),
        "live_projection": int(round(fair_meta.get("live_weight", 0) * 100)),
        "team_recent": 0,
        "h2h": 0,
    }
    base_weights = weights  # Geriye uyumluluk için aynı yapıyı koru.
    fair_edge = round(fair_line - live, 1) if fair_line is not None else None

    team_context = build_team_context(h2h_metrics, live, direction)
    h2h_note = (team_context or {}).get("h2h_note") or "H2H verisi yok veya okunamadı."
    if h2h_quality_score is not None and h2h_quality_score < 70:
        h2h_note = f"{h2h_note} Veri kalitesi düşük ({h2h_quality_score}/100)."
    form_games = sorted({
        int(profile.get("games"))
        for profile in (h2h_metrics.get("home_last6") or {}, h2h_metrics.get("away_last6") or {})
        if profile.get("games") is not None and int(profile.get("games")) >= 3
    }, reverse=True)
    if form_games:
        form_history_label = f"Son {'/'.join(str(g) for g in form_games)} maç"
    elif h2h_total is not None:
        form_history_label = "H2H"
    else:
        form_history_label = "Son form"
    history_note = (
        f"{form_history_label}/H2H adil toplamı {history_total:.1f}."
        if history_total is not None
        else "Son 3-4-5 maç ve H2H verilerinden adil toplam çıkarılamadı."
    )
    pace = _pace_note(
        projected_total,
        live,
        pace_data,
        projection_components=live_projection.get("components") or {},
        h2h_metrics=h2h_metrics,
    )
    script = _script_warning(score, status, match_name, tournament)

    # Çeyrek hız anomalisi uyarısı
    pace_anomaly_direction = (pace_data or {}).get("anomaly_direction")
    pace_anomaly_pct = (pace_data or {}).get("anomaly_pct")
    quarter_paces = (pace_data or {}).get("quarter_paces", {})
    pace_anomaly_note = ""
    if pace_anomaly_direction and pace_anomaly_pct is not None:
        pace_anomaly_note = (
            f"Çeyrek hız anomalisi (%{abs(int(pace_anomaly_pct))} sapma): "
            f"ortalamaya dönüş {pace_anomaly_direction} tarafını destekler."
        )

    form_quality_notes = [
        note for note in h2h_quality_notes
        if "son 3-4-5 maç" in note.lower() or "son 6 yerine" in note.lower()
    ]
    other_h2h_quality_notes = [note for note in h2h_quality_notes if note not in form_quality_notes]
    warnings = [h2h_note, history_note, *form_quality_notes, pace, script]
    warnings.extend(live_projection.get("notes") or [])
    warnings.extend(other_h2h_quality_notes[:3])
    if pace_anomaly_note:
        warnings.insert(0, pace_anomaly_note)
    if abs(line_delta_open) >= 20:
        warnings.append(f"Açılış-canlı farkı çok yüksek ({line_delta_open:+.1f}); bu bölge ekstra riskli.")
    if projected_total is None:
        warnings.append("Adil barem hesaplanamadı: maç süresi/projeksiyon güvenilir okunamadı.")
    if fair_edge is not None and abs(fair_edge) <= 3:
        warnings.append("Adil barem canlıya çok yakın; net değer alanı zayıf.")

    decision = _decision_from_components(
        legacy_direction=legacy_direction,
        live=live,
        opening=opening,
        diff=line_delta_open,
        threshold=threshold,
        status=status,
        alert_moment=match.get("alert_moment") or "",
        score=score,
        match_name=match_name,
        tournament=tournament,
        projected_total=projected_total,
        fair_edge=fair_edge,
        history_total=history_total,
        h2h_over_pct=h2h_metrics.get("h2h_over_pct"),
        h2h_quality_score=h2h_quality_score,
        pace_anomaly_direction=pace_anomaly_direction,
        pace_anomaly_pct=pace_anomaly_pct,
        projection_quality=projection_quality,
        signal_count=match.get("signal_count") or 1,
        backtest_profile=backtest_profile,
    )
    direction = decision["direction"]
    if decision.get("flip_reason"):
        warnings.insert(0, decision["flip_reason"])
    selection = _classify_signal(decision)
    warnings.insert(0, selection["selection_reason"])

    if fair_line is None:
        if projected_total is None:
            recommendation = "Adil barem hesaplanamadı: maç süresi/projeksiyon güvenilir okunamadı."
        else:
            recommendation = "Adil barem hesaplanamadı; sadece eşik uyarısı olarak izle."
    elif fair_edge >= 6:
        recommendation = "Adil barem canlıdan yüksek: değer ÜST tarafında; maç hızlanabilir."
    elif fair_edge <= -6:
        recommendation = "Adil barem canlıdan düşük: değer ALT tarafında; maç yavaşlayabilir."
    elif fair_edge > 0:
        recommendation = "Adil barem canlıdan az yüksek: ÜST tarafı hafif değerli, temkinli izle."
    elif fair_edge < 0:
        recommendation = "Adil barem canlıdan az düşük: ALT tarafı hafif değerli, temkinli izle."
    else:
        recommendation = "Adil barem canlıyla aynı; bahis için net avantaj yok."

    summary = f"Adil Barem: {fair_line:.1f}" if fair_line is not None else "Adil Barem: hesaplanamadı"
    if fair_edge is not None:
        summary += f" | Canlıya göre {fair_edge:+.1f}"

    result = {
        "direction": direction,
        "legacy_direction": legacy_direction,
        "final_direction": direction,
        "fair_line": fair_line,
        "fair_edge": fair_edge,
        "projected_total": _safe_round(projected_total),
        "market_total": _safe_round(market_total),
        "team_recent_total": _safe_round(team_recent_total),
        "home_last6": h2h_metrics.get("home_last6") or {},
        "away_last6": h2h_metrics.get("away_last6") or {},
        "h2h_total": _safe_round(h2h_total),
        "history_total": _safe_round(history_total),
        "h2h_games": h2h_games,
        "h2h_source": h2h_metrics.get("h2h_source") or "",
        "h2h_body_chars": h2h_metrics.get("h2h_body_chars"),
        "h2h_quality_score": h2h_quality_score,
        "h2h_quality_notes": h2h_quality_notes,
        "weights": weights,
        "base_weights": base_weights,
        "fair_components": {key: _safe_round(value) for key, value in components.items()},
        "fair_meta": fair_meta,
        "pure_projected_total": _safe_round(pure_projected_total),
        "elapsed_minutes": _safe_round(elapsed_minutes) if elapsed_minutes is not None else None,
        "projection_quality": projection_quality,
        "projection_components": live_projection.get("components") or {},
        "projection_notes": live_projection.get("notes") or [],
        "raw_projected_total": live_projection.get("raw_projected_total"),
        "quarter_scores": quarter_scores,
        "live_stats": live_stats,
        "odds_snapshot": odds_snapshot,
        "opening_delta": line_delta_open,
        "recommendation": recommendation,
        "pace_note": pace,
        "h2h_note": h2h_note,
        "history_note": history_note,
        "script_note": script,
        "warnings": warnings,
        "summary": summary,
        "team_context": team_context,
        "threshold": threshold,
        "pace_anomaly_direction": pace_anomaly_direction,
        "pace_anomaly_pct": pace_anomaly_pct,
        "quarter_paces": quarter_paces,
        "pace_anomaly_note": pace_anomaly_note,
        "signal_scores": decision.get("signal_scores"),
        "signal_votes": decision.get("signal_votes"),
        "backtest": decision.get("backtest"),
        "flip_reason": decision.get("flip_reason"),
        "telegram_eligible": selection.get("telegram_eligible"),
        "selection_reason": selection.get("selection_reason"),
    }
    return result
