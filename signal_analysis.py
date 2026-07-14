import json
import re
from statistics import mean

from projection import calculate_live_projection, calculate_quarter_ppm, game_clock, parse_score


FAIR_MODEL_VERSION = "calibrated_fair_v1"
_FAIR_CALIBRATION = {
    1: (0.51, 0.002),
    2: (0.34, -0.110),
    3: (0.11, 0.280),
    4: (0.12, 1.143),
}


def calculate_fair_line(
    *,
    prematch: float | None,
    pure_pace_projection: float | None,
    elapsed_minutes: float | None,
    total_game_minutes: int = 40,
    data_quality: float = 50.0,
    live_line: float | None = None,
    period: int | None = None,
    current_total: float | None = None,
) -> tuple[float | None, dict]:
    """Apply the fixed v1 period calibration against the live market.

    The live line remains the main anchor. The coefficients are research
    parameters, not a probability estimate or proof of market advantage.
    """
    total = max(20, int(total_game_minutes or 40))
    progress = (
        max(0.0, min(1.0, float(elapsed_minutes) / total))
        if elapsed_minutes is not None
        else 0.0
    )
    meta = {
        "model_version": FAIR_MODEL_VERSION,
        "prematch_weight": 0.0,
        "opening_weight": 0.0,
        "live_weight": 0.0,
        "live_market_weight": 0.0,
        "model_weight": 0.0,
        "calibration_intercept": 0.0,
        "progress": round(progress, 3),
        "data_quality": round(float(data_quality), 1),
        "anchor": "none",
    }
    if pure_pace_projection is None:
        fallback = live_line if live_line is not None else prematch
        if fallback is None:
            return None, meta
        meta["anchor"] = "market_only"
        meta["live_market_weight"] = 1.0 if live_line is not None else 0.0
        meta["opening_weight"] = 0.0 if live_line is not None else 1.0
        return round(float(fallback), 1), meta

    if elapsed_minutes is not None and float(elapsed_minutes) >= total:
        fair = float(pure_pace_projection)
        if current_total is not None:
            fair = max(fair, float(current_total))
        meta.update({"model_weight": 1.0, "live_weight": 1.0, "anchor": "final_score"})
        return round(fair, 1), meta

    if live_line is None:
        meta.update({"model_weight": 1.0, "live_weight": 1.0, "anchor": "projection_only"})
        fair = float(pure_pace_projection)
    else:
        beta, intercept = _FAIR_CALIBRATION.get(int(period or 0), (0.0, 0.0))
        fair = float(live_line) + beta * (
            float(pure_pace_projection) - float(live_line)
        ) + intercept
        meta.update(
            {
                "live_market_weight": round(1.0 - beta, 3),
                "model_weight": round(beta, 3),
                "live_weight": round(beta, 3),
                "calibration_intercept": round(intercept, 3),
                "anchor": "live_market_calibrated",
            }
        )
    if current_total is not None:
        fair = max(float(current_total), fair)
    return round(fair, 1), meta


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
    return text if text in {"ALT", "ÜST"} else ""


def _canonical_backtest_outcome(row: dict, analysis: dict, direction: str, success: bool) -> tuple[str, bool]:
    stored_direction = _normalize_direction(direction)
    final_direction = _normalize_direction(
        analysis.get("final_direction") or analysis.get("direction") or stored_direction
    )
    return final_direction or stored_direction, success


def _clamp(value: float, low: float, high: float) -> float:
    return max(low, min(high, value))


def _split_match_name(match_name: str) -> tuple[str, str]:
    parts = [part.strip() for part in (match_name or "").split(" - ", 1)]
    if len(parts) == 2:
        return parts[0], parts[1]
    return match_name or "Home", "Away"


_TEAM_SCORE_MIN = 20  # covers low-scoring women's/youth leagues (e.g. Senegal W-D1)
_TEAM_SCORE_MAX = 180


def _extract_h2h_metrics(body_text: str, match_name: str, *, include_team_form: bool = True) -> dict:
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
        # (pattern, default_games_when_no_capture, games_in_first_group)
        patterns = [
            (
                rf"(?:Last\s*5|Last\s*Matches|Recent\s*Form).{{0,220}}{escaped}.{{0,220}}"
                rf"(\d{{2,3}}(?:\.\d+)?)\s*points?\s*per\s*match[,\s]*"
                rf"(\d{{2,3}}(?:\.\d+)?)\s*opponent\s*points?\s*per\s*game.{{0,160}}"
                rf"(?:Total\s*points?\s*over%[:\s]*(\d+(?:\.\d+)?)%)?",
                5,
                False,
            ),
            (
                rf"(?:Last\s*5|Last\s*Matches|Recent\s*Form).{{0,260}}{escaped}.{{0,260}}"
                rf"pts\s+(\d{{2,3}}(?:\.\d+)?)\s*{pts_sep}\s*(\d{{2,3}}(?:\.\d+)?)\s+"
                rf"{per_game}",
                5,
                False,
            ),
            # AiScore H2H page format: "TeamName N [chars] Home/Away [chars] pts X – Y per game"
            # The captured N (1-29) is the actual games count for the section the
            # page is averaging over; preserve it instead of defaulting to 5.
            (
                rf"{escaped}.{{0,10}}\b([1-9]|[12]\d)\b.{{0,80}}?(?:Home|Away|All).{{0,250}}?"
                rf"pts\s+(\d{{2,3}}(?:\.\d+)?)\s*{pts_sep}\s*(\d{{2,3}}(?:\.\d+)?)\s+"
                rf"{per_game}",
                None,
                True,
            ),
        ]
        for pattern, default_games, games_first in patterns:
            m = re.search(pattern, text, re.IGNORECASE)
            if not m:
                continue
            if games_first:
                games = int(m.group(1))
                ppg, oppg = float(m.group(2)), float(m.group(3))
                over_pct = None
            else:
                ppg, oppg = float(m.group(1)), float(m.group(2))
                games = default_games
                over_pct = (
                    float(m.group(3))
                    if m.lastindex and m.lastindex >= 3 and m.group(3)
                    else None
                )
            if ppg <= 0 or oppg <= 0:
                continue
            return {
                "team": team_name,
                "games": games,
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
        score_nums = [m for m in score_matches if _TEAM_SCORE_MIN <= int(m.group(1)) <= _TEAM_SCORE_MAX]
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

        # AiScore "recent matches" rows look like:
        #   "[W|L|D] <home_team> <away_team> <home_score> <away_score>"
        # The opp_name we pass in is the *upcoming* opponent, not the actual
        # opponent of this past row, so opp_left/opp_right are usually -1.
        # When we have no opponent signal, infer team-first vs team-second by
        # checking whether anything other than the result marker (W/L/D) sits
        # between chunk-start and the team name.
        result_prefix_re = r"^[\s,;:]*(?:[WLD](?:NP|ND)?)?[\s,;:]*"

        def _team_is_chunk_head(haystack: str, pos: int) -> bool:
            if pos < 0:
                return False
            head = haystack[:pos]
            head = re.sub(result_prefix_re, "", head, count=1)
            return head.strip() == ""

        if team_left >= 0 and opp_left >= 0:
            is_first_team = team_left < opp_left
        elif team_right >= 0 and opp_right >= 0:
            # Less common layout: "80 76 Team A Team B".
            is_first_team = team_right < opp_right
        elif team_left >= 0 and team_right < 0:
            is_first_team = _team_is_chunk_head(before_scores, team_left)
        elif team_right >= 0 and team_left < 0:
            is_first_team = not _team_is_chunk_head(after_scores, team_right)
        else:
            return None

        team_score = score_a if is_first_team else score_b
        opp_score = score_b if is_first_team else score_a
        if not (
            _TEAM_SCORE_MIN <= team_score <= _TEAM_SCORE_MAX
            and _TEAM_SCORE_MIN <= opp_score <= _TEAM_SCORE_MAX
        ):
            return None
        return {"for": team_score, "against": opp_score, "total": team_score + opp_score}

    def _is_mutual_score_chunk(chunk: str, left_team: str, right_team: str) -> bool:
        score_matches = list(re.finditer(r"\b(\d{2,3})\b", chunk))
        score_nums = [m for m in score_matches if _TEAM_SCORE_MIN <= int(m.group(1)) <= _TEAM_SCORE_MAX]
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
            games = recent.get("games") or 5
            return {
                "team": team_name,
                "games": games,
                "source_label": f"Son {games} maç",
                "ppg": ppg,
                "oppg": oppg,
                "avg_total": avg_total,
                "over_pct": recent.get("over_pct"),
                "label": f"{round(ppg)}+{round(oppg)} = {round(avg_total)}",
                "scores": [],
                "source": "last5_stats",
            }
        return {}

    def h2h_score_rows_from_match_rows() -> list[dict]:
        if not text:
            return []
        rows: list[dict] = []
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
                key = (match.group(0), parsed["for"], parsed["against"])
                if 100 <= total <= 320 and key not in seen:
                    seen.add(key)
                    rows.append({
                        "date": match.group(0),
                        "home": parsed["for"],
                        "away": parsed["against"],
                        "total": total,
                    })
        return rows[:12]

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

    h2h_score_rows = h2h_score_rows_from_match_rows()
    h2h_row_totals = [row["total"] for row in h2h_score_rows]
    if h2h_avg_total is None and h2h_row_totals:
        h2h_avg_total = round(mean(h2h_row_totals), 1)
        h2h_source = "score_rows"
        if h2h_games is None:
            h2h_games = len(h2h_row_totals)
    if h2h_games is None and h2h_row_totals:
        h2h_games = len(h2h_row_totals)

    expected_total = None
    home_last6 = {}
    away_last6 = {}
    home = {}
    away = {}
    if include_team_form:
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

    if include_team_form:
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
    if include_team_form and expected_total is None:
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
        "h2h_score_rows": h2h_score_rows,
        "h2h_row_totals": h2h_row_totals,
        "h2h_body_chars": len(text),
        "h2h_quality_score": quality_score,
        "h2h_quality_notes": h2h_quality_notes,
    }


def extract_h2h_metrics(body_text: str, match_name: str, *, include_team_form: bool = True) -> dict:
    return _extract_h2h_metrics(
        body_text,
        match_name,
        include_team_form=include_team_form,
    )


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
        if game_counts else "Takım form ortalaması"
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
        alignment, alignment_code = "H2H profili nötr", "neutral"
    elif support_points > against_points:
        alignment, alignment_code = f"H2H profili {direction} sinyalini destekliyor", "support"
    elif against_points > support_points:
        alignment, alignment_code = f"H2H profili {direction} sinyaline karşı", "against"
    else:
        alignment, alignment_code = "H2H profili karışık sinyal veriyor", "mixed"

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
    for value in (
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
    for value in (
        odds.get("opening_median"),
    ):
        parsed = _safe_float(value)
        if parsed is not None:
            return round(parsed, 1)
    for key in ("opening_total", "opening", "baseline"):
        parsed = _safe_float(match.get(key))
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
    if script_adj is not None and abs(float(script_adj)) >= 2:
        context_bits.append(f"maç scripti {float(script_adj):+.1f}")
    if history_gap is not None and abs(history_gap) >= 6:
        side = "daha yüksek toplam profili" if history_gap > 0 else "daha düşük toplam profili"
        context_bits.append(f"H2H {side} ({history_gap:+.1f})")
    context = f" Destekleyen/dengeleyen unsur: {', '.join(context_bits)}." if context_bits else ""

    if gap >= 6:
        base = (
            f"Canlı projeksiyon {projected_total:.1f}; baremin {gap:.1f} üstünde. "
            f"Bu otomatik ÜST demek değil; tempo sürdürülebilirliği, şut/faul trafiği ve H2H profiliyle doğrulanmalı."
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
    Silinen ve sonucu belli sinyallerden yön bazlı bir profil çıkarır.
    Her bucket için iki aday yön de skorlanır: aynı yöndeki başarılar + karşı yöndeki
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
        direction, success = _canonical_backtest_outcome(row, analysis, direction, success)
        if not direction:
            continue
        resolved += 1
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
    Sabit araştırma aday kuralını ve insan-okunur açıklamayı döndürür.

    Bu fonksiyon oynanabilirlik sınıfını hazırlar. Telegram teslimatı PAS,
    TEST ve ONAY kayıtlarının tamamı için açıktır.
    """
    final_direction = _normalize_direction(decision.get("direction"))
    fair_edge = _safe_float(decision.get("fair_edge"))
    projected_gap = _safe_float(decision.get("projected_gap"))
    projection_direction = None
    if projected_gap is not None and abs(projected_gap) >= 6:
        projection_direction = "ÜST" if projected_gap > 0 else "ALT"
    calibrated_direction = None
    if fair_edge is not None and abs(fair_edge) >= 0.05:
        calibrated_direction = "ÜST" if fair_edge > 0 else "ALT"

    projected_gap_abs = abs(projected_gap) if projected_gap is not None else 0.0
    projection_aligned = projection_direction == final_direction
    calibration_aligned = calibrated_direction == projection_direction
    projection_quality = _safe_float(decision.get("projection_quality"))
    period = decision.get("period")
    model_validated = bool(decision.get("model_validated"))
    candidate_eligible = (
        projected_gap_abs >= 6.0
        and projection_aligned
        and calibration_aligned
        and projection_quality is not None
        and projection_quality >= 85
        and period in {2, 3}
        and model_validated
    )

    if candidate_eligible:
        reason = (
            f"Kalibre projeksiyon canlı baremden {projected_gap_abs:.1f} puan "
            f"{'yüksek' if projected_gap > 0 else 'düşük'}; Q{period} veri kalitesi "
            f"{projection_quality:.0f}/100. Bu yalnız ileri tarihli araştırma adayıdır."
        )
    else:
        if projected_gap is None or fair_edge is None:
            reason = "Projeksiyon/adil barem hesaplanamadı; araştırma adaylığı için veri yok."
        elif projected_gap_abs < 6.0:
            reason = (
                f"Projeksiyon canlıya çok yakın ({projected_gap_abs:.1f} puan; "
                "araştırma eşiği 6.0); PAS."
            )
        elif not calibration_aligned:
            reason = "Kalibrasyon ile projeksiyon yönü uyumsuz; PAS."
        elif period not in {2, 3}:
            reason = "Yalnız doğrulanan Q2/Q3 araştırma pencereleri aday kabul edilir; PAS."
        elif not model_validated or projection_quality is None or projection_quality < 85:
            reason = "Projeksiyon veri kalitesi/maç formatı araştırma adaylığına yeterli değil; PAS."
        else:
            reason = "Projeksiyon yönü ile nihai yön uyumsuz; PAS."

    return {
        "candidate_eligible": candidate_eligible,
        "candidate_rule_id": "projection_edge_6_q2q3_v2",
        "send_allowed": True,
        "selection_reason": reason,
    }


def _decision_from_components(
    *,
    legacy_direction: str,
    live: float,
    opening: float,
    diff: float,
    status: str,
    alert_moment: str = "",
    tournament: str = "",
    projected_total: float | None = None,
    fair_edge: float | None = None,
    projection_quality: float | None = None,
    signal_count: int = 1,
    backtest_profile: dict | None = None,
) -> dict:
    """Choose direction from the only production inputs that can affect it.

    The previous vote graph calculated H2H, script, pace and market scores but
    then unconditionally overwrote their result with this fair/projection rule.
    Keeping that dead graph made diagnostics look influential when they were
    not. Historical buckets remain descriptive evidence only.
    """
    legacy_direction = _normalize_direction(legacy_direction) or (
        "ALT" if float(live) - float(opening) > 0 else "ÜST"
    )
    abs_diff = abs(float(diff or 0))
    projected_gap = None
    if projected_total is not None:
        projected_gap = round(float(projected_total) - float(live), 1)

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
    clock = game_clock(status, "", tournament)

    # Fair değer projeksiyonun kalibre edilmiş dönüşümüdür; bağımsız bir oy
    # değildir. Yönü yalnız güçlü projeksiyon sapması kalibrasyonla aynı yönü
    # gösterdiğinde değiştirir. Böylece Q4 intercept tek başına yön çeviremez.
    calibrated_alignment = bool(
        fair_edge is not None
        and projected_gap is not None
        and float(fair_edge) * float(projected_gap) > 0
    )
    if projected_gap is not None and abs(float(projected_gap)) >= 6.0 and calibrated_alignment:
        final_direction = "ÜST" if float(projected_gap) > 0 else "ALT"
    else:
        final_direction = legacy_direction

    best_rate = backtest_scores.get(final_direction, {}).get("rate")
    fair_abs = abs(float(fair_edge)) if fair_edge is not None else 0.0
    projected_abs = abs(float(projected_gap)) if projected_gap is not None else 0.0
    return {
        "direction": final_direction,
        "legacy_direction": legacy_direction,
        "projection_quality": projection_quality,
        "fair_edge": fair_edge,
        "fair_edge_abs": round(fair_abs, 1),
        "projected_gap": projected_gap,
        "projected_gap_abs": round(projected_abs, 1),
        "opening": float(opening) if opening is not None else None,
        "live": float(live) if live is not None else None,
        "abs_diff": round(abs_diff, 1),
        "signal_count": signal_count,
        "period": clock.get("period"),
        "model_validated": bool(clock.get("model_validated")),
        "backtest": {
            "sample_size": (backtest_profile or {}).get("sample_size", 0),
            "keys": backtest_keys,
            "scores": backtest_scores,
            "chosen_rate": best_rate,
            "chosen_samples": backtest_scores.get(final_direction, {}).get("samples", 0),
        },
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
        status=alert.get("status") or "",
        alert_moment=alert.get("alert_moment") or "",
        tournament=alert.get("tournament") or "",
        projected_total=analysis.get("projected_total"),
        fair_edge=analysis.get("fair_edge"),
        projection_quality=analysis.get("projection_quality"),
        signal_count=alert.get("signal_count") or 1,
        backtest_profile=backtest_profile,
    )
    selection = _classify_signal(decision)
    warnings = analysis.get("warnings") if isinstance(analysis.get("warnings"), list) else []
    selection_reason = selection.get("selection_reason")
    if selection_reason and selection_reason not in warnings:
        warnings = [selection_reason, *warnings]
    return {
        **analysis,
        **decision,
        "final_direction": decision.get("direction"),
        "candidate_eligible": selection.get("candidate_eligible", False),
        "candidate_rule_id": selection.get("candidate_rule_id"),
        "selection_reason": selection.get("selection_reason"),
        "warnings": warnings,
    }


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
    odds_snapshot = match.get("odds_snapshot") if isinstance(match.get("odds_snapshot"), dict) else {}

    legacy_direction = _normalize_direction(match.get("direction") or ("ALT" if live - opening > 0 else "ÜST"))
    direction = legacy_direction
    line_delta_open = round(live - opening, 1)
    h2h_body = (context.get("h2h") or {}).get("body_text", "") if isinstance(context, dict) else ""
    h2h_metrics = _extract_h2h_metrics(h2h_body, match_name, include_team_form=True)
    clock = game_clock(status, match_name, tournament)
    market_total = _market_total(match, opening)
    live_projection = calculate_live_projection(
        score,
        status,
        match_name,
        tournament,
        quarter_scores=quarter_scores,
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

    # H2H/SF verisini görünürlük için tutuyoruz; adil barem hesabına ağırlık vermez.
    team_recent_total = h2h_metrics.get("expected_total")
    h2h_total = h2h_metrics.get("h2h_avg_total")
    h2h_games = h2h_metrics.get("h2h_games")
    h2h_quality_score = h2h_metrics.get("h2h_quality_score")
    h2h_quality_notes = h2h_metrics.get("h2h_quality_notes") or []
    history_values = [value for value in (h2h_total,) if value is not None]
    history_total = round(mean(history_values), 1) if history_values else None

    # Şeffaflık için H2H bileşenini raporluyoruz (0 weight).
    components = {
        "opening": market_total,
        "market": market_total,
        "live_market": live,
        "live_projection": pure_projected_total,
        "projection": pure_projected_total,
        "h2h": h2h_total,
    }
    parsed_home, parsed_away = parse_score(score)
    current_total = (
        parsed_home + parsed_away
        if parsed_home is not None and parsed_away is not None
        else None
    )
    fair_line, fair_meta = calculate_fair_line(
        prematch=market_total,
        pure_pace_projection=pure_projected_total,
        elapsed_minutes=elapsed_minutes,
        total_game_minutes=total_game_min,
        data_quality=projection_quality if projection_quality is not None else 50.0,
        live_line=live,
        period=period,
        current_total=current_total,
    )
    weights = {
        "opening": int(round(fair_meta.get("opening_weight", 0) * 100)),
        "live_market": int(round(fair_meta.get("live_market_weight", 0) * 100)),
        "live_projection": int(round(fair_meta.get("model_weight", 0) * 100)),
        "h2h": 0,
    }
    base_weights = weights  # Geriye uyumluluk için aynı yapıyı koru.
    fair_edge = round(fair_line - live, 1) if fair_line is not None else None

    team_context = build_team_context(h2h_metrics, live, direction)
    h2h_note = (team_context or {}).get("h2h_note") or "H2H verisi yok veya okunamadı."
    if h2h_quality_score is not None and h2h_quality_score < 70:
        h2h_note = f"{h2h_note} Veri kalitesi düşük ({h2h_quality_score}/100)."
    history_note = (
        f"H2H adil toplamı {history_total:.1f}."
        if history_total is not None
        else "H2H verisinden adil toplam çıkarılamadı."
    )
    pace = _pace_note(
        projected_total,
        live,
        pace_data,
        projection_components=live_projection.get("components") or {},
        h2h_metrics=h2h_metrics,
    )
    projection_components = live_projection.get("components") or {}
    quarter_totals = live_projection.get("quarter_totals") or []
    quarter_ppm = calculate_quarter_ppm(
        quarter_totals,
        period=period,
        remaining_min=remaining_min,
        quarter_length=quarter_length,
    )
    match_ppm = _safe_round(projection_components.get("current_pace_per_min"), 2)
    sustainable_ppm = _safe_round(projection_components.get("sustainable_pace_per_min"), 2)
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
    warnings = [h2h_note, history_note, pace, script]
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
        status=status,
        alert_moment=match.get("alert_moment") or "",
        tournament=tournament,
        projected_total=projected_total,
        fair_edge=fair_edge,
        projection_quality=projection_quality,
        signal_count=match.get("signal_count") or 1,
        backtest_profile=backtest_profile,
    )
    direction = decision["direction"]
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
        "h2h_score_rows": h2h_metrics.get("h2h_score_rows") or [],
        "h2h_row_totals": h2h_metrics.get("h2h_row_totals") or [],
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
        "projection_components": projection_components,
        "projection_model_version": live_projection.get("model_version"),
        "fair_model_version": fair_meta.get("model_version"),
        "game_format": projection_components.get("game_format"),
        "model_validated": projection_components.get("model_validated"),
        "projection_notes": live_projection.get("notes") or [],
        "raw_projected_total": live_projection.get("raw_projected_total"),
        "quarter_scores": quarter_scores,
        "quarter_totals": quarter_totals,
        "quarter_ppm": quarter_ppm,
        "quarter_length": quarter_length,
        "match_ppm": match_ppm,
        "sustainable_ppm": sustainable_ppm,
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
        "backtest": decision.get("backtest"),
        "projected_gap": decision.get("projected_gap"),
        "candidate_eligible": selection.get("candidate_eligible", False),
        "candidate_rule_id": selection.get("candidate_rule_id"),
        "selection_reason": selection.get("selection_reason"),
    }
    return result
