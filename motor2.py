"""Independent, experimental boxscore totals analysis. No M1 direction input."""

import math
import re
from datetime import datetime, timezone

from live_signals import valid_total
from match_state import confirmed_12_minute_quarters, game_clock, parse_score


VERSION = "m2-boxscore-v1"
MAX_CAPTURE_DELAY_SECONDS = 60
MAX_CLOCK_SKEW_SECONDS = 15


def m2_status(state, message, *, context=None, reason_code=None):
    return {"version": VERSION, "state": state, "direction": None,
            "message": message, "quality": "YETERSİZ", "reasons": [],
            "checks": [], "limitations": [], "context": context or {},
            "reason_code": reason_code}


def pending_m2(match, *, enabled=True):
    clock = game_clock(match["status"], match["match_name"], match["tournament"])
    context = {"match_id": str(match["match_id"]), "score": match["score"],
               "status": match["status"], "line": match["inplay_total"],
               "observed_at": match.get("market_captured_at"),
               "quarter_length": clock.get("quarter_length"),
               "period_count": clock.get("period_count")}
    return m2_status("pending" if enabled else "disabled",
                     "Veri bekleniyor" if enabled else "M2 kapalı", context=context)


def _elapsed(status, context):
    # Use M1's frozen duration, including an already verified 12-minute override.
    length, count = context.get("quarter_length"), context.get("period_count")
    with confirmed_12_minute_quarters(length == 12):
        clock = game_clock(status, tournament="NCAA" if count == 2 else "")
    period, remaining = clock.get("period"), clock.get("remaining_min")
    if (not period or remaining is None or not length or not count
            or not 1 <= period <= count or not 0 <= remaining <= length):
        return None
    return (period - 1) * length + length - remaining


def _shot_estimate(made, attempts, prior, prior_attempts):
    # Weak, declared modelling assumptions, not retrieved team/league averages.
    a = made + prior * prior_attempts
    b = attempts - made + (1 - prior) * prior_attempts
    mean = a / (a + b)
    std = math.sqrt(a * b / ((a + b) ** 2 * (a + b + 1)))
    return mean, max(0, mean - std), min(1, mean + std)


def evaluate_m2(context, data, *, now=None):
    """Evaluate only contemporaneous, reconciled, independently fetched facts."""
    result = m2_status("unavailable", "Veri çekilemedi; sinyal üretilemedi", context=context)

    def reject(message, check, reason_code):
        result["message"] = message
        result["reason_code"] = reason_code
        result["checks"].append({"label": check, "passed": False})
        return result

    if not data.get("available"):
        result["source_error"] = data.get("error")
        errors = {
            "observation_score_mismatch": "İstatistik skoru sinyal anıyla uyuşmuyor",
            "match_identity_mismatch": "Maç kimliği doğrulanamadı",
            "statistics_fetch_timeout": "İstatistik isteği zaman aşımına uğradı",
            "site_codec_unavailable": "AIScore istatistik okuyucusu kullanılamıyor",
        }
        reason = "score_mismatch" if data.get("error") == "observation_score_mismatch" else (
            "identity_mismatch" if data.get("error") == "match_identity_mismatch" else "fetch_failed")
        return reject(errors.get(data.get("error"), result["message"]), "Veri erişimi", reason)
    if str(data.get("match_id")) != str(context.get("match_id")):
        return reject("Maç kimliği uyuşmuyor; sinyal üretilemedi", "Maç kimliği", "identity_mismatch")
    result["checks"].append({"label": "Maç kimliği", "passed": True})
    original_score = parse_score(context.get("score", ""))
    if original_score[0] is None or parse_score(data.get("score", "")) != original_score:
        return reject("Skor sinyal anıyla uyuşmuyor; sinyal üretilemedi", "Skor eşleşmesi", "score_mismatch")
    result["checks"].append({"label": "Sinyal skoru / istatistik skoru", "passed": True})
    if data.get("fetch_method") != "uncached_public_api":
        return reject("Güncel istatistik çekimi doğrulanamadı", "Cache kullanılmadan çekim", "freshness_unverified")
    result["checks"].append({"label": "Cache kullanılmadan çekim", "passed": True})
    current_time = now or datetime.now(timezone.utc)
    try:
        observed = datetime.fromisoformat(str(context["observed_at"]).replace("Z", "+00:00"))
        captured = datetime.fromisoformat(str(data["captured_at"]).replace("Z", "+00:00"))
        valid_age = (observed.tzinfo is not None and captured.tzinfo is not None
                     and 0 <= (captured - observed).total_seconds() <= MAX_CAPTURE_DELAY_SECONDS
                     and 0 <= (current_time - captured).total_seconds() <= MAX_CAPTURE_DELAY_SECONDS)
    except (KeyError, TypeError, ValueError):
        valid_age = False
    if not valid_age:
        return reject("Sinyal anına ait güncel veri doğrulanamadı", "Yakalama zamanı", "capture_expired")
    result["captured_at"] = data["captured_at"]
    result["checks"].append({"label": "Sinyal sonrası en fazla 60 saniyede okuma", "passed": True})
    elapsed = _elapsed(context.get("status", ""), context)
    header = re.search(r"\b(Q[1-4]|[1-4]Q)\s*[-\s]?\s*(\d{1,2}:[0-5]\d)\b",
                       data.get("top_text", ""), re.I)
    page_status = f"Q{header[1].replace('Q', '').replace('q', '')} {header[2]}" if header else ""
    page_elapsed = _elapsed(page_status, context)
    with confirmed_12_minute_quarters(context.get("quarter_length") == 12):
        original_period = game_clock(context.get("status", ""),
                                     tournament="NCAA" if context.get("period_count") == 2 else "").get("period")
    if (elapsed is None or page_elapsed is None
            or original_period != int(page_status[1])
            or not 0 <= round((page_elapsed - elapsed) * 60) <= MAX_CLOCK_SKEW_SECONDS
            or str(data.get("status_id")) != str(2 * int(page_status[1]))):
        return reject("Oyun saati sinyal anıyla uyuşmuyor", "Periyot / oyun saati", "clock_mismatch")
    result["checks"].append({"label": "Aynı periyot, oyun saati farkı en fazla 15 saniye", "passed": True})
    # Report absent shot data before testing events: otherwise leagues without
    # either feed misleadingly appear to have only an event timing problem.
    if not data.get("full_boxscore_available"):
        result["data_issues"] = list(data.get("issues") or [])
        mismatched = any("mismatch" in issue or "invalid" in issue
                         for issue in result["data_issues"] if ":boxscore_" in issue)
        return reject("Ayrıntılı şut verisi skorla uyuşmuyor" if mismatched
                      else "İki takımın ayrıntılı şut verisi eksik",
                      "Tam boxscore / sayı aritmetiği",
                      "boxscore_mismatch" if mismatched else "boxscore_missing")
    # Timeline corroborates the scoreboard; do not infer possessions from event subjects.
    current_period = int(page_status[1])
    corroborated = False
    for event in data.get("events", []):
        if event.get("period") != current_period or parse_score(str(event.get("score") or "")) != original_score:
            continue
        event_elapsed = _elapsed(f"Q{current_period} {event.get('clock', '')}", context)
        if event_elapsed is not None and 0 <= round((page_elapsed - event_elapsed) * 60) <= 30:
            corroborated = True
            break
    if not corroborated:
        return reject("Olay akışı skor ve saati doğrulamıyor", "Olay akışı güncelliği",
                      "timeline_missing" if not data.get("events") else "timeline_mismatch")
    result["checks"].append({"label": "Olay akışında aynı skor, en fazla 30 saniye oyun farkı", "passed": True})
    boxes = [data["teams"][side].get("boxscore") for side in ("home", "away")]
    if any(not box or any(box.get(key) is None for key in ("offensive_rebounds", "turnovers")) for box in boxes):
        return reject("Hücum ribaundu veya top kaybı verisi eksik", "Pozisyon hacmi verileri", "possession_fields_missing")
    result["checks"].append({"label": "İki takımın şutları, hücum ribaundu ve top kayıpları", "passed": True})
    game_minutes = context["quarter_length"] * context["period_count"]
    remaining = game_minutes - elapsed
    line = valid_total(context.get("line"))
    if elapsed < 12 or remaining < 3 or line is None or sum(original_score) >= line:
        return reject("Oyun süresi veya barem bağımsız analiz için uygun değil", "Analiz yapılabilir oyun aralığı", "game_ineligible")

    possessions = []
    for box in boxes:
        fga, fta = box["field_goals_attempted"], box["free_throws_attempted"]
        orb, tov = box["offensive_rebounds"], box["turnovers"]
        if fga < 15 or orb > fga - box["field_goals_made"] + fta - box["free_throws_made"]:
            return reject("Şut örneklemi veya ribaund tutarlılığı yetersiz", "Örneklem / ribaund tutarlılığı", "sample_invalid")
        possessions.append(0.96 * (fga + 0.44 * fta - orb + tov))
    average_possessions = sum(possessions) / 2
    if average_possessions <= 0 or abs(possessions[0] - possessions[1]) > max(4, average_possessions * .10):
        return reject("Takımların pozisyon tahminleri tutarsız", "Pozisyon tahmini tutarlılığı", "possessions_mismatch")
    result["checks"].append({"label": "Pozisyon hacmi / iki takım tutarlılığı", "passed": True})
    result["quality"] = "ORTA"
    result["limitations"] = [
        "Sağlayıcının istatistik güncelleme zamanı yok; skor, saat ve olay akışıyla çapraz kontrol edildi.",
        "Pozisyon sayısı tahmindir; olaylardan kesin pozisyon veya bonus faul sayılmadı.",
        "Şut kalitesi, sakatlık, güncel beş ve takım sezon ortalamaları bu sürümde bulunmuyor.",
        "Şut dengeleme oranları genel model varsayımlarıdır; takım ortalaması veya başarı olasılığı değildir.",
        "Tahmin aralığı senaryo duyarlılığıdır; istatistiksel güven aralığı değildir.",
    ]
    # All model assumptions are frozen with the assessment for later comparison.
    assumptions = {"two_points": (.52, 12, 2), "three_points": (.35, 18, 3),
                   "free_throws": (.75, 8, 1)}
    rate, low_rate, high_rate = 0., 0., 0.
    team_metrics = []
    for side, box, poss in zip(("home", "away"), boxes, possessions):
        adjusted = 0.
        shooting = {}
        for shot, (prior, weight, points) in assumptions.items():
            attempts, made = box[shot + "_attempted"], box[shot + "_made"]
            mean, lower, upper = _shot_estimate(made, attempts, prior, weight)
            attempt_rate = attempts / elapsed
            rate += points * attempt_rate * mean
            low_rate += points * attempt_rate * lower
            high_rate += points * attempt_rate * upper
            adjusted += points * attempt_rate * mean
            shooting[shot] = {"made": made, "attempted": attempts,
                              "observed_pct": round(100 * made / attempts, 1) if attempts else None,
                              "model_pct": round(100 * mean, 1)}
        team_metrics.append({"side": side, "shooting": shooting,
                             "possessions_estimate": round(poss, 1),
                             "offensive_rebounds": box["offensive_rebounds"],
                             "turnovers": box["turnovers"], "personal_fouls": box.get("personal_fouls"),
                             "future_ppm": round(adjusted, 2)})
    score = sum(original_score)
    center = score + remaining * rate
    lower, upper = score + remaining * low_rate * .95, score + remaining * high_rate * 1.05
    required_edge = max(4, line * .02)
    blowout = abs(original_score[0] - original_score[1]) >= 20 and elapsed >= game_minutes / 2
    direction = "ÜST" if lower > line + required_edge else "ALT" if upper < line - required_edge else "PAS"
    if blowout:
        direction = "PAS"
        result["reasons"].append("İkinci yarıda 20+ sayı farkı var; rotasyon ve tempo değişebilir.")
    result.update(state="ready", direction=direction,
                  message=direction if direction != "PAS" else "PAS · Belirgin avantaj yok",
                  projection=round(center, 1), projection_low=round(lower, 1),
                  projection_high=round(upper, 1), edge=round(center - line, 1),
                  required_edge=round(required_edge, 1), remaining_minutes=round(remaining, 2),
                  possessions_per_team=round(average_possessions, 1),
                  possessions_per_team_per_minute=round(average_possessions / elapsed, 2),
                  teams=team_metrics, assumptions={shot: {"rate": prior, "pseudo_attempts": weight}
                                                  for shot, (prior, weight, _) in assumptions.items()},
                  pace_scenario_pct=5)
    result["reasons"].append(f"Tahmini takım başı pozisyon hızı {average_possessions / elapsed:.2f}/dk; sayıdan ayrı hücum hacmi kontrol edildi.")
    for side, metrics in zip(("Ev", "Deplasman"), team_metrics):
        three = metrics["shooting"]["three_points"]
        two = metrics["shooting"]["two_points"]
        result["reasons"].append(f"{side}: 2 sayı {two['made']}/{two['attempted']}, 3 sayı {three['made']}/{three['attempted']}. "
                                 f"3 sayı devam varsayımı %{three['model_pct']}; geçici isabet serisine temkinli yaklaşıldı.")
    total_fga = sum(box["field_goals_attempted"] for box in boxes)
    total_fta = sum(box["free_throws_attempted"] for box in boxes)
    result["reasons"].append(f"Serbest atış hacmi {total_fta} deneme / {total_fga} saha içi deneme; hücum ribaundu ve top kayıpları pozisyon hesabına katıldı.")
    if direction == "ÜST":
        result["reasons"].append(f"Temkinli alt senaryo {lower:.1f}, {line:.1f} bareminin gereken {required_edge:.1f} sayı farkıyla üzerinde.")
    elif direction == "ALT":
        result["reasons"].append(f"Yüksek sayı senaryosu {upper:.1f}, {line:.1f} bareminin gereken {required_edge:.1f} sayı farkıyla altında.")
    else:
        result["reasons"].append(f"{lower:.1f}–{upper:.1f} senaryo aralığı, {line:.1f} baremine karşı yeterince belirgin yön vermiyor.")
    return result
