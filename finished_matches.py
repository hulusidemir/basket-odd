"""
finished_matches.py — Finished matches blueprint.
Provides an isolated page and API for archived signal results.
"""

import asyncio
from datetime import datetime
import threading

from flask import Blueprint, jsonify, render_template

from config import Config
from db import Database

config = Config()
db = Database(config.DB_PATH)
db.init()
manual_check_lock = threading.Lock()

finished_matches_bp = Blueprint(
    "finished_matches",
    __name__,
    template_folder="templates",
)


def _fold_text(value) -> str:
    return (
        str(value or "")
        .strip()
        .lower()
        .replace("ı", "i")
        .replace("ş", "s")
        .replace("ğ", "g")
        .replace("ü", "u")
        .replace("ö", "o")
        .replace("ç", "c")
    )


def _result_key(value) -> str:
    return _fold_text(value)


def _direction_key(value) -> str:
    return "alt" if _fold_text(value) == "alt" else "ust"


def _safe_signal_no(signal: dict) -> int:
    try:
        return int(signal.get("signal_count") or 0)
    except (TypeError, ValueError):
        return 0


def _safe_match_key(signal: dict) -> str:
    return str(
        signal.get("match_id")
        or f"{signal.get('match_name', '-')}-{signal.get('tournament', '-')}-{signal.get('id', '-')}"
    )


def _safe_float(value) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _is_suspicious_signal(signal: dict) -> bool:
    total = _safe_float(signal.get("final_total"))
    return _fold_text(signal.get("final_status")) == "ft" and total is not None and total < 100


def _pct(success: int, total: int) -> float:
    if total <= 0:
        return 0.0
    return round((success / total) * 100, 1)


def _direction_stats(signals: list[dict], direction: str) -> dict:
    scoped = [signal for signal in signals if _direction_key(signal.get("direction")) == direction]
    successful = [signal for signal in scoped if _result_key(signal.get("result")) == "basarili"]
    failed = [signal for signal in scoped if _result_key(signal.get("result")) == "basarisiz"]
    unique_success_matches = len({_safe_match_key(signal) for signal in successful})
    return {
        "total": len(scoped),
        "success": len(successful),
        "fail": len(failed),
        "success_rate": _pct(len(successful), len(successful) + len(failed)),
        "unique_success_matches": unique_success_matches,
        "repeated_success_signals": len(successful) - unique_success_matches,
    }


def build_finished_matches_report(signals: list[dict]) -> dict:
    if not signals:
        return {
            "generated_at": datetime.now().isoformat(timespec="seconds"),
            "headline": "Arşivde raporlanacak biten maç sinyali yok.",
            "summary": "Önce biten maç kontrolü çalıştırıldığında burada otomatik analiz görünecek.",
            "cards": [],
            "highlights": [],
            "mixed_examples": [],
            "suspicious_examples": [],
        }

    grouped: dict[str, dict] = {}
    for signal in signals:
        key = _safe_match_key(signal)
        if key not in grouped:
            grouped[key] = {
                "match_name": signal.get("match_name") or "-",
                "tournament": signal.get("tournament") or "-",
                "signals": [],
            }
        grouped[key]["signals"].append(signal)

    match_groups = []
    for key, group in grouped.items():
        ordered = sorted(
            group["signals"],
            key=lambda signal: (_safe_signal_no(signal), signal.get("alerted_at") or "", signal.get("id") or 0),
        )
        success_numbers = []
        fail_numbers = []
        push_numbers = []
        suspicious_count = 0

        for signal in ordered:
            result_key = _result_key(signal.get("result"))
            signal_no = _safe_signal_no(signal)
            if result_key == "basarili":
                success_numbers.append(signal_no)
            elif result_key == "basarisiz":
                fail_numbers.append(signal_no)
            else:
                push_numbers.append(signal_no)

            if _is_suspicious_signal(signal):
                suspicious_count += 1

        match_groups.append(
            {
                "key": key,
                "match_name": group["match_name"],
                "tournament": group["tournament"],
                "total_signals": len(ordered),
                "success_count": len(success_numbers),
                "fail_count": len(fail_numbers),
                "push_count": len(push_numbers),
                "success_numbers": success_numbers,
                "fail_numbers": fail_numbers,
                "push_numbers": push_numbers,
                "suspicious_count": suspicious_count,
                "sequence": ordered,
            }
        )

    total_signals = len(signals)
    success_signals = sum(1 for signal in signals if _result_key(signal.get("result")) == "basarili")
    fail_signals = sum(1 for signal in signals if _result_key(signal.get("result")) == "basarisiz")
    push_signals = sum(1 for signal in signals if _result_key(signal.get("result")) == "iade")
    unique_matches = len(match_groups)
    repeated_matches = sum(1 for group in match_groups if group["total_signals"] > 1)
    extra_signals = total_signals - unique_matches
    mixed_matches = [group for group in match_groups if group["success_count"] > 0 and group["fail_count"] > 0]
    suspicious_signals = [signal for signal in signals if _is_suspicious_signal(signal)]
    suspicious_matches = [group for group in match_groups if group["suspicious_count"] > 0]

    alt_stats = _direction_stats(signals, "alt")
    ust_stats = _direction_stats(signals, "ust")

    high_number_signals = [signal for signal in signals if _safe_signal_no(signal) >= 4]
    high_number_success = sum(1 for signal in high_number_signals if _result_key(signal.get("result")) == "basarili")
    quality_b_or_better = [
        signal for signal in signals if str(signal.get("quality_grade") or "").upper() in {"A++", "A+", "A", "B"}
    ]
    quality_b_or_better_success = sum(
        1 for signal in quality_b_or_better if _result_key(signal.get("result")) == "basarili"
    )

    diff = abs(alt_stats["success_rate"] - ust_stats["success_rate"])
    if suspicious_signals:
        headline = "Temkinli yaklaşmak daha doğru: arşivde veri kalitesi sorunu da var."
    elif _pct(success_signals, success_signals + fail_signals) >= 65:
        headline = "Genel tablo olumlu; sinyaller arşivde avantajlı görünüyor."
    elif _pct(success_signals, success_signals + fail_signals) >= 55:
        headline = "Sınırlı bir avantaj var, ama çok güvenli bir tablo değil."
    else:
        headline = "Net bir üstünlük görünmüyor; sistemi tek başına güvenilir saymak erken."

    if diff < 5:
        direction_summary = "ALT ve ÜST performansı birbirine yakın."
    elif alt_stats["success_rate"] > ust_stats["success_rate"]:
        direction_summary = f"ALT tarafı {diff:.1f} puan önde."
    else:
        direction_summary = f"ÜST tarafı {diff:.1f} puan önde."

    summary = (
        f"Toplam {total_signals} sinyal, {unique_matches} benzersiz maça dağılmış durumda. "
        f"Genel başarı oranı %{_pct(success_signals, success_signals + fail_signals):.1f}. "
        f"{direction_summary} Karışık maç sayısı {len(mixed_matches)}. "
        f"Şüpheli FT kaydı {len(suspicious_signals)}."
    )

    mixed_examples = []
    for group in sorted(mixed_matches, key=lambda item: (-item["total_signals"], item["match_name"]))[:5]:
        parts = []
        if group["fail_numbers"]:
            parts.append(
                "Başarısız: " + ", ".join(f"#{number}" for number in group["fail_numbers"] if number)
            )
        if group["success_numbers"]:
            parts.append(
                "Başarılı: " + ", ".join(f"#{number}" for number in group["success_numbers"] if number)
            )
        if group["push_numbers"]:
            parts.append("İade: " + ", ".join(f"#{number}" for number in group["push_numbers"] if number))
        mixed_examples.append(
            {
                "match_name": group["match_name"],
                "tournament": group["tournament"],
                "signal_count": group["total_signals"],
                "summary": " | ".join(parts),
            }
        )

    suspicious_examples = []
    for signal in sorted(
        suspicious_signals,
        key=lambda item: (item.get("finished_at") or "", item.get("id") or 0),
        reverse=True,
    )[:5]:
        suspicious_examples.append(
            {
                "match_name": signal.get("match_name") or "-",
                "tournament": signal.get("tournament") or "-",
                "direction": signal.get("direction") or "-",
                "final_status": signal.get("final_status") or "-",
                "final_score": signal.get("final_score") or "-",
                "final_total": signal.get("final_total"),
            }
        )

    cards = [
        {
            "title": "Toplam sinyal",
            "value": total_signals,
            "detail": f"{unique_matches} benzersiz maç, {extra_signals} ekstra tekrar kaydı.",
        },
        {
            "title": "Genel başarı",
            "value": f"%{_pct(success_signals, success_signals + fail_signals):.1f}",
            "detail": f"{success_signals} başarılı, {fail_signals} başarısız, {push_signals} iade.",
        },
        {
            "title": "ALT vs UST",
            "value": f"%{alt_stats['success_rate']:.1f} / %{ust_stats['success_rate']:.1f}",
            "detail": f"ALT {alt_stats['success']}/{alt_stats['total']} | ÜST {ust_stats['success']}/{ust_stats['total']}.",
        },
        {
            "title": "Karışık maç",
            "value": len(mixed_matches),
            "detail": "Aynı maç içinde hem başarılı hem başarısız sinyal veren maç sayısı.",
        },
        {
            "title": "4+ sinyaller",
            "value": f"%{_pct(high_number_success, len(high_number_signals)):.1f}",
            "detail": f"{len(high_number_signals)} kayıtta #4 ve sonrası sinyallerin başarı oranı.",
        },
        {
            "title": "Kalite B ve üzeri",
            "value": f"%{_pct(quality_b_or_better_success, len(quality_b_or_better)):.1f}",
            "detail": f"{len(quality_b_or_better)} kayıtta kalite notu B ve üzeri olan sinyaller.",
        },
        {
            "title": "Şüpheli FT",
            "value": len(suspicious_signals),
            "detail": f"{len(suspicious_matches)} maçta basketbol için anormal düşük final total görüldü.",
        },
    ]

    highlights = [
        (
            f"ALT tarafı %{alt_stats['success_rate']:.1f}, ÜST tarafı %{ust_stats['success_rate']:.1f}. "
            f"ALT biraz önde olsa da fark çok büyük değil."
        ),
        (
            f"ALT başarılarının {alt_stats['success']} kaydının yalnızca {alt_stats['unique_success_matches']} tanesi "
            f"benzersiz maçtan geliyor; {alt_stats['repeated_success_signals']} tanesi aynı maç tekrarından."
        ),
        (
            f"ÜST başarılarının {ust_stats['success']} kaydının {ust_stats['unique_success_matches']} tanesi "
            f"benzersiz maçtan geliyor; {ust_stats['repeated_success_signals']} tanesi aynı maç tekrarından."
        ),
        (
            f"#4 ve sonrası sinyallerin başarı oranı %{_pct(high_number_success, len(high_number_signals)):.1f}; "
            f"#1-3 aralığında bu oran daha düşük kalıyor."
        ),
    ]

    if suspicious_signals:
        highlights.append(
            f"En büyük risk veri kalitesi: {len(suspicious_signals)} kayıtta durum FT iken final total 100'ün altında."
        )

    return {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "headline": headline,
        "summary": summary,
        "cards": cards,
        "highlights": highlights,
        "mixed_examples": mixed_examples,
        "suspicious_examples": suspicious_examples,
    }


@finished_matches_bp.route("/finished-matches")
def finished_matches_page():
    return render_template("finished_matches.html")


@finished_matches_bp.route("/api/finished-matches")
def api_finished_matches():
    return jsonify(db.recent_finished_matches(limit=1000))


@finished_matches_bp.route("/api/finished-matches/report")
def api_finished_matches_report():
    signals = db.recent_finished_matches(limit=None)
    return jsonify(build_finished_matches_report(signals))


@finished_matches_bp.route("/api/finished-matches/check-now", methods=["POST"])
def api_finished_matches_check_now():
    if not manual_check_lock.acquire(blocking=False):
        return jsonify({"error": "running"}), 409

    try:
        from finished_match_service import run_finished_match_cycle

        summary = asyncio.run(run_finished_match_cycle(db, config))
        return jsonify(summary)
    finally:
        manual_check_lock.release()


@finished_matches_bp.route("/api/finished-matches/<int:finished_match_id>", methods=["DELETE"])
def api_delete_finished_match(finished_match_id: int):
    deleted = db.delete_finished_match(finished_match_id)
    if not deleted:
        return jsonify({"error": "not found"}), 404
    return jsonify({"id": finished_match_id, "deleted": True})


@finished_matches_bp.route("/api/finished-matches/clear", methods=["POST"])
def api_clear_finished_matches():
    deleted_count = db.clear_finished_matches()
    return jsonify({"cleared": True, "deleted_count": deleted_count})
