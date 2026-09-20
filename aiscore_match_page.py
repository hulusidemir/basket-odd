"""Read only the requested game's public state, with a DOM scoreboard fallback."""

import re
from urllib.parse import urlsplit, urlunsplit


def mobile_match_url(value: str) -> str:
    parsed = urlsplit(str(value or ""))
    if parsed.hostname not in {"aiscore.com", "www.aiscore.com", "m.aiscore.com"}:
        raise ValueError("Invalid AiScore host")
    path = re.sub(r"/(odds|h2h|stats|lineups|standings|summary)/?$", "", parsed.path.rstrip("/"))
    if not path.startswith("/basketball/match-") or len(path.strip("/").split("/")) != 3:
        raise ValueError("Invalid basketball match URL")
    return urlunsplit(("https", "m.aiscore.com", path, "", ""))


def normalize_match_page(parsed: dict, expected_id: str) -> dict:
    result = dict(parsed or {})
    source = result.pop("sourceMatch", None)
    if not isinstance(source, dict):
        return result
    if str(source.get("id") or "") != expected_id:
        return {"_check_error": "match_identity_mismatch"}

    status_id = source.get("statusId")
    match_status = source.get("matchStatus")
    if status_id is None and match_status is None:
        return result
    # These are explicit completion flags in the mobile basketball source.
    # A live source observation overrides stale FT labels elsewhere in the DOM.
    finished = str(status_id) in {"10", "105"} if status_id is not None else str(match_status) == "3"
    result["sourceVerified"] = True
    result["isFinished"] = finished
    if finished:
        result["status"] = "Full Time"
        scores = [source.get("homeScores"), source.get("awayScores")]
        if all(isinstance(values, list) and len(values) == 5
               and all(type(v) is int and 0 <= v <= 200 for v in values) for values in scores):
            result["score"] = f"{sum(scores[0])} - {sum(scores[1])}"
    else:
        if re.fullmatch(r"Full Time|FT|Finished|Ended|Final", result.get("status", ""), re.I):
            result["status"] = ""
    return result


async def read_match_page(page, expected_id: str, timeout_ms: int) -> dict:
    # Wait for the scoreboard/application state instead of a fixed sleep.
    try:
        await page.wait_for_function(
            """() => !!(window.$nuxt?.$store?.state?.basketball?.basketballDetailMatchData?.match?.id
                || window.__NUXT__?.state?.basketball?.basketballDetailMatchData?.match?.id
                || document.querySelector('.score, .topBox'))""",
            timeout=timeout_ms,
        )
    except Exception:
        pass  # Parse once so blocked/empty pages still get a precise error code.
    return normalize_match_page(await page.evaluate(MATCH_PAGE_JS), expected_id)


MATCH_PAGE_JS = r"""
() => {
  const text = s => (s || '').replace(/\s+/g, ' ').trim();
  const leaf = el => el.children.length === 0;
  const allLeafs = Array.from(document.querySelectorAll('span, div, strong, b'))
    .filter(leaf)
    .map(el => {
      const rect = el.getBoundingClientRect();
      return {
        el,
        txt: text(el.innerText),
        rect,
        size: parseFloat(window.getComputedStyle(el).fontSize) || 0,
        cls: (el.className || '').toString(),
      };
    })
    .filter(o => o.txt && o.rect.width > 0 && o.rect.height > 0);
  const mainLeafs = allLeafs.filter(o => o.rect.top >= 0 && o.rect.top < 460);

  const finishedRe = /^(Full Time|FT|Finished|Ended|Final)$/i;
  const liveRe     = /^(Q[1-4]|[1-4]Q|OT|HT|1st|2nd|3rd|4th|BT)(\s*[-\s]?\s*\d{1,2}:\d{2})?$/i;
  const clockRe    = /^\d{1,2}:\d{2}$/;

  let statusEl = null;
  let status = '';
  let isFinished = false;

  // Do not infer "finished" from score-looking elements or CSS classes.
  // Some live AiScore pages expose final-score-ish containers before the
  // match ends; only an explicit Full Time/FT label may settle results.

  const nums = mainLeafs.filter(o => /^\d{1,3}$/.test(o.txt));

  let score = '';
  let scorePair = null;

  // Strategy A: AiScore renders scores in <div class="score ...">.
  // class~="score" is an exact token match (unlike [class*="score"] substring).
  const scoreClassEls = nums.filter(n =>
    n.cls.split(/\s+/).includes('score') && n.size >= 16
  );
  if (scoreClassEls.length >= 2) {
    scoreClassEls.sort((a, b) => b.size - a.size || a.rect.left - b.rect.left);
    const home = scoreClassEls[0];
    let away = null;
    for (let i = 1; i < scoreClassEls.length; i++) {
      const c = scoreClassEls[i];
      if (Math.abs(c.rect.top - home.rect.top) < 20 && c.rect.left !== home.rect.left) {
        away = c; break;
      }
    }
    if (!away) away = scoreClassEls[1];
    const leftEl  = home.rect.left <= away.rect.left ? home : away;
    const rightEl = home.rect.left <= away.rect.left ? away : home;
    score = `${leftEl.txt} - ${rightEl.txt}`;
    scorePair = [leftEl, rightEl];
  }

  // Strategy B: directly combined "93-62" pattern near the main scoreboard.
  if (!score) {
    const combined = mainLeafs
      .find(o => /^\d{1,3}\s*[-–]\s*\d{1,3}$/.test(o.txt) && o.size >= 16);
    if (combined) {
      score = combined.txt;
      scorePair = [combined, combined];
    }
  }

  // Strategy C (last resort): top-of-page biggest-two heuristic,
  // but require both numbers to share font size and be clearly large.
  if (!score) {
    const top = nums.filter(n => n.rect.top < 300)
      .sort((a, b) => b.size - a.size);
    if (top.length >= 2 && top[0].size >= 20 && Math.abs(top[0].size - top[1].size) < 2) {
      const a = top[0], b = top[1];
      const leftEl  = a.rect.left <= b.rect.left ? a : b;
      const rightEl = a.rect.left <= b.rect.left ? b : a;
      score = `${leftEl.txt} - ${rightEl.txt}`;
      scorePair = [leftEl, rightEl];
    }
  }

  if (scorePair) {
    const left = scorePair[0], right = scorePair[1];
    const scoreCenterY = (
      (left.rect.top + left.rect.bottom) / 2 +
      (right.rect.top + right.rect.bottom) / 2
    ) / 2;
    const minX = Math.min(left.rect.left, right.rect.left) - 220;
    const maxX = Math.max(left.rect.right, right.rect.right) + 220;
    const nearScoreStatus = mainLeafs.filter(o => {
      const cx = (o.rect.left + o.rect.right) / 2;
      const cy = (o.rect.top + o.rect.bottom) / 2;
      return cy >= scoreCenterY - 95
        && cy <= scoreCenterY + 95
        && cx >= minX
        && cx <= maxX
        && (finishedRe.test(o.txt) || liveRe.test(o.txt) || clockRe.test(o.txt));
    });
    const live = nearScoreStatus.find(o => liveRe.test(o.txt) || clockRe.test(o.txt));
    const final = nearScoreStatus.find(o => finishedRe.test(o.txt));
    if (live) {
      statusEl = live.el;
      status = live.txt;
      isFinished = false;
    } else if (final) {
      statusEl = final.el;
      status = 'Full Time';
      isFinished = true;
    }
  }

  if (!status) {
    const live = mainLeafs.find(o => liveRe.test(o.txt) || clockRe.test(o.txt));
    const final = mainLeafs.find(o => finishedRe.test(o.txt) && o.rect.top < 360);
    if (live) {
      statusEl = live.el;
      status = live.txt;
    } else if (final) {
      statusEl = final.el;
      status = 'Full Time';
      isFinished = true;
    }
  }

  // Basketball sanity guard: reject live/non-final and absurd totals before writing anywhere.
  if (!isFinished) {
    score = '';
  } else {
    const m = score.match(/^\s*(\d{1,3})\s*[-–]\s*(\d{1,3})\s*$/);
    if (m) {
      const total = parseInt(m[1]) + parseInt(m[2]);
      if (total < 60 || total > 400) score = '';
    } else {
      score = '';
    }
  }

  const title = text(document.title || '')
    .replace(/\s*\|.*/, '')
    .replace(/\s*-\s*AiScore.*/i, '')
    .replace(/\s*live score.*/i, '')
    .replace(/\s*betting odds.*/i, '')
    .trim();
  const pageText = text(document.body ? document.body.innerText : '').slice(0, 600);

  const candidates = [window.$nuxt?.$store?.state?.basketball,
                      window.__NUXT__?.state?.basketball];
  const basketball = candidates.find(b => b?.basketballDetailMatchData?.match?.id);
  const match = basketball?.basketballDetailMatchData?.match;
  const sourceMatch = match ? {
    id: match.id, statusId: match.statusId, matchStatus: match.matchStatus,
    homeScores: match.homeScores, awayScores: match.awayScores,
  } : null;
  return { status, score, isFinished, title, pageText, sourceMatch };
}
"""
