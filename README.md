# Basketball Odds Monitor (AIScore)

This Python/Flask application monitors AIScore basketball totals, stores opening-vs-live line anomalies in SQLite, and evaluates them with versioned projection, fair-line, data-quality, and prospective evidence rules. Crossing `THRESHOLD` creates a stored alert and sends it to Telegram with a `PAS`, `TEST`, or `ONAY` label; the label, not delivery, communicates playability.

The current `trusted_70_v2` policy treats 70% as a validation target, not as a proven or guaranteed win rate. Until the fixed strategy has enough automatically settled forward trials, eligible signals remain `SHADOW`; signals that fail candidate or data requirements are `BLOCKED`.

## Current Features

- Live AIScore scraping with bounded retries, concurrency, per-match failure isolation, and coverage reports
- Opening/live totals taken from the first readable bookmaker block
- Opening-vs-live anomaly storage with per-match, per-period, and repeat-signal protection
- Versioned `shadow_projection_v1` projection and `calibrated_fair_v1` fair line
- Fixed `projection_edge_6_q2q3_v2` research-candidate rule
- Prospective `BLOCKED` / `SHADOW` / `TRUSTED` evidence gate and durable trial ledger
- Telegram outbox with per-recipient delivery IDs and bounded retries; every threshold alert is delivered with its `PAS`, `TEST`, or `ONAY` label
- Flask dashboard for live review, actions, notes, reports, and CSV export
- Deletion-time `display_snapshot`; deleted signals are displayed without recalculating model, star, projection, or fair-line fields
- Separate final-score settlement that requires an explicit final status
- Upcoming-match scraping with provenance, stale/partial health reporting, and non-destructive partial updates
- Optional country/league blacklist, bankroll tools, and balance tracker

## Alert and Trust Logic

### 1. Raw anomaly

| Condition | Initial direction |
|---|---|
| `Live - Opening >= THRESHOLD` | `ALT` |
| `Opening - Live >= THRESHOLD` | `ÜST` |

The raw threshold controls which opening/live anomalies are stored. Lowering it creates more stored alerts but does not bypass model or evidence checks.

### 2. Fixed research candidate

`projection_edge_6_q2q3_v2` requires all of the following:

- A validated 4x10 game clock in Q2 or Q3
- At least a 6-point absolute projection edge versus the live line
- Projection direction aligned with the stored signal direction
- Fair calibration aligned with the projection direction
- Projection data quality of at least 85

Data reliability is assessed separately. It includes valid score/clock/lines, a supported format, no overtime or locked market, live line above the current score, a durable source URL for final settlement, and one bookmaker block with readable opening and in-play totals.

### 3. Prospective trust gate

The active policy is `trusted_70_v2`, strategy version `3`. One bookmaker with readable opening and in-play totals is sufficient; cross-bookmaker count/spread comparison is not a gate. A trial-eligible alert is stored in `signal_trials` once per match and is settled only from an automatic final score. Evidence never mixes another strategy fingerprint or evidence epoch and never uses results that occurred after the alert decision.

`TRUSTED` requires at least 100 unique resolved trials, at least 90% resolution coverage, an overall 95% Wilson lower bound of at least 70%, and two stable 50-match blocks. Until every condition passes, an otherwise eligible candidate is `SHADOW`. `quality_score` is the frozen `basketball_expert_v1` ranking score (data 20, projection edge 30, pace 20, phase 10, game script 10, market context 10); it is not a win probability, gate decision, or Telegram permission.

## Architecture

- `main.py`: live loop, raw anomaly evaluation, alert persistence, and Telegram orchestration
- `aiscore_scraper.py`: live AIScore parsing, paired bookmaker consensus, and scrape health report
- `signal_analysis.py`: projection/fair context, direction, and fixed candidate selection
- `signal_quality.py`: input reliability, frozen expert confidence ranking, and gate-specific model-support score
- `signal_gate.py`: immutable strategy identity and prospective evidence decision
- `projection.py`, `pace_tracker.py`: clock/format-aware projection and pace lifecycle
- `db.py`: SQLite schema, alerts, snapshots, trial ledger, and Telegram outbox
- `finished_match_service.py`: explicit-final settlement for active/deleted alerts
- `notifier.py`: Telegram formatting and delivery
- `dashboard.py`: Flask API and dashboard data shaping
- `upcoming_scraper.py`, `upcoming_app.py`: upcoming-match collection, health, persistence, and API
- `templates/`: dashboard, deleted-signals, upcoming, and bankroll frontends

## Quick Start (Linux)

### 1. Install dependencies

```bash
sudo apt update
sudo apt install -y python3 python3-venv
```

### 2. Set up the project environment

```bash
cd /path/to/basket-odd
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
python -m playwright install chromium
python -m playwright install-deps
```

### 3. Create and edit `.env`

```bash
cp .env.example .env
```

Required for the live bot:

```ini
TELEGRAM_TOKEN=your_bot_token
TELEGRAM_CHAT_ID=chat_id_or_comma_separated_ids
```

Optional filtering:

```ini
BLACKLIST=NBA,China,CBA
```

### 4. Run the live bot

```bash
source venv/bin/activate
python main.py
```

## Dashboard

Run the dashboard and scheduled checks:

```bash
python run.py
```

Default URL: `http://localhost:5151`. Override it with `DASHBOARD_PORT`.

Dashboard capabilities include:

- Review active alerts on desktop and mobile layouts
- Inspect model support, data reliability, gate state, and gate reason codes
- Mark bet/pass actions and add notes
- Delete or automatically archive alerts while freezing their live display snapshot
- Settle only from explicit final results and report deleted-signal outcomes
- Review insights, reports, CSV exports, upcoming matches, and bankroll pages

## Environment Variables

| Variable | Description |
|---|---|
| `TELEGRAM_TOKEN` | Telegram bot token; required by `main.py` |
| `TELEGRAM_CHAT_ID` | One or more recipient IDs, comma-separated |
| `THRESHOLD` | Raw opening-vs-live anomaly threshold in points; does not bypass the trust gate |
| `POLL_INTERVAL_MIN` | Minimum live-loop delay in seconds |
| `POLL_INTERVAL_MAX` | Maximum live-loop delay in seconds |
| `MAX_SIGNALS_PER_MATCH` | Maximum stored alerts per match |
| `SAME_DIRECTION_MIN_LIVE_DELTA` | Required live-line movement before storing the same direction again; default `10` |
| `DB_PATH` | Main SQLite database path; default `basketball.db` |
| `LOG_LEVEL` | Logging level such as `DEBUG`, `INFO`, or `WARNING` |
| `DASHBOARD_PORT` | Dashboard port; default `5151` |
| `AISCORE_URL` | AIScore basketball listing URL |
| `AISCORE_TIMEZONE` | Timezone for upcoming-date filtering; default `Europe/Istanbul` |
| `AISCORE_CONCURRENCY` | Concurrent live detail workers, clamped to `1..8`; default `2` |
| `UPCOMING_CONCURRENCY` | Concurrent upcoming detail workers, clamped to `1..8`; default `2` |
| `UPCOMING_DAYS_AHEAD` | Upcoming window, `0..14`; `0` means today's upcoming listing |
| `UPCOMING_MAX_MATCHES` | Detail limit, default `12`; `0` removes the configured cap, with an internal fetch ceiling of 500 |
| `UPCOMING_FETCH_TIMEOUT_SECONDS` | Configured floor for an upcoming fetch timeout, clamped to `60..3600`; workload budgeting may raise the effective timeout |
| `UPCOMING_MATCH_TIMEOUT_SECONDS` | Per-detail timeout, clamped to `30..180` seconds |
| `UPCOMING_STALE_AFTER_SECONDS` | Age at which an unfinished upcoming job is marked stale, clamped to `300..86400`; default `1800` |
| `MAX_MATCHES_PER_CYCLE` | Maximum live matches scanned per polling cycle |
| `PAGE_TIMEOUT_MS` | Playwright page timeout, validated in the `5000..120000` range |
| `PLAYWRIGHT_PROXY` | Optional Playwright proxy URL; credentials must not be committed or logged |
| `AISCORE_DEBUG_SCREENSHOT` | Optional local screenshot path used when the live listing yields no verified links |
| `BLACKLIST` | Case-insensitive comma-separated terms matched against match, tournament, and URL |
| `BALANCE_TRACKER_DB` | Optional separate balance-tracker SQLite path |
| `BALANCE_TRACKER_PORT` | Standalone balance-tracker port; default `5161` |

## Notes on Blacklist Matching

Blacklist terms are matched case-insensitively against the match name, tournament/country text, and match URL.

```ini
BLACKLIST=NBA,China,CBA,WNBA
```

## Deleted-Signal Data Model

All model calculations belong to the active dashboard flow. At deletion time, the exact display fields are frozen in `display_snapshot`. The deleted-signals page reads that frozen model view and does not rerun current projection, fair-line, quality, or star logic. If the match later finishes, only `final_status`, `final_score`, and `result` are overlaid as settlement metadata. An archived match tied to an unresolved prospective trial is protected from permanent purge until its automatic final result is recorded; manual UI labels never replace that evidence result.

## Troubleshooting

### No Telegram alerts

- Verify `TELEGRAM_TOKEN` and every `TELEGRAM_CHAT_ID`; check startup and delivery logs.
- Inspect the alert's gate state and reason codes. `BLOCKED` and `SHADOW` are intentionally not sent.
- Check whether the fixed v2 trial ledger has enough automatically resolved evidence. A fresh epoch cannot immediately produce `TRUSTED` alerts.
- Check the Telegram outbox state for `retry` or `failed` deliveries.
- Do not lower `THRESHOLD` to bypass validation; it only increases raw stored anomalies.

### Few or no parsed matches

- Inspect the live/upcoming `last_report` fields: listing status, discovered, attempted, parsed, failed, coverage, and partial/error reason.
- Confirm Playwright Chromium is installed and the configured proxy, if any, is reachable.
- AIScore DOM/Nuxt changes require parser/fixture updates; retries cannot repair a changed upstream schema.

### `.env` changes do not apply

The application reads environment variables at process startup. Restart the relevant service after changing `.env`.
