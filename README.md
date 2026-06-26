# Basketball Odds Monitor (AIScore)

This project monitors live basketball matches on AIScore and detects Total Points line anomalies by comparing the opening total with the live total. When the configured threshold is reached, it sends Telegram alerts and stores events in SQLite.

## Current Features

- Live AIScore scraping for basketball totals
- Opening total vs live total anomaly detection
- Telegram alerts (single or multiple chat IDs)
- Per-match signal cap to avoid spam
- Projection-based review for live total context
- Flask dashboard for reviewing alerts and actions
- Country/league visibility in tournament field (for example `Japan : Japan League B3`)
- Repeated-signal indicator (for example `2nd signal`, `3rd signal`)
- Optional keyword blacklist (for example `NBA,China,CBA`)

## Alert Logic

| Condition | Direction |
|---|---|
| Live - Reference >= THRESHOLD | `ALT` candidate |
| Reference - Live >= THRESHOLD | `UST` candidate |

Reference means the opening total. The app stores the resulting `ALT` or `ÜST` signal with its live context and reason.

## Architecture

- `main.py`: Bot loop, anomaly evaluation, alert orchestration
- `aiscore_scraper.py`: AIScore scraping and odds extraction
- `notifier.py`: Telegram messaging
- `projection.py`: Current-pace final total projection
- `db.py`: SQLite schema and data access
- `dashboard.py`: Flask API + UI backend
- `templates/dashboard.html`: Dashboard frontend

## Quick Start (Linux)

### 1. Install dependencies

```bash
sudo apt update
sudo apt install -y python3 python3-venv
```

### 2. Set up project environment

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

Required values:

```ini
TELEGRAM_TOKEN=your_bot_token
TELEGRAM_CHAT_ID=chat_id_or_comma_separated_ids
```

Optional filtering:

```ini
BLACKLIST=NBA,China,CBA
```

### 4. Run the bot

```bash
source venv/bin/activate
python main.py
```

## Dashboard

Run dashboard server:

```bash
python run.py
```

Default URL:

- `http://localhost:5050`

Dashboard capabilities:

- View recent alerts (table and mobile cards)
- Mark alerts as bet placed / ignored
- Delete alerts
- Open signal quality details per alert
- See repeated signal badges

## Environment Variables

| Variable | Description |
|---|---|
| `TELEGRAM_TOKEN` | Telegram bot token (required) |
| `TELEGRAM_CHAT_ID` | One or more chat IDs, comma-separated |
| `THRESHOLD` | Trigger threshold in points |
| `POLL_INTERVAL_MIN` | Minimum loop delay (seconds) |
| `POLL_INTERVAL_MAX` | Maximum loop delay (seconds) |
| `MAX_SIGNALS_PER_MATCH` | Maximum stored signals per match |
| `SAME_DIRECTION_MIN_LIVE_DELTA` | Minimum live-total difference before repeating the same direction (default: 10) |
| `DB_PATH` | SQLite file path |
| `LOG_LEVEL` | `DEBUG`, `INFO`, `WARNING` |
| `AISCORE_URL` | AIScore basketball page |
| `AISCORE_TIMEZONE` | Timezone used for upcoming match date filtering |
| `UPCOMING_DAYS_AHEAD` | Upcoming date window; `0` means today only |
| `UPCOMING_MAX_MATCHES` | Maximum upcoming matches to detail per fetch; `0` means no cap |
| `UPCOMING_FETCH_TIMEOUT_SECONDS` | Total timeout for one upcoming fetch job |
| `UPCOMING_MATCH_TIMEOUT_SECONDS` | Timeout for one upcoming match detail page |
| `MAX_MATCHES_PER_CYCLE` | Scan cap per loop |
| `PAGE_TIMEOUT_MS` | Playwright page timeout |
| `BLACKLIST` | Comma-separated keywords to skip matches |

## Notes on Blacklist Matching

Blacklist terms are matched case-insensitively against:

- Match name
- Tournament/country text
- Match URL

Example:

```ini
BLACKLIST=NBA,China,CBA,WNBA
```

## Troubleshooting

### No Telegram alerts

- Verify `TELEGRAM_TOKEN` and `TELEGRAM_CHAT_ID`
- Set `THRESHOLD=1` temporarily for quick testing

### `.env` changes do not apply

The app reads `.env` at startup. Restart the process after any `.env` update.
