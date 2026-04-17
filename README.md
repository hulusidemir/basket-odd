# Basketball Odds Monitor (AIScore)

This project monitors live basketball matches on AIScore and detects Total Points line anomalies by comparing the latest pre-match total with the live total. If a pre-match line is unavailable, it falls back to the opening line. When the configured threshold is reached, it sends Telegram alerts and stores events in SQLite.

## Current Features

- Live AIScore scraping for basketball totals
- Pre-match vs live total anomaly detection, with opening-line fallback
- Telegram alerts (single or multiple chat IDs)
- Per-match cooldown to avoid spam
- Signal quality scoring and projection-based review
- Flask dashboard for reviewing alerts and actions
- Country/league visibility in tournament field (for example `Japan : Japan League B3`)
- Repeated-signal indicator (for example `2nd signal`, `3rd signal`)
- Optional keyword blacklist (for example `NBA,China,CBA`)

## Alert Logic

| Condition | Direction |
|---|---|
| Live - Reference >= THRESHOLD | `ALT` (line moved up) |
| Reference - Live >= THRESHOLD | `UST` (line moved down) |

Reference means pre-match total first, opening total if no pre-match line is available. The bot checks cooldown per match and direction before sending a new alert.

## Architecture

- `main.py`: Bot loop, anomaly evaluation, alert orchestration
- `aiscore_scraper.py`: AIScore scraping and odds extraction
- `notifier.py`: Telegram messaging
- `signal_quality.py`: Signal scoring and counter-signal logic
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
| `ALERT_COOLDOWN_MINUTES` | Cooldown for same match + direction |
| `DB_PATH` | SQLite file path |
| `LOG_LEVEL` | `DEBUG`, `INFO`, `WARNING` |
| `AISCORE_URL` | AIScore basketball page |
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
