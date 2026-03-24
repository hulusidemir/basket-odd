# Basketball Odds Monitor (AIScore)

This project monitors live basketball matches on AIScore and detects Total Points line anomalies by comparing opening and live totals. When the configured threshold is reached, it sends Telegram alerts, stores events in SQLite, and can generate short AI analysis replies using Gemini with Google Search grounding.

## Current Features

- Live AIScore scraping for basketball totals
- Opening vs live total anomaly detection
- Telegram alerts (single or multiple chat IDs)
- Per-match cooldown to avoid spam
- AI follow-up analysis (Gemini) as a reply to the original alert
- Flask dashboard for reviewing alerts and actions
- Country/league visibility in tournament field (for example `Japan : Japan League B3`)
- Repeated-signal indicator (for example `2nd signal`, `3rd signal`)
- Optional keyword blacklist (for example `NBA,China,CBA`)

## Alert Logic

| Condition | Direction |
|---|---|
| Live - Opening >= THRESHOLD | `ALT` (line moved up) |
| Opening - Live >= THRESHOLD | `UST` (line moved down) |

The bot checks cooldown per match and direction before sending a new alert.

## AI Analysis Flow

1. The alert is sent immediately.
2. AI analysis runs in the background.
3. The analysis is sent as a Telegram reply to the original alert.
4. The analysis is stored in the database and shown on the dashboard.

AI output is configured to stay concise and practical.

## Architecture

- `main.py`: Bot loop, anomaly evaluation, alert orchestration
- `aiscore_opera_scraper.py`: AIScore scraping and odds extraction
- `notifier.py`: Telegram messaging
- `analyzer.py`: Gemini analysis with Google Search tool
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
BROWSER_MODE=headless
```

Optional AI values:

```ini
GEMINI_API_KEY=your_gemini_api_key
GEMINI_MODEL=gemini-2.5-flash
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

## Quick Start (Windows / Opera VPN Mode)

Use this mode when AIScore is blocked by your ISP.

1. Install Python 3.10+ and Opera.
2. Enable Opera VPN once in browser settings.
3. Install dependencies:

```powershell
cd C:\path\to\basket-odd
python -m venv venv
.\venv\Scripts\activate
pip install -r requirements.txt
python -m playwright install chromium
```

4. Configure `.env`:

```ini
TELEGRAM_TOKEN=your_bot_token
TELEGRAM_CHAT_ID=your_chat_id
BROWSER_MODE=opera
```

5. Run:

```powershell
.\venv\Scripts\activate
python main.py
```

## Dashboard

Run dashboard server:

```bash
python dashboard.py
```

Default URL:

- `http://localhost:5050`

Dashboard capabilities:

- View recent alerts (table and mobile cards)
- Mark alerts as bet placed / ignored
- Delete alerts
- Open AI analysis modal per alert
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
| `BROWSER_MODE` | `opera` or `headless` |
| `OPERA_CDP_URL` | Opera CDP URL |
| `OPERA_CDP_PORT` | Opera CDP port |
| `OPERA_BINARY` | Optional Opera executable path |
| `AISCORE_URL` | AIScore basketball page |
| `MAX_MATCHES_PER_CYCLE` | Scan cap per loop |
| `PAGE_TIMEOUT_MS` | Playwright page timeout |
| `GEMINI_API_KEY` | Gemini API key (optional) |
| `GEMINI_MODEL` | Gemini model name |
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

### No Gemini analysis received

- Ensure `GEMINI_API_KEY` is set in `.env`
- Install dependency in the runtime environment:

```bash
pip install google-genai
```

- Restart the bot after `.env` changes

### No Telegram alerts

- Verify `TELEGRAM_TOKEN` and `TELEGRAM_CHAT_ID`
- Set `THRESHOLD=1` temporarily for quick testing

### AIScore access issues

- Use `BROWSER_MODE=opera` with Opera VPN on restricted networks
- Increase logging with `LOG_LEVEL=DEBUG`

### `.env` changes do not apply

The app reads `.env` at startup. Restart the process after any `.env` update.

