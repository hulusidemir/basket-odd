# Basketball Odds Monitor (AIScore)

A bot that monitors live basketball matches on AIScore, comparing **Opening odds** vs **In-play odds** for Total Points. When the difference exceeds a configurable threshold, it sends a Telegram notification.

## Alert Rules

| Condition | Direction |
|---|---|
| In-play − Opening ≥ THRESHOLD | **ALT** (line went up) |
| Opening − In-play ≥ THRESHOLD | **ÜST** (line went down) |

Default threshold: **10 points**. Cooldown: **15 minutes** per match per direction.

## How It Works

Each cycle:
1. Opens `https://www.aiscore.com/basketball` and collects live match links.
2. Navigates to each match's `/odds` page.
3. Reads the first bookmaker's **Total Points** column — **1st row** (opening) and **3rd row** (in-play).
4. If the difference ≥ threshold → sends a Telegram alert.
5. Waits 25–40 seconds (randomized) before the next cycle.

## Browser Modes

| Mode | `BROWSER_MODE` | Use Case |
|---|---|---|
| **Opera** | `opera` | Local machine where ISP blocks AIScore. Opera's built-in VPN bypasses the block. |
| **Headless** | `headless` | Server/VPS where AIScore is accessible without VPN. No GUI needed. |

In Opera mode, the bot **automatically finds and launches Opera** with the correct CDP port — no manual steps needed.

---

## Setup — Windows (Opera Mode)

### 1. Install prerequisites

- [Python 3.10+](https://www.python.org/downloads/) — check **"Add Python to PATH"** during install
- [Opera Browser](https://www.opera.com/)

### 2. Enable Opera VPN (one time only)

Open Opera → Settings → VPN → Enable VPN. Close Opera. The setting is saved in the profile and reused automatically.

### 3. Install dependencies

Open PowerShell and navigate to the project folder:

```powershell
cd C:\path\to\basket-odd
python -m venv venv
.\venv\Scripts\activate
pip install -r requirements.txt
python -m playwright install chromium
```

### 4. Configure environment

```powershell
copy .env.example .env
```

Edit `.env`:

```ini
TELEGRAM_TOKEN=your_real_token
TELEGRAM_CHAT_ID=your_real_chat_id
BROWSER_MODE=opera
```

All other values have sensible defaults. Opera binary path and CDP port are auto-detected.

### 5. Run

```powershell
.\venv\Scripts\activate
python main.py
```

Opera will open minimized with VPN enabled. The bot runs in your terminal.

---

## Setup — Linux Server (Headless Mode)

### 1. Install prerequisites

```bash
sudo apt update && sudo apt install -y python3 python3-venv
```

### 2. Install dependencies

```bash
cd /path/to/basket-odd
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
python -m playwright install chromium
python -m playwright install-deps
```

### 3. Configure environment

```bash
cp .env.example .env
```

Edit `.env`:

```ini
TELEGRAM_TOKEN=your_real_token
TELEGRAM_CHAT_ID=your_real_chat_id
BROWSER_MODE=headless
```

### 4. Run

```bash
source venv/bin/activate
python main.py
```

For persistent background execution:

```bash
nohup python main.py > bot.log 2>&1 &
```

---

## Configuration Reference (.env)

| Variable | Default | Description |
|---|---|---|
| `TELEGRAM_TOKEN` | — | Telegram Bot API token (required) |
| `TELEGRAM_CHAT_ID` | — | Telegram chat ID for alerts (required) |
| `BROWSER_MODE` | `opera` | `opera` or `headless` |
| `THRESHOLD` | `10` | Minimum point difference to trigger alert |
| `ALERT_COOLDOWN_MINUTES` | `15` | Minutes before re-alerting same match + direction |
| `POLL_INTERVAL_MIN` | `25` | Minimum seconds between cycles |
| `POLL_INTERVAL_MAX` | `40` | Maximum seconds between cycles |
| `MAX_MATCHES_PER_CYCLE` | `80` | Max matches to scan per cycle |
| `PAGE_TIMEOUT_MS` | `30000` | Page load timeout (ms) |
| `LOG_LEVEL` | `INFO` | `DEBUG`, `INFO`, or `WARNING` |
| `AISCORE_URL` | `https://www.aiscore.com/basketball` | AIScore basketball page |
| `OPERA_CDP_URL` | `http://127.0.0.1:9222` | Opera CDP address (opera mode only) |
| `OPERA_CDP_PORT` | `9222` | Opera CDP port (opera mode only) |
| `OPERA_BINARY` | auto-detect | Opera executable path override (opera mode only) |
| `DB_PATH` | `basketball.db` | SQLite database file path |

## Troubleshooting

### "Opera not found" error
Opera is installed in a non-standard location. Set `OPERA_BINARY` in `.env`:
```ini
OPERA_BINARY=C:\Users\YourName\AppData\Local\Programs\Opera\opera.exe
```

### No matches found
- AIScore may show a captcha or popup. Open the site manually in Opera once and dismiss any popups.
- Set `LOG_LEVEL=DEBUG` to see detailed scraping output.
- Check `debug_aiscore.png` (auto-generated when no matches are found).

### No Telegram notifications
- Verify `TELEGRAM_TOKEN` and `TELEGRAM_CHAT_ID` are correct.
- For testing, set `THRESHOLD=1` to trigger easily.

### CDP connection failed (Opera mode)
- The bot auto-launches Opera. If it still fails, ensure no other Opera instance is running on the same port.
- Try killing existing Opera processes and restarting the bot.
