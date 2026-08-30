# Basket Odds Monitor

AIScore basketbol maçlarında aynı bahis şirketine ait açılış ve canlı toplam
baremini karşılaştıran Python/Flask uygulaması.

Bir sinyal yalnız şu koşulla oluşur:

```text
abs(canlı toplam - açılış toplam) >= THRESHOLD
```

Canlı barem açılıştan yüksekse yön `ALT`, düşükse `ÜST` olur. Uygulama
projeksiyon, adil barem, H2H, takım formu, tempo, kalite puanı veya kanıt skoru
hesaplamaz.

## Korunan operasyonel kurallar

- Açılış ve canlı toplam mümkün olduğunda aynı bookmaker satırından eşlenir.
- Aynı maçta dönem başına en fazla bir sinyal oluşur.
- Maç başına toplam sinyal sınırı ve aynı yöndeki minimum canlı-barem farkı uygulanır.
- Kara listedeki takım/lig sinyal ve bekleyen Telegram gönderimini engeller.
- Sinyaller SQLite'a yazılır; Telegram teslimatı kalıcı outbox ile tekrar denenir.
- Final skorlar otomatik kontrol edilerek arşivlenen sinyaller sonuçlandırılır.
- Dashboard aktif sinyalleri, arşivi, yaklaşan maçları ve takip işlemlerini gösterir.

## Kurulum

```bash
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
python -m camoufox fetch
```

`.env` için temel alanlar:

```dotenv
TELEGRAM_TOKEN=...
TELEGRAM_CHAT_ID=...
AISCORE_URL=https://m.aiscore.com/basketball
THRESHOLD=10
DB_PATH=basketball.db
PLAYWRIGHT_PROXY=socks5://127.0.0.1:9050
```

Canlı bot `python main.py`, birleşik dashboard `python run.py` ile çalışır.
Servis kurulumunda bu süreçler systemd tarafından yönetilebilir.

## Test

```bash
python -m pytest -q
python -m compileall -q .
```

Yerel veritabanı, loglar, `venv/` ve `__pycache__/` kaynak dosya değildir.
