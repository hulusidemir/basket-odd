# Basket Tahmin Botu (AIScore + Opera VPN)

Bu bot, AIScore'da canlı basketbol maçlarını gezer ve her maçta:
- Opening odds -> Total Points
- In-play odds -> Total Points

değerlerini karşılaştırır.

Fark `THRESHOLD` (varsayılan 10) ve üzerindeyse Telegram bildirimi yollar.

Kurallar:
- In-play - Opening >= 10 -> ALT
- Opening - In-play >= 10 -> UST

## Neden Opera?

AIScore bazı ağlarda masaüstünden erişimi engelleyebiliyor.
Opera'nın kendi VPN'i ile sadece Opera trafiğini VPN'den geçirip botu çalıştırıyoruz.

## Kurulum

1) Sanal ortam

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
venv/bin/playwright install chromium
```

2) Ortam dosyası

```bash
cp .env.example .env
```

`.env` içine şunları gir:

- `TELEGRAM_TOKEN`
- `TELEGRAM_CHAT_ID`
- gerekirse `THRESHOLD`

3) Opera'yı CDP ile aç

Önce Opera içinde VPN'i ON yap.
Sonra terminalden Opera'yı uzaktan kontrol portuyla başlat:

```bash
opera --remote-debugging-port=9222 --user-data-dir=$HOME/.opera-cdp-profile
```

Notlar:
- Sisteminde komut `opera-beta` veya başka isimde olabilir.
- Bot bu açık Opera oturumuna bağlanır.
- Bot ile Opera ayni makinede olmalı. SSH ile baska sunucuda calistirirsan `127.0.0.1:9222` baglanamaz.

## Çalıştırma

Yeni bir terminal aç:

```bash
cd basket-tahmin
source venv/bin/activate
python main.py
```

## Nasıl Çalışır?

Her döngüde:
1. `https://www.aiscore.com/basketball` sayfasına gider.
2. Maç linklerini toplar.
3. Her maça girer.
4. `Odds` sekmesini açar.
5. `Opening odds` ve `In-play odds` satırlarından `Total Points` line değerlerini okur.
6. Fark `THRESHOLD` ve üzerindeyse Telegram bildirimi gönderir.

## Onemli Ayarlar (.env)

- `THRESHOLD=10`
- `POLL_INTERVAL_MIN=25`
- `POLL_INTERVAL_MAX=40`
- `OPERA_CDP_URL=http://127.0.0.1:9222`
- `AISCORE_URL=https://www.aiscore.com/basketball`
- `MAX_MATCHES_PER_CYCLE=40`
- `PAGE_TIMEOUT_MS=30000`

## Sorun Giderme

1) `CDP bağlanamadı` hatası:
- Opera'yı `--remote-debugging-port=9222` ile başlattığından emin ol.
- `.env` içindeki `OPERA_CDP_URL` doğru mu kontrol et.

2) Maç bulunamıyor:
- AIScore sayfasında captcha/popup olabilir.
- Opera penceresinde siteyi bir kez manuel açıp popup'ları kapat.

3) Bildirim gelmiyor:
- `TELEGRAM_TOKEN` ve `TELEGRAM_CHAT_ID` doğru mu kontrol et.
- İlk test için `THRESHOLD=1` yapıp dene.
