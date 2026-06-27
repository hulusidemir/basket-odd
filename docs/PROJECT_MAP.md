# Project Map

## Klasör Yapısı

- `main.py`: canlı takip döngüsü ve alert oluşturma.
- `dashboard.py`: Flask uygulaması, API route'ları, silinen maç raporları, dashboard veri şekillendirme.
- `run.py`: dashboard başlatıcı; `bankroll` ve `balance_tracker` blueprint'lerini bağlar, zamanlanmış işleri başlatır.
- `db.py`: SQLite şeması, migration'lar, CRUD, alert/action kalıcılığı.
- `config.py`: ortam değişkenlerini yükleme ve doğrulama.
- `aiscore_scraper.py`: canlı AIScore scraping.
- `upcoming_scraper.py`, `upcoming_app.py`: yaklaşan maç scraping ve UI/API.
- `finished_match_service.py`: biten maç taramaları ve sonuçlandırma.
- `signal_analysis.py`, `signal_quality.py`, `signal_repeat.py`, `projection.py`, `pace_tracker.py`: sinyal analizi, kalite, tekrar koruması, projeksiyon ve tempo takibi.
- `notifier.py`: Telegram mesajları.
- `templates/`: dashboard, silinen maçlar, bankroll ve yaklaşan maçlar HTML/CSS/JS.
- `static/`: statik varlıklar.
- `balance_tracker/`: ayrı balance tracker blueprint'i ve template'i.
- `tests/`: unit testler.

## Genel Veri Akışı

1. `main.py`, `Config`, `Database`, `TelegramNotifier`, `AiscoreScraper` ve `PaceTracker` kurar.
2. `AiscoreScraper` canlı AIScore toplam sayı verilerini okur.
3. `main.process_match()` maçları filtreler, yön hesaplar, analiz üretir, tekrar koruması uygular, alert'i `db.py` üzerinden kaydeder ve Telegram gönderir.
4. `dashboard.py` aktif/silinen alert'leri okur, analiz/kalite/history ile zenginleştirir ve JSON API'leri sunar.
5. Template'lerdeki kullanıcı aksiyonları dashboard API'lerini çağırır; `alerts` ve `match_actions` güncellenir.
6. `finished_match_service.py` final skorlarını kontrol eder ve alert sonuçlarını kapatır.

## Hangi İş İçin Nereye Bakılır

- Canlı sinyal üretimi: `main.py`, `signal_analysis.py`, `signal_quality.py`, `signal_repeat.py`.
- Scraper sorunları: `aiscore_scraper.py`, `upcoming_scraper.py`, `live_matches_worker.py`.
- Dashboard API/UI: `dashboard.py`, `templates/dashboard.html`, `templates/deleted_matches.html`.
- Silinen maç/sonuç mantığı: `finished_match_service.py`, `dashboard.py` içindeki deleted-match route'ları.
- Veritabanı değişiklikleri: önce `db.py`, sonra etkilenen çağıran dosyalar.
- Telegram çıktısı: `notifier.py`.
- Yaklaşan maçlar: `upcoming_app.py`, `upcoming_scraper.py`, `templates/upcoming_matches.html`.
- Bankroll/balance araçları: `bankroll.py`, `templates/bankroll.html`, `balance_tracker/`.

## Kritik Modüller

`db.py`, `main.py`, `dashboard.py`, `finished_match_service.py`, `signal_analysis.py` ve `signal_quality.py` en yüksek etki alanına sahiptir. Buradaki değişiklikler alert gönderimini, sonuç istatistiklerini veya saklanan tarihsel veriyi etkileyebilir.

## Hassas Yerler

SQLite migration'ları, sonuçlandırma yönü, Telegram gönderim koşulları, scraper selector'ları, süre/period parsing ve silinen maç snapshot davranışı dikkat ister. `.env`, Telegram config, yerel DB veya private/local artifact içinden gizli veri loglama ya da dokümante etme.
