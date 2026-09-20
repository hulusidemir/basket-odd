# Project Map

- `main.py`: sinyal akışının doğrulama/filtre/kayıt/bildirim adımları, tekrar korumaları ve Telegram outbox.
- `live_signals.py`: maç önü/açılış referansı, yüzdesel/hibrit eşik ve çeyrek bazlı saf karar kuralları.
- `aiscore_scraper.py`: mobil AIScore canlı maç/barem scraper'ı ve sağlık raporu.
- `aiscore_browser.py`: canlı/final/yenileme için ortak Scrapling ayarları ve ayrı profil seçimi.
- `aiscore_match_page.py`: mobil maç URL'si, kimlik/final doğrulaması ve skor tablosu okuyucusu.
- `aiscore_final_scraper.py`: süre sınırlı final taraması, tarayıcı yaşam döngüsü ve maç bazlı ilerleme.
- `aiscore_scoreboard.py`: aynı maçın çeyrek skorlarını satır/sütun hücrelerinden okuyan ortak DOM kodu.
- `match_state.py`: skor ve canlı periyot/saat ayrıştırma ile şeffaf tempo projeksiyonu.
- `notifier.py`: sade Telegram sinyal mesajı.
- `db.py`: SQLite şeması, aktif/arşiv kayıtları, kullanıcı işlemleri ve outbox.
- `dashboard.py`: Flask sayfaları ve API route'ları.
- `finished_match_service.py`: doğrulanmış final gözlemlerini anında arşivleme ve sonuçlandırma.
- `finished_scan_jobs.py`: düğme ve saatlik worker'ın paylaştığı tek aktif tarama işi/durum bilgisi.
- `scheduled_tasks.py`: saatlik aktif tarama başlatma ve arşiv sonuç kontrolü.
- `static/finished_scan.js`: tarama başlatma, ilerleme sorgulama ve sayfa yenilendiğinde çalışan işe bağlanma.
- `upcoming_scraper.py`, `upcoming_app.py`: yaklaşan maç/saat/barem listesi ve bağımsız analizin çekim/kayıt akışı.
- `upcoming_odds.py`: gelecek maçlarda doğrulanmış mobil toplam piyasası ve aynı bookmaker açılış/maç önü okuyucusu.
- `upcoming_history_scraper.py`: mobil H2H sayfasının iki ayrı takım geçmişini kimlik/final doğrulamasıyla okur.
- `upcoming_signals.py`: son 10 geçmiş maçtan en düşük 3 / en yüksek 2 hücum skoru, tahmini toplam, Edge ve bağımsız ALT/ÜST hesabı.
- `signal_repeat.py`: aynı yöndeki tekrar sinyali için canlı barem mesafesi.
- `signal_lists.py`: takım/lig kara-beyaz liste eşlemesi.
- `templates/`: aktif, arşiv ve yaklaşan maç arayüzleri.
- `static/signal_display.js`: canlı ve arşiv PPM/ekran değerlerini hesap yapmadan gösteren ortak bileşenler.
- `static/ppm_calculator.js`, `static/ppm_calculator.css`, `templates/_ppm_calculator.html`: işlemlerden açılan, kullanıcı PPM seçimiyle kalan süre için maç sonu senaryosu hesaplayan ortak modal.
- `run.py`: dashboard, bankroll ve zamanlanmış işleri birleştirir.

Canlı akış: mobil listing → maçın total odds sayfası → aynı bookmaker açılış/canlı
ve maç önü baremi → dinamik eşik/çeyrek/tekrar/kara liste kontrolleri → SQLite → Telegram.

Canlı dashboard'daki tempo projeksiyonu yalnız gösterim amaçlıdır ve sinyal
üretimine katılmaz. Sonuçlar yalnız otomatik final skor kontrolüyle yazılır.
Arşivleme, gösterim verisini ve silinme zamanını aynı işlemde kaydeder.
