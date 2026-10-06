# Project Map

- `main.py`: sinyal akışının doğrulama/filtre/kayıt/bildirim adımları, tekrar korumaları, taramadan bağımsız Telegram outbox worker'ı ve oturum arızasına göre bekleme.
- `live_signals.py`: Future Pace v5; maç önü/açılış PPM öncülü, tempo bandı ve gereken kalan PPM/sayı avantajından ALT/ÜST/PAS kararı.
- `pace_calculator.py`: kronolojik tempo pencereleri ve maç önü PPM öncülüne yaklaştırma.
- `signal_quality.py`: sinyal anında dondurulan Quality v1 bileşenleri ve etiketi.
- `forward_validation.py`: gerçek yayımlanan tahminin kod/ayar bağlamını dondurur; SQLite'ı salt okunur değerlendirir, maç başına ilk sinyali ve ayrı politikaları raporlar.
- `aiscore_scraper.py`: mobil AIScore canlı maç/barem scraper'ı, süre bütçeli adil tarama kuyruğu ve sağlık/kapsama raporu.
- `live_market.py`: bet365 ana total kimliği, ham history protobuf çözümü, sağlayıcı tazeliği ve gönderim öncesi kaynak kanıtı doğrulaması.
- `aiscore_browser.py`: canlı/final/yenileme için ortak Scrapling ayarları ve ayrı profil seçimi.
- `aiscore_match_page.py`: mobil maç URL'si, kimlik/final doğrulaması ve skor tablosu okuyucusu.
- `aiscore_final_scraper.py`: süre sınırlı final taraması, tarayıcı yaşam döngüsü ve maç bazlı ilerleme.
- `aiscore_scoreboard.py`: aynı maçın çeyrek skorlarını satır/sütun hücrelerinden okuyan ortak DOM kodu.
- `aiscore_basketball_data.py`: aynı maçın şut/FT, ribaund, top kaybı, faul ve olay verisini cache'siz public API'den okur; skor/aritmetik doğrulaması. M2 okuyucusudur; M1 kararına girmez.
- `motor2.py`, `motor2_worker.py`: yeni sinyal satırlarında bağımsız şut/hücum hacmi değerlendirmesi; ayrı profil/görev, veri kapıları ve dondurulan ALT/ÜST/PAS.
- `static/motor2.js`, `static/motor2.css`, `templates/_motor2_modal.html`: canlı/arşiv M2 sütunu ve ayrı veri kalitesi/gerekçe modali.
- `docs/MOTOR2.md`: M2 model varsayımları, nullable migration, arşiv ve çalışma sınırları.
- `match_state.py`: skor ve canlı periyot/saat ayrıştırma ile şeffaf tempo projeksiyonu.
- `reversal_features.py`: yalnız sinyal anındaki piyasa/tempo verilerinden ayrık pencere feature'ları; yön kararına katılmaz.
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

Canlı akış: mobil listing → maçın total odds sayfası → bet365 ana total ve history
doğrulaması → Future Pace v5/tekrar/kara liste kontrolleri (Quality v1 yalnız açıklayıcı)
→ kaynak kanıtıyla SQLite → gönderim öncesi yeniden doğrulama → Telegram.

Canlı dashboard'daki tempo projeksiyonu yalnız gösterim amaçlıdır ve sinyal
üretimine katılmaz. Sonuçlar yalnız otomatik final skor kontrolüyle yazılır.
Arşivleme, gösterim verisini ve silinme zamanını aynı işlemde kaydeder.
