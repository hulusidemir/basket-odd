# Project Map

- `main.py`: sinyal akışının doğrulama/filtre/kayıt/bildirim adımları, tekrar korumaları, taramadan bağımsız Telegram outbox worker'ı ve oturum arızasına göre bekleme.
- `live_signals.py`: Future Pace v9; yakın sayı hızı ve dondurulmuş sıcak-fazla kalibrasyonuyla tek merkez, her barem için yön ve ayrı Telegram avantaj koşulları.
- `pace_calculator.py`: kronolojik tempo pencereleri, aynı kaynak aralığını tek sayma, öncüle yaklaştırma ve sıcak başlangıç devam hesabı.
- `signal_quality.py`: sinyal anında dondurulan Quality v1 bileşenleri ve etiketi.
- `forward_validation.py`: gerçek kayıtlı tahminin kod/ayar/aralık bağlamını dondurur; SQLite'ı salt okunur değerlendirir, ilk sinyal/teslim alt kümesi, model/piyasa/baz hatası ve M2 durumlarını ayrı politikalarla raporlar.
- `aiscore_scraper.py`: mobil AIScore canlı maç/barem scraper'ı, süre bütçeli adil tarama kuyruğu ve sağlık/kapsama raporu.
- `live_market.py`: herhangi bir şirketin ana total kimliği, aynı şirketin canlı state/DOM kanıtı; boş canlı hücre için kimlikli ham history fallback'i ve gönderim öncesi doğrulama.
- `aiscore_browser.py`: canlı/final/yenileme için ortak Scrapling ayarları ve ayrı profil seçimi.
- `aiscore_match_page.py`: mobil maç URL'si, kimlik/final doğrulaması ve skor tablosu okuyucusu.
- `aiscore_final_scraper.py`: süre sınırlı final taraması, tarayıcı yaşam döngüsü ve maç bazlı ilerleme.
- `aiscore_scoreboard.py`: aynı maçın çeyrek skorlarını satır/sütun hücrelerinden okuyan ortak DOM kodu.
- `aiscore_basketball_data.py`: aynı maçın şut/FT, ribaund, top kaybı, faul ve olay verisini cache'siz public API'den okur; skor/aritmetik doğrulaması. M2 okuyucusudur; M1 kararına girmez.
- `motor2.py`, `motor2_worker.py`: canlı akıştan kaldırılmış eski deneysel şut/hücum hacmi motoru; bot import etmez veya worker başlatmaz.
- M2 arayüz dosyaları kaldırıldı; canlı ve geçmiş DTO'lar eski M2 alanlarını da kullanıcı API'sine taşımaz.
- `docs/MOTOR2.md`: M2 model varsayımları, nullable migration, arşiv ve çalışma sınırları.
- `docs/M2_REVIEW_2026-10-07.md`: üretim M2 ret özeti, gerçek kaynak kontrolleri ve canlı akıştan kaldırma kararı.
- `match_state.py`: skor ve canlı periyot/saat ayrıştırma ile şeffaf tempo projeksiyonu.
- `reversal_features.py`: yalnız sinyal anındaki piyasa/tempo verilerinden ayrık pencere feature'ları; yön kararına katılmaz.
- `reversal_audit.py`: ilk sinyal zamanında bilinen verilerle sonraki beş dakika/kalan normal süre hız değişimi ve final toplamı için salt okunur kronolojik test; yalnız toplam metrikler, ayrı tarihsel kaynak doğrulama alt grubu.
- `directional_audit.py`: ALT/ÜST kayıt sonuçları, kaynak doğrulanan politika grupları ve eğitimde öğrenilen ÜST sapmasının sonraki testte merkez/filtre etkisi; salt okunur, canlıya kalibrasyon uygulamaz.
- `over_signal_audit.py`: gerçek v8 devam fonksiyonuyla ilk sinyal anında v7/v8 eşleştirilmiş salt okunur tekrar hesabı; sabit eski ÜST örneklemi ve kalan bildirim kapsamı ayrı.
- `over_calibration.py`, `models/over_calibration_v1.json`: canlı DB/eğitim olmadan dondurulmuş sıcak-fazla düzeltmesi; yalnız tanımlı süre/öncül rejiminde uygulanır.
- `over_calibration_audit.py`: üç ayrık eğitim içi zaman bloğunda katsayı/yöntem seçimini tekrar üretir; gerçek v9 fonksiyonuyla yön/kapsam karşılaştırmasını salt okunur yapar.
- `docs/OVER_SIGNAL_REPAIR_2026-10-07.md`: v8 motor düzeltmesi, formülü ve sınırlı retrospektif etkisi.
- `docs/DIRECTIONAL_CALIBRATION_2026-10-07.md`: ayrı yön başarısı ve ÜST kalibrasyon denemesinin kapsam/isabet/sinyal azalması ölçümleri.
- `notifier.py`: sade Telegram sinyal mesajı.
- `db.py`: SQLite şeması, aktif/arşiv kayıtları, kullanıcı işlemleri ve outbox; dondurulmuş gözlem tahminleri ve otomatik final gözlemleri.
- `forecast_tracking.py`: kayıtlı tahmin ve sinyallerin otomatik finalden salt okunur başarı hesabı; maç başına ilk kayıt, ALT/ÜST ve bekleyen/iade/yönsüz ayrımı.
- `templates/forecasts.html`, `static/forecasts.js`: tüm güncel tahminler, geçici barem karşılaştırması; hafif başarı kartları ve bütün tahminlerin sayfalı geçmişi.
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
- `static/ppm_calculator.js`, `static/ppm_calculator.css`, `templates/_ppm_calculator.html`: yalnız canlı sinyallerin işlemlerinden açılan, kullanıcı PPM seçimiyle kalan süre için maç sonu senaryosu hesaplayan modal.
- `run.py`: dashboard, bankroll ve zamanlanmış işleri birleştirir.

Canlı akış: mobil listing → maçın total odds sayfası → uygun şirketin aynı satır
canlı state/DOM doğrulaması (boş hücrede aynı şirket history fallback'i)
→ frozen v9 gözlem tahmini → ayrı avantaj/tekrar/kara liste kontrolleri (Quality v1 yalnız açıklayıcı)
→ kaynak kanıtıyla SQLite → gönderim öncesi yeniden doğrulama → Telegram.

Canlı dashboard'daki tempo projeksiyonu yalnız gösterim amaçlıdır ve sinyal
üretimine katılmaz. Sonuçlar yalnız otomatik final skor kontrolüyle yazılır.
Arşivleme, gösterim verisini ve silinme zamanını aynı işlemde kaydeder.
