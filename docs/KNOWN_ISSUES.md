# Known Issues

Bu dosya aktif hata, operasyonel risk ve kanıt eksiklerini listeler. Yeni oturumda önce `AGENTS.md`, `docs/PROJECT_MAP.md`, `docs/CURRENT_STATE.md`, `docs/DECISIONS.md` ve ardından bu dosya okunmalıdır.

## Açık Riskler ve Sınırlamalar

### 1. Eski V2 gate kanıtı yeni skora aktarılamaz

- İlgili dosyalar: `signal_gate.py`, `signal_analysis.py`, `signal_quality.py`, `projection.py`, `db.py`
- Durum: `trusted_70_v2` artık aktif akışta değildir. Eski fingerprint ile oluşan `signal_trials` yalnız tarihsel bütünlük için korunur.
- Etki: Eski gate sonuçları `market_edge_league_v1` veya yeni fair/projeksiyon sürümünün kanıtı sayılamaz.
- Sonraki adım: Yeni skor için ayrı epoch ve ileri tarihli ledger kullan; eski trial'ları yeni yıldız eşiklerini açmak için birleştirme.

### 2. Yeni adil barem ve sinyal skorunun piyasa üstünlüğü henüz kanıtlanmış değil

- İlgili dosyalar: `projection.py`, `signal_analysis.py`, `signal_quality.py`
- Durum: `current_pace_projection_v2` şeffaf ham tempo uzatımıdır; `independent_pace_fair_v2` canlı baremden bağımsız konservatif kalan-tempo tahminidir. Zaman sıralı mevcut kontrol 65-74 bandında umut verse de örnek küçüktür.
- Etki: Sinyal skoru araştırma sıralamasıdır fakat tek başına oynama kararı veya `%70` garantisi üretemez. Dört ve beş yıldız bu nedenle kilitlidir.
- Sonraki adım: Katsayıları ve `market_edge_league_v1` formülünü sabit tutarak ayrı ileri tarihli ledger aç; en az 100 benzersiz sonuçta skor bantlarını tekrar değerlendir.
- Ek sınır: Katsayıları yeniden üreten sürümlü eğitim artifact'i ve final toplam için prediction interval henüz yoktur; mevcut skor olasılık değildir.

### 3. AIScore DOM/Nuxt değişikliği veri akışını bozabilir

- İlgili dosyalar: `aiscore_scraper.py`, `upcoming_scraper.py`, `market_evidence.py`
- Durum: Retry, timeout, selector fallback, maç bazlı izolasyon ve sağlık raporu eklendi; dış sitenin DOM/Nuxt sözleşmesi yine proje kontrolü dışındadır. Piyasa kanıtı ayrıca masaüstü Nuxt boxscore takım toplamlarına bağlıdır.
- Etki: Discovered/parsed sayısı düşebilir, market alanları kaybolabilir, upcoming akışı kısmi duruma geçebilir veya kanıt etiketi güvenli biçimde `İSTATİSTİK YETERSİZ` kalabilir.
- Sonraki adım: `last_report` kapsam ve hata trendleriyle birlikte kanıt etiketlerinin veri yeterlilik oranını izle. Upstream değişiklikte debug artifact'i hassas veri içermeden inceleyip parser fixture'larını güncelle.

### 4. Tek bookmaker verisi kaynak hatasına daha duyarlıdır

- İlgili dosyalar: `aiscore_scraper.py`, `signal_quality.py`
- Durum: Aynı bookmaker bloğunda opening ve in-play total okunması yeterlidir; ikinci bookmaker veya spread karşılaştırması aranmaz.
- Etki: Daha fazla sinyal veri kapısından geçebilir, fakat seçilen tek kaynaktaki yanlış/stale satırın etkisi artar.
- Sonraki adım: Tek bookmaker rejiminin sonuçlarını sürüm 3 fingerprint'i altında ayrı izle; eski iki-bookmaker kanıtıyla karıştırma.

### 5. Aktif sinyal skoru yalnız 4x10 formatında doğrulandı

- İlgili dosyalar: `projection.py`, `signal_analysis.py`, `signal_quality.py`, `signal_gate.py`
- Durum: NBA 4x12 ve NCAA formatları süre/projeksiyon görünürlüğü için ayrıştırılsa da `fair_edge_4_projection_5_q2q3_v3` araştırma rejimi 4x10 ile sınırlıdır. NBA Summer League ayrıca doğru biçimde 4x10 tanınır.
- Etki: Diğer formatların güvenilir sinyal olarak açılması için yeterli ayrı kanıt yoktur.
- Sonraki adım: Her format için bağımsız strateji/fingerprint, epoch ve ileri tarihli örneklem kullan.

### 6. Yaklaşan maç sağlık geçmişi restart sonrası kalıcı değil

- İlgili dosyalar: `upcoming_scraper.py`, `upcoming_app.py`
- Durum: Son `last_report` process belleğinde tutulur; maç satırlarının `fetched_at` zamanı DB'de kalıcıdır.
- Etki: UI güncel fetch'in stale/kısmi durumunu gösterebilir ancak çoklu restart boyunca sağlık trendi çıkaramaz.
- Sonraki adım: Operasyonel ihtiyaç oluşursa ayrı, boyutu sınırlı fetch-run tablosu veya metrics sink ekle.

### 7. Gerçek upstream uçtan uca test kapsamı sınırlı

- İlgili klasör: `tests/`
- Durum: Gate, trial ledger, Telegram outbox, final/snapshot, bookmaker parsing ve upcoming kısmi reconcile için odaklı testler vardır. Testler AIScore'un gelecekteki gerçek DOM değişikliklerini önceden garanti edemez.
- Etki: Unit testler geçerken dış site değişimi üretimde parse kapsamını azaltabilir.
- Sonraki adım: Anonimleştirilmiş HTML/JSON fixture'larını ve sağlık alarm eşiklerini gerçek hata örneklerinden güncelle.

### 8. Büyük inline dashboard template'leri bakım maliyeti taşıyor

- İlgili dosyalar: `templates/dashboard.html`, `templates/deleted_matches.html`, `templates/upcoming_matches.html`
- Durum: CSS ve JavaScript'in önemli bölümü template içinde yaşıyor.
- Etki: Yerel bir UI değişikliği mobil/desktop görünüm veya başka bir route'u istemeden etkileyebilir.
- Sonraki adım: Büyük frontend refactor yalnız ayrı kapsam ve görsel regresyon kontrolüyle yapılmalı.

### 9. Eski DB'lerde kullanılmayan legacy kolonlar kalabilir

- İlgili dosya: `db.py`
- Durum: Migration artık bu kolonları sessiz/yıkıcı biçimde düşürmez; geriye uyumluluk için kullanılmayan alanlar fiziksel olarak kalabilir.
- Etki: Fonksiyonel hata oluşturmaz fakat şema borcu ve disk alanı bırakır.
- Sonraki adım: Temizlik gerekiyorsa kullanıcı onayı, yedek ve açık tek seferlik migration ile yap.

### 10. Dashboard erişim doğrulaması yok

- İlgili dosyalar: `run.py`, `dashboard.py`
- Durum: Flask varsayılan olarak `0.0.0.0` üzerinde dinler; destructive API route'larında uygulama içi kullanıcı doğrulaması/CSRF token'ı yoktur.
- Etki: Port güvenilmeyen LAN/İnternet'e açılırsa yetkisiz veri değişikliği veya silme riski doğar.
- Sonraki adım: Servisi yalnız güvenilir host/reverse proxy arkasında tut; dış erişim gerekiyorsa kimlik doğrulama, CSRF ve TLS'i birlikte ekle.

### 11. Büyük silinen-sinyal geçmişi dashboard profil maliyetini artırabilir

- İlgili dosyalar: `dashboard.py`, `db.py`
- Durum: Canlı API'nin betimsel geçmiş profilleri silinen kayıtları okuyabilir; trial evidence hot path'i son 200 kayıtla sınırlandırılmış olsa da UI profilleri henüz kalıcı özet/cache kullanmaz.
- Etki: Çok büyük arşivde `/api/alerts` ve rapor uçları yavaşlayabilir.
- Sonraki adım: Snapshot semantiğini değiştirmeden result/version anahtarlı profil cache'i veya özet tablo ekle.

## Çözülen Başlıklar

- Telegram başarı logu ve teslimat: Kalıcı outbox, alıcı bazlı mesaj ID'si ve sınırlı retry ile izlenir; eşiği geçen kayıtların tamamı sinyal skoru ve oynanabilirlik yorumuyla gönderim gerektirir.
- Final olmayan skordan sonuç üretme: Settlement yalnız açık final etiketi ve makul otomatik final skorla yapılır.
- Silinen sinyallerde yeniden hesaplama: Silme anındaki `display_snapshot` kullanılır; model/projeksiyon/fair tekrar hesaplanmaz.
- Eski yıldızlı filtre ve kurallar: Canlı/silinen arayüzden, veri hazırlama akışından ve eski snapshot API çıktısından kaldırılmıştır.
- Final ile silme anı skorunun karışması: `status/score` ve `final_status/final_score` ayrılmıştır.
- Geçersiz yönün `ALT` sayılması: Geçersiz yön boş/güvenli duruma düşer.
- Riskli DB kolon düşürme ve sessiz migration hataları: Migration veri koruyucu hale getirilmiş, duplicate-column dışı operasyonel hatalar görünür bırakılmıştır.
- README dashboard portu: Varsayılan `5151` ve `DASHBOARD_PORT` override'ı belgelenmiştir.
- Yaklaşan maç kısmi taramasında eski satırların silinmesi: Reconcile yalnız eksiksiz ve sağlıklı nesilde çalışır.

## Önerilen İzleme Sırası

1. Sabit v2 fingerprint için trial sayısı, çözüm kapsamı, Wilson alt sınırı ve iki 50 maçlık blok istikrarını izle.
2. Canlı ve yaklaşan scraper `last_report` kapsam/partial/error değerlerini takip et.
3. Eşleşmiş bookmaker bulunamama oranını lig bazında ölç; doğrulama olmadan güven eşiklerini düşürme.
4. 4x12 ve NCAA için ayrı model/epoch açmadan önce yeterli ileri tarihli veri biriktir.
5. Upstream değişikliklerden anonim fixture ve regresyon testi üret.
