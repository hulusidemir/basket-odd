# Current State

## Mevcut Durum

Uygulama aktif bir Python/Flask AIScore basketbol odds takip sistemidir. Canlı scraping, sinyal üretimi, Telegram uyarıları, SQLite kalıcılığı, dashboard inceleme, silinen maç takibi, yaklaşan maç akışları, bankroll ekranları ve ayrı balance tracker desteklenir.

Yakın dönemde legacy `C_A` ve `100 Profil` profil mantığı runtime, UI, scoring ve testlerden kaldırıldı. Mevcut mimaride normal sinyal yönü (`ALT` / `ÜST`) oynanabilir yön kabul edilmelidir.

## Çalışan Özellikler

- Canlı AIScore basketbol total scraping.
- Maç ve period bazlı korumalarla açılış/canlı barem anomalisi tespiti.
- Projeksiyon, H2H bağlamı, sinyal kalitesi ve tekrar koruması.
- `main.py` üzerinden Telegram alert gönderimi.
- Aktif alert, aksiyon, not, sil/takip/bahis/pas durumları için Flask dashboard.
- Silinen maç ekranları, sonuç kontrolü, raporlar, insights ve CSV export.
- `run.py` aktifken zamanlanmış biten maç kontrolleri.
- Yaklaşan maç fetch/list/save/follow akışları.
- Bankroll ve balance tracker blueprint'leri.
- Quarter PPM, sinyal kalitesi, tekrar koruması ve display snapshot unit testleri.

## Yarım Kalan veya Riskli Alanlar

- Scraper'lar AIScore DOM'una bağlıdır; site değişirse kırılabilir.
- Dashboard template'leri büyük inline HTML/CSS/JS dosyalarıdır; küçük UI değişiklikleri geniş görsel etki yaratabilir.
- Mevcut lokal DB'lerde eski implementasyonlardan kalma kolonlar veya stale snapshot verisi olabilir.
- `pytest` requirements içinde var, fakat lokal ortamda bağımlılıklar kurulmamış olabilir.
- Commit geçmişinde katı bir mesaj standardı görünmüyor.

## Bilinen Hatalar

Şu an doğrulanmış açık bug bu dosyada kayıtlı değil. En olası hata sınıfları scraper kırılmaları, sonuçlandırma yön uyumsuzlukları ve dashboard render regresyonlarıdır.

## Sonraki Oturumlarda Nereden Devam Edilmeli

`AGENTS.md`, ardından bu dosya ve `PROJECT_MAP.md` ile başla. Yeni buglarda önce özellik alanını belirle ve sadece haritada işaretlenen dosyaları incele. Davranış değişikliklerinde mümkünse `tests/` altında odaklı test ekle/güncelle ve kalıcı kararları `docs/DECISIONS.md` içine işle.
