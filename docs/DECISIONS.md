# Decisions

## Normal Sinyal Yönü Kanoniktir

Uygulama saklanan/analiz edilen `ALT` veya `ÜST` yönünü oynanabilir yön kabul etmelidir. Legacy profil bazlı yön override'ları güvenilmez davranış ve karışık sonuçlandırma/raporlama ürettiği için kaldırıldı. Sonuçlandırma sırası `analysis.final_direction`, `analysis.direction`, sonra `alert.direction` olmalıdır.

## Tarihsel Snapshot'lar Stabil Kalmalı

Silinen alert'ler aktifken yakalanmış `display_snapshot` taşıyabilir. Raporlar anlamlı tarihsel görüntü bağlamını korumalı; final skor, durum, sonuç ve not gibi settlement metadata'sının güncellenmesine izin vermelidir.

## Lokal Depo SQLite'tır

Kalıcılık katmanı `db.py` üzerinden SQLite'tır. Migration'lar geriye dönük uyumlu olmalı ve eski lokal DB dosyalarını tolere etmelidir. Kullanıcı açıkça istemedikçe yıkıcı veri temizliği yapma.

## Flask Template'leri Şimdilik Inline Kalır

Dashboard frontend'i şu an büyük ölçüde `templates/` içindeki büyük dosyalarda yaşar. Küçük değişikliklerde mevcut inline pattern'leri kullanmaya devam et. Kullanıcı büyük frontend yeniden yapılandırması istemedikçe build system ekleme.

## Scraping İzole Edilir

AIScore'a özel parsing `aiscore_scraper.py` ve `upcoming_scraper.py` içinde kalmalıdır. Aşağı modüller normalize match dictionary tüketmeli, browser/DOM varsayımlarını kopyalamamalıdır.

## Konfigürasyon Environment'tan Gelir

Runtime ayarları `Config` ve `.env` üzerinden okunur. Token, chat ID, lokal path veya kullanıcıya özel credential hard-code etme. Sadece değişken adlarını dokümante et, değerleri yazma.

## Testler Risk Alanlarını Hedeflemeli

Yön seçimi, sonuçlandırma, sinyal kalitesi, projeksiyon/clock parsing, tekrar koruması ve DB kalıcılığı etrafındaki testlere öncelik ver. Sadece UI template değişiklikleri manuel kontrol ve Python import/sözdizimi kontrolleriyle doğrulanabilir.
