# ALT/ÜST ayrımı ve ÜST kalibrasyon denemesi — 7 Ekim 2026

Kullanıcı ÜST sinyallerine güvenmediğini, ALT/ÜST'ün ayrılıp ÜST tarafının
kalibre edilmesini istedi. Salt okunur SQLite incelemesi ve kronolojik
deneme yapıldı. Tekil maç/ham kaynak/kişisel bilgi bu rapora alınmadı.

## Kayıtlı sinyaller

Her maçın ilk sinyali sonuç veya yönüne bakılmadan seçildi. Otomatik finali
olmayanlar bekleyen, iade olanlar başarı paydası dışındadır. Eski motor ve
kaynak politikalarının karıştığı geniş geçmiş, güncel motor başarısı değildir.

| İlk sinyal yönü | Doğru | Yanlış | Bekleyen | İsabet |
| --- | ---: | ---: | ---: | ---: |
| ALT | 438 | 288 | 3 | %60,3 |
| ÜST | 198 | 201 | 3 | %49,6 |

Bu ÜST'lerin tamamını ters çevirmek 201/399 = %50,4 isabet olurdu; otomatik
ALT yönünün üstünlüğü çıkmaz. Kaynak/politika doğrulanan son **tamamlanmış
eski motor** grubunda ALT 8/18, ÜST 1/7; ÜST merkezinin finalden ortalama
sapması +17,214 sayı. Bu grupta ALT da güçlü bir sonuç göstermiyor.
Güncel v7 sinyal gruplarının final sonuçları incelemede bekliyordu.

## Tekrar çalıştırılabilir deneme

`venv/bin/python directional_audit.py` üretim dosyasını `mode=ro/query_only`
ile açar, yalnız toplam metrikler üretir. Servis veya kayıt değiştirmez.
Yeni CLI bağımlılığı, scraper, worker ve DB migration'ı yoktur.

İlk sinyaldeki skor/saat/maç önü toplamı ve o anda mevcut snapshot prefix'i
ile v7'nin tek merkez formülü yeniden hesaplandı. Bu, eski kayda yeni v7
başarısı yazmak veya eski sinyal anındaki projeksiyonu değiştirmek değildir.
Maç başına ilk sinyal seçilmiş örneklerdir; bütün maçları temsil etmez.

1.078 uygun otomatik final gözlemi vardı. Takvim günü kesiti 29 Eylül 2026
00:00 UTC: 623 eğitim, 451 sonraki test; kesitten önce sonucu gözlenmeyen
eğitim adayları alınmadı. Aynı maç iki grupta yok. Bu veri önceki araştırmalarda
da incelendiğinden sonuç **geriye dönük kronolojik testtir**, yeni bir ileri
dönük başarı doğrulaması diye sunulmaz.

Eğitimde bildirim avantajı koşulunu geçen 222 ÜST adayının merkez-final
ortalama sapması 8,972961 sayıydı. Negatif sapma düzeltme diye eklenmez;
bu tek parametre test sonuçlarından veya test alt grubundan öğrenilmedi.
İki farklı etkisi ayrı ölçüldü:

| Test yöntemi | Değerlendirme / sinyal | Doğru / yanlış | İsabet | Etkisi |
| --- | ---: | ---: | ---: | --- |
| Ham v7 ÜST adayları | 171 | 85 / 86 | %49,71 | Başlangıç |
| ÜST merkezinden eğitim sapmasını çıkarma | 171 | 95 / 76 | %55,56 | 116 ALT / 55 ÜST'e dönüşür |
| Aynı payı ek ÜST bildirim avantajı olarak isteme | 25 | 17 / 8 | %68 | ÜST sinyallerinin %85,4'ü gider |

Merkez düzeltmesinde MAE 13,490'dan 11,998 sayıya indi. ÜST'e ek bildirim
avantajı isteyen filtre, merkez düzeltmesi değildir; aynı barem ve ham ÜST
yönünde kalan alt kümeyi seçer. İyi görünen isabeti bütün 171 adayın isabeti
diye gösterilmez. Bu 25 örnekte isabet için Wilson aralığı %48,4–82,8;
lig/gün ilişkilerini hesaba katmıyor.

Tarihsel kaynak/politika doğrulanan test alt grubunda ham v7 tekrarı ÜST
5/21; merkez düzeltmesi 14/21 (17 ALT / 4 ÜST), filtre yalnız 1/1 bırakır.
Tek sinyalin %100 görünmesi kullanılabilir bir kalibrasyon sonucu değildir.
ALT adaylarında düzeltme denenmedi: geniş testte 166/280, doğrulanan alt
grupta 28/50. Final toplamlarında ayrıştırılamayan uzatma bulunabilir;
bu etiketler temiz normal süre finali veya gelecek hız değişimi etiketi değildir.

Kalibrasyon ve değerlendirme verilerini ayırma ilkesi, olasılık modellerinde
de bağımsız değerlendirme gerektirir:
[scikit-learn kalibrasyon belgesi](https://scikit-learn.org/stable/modules/calibration.html).
Bu uygulamadaki deney puan tahmini sapması içindir; kazanma olasılığı
kalibrasyonu veya kârlılık ölçümü değildir.

## Uygulanan değişiklik ve karar

Canlı ana ekranda ALT/ÜST kartlarına her yönün ilk-sinyal doğru/yanlış,
bekleyen ve başarı yüzdesi eklendi. Eski motor kayıtlarını da içerdiği
açıkça yazılır. GET `/api/signals/performance` yalnız toplam metrikler
döndürür; yenileme 60 saniye, mevcut indeksle ilk satır seçilir.

Tüm tahminler ekranında ayrıca ALT/ÜST **tahmin** başarıları gösterilir.
Küçük avantajlı/bildirimsiz tahminler bu gruba dahildir. İlk tahmin ÜST
kaybettiğinde sonraki ALT kazanması o maçı ALT grubuna taşımaz. Yönsüz,
geçersiz, bekleyen ve iade doğru/yanlış paydasına katılmaz. Barem senaryosu
geçmiş/başarıyı değiştirmez. Arşiv gösterimi yeniden hesaplanmaz ve sonuç
yazma yetkisi otomatik final servisinde kalır.

9 sayılık pay üretime alınmadı: üstte azalma çok büyük ve temiz alt grupta
yalnız bir aday kalıyor. M1'in hesap/avantaj eşiği, sinyal sayısı kuralları ve
Telegram gönderimi değişmedi. Ayrı takip uygulandı; otomatik yön ters
çevirme, canlı sapma parametresi veya başarı garantisi eklenmedi.

## Doğrulama ve çalışan uygulama

Son paket 468 test (35,27 saniye), ilgili Python compileall, dashboard
inline JS/static JS sözdizimi ve diff kontrolü geçti. Testler ilk kayıp
sonrası karşı yönün ilk grubun yerine geçmemesini, yalnız otomatik final
yetkisini, geçersiz finali, boş örnekte yüzde olmamasını, eğitim sonucunun
test başlangıcından önce gözlenmesini, test sonuçlarının öğrenilen payı
değiştirmemesini ve kaybolan sinyal kapsamının gösterilmesini kapsar.

Üretimden ayrı fixture/test client ve geçici tarayıcıyla 1440/390px:
ana/tüm-tahmin yön kartları ve barem senaryosunun başarıya etkisizliği
doğrulandı; JS hatası 0, M2 yine yok. Tarayıcı kapatıldı. Tutarlı 0600
SQLite yedeği/bütünlük kontrolü ardından yalnız mevcut dashboard servisi
yenilendi; bot PID'si değişmedi. Bot ve dashboard active/running,
restart/traceback 0. İki sayfa ve iki takip API'si HTTP 200. İlk üretim
kontrolünde sinyal toplam API'si 35,31ms, tahmin geçmişi 9,57ms;
SQLite quick_check ok. Uygulanan değişiklik başarı takibidir; canlı motor
kalibrasyonunun iyileştiği veya ÜST'ün artık güvenilir olduğu söylenmez.
