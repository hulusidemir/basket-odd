# Sıcak hızın devamını kalibre etme — V9

Kullanıcı v8'in yön isabetini iyileştirmemesini yeterli bulmadı. Motor
matematiği tekrar incelendi; ekran değişmedi. V8'e yakın aralık eklemek,
o aralık hızlıyken yüksek hızın devamını hâlâ fazla tahmin ediyordu.
Eğitimde sıcak/yakın hızlı 203 gözlemde merkez sapması +7,205 sayı;
yakın yavaş 35 gözlemde -3,561; yakın geçmişsiz 17 gözlemde -3,044.
Bu nedenle bütün gözlemlerden sabit bir sayı çıkarmak yerine yalnız
yüksek yakın hızın desteklediği sıcak fazlanın devamı kalibre edildi.

## Matematik ve canlı sınırlar

V8 kalan hızını `f`, maç önü öncülünü `p` sayalım. V9 kalan hız
`max(0, f - 1.015189277288373 * max(0, f-p))`.
Merkez `mevcut skor + kalan dakika * kalan hız`. Maç önü hızının altına
küçük bir düzeltme mümkündür; mevcut skor çıkarılmaz ve kalan hız negatif
olamaz. Model kalan sayı tahminini değiştirir; ek avantaj/veto eşiği değildir.

Yalnız bütün-maç hızı öncülden yüksek, geçerli yakın pencere hızı >= öncül,
40/48 dakikalık maç, oynanan >=12 dakika, kalan >=5,5 dakika ve öncül gücü
10 dakika rejiminde uygulanır. Yakın yavaş, geçmişsiz, erken/geç, farklı
öncül veya süre durumları v8 hesabıyla tahmin vermeye devam eder. Her geçerli
maç/barem için tek merkez vardır; mevcut canlı barem katsayıya veya rejim
seçimine girmez. ALT/ÜST kalibre edilmiş toplamla karşılaştırılır.

`OVER_CALIBRATION_ENABLED=true`; false v8'e geri döner.
`OVER_CONTINUATION_ENABLED=false` bütün-maç v7 hesabını geri getirir ve
kalibrasyon rejimine girmez. Süre/avantaj/tekrar/kara liste kuralları aynıdır.
Kod, bayraklar, model hash'i, düzeltme ve yöntem yeni v9 bağlamında dondurulur.
Eski v7/v8 tahmin/sonuçlar yeniden yazılmaz. Nullable JSON alanları kullanılır;
yeni tablo/migration/backfill veya worker yok. Canlıda DB'den model eğitme,
internet tahmin servisi veya yeni istatistik kaynağı kullanılmaz.

## Seçim: eğitim içinde, sonuç testi dışında

1.078 uygun ilk sinyal gözlemi; ilk kayıt yön/sonuç/uygunluktan önce seçilir.
623 eğitim, 451 sonraki test; ayrım 29 Eylül 2026 00:00 UTC. Aradaki dört
gözlemin eğitim finali geç olduğundan eğitimden çıkarılır. Ortak maç 0.
Eğitim içinde üç ayrık takvim bloğu ile ilerleyen doğrulama yapıldı;
her fit yalnız doğrulama başlangıcından önce sonuçlanmış maçları içerir.

| Eğitim içi aday | Toplam sayı MAE |
|---|---:|
| Düzeltmesiz v8 | 11,074444 |
| Sabit sayı sapması | 10,769345 |
| Kalan dakika başına sapma | 10,636781 |
| Öncül PPM'ye oranlı sapma | 10,628495 |
| Sıcak fazlanın devamını düzeltme | 10,504250 |

Son aday yalnız eğitim içi en düşük MAE ile seçildi. Tek katsayı
`sum(x*(v8_merkez-final))/sum(x*x)`, `x=kalan*(f-p)`; katsayı negatifse
0. Fit sıcak/yakın hızlı 203 eğitim gözleminde 1,015189277288373 üretti.
Model kaynak dosyası `models/over_calibration_v1.json`; salt okunur araç
aynı katsayı/seçimi yeniden üretir. Sonraki test sonucu katsayı seçiminde
kullanılmaz. Önceki incelemeler nedeniyle test, yeni ileri test değildir.

## Aynı gözlemlerde sonuç

| Sabit örneklem | V8 doğru/toplam | V9 doğru/toplam | V8 MAE | V9 MAE |
|---|---:|---:|---:|---:|
| Sonraki 451 gözlem | 252/451 | 263/451 | 11,924 | 11,466 |
| Eski ÜST 171 aday | 85/171 | 96/171 | 13,106 | 11,884 |
| Kaynak doğrulanan eski ÜST 21 aday | 6/21 | 15/21 | 16,702 | 10,764 |

171 eski ÜST adayında 117 yön ALT'a, 54 yön ÜST'e dönüştü/kaldı; eşitlik 0.
Bu %56,14 (96/171) **ilk ÜST adaylarının sonradan kalibre edilmiş yön
isabetidir**; ÜST bahislerinin %56,14 isabeti diye sunulmaz. Kalan yeterli
avantajlı ÜST bildirimleri 15, sonuç 9/15 (%60). Küçük kaynak doğrulama
alt grubunda beş ÜST yönü kalır fakat minimum avantajı aşan ÜST bildirimi
yoktur; buradan ÜST bildirim başarısı çıkarılamaz.

451 tahminin tamamı korunur. Tüm bildirimler 444 (v8) →316 (v9): v9'da
301 ALT, 15 ÜST; toplam 185 doğru/131 yanlış. Azalan bildirimi gizlemiyoruz:
ek filtre yoktur ama düzelen merkezle yeterli avantaj çıkmayan tahmin
Telegram koşulunu karşılamaz. Orijinal adaylar başarı paydasından atılmaz;
yalnız kalan iyi ÜST'ler seçilerek yüksek başarı gösterilmez.

Eski sinyal motorları örneklemi seçmiştir; bütün canlı maçlara genellenmez.
Kaynak doğrulaması tarihsel yakalama için yapılır, yeni v9 ileri başarısı
değildir. Final toplamları uzatma ayrışmadan kullanılmaktadır; normal süre
hedefi için temiz etiket değildir. Kalibre edilmiş olasılık veya kârlılık
iddiası yoktur. Yön isabetinde ölçülen artış, gelecekte her ÜST'ün doğru
olacağını veya belirli bir canlı başarı oranını garanti etmez.

```bash
venv/bin/python over_calibration_audit.py --db basketball.db
```

Araç salt okunur tutarlı SQLite işlemi kullanır; yalnız toplam metrik çıkarır.
Zaman sırasının korunmasının gerekçesi için birincil kaynak:
[scikit-learn TimeSeriesSplit](https://scikit-learn.org/stable/modules/generated/sklearn.model_selection.TimeSeriesSplit.html).

## Test ve canlı doğrulama

Tam paket 491 test (44,12s); ardından ek v7/v8 dondurulmuş politika
uyumluluğu 2 test geçti. Compileall ve diff kontrolü temiz. Aynı skor/saatte
eski ÜST'ün kalibre ALT'a dönmesi, yeterince yüksek gerçekleşen skorda ÜST'ün
korunması, bütün baremlerde tek merkez, yavaş/geçmişsiz/farklı öncül gücünde
v8'e dönüş ve worker'ın aynı düzeltmeyi tahmin/kararda dondurması test edildi.

04:28 Türkiye saatinde tutarlı 0600 SQLite yedeği sonrası mevcut bot ve
dashboard yenilendi. İkisi active/running, restart0; v9/iki bayrak true ve
implementation hash başlangıç kaydında doğrulandı. Yeni invocation'larda
traceback0, iki ekran ve üç API HTTP200, SQLite quick_check ok.
Son kontrolde dört gerçek v9 tahmini kaydoldu; dört implementation/model
hash'i eşleşti. Bu dört gözlem kalibrasyon rejimine girmedi (uygulama0),
dolayısıyla canlıda kalibrasyon uygulandığı veya yeni final başarısı görüldüğü
iddia edilmez. Test worker akışında uygulama ve frozen merkez eşleşmesi geçti.
