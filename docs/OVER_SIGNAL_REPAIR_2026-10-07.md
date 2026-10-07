# ÜST devam hesabı — 7 Ekim 2026

Sonraki kullanıcı talebiyle v9 kalibrasyonu eklendi. Sabit 171 eski ÜST
adayının sonraki dönem yön isabeti 85/171'den 96/171'e, kaynak doğrulanan
21 adayda 5/21'den 15/21'e çıktı. Bu sonuç dönüştürülen ALT yönlerini de
saymaktadır; yalnız ÜST başarısı değildir. Yeterli avantajlı ÜST'ler 15
(9 doğru/6 yanlış); kaynak doğrulanan küçük grupta ÜST bildirimi 0.
451 tahmin korunur, toplam bildirim 444 (v8) →316 (v9). Yeni hesap ve
eğitim/test ayrıntıları: [V9 raporu](OVER_CALIBRATION_V9_2026-10-07.md).
Aşağıdaki bölüm v8 düzeltmesinin önceki ölçümüdür.

Kullanıcı ÜST sinyal mantığını düzeltmeyi istedi; ekran geliştirmesi yapılmadı.
V7 son 2/5 dakikanın hızını açıklayıcı olarak hesaplıyor fakat kalan sayı
merkezine koymuyordu. Worker'ın sürekli tahmini de geçmiş gözlemler almıyordu.
Bu yüzden hızlı başlangıç sonrası yavaşlama yeni merkezde doğrudan yoktu.

V8 formülü: öncül `p = maç önü toplamı / normal süre`, öncül dakika `m=10`,
bütün-maç kalan hızı `w=(skor + m*p)/(oynanan dakika + m)`.
`w>p` ise tek yakın aralık seçilir: son 5 dakika; yoksa son 2 dakika.
Seçilen aralık `r=(aralık sayısı + m*p)/(aralık dakika + m)`.
Kalan hız `min(w,max(p,r))`; aralık yoksa `p`. `w<=p` ise eski `w` aynıdır.
Merkez `skor + kalan dakika * kalan hız`. Yakın aralıklar oy sayılmaz.
Yavaşlayan sıcak başlangıç kalan sürede aynı şekilde sürdürülmez; yüksek
yakın hız bütün-maç tahminini destekliyorsa merkez korunur. Maç önü hızı
bir tahmin öncülüdür; maçın gerçekten o hızda devam edeceği iddiası değildir.

Örnek: 160 maç önü, 20 dakikada 100 sayı, canlı 185. Eski merkez 193,33.
Son 5 dakikada 15 sayı varsa yeni merkez 180 (ALT); 35 sayı varsa 193,33
(ÜST). Skor/saat/barem aynı; fark yakın gözlemde. Her geçerli gözlemde
tahmin vardır; yeni minimum pencere sayısı/yetersizlik kapısı eklenmedi.
Erken/geç ve küçük avantaj tahminleri de kaydolur; Telegram'ın önceki zaman,
avantaj, skor farkı, tekrar ve liste kuralları devam eder.

`OVER_CONTINUATION_ENABLED=true` varsayılandır; false bütün-maç hesabını
geri getirir ve ayrı politika yaratır. `future_pace_v8` / yeni implementation
hash'i ve bayrak kaydedilir. Her yeni tahmin/karar düzeltme miktarı, ham merkez,
seçilen yakın aralık ve kaynak bilgisini dondurur. Yakalama zamanını aşan,
bozuk saatli, geriye kayan veya skor düzeltmesinden önce kalan aralıklar
dışlanır. Güncel gözlem aynı şekilde dahil edildiği için freeze ve sinyal
merkezi tutarlıdır. 48 dakika teyidinde eski 40 dakika aralıkları alınmaz.

## Eşleştirilmiş retrospektif kontrol

Üretim SQLite salt okunur ve tutarlı işlemle okundu. İlk sinyal uygunluk,
yön veya sonuca bakılmadan seçildi. 1.078 otomatik finali olan uygun gözlem;
623 eğitim, 451 sonraki test; ayrım 29 Eylül 2026 00:00 UTC. Eğitim finali
test başlangıcından önce, ortak maç 0. Zaman boşluğundaki dört gözlem iki
gruptan da çıkarıldı. Üretimde kullanılan `get_over_continuation` aynen
çalıştırıldı. Sonraki skorlar özelliğe girmez. Güçlü kaynak alt grubunda
worker'ın doğrulanmış geçmiş filtresi de uygulanır.

| Sabit örneklem | Önce doğru/toplam | Sonra doğru/toplam | Önce MAE | Sonra MAE |
|---|---:|---:|---:|---:|
| Sonraki 451 gözlem | 252/451 | 252/451 | 12,054 | 11,924 |
| Eski ÜST yönlü 171 gözlem | 85/171 | 85/171 | 13,490 | 13,106 |
| Kaynak doğrulanan eski ÜST 21 gözlem | 5/21 | 6/21 | 17,548 | 16,702 |

Eski ÜST 171 adaydan altısı ALT yönüne geçti; düzeltme ortalama 1,484 sayı.
ÜST bildirimleri 171'den 164'e (%95,9 korunma) düştü; kalanlar 82/164 (%50).
Kaynak doğrulanan grupta 21'den 20 ÜST bildirimi kaldı; 5/20 (%25).
Sabit örneklemde değişen yönler de başarı paydasında tutuldu; yalnız kalan
iyi adaylar seçilerek başarı şişirilmedi.

Eğitimde bounded iki katsayı denemesi, yakın hızın desteklemediği sıcak
fazlayı bütünüyle kaldırmayı, yakın hız öncül altındaysa ayrıca baskı
eklememeyi seçti (1,0). Üretimde katsayı fit worker'ı yok. Ayrı bütün-maç
sıcaklığı ve yavaşlamayı agresif çıkaran deneme ÜST bildirimlerini aşırı
azalttığı için uygulanmadı. Son 2 dakika fallback'i ve geçerli saat filtreleri
eklendiğinde gerçek üretim fonksiyonunun yukarıdaki etkisi tekrar ölçüldü.

Bu veri daha önce incelendi; yeni ileri test değildir. Eski motorlar
örneklemi seçmiştir; tüm canlı maçlara genellenmez. Eski kanıtsız gözlemler
araştırma verisidir; güçlü kaynak grubu ayrıca gösterilir. Finallerde uzatma
ayrışmamıştır; normal süre hedefiyle tam uyumlu etiket değildir. Odds/stake
ölçülmediği için kârlılık iddiası yoktur. Sonuç yön başarısının çözüldüğünü
göstermiyor; somut devam hatası onarıldı, başarı artışı kanıtlanmadı.

Tekrarlanabilir salt okunur ölçüm:

```bash
venv/bin/python over_signal_audit.py --db basketball.db
```

Basketbol tempo tanımı hücum sayısına dayanır; bu uygulamanın kullandığı
sayı/dakika hücum sayısı ölçümü değildir. Kaynak:
[NBA glossary](https://www.nba.com/stats/help/glossary).
Yeni migration veya tarihsel yeniden yazma yok; ek bilgiler mevcut nullable
JSON bağlamına girer. Eski SQLite ve dondurulmuş arşiv gösterimi korunur.

## Doğrulama ve servis

Tam paket: 486 test, 36,28 saniye; compileall ve diff kontrolü geçti.
Regresyonlar aynı skor/saatte hızlı/yavaş son bölümün farklı karar üretmesini,
5/2 dakika seçimini, aynı aralığın tekrarıyla sonucun değişmemesini, gelecekte
yakalanmış/bozuk saatli aralıkların dışlanmasını, skor düzeltmesinde freeze ve
yayın merkezinin aynı kalmasını, eski v7 politikasının değişmeden geçerli
kalmasını ve audit'in salt okunur olmasını kapsar.

04:10 Türkiye saatinde tutarlı, 0600 SQLite yedeği sonrası mevcut bot ve
dashboard servisleri yenilendi. İkisi active/running, otomatik restart 0;
bot startup v8/continuation true ve implementation hash eşleşti. Yeni
invocation'larda traceback 0. Ana ekran, tahmin ekranı ve üç mevcut takip
API'si HTTP 200; SQLite quick_check ok. Bu operasyon kontrolü ileri yön
başarısı veya Telegram teslim başarısı ölçümü değildir.
Son canlı kontrolde beş gerçek v8 tahmini kaydoldu; beşinin kod hash'i
eşleşti, devam ayarı açık ve dördünde düzeltme uygulandı. Servisler hâlâ
active/running; restart ve traceback 0. Bu tahminlerin final başarısı henüz
ölçülmedi.
