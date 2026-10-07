# Sayı hızının değişmesi: kayıtlarla doğrulama — 7 Ekim 2026

## Sorulan soru ve bulunan sorun

Kullanıcı hızlı başlayan maçın sonradan yavaşlamasını ve yavaş başlayan
maçın ikinci/üçüncü periyotta hızlanmasını önceden değerlendirmek istiyor.
Bu inceleme sinyal anında bilinen sayı hızını, sonrasında gerçekten
gerçekleşen hızla karşılaştırır. Ölçülen şey **sayı/dakika**; hücum sayısı
veya şut isabetinin ayrı ayrı ölçümü değildir.

V7'nin merkez tahmini bütün maçın sayı hızını, sabit 10 dakikalık maç önü
öncülüyle birleştiriyor. Ayrık son/önceki pencere ve hız değişimi alanları
`reversal_features.py` içinde toplanıyor ama ana yön kararına girmiyor.
İncelemede 1.355 sinyal, 1.126 farklı maç ve yaklaşık 68 bin canlı gözlem
okundu. 348 sinyalde reversal özellikleri var: 123 FULL, 78 RECENT_ONLY,
147 INSUFFICIENT_HISTORY. Eski kayıtlar sonradan özellik/kanıtla doldurulmadı.

**Sonuç:** Bu kayıtlarda uç sayı hızının devamını beklemek zayıf bir hesap.
Maç önü sayı hızına dönüş, değişimin yönünü iyi yakalıyor. Ancak V7 bu
dönüşün büyüklüğünü küçük hesaplıyor. Yönü doğru tahmin etmek ile anlamlı
büyüklükteki değişimi ve ALT/ÜST sonucunu doğru tahmin etmek ayrı sorunlar.

## Ölçüm yöntemi

`reversal_audit.py` SQLite'ı `mode=ro`, `query_only` ve tek okuma transaction'ı
ile açar. Çıktısı yalnız toplamlar, model katsayıları ve metriklerdir; maç,
takım, lig, ID, URL, ham kaynak, hesap veya mesaj bilgisi içermez.

- Her maçın ilk sinyali **uygunluk ve sonuç kontrolünden önce** seçilir.
  İlk kayıt uygun değilse aynı maçın sonraki kaydı yerine alınmaz.
- Özellikler sinyal zamanına kadar kaydedilmiş gözlemlerden kurulur.
  Sonraki skorlar yalnız hedef etiketi olur. O anki skor, oyun saati ve
  son snapshot eşleşir; 120 saniyeden eski başlangıç kabul edilmez.
- Eski şemada saat metni yoksa periyot, oynanan süre ve kalan süre birlikte
  kontrol edilir; bu daha zayıf kayıt biçimi ayrıca sayılır. Dolu saat
  metni varsa periyot/süre eşleşmesi zorunludur.
- Beş dakika hedefinde gerçek bitiş gözlemi 4,5–5,5 dakika aralığındadır;
  bölme gerçek süreyle yapılır. Diğer hız hedefi normal sürenin son bir
  dakikasındaki gerçek gözleme kadar gider. Sonraki saat/skor gerilemesi
  olan aralıklar kullanılmaz. Uzatma gözlemleri hız hedefine girmez.
- Tamamlanmış maç toplamı ayrıca değerlendirilir: yalnız otomatik final
  sonucu, sinyalden sonraki settlement zamanı ve tutarlı toplam kabul
  edilir. Ayrıştırılmamış uzatma bu toplamda bulunabilir; bu grup normal
  sürenin gerçek sayı hızı diye sunulmaz.
- Eski kaynak kanıtı bulunmayan kayıtlar araştırmaya katılır fakat doğrulanmış
  canlı tahmin sayılmaz. Ayrı kaynak kontrolü mevcut forward raporunun
  tarihsel ham kanıt/politika doğrulamasını kullanır.
- Maçlar tarihle sıralanır; yaklaşık ilk %70 eğitim, sonraki takvim günleri
  testtir. Bu veri kesitinde test 1 Ekim 2026 00:00 UTC'de başlar. Test
  başladığında henüz gözlenmemiş eğitim sonuçları dışlanır. Aynı maç eğitim
  ve testte bulunmaz.
- Sabit özellik grupları: maç önü hızından sapma, son pencere sapması,
  önceki/son pencere farkı, eksik veri göstergeleri, süre oranı, skor farkı
  oranı, periyot içindeki konum. İkinci model canlı baremin gerektirdiği
  kalan hızı da kullanır. Ridge cezası 1/10/100 arasından yalnız eğitim
  içindeki daha sonraki tarihlerde seçilir. Test sonucuyla eşik/model ayarı
  değiştirilmez. Yeni bağımlılık yoktur.

## Hız değişimi: sonuçlar

Sinyal anındaki bütün maç sayı hızı, maç önü beklenen hızdan en az %10
yukarıda/aşağıda ise aşağıdaki gruplara girer. Bunlar ilk sinyal anlarıdır;
ilk çeyreğin başlangıcında verilmiş tahminlerin başarısı değildir. Test
örneklerinin çoğu ikinci periyottadır.

| Sonraki beş dakika | Tüm uygun maçlar | Ayrı tarih testinde |
| --- | ---: | ---: |
| Hızı %10 yüksek olanlarda sonradan yavaşlama | 116 / 147 (%78,9) | 44 / 58 (%75,9) |
| Hızı %10 düşük olanlarda sonradan hızlanma | 156 / 216 (%72,2) | 55 / 76 (%72,4) |

563 maçta temiz beş dakika hedefi var: 370 eğitim, 193 test. Kalan normal
süre hedefinde 332 maç var: 228 eğitim, 104 test. Eksik kalan maçlar PAS'a
çevrilmedi; yalnız araştırmada gerçekleşen hızı ölçülemediği için o hedefin
başarı paydasında bulunmuyor.

| Kalan hızı hesaplama yöntemi | Beş dakika hız MAE | Beş dakika yönü | Kalan normal süre hız MAE | Kalan normal süre yönü |
| --- | ---: | ---: | ---: | ---: |
| Mevcut bütün maç hızı aynı kalır | 1,0405 | Yön değişimi tahmin etmez | 0,6587 | Yön değişimi tahmin etmez |
| Maç önü hızına dönüş | 0,8320 | 138 / 193 (%71,5) | 0,4888 | 86 / 104 (%82,7) |
| V7, sabit 10 dakika öncül | 0,9189 | 138 / 193 (%71,5) | 0,5350 | 86 / 104 (%82,7) |
| Son kısa pencere aynı kalır | 1,7213 | 39 / 193 | 1,4420 | 16 / 104 |
| Geçmiş pencere/trend ridge | 0,8430 | 136 / 193 | 0,4920 | 84 / 104 |
| Geçmiş + piyasa ridge | 0,8351 | 136 / 193 | 0,4774 | 85 / 104 |
| Canlı baremin gerektirdiği kalan hız | 0,8106 | 141 / 193 | 0,4681 | 85 / 104 |

MAE sayı/dakika cinsindendir. Maç önü dönüş hesabı ve V7 aynı dönüş yönünü
verir; V7 dönüş miktarını küçültür. Yalnız işaret doğruluğu için %95 Wilson
aralığı beş dakika testinde %64,8–77,4, kalan süre testinde %74,3–88,8'dir.
Bu oranlar tek maç için kalibre edilmiş olasılık değildir.

Küçük bir farkı da "hız değişti" saymak başarıyı olduğundan etkili gösterebilir.
Bu yüzden ikinci ölçüm üç sınıflıdır: en az %10 hızlanma, en az %10 yavaşlama,
ve bu aralıktaki değişimsizlik. Aynı 193 maçta V7 **54 / 193 (%28,0)**,
maç önü dönüşü **108 / 193 (%56,0)** doğru sınıflar. V7 158 maçta değişimi
%10'dan küçük hesaplamış; gerçekte yalnız 33 maç bu aralıkta kalmış.
104 maçlık kalan süre testinde aynı doğrular V7 52, maç önü dönüşü 61'dir.

Bu, "V7 dönüş yönünü hiç hesaplamıyor" demek değildir. **Hız değişiminin
büyüklüğünü fazla bastırdığı** yönünde bu veri kesitinde doğrudan bulgudur.

Son pencerenin önceki pencereye göre değişimini aynen sürdürmek de çözüm
çıkmadı: bu özelliği olan 96 test maçında sonraki beş dakikanın son pencereye
göre değişim yönünü yalnız 13 kez doğru buldu. Geçmiş/trend modelleri,
eğitimde öğrenilen katsayılara rağmen basit dönüş hesabına ek üstünlük sağlamadı.

## Maç toplamı ve ALT/ÜST: aynı başarı oranı değildir

1.078 maçta uygun otomatik final toplamı var. 747 eğitim, 327 test, dört
eğitim sonucu test sınırında henüz gözlenmediği için dışlandı. Testte gerçek
kaydedilmiş barem aynı kalır; fiyat, gerçekleşmiş bahis işlemi veya kâr hesabı
yapılmaz. Bu, geçmiş ilk sinyaller üzerindeki yeniden hesaplamadır; V7'nin
yayına alındıktan sonraki performansı değildir.

| Yöntem | Final toplam MAE | ALT/ÜST doğru / yanlış |
| --- | ---: | ---: |
| V7 | 12,4933 | 180 / 147 |
| Maç önü hızına dönüş | 11,1911 | 174 / 153 |
| Geçmiş pencere/trend ridge | 11,1500 | 170 / 157 |
| Geçmiş + piyasa ridge | 10,8348 | 176 / 151 |
| Sinyal anındaki piyasa baremi | 10,7492 | Merkez baremle eşit; yön önermez |

Dolayısıyla hızın normalleşmesini daha iyi hesaplamak final toplam hatasını
azaltıyor; bu geniş eski kesitte ALT/ÜST isabetini artırmıyor.

**Tarihsel ham kaynak/politika kontrolünü geçen 71 test maçını ayrıca
ayırınca:** V7 MAE 12,6702 ve 33 doğru / 38 yanlış; maç önü dönüşü MAE
10,8801 ve 40 doğru / 31 yanlış. Bu alt grubun sonucu geniş eski gruptan
farklıdır; biri diğerinin yerine başarı iddiası olarak kullanılmaz.

Beş dakika testinde kaynak kontrolünü geçen yalnız 36 maç vardır:
maç önü dönüşü ve V7 yönü 22 / 36 doğru. %10 büyüklük sınıflamasında
maç önü dönüşü 18 / 36, V7 11 / 36. Kalan süre son-dakika hedefindeki
104 test maçının tamamı eski kaynak kanıtsız gruptadır. Bu kapsama farkı
raporda açıkça bulunur; güçlü görünen %82,7 canlı doğrulama diye sunulmaz.

## İnternet araştırmasının bu ölçüme katkısı

[Merritt ve Clauset'in doğrudan araştırması](https://arxiv.org/html/1310.4461v2)
NBA sayı olaylarında kısa seri devamını destekleyen güçlü bağımlılık bulmuyor;
periyot başı ve sonunda zamana bağlı farklılıklar inceliyor. Bu, son kısa
seriyi kalan maça uzatma yaklaşımını ayrıca sınamak için dayanak oldu.

[2025 PLOS ONE çalışması](https://journals.plos.org/plosone/article?id=10.1371/journal.pone.0320284)
1.141 NBA maçının hücum sürelerinde maç içi hızlı/yavaş bölümler buluyor ve
periyot içi zamana göre farklılıkları inceliyor. Ancak LightGBM hedefi
gelecekteki total veya hız dönüşü değil, ilgili bölümün iki takım arasındaki
skor farkıdır. Makaledeki yüksek R² uygulamanın bahis doğruluğu sayılamaz.
Yerel testte periyot içi konum özellik olarak kullanıldı; bu özelliğin
eklenmesi tek başına testte üstünlük sağlamadı.

Bu araştırmalar AIScore'un tüm ligleri için hazır bir ALT/ÜST kuralı vermez.
Yerel ölçüm, kaynakların söylediğinden ayrı ve doğrudan kayıtlarla yapıldı.

## Kod ve çalışma durumu

Yeni `reversal_audit.py` yeniden çalıştırılabilir, salt okunur araştırma
aracıdır. Ana motor, Telegram eşikleri, tüm tahminler ekranı ve arşiv
sonuçları bu ölçümle değiştirilmedi. Araştırılan ridge modelleri servise
yerleştirilmedi. Bu çalışma sinyal azaltma değişikliği değildir.

```bash
venv/bin/python reversal_audit.py
venv/bin/python reversal_audit.py --db /path/to/research-copy.db
```

`tests/test_reversal_audit.py` ilk sinyal seçimi, gelecek verisi sızıntısı,
saat/skor eşleşmesi, gerçek pencere süresi, gerileme, uzatma dışlama,
takvim günü bölümü, henüz gözlenmemiş sonuç eleme, test sonucuyla model
seçmeme ve salt okunur/gizli veri içermeyen çıktı kontrollerini içerir.

İlgili reversal/forward test grubu **45 geçti** (2,51 saniye). Değişen Python
kaynak/test için compileall ve `git diff --check` başarılı. Gerçek DB üzerinde
salt okunur rapor tekrar çalıştırılarak ana toplamların aynı kaldığı kontrol
edildi. Migration veya servis yeniden başlatması yoktur.
