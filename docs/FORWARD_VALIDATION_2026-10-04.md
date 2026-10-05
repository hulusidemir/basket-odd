# Canlı tahmin doğruluğu ve ileriye dönük değerlendirme — 4 Ekim 2026

## Aynı gün sonradan yapılan yayın düzeltmesi

Aşağıdaki ilk denetimde korunan 70 barajı, kullanıcının sinyal akışı ve doğru
barem talebi üzerine kaldırıldı. Loglarda geçen 71 aday gözleminin tamamını
engellediği görüldü; bu puanın sıralama başarısı yeterince doğrulanmıyor.
Tempo/avantaj kuralları ve kaynak doğrulaması korunur. Quality v1 yalnız
açıklayıcıdır; yeni frozen politikalar publication=verified_future_pace_v5
taşır ve MIN_SIGNAL_QUALITY parametresini içermez. Eski frozen politikalar
kendi eşik koşuluyla raporlanmaya devam eder. Bu iki politika birleştirilmez;
geçmiş kayıtlar ve ilk denetimin bulguları değiştirilmez. Yeni şema yok.

Gerçek motor entegrasyonunda yetersiz geçmiş PAS kalır; 187.5 kaynak
kanıtıyla 197.5 ikamesi DB/bildirim öncesi reddedilir; yeterli geçmiş ve
konservatif avantajla aynı 187.5 kayda ve bildirim adımına aynen geçer.
Yeni politikada düşük puanlı gerçek sinyal de değerlendirmeye girer; PAS
kararı yayımlanmaz ve kaynak geçersiz/eski ise Telegram retry iptal edilir.

## Veri denetimi ve hipotezler

Üretim SQLite dosyası salt okunur açıldı; `quick_check` sonucu `ok`.
İnceleme anında 1.274 sinyal / 1.054 maç vardı: 720 başarılı, 553 başarısız,
1 bekleyen. Eski sinyallerin hiçbirinde yeni bet365 ham kaynak kanıtı yok.
Bu geçmiş, doğru bookmaker verisiyle çalışan yeni akışın başarısı sayılamaz.
1.574 kanıtlı snapshot'ın 1.574'ü kayıtlı kimlik/skor/saat/barem kontrolünden
geçti. Snapshot sayısı tahmin veya bağımsız sonuç sayısı değildir.

Hipotez: **Quality v1 yüksek puan, sonraki dönemde daha güvenilir sıralama
sağlıyor mu?** Quality v1 grubunda her maçın ilk sinyali, sonucu bilinmeden
seçildi; tekrarlar çıkarıldı. Tarih sınırı önceki 2 Ekim denetimindeki
27 Eylül 00:00 UTC olarak korundu. Eşik araması/parametre optimizasyonu
yapılmadı. Aynı maç ilk ve sonraki döneme alınmadı.

| Grup | İlk dönem: 26 Eylül | Sonraki dönem: 27 Eylül–3 Ekim |
| --- | ---: | ---: |
| Tüm ilk sinyaller | 90/140 (%64,3) | 308/544 (%56,6) |
| ALT | 59/88 (%67,0) | 209/347 (%60,2) |
| ÜST | 31/52 (%59,6) | 99/197 (%50,3) |
| Quality < 70 | 74/121 (%61,2) | 287/509 (%56,4) |
| Quality >= 70 | 16/19 (%84,2) | 21/35 (%60,0) |
| Quality ROC-AUC, eşit puanlar 0,5 sayılır | 0,632 | 0,503 |

İlk dönemde ayrıca bir bekleyen maç vardı; kayıp sayılmadı. Sonraki dönem
genel başarı için Wilson %95 aralığı %52,4–60,7, Quality >=70 için
%43,6–74,4. Lig/gün bağımlılığı bu aralıklara dahil değildir. Küçük ilk
dönemin %84,2 sonucu yüksek kalite güvencesi olarak genellenemez.
Yüksek puan hipotezi bu veride yeterince desteklenmiyor; yeni bir kalite
formülü veya daha iyi görünen eşik üretime taşınmadı. Mevcut 70 yayın eşiği
kalibre edilmiş kazanma olasılığı değildir; ileriye dönük sınanacak mevcut
politikanın bir parçası olarak korunur.

Bu, **geriye dönük bir teşhistir**: söz konusu tarih aralığı önceki
çalışmalarda zaten incelenmiştir ve bağımsız/hiç görülmemiş test değildir.
Bookmaker kaynak kanıtı ve bahis getirisi olmadan kârlılık çıkarılamaz.

## Kanıtlanan kod kusurları

- 20 dakikalık devrelerde `Q2 18:30` gibi doğrulanmış bir saat, yalnız 12
  dakikaya kadar kabul eden yardımcıyla kaydediliyor ve boş kalıyordu.
  Bu gözlem daha sonra kaynak kanıtıyla eşleşmediği için tempo geçmişinden
  çıkıyordu. Saat artık doğrulanan maç formatının kalan süresinden kaydedilir.
  Mevcut üretim snapshot'larında boş-saat kaynak uyuşmazlığı 0; gerçek
  üretim etkisinin sayısı bu incelemede gösterilemedi. Kusur entegrasyon
  testinde düzeltmeden önce başarısız oldu.
- `int(elapsed_minutes * 60)` bazı geçerli saatleri bir saniye eksiltiyordu:
  Q2 07:55, 725 yerine 724. 10/12/20 dakikalık formatların bütün geçerli
  saatlerini tarayan aritmetik kontrolde sırasıyla 168/158/248 etkilenen
  periyot+saat kombinasyonu bulundu. Kayıt ve motor aynı şekilde `round`
  kullanır. Önce başarısız, sonra geçen regression testi vardır.
- Oyun saniyesi uygun olsa bile kayıt zamanı sinyal anından sonra olan bir
  snapshot tempo çıpası olamaz. Motor kaynak yakalama zamanını kronolojik
  filtreye geçirir; zaman damgası olmayan/geçersiz veya daha sonra kaydedilen
  satırlar, bu zaman sınırı verildiğinde dışlanır.
- Bütün çıpalar elendiğinde boş geçmişten tek bir tempo türetiliyordu.
  Artık geçerli geçmiş yoksa pencere listesi boş kalır.
- Future Pace ayarlarındaki NaN/sonsuz/negatif matematik değerleri ve ters
  tolerans aralıkları başlangıçta reddedilir; geçerli ayar değerleri değişmez.

## Önceden sabitlenen ileriye dönük yöntem

Ek bir tahmin motoru, shadow akış veya PAS adayı kayıt sistemi yoktur.
Yalnız gerçekten kaydedilen/yayımlanan ALT/ÜST sinyali mevcut `alerts`
tablosunda ek bir nullable `prediction_context_json` alanı alır. Kod dosyalarının
hash'i process açılırken sabitlenir; karar ayarları, kararın alanları, gerçek
40/48/2x20 formatı, kaynak yakalama zamanı ve mevcut snapshot ID'leri kayıt
anında dondurulur. Kod/karar ayarı değişince farklı `policy_id` oluşur.
Kimlik bilgileri veya Telegram alıcıları kaydedilmez. Arşiv gösterimi ve API
bu teknik alanı taşımaz.

Değerlendirme yalnız frozen politikası ve ham kaynak doğrulaması bulunan
yeni gerçek tahminleri kullanır. Aynı politika/maç için kronolojik ilk
sinyal **sonuca bakılmadan** seçilir; ilk sinyal bekleyen veya kaynağı geçersiz
olsa dahi daha sonraki kazananla değiştirilmez. Her politika ayrı raporlanır.
ALT/ÜST ve UTC günler ayrı gösterilir. Kazanma oranının paydası başarılı+
başarısızdır; iade, bekleyen ve tutarsız sonuçlar ayrı kalır. Sonuç yalnız
otomatik final kontrolünden gelmelidir; kayıtlı final/barem aritmetiği de
doğrulanır. `--as-of` sonradan öğrenilen sonuçların erken rapora sızmasını
engeller. Raporda güncel odds yerine sinyal anındaki ham yanıt ve kaynak
zamanı doğrulanır.

```bash
venv/bin/python forward_validation.py --db basketball.db
venv/bin/python forward_validation.py --db basketball.db --as-of 2026-10-12T00:00:00Z
```

Rapor SQLite URI `mode=ro` ile çalışır; migration, UPDATE/DELETE, eğitim,
eşik araması veya Telegram gönderimi yapmaz. Yeni ileriye dönük sonuçlar
bu oturumda henüz yok; **tahmin başarısı kanıtlanmadı**. Yeni sonuçlar
birikmeden eşiği oynatmak ya da geçmiş yüksek başarıyı yeni sisteme taşımak
bu değerlendirmeyi geçersiz kılar. Günlük raporlar izleme içindir; her
gün farklı eşik seçmek için kullanılmaz. İlk değerlendirme sabit mevcut
politika için yedi tam UTC gününden sonra, bekleyen sonuç sayısı ve belirsizlik
aralıklarıyla birlikte yapılır; yedi gün tek başına yeterli kanıt sayılmaz.

Rapor yayınlanmış seçilmiş sinyalleri ölçer; bütün maç evrenini veya
manuel kara/beyaz listelerin zamanla değişmesinin etkisini kanıtlamaz.
Win rate bahis getirisi değildir; odds fiyat biçimi ve bookmaker uzatma/
settlement sözleşmesi doğrulanmadan ROI/kârlılık sunulmaz. Tempo bandı da
istatistiksel güven aralığı değildir.

## Migration ve doğrulama

Tek migration: `alerts.prediction_context_json TEXT`, nullable ve eklemeli.
Eski satırlar NULL kalır; geçmişe sürüm/özellik atama yapılmaz. Eski şemayı
salt okunur değerlendirme, eklemeli/idempotent migration ve eski satırın
birebir korunması test edilir. Üretimde SQLite backup alınarak uygulanır.

Tam regression paketi: **354 test geçti**. Kaynak zamanı, ilk sinyalin
seçimi, bekleyen sonucun sonraki kazananla değiştirilmemesi, iadeler,
tutarsız sonuç, ham kaynak/karar bozulması, politika ayrımı, gizli ayarların
dışlanması ve eski SQLite uyumluluğu kontrolleri dahil.

Python compileall ve `git diff --check` geçti. Son startup logu değişikliğinden
sonra ilgili 33 test de geçti. Consistent SQLite backup 0600 izinle yerel,
git dışı `.codex/backups/` altında alındı. Bot 19:56:50, dashboard 19:56:49
Türkiye saatinde mevcut systemd servisleri üzerinden yeniden başlatıldı.
İkisi de active/running; yeni implementation hash'i startup logunda doğrulandı.
Migration sonrası backup'taki 1.274 alert'in bütün eski kolonları ve bütün
eski snapshot satırları birebir aynı; yeni bağlam kolonları 1.274/1.274 NULL.
DB `quick_check` yine ok. Ana/arşiv sayfaları ve API'leri HTTP 200; teknik
bağlam API çıktısında yok.

19:59:42 kontrolünde ilk çevrim 135 saniyede 50 maçtan 26'sını tamamladı:
3 doğrulanmış gözlem, 23 güvenli atlama, 24 erteleme, işleme/döngü hatası 0.
Üç yeni snapshot'ın üçünün yakalama anı kaynak kontrolü geçti. Kapsama %52;
kalan maçlar sonraki çevrimde öncelik alır. Kaynak doğrulamasının geçmesi
bütün maçların okunabildiği veya sinyalin başarılı olduğu anlamına gelmez.
Yeni alert/ileri sonuç/gerçek sinyal Telegram teslimi henüz 0.
