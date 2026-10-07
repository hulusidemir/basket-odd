# Basket Odds Monitor

AIScore basketbol maçlarında aynı bahis şirketine ait maç önü (yoksa açılış)
ve canlı toplam baremini karşılaştıran Python/Flask uygulaması.

Güncel karar motoru Future Pace v9'dur. Her doğrulanmış canlı gözlemde bir
merkez tahmin hesaplanır; tahmin üretimi ile Telegram uygunluğu ayrıdır.

```text
öncül PPM = (geçerli maç önü baremi, yoksa açılış) / normal maç süresi
ham kalan PPM = (mevcut skor + öncül PPM × öncül dakika) / (oynanan dakika + öncül dakika)
son pencere = geçerli son 5 dakika, yoksa son 2 dakika
pencere PPM = (pencere sayısı + öncül PPM × öncül dakika) / (pencere dakika + öncül dakika)
ham kalan PPM > öncül PPM ise kalan PPM = min(ham kalan PPM, max(öncül PPM, pencere PPM))
pencere yoksa sıcak başlangıçta kalan PPM = öncül PPM; diğer durumda kalan PPM = ham kalan PPM
kalibrasyon rejiminde kalan PPM = max(0, kalan PPM - 1.015189277288373 × (kalan PPM - öncül PPM))
tahmini toplam = mevcut skor + kalan PPM tahmini × kalan normal süre
barem tahmini toplamdan küçükse ÜST, büyükse ALT; tam eşitlik EŞİT
```

`PRIOR_EQUIV_MINUTES` varsayılan 10 dakikadır; bu çalışmada sonuçlara göre
ayarlanmadı. Sıcak başlangıcın kalan süreye taşınması artık tek bir yakın
aralığın sayı hızıyla sınırlandırılır. Son 5/2 dakika birden fazla bağımsız
oy sayılmaz; periyot aralıkları açıklayıcıdır. Pencere eksikliği tahmini
engellemez. `OVER_CONTINUATION_ENABLED` varsayılan true; false eski bütün-maç
hesabını geri getirir ve farklı dondurulmuş politika oluşturur.
V9'da yüksek yakın hızın kalan süreye taşınma hatası eğitim verisinden
öğrenilen tek katsayıyla düzeltilir. Yalnız 40/48 dakika, oynanan >=12,
kalan >=5,5, 10 dakikalık öncül ve doğrulanmış yakın hız >= öncül rejiminde
uygulanır; başka gözlemler v8 hesabıyla devam eder. Bayrak
`OVER_CALIBRATION_ENABLED` varsayılan true. Dondurulmuş model/hash ve
uygulanan düzeltme tahmin/kararda saklanır; canlı eğitim veya DB okuması yok.
Model önceki kayıtlarda seçilmiş veriye dayanır; ayrıntı ve kapsam:
`docs/OVER_CALIBRATION_V9_2026-10-07.md`.
`MIN_VALID_FUTURE_PACES` ve `MAX_FUTURE_BAND_WIDTH` eski
v5/v6 ayarları olarak korunur, yeni yönü veto etmez.

`/forecasts` ekranında bildirim çıkmayan erken/geç veya küçük avantajlı
maçların tahminleri de bulunur. Farklı bir barem girilerek aynı dondurulmuş
merkezle yön/fark karşılaştırılabilir; bu senaryo kayıtlı tahmini değiştirmez.
Tam eşitlikte rastgele ALT/ÜST seçilmez. Son üç dakikada doğrulanmış yeni
gözlemi olmayan maç güncel tahminler ekranından çıkar.

Aynı ekrandaki başarı kartları maç başına **ilk kayıtlı tahmini** değerlendirir.
Başarı = doğru / (doğru + yanlış); bekleyen, iade ve EŞİT/yönsüz kayıtlar
orana katılmaz. "Tahmin geçmişi" bölümünde bütün dondurulmuş tahminler ve
otomatik final sonuçları 50'şer kayıtla gösterilir. Geçici barem senaryoları
geçmişe veya başarı oranına yazılmaz. Başarı özeti ve geçmiş dakikada bir
yenilenir; sayfalama yeni kayıtlar gelince önceki kayıtları atlamaz.

Telegram için ilk 12 dakika ve son 5 dakikanın altı beklenir; minimum sayı
avantajı `MIN_EDGE_POINTS` ile `canlı × MIN_EDGE_RATIO` değerlerinin büyüğüdür.
Büyük skor farkı ve piyasa hareketi gereken avantajı artırır. Tekrar, periyot
ve liste kuralları korunur. Uzatma ve belirsiz periyot hesaplanmaz. Tahmini
toplam normal süre içindir; uzatma dahil piyasa ile sözleşme uyumu henüz
çözülmüş değildir. Bu tahmin kalibre edilmiş kazanma olasılığı değildir.
Eski `THRESHOLD*`, `Q*_THRESHOLD_MULTIPLIER` ve `DISABLE_Q4_SIGNALS` alanları
uyumluluk için kalır. Dashboard'daki basit tempo projeksiyonu karar hesabı değildir.

Canlı tarama sonuçları bütün maçların bitmesi beklenmeden, her maç okunur okunmaz
işlenir. Canlı tarayıcı iki ayrıntı sekmesiyle başlar; bağlantı hatasında
eşzamanlılığı otomatik düşürür ve sağlıklı döngülerden sonra yapılandırılan
`AISCORE_CONCURRENCY` tavanına doğru kademeli artırır. Doğrulanmış tam maç `Total Points` (`bs`) satırı hangi şirkete ait olursa
olsun kullanılabilir. Açılış, maç önü ve canlı değerler aynı şirketten alınır;
farklı şirketlerin baremleri birleştirilmez. Canlı uygulama verisi ve seçilen
şirketin ekrandaki hücresi aynı baremi göstermeli, kilitsiz olmalı ve maç
kimliği/periyot/skor doğrulanmalıdır. Dolu canlı hücre için odds tarihçesi
zorunlu değildir; `aiscore_live_v2` kanıtı yakalama anı ve uygulama/ekran
uyumunu saklar. Bağımsız sağlayıcı tazeliği kanıtı olarak sunulmaz.
Boş canlı hücre için aynı şirketin doğrulanmış güncel tarihçesi kullanılabilir;
bu fallback'te sağlayıcı zaman/skor/saat ve ham yanıt kontrolü sürer.
Uygun olmayan şirket tüm maçı engellemez; diğer şirket denenir.

Quality v1 puanı yalnız açıklayıcıdır; başarı olasılığı veya yayın barajı değildir.
Eski `MIN_SIGNAL_QUALITY` ayarı yayın kararına katılmaz. Sinyal için doğrulanmış
barem ve v9 merkez tahmininin yeterli sayı avantajı zorunludur.
Yeni gerçek sinyal, kaynak kanıtıyla birlikte kararın kod sürümünü ve kullanılan
ayarları dondurur. İleriye dönük değerlendirme
`venv/bin/python forward_validation.py --db basketball.db` ile salt okunur çalışır; her sürümde maçın ilk sinyalini
sonuçtan önce seçer, ALT/ÜST ve gün sonuçlarını ayrı gösterir. Model/piyasa/maç
önü kalan-hız bazının ortalama hatası ve kayıtlı/teslim edilmiş sinyal paydaları
da ayrı verilir. M2 erişim durumları/gerekçeleri raporda bulunur. Eski v5 ve
v6/v7/v8/v9 politikaları karışmaz; geçmiş tahminler yeniden yazılmaz. Yöntem ve mevcut
kanıtın sınırları `docs/FORWARD_VALIDATION_2026-10-04.md` içindedir.
Bildirim çıkmayan tahminler de `match_live_snapshots.forecast_json` içinde
ilk kayıt anında dondurulur. Saatlik otomatik biten maç taraması bu maçları
ayrıca kontrol eder, final gözlemini `forecast_match_results` içine yazar.
Salt okunur rapor `all_forecasts` altında her maç/politikanın ilk tahminini
sonuçtan önce seçer. Sonraki kazanan ilk kaybın yerine geçmez. Yeni nullable
kolon ve final tablosu eklemelidir; eski snapshot/arşiv/sonuçlar doldurulmaz.
Geçmiş/başarı ekranı için yalnız iki küçük kısmi indeks eklenir; yeni kayıt
tablosu, scraper veya arka plan worker'ı yoktur. `/api/forecasts/history`
salt okunur ve sayfa başına en fazla 100 kayıt döndürür. Ekranın maç başına
ilk kayıt özeti ile CLI'ın politika başına ilk kayıt raporu ayrı örneklemlerdir.
V7 davranışı ve başarının sınırları `docs/SIGNAL_REPAIR_2026-10-07.md` içindedir.
Bellekte `MAX_LIVE_OBSERVATION_AGE_SECONDS` süresini aşan gözlem sinyal üretemez.
Skor ve oyun saati ilerlediği halde değişmeyen barem de stale kabul edilip atlanır.
Tek bir bozuk ayrıntı sekmesi `LIVE_MATCH_TIMEOUT_SECONDS`, bütün canlı tarama
çevrimi ise `LIVE_SCRAPE_TIMEOUT_SECONDS` ile sınırlıdır; zaman aşımı sonraki
çevrimlerin çalışmasını engellemez.

Yoğun canlı listede ayrıntı okuma bütçesi dolarsa tamamlanan sonuçlar hemen
işlenir, ertelenen maçlar sonraki güncel listede öncelik alır. Maç sayısı
limiti de listenin başındaki maçları sürekli tekrar etmek yerine aynı
devam kuyruğunu kullanır. Kapasite ertelemesi sağlık raporunda `continuing`
ve gerçek kapsama sayılarıyla gösterilir; tarayıcı oturumu korunur. Kaynak
hataları veya hiç ilerlemeyen tarama `partial/error` olarak bildirilir.
Maça ait kaynak/history hatası diğer maçların oturumunu sıfırlamaz; kısmi
kapsama açıkça raporlanırken sonraki tarama kısa polling ile devam eder.
History/source isteği aynı maçta hemen ikinci bir navigasyonla tekrarlanmaz;
sonraki çevrimde yeniden okunur. Ağ/tarayıcı arızası veya hiç ilerlemeyen
tarama oturum yenileme ve uzun beklemeyi tetikler.
Eksik/kilitli/kimliği uyuşmayan kaynak satırına history isteği yapılmaz.

## Korunan operasyonel kurallar

Canlı takip sağlıklı çevrimlerde aynı tarayıcı oturumunu kullanır; boşalan sekme
hemen sıradaki maçı okur. `LIVE_POLL_SECONDS=4` sağlıklı taramalar arasındaki
beklemedir. Oturum/liste arızalarında `POLL_INTERVAL_MIN/MAX` beklemesi ve
oturum yenileme uygulanır; maç bazındaki veri hataları bütün akışı bekletmez.

- Açılış ve canlı toplam mümkün olduğunda aynı bookmaker satırından eşlenir.
- Aynı maçta dönem başına en fazla bir sinyal oluşur.
- Maç başına toplam sinyal sınırı ve aynı yöndeki minimum canlı-barem farkı uygulanır.
- Kara takım sinyali ve bekleyen Telegram gönderimini engeller. Beyaz takım,
  kara lig için istisnadır; kara rakip takım varsa engel devam eder. Beyaz lig
  tercih işaretidir. Listede olmayan maçlar normal sinyal kurallarını kullanır.
- Sinyaller SQLite'a yazılır; Telegram teslimatı kalıcı outbox ile tekrar denenir.
  Outbox taramadan bağımsız iki saniyede bir kontrol edilir. İlk gönderimi
  devam eden sinyal retry dışında tutulur; tazelik ve kara liste doğrulaması
  her tekrar öncesinde korunur. Gönderim hâlâ başarısızsa sekiz deneme sınırı
  ve kaynak tazeliği iptali uygulanır.
- Final skorlar otomatik kontrol edilerek arşivlenen sinyaller sonuçlandırılır.
- Biten maçlar mobil kaynaktan sıralı ve yeniden denemeli kontrol edilir; manuel
  buton ile saat başındaki görev aynı arka plan işini kullanır. Kontrol edilen,
  arşivlenen ve okunamayan maç sayıları ekranda görünür; sayfa yenilenince devam
  eden taramaya yeniden bağlanılır. Doğrulanan bitmiş maçlar hemen arşivlenir.
- Dashboard aktif sinyalleri, arşivi, yaklaşan maçları ve takip işlemlerini gösterir.
- Ana ekrandaki açılır takım/lig bölümünden listeler aranabilir ve yönetilebilir.
  Modalda kara/beyaz seçimi yapılır; seçili düğmedeki `Çıkar` kaydı kaldırır.
  Bir takım veya lig aynı anda yalnız bir listede bulunabilir.
- Canlı PPM sütunu `mevcut → gereken (% fark)` gösterir. Yüzdenin tabanı mevcut
  tempodur; pozitif değer gereken hızlanmadır. Eksik veri yüzde sıfırmış gibi gösterilmez.

## Kurulum

```bash
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
python -m camoufox fetch
```

`.env` için temel alanlar:

```dotenv
TELEGRAM_TOKEN=...
TELEGRAM_CHAT_ID=...
AISCORE_URL=https://m.aiscore.com/basketball
PRIOR_EQUIV_MINUTES=10
OVER_CONTINUATION_ENABLED=true
OVER_CALIBRATION_ENABLED=true
MIN_VALID_FUTURE_PACES=2
MIN_ELAPSED_MINUTES=12
MIN_REMAINING_MINUTES=5
MIN_EDGE_POINTS=4
MIN_EDGE_RATIO=0.02
DB_PATH=basketball.db
PLAYWRIGHT_PROXY=socks5://127.0.0.1:9050
FINISHED_PAGE_TIMEOUT_MS=20000
FINISHED_RETRY_ATTEMPTS=1
AISCORE_CONCURRENCY=4
LIVE_MATCH_TIMEOUT_SECONDS=90
LIVE_SCRAPE_TIMEOUT_SECONDS=180
MAX_LIVE_OBSERVATION_AGE_SECONDS=20
MAX_LIVE_PROVIDER_AGE_SECONDS=30
LIVE_LINE_STALE_SECONDS=90
LIVE_LINE_STALE_SCORE_DELTA=10
LIVE_LINE_STALE_GAME_MINUTES=2
```

Canlı bot `python main.py`, birleşik dashboard `python run.py` ile çalışır.
Servis kurulumunda bu süreçler systemd tarafından yönetilebilir.

## Maç durumunu güncelle

Aktif sinyalin işlemlerindeki dönen ok veya modal içindeki **Maç durumunu
güncelle** düğmesi güncel skor, çeyrek/saat ve canlı baremi çeker. Güncel PPM,
kalan sayı/süre ve tempo yorumu her zaman sinyalin verildiği baremi kullanır.
Örneğin 180 ALT sinyalinin değerlendirmesi, yeni canlı barem 170 olsa da
180 üzerinden yapılır. Yeni canlı barem ayrı gösterilir.

Güncel durum modalın üstündedir; sinyal anındaki bilgiler altındadır. İstek
sürerken yüklenme mesajı görünür, tekrar tıklama engellenir. Veri alınamazsa
açık hata gösterilir; varsa son başarılı kontrol saat bilgisiyle korunur.
Bu geçici bilgi aynı sayfada modal kapanıp açılınca kalır, sayfa yeniden
yüklendiğinde sıfırlanır. Orijinal sinyal, arşiv snapshot'ı ve sonuç alanları
değişmez. Kesin sonuç mevcut otomatik final kontrolüyle yazılır.

## Test

```bash
python -m pytest -q
python -m compileall -q .
```

Yerel veritabanı, loglar, `venv/` ve `__pycache__/` kaynak dosya değildir.

## Veri düzeni

`Database.init()` başlangıçta `alerts` tablosuna nullable `reference_used`
(prematch/opening), `reference_total` ve `effective_threshold` kolonlarını
`BEGIN IMMEDIATE` altında ekler. Geçiş tekrar çalıştırılabilir; mevcut sinyal
satırları ve snapshot'lar yeniden yazılmaz. Yeni sinyallerde `diff`, karar
referansına göre farkın mutlak değeridir; Telegram işaretli farkı kayıtlı yönle
gönderir ve tekrar denemede aynı referansı/eşiği kullanır. Dashboard açılış
hareketini korur, modalda ayrıca karar referansını, farkını ve eşiğini gösterir.
Bu bilgiler de arşiv snapshot'ına alınır; eksik snapshot alanları boş kalır.
V5'te `effective_threshold=0`; karar eşiği yüzde barem farkı değildir.

Canlı ve arşivlenmiş sinyaller `alerts`, yaklaşan maçlar `upcoming_matches`
tablosundadır. Yaklaşan maçları temizlemek sinyal kayıtlarına dokunmaz.
Dashboard arşivleme işlemi, gösterim verisini ve arşiv durumunu tek işlemde
kaydeder. Sonuçlar yalnız final skor kontrolüyle yazılır.

6 Eylül 2026 temizliğinde kullanılmayan model tablosu, yerel eski veritabanı
yedekleri ve eski dışa aktarımlar kaldırıldı. Çalışan tabloların şeması ve
mevcut sinyal kayıtları korundu; tablo yeniden oluşturma veya sinyal taşıma
migrasyonu gerekmedi.

Liste yönetimi güncellemesinde `Database.init()` yalnız `signal_lists` kayıtlarının
adlarını eşleştirme için normalleştirir ve `(scope, normalized_value)` benzersiz
indeksini ekler. Eski çakışmalarda önceki engelleme davranışını korumak için kara
kayıt kalır; yinelenen liste kayıtları kaldırılır. Sinyaller ve arşiv snapshot'ları
değişmez. Geçiş tek işlemde yapılır ve tekrar çalıştırılabilir. Python servisleri
yeniden yüklendiğinde yeni kurallar devreye girer. Ortam yapılandırmasındaki
`BLACKLIST` filtresi ayrıca geçerlidir; beyaz liste bu filtreyi aşmaz.

Arşiv görünümü açılış/mevcut/gereken PPM'i, yüzde farkını, kalan sayı ve süreyi,
tempo yorumunu, sinyal saatini ve kullanıcı işaretlerini canlı görünümden saklar.
Geçmişte PPM veya yön düğmesi kayıtlı modalı açar. Takım geçmişi de arşiv anındaki
haliyle kalır. Snapshot sürümü 2 bu alanları JSON'a ekler; tablo geçişi gerekmez.
Eski snapshot'lar değiştirilmez, eksik alanları sonradan hesaplanmaz.

### ALT/ÜST başarı takibi

ALT/ÜST sinyallerinin ayrı doğru/yanlış, bekleyen ve başarı oranı ana
ekrandaki yön kartlarında görünür; karma eski motorlar dahildir. Her maçın
ilk kayıtlı sinyali bir kez sayılır. Tüm tahminler ekranı bildirimsiz
tahminleri de kapsayan ayrı ALT/ÜST özetini gösterir. Kaynak/kalibrasyon
denemesi için `venv/bin/python directional_audit.py` yalnız toplam metrikler
üretir; canlı sinyali değiştirmez. Ayrıntılar:
[yön kalibrasyon raporu](docs/DIRECTIONAL_CALIBRATION_2026-10-07.md).

### Dashboard M2

7 Ekim incelemesi sonrası M2 canlı akıştan kaldırıldı: bot M2 görevi/tarayıcısı
başlatmaz, yeni sinyallere M2 analizi eklemez. Canlı ve geçmiş ekranlarında
M2 sütunu, düğmesi ve modalı yoktur; eski M2 JS/CSS/modal dosyaları kaldırıldı.
Canlı/geçmiş API'leri eski M2 alanlarını döndürmez. Eski `M2_ENABLED` ortam
ayarı kullanılmaz. DB'deki dondurulmuş eski kayıtlar aynen korunur; migration
veya geçmişi yeniden yazma yok. İnceleme: [M2_REVIEW_2026-10-07.md](docs/M2_REVIEW_2026-10-07.md).
Eski deneysel modelin açıklaması: [MOTOR2.md](docs/MOTOR2.md).
