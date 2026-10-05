# Basket Odds Monitor

AIScore basketbol maçlarında aynı bahis şirketine ait maç önü (yoksa açılış)
ve canlı toplam baremini karşılaştıran Python/Flask uygulaması.

Güncel karar motoru Future Pace v5'tir:

```text
öncül PPM = (geçerli maç önü baremi, yoksa açılış) / normal maç süresi
gereken PPM = (canlı toplam - mevcut toplam skor) / kalan normal süre
tempo bandı = öncüle yaklaştırılmış geçerli tempo pencerelerinin alt/üst sınırı
gereken PPM bandın altında ve sayı avantajı yeterliyse ÜST
gereken PPM bandın üstünde ve sayı avantajı yeterliyse ALT
diğer durumlar PAS
```

Tempo pencereleri tüm maç, son 2/5 dakika ve mevcut periyottur. Varsayılan
en az iki geçerli tempo gerekir; ilk 12 dakika, son 5 dakikanın altı ve fazla
geniş tempo bandı PAS olur. Minimum sayı avantajı `MIN_EDGE_POINTS` ile
`canlı × MIN_EDGE_RATIO` değerlerinin büyüğüdür; büyük skor farkı ve büyük
piyasa hareketi gereken avantajı artırır. Uzatma ve belirsiz periyot sinyalsizdir.
Q4 kalan süre ve diğer v5 kontrollerini karşılıyorsa değerlendirilir.
Eski `THRESHOLD*`, `Q*_THRESHOLD_MULTIPLIER` ve `DISABLE_Q4_SIGNALS`
alanları yapılandırmada uyumluluk için kalır; v5 yön kararında kullanılmaz.
Dashboard'daki tempo projeksiyonu yalnız mevcut skor ve oynanan sürenin basit
doğrusal gösterimidir; sinyal üretimine katılmaz.

Canlı tarama sonuçları bütün maçların bitmesi beklenmeden, her maç okunur okunmaz
işlenir. Canlı tarayıcı iki ayrıntı sekmesiyle başlar; bağlantı hatasında
eşzamanlılığı otomatik düşürür ve sağlıklı döngülerden sonra yapılandırılan
`AISCORE_CONCURRENCY` tavanına doğru kademeli artırır. Yalnız doğrulanmış tam
maç `Total Points` piyasasının bet365 (`company_id=2`, `bs`) satırı kabul edilir.
Canlı barem bet365 history yanıtındaki en yeni kayıttan alınır; kilitliyse
eski satıra dönülmez. Maç
kimliği, periyot ve skor aynı, saat farkı en fazla 30 saniye olmalıdır.
Sağlayıcı güncellemesi `MAX_LIVE_PROVIDER_AGE_SECONDS` (varsayılan 30 saniye)
sınırını aşarsa veya doğrulama yapılamazsa sinyal gönderilmez. Listede canlı
bet365 satırı varsa history ile eşleşmelidir; sütun boşsa açılış/maç önü aynı
bet365 satırından, canlı barem doğrulanan history'den alınır. Başka bookmaker,
alternate barem veya eski history satırına fallback yapılmaz. Ham history yanıtı
ve kaynak kimliği yeni sinyalde saklanır; mevcut geçmiş kayıtlar değiştirilmez.
Quality v1 puanı yalnız açıklayıcıdır; başarı olasılığı veya yayın barajı değildir.
Eski `MIN_SIGNAL_QUALITY` ayarı yayın kararına katılmaz. Sinyal için doğrulanmış
güncel barem ve Future Pace v5'in tempo/sayı avantajı koşulları zorunludur.
Yeni gerçek sinyal, kaynak kanıtıyla birlikte kararın kod sürümünü ve kullanılan
ayarları dondurur. İleriye dönük değerlendirme
`venv/bin/python forward_validation.py --db basketball.db` ile salt okunur çalışır; her sürümde maçın ilk sinyalini
sonuçtan önce seçer, ALT/ÜST ve gün sonuçlarını ayrı gösterir. Yöntem ve mevcut
kanıtın sınırları `docs/FORWARD_VALIDATION_2026-10-04.md` içindedir.
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
