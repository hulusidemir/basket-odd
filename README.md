# Basket Odds Monitor

AIScore basketbol maçlarında aynı bahis şirketine ait maç önü (yoksa açılış)
ve canlı toplam baremini karşılaştıran Python/Flask uygulaması.

Referans ve dinamik eşik kuralı:

```text
referans = geçerli maç önü baremi, yoksa açılış baremi
eşik = referans × THRESHOLD_PERCENT / 100 × çeyrek katsayısı
Q2 ALT için eşik ayrıca Q2_ALT_THRESHOLD_MULTIPLIER ile çarpılır
hybrid modunda eşik = max(THRESHOLD, eşik)
abs(canlı toplam - referans) >= eşik
```

Canlı barem referanstan yüksekse yön `ALT`, düşükse `ÜST` olur.
Varsayılan `THRESHOLD_MODE=percent`, `THRESHOLD_PERCENT=7` değerleridir:
240 referansta eşik 16,8; 135 referansta 9,45 puandır. `THRESHOLD=10` yalnız
`hybrid` modunda alt sınır olarak kullanılır; percent modunu etkilemez.
Q4 varsayılan olarak kapalıdır; `DISABLE_Q4_SIGNALS=false` ile açılabilir.
Uzatma ve periyodu belirlenemeyen durumlarda sinyal üretilmez.
Q1–Q4 katsayıları ve Q2 ALT ek katsayısı varsayılan 1'dir. Örneğin Q1 katsayısı
1,2 eşiği %20 artırır; Q2 ALT katsayısı 0,9 yalnız bu yönde eşiği %10 azaltır.
Katsayılar ve Q4 yasağı dört çeyrekli maçlara uygulanır; NCAA gibi iki devreli
maçlarda ikinci devre Q2 olarak değerlendirilmez.
Dashboard'daki tempo projeksiyonu yalnız mevcut skor ve oynanan sürenin basit
doğrusal gösterimidir; sinyal üretimine katılmaz.

Canlı tarama sonuçları bütün maçların bitmesi beklenmeden, her maç okunur okunmaz
işlenir. Canlı tarayıcı iki ayrıntı sekmesiyle başlar; bağlantı hatasında
eşzamanlılığı otomatik düşürür ve sağlıklı döngülerden sonra yapılandırılan
`AISCORE_CONCURRENCY` tavanına doğru kademeli artırır. Yalnız doğrulanmış tam
maç `Total Points` piyasası ve kilitsiz aynı bookmaker satırı kabul edilir.
Bellekte `MAX_LIVE_OBSERVATION_AGE_SECONDS` süresini aşan gözlem sinyal üretemez.
Skor ve oyun saati ilerlediği halde değişmeyen barem de stale kabul edilip atlanır.
Tek bir bozuk ayrıntı sekmesi `LIVE_MATCH_TIMEOUT_SECONDS`, bütün canlı tarama
çevrimi ise `LIVE_SCRAPE_TIMEOUT_SECONDS` ile sınırlıdır; zaman aşımı sonraki
çevrimlerin çalışmasını engellemez.

## Korunan operasyonel kurallar

Canlı takip sağlıklı çevrimlerde aynı tarayıcı oturumunu kullanır; boşalan sekme
hemen sıradaki maçı okur. `LIVE_POLL_SECONDS=4` sağlıklı taramalar arasındaki
beklemedir. Hatalarda `POLL_INTERVAL_MIN/MAX` beklemesi ve oturum yenileme uygulanır.

- Açılış ve canlı toplam mümkün olduğunda aynı bookmaker satırından eşlenir.
- Aynı maçta dönem başına en fazla bir sinyal oluşur.
- Maç başına toplam sinyal sınırı ve aynı yöndeki minimum canlı-barem farkı uygulanır.
- Kara takım sinyali ve bekleyen Telegram gönderimini engeller. Beyaz takım,
  kara lig için istisnadır; kara rakip takım varsa engel devam eder. Beyaz lig
  tercih işaretidir. Listede olmayan maçlar normal sinyal kurallarını kullanır.
- Sinyaller SQLite'a yazılır; Telegram teslimatı kalıcı outbox ile tekrar denenir.
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
THRESHOLD_MODE=percent
THRESHOLD_PERCENT=7
THRESHOLD=10
DISABLE_Q4_SIGNALS=true
Q1_THRESHOLD_MULTIPLIER=1
Q2_THRESHOLD_MULTIPLIER=1
Q3_THRESHOLD_MULTIPLIER=1
Q4_THRESHOLD_MULTIPLIER=1
Q2_ALT_THRESHOLD_MULTIPLIER=1
DB_PATH=basketball.db
PLAYWRIGHT_PROXY=socks5://127.0.0.1:9050
FINISHED_PAGE_TIMEOUT_MS=20000
FINISHED_RETRY_ATTEMPTS=1
AISCORE_CONCURRENCY=4
LIVE_MATCH_TIMEOUT_SECONDS=90
LIVE_SCRAPE_TIMEOUT_SECONDS=180
MAX_LIVE_OBSERVATION_AGE_SECONDS=20
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
