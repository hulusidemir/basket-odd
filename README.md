# Basket Odds Monitor

AIScore basketbol maçlarında aynı bahis şirketine ait açılış ve canlı toplam
baremini karşılaştıran Python/Flask uygulaması.

Bir sinyal yalnız şu koşulla oluşur:

```text
abs(canlı toplam - açılış toplam) >= THRESHOLD
```

Canlı barem açılıştan yüksekse yön `ALT`, düşükse `ÜST` olur.
Dashboard'daki tempo projeksiyonu yalnız mevcut skor ve oynanan sürenin basit
doğrusal gösterimidir; sinyal üretimine katılmaz.

## Korunan operasyonel kurallar

- Açılış ve canlı toplam mümkün olduğunda aynı bookmaker satırından eşlenir.
- Aynı maçta dönem başına en fazla bir sinyal oluşur.
- Maç başına toplam sinyal sınırı ve aynı yöndeki minimum canlı-barem farkı uygulanır.
- Kara takım sinyali ve bekleyen Telegram gönderimini engeller. Beyaz takım,
  kara lig için istisnadır; kara rakip takım varsa engel devam eder. Beyaz lig
  tercih işaretidir. Listede olmayan maçlar normal sinyal kurallarını kullanır.
- Sinyaller SQLite'a yazılır; Telegram teslimatı kalıcı outbox ile tekrar denenir.
- Final skorlar otomatik kontrol edilerek arşivlenen sinyaller sonuçlandırılır.
- Biten maçlar mobil kaynaktan sıralı ve yeniden denemeli kontrol edilir; manuel
  buton ile saat başındaki görev aynı akışı kullanır.
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
THRESHOLD=10
DB_PATH=basketball.db
PLAYWRIGHT_PROXY=socks5://127.0.0.1:9050
FINISHED_PAGE_TIMEOUT_MS=20000
FINISHED_RETRY_ATTEMPTS=1
```

Canlı bot `python main.py`, birleşik dashboard `python run.py` ile çalışır.
Servis kurulumunda bu süreçler systemd tarafından yönetilebilir.

## Test

```bash
python -m pytest -q
python -m compileall -q .
```

Yerel veritabanı, loglar, `venv/` ve `__pycache__/` kaynak dosya değildir.

## Veri düzeni

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
