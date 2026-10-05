# Decisions

## Maç kaynak hatasının oturum arızasından ayrılması ve hızlı Telegram retry

5 Ekim 2026 — Botun 4 Ekim 20:42 yeniden başlamasından sonraki salt okunur
günlük incelemesinde 10 sinyal teslimi, bir Telegram timeout'u ve bu gönderimin
sonraki çevrimde kaynak süresi dolduğu için iptali görüldü. History/source
okuma hataları kısmi taramada tüm tarayıcıyı kapatıp 25–40 saniye bekletiyordu.
Maç bazındaki eksik/bozuk kaynak `partial` ve gerçek kapsamayla raporlanır;
tamamlanan maçlar varsa oturum korunur ve dört saniyelik polling kullanılır.
Bu kaynak hataları artık sıfır parsed kayıt durumunda ana döngü exception'ına
çevrilmez. Navigasyon arızası, kapanmış tarayıcı, bütün maçların timeout olması
veya bütçe boyunca sıfır ilerleme `session_reset_required` ile oturumu yeniler;
liste/hard timeout arızalarının mevcut toparlanması korunur. Kaynak/history
isteği aynı çevrimde ikinci navigasyonla tekrarlanmaz, sonraki çevrimde denenir.
Ham history response'u Vue'nun ekranda render etmeyi bitirmesini beklemez.

Telegram outbox taramadan bağımsız, aynı asyncio döngüsünde iki saniyede bir
çalışır. İlk gönderimi devam eden alert ID'si retry dışında tutulur; gönderim
sona erince veya hata verince koruma bırakılır. Worker hatası taramayı kapatmaz;
bot kapanışında worker iptal edilir ve scraper kapatılır. Her retry'da mevcut
ham kaynak/barem/tazelik ve kara liste doğrulamaları sürer. Geçerli sinyal
sayısını artırmak için kaynak yaşı veya Future Pace kuralları gevşetilmez.
Şema/migration, geçmiş sinyal, arşiv snapshot'ı veya sonuç değişikliği yoktur.

## Kanıtlanmamış kalite yayın barajının kaldırılması

4 Ekim 2026 — Kullanıcı önceki sinyal akışını koruyarak barem hatasının
giderilmesini istedi. Yeni 70 barajı sonrası loglarda Future Pace'in geçen
71 gözleminin tamamı (43 farklı maç) kalite 0–69 nedeniyle engellendi.
Quality v1'in sonraki dönem sıralama başarısı AUC 0,503; bu veto için yeterli
kanıt yok. Önceki 70 yayın eşiği kararı kaldırıldı: mevcut tempo, konservatif
sayı avantajı, zaman, tekrar ve kara liste kuralları korunur; kalite v1 yalnız
açıklayıcı olarak aynı şekilde saklanır. Eski MIN_SIGNAL_QUALITY ortam ayarı
uyumluluk için okunur, yayın ve retry kararına katılmaz. ALT/ÜST dışındaki
kararlar filtre gerekçesi boş olsa bile yayımlanamaz. Bet365/bs/raw history
kimlik, barem, skor, saat, tazelik ve gönderim öncesi doğrulaması zorunludur.
Yeni frozen politikada publication=verified_future_pace_v5 açıkça belirtilir;
eski frozen politikaların kendi 70 koşulu değerlendirmede korunur. Eski
alert/snapshot/arşiv, kalite formülü ve sonuçlar yeniden yazılmaz. Yeni DB
migration veya shadow sistem yok. Bu değişiklik tahmin başarısı garantisi
değildir; yeni kaynakla ileriye dönük sonuçlar henüz yeterli değildir.

## Geçmişe uyarlama olmadan gerçek tahmin doğrulaması

4 Ekim 2026 — Kullanıcının doğru AiScore verisi ve ileriye dönük sınama
talebi kapsamında Quality v1 ve 70 yayın eşiği geçmiş başarılara göre yeniden
ayarlanmaz. Maç başına ilk sinyalle sonraki dönemde Quality AUC 0,503;
yüksek skorun güvenilirlik üstünlüğü gösterilemedi. Doğru kaynaklı yeni
tahminin başarısı henüz ölçülemiyor. Kanıtlı aritmetik/zaman kaydı kusurları
regression testleriyle düzeltilir. Yalnız gerçek yayımlanan sinyallerin
kod/ayar politikasını donduran nullable DB bağlamı eklenir; shadow sistem
ve geçmişe backfill yoktur. Salt okunur rapor sonuçtan önce maçın ilk
sinyalini seçer ve her politika için ayrı ALT/ÜST/gün sonuçlarını verir.
Bekleyen, iade ve geçersiz sonuç başarı sayılmaz. Ayrıntılar ve önceden
sabitlenen değerlendirme yöntemi `FORWARD_VALIDATION_2026-10-04.md` içindedir.

## Süre bütçeli canlı tarama ve kalan maçların önceliği

4 Ekim 2026 — İlk iki üretim çevrimi 50/49 maçta 180 saniye sınırını aştı.
Tarayıcı kapanıp yeni çevrim aynı listenin başından başlıyordu; kuyruğun sonu
geride kalıyordu ve eşzamanlılık artışı için tamamlanmış çevrim oluşmuyordu.
Hard watchdog artırılmaz ve kaynak doğrulaması gevşetilmez. İç ayrıntı
deadline'ı bütçenin %25'i (en çok 45 saniye) kadar cleanup payı bırakır.
Okunamadan kalan maçlar sonraki güncel listedeki mevcut maçlarla kesiştirilip
önce taranır. Zaman/maç kapasitesi nedeniyle erteleme kaynak arızasından ayrı
`continuing` raporlanır; oturum ve kısa polling korunur. Tamamlanan,
ertelenen ve iptal edilen görev sayıları gerçeğe göre kaydedilir; kapsama
oranı eksikliği saklamaz. Kaynak hataları ve sıfır ilerleme hâlâ arızadır.
History ön kontrolü aynı kaynak kurallarını kullanır; history sonrası tam
doğrulama ve gönderim sınırındaki kanıt kontrolü korunur. Eski DB/snapshot
ve arşiv kayıtları değiştirilmez; migration gerekmez.

## Bet365 kaynak doğrulaması ve yayın eşiği

4 Ekim 2026 — Kullanıcının canlı barem hatasını düzeltip servisi yeniden başlatma
talebiyle bet365 (`company_id=2`) ve ana `bs` total market'i zorunlu tutulur.
İlk geçerli bookmaker satırının sıra bağımlılığı kaldırılır. Canlı baremin
asıl kaynağı aynı kimlikteki sağlayıcının en yeni history kaydıdır. Listede canlı
değer varsa history ile eşleşmelidir; canlı sütunu boşsa doğrulanan history
kullanılır. Açılış/maç önü aynı bet365 satırından alınır.
Eşleşmeyen/kilitli/eski/ambiguous kayıtlar atlanır; alternatif veya eski bareme
dönülmez. Sağlayıcı zaman damgası yoksa tazelik kanıtlanmış sayılmaz. Tarihçe
yanıtının ham body'si yeni alert ile saklanır ve gönderim öncesi yeniden doğrulanır.
Eski alert'lerin live/quality/result alanları ve arşiv gösterimi korunur.
Bu karar önceki Quality v1'in bütün ALT/ÜST adaylarını yayınlama politikasını
değiştirir: varsayılan minimum kalite 70'tir. Sayısal puanlama ve Future Pace v5
matematiği korunur; eşik bir backtest optimizasyonu veya başarı garantisi değildir.
Eski snapshot'larda bet365 kanıtı olmadığı için yeni tempo çıpası olamazlar.
Nullable kaynak kanıtı kolonları eklenir; NULL geçmişe backfill yapılmaz.

Kesinti sonrası 4 Ekim devamı: boş canlı sütunu yalnız tek ve erişilebilir
bet365 DOM hücresi gerçekten boşsa history ile tamamlanabilir; kilitli, eksik
veya çok baremli hücre kaynak kanıtı sayılmaz. Kaynak saatinde geçerli saniye
ve history Q öneki eşleşmesi zorunludur. Protobuf tekil alanlarında tekrar
varsa ilk değeri seçmek yerine yanıt reddedilir. Kaynak kanıtı olmayan eski
snapshot'lar yeni sinyalin süre formatını ve saat/skor kronolojisini de
belirleyemez. İlk kanıtlı gözlem eski gözlemle aynı değerlere sahip olsa bile
ayrı eklenir; DB geçmişi değiştirilmez ve ek migration gerekmez. Ham odds
kanıtı dashboard görünümüne ve arşiv display_snapshot'ına taşınmaz.

## Reversal için ileriye dönük feature toplama

2 Ekim 2026 — Future Pace v5'in ALT/ÜST/PAS yönü ve Quality v1 puanlaması
korunur. Gelecekteki reversal araştırması için yalnız yeni sinyallerde
hesaplanan türetilmiş veriler `alerts.reversal_features_json` içinde saklanır;
`alerts.reversal_features_version` basit sürüm işaretidir. Opening, prematch,
live, Fair Total ve periyot zaten kolonlarda bulunduğundan tekrar kolon
eklenmez. JSON'daki eksik tempo penceresi sayısal sıfır yerine NULL'dır.
Pencere verileri v5'in yön kararına veya Quality'ye girmez. Eski kayıtlar
backfill edilmez; snapshot geçmişi korunur. Eski SQLite dosyası iki nullable
kolonun kilitli, tekrarlanabilir eklenmesiyle uyumludur.

## Format, kronoloji ve Quality v1 sürekliliği

Normal süre yalnız turnuva metadata'sından seçilir; takım adındaki NBA/NCAA
ibareleri süre veya devre biçimini değiştirmez. Saat gerilemesi ve yakalanma
zamanı eski olan gözlem sinyal akışına alınmaz. Skor düşüşü ilk gözlemde
sinyal üretmez; sonraki oyun zamanı gözlemi düzeltmeyi doğrularsa tempo
pencereleri düzeltmeden itibaren kurulur. Tek gözlemlik skor düşüşü tempo
penceresinden çıkarılır. Geçmiş snapshot ve arşiv gösterimi yeniden yazılmaz.

Quality v1'in ALT negatif barem hareketi ve ÜST tempo katkısı eşik civarında
kademeli hesaplanır. Bileşenler ve etiket aralıkları korunur. DB şeması ve
geçişi değişmez; eski quality alanları doldurulmaz. Future Pace v5 karar
matematiği ve Fair Total merkezi değişmez. AiScore `Total Points` etiketinde
normal süre/uzatma sözleşmesi açık olmadığı için settlement sözleşmesi
doğrulanmamıştır; sonuçlandırma tahminle değiştirilmez.

## Future Pace v5 selectivity ve Quality v1

Future Pace v5 aday kararı ve adil barem hesabı korunur. ENGINE PAS, motorun
gerçekten ALT/ÜST üretmediği durumdur. ALT'da referansın en az 10 sayı altındaki
canlı barem ile ÜST'te düşük tempo avantajı veya 8 sayının altındaki adil barem
farkı yalnız Quality v1 bileşenlerini düşürür. Eşikler `Config` üzerinden ayarlanır.
Quality etiketleri: 0–39 PAS, 40–54 DÜŞÜK, 55–69 ORTA, 70–84 YÜKSEK,
85–100 ÇOK YÜKSEK. QUALITY PAS, motorun ALT/ÜST ürettiği fakat kalitenin düşük
olduğu kayıttır; gözlem ve sonuç ölçümü için saklanır, yayınlanır ve sonuçlandırılır.

Quality v1 yalnız kayda girecek sinyal için hesaplanır; Telegram veya dashboard
yayınını filtrelemez. `alerts` tablosuna dört nullable kalite alanı eklemeli,
tekrar çalıştırılabilir migration ile gelir. Puan ve bileşenler INSERT sırasında
dondurulur; arşiv ve ekran DB alanlarını okur. Eski kayıtlar NULL kalır ve
geçmiş sinyaller için backfill yapılmaz.

## Canlı tekrar tarama gecikmesi

Sağlıklı taramalar aynı Scrapling oturumunu kullanır. Ayrıntılar sınırlı sayıda
eşzamanlı görevle, boşalan kapasite hemen doldurularak okunur. Maç sonuçları
tamamlanınca hemen iletilir. Sağlıklı çevrimler arasında varsayılan dört saniye
beklenir; kaynak arızasında eski bekleme aralığı ve oturum yenileme uygulanır.
Kaynağın kendi gecikmesi bu iyileştirmeyle ölçülmüş veya giderilmiş sayılmaz.

## Canlı tarama süre sınırları

20 Eylül 2026 — Playwright sayfa zaman aşımı tek başına çevrim süresi garantisi
değildir; hatalı navigasyondan sonra tarayıcı tarafındaki bekleme veya kapatma
çağrısı askıda kalabilir. Bu nedenle yeniden-deneme gecikmesi asyncio ile yapılır,
maç görevi ve bütün tarama çevrimi ayrı üst sürelerle sınırlandırılır. Maç zaman
aşımı kısmi/sağlıksız kapsama sayılır; çalışan süreç kapanmadan ana döngü sonraki
çevrime geçebilir. Varsayılan sınırlar maç için 90, çevrim için 180 saniyedir.

## Canlı barem tazeliği ve düşük gecikme

20 Eylül 2026 — Canlı maç verisi toplu tarama sonunda değil, her ayrıntı
sayfası tamamlandığı anda işlenir. Canlı paralellik iki sekmeden başlar,
navigasyon hatasında otomatik azalır ve üç sağlıklı çevrimle kademeli yükselir.
Karar/bildirim işi taramadan bağımsız çalışır ve final taramasının
sıralılık kararı değişmez. Odds sayfasında skor
ve saat varsa eksik çeyrek tablosu için ikinci navigasyon yapılmaz.

DOM okuyucusu yalnız kesin tam maç Total Points pazarını ve aynı aktif
bookmaker satırındaki açılış/maç önü/canlı hücrelerini kabul eder. Kilitli,
askıdaki veya belirsiz sayı içeren hücreler kullanılmaz. Yakalama ile karar
arasındaki yaş sınırlıdır. Ayrıca aynı bookmaker baremi sabitken hem skor hem
oyun saati belirlenen eşikleri geçerse gözlem stale sayılır. Bu korumalar
yanlış sinyal yerine eksik sinyali tercih eder. Stale kararı liste filtresinden
sonra uygulanır; engellenmiş maç gereksiz sağlık alarmı üretmez. DB şeması değişmez.

## Scraper, sonuçlandırma ve tarama işi ayrımı

16 Eylül 2026 — Final akışı Scrapling/Patchright üzerinde kalır. Ortak tarayıcı
ayarları `aiscore_browser.py`, final kaynak okuyucusu `aiscore_final_scraper.py`,
skor/maç kimliği doğrulaması `aiscore_match_page.py` içindedir. Canlı overview
okuyucusu da aynı kaynak-final normalleştirmesini kullanır. SQLite yazımı yalnız
`finished_match_service.py` ve mevcut snapshot arşivleme callback'i üzerinden olur.

Gerçek kaynak kontrolünde Cloudflare geçildikten sonra skor tablosunun görünür
olduğu, buna rağmen sayfanın `load` olayının tamamlanmadığı görüldü. Final okuyucusu
Scrapling navigasyonu sürerken aynı sayfanın hazır maç verisini gözler; doğrulanmış
gözlem alındığında bekleyen fetch iptal edilir. Önceki maçı gösteren havuz sayfası
URL eşleşmeden okunmaz. Kaynak state'i varsa maç kimliği eşleşmeli; açık final
durumu ve güvenilir skor gerekir. Çeyrek bitişi tek başına final değildir.

Düğme POST isteği HTTP 202 ile iş kimliğini döndürür; aynı endpoint'e GET tarama
durumunu verir. Saatlik worker aynı iş yöneticisini kullanır. İkinci istek çalışan
işe bağlanır. Sayfa kapanması taramayı durdurmaz; yenilenen sayfa yeniden bağlanır.
İş durumu süreç belleğindedir, servis yeniden başlayınca sıfırlanır. Her doğrulanan
final hemen arşivlenir; diğer maçların veya tüm taramanın bitmesi beklenmez.
Süre bütçesinde önce eski aktif maçlar kontrol edilir. DB migration yoktur.

## Final kontrollerinde süre ve profil kilidi

16 Eylül 2026 — Scrapling sayfa timeout'u Cloudflare çözümünün toplam
süresini sınırlamadığı için final kontrollerinde asyncio süre sınırları
kullanılır. Tek deneme 120, tarama döngüsü 600, tarayıcı açılışı 90 saniyedir.
Kapanış ve yedek driver temizliği ayrı ayrı 15 saniyeyle sınırlıdır.
İptal veya zaman aşımı kilitleri bırakır. Taramada daha önce doğrulanmış
final gözlemleri korunur; kontrol edilemeyen maç bitmiş kabul edilmez.
Aktif/arşiv ve manuel/saatlik kontroller aynı süreçte tek kalıcı final
tarayıcı profilini ortak kilitle kullanır. Yoğun istek yeni tarayıcı açmaz.
Bu değişiklik SQLite şeması veya arşiv snapshot'larını değiştirmez.

## Referans ve dinamik sinyal kuralı

15 Eylül 2026 kullanıcı talimatıyla açılış/sabit eşik kuralı değiştirildi.
Referans, aynı bookmaker satırının geçerli maç önü baremidir; eksik veya
geçersizse açılışa dönülür. `diff = abs(live - reference)` saklanır; canlı
referansın üzerindeyse ALT, altındaysa ÜST üretilir.

Varsayılan eşik `%7 × referans`tır. Q1–Q4 eşik katsayıları ayrı ayarlanabilir;
Q2 ALT ayrıca kendi katsayısını kullanır. Tüm katsayılar varsayılan 1'dir.
`THRESHOLD_MODE=hybrid` seçilirse katsayılardan sonra eşik en az `THRESHOLD`
olur. `%7` yapılandırmada `7` olarak yazılır. Sınır karşılaştırması Decimal
ile yuvarlanmadan yapılır; eşitlik sinyal üretir.

Q4 varsayılan kapalıdır, `DISABLE_Q4_SIGNALS=false` ile açılır. Uzatma ve
belirsiz periyotlar sinyalsizdir. Q4 yasağı ve çeyrek katsayıları yalnız dört
çeyrekli maçlara uygulanır. Maç/çeyrek tekrar korumaları ve liste öncelikleri
aynı kalır. Karar fonksiyonları `live_signals.py`, akış adımları `main.py` içindedir.

Nullable `reference_used`, `reference_total`, `effective_threshold` kolonları
`Database.init()` içinde kilitli, eklemeli migration ile oluşturulur. Yeni
sinyalin referans ve eşiği kayıt sırasında dondurulur. Telegram tekrarları
bu kayıtları kullanır. Dashboard açılış hareketiyle karar farkını ayrı gösterir;
arşivde yeni alanlar da yalnız snapshot'tan okunur.

## Mobil kaynak

Masaüstü AIScore erişimi ağ/koruma katmanında kararsız olduğundan bütün scraper
akışları mobil alana ve Camoufox'a taşındı. Kullanıcıya gösterilen kaynak URL'si
de mobil sürüme dönüştürülebilir.

Final kontrolünde paralel sekmeler proxy üzerinden eksik kapsama ürettiği için
maçlar sıralı kontrol edilir. Mobil sayfa masaüstü sayfaya göre final durumunu ve
iki takım skorunu daha tutarlı verdiğinden masaüstü fallback kullanılmaz.

12 Eylül 2026 itibarıyla final kontrolü de Camoufox yerine Scrapling/Patchright
oturumuyla yapılır. Camoufox bütün final sayfalarında Cloudflare ara sayfasında
kaldığı için manuel buton ve iki otomatik worker aynı anda işlevsizdi. Final
kontrolü canlı tarayıcıdan ayrı kalıcı profil kullanır; doğrulama çerezleri sonraki
kontrollere taşınır ve servislerin profil kilitleri birbirinden ayrılır.

## Canlı tarayıcı motoru

12 Eylül 2026 itibarıyla canlı liste ve odds ayrıntıları Scrapling 0.4.15'in
Patchright tabanlı `AsyncStealthySession` oturumundan okunur. Mevcut proxy ile
Camoufox liste sayfasında Cloudflare doğrulamasında kalırken Scrapling doğrulamayı
geçip canlı listeyi ve odds ayrıntılarını okuyabildiği için bu motor seçilmiştir.
Liste sonrasında aynı browser context korunur; Cloudflare cookie'leri ayrıntı
sayfalarına taşınır. Profil kaynak ağacının dışında yerel cache'te tutulur.

HTTP 200 tek başına başarı sayılmaz. Live sekmesi ile doğrulanmış canlı bağlantı
veya açık sıfır maç durumu bulunmalıdır. Ayrıntılarda mevcut aynı bookmaker ve
canlı çekirdek veri doğrulamaları uygulanmaya devam eder. Sinyal kuralı değişmez.

## Temiz veri tabanı

SQLite şeması yalnız çalışan özelliklerin tablolarını ve alanlarını içerir.
6 Eylül 2026 temizliği çalışan tabloların şemasını değiştirmedi. Kullanılmayan
ek tablo tek işlemde kaldırıldı; sinyaller ve kullanıcı işaretleri korundu.
Yaklaşan maç kayıtları sinyal tablosuna yazılmaz ve bu ekranın temizlik
işlemleri sinyallere dokunmaz.

## Canlı tempo projeksiyonu

Dashboard projeksiyonu yalnız sinyal anındaki toplam skorun oynanan dakikaya
bölünüp normal maç süresiyle çarpılmasıdır. Skor, periyot veya kalan süre
güvenilir ayrıştırılamazsa değer gösterilmez. Bu değer bir karar modeli, adil
barem veya sinyal filtresi değildir.

## Geçmiş görünümü değişmezdir

Canlı dashboard'dan arşivlenen her sinyalin gösterim verisi `display_snapshot`
içinde dondurulur. Geçmiş sinyaller API'si ve sayfası projeksiyon, barem farkı,
liste işareti veya başka bir canlı görünüm alanını yeniden hesaplayamaz. Yalnız
arşivden sonra kalıcı olarak yazılan final skor, final durumu ve sonuç alanları
snapshot üzerine eklenebilir. Snapshot'ında bir alan bulunmayan eski kayıtta
hesap yapmak yerine boş değer gösterilir.

Sonuç bilgisi yalnız otomatik biten maç kontrolü tarafından yazılır. Geçmiş
sinyaller sayfasında sonuç salt okunurdur ve manuel sonuç değiştirme API'si
sunulmaz.

## Kara ve beyaz liste önceliği

Kara takım > beyaz takım istisnası > kara lig sırası uygulanır. Beyaz lig tercih
işaretidir, kara takımı geçersiz kılmaz. Beyaz liste tek başına zorunlu kabul
listesi değildir; normal maçlar aynı sinyal eşikleriyle değerlendirilir.
Ortam yapılandırmasındaki bağımsız BLACKLIST filtresi korunur.

Liste kaydı takım/lig kapsamı ve normalleştirilmiş adla benzersizdir. Taşıma tek
UPSERT işlemidir; kayıt ID'si korunur. Normalleştirme Unicode NFKD, küçük harf,
aksan kaldırma, Türkçe ı dönüşümü ve ardışık boşluk birleştirmedir. Eski SQLite
dosyalarında başlangıç geçişi yinelenen kayıtları birleştirir; eski etkin engeli
korumak için kara kayıt önceliklidir. Geçiş BEGIN IMMEDIATE altında yalnız liste
tablosunu etkiler; mevcut sinyaller ve dondurulmuş arşiv işaretleri değişmez.

## Eksiksiz arşiv gösterimi

PPM hesapları ve tempo/sinyal karşılaştırma etiketleri yalnız canlı dashboard
DTO'sunda hazırlanır. Geçmişte açılış baremi/maç süresi bölmesi, kalan süre
hesabı veya eski alanlardan yeni gösterim alanı türetme yapılmaz. Gösterim
fonksiyonları iki sayfada ortaktır; geçmiş bunlara yalnız snapshot değerlerini verir.
Mevcut sonuç filtreleri ve sayfa özetleri kalıcı sonuçları saymaya devam eder;
sinyal değerlendirmesi yeniden yapılmaz. Final skor toplamı geçmiş tablosunda hesaplanmaz; kalıcı `final_total` alanı
final skorun yanında parantez içinde gösterilir.

## Kullanıcının PPM senaryosu

İşlemlerden açılan PPM hesaplayıcı, kullanıcının açık isteğiyle yapılan geçici
bir senaryo hesabıdır. Arşivin dondurulmuş projeksiyonunu yeniden oluşturmaz
ve eksik snapshot alanlarını tamamlamaz. Kayıtlı sinyal skoru ve kalan süre
ile kullanıcı PPM seçimini kullanır; sonuç hiçbir API veya DB alanına yazılmaz.
Sinyal üretimi ve otomatik sonuçlandırma kuralları değişmez.

Takım geçmişi snapshot'ı arşiv anında yakalanır. Beş görünen satır yalnız gerekli
alanları içerir; başka kayıtların bütün snapshot'larını saklayarak zincirleme
büyüme yaratmaz. JSON snapshot şeması 2'dir; SQLite tablo geçişi veya eski
arşivleri doldurma işlemi yoktur. Arşiv sonrası yalnız kalıcı final/sonuç
gözlemleri güncellenebilir.

Final toplam güncellemesi: `Database.init()` eski dosyalara nullable INTEGER
`final_total` alanını ekler ve mevcut final skorlardan bir kez doldurur. Snapshot
ve sinyal alanları değişmez. Yeni final gözlemlerinde skor ve toplam aynı işlemde
yazılır; skor temizlenirse toplam da temizlenir. Geçmiş API yalnız bu alanı okur.

## Gelecek maçların bağımsız hücum analizi

Bu ek katman yalnız upcoming kayıtlarının JSON payload'ına yazar. Canlı
`alerts`, Telegram, arşiv snapshot'ı, otomatik sonuçlandırma ve bankroll
kurallarıyla bağlantısı yoktur. Yeni tablo/kolon veya eski veriyi doldurma
işlemi yoktur; mevcut SQLite dosyaları doğrudan uyumludur.

Geçmiş havuzu varsayılan olarak son 10 tamamlanmış maçtır (istekte havuz
büyüklüğü belirtilmedi). Takım kimliği ve maç sayfası kimliği eşleşmelidir.
Kaynağın Q1–Q4 ve birleşik uzatma skorları takım final sayısını oluşturur.
Gelecekteki, canlı, iptal veya tekrarlanan maçlar elenir. Kesin saat bulunan
aynı günkü bitmiş maçlar başlangıçtan önceyse kullanılabilir; yalnız tarihi
bilinen aynı gün maçları sırası doğrulanamadığından dışarıda kalır.
En düşük 3 ve en yüksek 2 seçiminde aynı maç tekrarlanmaz. En az 5 maç varsa
hesap yapılır; gerçek örneklem sayısı ekranda gösterilir. Decimal aritmetiği
eşitlikte sahte sinyal oluşmasını engeller. Edge = tahmini toplam - açılış.

Açılış için ilk geçerli bookmaker satırı tercih edilir; maç önü değeri yalnız
aynı satırdan okunur. Açılış yoksa doğrulanmış maç önü gösterilebilir ancak
sinyal üretilmez. Tüm açılış/maç önü gözlemi yeniden denemede birlikte
değiştirilir, farklı şirketlerin değerleri birleştirilmez.
