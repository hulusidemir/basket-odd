# Decisions

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
