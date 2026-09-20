# Current State

20 Eylül 2026 — Canlı bot sağlıklı taramalar arasında tarayıcı oturumunu korur.
Boşalan ayrıntı sekmesi yavaş grup arkadaşını beklemeden sıradaki maçı alır.
Sağlıklı çevrim sonrası bekleme LIVE_POLL_SECONDS (varsayılan 4 saniye), hatalı
çevrimlerde mevcut POLL_INTERVAL_MIN/MAX aralığıdır. Kısmi/hatalı taramada veya
iptalde oturum kapatılır; sağlıklı oturumlar 30 dakikada bir yenilenir.
Maç ve çevrim süre sınırları korunur; DB migration ve sinyal kuralı değişikliği yoktur.

20 Eylül 2026 — Canlı botun navigasyon hatasından sonra saatlerce askıda kalması
düzeltildi. Tarayıcıya bağlı yeniden-deneme beklemeleri asyncio saatine taşındı.
Her maç ayrıntısı varsayılan 90 saniye, bütün canlı tarama çevrimi 180 saniye ile
sınırlı; iptal sırasında sekme kapatma da 5 saniyeyi aşamaz. Zaman aşımına uğrayan
maç sağlıksız kapsama olarak raporlanır ve sonraki çevrim devam eder. SQLite
şeması, sinyal kuralları ve arşiv verileri değişmez.

20 Eylül 2026 — Canlı barem gecikmesi ve stale odds kabulü düzeltildi. Canlı
ayrıntı taraması iki sekmeyle başlar; navigasyon hatasında bire düşer ve üç
sağlıklı çevrimden sonra yapılandırılmış üst sınıra doğru birer artar. Her sonuç, bütün tarama
bitmeden bağımsız bir karar/bildirim görevine aktarılır; Telegram beklemesi diğer
odds sekmelerini durdurmaz. Sinyal için gerekmeyen eksik çeyrek tablosu
artık ikinci overview navigasyonunu tetiklemez; overview yalnız skor veya saat
eksikse açılır. Tam maç Total Points etiketi kesin eşleştirilir; kilitli,
askıdaki ve birden fazla barem içeren hücreler reddedilir. Gözlem yaşı 20
saniyeyi aşarsa sinyal üretmez. Aynı bookmaker baremi en az 90 saniye sabit
kalırken skor en az 10 ve oyun saati en az 2 dakika ilerlerse kaynak stale kabul
edilir. Stale işareti kara/beyaz liste kontrollerinden sonra uygulanır; zaten
engellenen maç scraper sağlığını bozmaz. SQLite migration yoktur; canlı sinyal
ve arşiv alanları değişmez.
Ana akışın pace parametre adları notifier sözleşmesiyle eşlendi; ilk Telegram
denemesi artık imza hatasıyla outbox tekrarına düşmez.

16 Eylül 2026 — Final taraması refaktör edildi. Tarayıcı ayarları, kaynak okuyucusu,
sonuçlandırma ve arka plan iş yönetimi ayrı modüllerde. Scrapling korunuyor.
Gerçek sayfada Cloudflare sonrasında skor görünürken tam sayfa yüklemesinin
beklediği doğrulandı; yeni okuyucu aynı tarayıcı sayfasından hazır final gözlemini
alarak bu beklemeyi keser. Yapılandırılmış kaynak verisi varsa maç kimliği ve açık
final kodu doğrulanır; yoksa mevcut skor tablosu okuyucusu kullanılır.

Canlı dashboard taramayı POST ile başlatır, ilerlemeyi GET ile izler. Saatlik
worker ve düğme aynı işi paylaşır; sayfa yenilenince çalışan işe bağlanılır.
Kontrol edilen/arşivlenen/okunamayan sayıları ayrı durum alanında görünür.
Doğrulanan finaller tarama sürerken anında snapshot ile arşivlenir ve sonuçlanır.
Eski aktif maçlar önce taranır. Diğer maçta kaynak hatası veya zaman aşımı önceki
başarılı arşivlemeleri engellemez. SQLite migration yoktur.
Gerçek tek seferlik kontrolde 37/37 maç 114 saniyede doğrulandı; 34 bitmiş maç
snapshot ile arşivlendi ve 48 sinyal otomatik sonuçlandırıldı. Kaynak/arşiv hatası
olmadı. Kontrolün tarayıcısı kapatıldı. Kalıcı dashboard sürecinin yeni iş API'sini
ve worker'ı kullanması için servis yeniden başlatılmalıdır.

16 Eylül 2026 — Biten maç taramasının süresiz bekleme hatası düzeltildi.
Servis günlüğünde 09:00 aktif taramasının tamamlanmadığı, sonraki saatlik
aktif kontrollerin başlamadığı görüldü. Scrapling'in Cloudflare beklemeleri
sayfa timeout'u ile sınırlanmıyordu; aktif tarama kilidi bırakılmıyordu.
Artık her maç denemesi 120 saniye, tarama döngüsü 600 saniye ile sınırlı.
Süre dolmadan doğrulanan finaller arşivlenip sonuçlandırılır; kalan maçlar
aktif kalır ve eksik kapsama bildirilir. Tarayıcı açılışı 90 saniye,
kapanış ve gerektiğinde driver temizliği ayrı ayrı 15 saniyeyle sınırlıdır.
Manuel/otomatik aktif ve arşiv final kontrolleri aynı profil kilidini paylaşır.
Yoğunluk 409, tamamen başarısız okuma 502, açılış zaman aşımı 504 döner;
arşivleme hatası ve kısmi başarısızlık da dashboard'da açık gösterilir.
SQLite migration yoktur; snapshot ve otomatik sonuçlandırma kuralları korunur.
Çalışan eski sürecin takılı taramasını kaldırmak ve düzeltmeyi yüklemek için
dashboard servisinin yeniden başlatılması gerekir.

15 Eylül 2026 — Canlı sinyal motoru geçerli maç önü baremini, yoksa açılışı
referans alır. Maç önü artık seçilen canlı bookmaker satırından okunur;
şirketler arası medyan kullanılmaz. Varsayılan eşik referansın %7'sidir.
`THRESHOLD_MODE=hybrid` seçilirse `THRESHOLD` minimum puan farkıdır.
Q1–Q4 katsayıları ve yalnız Q2 ALT için ek katsayı ayarlanabilir (varsayılan 1).
Dört çeyrekli maçlarda Q4 varsayılan kapalıdır; `DISABLE_Q4_SIGNALS=false`
ile açılır. Uzatma ve bilinmeyen periyotlar kapalı kalır. İki devreli maçlarda
çeyrek katsayıları kullanılmaz. `main.py` akışı küçük fonksiyonlara ayrıldı;
karar matematiği `live_signals.py` içindedir.
`Database.init()` nullable `reference_used`, `reference_total`,
`effective_threshold` kolonlarını güvenli ve tekrarlanabilir şekilde ekler.
Yeni sinyalin `diff` değeri referansa göre mutlak farktır. Referans ve eşik
Telegram tekrarlarında kayıtlı haliyle kullanılır; dashboard modalında ayrıca
gösterilir ve arşiv snapshot'ında dondurulur. Geçiş mevcut kayıtları silmez.

12 Eylül 2026 — Canlı liste okunamadığında boş sonuç artık sağlıklı çevrim
sayılmaz. Kaynak canlı maç bildiriyor ancak bağlantılar doğrulanamıyorsa veya
boş liste açıkça doğrulanmamışsa üç liste denemesinden sonra hata bildirilir;
ana döngünün mevcut ardışık hata takibi devrede kalır. Kaynağın doğruladığı
sıfır maç durumu normal boş sonuçtur. SQLite şeması değişmez.

Canlı liste ve odds ayrıntıları aynı Scrapling `AsyncStealthySession` içinde
okunur. Cloudflare çözümü, WebRTC koruması ve mevcut proxy etkindir; doğrulama
için en az 90 saniye ayrılır. Liste alındıktan sonra context yenilenmediği için
Cloudflare oturumu ayrıntı sayfalarına taşınır. "Just a moment" sayfası veya
doğrulanmamış boş liste sağlıklı çevrim sayılmaz.

Final tarayıcısı da Cloudflare engelini aşmak için Scrapling/Patchright kullanır.
Canlı tarayıcıdan ayrı kalıcı profil taşıdığı için iki systemd servisi aynı anda
çalışırken Chromium profil kilidi oluşmaz. Manuel "Bitenleri kontrol et" işlemi
ve saat başındaki worker aynı düzeltilmiş oturumu kullanır. Gelecek maç tarayıcısı
Camoufox kullanmaya devam eder. SQLite şeması ve sinyal kuralları değişmez.

Uygulama mobil `m.aiscore.com` kaynağını kullanır. Canlı ve final kontrolleri
Scrapling/Patchright, yaklaşan maç akışı Camoufox üzerinden çalışır. Masaüstü AIScore alanı
mevcut ağda güvenilir olmadığı için bütün akışlar mobil kaynağa yönlendirilmiştir.

Dashboard'daki manuel biten maç kontrolü ile saat başındaki otomatik kontrol
aynı servis fonksiyonunu kullanır. Final sayfaları proxy kararlılığı için sırayla
açılır; geçici hata bir kez yeniden denenir. Özet ve loglar gerçek doğrulama
kapsamasını, yeniden deneme sayısını ve ulaşılamayan maçları ayrı ayrı bildirir.
Ulaşılamayan maç aktif bırakılır ve sonraki çevrimde yeniden kontrol edilir.
Aynı anda ikinci bir aktif maç taraması başlatılmaz.

Sinyal motoru canlı toplam ile maç önü/açılış referansı arasındaki mutlak farkı
dinamik eşikle karşılaştırır. Canlı barem referanstan yüksekse `ALT`, düşükse
`ÜST` yönü oluşur; periyot, liste ve tekrar kontrolleri ayrıca uygulanır.

Dashboard yalnız kaynak gerçeklerini, ham barem değişimini, sinyal yönünü,
kullanıcı işaretlerini ve final sonucunu gösterir. Canlı tabloda ayrıca sinyal
anındaki skor ve oynanan süreden, mevcut hızın normal maç süresince değişmeden
sürdüğü varsayımıyla basit tempo projeksiyonu hesaplanır; bu bir tahmin modeli
veya adil barem değildir. Yaklaşan maçlar program ve açılış/maç önü baremlerine ek olarak bağımsız
hücum ortalaması analizini gösterir. Bu analiz canlı sinyal motoruna katılmaz.

Geçmiş sinyal görünümü canlı satır arşivlenirken kaydedilen `display_snapshot`
verisini kullanır. Projeksiyon dahil canlı görünüm alanları geçmiş okunurken
yeniden hesaplanmaz; eski bir kaydın snapshot'ında alan yoksa boş gösterilir.
Arşiv sonrası eklenen final skor ve sonuç yalnız kalıcı kayıttan okunur.
Arşiv özeti ALT/ÜST yönlerinin toplam, başarılı ve başarısız sinyal sayılarını
ve tüm/benzersiz sinyallerin başarı yüzdelerini gösterir; sonuç, yön ve Q1-Q4
çeyrek filtreleri birlikte kullanılabilir.
Geçmiş tablosunda maç adının solundaki düğme yalnız o maçın sinyallerini
gösterir; aynı maçtaki tekrar sinyalleri `2. sinyal`, `3. sinyal` biçiminde
etiketlenir.

Yerel temizlik tamamlandı: kullanılmayan model tablosu, eski veritabanı
yedekleri, dışa aktarım ve kapalı eski log kaldırıldı. Yedi çalışan tablonun
içeriği ve mevcut 318 sinyal temizlik sırasında değişmedi. Kullanılmayan
manuel sonuç yazma metodu ve ayrı snapshot yazma yolu kaldırıldı.
Yaklaşan maç işlemleri yalnız kendi tablolarını kullanır.

Gelecek Maçlar ekranı eksik baremleri sıfır yerine boş gösterir; barem sayacı
açılış veya maç önü baremi bulunan kayıtları sayar. Çekim hataları, kısmi
güncellemeler ve eski veri bilgisi ekranda görünür. Takip/silme/temizleme
işlemlerinde hata gösterilir ve yinelenen tıklamalar engellenir. Gecikmiş liste
yanıtları yeni ekran verisini ezmez. Liste sorgusu takılı kalan çekimi zaman
aşımına düşürür; eski çekimin veritabanına yazması iş kimliği ve kilit ile
engellenir. Bu düzeltmeler SQLite şemasını veya canlı sinyal kurallarını değiştirmez.

Canlı sinyal modalının en üstündeki PPM bölümü, sinyal anındaki bareme kalan
sayıyı normal sürede kalan dakikaya böler ve maçın o ana kadarki ortalama
PPM'iyle karşılaştırır. Kısa yorum yalnız tempo karşılaştırmasıdır; sinyal
kuralını değiştirmez. Bu alan arşivlenirken snapshot'a alınır. Eski snapshot'ta
bulunmuyorsa geçmiş okunurken hesaplanmaz.

Gereken PPM yanında, mevcut tempo baz alınarak yüzde farkı gösterilir:
`(gereken / mevcut - 1) × 100`. Hesap yuvarlanmamış değerlerden yapılır.
Pozitif yüzde gereken hızlanmayı belirtir. Mevcut tempo sıfırsa veya kalan
süre yoksa yüzde gösterilmez. Yüzde de karşılaştırmayla birlikte arşivlenir.

Sinyal modalı kompakt genişlikte açılır. PPM mavi/mor vurgularla, maç özeti
tek şeritte gösterilir. Çeyrek skorları ve çeyrek temposu modalda gösterilmez. Tempo ile sinyal yönü ters olduğunda açık bir etiket
gösterilir. Takım geçmişinin ayrıntıları açılır bölümdedir.

Çeyrek skorları yalnız maçın skor tablosundaki sayısal hücrelerden okunur.
Ortak okuyucu hem çeyrek sütunlarını hem toplamı başta/sonda olan takım
satırlarını destekler. Takım isimleri ve sayfanın diğer sayıları kullanılmaz.
Çeyrek toplamları sinyalin toplam skoruyla tam eşleşmelidir. Çeyrek bilgisi
için açılan ikinci sayfa, mevcut sinyal skorunu veya zamanını değiştirmez.
Önceden kaydedilmiş eksik çeyrek verileri sonradan yeniden oluşturulmaz.

Ana ekrandaki açılır takım/lig bölümünde kara ve beyaz liste kayıtları, türleri ve
sayıları görünür. Arama, tür filtresi, ekleme ve çıkarma desteklenir. Modalda iki
takım ve lig için kara/beyaz düğmeleri vardır; seçili düğme kaydı çıkarır, diğer
düğme kaydı taşır. İşlem hatası ilgili bölümde gösterilir. Canlı PPM sütunu da
modalın backend karşılaştırmasını `mevcut → gereken (% fark)` biçiminde kullanır.
Yüzde farkı renk kodludur: pozitif (hızlanma gerekli) amber, negatif (tempo
yeterli) yeşil, sıfır nötr gri. Tempo sinyal yönüyle uyumluysa yüzde farkının
yanında yeşil ✓ işareti görünür. Renk ve ✓ `ppmChangeTone` ve `ppmFlow`
fonksiyonlarında belirlenir; CSS sınıfları `ppm-change-positive`,
`ppm-change-negative`, `ppm-change-neutral` ve `ppm-aligned-tick` ile uygulanır.

Kara takım her zaman engellenir. Beyaz takım kara lig için istisnadır; beyaz lig
tercih işaretidir. Listede olmayan maçlar normal kurallara tabidir. Aynı eşleşme
hem sinyal üretiminde hem bekleyen Telegram kontrolünde kullanılır. Bir takım/lig
aynı anda iki listede bulunamaz; Türkçe harf ve boşluk farkları tek ada eşlenir.

Arşiv snapshot sürümü 2, açılış PPM'ini, mevcut/gereken PPM'i, yüzde farkını,
kalan sayı/süreyi, tempo yorumunu, sinyalle uyum etiketini ve sinyal saati yazısını
canlı DTO'dan kopyalar. Açılış PPM'i artık tarayıcıda bölme yapılarak üretilmez.
Takım geçmişinin görünen beş satırı ve özeti arşiv öncesi kaydedilir; iç içe
snapshot'lar kopyalanmaz. Bütün arşivleme yolları aynı yakalama işlevini kullanır.

Geçmiş tablosunda açılış PPM'i, mevcut → gereken PPM (% fark), liste işaretleri,
takip/oynandı/gözardı durumları ve sinyal saati gösterilir. Yön veya PPM düğmesi
yalnız snapshot verisiyle modal açar. Ortak signal_display.js hesap yapmaz;
kayıtlı sayıları biçimlendirir. Arşiv takım geçmişi endpoint'i de snapshot okur.
Eksik yeni alanlar eski kayıtlarda boş kalır; eski snapshot'lar güncellenmez.
Geçmiş tablosunda arşivlenme tarihi sütunu kaldırılmıştır. Projeksiyon sütunu
canlı dashboard ile aynı stili kullanır (kenarlıksız, transparan, büyük sayı).
Geçmiş sayfasındaki sonuç kontrol ve arşiv temizleme butonları hata durumunda
kullanıcıya bilgi verir ve buton kontrol sırasında devre dışı kalır.

Geçmiş Final sütunu `80 - 75 (155)` biçimindedir. Toplam `final_total` alanından
okunur; sonuç yazılırken hazırlanır. Eski final skorlar için toplam, başlangıç
şema geçişinde bir kez saklanır; geçmiş sayfası toplam hesaplamaz.

Canlı ve arşiv tablolarının işlemler alanında PPM hesaplayıcı bulunur. Modal,
sinyal anındaki mevcut PPM ile açılır; sürgü veya elle yazılabilen PPM alanı,
kalan sürede iki takımın toplam sayı/dakikasını değiştirir. Alan virgül ve nokta
ile en fazla iki ondalık kabul eder; sürgüyle eş zamanlı güncellenir.
Senaryo toplamı `sinyal skoru + seçilen PPM × kalan
dakika` olarak hesaplanır ve sinyal baremiyle karşılaştırılır. Yalnız normal
süreyi kapsar. Skor, kalan süre veya mevcut PPM eksikse hesaplama sunulmaz.
Arşivde yalnız kayıtlı snapshot girdileri kullanılır; senaryo geçicidir,
arşiv projeksiyonuna, final sonucuna veya sinyal kararına yazılmaz.

8 Eylül 2026 — Gelecek Maçlar ek analizi: AiScore mobil H2H sayfasının
`h2hDatas_home` / `h2hDatas_away` verileri kullanılır; iki takımın birbirleriyle
oynadığı H2H listesi kullanılmaz. Son 10 doğrulanmış tamamlanmış maç içinde
atılan sayıya göre en düşük 3 ve en yüksek 2 farklı maç seçilir; toplam 5'e
bölünür. İki hücum ortalamasının toplamı açılış baremiyle karşılaştırılır.
Negatif Edge ALT, pozitif Edge ÜST, sıfır Edge sinyalsiz eşitliktir.
Her takımda en az 5 geçerli maç gerekir. Eksik açılış ve kaynak hatası ayrı
gösterilir; maç önü baremi açılış yerine kullanılmaz.

Açılış okuyucusu yalnız `.oddsContent` altında doğrulanmış `Total Points`
piyasasının `.border1` açılış ve aynı bookmaker'ın `.border2` maç önü
hücrelerini kullanır. Metindeki ilk büyük sayı/satır sırası yedekleri kaldırıldı.
Yeniden deneme piyasa değerlerinin tamamını beraber değiştirir.

Analiz, seçilen maçlar ve hesap zamanı `upcoming_matches.payload_json` içindeki
`upcoming_analysis` alanına kaydedilir. SQLite migration yoktur. Eski kayıtlar
yeniden çekilene kadar analiz göstermez; önceki hatalı açılışlar yeni çekimle
yenilenir. Kaynak geçici hata verirse program/barem satırı korunur ve analiz
durumu gösterilir. History işi 30 saniyeyle sınırlıdır; ana çekim bütçesine dahil
edilmiştir. Yeni tarayıcı kontrolleri kısa ömürlüdür; çalışan servisler yeniden
başlatılmadı.
