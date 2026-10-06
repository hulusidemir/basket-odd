# Current State

7 Ekim 2026 — M2'nin sürekli veri yetersiz görünmesi incelendi. Ayrı profilin
doğrudan `page.goto` yolu Scrapling'in Cloudflare çözümünü kullanmıyordu;
M2 artık `session.fetch` üzerinden gezinir ve aynı sekmede kimlik/URL hazır
olunca API'yi okur. Tam yükleme olayının beklemesi veri okumayı engellemez;
fetch/okuma görevleri başarı, timeout ve iptalde temizlenir. Tam 15 saniyelik
oyun saati farkının float hatasıyla reddedilmesi düzeltildi. Süre dolması,
skor/saat/kimlik uyuşmazlığı, erişim hatası ve eksik şut/olay verisi ayrı
gerekçe kodlarıyla gösterilir. Boxscore eksikliği olay kontrolünden önce
raporlanır. Model eşikleri, aynı sinyal skoru koşulu ve 60 saniye okuma
sınırı korunur; yeni veri kaynağı veya takım ortalaması eklenmedi.
M2/okuyucu/dashboard ve M1 runtime/yön regresyonlarında 89 test geçti;
değişen Python dosyalarının compileall ve JS sözdizimi kontrolleri başarılı.
Servis/üretim DB'sine müdahale edilmedi. Canlı kaynak başarısı ve M2'nin
tahmin üstünlüğü bu düzeltmeyle henüz doğrulanmış değildir.

6 Ekim 2026 — Kullanıcı talebiyle dashboard ve arşive bağımsız M2 sütunu,
ayrı gerekçe/veri kalitesi modali eklendi. Yeni gerçek sinyalin bağlamı
kaydedilir; ayrı profil/görev boxscore ve olayları çeker. M1 karar ve Telegram
akışı M2'yi beklemez, M2 hiçbir sinyali veto etmez. Skor/saat/aritmetik/olay
kontrolleri başarısızsa yön yok; yeterli veride bağımsız ALT/ÜST/PAS vardır.
Genel şut öncülleri açık model varsayımlarıdır; takım ortalaması değildir.
Provider güncelleme zamanı henüz yok: veri kalitesi en fazla ORTA.
Tek nullable m2_analysis_json migration; eski kayıtlar NULL, backfill yok.
Arşiv yalnız frozen m2 gösterir. Yerel fixture ile iki sayfada masaüstü/mobil
modal, Escape ve sıralama kontrolü başarılı; M1 regresyonları dahil tam paket
397 test geçti. Servis/üretim DB'si/Telegram'a müdahale edilmedi; canlı M2
çekimi ve tahmin başarısı bu entegrasyonla henüz doğrulanmadı.
Ayrıntılar: docs/MOTOR2.md.

6 Ekim 2026 — Kullanıcının basketbol verisini gerçekten çekmeyi araştırma
talebiyle ayrı geçici profillerde AIScore'a bağlanıldı. Beş canlı maçın üç
NBA maçında detaylı boxscore ve olay akışı; Arjantin/Nikaragua örneklerinde
temel sayı türleri/faul verisi vardı, ayrıntılı boxscore/olay akışı boştu.
Tamamlanmış Japonya ve EuroCup örnekleri de ayrıntılı veriyi verdi. Takımın
son maçları, oyuncu sezon istatistikleri ve lig takım ortalamaları public
API'den alındı; eski bazı endpoint'ler boş dönerken güncel database yolları
çalıştı. Barem sayfası boxscore yüklemiyor; aynı sekmeden `lineups`/`tlive`
yanıtları site protobuf şemasıyla, 30 dakikalık JS cache'i kullanılmadan
çekildi. Üç ardışık canlı çekimde olay sayısı 305/307/308 oldu; skor ile
boxscore her seferinde tam eşzamanlı değildi ve uyuşmayan taraflar reddedildi.
`aiscore_basketball_data.py` yalnız araştırma okuyucusudur; main/motor/DB/
Telegram'a bağlanmadı. Yakalama zamanı provider tazeliği kanıtı değildir.
İlgili 33 test geçti; Python compileall başarılı. Mevcut motor, servis ve geçmiş
sinyaller değişmedi; yalnız yeni okuyucu/test ve dokümantasyon eklendi. Bütün özel
kontrol tarayıcıları kapatıldı. Ayrıntılar:
`docs/AISCORE_BASKETBALL_DATA_RESEARCH_2026-10-06.md`.

5 Ekim 2026 yaklaşık 01:24 Türkiye saati — Kullanıcı bot/dashboard servislerini
01:19:36 / 01:19:35'te yeniden başlattı. Startup implementation hash'i mevcut
çalışma ağacıyla birebir eşleşti. 4,5 dakikalık salt okunur üretim kontrolünde
10 tarama tamamlandı: 9 `ok`, bir `partial`. Son üç çevrim 14,96 / 14,83 /
15,47 saniye; eşzamanlılık 2'den 4'e çıktı. Tek maçın provider source okuması
aksadığı kısmi çevrimde oturum yenilenmedi; sonraki çevrim toparlandı. Ana
döngü, maç işleme, Telegram ve outbox hata sayısı 0; servisler active/running,
otomatik restart 0. 10 geçerli gözlem ile üç maçta sekiz snapshot yazıldı;
sekizinin de kaynak kimliği/barem/skor/saat ve yakalama anındaki sağlayıcı
tazeliği doğrulaması geçti, beş skor/saat ilerlemesi görüldü. Motor logunda
erken maç dört, sayı avantajı yok dört, tempo bandı içinde bir PAS var;
yeni ALT/ÜST adayı veya yeni alert henüz yok. Telegram bot kimlik doğrulaması
ve iki yapılandırılmış hedefin salt okunur erişim kontrolü başarılı; test
mesajı gönderilmedi, bu kontrol sırasında gerçek sinyal teslimi gözlenmedi.
SQLite quick_check ok; ana/arşiv sayfaları, aktif/arşiv API'leri ve mevcut
tarama durumu endpoint'i HTTP 200. Bu kontrol kodu/veriyi/servis süreçlerini
değiştirmedi; önceki 366 testlik düzeltme artık çalışan botta yüklenmiş durumda.

5 Ekim 2026 — Tekrarlayan tarama hatası, gecikme ve eksik bildirim incelemesi:
4 Ekim 20:42 bot başlangıcından sonraki günlükte 10 teslim edilmiş sinyal,
bir Telegram timeout'u ve tazelik süresi dolduğu için iptal edilen retry
görüldü. Maç kaynak/history hataları artık tüm oturumu kapatmaz ve genel
25–40 saniyelik beklemeyi tetiklemez; `partial` kapsama/gerekçe raporu korunur.
Kaynak/history başarısızlığı aynı maçta ikinci navigasyonla hemen denenmez;
bir sonraki çevrimde yeniden okunur. Navigasyon/kapanmış tarayıcı, bütün
maçların timeout olması veya bütçe boyunca sıfır ilerleme oturum yenileme
ve uzun bekleme gerektirir. Vue history render promise'i ham yanıt okumayı
bekletemez. Telegram outbox taramadan bağımsız iki saniyede bir çalışır;
ilk gönderimi devam eden ID retry dışında tutulur. Kapanış worker'ı durdurur
ve scraper'ı kapatır. Kaynak kimliği/barem/skor/saat/tazelik kontrolleri ve
Future Pace kuralları korunur. DB migration ve geçmiş veri değişikliği yok.
Yavaş tarama sırasında ilk timeout sonrası teslimin toparlanması, eşzamanlı
retry koruması, bekleyen Vue promise'i, kaynak hatasından sonra aynı oturumla
devam ve gerçek ağ arızasında oturum yenileme dahil **366 test** geçti;
değişen Python dosyaları/testler compileall ve diff-check geçti.
Geçici profilli, DB/Telegram yazımı olmayan 90 saniyelik canlı kaynak kontrolü
listing yükleme aşamasında timeout oldu; özel kontrol tarayıcısı kapatıldı.
Üretimde hız iyileşmesi/yeni sinyal teslimi bu kodla henüz doğrulanmadı.
Çalışan servis değiştirilmedi; AGENTS.md servis süreçlerine dokunmayı
yasakladığı için yalnız botu yeniden başlatmaya kullanıcı onayı istendi.

4 Ekim 2026 — Kullanıcının doğru barem ve çalışan sinyal akışı talebiyle
kanıtlanmamış Quality 70 yayın/retry barajı kaldırıldı. Logdaki 71 geçen
aday gözlemi (43 maç) bu baraj yüzünden engellenmişti. Kalite v1 formülü
aynı ve yalnız açıklayıcı; Future Pace v5'in tempo, zaman, konservatif sayı
avantajı, tekrar ve kara liste kuralları korunur. ALT/ÜST dışındaki kararlar
filtre gerekçesi boş olsa da yayımlanmaz. Bet365/bs ham kaynak, kimlik,
skor, saat, barem ve tazelik kontrolleri korunur. Eski MIN_SIGNAL_QUALITY
ortam ayarı artık yayını engellemez. Yeni frozen politika bunu açıkça
belirtir; eski politikalar eski eşikleriyle değerlendirilir, geçmiş değişmez.
Yeni DB migration yok. Gerçek motorla 187.5 → 197.5 ikamesinin reddi,
geçerli 187.5'in DB/bildirim adımına aynen ulaşması, yetersiz geçmişte PAS,
düşük puanlı gerçek tahminin değerlendirilmesi ve retry regression'ları dahil
**359 test**, compileall ve diff-check geçti. Yeni tahmin başarısı henüz
kanıtlanmış değildir.
SQLite backup sonrası yalnız bot mevcut servis üzerinden 20:42:03 Türkiye
saatinde yeniden başlatıldı; çalışan kod hash'i ve yeni yayın politikası
startup logunda doğrulandı. İki servis active/running, otomatik restart 0.
Ana/arşiv sayfaları ve API'leri HTTP 200, DB quick_check ok. Backup'taki
bütün alert ve snapshot satırları birebir aynı (değişen/silinen 0).
20:44:29 kontrolünde ilk çevrim 135 saniyede 43 maçın 29'unu tamamladı
(%67,4 kapsama); 14 maç devam kuyruğunda. 3 yeni snapshot'ın 3'ü kaynak
doğrulamasından geçti. İşleme/döngü/Telegram hata sayısı 0; eski ve uyuşmayan
kaynaklar güvenli atlandı. Yeni gerçek sinyal ve Telegram sinyal teslimi
henüz gözlenmedi; ileriye dönük başarı hakkında çıkarım yapılmaz.

4 Ekim 2026 — Veri/model denetimi ve ileriye dönük değerlendirme:
`docs/FORWARD_VALIDATION_2026-10-04.md`. 1.274 eski sinyal yeni bet365 ham
kaynak kanıtı taşımıyor. Quality v1 için maç başına ilk sinyallerle sonraki
dönem AUC 0,503; mevcut kalite eşiğinin üstünlüğü kanıtlanmadı. Yeni eşik
uyarlaması yapılmadı. 20 dakikalık devre saatinin boş kaydı, float kaynaklı
bir saniye eksilme ve karar anından sonra kaydedilen tempo çıpası düzeltildi.
Gerçek yayımlanan tahmin `prediction_context_json` içinde kod/ayar politikasını
dondurur; shadow motor/adayı yok. `forward_validation.py` salt okunur raporda
politikaları ayırır, ilk maç sinyalini sonuçtan önce seçer, bekleyen/iade ve
tutarsız sonuçları ayırır. Tek eklemeli nullable kolon; eski kayıtlar NULL
kalır. Tam test paketi **354 geçti**. Yeni ileriye dönük tahmin sonuçları
henüz yok; başarı/kârlılık doğrulanmış değildir.
19:56 Türkiye saatinde backup sonrası iki mevcut servis yeniden başlatıldı;
yeni implementation hash'i doğrulandı. Eski alert/snapshot değişikliği 0,
ilk çevrimde kaynak doğrulaması geçen yeni snapshot 3/3; loop/işleme hatası 0.
Tam maç kapsaması ilk çevrimde %52, kalanlar sonraki çevrimde öncelikli.
Dashboard sayfaları/API HTTP 200. Yeni gerçek sinyal/sonuç henüz yok.

4 Ekim 2026 — Tekrarlayan canlı tarama timeout'u düzeltildi. İç scheduler,
180 saniyelik hard watchdog'dan önce sekme iptali/oturum temizliği için
45 saniye pay bırakarak sonuçları ve kapsama raporunu döndürür. Tamamlanmayan
maçlar güncel listede hâlâ varsa sonraki çevrimde önce okunur; maç sayısı
limiti de aynı adil sırayı kullanır. Salt kapasite ertelemesi `continuing`
durumudur: oturum korunur ve normal kısa polling kullanılır. Gerçek kaynak
hatası veya sıfır ilerleme `partial/error` kalır; eksik kapsama 100% gösterilmez.
`completed_count`, `deferred_count`, `interrupted_count`, `budget_exhausted`
sağlık loguna eklenir. Kaynak yapısı/kimliği/bet365 satırı/saat/skor/DOM
ön kontrolü history isteğinden önce yapılır; son history doğrulaması yine
zorunludur. Nuxt/Vue kimliği beklenirken önceki sayfanın hazır state'i kabul
edilmez; `2Q` saatleri `Q2` olarak normalleşir. 337 test ve compileall geçti.
Yeni DB migration yok; yön/kalite/sonuç ve arşiv gösterimi değişmedi.
Bot düzeltmeyle 4 Ekim 2026 19:00:36 Türkiye saati yeniden başlatıldı.
19:08 kontrolünde üç ardışık çevrim yaklaşık 135 saniyede sonuçlarını döndürdü:
20/40, 23/50 ve 16/50 maç kontrolü; kalanlar açık `continuing` raporuyla devam
kuyruğuna taşındı. Ana döngü, maç işleme ve Telegram hata logu 0; eşzamanlılık
üçüncü çevrimden sonra 2'den 3'e yükseldi. 17 yeni snapshot'ın tümü yakalama
anındaki kimlik/skor/barem/tazelik kontrolünden geçti. İki servis
active/running, otomatik restart 0, dashboard HTTP 200. Yeni sinyal 0;
Telegram sinyal teslimi bu kontrol sırasında gözlenmedi. Dashboard yeniden
başlatılmadı. Geçersiz kaynaklar reddedilmeye devam eder; her tek çevrimde
tüm canlı listenin okunmuş olduğu iddia edilmez.

4 Ekim 2026 18:47 Türkiye saati — Kullanıcı servisleri 18:40:10'da yeniden
başlattı. Bot/dashboard active/running, otomatik restart 0. Başlangıç logu
Future Pace v5/bet365 doğrulaması/Quality 70 ile yeni kodun yüklendiğini
gösteriyor. Dashboard, arşiv, aktif/arşiv API ve tarama durum API'si HTTP 200;
ham kaynak kanıtı API'de görünmüyor. Saatlik worker'lar başlamış, sonraki
kontroller 19:00:05 ve 19:10:05. SQLite quick_check başarılı.
Canlı bot henüz tam sağlıklı değil: 50/49 maç bulunan ilk iki çevrim
18:43:12 ve 18:46:38'de 180 saniye sınırında iptal edildi; tamamlanmış çevrim
özeti yok. Yeniden başlatma sonrası 21 kaynak kanıtlı snapshot kaydedildi;
ilk kontrol edilen 16'sının yeni DOM kontrolleri, kimlik/skor/barem ve yakalama
anındaki tazelik doğrulaması geçti. Maç işleme/Telegram hata logu 0, yeni
sinyal 0; Telegram sinyal teslimi uçtan uca gözlenmedi. Kaynakta eski,
kilitli ve uyuşmayan history gözlemleri reddediliyor. Bu sağlık kontrolünde
servise/koda/üretim verisine müdahale edilmedi; tarama süresi sorunu açık.

4 Ekim 2026 — Kesinti sonrası devam incelemesinde, boş canlı sütununda
kilitli/çok baremli/eksik DOM hücresinin history üzerinden kabul edilmesi
düzeltildi. Görünür bet365 satırı ve hücresi tek ve erişilebilir olmalıdır;
gerçekten boş hücrede taze history kullanılabilir. History saatinin saniyesi
ve varsa Q öneki doğrulanır; protobuf tekil alan tekrarları reddedilir.
Kanıtsız eski snapshot'lar artık yalnız tempo pencerelerinden değil, yeni
akışın süre formatı ve saat/skor kronolojisi kontrollerinden de dışlanır.
İlk doğrulanmış snapshot eski satırla aynı değerlerde olsa da kaydedilir;
eski kayıtlar korunur. Ham kaynak JSON'u dashboard API'sine/display_snapshot'a
kopyalanmaz, DB'deki kanıt alanında kalır. Bu devamda yeni migration yoktur.
Salt okunur geçmiş kontrolü: 1.274 sinyal, 1.273 sonuçlanmış; kayıtlı final
toplamıyla sonuç/yön aritmetiği ve frozen live/direction uyuşmazlığı 0.
Bu, eski bookmaker kaynağını doğrulamaz. 331 test, compileall ve diff kontrolü
geçti. Bot/dashboard active/running; bu devamdaki düzeltmeler için servisler
yeniden başlatılmadı. Future Pace v5 ve Quality v1 matematiği değişmedi.

4 Ekim 2026 — Canlı barem yalnız AIScore `bs` ana total market'inin bet365
(`company_id=2`) history kaydından alınır. Vue kaynak maç kimliği, market, görünür satır
ve exact match/bookmaker/market URL'sindeki ham history yanıtı birlikte doğrulanır.
Liste canlı sütunu boş olabilir; bu durumda taze history baremi kullanılır.
Listede canlı değer varsa aynı history baremiyle eşleşmesi zorunludur.
History seçimi barem büyüklüğü veya response sırası yerine `updateTime` ile yapılır;
en yeni kayıt kilitli/uyuşmaz/eskiyse eski satıra fallback yoktur. Kaynak periyot
ve skor aynı, oyun saati farkı en fazla 30 saniye, sağlayıcı güncellemesi varsayılan
30 saniyeden yeni olmalıdır. Kaynak okunamazsa sinyal üretilmez. Gönderim ve outbox
tekrarında ham yanıt tekrar çözülür, kimlik/barem/güncelleme doğrulanır.
Yeni yayınlar için `MIN_SIGNAL_QUALITY=70`; puan bir kazanma olasılığı değildir.
Future Pace v5 karar matematiği değişmedi. Kaynak kanıtı olmayan eski snapshot'lar
yeni tempo hesabına katılmaz; eski arşiv/sinyal/barem/sonuçlar yeniden yazılmaz.
`alerts` ve `match_live_snapshots` için nullable `market_provenance_json` kolonları
eklemeli/idempotent migration ile gelir. Sinyal ham history body ve SHA-256'yı,
snapshot yalnız kaynak satırları/en yeni history kaydı ve SHA-256'yı saklar.
Detaylı inceleme ve sınırlar: `docs/LIVE_TOTAL_AUDIT_2026-10-04.md`.

2 Ekim 2026 — Gelecekteki Direction v2 denetimi için yeni ALT/ÜST kayıtlarına
signal-time `reversal_features_version='v1'` ve `reversal_features_json`
eklenir. JSON, opening/prematch/live hareketleri, pregame/current/required PPM,
Fair Total farkı, basit projeksiyon, maç aşaması ve birbirine değen ama
çakışmayan son 2–3 / önceki 3–5 dakika tempo pencerelerini saklar.
Kaynak gözlem ve feature dondurma zamanları ile gözlem yaşı da kaydedilir.
Eksik çıpalar NULL'dır; kapsam FULL, RECENT_ONLY veya
INSUFFICIENT_HISTORY olarak dondurulur. Kaynak snapshot gözlemi 20 dakikadan
eskiyse, sinyal anından sonraysa, saat/skor kronolojisi veya 40/48 dakika
formatı tutmuyorsa pencereye alınmaz. Motor, Quality v1, Telegram, sonuç ve
dashboard ana görünümü değişmez. İki nullable kolon eklemeli/idempotent
migration ile oluşur; eski sinyaller NULL kalır, silme/backfill yoktur.
Mevcut DB üzerinde salt-okunur kapsama: 1.007 sinyalin 421'i FULL,
203'ü RECENT_ONLY, 383'ü INSUFFICIENT_HISTORY. Yeni kayıtların yazılması
için çalışan bot servisinin yeni kodla yeniden başlaması gerekir.

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

Güncel sinyal motoru Future Pace v5'tir: maç önü/açılış PPM öncülüne
yaklaştırılan tempo pencerelerinin bandı, canlı baremin kalan sürede gerektirdiği
PPM ile karşılaştırılır. Bandın altında yeterli sayı avantajı ÜST, üstünde ALT
üretir; bandın içi, eksik veri, erken/geç oyun veya yüksek volatilite PAS'tır.
Canlı baremin açılışa göre artması/azalması tek başına yön belirlemez.
Periyot, liste, tekrar ve minimum kalite kontrolleri ayrıca uygulanır.

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
