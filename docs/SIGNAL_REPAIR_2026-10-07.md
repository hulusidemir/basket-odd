# Sinyal hatası ölçümü ve Future Pace v6 — 7 Ekim 2026

Kullanıcı sinyallerin neden yanıldığının ölçülmesini, internet kaynaklarının
incelenmesini ve uygulamanın düzeltilmesini istedi. İnceleme salt okunur SQLite
bağlantısıyla (`mode=ro`, `query_only`) yapıldı. Tekil maçlar, ham kaynak
yanıtları, kişisel bilgiler ve hesap bilgileri bu belgeye aktarılmadı.
Servisler ve üretim verisi değiştirilmedi.

## Ölçülen durum

Önce her politika/maçın kronolojik ilk kaydı seçildi; ardından sinyal anındaki
ham bet365 kaynağı ve dondurulmuş politika doğrulandı. Bekleyen sonuçlar
başarı paydasına alınmadı. Aşağıdaki gruplar birbirine katılmadı.

| Politikanın ilk kayıt zamanı (UTC) | İlk maç | Kazanç / kayıp / bekleyen | Model MAE | Piyasa MAE | Maç önü kalan-hız bazı MAE |
| --- | ---: | --- | ---: | ---: | ---: |
| 4 Ekim 17:45 | 10 | 5 / 5 / 0 | 15,86 | 13,90 | 14,27 |
| 4 Ekim 23:35 | 37 | 19 / 18 / 0 | 12,99 | 10,82 | 11,47 |
| 6 Ekim 08:02 | 25 | 9 / 14 / 2 | 11,68 | 9,07 | 8,71 |

MAE = final toplamına ortalama mutlak uzaklık, sayı cinsinden. Maç önü bazı
`sinyal skoru + kalan normal dakika × referans / normal maç süresi` hesabıdır.
Bu, canlı veriyi veya finali model girdisine sızdırmaz. Piyasa karşılaştırması
sinyal anındaki doğrulanmış baremle yapılır; gerçek bahis giriş fiyatı değildir.

Son grupta ALT 8/17, ÜST 1/6. ALT model merkezinin finalden ortalama sapması
−8,37; ÜST'ün +15,73 sayı. Model seçilen ALT örneklerinde aşağı, ÜST
örneklerinde yukarı sapıyor. Bunlar küçük ve yayımlanan sinyallerden seçilmiş
gruplardır; bütün maçların hata dağılımı veya kalıcı lig/yön etkisi değildir.
Bu veriden sabit bir +/− puan düzeltmesi öğrenilmedi.

M2'nin 26 kaydının tamamında `match_identity_mismatch` vardı. Bunun yerine
genel "her maçta veri eksik" açıklaması kullanılamaz. İki geçici kaynak
kontrolünden ilkinde ortamın proxy ayarı yüklenmediği için timeout oldu.
İkinci kontrol `.env` yüklenerek üretimden ayrı geçici profilde yapıldı:
sayfa URL'si, kaynak maç ID'si ve detailMatchId eşleşti; public istatistik
API'si okundu. Bu örnekte tam boxscore yoktu. Kontrol güncel/tamamlanmış
kaynak erişimini gösterir; geçmiş sinyal anında ayrıntılı veri bulunduğunu
veya M2 tahmininin başarılı olduğunu göstermez. İki tarayıcı da kapatıldı.

## Hata yolları ve kaynak incelemesi

1. M1 sayı/dakikayı kalan oyun için sürdürülebilir hız olarak kullanıyor.
   Hücum hacmi ile isabet verimini ayıracak verisi yok. [NBA tempo tanımı](https://www.nba.com/stats/help/glossary)
   hücum sayısı üzerinden yapılır. [Scoring dynamics araştırması](https://arxiv.org/abs/1310.4461)
   sayı olaylarını stokastik süreçle inceler;
   [gamma süreçli canlı NBA modeli](https://www.sciencedirect.com/science/article/pii/S0377221719309233)
   de mevcut skor koşullu tahmin problemi kurar. Son makalenin yalnız erişilen
   özeti kullanıldı; tam metin erişimi 403 verdi. Bu çalışmaların sonuçları
   AIScore/diğer liglere doğrulanmış bahis üstünlüğü olarak aktarılmadı.
2. Aynı kaynak aralığı son iki dakika ile gözlenen periyot bölümüne aynı anda
   girebiliyor. İlk üç politika grubunda sırasıyla 4/10, 11/37 ve 8/25 kayıtta
   tekrar aralık vardı. Sayısal hızların eşitliği tek başına tekrar sayılmadı;
   başlangıç/bitiş skor ve zamanları karşılaştırıldı. Yalnız tekrarları
   kaldırmak bu kayıtların ALT/ÜST yönlerini değiştirmedi; bu kusur geçmiş
   kayıpların tek nedeni olarak sunulamaz.
3. M2 eski yolunda yeni profil ham `goto` ile geziniyor; Scrapling'in challenge
   çözümünü çağırmıyor. Ayrıca yalnız kaynak maç ID'sinin hazır olması kontrol
   edilip okuyucunun istediği diğer kimlik alanları hazır olmadan okunabiliyor.
   Bu mekanizma fixture ile yeniden üretildi. Geçmiş 26 başarısız çekimde hangi
   kimlik alanının başarısız olduğu kaydedilmediğinden hepsinin tek kök nedeni
   kesin belirlenemez.
4. [bet365 genel basketbol kuralları](https://help.bet365.com/s/en/sportsrules/basketball)
   canlı maç bahislerinde uzatmayı içeriyor. Mevcut motor normal süreyle hesap
   yapıyor. Bu genel sayfa belirli AIScore market sözleşmesinin veya kullanıcının
   gerçek bahisinin birebir doğrulaması değildir. Bu çalışmada uydurma uzatma
   olasılığı/sayısı eklenmedi; eksiklik sürüyor.

## Yapılan değişiklik

**M1 Future Pace v6:** Gözlem aralıkları başlangıç/bitiş zaman ve skoruyla tekilleşir.
Eşit sayısal hızı veren farklı aralıklar korunur. Önceden "mevcut çeyrek"
denen veri gerçek başlangıç bilinmiyorsa "gözlenen periyot bölümü" olarak
tanımlanır. Maç önü hızına dönüş senaryosu bantta ayrıca bulunur; gerçek bir
gözlem veya bağımsız doğrulama olarak sayılmaz. ALT/ÜST için sayı avantajı
hem gözlenen hızlarda hem bu senaryoda aynı yönde yeterli olmalıdır.
Merkez tahmin hâlâ gözlenen hız tahminlerinin medyanıdır; bu senaryo olasılık
kalibrasyonu veya öğrenilmiş tahmin güven aralığı değildir.

Kural değişikliği, farklı başlangıç/bitiş aralıkları ve maç önü senaryosu yeni
gerçek sinyalin prediction context'inde dondurulur. `engine=future_pace_v6`
ve `publication=verified_future_pace_v6` önceki v5 politikalarından ayrılır.
Kaynak kontrolleri, kalite formülü, mevcut tekrar/liste kuralları korunur.
Eski kayıtlara v6 etiketi veya yeni tahmin yazılmaz. DB migration yoktur;
mevcut nullable JSON kolonları kullanılır.

V6, saklanan ilk sinyal anları üzerinde yeniden çalıştırıldı. İlk üç grupta
sırasıyla 9/10, 35/37 ve 21/25 sinyal PAS'a döndü; kalanlar 1, 2 ve 4 ALT.
Bu yalnız davranış farkıdır. Karşılaştırma seçilmiş eski sinyallerdedir;
v6'nın yeni seçeceği bütün fırsatlar değildir. Yeni başarı veya kârlılık
iddiası çıkarılmaz. Özellikle gerçek ve kalıcı tempo değişiminde v6 fırsat
kaçırabilir; bu senaryo seçiciliğinin bedelidir.

**M2:** Scrapling'in yönetilen `fetch` yolu ve aynı sekmede eşzamanlı hazır-veri
okuması kullanılır. URL, kaynak ID ve ayrıntı ID hazır olana kadar beklenir;
hazırlık ve okuyucu aynı kimlik fonksiyonunu kullanır. Kimlik reddinde hangi
kontrolün başarısız olduğu da kaydedilir. Okuma/havuz görevleri timeout/iptal
dahil temizlenir. Önceki turdaki 15 saniye float sınırı ve hata etiketi
ayrımı düzeltmeleri korunur. Sinyal skoru/saat koşulları gevşetilmedi ve
eski başarısız M2 kayıtları yeni veriyle doldurulmadı.

**Ölçüm:** `forward_validation.py` model/piyasa/maç önü bazının hatasını,
yönleri, ilk kayıt ve teslim edilmiş alt kümeyi ayrı raporlar. İlk kayıt
iptalse sonraki gönderilmiş kazanan onun yerine seçilmez. M2 durum ve hata
kodları da her politikada sayılır. Sonuçlar otomatik final kaynağı ve rapor
zamanı koşuluyla ölçülür; report hiçbir DB alanını değiştirmez.

**Telegram retry:** Barem farkının işareti ALT/ÜST yönünden çıkarılmaz;
kayıtlı canlı eksi gerçek referans değeri gönderilir. Bu, yeniden gönderimde
yanlış hareket gösterimini düzeltir; başlı başına yön başarısı düzeltmesi değildir.

## Doğrulama ve çalışma sınırı

Gerçek zaman/skor geçmişiyle ALT, ÜST, PAS, sıcak/soğuk sayı serisi ve yetersiz
ayrı aralık davranışını doğrulayan testler eklendi. Herhangi bir geçerli yön
döndürmeyi yeterli sayan 20 senaryo testi kaldırılıp gerçek history kullanan
kesin yön/gerekçe testleriyle değiştirildi. Tek bir aralığın iki adla sayılıp
DB/Telegram'a ulaşması, geç hidrasyon, tahmin-piyasa hatası, ilk iptalden sonra
kazanan seçmeme ve retry işareti için regresyonlar bulunur.
Tam test paketi **406 geçti** (31,08 saniye); değişen Python kaynak/test
dosyalarında compileall, JS sözdizimi ve git diff-check başarılı.

Yeni politika için ileriye dönük sonuç henüz yok. M2 takım/lig şut öncülleri,
kadro/bonus, şut kalitesi, uzatma ve gerçek bahis giriş baremi/fiyatı sorunları
bu değişiklikte tamamlanmış sayılmaz. Çalışan servise uygulama için yeniden
başlatma gerekir; AGENTS.md talimatı nedeniyle kullanıcı açıkça onaylamadan
servis süreçlerine müdahale edilmedi.

## Kullanıcı onayıyla servise uygulama

7 Ekim 2026 yaklaşık 00:47 Türkiye saatinde kullanıcı yeniden başlatmayı açıkça
istedi. Tutarlı SQLite yedeği alındı (0600; bütünlük kontrolü başarılı), mevcut
`basket-bot.service` yeniden başlatıldı. Başlangıç hash'i mevcut kodla eşleşti;
v6 ve yeni publication etiketi doğrulandı. İlk dört tarama `ok`, ana döngü/maç
işleme hatası ve otomatik restart 0; üretim SQLite quick_check ok.

Dört çevrimde doğrulanmış maç motora ulaşmadı. Son taramanın dört bağlantısında
üç `bet365_missing_or_ambiguous`, bir `live_clock_or_score_unverified` vardı.
Bu sıfır sinyal gözlemi v6'nın karar elemesine bağlanamaz. Yeni gerçek sinyal,
M2 analizi, teslim ve ileriye dönük tahmin başarısı henüz doğrulanmadı.

## Son kullanıcı talebi: şirket bağımsız kaynak ve sürekli tahmin (v7)

Kullanıcı bet365 zorunluluğunu kaldırmayı istedi ve bildirim tercihini açıkça
"Tüm tahminler ekranda, avantaj varsa Telegram" olarak seçti. V6'nın maç önü
senaryosunda da aynı avantajı zorunlu tutması kaldırıldı. Bunun doğruluğu
artırdığına dair kanıt bulunmamıştı; daha az sinyal sorunun çözümü sayılmadı.

Uygun bir şirketin ana total satırı, o şirketin uygulama verisi ve ekrandaki
canlı hücresi ile doğrulanır. Kaynak kimliği, tek barem, kilitsiz durum ve
skor/periyot eşleşmesi zorunludur. Dolu canlı hücre history isteğini beklemez.
Tarihçe satırının güncelleme yaşını dolu canlı hücrenin zorunlu heartbeat'i
saymak kaldırıldı. Yeni `aiscore_live_v2` kanıtı kaynak/ekran uyumunu ve yakalama
zamanını gösterir; bağımsız provider güncelleme/tazelik kanıtı değildir.
Canlı sütun gerçekten boşsa aynı şirketin doğrulanmış history'siyle tamamlanır;
bu fallback'in ham yanıt, en yeni kayıt, skor, saat ve sağlayıcı yaşı koşulları
korunur (`aiscore_history_v2`). Uygun olmayan şirketin yerine bir başkası denenir.
Farklı şirketlerin açılış/maç önü/canlı değerleri birleştirilmez. Eski
`bet365_history_v1` kayıtlarının kanıt doğrulaması gevşetilmez.

**Merkez tahmin:** V7 bütün maç skorunu bir kez kullanır; öncül PPM aynı
şirketin maç önü, yoksa açılış bareminden gelir. Mevcut 10 dakika öncül gücü
sonuçlara göre değiştirilmedi:

```text
kalan PPM = (mevcut skor + öncül PPM × öncül dakika) / (oynanan dakika + öncül dakika)
tahmini toplam = mevcut skor + kalan PPM × kalan normal süre
```

Bu, basit bir sabit hız/yaklaştırma tahminidir. Gamma–Poisson modelinde öncül
ve sayım bilgisini birleştirme ile ilişkili matematik için
[Stan'ın resmi posterior predictive açıklaması](https://mc-stan.org/docs/2_24/stan-users-guide/posterior-predictive-simulation-in-stan.html)
incelendi. Basketbolun 1/2/3 sayılık, bağımlı olaylarını bağımsız Poisson puanları
saymanın doğru olduğu iddia edilmez; uygulama bu modelden kazanma yüzdesi veya
kalibre güven aralığı üretmez. Şut/hücum kalitesi ile uzatma sorunları sürer.

Tahmini toplamın altındaki barem ÜST, üstündeki ALT, tam eşiti EŞİT olur.
Gözlem aralıkları açıklayıcı kalır; birden çok pencere oyu veya maç önü
senaryosu vetosu yoktur. Erken/geç ve küçük avantajlı gözlem de tahmin üretir.
Telegram için önceki zaman, minimum sayı avantajı, tekrar ve liste kuralları
korunur. `/forecasts` ekranı yeni dondurulmuş tahminleri gösterir; kullanıcı
başka bir barem girip aynı merkezle geçici karşılaştırma yapabilir.

**Ölçüm:** Önceki üç seçilmiş grupta yeni merkezin ortalama hatası sırasıyla
14,114 / 13,203 / 10,390 sayıydı (10/37/23 sonuçlanmış ilk gözlem).
Eski merkez 15,860 / 12,992 / 11,683, piyasa 13,900 / 10,824 / 9,065 idi.
Yeni merkezin piyasa üstünlüğü yok; yön doğruluğu aynı 5/10, 19/37, 9/23 kaldı.
Bu ölçümden yeni başarı garantisi çıkarılmaz. İki bekleyen maç da sonuçlandıktan
sonraki karar tekrarında son 25 gözlemin tümü yayın avantajını geçti
(18 ALT / 7 ÜST); sonuç 9 kazanç / 16 kayıptı. V6'nın 21/25 PAS elemesi v7'de
yoktur, fakat bu seçilmiş eski kesitte doğruluk düzelmiş değildir.

Bildirim olmayanlar da `match_live_snapshots.forecast_json` içine gözlem anında
dondurulur. Otomatik biten maç kontrolü bu maçları da tarar ve ayrı
`forecast_match_results` tablosuna yalnız doğrulanmış final gözlemi yazar.
Raporun `all_forecasts` bölümü maç/politika başına ilk tahmini sonuçtan önce
seçer; sonraki kazanan ilk kaybın yerine geçmez. As-of kontrolü finalin
daha sonra alınmasını gözetir. Tahminlerin normal süre hesabıyla final toplamın
uzatma içerebilmesi sınırı raporda gözetilmelidir; settlement sözleşmesi doğrulanmış
sayılmaz. Yeni nullable kolon ve final tablosu eklemelidir; eski snapshot,
alert, arşiv ve sonuçlar backfill edilmez; manuel sonuç API'si eklenmedi.

**Doğrulama:** Tam paket **416 test geçti** (34,81 saniye). Python compileall,
JS sözdizimi ve diff-check başarılı. Gerçek Patchright fixture'larında bet365
olmaması/kilitli/eksik hücre ve başka şirketin baremi; dolu hücrede history'siz
doğrulama; boş hücrede aynı şirket history'si kontrol edildi. Erken/geç ve küçük
avantajlı tahmin kaydı, farklı barem karşılaştırması, başka şirketle Telegram
retry, gözlem süresi dolması, eklemeli migration ve sinyalsiz maçın otomatik
final/ilk kayıp raporu test edildi. Sunucu başlatmadan Flask test client'ını
geçici tarayıcıya bağlayan ekran kontrolünde 1440/390px görünümü, senaryo/sıfırlama
ve güvenli metin gösterimi başarılı; tarayıcı hata sayısı 0 ve kapatıldı.

**Servise uygulama:** 7 Ekim yaklaşık 01:04 Türkiye saatinde tutarlı 0600
SQLite yedeğinden sonra mevcut bot/dashboard servisleri yenilendi. V7,
publication ve implementation hash'i doğrulandı; servisler active/running,
otomatik restart 0. Ana sayfa, `/forecasts`, `/api/forecasts` HTTP 200;
eklemeli migration ve SQLite quick_check başarılı. Yaklaşık 01:06'daki ilk
üç çevrimde bir partial, ardından iki ok vardı; üç doğrulanmış gözlem işlendi
ve bir maçta üç v7 tahmini kaydedildi. İlk çevrimdeki iki provider hazırlık
hatası sonraki çevrimde görülmedi. Son iki taramada üç bağlantı uygun bir
şirket baremi olmadığı için atlandı (`bookmaker_odds_unavailable`).
Üretimde bet365 dışı kaynak, yeni alert/Telegram teslimi ve yeni M2 sonucu bu
kısa kontrolde gözlenmedi; bu yolların doğrulaması fixture testleridir.
Yeni `all_forecasts` raporunda bir politika ve dışlanan tahmin 0; final henüz
yoktur. Bu servis doğrulaması tahmin başarısı kanıtı değildir.
