# Decisions

## 8 Ekim 2026 — Olasılık yayın filtresi yok; geçmiş kapsamı genişletme

Kullanıcının tüm uygun sinyalleri gönderme talebi %60 olasılık barajının
yerini alır. Olasılık yön/dağılım hesabında ve yüzde gösteriminde kullanılır,
yayını veto etmez; kaynak/süre/sayı farkı/tekrar/listeler korunur. Eski
ortam değişkeni olasılık filtresini geri açamaz; politika bunu false saklar.

Eski otomatik arşiv finalleri ve kaynak kanıtlı snapshot'lar güncel tabanla
salt okunur yeniden kurulabilir. Farklı eski model toplamları tek bir
modelmiş gibi birleştirilmez; eksik kanıttan veri uydurulmaz ve arşivlere
hesap yazılmaz. Bilinen 215 final gelecek kayıtlara ait hata modeline girer;
115/65 kronolojik yöntem kontrolü ayrı kalır. İki yön kontrolü geçmediği için
yeniden hazırlanmış model doğrulanmış başarı olarak sunulmaz. Lig bazlı
veri/hata incelenir; 74 ligde en büyük uygun grup17 olduğundan ayrı katsayı
eklenmez. Ayrıntı: docs/PROBABILITY_HISTORY_2026-10-08.md.

## 8 Ekim 2026 — OLASILIK başlığı ve yüzde renkleri

Son kullanıcı talebi K.O başlığının yerini alır: sütun OLASILIK olur.
Yüzde kutusu <0,60 pembe/kırmızı, 0,60–<0,70 sarı, 0,70–<0,80 yeşil,
≥0,80 mavi gösterilir. Renk doğrudan olasılıktan gelir; eski puana veya
modelin doğrulama durumuna dayanmaz. Ortak gösterim canlı, arşiv, güncel
tahmin ve tahmin geçmişi için kullanılır; yalnız yüzde/sayı korunur.

## 8 Ekim 2026 — K.O sütununu sadeleştirme

Kullanıcının açık talebiyle olasılık sütun başlığı K.O, hücre içeriği yalnız
yüzde ve sayı olur. Başlık tek satırdır; Tahmin alt etiketi kaldırılır.
Bu görünüm canlı, arşiv ve tahmin tablolarında ortak kullanılır.

## 8 Ekim 2026 — Hesaplanan kazanma yüzdesi ve v10 karar mantığı

Kullanıcı önceki yüzdeyi gizleme/eski puan gösterme kararını reddetti. Bu talep
7 Ekim olasılık gösterim kuralının yerini alır: hesaplanan olasılık ekranda
gösterilir; model doğrulaması ayrı metadata/ölçüm olarak tutulur. Puan yüzdeye
çevrilmez; puan ve bileşenleri arayüzden kaldırılır. V10 yön ve kazanma ihtimali
aynı final dağılımından gelir. Öğrenilmiş final hata ortalaması taban toplamına
eklenir; kalan süre ve eğitim örneklemi hata genişliğinde dikkate alınır.
Şimdiden kazanılmış sayı dağılımın fiziksel alt sınırıdır. Bildirim varsayılan
en az0,60 kazanma ihtimali ve mevcut sayı avantajı koşullarını birlikte arar.

Aktif v9 kayıt için yalnız saklanmış sinyal anı merkez/clock kullanılır.
Arşivde snapshot'a sonradan olasılık eklenmez. Yeni kayıt dağılımını geçmişte
kontrol ederken bugünün modelini çağırmak yerine frozen değerler doğrulanır.
Eski politika ölçümleri korunur. Ayrıntı
docs/SIGNAL_PROBABILITY_V10_2026-10-08.md içinde.

Yükleme kontrolünde AIScore mobilin açılış/maç önü hücrelerini gizlediği
kanıtlandı. Render doğrulama v2'de canlı hücre görünür ve aynı şirketin state
değeriyle eşleşmiş olmak zorunda; açılış/maç önü yalnız görünür olduğunda
karşılaştırılır. Gizli hücre okunmuş/doğrulanmış gibi etiketlenmez. Eksik
hücre veya görünür uyuşmazlık reddedilir. Eski frozen v1 kanıtların kendi
kuralları korunur. İki servis kullanıcının açık yenileme talimatıyla yedek
sonrası yenilendi; yeni v10 kayıt üretim verisinde doğrulandı.

## 7 Ekim 2026 — Kazanma olasılığı ve görünür canlı barem

Kullanıcı sinyal kalitesini doğru ALT/ÜST ve kazanma olasılığı olarak tanımladı.
Oran fiyatı/net getiri kabul ölçütü değildir; uzatma için yeni koşul eklenmez.
Yeni kaynak yakalaması aynı şirketin görünür üç toplam hücresini state ile
karşılaştırır, gizli değerleri dışlar ve ikinci tutarlı yakalamayı kullanır.
Şut/hücum tekrar denemesinde güvenilir tam veri çıkmadığından motora eklenmedi.

Quality v1 kazanma olasılığına çevrilmez. Eski kayıt puan olarak ayrılır;
Tahmini toplam ile ayrı dondurulmuş olasılık gösterilir. Yönün zaman sıralı
kontrolü geçmediyse yüzde yoktur. Mevcut aday ALT/ÜST için kabul false;
bildirim matematiği aynı kaldığından başarı artışı iddiası yoktur. Canlı eğitim,
DB migration veya geçmişi yeni modelle yeniden hesaplama yok. Sayısal yöntem,
kabul koşulları ve kanıt sınırları
docs/SIGNAL_PROBABILITY_REPAIR_2026-10-07.md içinde açıklanır.

## Canlı bilgi kartlarını sadeleştirme ve kartlardan filtreleme

7 Ekim 2026 — Kullanıcının yeni talebiyle canlı sinyal kartlarında başarı
yüzdesi/doğru/yanlış/bekleyen gösterimi ve performans sorgusu kaldırılır.
Kartlar mevcut aktif sinyal sayılarını gösteren klavyeyle erişilebilir
filtre düğmeleridir. ÜST/ALT yön filtresi, Oynanan bahis işaretli kayıtları
seçer; Aktif sinyal/Tümü yön ve oynanan seçimini temizler. Arama ve maç
filtresi korunur, toolbar ile kart seçimi eşzamanlıdır.
Geçmiş işlemlerindeki PPM hesaplayıcı ve ona ait modal/asset yüklemeleri
kaldırılır; canlı senaryo hesabı korunur. Arşiv PPM sütunu/detayı yalnız
snapshot değerlerini göstermeye devam eder; sonuçlar veya kayıtlar değişmez.
Bu talep önceki canlı başarı kartı ve arşiv hesaplayıcı gösterimi kararlarının
yerini alır. DB migration veya servis yeniden başlatma gerekmez.

## M2'yi geçmiş arayüzü ve kullanıcı API'lerinden de kaldırma

7 Ekim 2026 — Kullanıcının yeni talebi önceki eski arşiv M2 gösterimini
koruma kararının yerini alır. Canlı/geçmiş sayfasında M2 sütunu/düğmesi/modalı
ve sıralama girdisi yok; kullanılmayan JS/CSS/modal dosyaları silinir.
Canlı ve frozen arşiv DTO'ları M2 alanlarını API'ye taşımaz. Kalıcı eski
JSON/snapshot değiştirilmez veya silinmez; arşiv alanları yeniden hesaplanmaz,
otomatik sonuçlar korunur. Yeni DB migration veya worker yoktur.

## V9: yakın hızın sürdüğü sıcak rejimde ampirik kalan sayı düzeltmesi

7 Ekim 2026 — Kullanıcı v8 yön başarısının aynı kalmasını kabul etmedi.
Eğitimde yakın hızlı sıcak grubun sapması +7,205, yakın yavaş grubun sapması
-3,561 sayı olduğundan bütün ÜST adaylarına aynı sayı çıkarma uygulanmaz.
Yalnız sıcak/yakın hızlı rejimde, v8'in öncül üzerindeki kalan-hız fazlası
eğitimde öğrenilen tek katsayıyla kalibre edilir. Beş basit aday üç ayrık
eğitim içi zaman bloğunda toplam MAE ile karşılaştırıldı; sıcak-fazla yöntemi
seçildi. 203 uygun eğitim gözleminde katsayı 1,015189277288373. Sonraki test
katsayı seçiminde kullanılmaz; önceden incelenmiş veri ileri test denmez.

Canlı 40/48 dakika, oynanan>=12, kalan>=5,5, öncül gücü10 ve geçerli yakın
hız>=öncül rejiminde uygulanır. Baremden bağımsız tek merkezdir; öncülün
altına küçük kalan-hız düzeltmesi olabilir, mevcut skor çıkarılmaz. Diğer
gözlemler v8 ile devam eder; veri yetersizliği veya yeni yayın eşiği yok.
Model/hash/bayrak dondurulur, canlı model eğitimi veya yeni worker yok.
Eski v7/v8 sonuç ve snapshot'lar değişmez. Yeni şema/migration gerekmez.

Sabit 171 eski ÜST adayında v8 85/171, v9 96/171; bunlar ALT'a dönüşleri de
içerir. Yeterli avantajlı ÜST 164→15, sonuç9/15; kaynak doğrulanan21 adayda
6/21→15/21 fakat avantajlı ÜST bildirimi0. 451 tahmin korunur; toplam
bildirim444→316. Kapsam düşüşü gizlenmez ve 96/171 oranı yalnız ÜST bahis
başarısı diye sunulmaz. Karar güçlü ÜST sayısını zorla korumak yerine
merkezi ölçülen sapmaya göre düzeltir; kullanıcının tüm tahminler görünür,
avantaj varsa Telegram tercihi sürer. Ayrıntı OVER_CALIBRATION_V9_2026-10-07.md.

## V8: sıcak başlangıçta ÜST devamını yakın sayı hızıyla sınırlandırma

7 Ekim 2026 — Kullanıcı başarı ekranı yerine ÜST sinyal matematiğini istedi.
V7 son 2/5 dakikayı hesaplıyor fakat merkezde kullanmıyordu; worker da kayıtlı
tahmini geçmişsiz hesaplıyordu. V8 sıcak bütün-maç hızını, son 5 dakikanın
öncüle yaklaştırılmış hızıyla sınırlar; 5 dakika yoksa 2 dakika, o da yoksa
maç önü hızı kullanılır. Soğuk bütün-maç hesabı aynıdır. Devam hızı öncülün
altına ayrıca itilmez. Aynı baremden bağımsız tek merkez bütün geçerli
gözlemlerde korunur; bildirim eşikleri veya veri yetersizliği vetosu eklenmez.
Sayı/dakika ölçülür; hücum hızı veya kalibre edilmiş kazanma olasılığı denmez.

Yakalama zamanından sonraki/bozuk saatli/geri kayan/düzeltilen skor öncesi
aralıklar kullanılmaz. Güncel gözlem düzeltme teyidi için matematikte aynı
şekilde temsil edilir; DB'ye yazılmadan ve yazıldıktan sonra merkez aynıdır.
Yeni 48 dakika teyidinde eski 40 dakika geçmişi dışlanır. Parametre ve sürüm
politikada dondurulur; eski v7 sonuçları kabul edilmeye devam eder ve yeniden
yazılmaz. Yeni tablo/migration/backfill/worker veya UI değişikliği yoktur.

İncelenmiş kronolojik sonraki kesitte sabit 171 eski ÜST adayında MAE
13,490→13,106; yön isabeti 85/171→85/171. Kalan ÜST bildirimleri 164,
82 doğru/82 yanlış. Kaynak doğrulanan 21 adayda yön 5/21→6/21, kalan
20 ÜST bildirimi 5/20. Sayı tahmin hatası azalırken yön başarısının çözüldüğü
iddia edilmez. Tam ölçüm ve sınırlar OVER_SIGNAL_REPAIR_2026-10-07.md içinde.

## ALT/ÜST başarılarını ayrı izleme ve ÜST düzeltmesinin kapsamını ölçme

7 Ekim 2026 — Kullanıcı ÜST güvenini sorguladı ve ayrı kalibrasyon istedi.
Karma eski motorların ilk sinyallerinde ALT 438/726, ÜST 198/399; ÜST'ü
ters çevirmek %50,4. Eğitimde öğrenilen yaklaşık 9 sayılık ÜST sapması
sonraki testte merkez isabetini 85/171'den 95/171'e çıkardı; 116 aday ALT'a
döndü. Aynı payı ek bildirim avantajı yapmak 17/25'e çıkarırken 171 ÜST'ten
yalnız 25'ini bıraktı; kaynak doğrulanan alt grupta yalnız bir ÜST kaldı.
Bu nedenle canlı kalibrasyon/filtre eklenmedi. Gerçek kaynak alt grubu ve
eski sinyallerin v7 tekrar hesabı gerçek v7 ileri başarı diye sunulmaz.
ALT/ÜST sinyal ve bütün-tahmin başarısı ayrı ekranda gösterilir. Her maçın
ilk kaydı yön/sonuçtan önce seçilir; sonraki kazanan veya karşı yönlü tahmin
ilk kaydın grubunu değiştirmez. Oranlardan bekleyen/iade/geçersiz/yönsüz
çıkarılır; karma motor kapsamı açıkça yazılır. Yeni toplam-sinyal API'si
salt okunur ve dakikalıktır; arşiv/migration/backfill/manuel sonuç yok.
Tekrar çalıştırılabilir directional_audit.py üretim verisini değiştirmez.

## M2'yi canlı akıştan kaldırma

7 Ekim 2026 — Kullanıcı M2'nin incelenmesini, çalışmıyorsa kaldırılmasını
istedi. Başlangıçta 28, son kontrolde 30 üretim denemesinin tamamı başarısızdı.
İlk iki kaynak kontrolünden biri gerçek veride değerlendirme üretti; fakat
aday onarımın gerçek worker/kayıt akışında dört ek denemenin tamamı reddedildi.
Tek başarılı kaynak okuması operasyonel kullanılabilirlik ve tahmin üstünlüğü
kanıtı sayılmadı. Son karar: bot M2 modüllerini import etmez, worker/tarayıcı
başlatmaz, yeni sinyallerde M2 analizi kaydetmez. Canlı sütun/modal kaldırılır;
eski `M2_ENABLED` ayarı artık kullanılmaz. M1 matematiği, Telegram avantaj
kuralları ve tüm tahminlerin başarı takibi korunur. Kolon/ham eski kayıt ve
dondurulmuş arşiv M2 gösterimi tutulur; migration/backfill veya geçmiş silme
yoktur. Eski deneysel modüller çalışan uygulamaya bağlanmaz; tekrar açma
ayarına dönüştürülmez. Aday v2 onarımı üretime alınmadı; eski v1 tanımı
korundu. Ayrıntılar M2_REVIEW_2026-10-07.md içinde.

## Tüm tahminlerin hafif geçmiş ve başarı takibi

7 Ekim 2026 — Kullanıcı bütün tahminler için hafif başarı/geçmiş istedi.
Mevcut frozen snapshot ve otomatik final tabloları yeniden kullanılır;
yeni tahmin tablosu veya worker eklenmez. Her kayıt geçmişte görünür,
headline başarı maç başına ilk kaydedilen tahminden hesaplanır. Bu seçim
sonuçtan önce yapılır ve geçersiz/kaybeden ilk kayıt sonrakiyle değiştirilmez.
Doğru/(doğru+yanlış) paydasından bekleyen, iade, EŞİT ve geçersiz çıkarılır.
UI'da aynı maçın tekrarları başarıyı şişirmez. Bu ekranın ilk-maç özeti ile
CLI'ın ilk-maç/politika raporu ayrı örneklemlerdir. Geçmiş gösterim alanları
context'ten aynen okunur; model tekrar çalıştırılmaz, kullanıcı barem senaryosu
kaydedilmez. Sonuç yazma yetkisi mevcut otomatik final servisinde kalır.
İki eklemeli kısmi indeks, 50 kayıt varsayılan/100 üst sınır ve ID cursor ile
hafif sorgu/sayfalama kullanılır; yenileme dakikada bir. Eski SQLite satırları
ve arşivler doldurulmaz veya yeniden hesaplanmaz.

## Hız dönüşü araştırmasında hedefleri ve kaynak gruplarını ayırma

7 Ekim 2026 — Kullanıcının hızlanan/yavaşlayan maçın devamını değerlendirme
talebi için salt okunur kronolojik araştırma aracı eklendi. İlk sinyal
uygunluk/sonuçtan önce seçilir; eğitim/test aynı maçı paylaşmaz, eğitim
sonucu test başlangıcından önce gözlenmiş olmalıdır. Kısa gelecekteki
sayı hızı, kalan normal süre hızı, %10 değişimin büyüklük sınıfı ve final
ALT/ÜST ayrı ölçülür. Eski kanıtsız kayıtlar araştırmaya katılabilir;
tarihsel kaynak/politika doğrulama alt grubu ayrıca gösterilir, yeni canlı
başarı sayılmaz. Finalde ayrıştırılamayan uzatma normal süre hızı diye
etiketlenmez. Özellik/ceza seçimi eğitim içinde kalır; testte daha iyi
görünen alt gruba bakarak üretim motoru yeniden ayarlanmaz. Ana motor
bu araştırmada değişmedi; ayrıntılar TEMPO_REVERSAL_2026-10-07.md içinde.

## V7: şirket zorunluluğu olmadan bütün tahminler, ayrı bildirim avantajı

7 Ekim 2026 — Kullanıcı bet365 zorunluluğunu reddetti; tüm tahminleri ekranda,
yeterli avantajı olanları Telegram'da istedi. Bu talep v6 seçiciliği ve eski
bet365 zorunluluğu kararlarının yerini alır. Aynı şirketin uygulama ve DOM
canlı baremi uyumluysa tarihçe zorunlu değildir; boş hücre yalnız aynı şirketin
güncel, ham kanıtlı history'siyle tamamlanır. Uygun olmayan şirkette diğer
şirkete geçilir; şirketler arası açılış/canlı birleştirme yapılmaz. Dolu
hücrenin v2 kanıtı yakalama anını/uyumu gösterir, bağımsız provider tazeliğini
kanıtladığı iddia edilmez. Eski v1'in sıkı tarihçe kontrolleri korunur.

V6'nın maç önü senaryosunda da ayrı avantaj istemesi ve farklı tempo aralıklarının
veto oyları kaldırılır. V7 tek merkez kullanır: kalan PPM=(skor + öncül PPM ×
öncül dakika)/(oynanan dakika + öncül dakika). Mevcut 10 dakika öncül gücü
sonuçlara göre değiştirilmedi. ALT/ÜST merkez ile barem karşılaştırmasıdır;
tam eşitlik EŞİT. Her erken/geç/küçük avantajlı geçerli gözlemin tahmini de
dondurulur. Telegram'ın zaman, minimum sayı avantajı, tekrar ve liste
kuralları korunur. Bu sürekli tahmin erişimi veya daha fazla yayın,
başarının arttığı anlamına gelmez: seçilmiş geçmişte yön doğruluğu artmadı.

`match_live_snapshots.forecast_json` nullable eklemeli migration'dır.
`forecast_match_results` yalnız otomatik final taramasından doldurulur;
sinyalsiz tahmin maçları da taranır. İleri rapor hem sinyalleri hem tüm
tahminlerin maç/politika başına ilk kaydını sonuçtan önce seçer. Eski kayıt,
arşiv ve sonuçlar yeniden yazılmaz. Tahmin normal süre içindir; uzatma ve
kalibre edilmiş olasılık sorunu sürer. `/forecasts` ve geçici barem senaryosu
UI'da görünür; senaryo DB tahminini değiştirmez.

## Future Pace v6: farklı kaynak aralıkları ve maç önü senaryosu

7 Ekim 2026 — Kullanıcı sinyal hatalarını ölçüp uygulamayı düzeltmeyi istedi.
Üç ayrı doğrulanmış politika grubunda M1 merkez hatası maç önü kalan-hız
bazından daha büyüktü. Şut/hücum verisi olmadan sıcak/soğuk sayı hızının
kalıcılığı kanıtlanamaz. V6 bandı hem farklı gözlem aralıklarını hem maç önü
hızına dönüş senaryosunu kapsar; senaryo gözlem sayısına eklenmez. Aynı
başlangıç/bitiş zaman ve skor aralığı bir kez sayılır. Model merkezi hâlâ
gözlem medyanıdır; bant istatistiksel güven aralığı değildir. Yeni engine/
publication v6, önceki v5 politikalarından ayrı dondurulur ve raporlanır.
Eski kayıp/kazançlardan eşik araması, yön ters çevirme veya backfill yoktur.
Bu seçicilik gerçek tempo değişimlerinde fırsat kaçırabilir; yeni dönemde
başarı üstünlüğü henüz yoktur. Kaynak/tekrar/liste/kalite ve otomatik sonuç
kuralları korunur. Şema migration'ı yok. M2 ortak kimlik hazırlığına kadar
bekler ve hangi alanın reddedildiğini kaydeder. Forward raporu gerçek
prediction context'ten model/piyasa/baz hatası ve ilk kayıt/teslim alt
kümesini ayırır. Retry hareket işareti kayıtlı live-reference'tır.
Ayrıntılı ölçüm, dış kaynaklar ve sınırlamalar SIGNAL_REPAIR_2026-10-07.md
içindedir. Servisler kullanıcı onayı olmadan yeniden başlatılmadı.

## M2 erişim ve hata ayrımı

7 Ekim 2026 — Ayrı M2 profilinde Cloudflare çözümü için ham `goto` yerine
Scrapling `session.fetch` kullanılır. Kaynak kimliği ve URL hazır olduğunda
aynı sekmedeki public API okuması yükleme olayı beklenmeden yapılabilir;
görevler iptal/timeout dahil temizlenir. Eksik şut verisi, eski sinyal anına
uyuşmayan skor/saat ve erişim hatası tek "veri yetersiz" etiketi altında
toplanmaz. JSON'a isteğe bağlı `reason_code` ve boxscore reddinde `data_issues`
eklenir; şema migration'ı yoktur, eski kayıt/arşivler yeniden yazılmaz.
İzin verilen saat farkı tam saniye karşılaştırılır; 15/30 saniye sınırları
değişmez. Bu, veri erişimi düzeltmesidir; model başarısı kanıtı değildir.

## Dashboard'da bağımsız M2

6 Ekim 2026 — Kullanıcı yeni motorun yönünü mevcut sinyal yanında ve ayrı
veri kalitesi/gerekçe modalinde istedi. Önceki araştırma sınırı bu katman için
kullanıcı talebiyle genişletildi. M2 yalnız yeni gerçek M1 satırlarında ayrı
profil/worker ile değerlendirilir; M1 yönüne bakmaz, M1 yayınını bekletmez veya
veto etmez, Telegram göndermez. Eksik veri ile PAS ayrılır. Sağlayıcı tazeliği
henüz kanıtlanmadığından kalite ORTA ile sınırlı; genel şut öncülleri açık
varsayımlardır. Tek nullable JSON migration, eskiye backfill yok. Arşiv yalnız
snapshot.m2 gösterir, arşivden sonra worker yazamaz. Model ve veri kapıları,
operasyon ve sınırlamalar MOTOR2.md içinde. Servis restart yapılmadı.

## Basketbol verisine gerçek erişim araştırması

6 Ekim 2026 — Kullanıcının ALT/ÜST yönü için ihtiyaç duyulan veriyi fiilen
çekmeyi deneme talebiyle canlı ve tamamlanmış AIScore örnekleri okundu.
Şut/FT denemeleri, ribaund, top kaybı, faul ve olay akışı bazı maçlarda
çekilebiliyor; diğerlerinde eksik. Yeni `aiscore_basketball_data.py` araştırma
okuyucusu mevcut sekmede public API'yi cache'siz okur, skor ve şut aritmetiği
uyuşmazlıklarını reddeder. Yakalama zamanı provider tazeliği sayılmaz.
Bu okuyucu mevcut v5/DB/Telegram'a bağlanmaz; yeni basketbol karar matematiği
bu araştırmayla üretime alınmış değildir. Örnekler ve erişim yöntemi
`AISCORE_BASKETBALL_DATA_RESEARCH_2026-10-06.md` içindedir. Eski sinyaller ve
snapshot'lar korunur; migration yoktur.

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
