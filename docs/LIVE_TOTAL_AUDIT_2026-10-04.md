# Canlı barem incelemesi — 4 Ekim 2026

## Kanıt ve kapsam

İlk inceleme salt okunur yapıldı. Ardından kullanıcı düzeltme ve servislerin
yeniden başlatılmasını istedi. Kaynak kod değişiklikleri bu ikinci talep kapsamında
hazırlandı. Geçmiş barem, sinyal, arşiv ve sonuç değerleri düzeltilmedi/silinmedi;
backfill yapılmadı. Sayılar DB'nin 1.274 alert / 62.942 snapshot içeren inceleme
anına aittir; son alert zamanı 2026-10-03 21:07:54 UTC.

## Manresa — Breogan

- Match ID: `m2q19srype9aek6`.
- Alert: `5068`, 2026-10-03 19:33:22 UTC / 22:33:22 Türkiye saati.
- Durum/skor: Q2 07:14, 30–24.
- Opening/reference: 182.5; prematch boş; stored live: 197.5.
- ALT, Quality 92, projection yaklaşık 169.2, Fair Total 165.7.
- Kaynak DOM yakalama: 19:33:22.882229 UTC; karar anındaki gözlem yaşı
  yaklaşık 0.032 saniye. Bu, sağlayıcının odds güncelleme zamanı değildir.

197.5 DB snapshot'larında Q1 sonunda 27–24 (19:26:01 UTC), Q2 09:05
27–24 (19:29:28), sinyalde 30–24 ve Q2 06:10 32–28 (19:36:21) olarak bulunur.
Snapshot'lar aynı parser'dan geldiği için bağımsız provider kanıtı değildir.
Sonradan scraper logunda 197.5'in stale sınıflandırması vardır; bu sınıflandırma
tek başına sinyal anındaki 197.5'in hangi bookmaker/market'ten geldiğini kanıtlamaz.

Kullanıcının aktardığı bet365 history: Q2 07:12 → 183.5, 07:20 → 184.5,
07:28/07:38 → 185.5, 07:42 → 186.5. Sonraki mesajda 187.5 belirtilmiştir.
Bu iki aktarım farklı anlara ait olabilir; ham zaman damgalı yanıt olmadan
tek bir sinyal anı baremi şeklinde birleştirilmedi.

**ROOT CAUSE: NOT PROVEN.** Sinyal anının ham odds/history yanıtı kalıcı olarak
saklanmamış ve browser cache içinde bulunamamıştır. 197.5'in bet365, başka şirket,
alternate market veya daha eski provider güncellemesinden geldiği kanıtlanamaz.
Raw response'da 197.5 var/yok hükmü verilemez: ilgili yanıt erişilebilir değildir.
Kod akışında stored live, parser çıktısından gelir; normalizasyon bir ondalığa
yuvarlar. 187.5'e 10 ekleyen bir hesaplama bulunmadı.

**Kanıtlanan seçim kusuru:** önceki canlı DOM okuyucusu şirket kimliğini
doğrulamadan ilk açılış/canlı eşleşen satırı seçiyordu. Company sırası değişirse
başka bookmaker seçilebiliyordu. Logo `alt="#"` gerçek şirket adını sağlamıyordu.
Main/alternate kimliği ve provider `updateTime` saklanmıyordu. Bu kusur,
197.5 olayının kesin kök nedeni olarak sunulamaz.

## Tüm geçmişin veri denetimi

- 1.274/1.274 alert live değeri, sinyal zamanından sonraya geçmeyen en son DB
  snapshot live değeriyle aynı. Bağımsız provider doğrulaması değildir.
- En son snapshot'ın skor/saat metni 757 alert'te tam eşleşir. Diğerlerinde
  kayıt anı/saat veya periyot sonu formatı tam aynı değildir.
- Önceki bağımsız cache incelemesinde 5 alert için sinyalin 20 saniye çevresinde
  raw odds-list yanıtı vardı: 4943, 4944, 4974, 5036, 5051. Stored live bu
  yanıtların satırlarında bulunuyordu; bu durum bet365 ana satırın seçildiğini
  tüm geçmiş için kanıtlamaz. Manresa yanıtı yoktu.
- URL/match ID uyuşmazlığı ve frozen display live uyuşmazlığı ilk kapsamlı
  incelemede bulunmadı. Kaynak market/bookmaker kimliği geçmişte saklanmadığı
  için provider düzeyindeki mismatch/alternate seçimi sayılamaz.
- **38 stale adayı:** sinyalden önce ardışık snapshot'larda aynı live en az
  90 saniye sabit, oyun süresi en az 120 saniye, skor en az 10 sayı ilerlemiş.
  Bunlar provider timestamp olmadan kesin yanlış barem sayılmaz.

Stale aday alert ID'leri:

3822, 3823, 3824, 3825, 3826, 3827, 3828, 3829, 3830, 3833, 3838, 3840,
3841, 3842, 3843, 3844, 3847, 3849, 3851, 3852, 3853, 3855, 3856, 3857,
3858, 3859, 3861, 3864, 3867, 3868, 3869, 3873, 3874, 3878, 3879, 3880,
3913, 3915.

İlk incelemede ayrıca büyük line sıçraması nedeniyle işaretlenen 5 aday:
3839 (164.5), 4253 (171.5), 4310 (182.5), 4906 (161.5), 5051 (146.5).
Bu hareketler bookmaker geçişi veya hata olarak kesin sınıflandırılamadı.

**AFFECTED SIGNAL COUNT:** kesin kaynak uyuşmazlığı kanıtlanan toplam bilinmiyor.
Kullanıcının bildirdiği Manresa olayı 1; ek 38 stale ve 5 line sıçrama adayı vardır.
Bu sayılar birbirine eklenerek kanıtlanmış hata sayısı şeklinde sunulamaz.

## Geçmiş sonuçlar

1.269 sonuçlanmış sinyal: 717 başarılı, 552 başarısız (%56.5 başarılı).
ALT: 496/833 (%59.5); ÜST: 221/436 (%50.7). Final skor ile yön/line sonucunun
aritmetik kontrolünde uyuşmazlık bulunmadı. Doğru sonuçlandırma, doğru odds
kaynağı veya gerçek bookmaker settlement sözleşmesi kanıtı değildir.

| Quality | Toplam | Başarılı | Başarısız |
|---|---:|---:|---:|
| Eski/boş | 459 | 247 | 212 |
| PAS | 390 | 222 | 166 |
| DÜŞÜK | 194 | 101 | 93 |
| ORTA | 160 | 103 | 56 |
| YÜKSEK | 62 | 38 | 23 |
| ÇOK YÜKSEK | 9 | 6 | 2 |

Son günler (UTC): 3 Ekim 84 başarılı / 54 başarısız / 4 bekleyen;
2 Ekim 67/55; 1 Ekim 30/28. Kaynak doğrulaması olmayan geçmiş veriden
yeni yön matematiğinin başarısını veya kalibre edilmiş bir kazanma olasılığını
iddia etmek mümkün değildir. Kalite eşiği bir geçmişe uyarlama optimizasyonu değildir.

## Uygulanan koruma

1. Canlı kaynak yalnız company ID 2 (bet365), full-game `bs` total market.
   Başka şirket veya başka satıra fallback yok.
2. Maç URL ID, Vue maç ID, component maç ID ve history request URL birlikte
   doğrulanır. Canlı barem history'den gelir; listede canlı değer varsa görünür
   bet365 hücresinde tek aynı değer olmalıdır. Canlı sütunu boş olabilir; başka
   şirketten doldurulmaz. Açılış/maç önü aynı bet365 satırından alınır.
3. Aynı bookmaker'ın en yeni history kaydı `updateTime` ile seçilir. Kilitli,
   farklı barem/periyot/skor, 30 saniyeden fazla saat farkı veya varsayılan
   30 saniyeden eski provider güncellemesi kabul edilmez. Daha eski kilitsiz
   history satırı seçilmez.
4. Yeni alert ham history body'sini, SHA-256'yı, şirket/market/maç kimliğini,
   kaynak satırlarını ve en yeni history kaydını saklar. Snapshot kaynak ve
   hash saklar; body tekrarını saklamaz. Eklemeli nullable kolonlar kullanılır.
5. Karar ve Telegram gönderiminden hemen önce ham yanıt yeniden çözülür.
   Eski/kanıtsız veya kalite eşiği altındaki bekleyen Telegram gönderimi iptal
   edilir; tarihsel alert live/result değeri değiştirilmez.
6. Kaynak kanıtı olmayan eski snapshot'lar yeni tempo hesabında kullanılmaz.
   Yayın eşiği varsayılan Quality 70. Future Pace v5 yön matematiği korunur.

## Doğrulama sınırı

316 test geçti; native Vue/DOM şirket sırası, duplicate/alternate hücre,
187.5/197.5 uyuşmazlığı, ham protobuf, eski/yanlış kimlikli veri, yayın sınırı,
outbox ve migration kontrolleri dahil. Python compile ve diff whitespace
kontrolü geçti. Test history body'leri sentetiktir; AIScore client şeması cache'deki
gerçek frontend kodundan doğrulanmıştır.

İlk izole canlı browser denemelerinde Cloudflare/bağlantı hataları görüldü.
Servis çalıştırıldığında gerçek bet365 history yanıtları alınıp çözüldü;
cache'deki ham yanıtların şirket ID'si, market URL'si ve en yeni kayıtları kontrol
edildi. İnceleme anında 7 maçın 6'sında odds-list bet365 `s` sütunu boştu,
birinde bet365 total satırı yoktu. History, listeden ayrı olarak zaman damgalı
live kayıtları içeriyordu; bu nedenle canlı baremin asıl kaynağı history seçildi.

İlk dağıtım kontrolünde yeni kaynak okumasının Nuxt/Vue state'i göremediği ortaya
çıktı. Kurulu Patchright API'si `page.evaluate` için varsayılan olarak isolated
context kullanır; global state ve Vue DOM expando'ları bu context'te görünmez.
Kaynak ve history çağrısı main context'te çalıştırılarak düzeltildi. Gerçek
Patchright browser ile offline uçtan uca test, isolated context'te kaynak
kimliğinin boş ve main context'te doğru olduğunu, exact bet365 HTTP yanıtının
yakalanıp doğrulandığını kanıtlar. Bu, eski Manresa olayının kanıtlanmış nedeni
olarak sunulmaz; eski parser DOM metnini okuyordu.

Kaynak erişilemezse veya güncel history skor/saatle eşleşmezse yeni akış sinyal
üretmez; bu durum kazanma garantisi veya bütün canlı maçların başarılı okunduğu
anlamına gelmez. Manresa olayını kesin sınıflandırmak için sinyal anındaki
ham odds/history, company/market ID ve provider güncelleme zamanı gerekir.

## Servis kontrolü

Bot son kodla 4 Ekim 2026 01:06:09 Türkiye saati, dashboard 00:56:54 itibarıyla
`active/running`. Dashboard HTTP 200. Son dağıtımdan sonraki ilk üç tamamlanmış
çevrimde sırasıyla 2, 1, 1 maç doğrulanıp işlendi; işleme ve ana döngü hatası 0.
Atlanan maçlar eski history, skor/kimlik uyuşmazlığı, periyot arası, duplicate en
yeni history veya eksik bet365 satırı nedeniyle reddedildi. İnceleme sonunda
4 doğrulanmış snapshot oluştu; yeni sinyal adayı henüz yayın koşullarını
karşılamadı, gerçek Telegram sinyal teslimi bu oturumda gözlenmedi.

Gerçek kaynak örnekleri:

| Kaynak zamanı (UTC) | Skor | Uygulama saati | Market | Bookmaker | Live | Yakalama anında yaş |
|---|---|---|---|---|---:|---:|
| 2026-10-03 22:06:28 | 23–58 | Q4 07:50 | bs | bet365/2 | 101.5 | 3.18 s |
| 2026-10-03 22:06:32 | 37–54 | Q3 05:40 | bs | bet365/2 | 148.5 | 7.61 s |
| 2026-10-03 22:07:15 | 40–56 | Q3 04:54 | bs | bet365/2 | 150.5 | 2.48 s |

Son örneğin history saati Q3 04:56; fark 2 saniye. Stored live, bet365 ham
history total ile aynı. İlk örneğin history saati Q4 07:44; fark 6 saniye.
Bu yeni gözlemler eski Manresa olayının kaynağını kanıtlamaz.

Servis başlatma öncesi kısıtlı izinli yerel SQLite backup alındı. Backup ile
karşılaştırmada eski alert barem/quality alanları ve eski snapshot verisinde
değişiklik 0. Kaynak kanıtı kolonları NULL olan tarihsel kayıtlar korunur.

## Kesinti sonrası devam kontrolü — 4 Ekim 2026

Başlangıç çalışma ağacındaki yarım kalan değişiklikler korundu. Mevcut 316
test geçti; eklenen hata senaryolarından 7'si düzeltme öncesinde başarısızdı.
Kanıtlanan ek kusurlar ve düzeltmeler:

- DOM hücresindeki kilit, canlı sütunu boşken birden fazla barem veya eksik
  hücre, önceki okuyucuda history kabulünü durdurmuyordu. Tek görünür bet365
  satırı/hücresi ve açık kilit/belirsizlik kontrolleri eklendi. Gerçekten boş
  hücrede güncel history kullanımı korunur. Patchright tarayıcı testleri bütün
  bu durumları gerçek DOM ile, dış kaynağa bağlanmadan sınar.
- History'nin Q öneki ile `status_id` çelişirse veya `06:74` gibi geçersiz
  saniye varsa önceki kontrol kabul edebiliyordu. Periyot öneki ve saat aralığı
  doğrulanır; protobuf'ta tekil total/status/update alanı tekrarı reddedilir.
- Kaynak kanıtı olmayan eski snapshot'lar tempo penceresinden çıkarılsa da
  40/48 dakika formatını ve saat/skor kronolojisini belirliyordu. Artık bu
  kontroller yalnız kanıtlı gözlemleri kullanır. İlk kanıtlı DB snapshot'ı
  eski satırla aynı değerlerde de kaydedilir; eski veri yeniden yazılmaz.
- Ham history kanıtı dashboard DTO'sundan çıkarıldı; yeni arşiv gösterimine
  kopyalanmaz. Kaynak kanıtı DB'de saklanmaya devam eder.

Salt okunur devam denetiminde 1.274 sinyalin 1.273'ü sonuçlanmıştı. Kayıtlı
final toplamına göre ALT/ÜST sonuç aritmetiğinde, snapshot live ve direction
ile kayıtlı sinyalin karşılaştırmasında uyuşmazlık 0. Eski sinyallerde kaynak
kanıtı olmadığı için bu kontrol eski odds'ın bookmaker doğruluğunu kanıtlamaz.
Üretim geçmişine UPDATE/DELETE veya backfill yapılmadı. Yeni migration yoktur;
önceki nullable kolon geçişi ve eski SQLite uyumluluk testleri geçer.

Tüm test paketi **331 geçti**; kaynak compileall ve diff whitespace kontrolü
geçti. README, proje haritası ve başlangıç logundaki eski yüzde-eşik anlatımı
mevcut Future Pace v5 ile eşlendi; yön/Quality matematiği değiştirilmedi.
Bot ve dashboard active/running görüldü; bu devam oturumunda servisler henüz
yeniden başlatılmadı. Yeni canlı sağlayıcı yanıtıyla üretim doğrulaması bu
devamda yapılmadı; tarayıcı kontrollerinde sentetik yanıt kullanıldı.

## Kullanıcının yeniden başlatması sonrası sağlık kontrolü

4 Ekim 2026 18:47 Türkiye saati: iki servis 18:40:10'da yeniden başlamış,
active/running ve otomatik yeniden başlama sayısı 0. Bot başlangıç logunda
Future Pace v5, bet365 `bs`, 30 saniye provider sınırı ve Quality 70 görülüyor.
Dashboard/aktif/arşiv/durum GET endpoint'leri HTTP 200; ham provider JSON'u
görünüm API'sinde yok. Saatlik görev thread'leri başlamış; SQLite quick_check
başarılı. Yeni snapshot'larda görünür hücre kontrolleri bulunuyor; ilk 16
kaynak kanıtlı gözlem kimlik/skor/barem ve yakalama-anı tazelik sınamasını geçti.

**Canlı takip tam sağlıklı değil:** ilk iki çevrim sırasıyla 50 ve 49 maç
buldu; 18:43:12 ve 18:46:38'de 180 saniye bütçesini aşarak iptal oldu.
Tamamlanan çevrim özeti yok. Gözlem kaydı çevrimden bağımsız ilerliyor:
yeniden başlatma sonrası 21 kanıtlı snapshot, sonuncusu 18:46:17.
Maç işleme hatası ve Telegram hata logu 0; yeni sinyal ve bekleyen Telegram
gönderimi 0. Yeni sinyal teslimi uçtan uca doğrulanmış sayılmaz. Kaynakta
eski/kilitli veya barem/skor/kimlik uyuşmazlığı olan history reddediliyor.
50 maçın ayrıntı okumasının neden bütçeyi aştığı bu salt-okunur kontrolde
kesinleştirilmedi. Kod/servis/üretim kayıtlarına müdahale edilmedi.

## Tarama bütçesi düzeltmesi ve üretimde doğrulama

4 Ekim 2026 — İki sekmeli ayrıntı kuyruğu 50 maçlık listeyi 180 saniyede
bitiremiyor, hard timeout oturumu kapatıyor ve sonraki çevrim yeniden listenin
başından başlıyordu. Süre/maç sınırında kalan maçlara sonraki güncel listede
öncelik veren devam kuyruğu eklendi. İç ayrıntı deadline'ı 180 saniyelik
watchdog'dan 45 saniye önce durur, görevleri temizler ve gerçek kapsama ile
tamamlanan sonuçları döndürür. Salt kapasite ertelemesi `continuing`, gerçek
kaynak hatası veya sıfır ilerleme hâlâ `partial/error` raporlanır. Eksik
kapsama saklanmaz. Kuyruk limiti nedeniyle listenin sonu sürekli atlanmaz.
Kanıtsız/kilitli/eksik bet365 satırları history isteğinden önce elenir;
history sonrası tam doğrulama ve gönderim sınırındaki kanıt kontrolü korunur.
Nuxt state'i beklenirken beklenen maç kimliği/market zorunludur.

337 test geçti; iptal/temizlik, kuyruğun devamı, değişen güncel liste,
maç sayısı limiti, oturumun kapasite ertelemesinde korunması, teknik hataların
ve sıfır ilerlemenin sağlıklı sayılmaması testlerle doğrulandı. Patchright
testleri reddedilen DOM için history isteği yapılmadığını da kontrol eder.
Compileall ve diff whitespace kontrolü başarılı. DB migration ve geçmişi
yeniden yazma yok; v5/Quality/sonuç matematiği değiştirilmedi.

Bot servisine düzeltme 19:00:36 Türkiye saati uygulandı; dashboard süreci
korundu. 19:08 kontrolünde iki servis active/running, otomatik restart 0 ve
dashboard HTTP 200. Yeni scheduler logu yüklendi.

| Çevrim rapor saati | Süre | Kontrol edilen / bulunan | Geçerli gözlem | Sonraki çevrime kalan | İşleme hatası |
|---|---:|---:|---:|---:|---:|
| 19:02:52 | 135,0 s | 20 / 40 | 8 | 20 | 0 |
| 19:05:11 | 135,1 s | 23 / 50 | 6 | 27 | 0 |
| 19:07:30 | 135,1 s | 16 / 50 | 3 | 34 | 0 |

Üç rapor `continuing`; kaynak hatası sayıları 0, ana döngü ve Telegram hata
logu 0. Üçüncü çevrim sonrası eşzamanlılık 2'den 3'e yükseldi. 17 yeni
snapshot kimlik/skor/barem ve yakalama-anı tazelik kontrolünden geçti.
Kaynakta kilitli, eski, eksik veya uyuşmayan kayıtlar reddediliyor; tam
listenin her tek çevrimde kontrol edildiği iddia edilmez. Yeni sinyal 0;
gerçek Telegram sinyal teslimi bu dağıtım kontrolünde gözlenmedi.
