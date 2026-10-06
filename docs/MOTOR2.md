# M2: bağımsız basketbol değerlendirmesi

6 Ekim 2026 kullanıcı talebi: dashboard'da mevcut motorun yanında M2 yönünü
göster; veri yoksa bunu açıkla, ayrı modalda veri kalitesini ve ALT/ÜST
gerekçesini göster. Bu talep, önceki yalnız araştırma okuyucusu kararını
bu bağımsız katman için genişletir.

## Akış ve mevcut motorun sınırı

- M1'in karar/tekrar/liste/kaynak doğrulaması, kayıt ve Telegram akışı korunur.
- Yalnız bundan sonra oluşacak gerçek sinyal satırı, INSERT sırasında M2
  `pending` bağlamı taşır. M2, M1'in ALT/ÜST yönünü kullanmaz.
- `motor2_worker.py` ayrı asyncio görevi ve ayrı `m2` tarayıcı profiliyle
  istatistiği çeker. M1 istatistik isteğini veya M2 sonucunu beklemez.
  M2'nin aynı bilgisayar/ağ kaynaklarını kullanmasının etkisi üretimde henüz
  ölçülmedi. Tek sekme kullanır; okuma bütçesi 40 saniyedir.
- 7 Ekim erişim düzeltmesi: yeni profilin sayfası `session.fetch` üzerinden
  açılır; böylece Scrapling'in Cloudflare çözümü uygulanır. Fetch sürerken
  aynı sekmenin URL/Nuxt kimliği gözlenir ve hazır olduğunda API okunur.
  Tam yükleme olayını beklemek zorunlu değildir. Görevler sonuç/timeout/
  iptalde kapatılır; havuz sekmesinin yaşam döngüsü Scrapling'e aittir.
- İlgili AIScore maç sayfasındaki public `lineups`/`tlive` protobuf yanıtları
  cache'siz okunur. Özel hesap veya yeni ücretli servis yoktur.
- Başarısızlık M2 durumuna dönüşür; M1 veto edilmez, yönü çevrilmez ve M2 için
  Telegram gönderilmez. `M2_ENABLED=false` yeni analizleri kapatır.
- M2 değerlendirmesi bir kez tamamlanır, sonraki canlı veriyle güncellenmez.
  Worker iptalde kendi tarayıcısını kapatır; canlı/final profillerine dokunmaz.

## Veri kapısı

Kimlik ve her iki takım skoru sinyal anıyla aynı olmalıdır. API okuması
sinyal gözleminden sonraki 60 saniye içinde, aynı periyotta ve en fazla
15 saniye oyun saati ilerlemesiyle yapılmalıdır. Olay akışında aynı skor ve
en fazla 30 saniye oyun saati farkı aranır. İki takımın şut denemeleri ve
isabetleri skorla aritmetik olarak uzlaşmalı; hücum ribaundu ve top kaybı
bulunmalıdır. Pozisyon tahminlerinin takım farkı `max(4, %10)` sınırını
aşamaz. Her takımda en az 15 saha içi deneme, oynanmış en az 12 dakika ve
kalan en az 3 dakika gerekir. Uzatma/kimliği veya saati belirsiz gözlem kabul
edilmez. Bu sürüm header'da Q1–Q4 saatini doğrular; H1/H2 veya yalnız devre
bitiş yazısı varsa M2 üretmez.

HTTP okuma zamanı sağlayıcı güncelleme zamanı değildir. Yeni çapraz
kontroller bu eksikliği tamamen çözmez; kullanılabilir verinin kalite etiketi
bu nedenle **ORTA** ile sınırlıdır. Eksik alan sıfır yapılmaz. Veri yokluğu,
veri uyuşmazlığı, süresi dolan analiz ve veri yeterli fakat avantaj olmayan
**PAS** ayrılır. Eski sinyaller **Kayıt yok** gösterir.
7 Ekim itibarıyla yeni başarısız analizler `reason_code` da saklar: erişim,
süre dolması, kimlik/skor/saat uyuşmazlığı, şut/olay eksikliği ve hacim
tutarsızlığı ayrı gösterilir. Şut verisi yoksa olay akışı kontrolünden önce
bu eksiklik raporlanır. 15/30 saniye sınırları tam saniye karşılaştırılır;
float yüzünden sınırdaki geçerli gözlem reddedilmez.

## Matematik ve açıklaması

Şut hacmi ve isabet oranları ayrı ele alınır. Takım başı pozisyon sayısı
`0.96 × (FGA + 0.44 × FTA − ORB + TOV)` ile yaklaşık hesaplanır; hücum
ribaundu yeni pozisyon sayılmaz. Formülün kaynağı:
[FIBA Basketball Terminology](https://assets.fiba.basketball/image/upload/documents-corporate-regional-offices-europe-programs-fiba-europe-coaching-certificate-fiba-europe-basketball-terminology.pdf).
Kesin pozisyon sayısı veya faul bonusu olay metninden çıkarılmaz.

2 sayı, 3 sayı ve serbest atış isabetleri ayrı beta ortalamalarıyla zayıf
öncüllere yaklaştırılır. **Bunlar AIScore'dan çekilen takım/lig ortalaması
değildir:** sırasıyla %52 / 12 deneme, %35 / 18 deneme, %75 / 8 deneme
model varsayımlarıdır. Gözlenen şut hacmi/dakika korunur; yeni beklenen sayı
hızı üç şut türünün deneme hızı ve dengelenmiş isabetiyle hesaplanır.
Pozisyon hesabı ayrıca tutarlılık kapısı ve hacim açıklamasıdır; aynı hücum
ribaunduna ikinci kez sayı bonusu eklenmez. Kişisel faul toplamı gösterilir;
bonus veya gelecek serbest atış sayısı buradan uydurulmaz.

Merkez tahmin = mevcut toplam + kalan dakika × beklenen sayı/dakika.
İsabet ortalamasının bir standart sapma alt/üst senaryosu ve tempo ±%5
senaryosu ayrı alt/üst projeksiyon oluşturur. Bu aralık bir tahmin güven
aralığı veya kazanma olasılığı değildir. ALT ancak üst senaryo baremin,
ÜST ancak alt senaryo baremin `max(4, barem × %2)` farkla ilgili tarafında
kalırsa verilir. Diğer durumlar PAS'tır. İkinci yarı 20+ sayı farkında
rotasyon/tempo belirsizliği yüzünden PAS verilir. M1'in açılış farkı, tempo
pencereleri, adil baremi veya kalite puanı kullanılmaz.

Şut kalitesi, güncel beş, sakatlıklar ve çekimi araştırılmış olsa da henüz
eşlemesi tamamlanmamış takım sezon ortalamaları bu sürümün girdisi değildir.
Başarı üstünlüğü ölçülmüş değildir; model varsayımları modalda görünür ve
sonuçla beraber kaydedilir. M2'nin sonucu mevcut M1 sonuç alanına yazılmaz.

## Saklama, arşiv ve açılış

Tek eklemeli nullable kolon: `alerts.m2_analysis_json`. `Database.init()`
kilit altında eksik kolonu ekler; tekrar çalıştırılabilir. Eski satırlar NULL
kalır; backfill, yeni tablo veya mevcut sinyal alanlarının yeniden yazılması
yoktur. Üretim DB'si bu geliştirme sırasında açılıp migrate edilmedi.

Canlı DTO `m2` nesnesini kaydedilmiş JSON'dan okur. Arşiv yalnız
`display_snapshot.m2` gösterir; ham kolon veya final verisiyle yeni analiz
yapmaz. Arşivlenmiş satırın bekleyen M2'si worker tarafından tamamlanamaz.
Arşivde bekleyen durum "Analiz tamamlanmadı" gösterilir. Modal açılması yeni
API çağrısı yapmaz. Ayrı modal M1 modali, filtre ve sıralamasıyla birlikte
çalışır; açık M2 canlı dashboard'un normal yenilemesinde güncellenir.

M2, bot ve dashboard mevcut servisleri yeni kodla yeniden başlatıldığında
yeni sinyallerde çalışır. Geliştirme sırasında bu servisler başlatılmadı veya
yeniden başlatılmadı; Telegram mesajı gönderilmedi. İlk gerçek M2 sinyalinin
uçtan uca kaynak çekimi ve ileriye dönük başarı kontrolü henüz yapılmadı.
