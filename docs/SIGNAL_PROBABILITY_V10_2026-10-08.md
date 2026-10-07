# V10: kazanma ihtimali ve tutarlı sinyal kararı

8 Ekim sonraki kullanıcı talebi: %60 yayın filtresi kaldırıldı, hata modeli
kanıtlı eski gözlemlerin yeniden kurulumu dahil 215 maçla gelecek kayıtlar için
yeniden hazırlandı. Bu belgedeki 38 maç/%60 bölümleri ilk uygulamanın tarihçesidir.
Güncel kapsam ve lig/sıklık kontrolü: `PROBABILITY_HISTORY_2026-10-08.md`.

Kullanıcı olasılık hesaplanmasını, puan/etiket gösterimiyle yetinilmemesini ve
ekrandaki taşmanın giderilmesini istedi. Önceki v1 hesapta `accepted=false`
olunca yüzdeyi tamamen kapatma kararı kaldırıldı. Olasılık tahmini ile geçmiş
kontrol sonucu iki ayrı şeydir; hesaplanan yüzde artık gösterilir.

## Matematik

V9 skor/öncül/yakın tempo ve sıcak-fazla düzeltmesi taban toplam `B` üretir.
Dondurulmuş kaynak doğrulanan eğitimde `(final-B)/sqrt(kalan dakika)` hatasının
ortalaması `mu`, yayılımı `s` öğrenilmiştir. V10 final dağılımının konumu
`B + sqrt(kalan dakika)*mu`, genişliği
`sqrt(kalan dakika)*s*sqrt(1+1/eğitim maçı)` olur. Son çarpan, eğitim ortalamasının
biliniyormuş gibi davranılmasını önler. Normal dağılım varsayımı ve kalan
dakikanın karekökü ölçeklemesi sürer; yeni şut/hücum verisi varsayılmaz.

Final skor tam sayıdır. ALT ihtimali `ceil(barem)-0,5` altında, ÜST ihtimali
`floor(barem)+0,5` üstünde kalan kütledir. Tam sayı baremde aradaki kütle iadedir;
kazanma ihtimaline eklenmez. Dağılım `mevcut toplam skor-0,5` altında kesilir ve
kalan kütle tekrar ölçeklenir: maçın zaten aldığı sayılar silinemez.

Yön ALT/ÜST ihtimallerinin büyüğünden gelir; eşitse EŞİT olur. Gösterilen yüzde
o yönün kazanma ihtimalidir. Sinyal, mevcut sayı avantajı koşulları yanında
`MIN_SIGNAL_WIN_PROBABILITY` koşulunu geçer; varsayılan0,60, geçerli aralık
0,5≤değer<1. Olasılık barajı yayını azaltabilir. Bu koşul öğrenilmiş başarı
garantisi veya finansal getiri hesabı değildir.

Model parametreleri önceki 38 eğitim maçından dondurulmuştur; canlı eğitim,
runtime DB taraması veya her yenilemede model değiştirme yoktur. Çekilmiş
skor, kalan süre, v9 tabanı ve geçmiş hata dışında görünmeyen hücum/şut
bilgisi hesaba eklenmez. Mevcut v9 hata modelinin v10'a taşınması bir model
varsayımıdır; v10 başarısı gelecek otomatik final sonuçlarıyla ayrıca ölçülür.
Eski kontrol metrikleri ileri başarı kanıtı olarak sunulmaz.

## Kayıt ve ekran

V10 `base_predicted_total`, `score_total`, ALT/ÜST/iade ihtimalleri, tercih
edilen yön, model hash ve politika parametrelerini ilk kayıt anında saklar.
Tahmini toplam da aynı hata düzeltmesini kullanır. Tarihsel yön kontrolü v10
için saved ihtimalleri denetler; bugünkü modelle yeniden hesaplamaz. Önceki
sürümlerin merkez/yön kontrolü aynıdır.

Aktif v9 kayıtta saklanan center ve clock ile yönün ihtimali hesaplanabilir.
Kontrol anında 19 aktif v9 kayıt vardı; 19'unda da hesap oluştu. Geçmiş arşiv
yalnız kendi snapshot'ından gösterir; eksik olasılık boş kalır. Eski puan
kutusu ve puan bileşenleri kaldırıldı. İhtimal kutusu ve sütun genişliği
taşmayı önler. Çok uç olasılık yuvarlanıp `%100` diye sunulmaz; `>%99,9` veya
`<%0,1` gösterilir. Baremi değiştiren geçici senaryo kendi olasılığını üretir;
asıl kayıt ve sonuç değişmez. Telegram aynı frozen ihtimali kullanır.

## Kontrol

Son tam paket 525 test (47,72s). Eski tempo/scraper testleri hata düzeltmesini
sıfırlayarak o aşamayı yalıtır; yeni olasılık testleri gerçek öğrenilmiş hata
ortalaması, dağılım genişliği, fiziksel skor sınırı, tam sayı iadesi, yön ile
yüzde tutarlılığı,0,60 yayın eşiği ve model değişse de frozen kayıt kontrolünü
ayrı sınar. İlk kaybeden tahmin sonraki kazananla değiştirilmez.
Son aktif-v9 ve uç-yüzde regresyonlarıyla ilgili 79 test daha geçti.

1863/1440/390px fixture tarayıcıda canlı, arşiv ve tahmin sayfaları; yüzde
kutusunun kendi hücresi içinde kalması, iç taşma olmaması ve eski puanın yokluğu
kontrol edildi. JavaScript hatası0; Python compileall, JS syntax ve diff geçti.
Yeni tablo/kolon, migration veya arşiv backfill yok. Gerçek DB yalnız toplam
kontrolleri için salt okunur açıldı. Kaynak doğrulaması ve şut/hücumun kapalı
kalması önceki kararla aynıdır; oran/net getiri/uzatma için yeni çalışma yok.
Kullanıcının açık talimatıyla tutarlı SQLite yedeği alındı (0600, quick_check
başarılı). İlk yenilemede gerçek mobil sayfanın gizlediği açılış/maç önü
sütunlarının yanlış uyuşmazlık sayıldığı görüldü: state159,5/159,5,
görünür canlı147,5. Render doğrulama v2 yalnız görünür açılış/maç önü
hücrelerini karşılaştırır; canlı hücrenin görünür ve eşleşmiş olması şarttır.
Eksik hücre/görünür uyuşmazlık reddi ve eski frozen kanıt kontrolü korunur.
Bu düzeltmenin ilgili 96 testi geçti; 8 Ekim 00:30'da iki servis yenilendi.
İki servis
active/running, restart0; yeni invocation günlüklerinde traceback yok.
Botun v10 publication ve mevcut implementation hash'i doğrulandı.
Canlı/tahmin sayfaları ve API'leri HTTP200; mevcut 3 aktif kaydın tümünde
sayısal olasılık var. İlk yeni v10 snapshot'ta sayısal olasılık,
implementation/model hash ve render v2 kanıtı üretim verisinde doğrulandı.

Günlük sıklık için 7 Ekim'de kaydedilmiş 3.652 v9 gözlem yeni karar ve tekrar
kurallarıyla salt okunur yeniden değerlendirildi: 89 maçtan 114 sinyal
(103 ALT, 11 ÜST). Eski sistemde 95 maçtan 123 kayıt vardı. Benzer yoğunlukta
100–120 sinyal / 80–90 maç yaklaşık beklentidir; tam gün ileriye dönük sonuç
değildir. Yeni DOM kontrolleri geçmiş veride tekrar doğrulanamaz; veri erişimi
ve maç yoğunluğu gerçek sayıyı değiştirebilir. Bu sayı başarı oranı kanıtlamaz.
