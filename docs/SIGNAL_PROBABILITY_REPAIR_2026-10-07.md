# Canlı barem ve kazanma olasılığı düzeltmesi

Kullanıcı canlı baremin herhangi bir görülebilir bahis şirketinden alınmasını,
oran fiyatı/net getiri yerine doğru ALT/ÜST kararının ve kazanma olasılığının
öncelik olmasını istedi. Uzatma için yeni çalışma istenmedi. Şut/hücum verisi
yeniden denenmeli, güvenilir çekilemiyorsa sinyal hesabına eklenmemeli.

## Canlı barem

Yeni yakalamalarda ana `Total Points` etiketi ve aynı şirketin görünür açılış,
maç önü ve canlı hücreleri uygulama verisiyle karşılaştırılır. Başka bir
şirketin değerleri birleştirilmez. Gizli alternatif hücreler okunmaz. İç içe
etiketlerde `1` + `35`, tam görünen `135` olarak okunur. Birden fazla farklı
toplam veya ekran/state uyuşmazlığı reddedilir; tahminle barem düzeltilmez.

Dolu canlı hücre için 150 ms sonra aynı şirket yeniden okunur. Son tutarlı
sayı/skor/saat birlikte kullanılır; değişen gerçek barem eski değere zorlanmaz.
İkinci yakalamada uyuşmazlık varsa diğer şirket denenir. Boş hücre için önceki
kimlikli history kontrolü sürer. Eski dondurulmuş kanıtlar yeni alanlar yok diye
geçersiz yapılmaz; eski kayıtlar yeni ekran kontrollerini geçmiş sayılmaz.

135/100 örneği kontrollü tarayıcı verisinde sınandı. Üretimde bu özel hatanın
gerçek kök nedeni yakalanmış değildir. Ekran ve AIScore verisi aynı yanlış
sayıda anlaşırsa bu kontroller hatayı bağımsız olarak kanıtlayamaz. Sağlayıcının
bağımsız güncellik kanıtı olmadan `provider_freshness_verified` false kalır.

## Olasılık

Eski Quality v1 puanı olasılığa çevrilmez. Ekran bunu eski puan olarak açıkça
etiketler. Yeni kayıtlar ayrı `win_probability` ve model hash taşır.
"Adil Barem" adı "Tahmini toplam" olur. Telegram ve tüm tahminler ekranı da
aynı kaydedilmiş olasılık durumunu gösterir. Geçici barem senaryosuna asıl
baremin olasılığı taşınmaz. Arşiv yalnız `display_snapshot` değerini okur;
eski kayıtta eksik olan olasılık/fark/eşik boş kalır.

Aday yöntem, final toplam eksi kayıtlı tahmini toplam hatasını kalan dakikanın
kareköküne böler; eğitimde hata ortalaması ve dağılım genişliği öğrenilir.
Normal dağılım ve karekök ölçeklemesi varsayımdır. Tam sayı baremde eşit final
ayrı iade olasılığıdır; ALT/ÜST kazanma olasılığına eklenmez.

Olasılık dilimlerindeki tahmin ile gerçekleşen başarıyı ayrı karşılaştırma
gerekçesi: [scikit-learn olasılık kalibrasyonu dokümanı](https://scikit-learn.org/stable/modules/calibration.html).
Tek bir toplam olasılık hatasının düşük olması tek başına güvenilir yüzdeleri
kanıtlamaz. Bu projede scikit-learn bağımlılığı eklenmedi.

Her maçta ilk uygun v9 gözlemi, sonuç/kazanan bilgisine bakmadan seçilir.
Erken bölüm eğitim, sonraki %30 kontrol olur. Kontrol başlangıcında henüz
sonuçlanmamış eğitim maçları çıkarılır. En az 60 eğitim maçı; ilgili yön için
en az 25 eğitim ve 25 kontrol maçı aranır. Ortalama ve olasılık dilimlerine
göre hata en fazla 10 yüzde puanı olmalı; olasılık hatası, eğitimde öğrenilmiş
sabit yön başarı tahmininden küçük olmalı. Bunlar temkinli kabul koşullarıdır,
başarı garantisi değildir. Kabul edilen modelde de kontrol grubunun kapsamadığı
olasılıklar ve %5–95 dışı değerler gösterilmez. Canlı model eğitilmez; rapor
model dosyasını otomatik güncellemez.

Bu oturumda salt okunur tutarlı okuma: 118 uygun sonuçlu v9 maç; zaman ayrımı
sonrası 38 eğitim, 36 kontrol. Sonuçların eğitim başlangıcında kullanılabilir
olması koşulu, erken gözlemlerin hepsinin eğitimde kullanılmasını engelledi.
ALT kontrolü 34 maç: ortalama tahmin %58,47, gerçekleşen başarı %52,94;
olasılık dilimleri toplam hatası 11,76 yüzde puanı. ÜST kontrolü yalnız 2 maç,
eğitimi 6 maç. İki yön de kabul edilmedi. Aktif dosyada kabul false olduğundan
uygulama yüzde üretmez: "Henüz doğrulanmadı". Bu sayıların kapsamı bildirim
başarısı değil, ilk uygun gözlem tahminleridir. Eski incelenmiş verideki bu
deneme yeni ileri başarı kanıtı değildir. Lig/gün bağımlılığı hesaba katılmaz.

## Basketbol verisi

Ayrı geçici tarayıcı profiliyle gerçek AIScore denemesi yapıldı; 150 saniye
süre sınırı, en fazla 6 maç ve tek ayrıntı sekmesi kullanıldı. 23 maç keşfedildi,
bir maç tamamlandı, bir canlı piyasa doğrulandı. Bu maçın sayfasında tamamlanan
iki istatistik okumasında `boxscore_missing` ve bir tarafta
`basic_stats_score_mismatch` görüldü. Sonraki okuma süre sınırında iptal edildi;
başarılı üçüncü okuma diye sayılmadı. Şut/hücum verisi motora eklenmedi;
M2 canlı akışta kapalı kalır. Bu sınırlı deneme AIScore'un hiçbir maçta veri
sunmadığını kanıtlamaz; güvenilir entegrasyon için yeterli değildir.

## Uyumluluk ve uygulama sınırı

Yeni tablo/kolon/migration yok. Mevcut `forecast_json`,
`prediction_context_json` ve `display_snapshot` kullanılır. Eski SQLite ve
arşivler yeniden yazılmaz. Sonuç yine yalnız otomatik final kontrolünden gelir.
V9 merkez/yön matematiği ve bildirim eşikleri değiştirilmedi. Bu değişiklik
daha yüksek kazanma başarısının kanıtı değil, kaynak ve güven gösterimi
düzeltmesidir. Çalışan servisler başlatılmadı, durdurulmadı veya yenilenmedi.

Kontroller: tam paket 511 test (44,03 saniye), son ek regresyon dahil olasılık
testleri 11/11; Python compileall, iki JS dosyasının sözdizimi ve diff kontrolü
başarılı. Ayrı fixture tarayıcıda 1440/390px canlı, geçmiş ve tahmin ekranları
doğrulandı; JavaScript hatası 0. Testlerde geçici DB ve tarayıcı kullanıldı;
gerçek denemede yalnız public kaynak okundu. Yeni Python kontrollerinin çalışan
bota geçmesi servis kodunun yeniden yüklenmesini gerektirir; bu oturumda
servis değiştirilmedi.
