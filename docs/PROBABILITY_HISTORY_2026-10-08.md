# Geçmiş örneklemi, lig kontrolü ve bütün uygun sinyaller

Kullanıcı 38 eğitim maçını yetersiz buldu, eski maçların ve lig farklarının
incelenmesini ve %60 olasılık yayın filtresinin kaldırılmasını istedi.

## 38 neden çıktı?

İlk yöntem yalnız v9 tabanlı kaydedilmiş tahminleri kullanıyordu. Zaman sıralı
kontrol sınırından önce 82 maç vardı; 44 final kontrol başladıktan sonra
kaydedilmişti. O gün bilinemeyen sonuçları eğitime katmamak için eğitim 38'e
düştü. 36 maç kontrol içindi; başka bitmiş maçlar bu ayrımda eğitim dışındaydı.
Bu kontrol modeli, gelecek maçlar için bilinen bütün finallerle yeniden
hazırlanacak modelle karıştırılmamalıydı.

İnceleme anında arşivde 1.485 sinyal / 1.226 farklı maç, 1.224 otomatik
sonuçlu ilk-sinyal maçı vardı. 1.273 sinyalde frozen tahmin/kaynak bağlamı yok.
Eski modellerin farklı toplamlarını doğrudan aynı tahminmiş gibi kullanmak
uygun değildir. Ancak kanıtlı eski snapshot'lardan bugünkü taban kurulabilir.

## Güvenli yeniden kurulum

SQLite yalnız okunur açılır. Maç/şirket/piyasa, skor/saat, açılış/maç önü/canlı
baremler ve yakalama zamanı eski kanıtın kendi kurallarıyla doğrulanır. Bugünün
taban hesabı yalnız o andan önceki doğrulanmış gözlemleri kullanır. İlk uygun
gözlem sonuç görülmeden seçilir; bozuk ilk gözlem sonraki kazananla değiştirilmez.
Otomatik archive/forecast finalleri birleştirilir, çelişkili final kullanılmaz.
Eksik kaynak kanıtından tahmin veya barem uydurulmaz; eski arşivler değişmez.

4–7 Ekim gözlemlerinden 215 kullanılabilir sonuçlu maç bulundu; 71'inde eskiden
kaydedilmiş forecast yoktu. Diğer eski v7/v8 gözlemleri de aynı güncel tabana
getirildi. Bir uygun maç sonuç bekliyor, çelişkili final0.

Geçmiş yöntem kontrolü 115 eğitim / 65 kontrol (38 ALT, 27 ÜST) kullanıyor;
sonucu kontrol başlangıcından sonra bilinen 35 erken maç bu eğitimden dışarıda.
İki yönün olasılık doğrulaması da geçmedi. Gelecek kayıtların dondurulmuş
hata modeli ise şu anda bilinen 215 finalin tamamıyla hazırlanır; bu model
geçmiş kontrol başarısı veya ileriye dönük doğrulama diye etiketlenmez.
`chronological_validation` ayrı; `prospective_validated=false` ve iki yönün
`accepted=false`. Eğitim veri özeti SHA256 ile izlenir; ham maç verisi modele
yazılmaz. Hata ortalaması0,7157051376, yayılımı2,8026604622
(kalan dakikanın kareköküne göre normalize edilmiş değerler).

## Ligler

Tüm ilk-sinyal sonuç geçmişi 189 lige, kullanılabilir tahmin hataları 74 lige
dağılıyor. Lig başına hata ortalaması/yayılımı ölçülür; otomatik lig katsayısı
eklenmez. Uygun örneklemi en büyük ligde bile yalnız 17 maç vardır.

| Lig | Kullanılabilir hata maçı | Tüm eski ilk-sinyal finalleri |
| --- | ---: | ---: |
| FIBA Europe Cup | 17 | 15 |
| Basketball Champions League | 11 | 15 |
| NBA | 11 | 11 |
| Club Friendship | 10 | 98 |
| EuroCup Basketball | 10 | 21 |
| Liga Nacional de Baloncesto Profesional | 7 | 39 |
| B.League Premier | 6 | 35 |

İlk sütun, sinyal çıkmasa da kayıtlı gözlemi olan maçları içerdiği için sinyal
geçmişinden büyük olabilir. NBA ile Champions League hata ortalamaları farklı;
küçük lig örneklemlerini kesin katsayı yapmak için yeterli kanıt yoktur.

## Yayın ve günlük sayı

Olasılık yayını veto etmez; %60 altı dahil diğer bütün kaynak, süre, sayı
avantajı, tekrar ve kara liste koşullarını geçen sinyaller gönderilir.
Eski `MIN_SIGNAL_WIN_PROBABILITY` yalnız kod uyumluluğu için0 olarak tutulur;
ortamdan okunan eski değer yayın filtresini geri açamaz. Yeni frozen politikada
`probability_publication_filter_enabled=false` bulunur.

7 Ekim'in kayıtlı 3.652 gözlemi mevcut tekrar ve liste kurallarıyla incelendi:

| Hata modeli | %60 filtresi | Sinyal | Farklı maç |
| --- | --- | ---: | ---: |
| Önceki 38 maç | açık | 114 | 89 |
| Önceki 38 maç | kapalı | 114 | 89 |
| Yeni 215 maç | açık | 100 | 85 |
| Yeni 215 maç | kapalı | 100 | 85 |

Yeni hesapta 53 ALT / 47 ÜST. Sıklığı bu örnekte olasılık eşiğinden ziyade
sayı avantajı koşulu sınırlar. Tek gözlenen gün, maç yoğunluğundan bağımsız
günlük vaat veya kazanma başarısı değildir. Model geçmiş finallerle sonradan
hazırlandığından bu tekrar ileriye dönük test değildir; eski kanıtlarda yeni
DOM kontrolleri ve eski manuel arşiv/liste değişimleri tekrar kurulamaz.

Komutlar: `probability_audit.py --db basketball.db --reconstruct` ve
`signal_volume_audit.py --db basketball.db --day 2026-10-07` salt okunur rapor
verir; model dosyasını kendiliğinden değiştirmez veya Telegram göndermez.

## Kontrol ve yükleme

Tam paket528 test (51,44s). Son audit metadata temizliği ve ek salt-okunur
günlük tekrar regresyonuyla ilgili20 test geçti. %60 altındaki uygun sinyalin
geçici DB'ye kaydı ve mock Telegram teslimi sınandı; gerçek mesaj gönderilmedi.
Eski kaynaktan yeniden kurulum, bozuk barem reddi, arşivin değişmemesi, bütün
bilinen finallerle ayrı model hazırlama ve kronolojik yönün kontrol modeliyle
seçilmesi sınandı. Compileall/diff başarılı.

Tutarlı0600 SQLite yedeği ve quick_check sonrası kullanıcının süren yenileme
yetkisiyle 8 Ekim01:52'de iki mevcut servis yenilendi. Active/running,
restart0, traceback0; bot yeni kod hash'ini ve filtre kapalı başlangıcını
doğruladı. Sayfa/API HTTP200. İlk3 yeni üretim snapshot'ında eğitim215,
implementation/model hash ve filtre=false doğrulandı; DB quick_check başarılı.
Yeni migration veya arşivlere hesap yazılması yok.
