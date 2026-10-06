# Sinyal başarısının neden tatmin edici olmadığı — 6 Ekim 2026

Bu değerlendirme 6 Ekim 2026, yaklaşık 03:03 Türkiye saati SQLite kesitiyle hazırlanmıştır. Kod, servisler, sinyaller, sonuçlar ve arşiv snapshot'ları değiştirilmedi. Mevcut `APPLICATION_LOGIC_REVIEW_2026-10-06.md` dosyası korundu. DB yalnız URI `mode=ro` ve `query_only` ile okundu; bu belge tekil maç kayıtları, kişisel bilgiler veya ham kaynak yanıtları içermez.

## Hüküm

Uygulama veri doğrulama ve operasyon açısından gelişmiş. Bununla birlikte piyasadan daha iyi tahmin, oynanabilir fiyat ve sürdürülebilir net getiri birlikte kanıtlanmış değil. Refaktörlerin çoğu veri ve çalışma hatalarını gideriyor; tahmin varsayımlarının doğruluğu ayrıca sınanmadan başarı artışı beklemek mümkün değil.

En belirgin tarihsel bulgu ALT ile ÜST arasındaki ayrışma. En belirgin model eksikliği sayı hızının, gerçek hücum temposu ile şut verimini ayırmaması. En belirgin araştırma eksikliği farklı sürüm, kaynak, teslimat ve bahis fiyatlarının aynı başarı yüzdesiyle değerlendirilmesi.

Mevcut DB'de tutulan sinyaller 19 Eylül–6 Ekim Türkiye tarihlerini kapsıyor. Git geçmişi daha eski geliştirmeleri gösterse de aylardır yapılan bütün değişikliklerin karşılaştırılabilir sonuç geçmişi elde bulunmuyor. Aşağıdaki bulgular tutulan kayıtları kapsar.

## Yeniden hesaplanan sonuçlar

1.321 sinyal, 1.095 maç: 744 başarılı, 573 başarısız, dört sonuçlanmamış. Başarı paydası yalnız kazanç ve kayıptır. Bütün sonuçlanmış satırlarda başarı %56,49; her maçın ilk sinyalini sonuca bakmadan seçince 623/1.091 = %57,10. Tekrar satırları bağımsız maçlar değildir.

| Grup | Başarılı | Başarısız | Başarı |
| --- | ---: | ---: | ---: |
| Eski kaynak dönemi ALT, karma sürümler | 498 | 338 | %59,57 |
| Eski kaynak dönemi ÜST, karma sürümler | 222 | 215 | %50,80 |
| Güncel kod politikası, maç başına ilk sonuçlanmış sinyal | 15 | 13 | %53,57 |
| Güncel politika ALT, ilk sinyal | 13 | 10 | %56,52 |
| Güncel politika ÜST, ilk sinyal | 2 | 3 | %40,00 |

Eski dönem 1.274 kayıt; yeni ham bookmaker kanıtı ve frozen politika taşımıyor. Eski bookmaker seçimi kusuru belgelenmiş olsa da her eski baremin yanlış olduğu kanıtlanmış değil. Eski başarıyı yeni doğrulanmış sistemin başarısı sayamayız.

Yeni kaynak/politika taşıyan 47 kaydın 47'si ham kaynak doğrulamasını geçti. Bunlar iki kod politikasına ayrılıyor. Güncel politika 36 sinyal/31 maç; ilk sinyallerin 28'i sonuçlanmış, üçü bekliyor. Önceki politika ilk 10 maçta 5/10. Bu gruplar yeni sistemin tek bir test örneklemi gibi birleştirilmemeli. Kod politikaları operasyonel değişikliklerden de ayrılabilir; bu ayrım mutlaka yeni bir basketbol modeli anlamına gelmez.

Güncel ilk 28 sonucun Wilson %95 aralığı %35,81–70,47. Lig/gün bağımlılığı bu aralıkta yok. Bu sonuç yeni motoru kesin başarısız ilan etmeye de, başarılı olduğunu söylemeye de yetmez. Eski ve yeni oran farkı doğrudan bir refaktörün etkisi olarak yorumlanamaz.

Quality v1 eski döneminde, maçın ilk sinyalleriyle 27 Eylül–3 Ekim grubunda ALT 209/347 = %60,23; ÜST 99/197 = %50,25. Aynı sonraki grupta kalite ROC-AUC 0,503. Quality >=70 grubunda 21/35 = %60; alt grupta 287/509 = %56,39. Önceki tek günün yüksek kalite 16/19 = %84,21 sonucu genellenememiş. Bu tarih aralıkları daha önce de incelendiğinden bağımsız yeni test değildir.

## İlk hipotez ile mevcut modelin farkı

İlk kural, piyasadaki açılış–canlı hareketinin aşırı tepki olduğunu varsayıyordu. Ancak hareketin bir bölümü zaten gerçekleşmiş sayılardır; bir bölümü yeni maç bilgisidir. Başlangıç 180 iken devrede 100 sayı ve canlı 195 olması, kalan yarı için 95 sayı fiyatlandığını gösterir. Tek başına +15 hareket, ALT avantajı kanıtı değildir. Devrede 75 sayı ve canlı 165 olduğunda kalan 90 sayı başlangıçtaki yarım maç beklentisine eşittir; -15 hareket kendiliğinden ÜST avantajı değildir. Bu örnekler açıklama amaçlıdır.

Future Pace v5, referansı normal süreye bölüp sayı/dakika öncülü kurar; geçmiş hızları bu öncüle yaklaştırır; canlı baremin kalan sürede gerektirdiği hızla karşılaştırır. Bu çerçeve daha anlamlı bir soruya bakıyor, fakat gelecekteki hızı doğru bildiğini kanıtlamıyor. Güncel ilk 23 ALT'ın 19'u referansın altındaki canlı baremde; sekiz ÜST'ün tamamı referansın üstünde oluşmuş. Dolayısıyla başlangıçtaki ters hareket kuralı artık yönü belirlemiyor.

Eski basit kurala dönmenin daha iyi olacağını mevcut sinyal arşivinden gösteremeyiz. Arşiv yalnız seçilmiş yayımlanan sinyalleri içeriyor; diğer kuralın seçeceği bütün fırsatların güvenilir gözlemleri ve sonuçları aynı kapsamda bulunmuyor.

## Basketbol açısından eksik kalan ayrım

Sayı/dakika kabaca hücum/dakika ile hücum başına verimin çarpımıdır. Aynı düşük sayı hızına yavaş hücumlar da, hızlı fakat kaçan şutlar da yol açabilir. Aynı yüksek hız hızlı oyundan da, geçici yüksek isabetten de gelebilir. Model bu durumları ayıracak canlı hücum/şut/bonus verisini kullanmıyor.

Bu nedenle düşük sayı hızında ALT vermek, kaçan şutların kalıcı olacağını fazla varsayabilir. Yüksek sayı hızında ÜST vermek, geçici isabetin devamını fazla varsayabilir. Bu açıklamalar modelin gözlemediği risklerdir; tek tek geçmiş kayıpların kanıtlanmış nedeni değildir. [NBA tanımında tempo hücum sayısıdır](https://www.nba.com/stats/help/glossary); uygulamanın PPM değeri toplam sayı hızıdır.

Faul/bonus, serbest atış, üçlük denemeleri, rotasyon, yakın maçın son bölüm davranışı ve uzatma ihtimali ayrıca modellenmiyor. Skor farkına uygulanan çarpan bu unsurların yerini tutmaz. Daha fazla veri ancak güvenilir, zamanında ve ileri dönem performansını artırıyorsa yararlıdır.

Model normal süre üzerinden hesap yapıyor. [bet365 genel basketbol kuralları](https://help.bet365.com/s/en-us/sportsrules/basketball) canlı maç bahislerinde uzatmayı dahil ediyor. Bu sayfa belirli AIScore `bs` etiketinin ve kullanıcının gerçek bahis sözleşmesinin doğrulanması yerine geçmez. Uzatma dahil piyasa kullanılıyorsa tahmin de bu riski kapsamalı; yalnız OT sırasında yeni sinyal vermemek yeterli değildir. Eski sonuçlar varsayımla değiştirilmemeli.

## Model farkı neden gerçek avantaj olmayabilir?

Güncel ilk 28 sonuçta model merkezinin final toplamına ortalama mutlak hatası 11,91 sayı; piyasa baremininki 10,96 sayı. Modelin finalden ortalama sapması -5,29 sayı; final modelden yüksek. 28 maçın 13'ünde model piyasadan daha yakın. Bu seçilmiş küçük grupta piyasanın geçildiği gösterilmiyor. Sabit +5 düzeltme veya yeni eşik seçimi için yeterli kanıt da yok. Merkez hatası tek başına yönün fiyat karşısındaki beklenen getirisini ölçmez.

Minimum avantaj varsayılan `max(4 sayı, canlı baremin %2'si)`. Ancak modelden çıkan dört sayılık farkın tahmin hatası veya fiyat maliyeti karşısında yeterli olduğu sınanmamış. Gerçekleşen hatanın çoğu kez çok daha büyük olması, modelin kendi tahminine göre fark ile doğrulanmış istatistiksel avantajın ayrılması gerektiğini gösteriyor.

Tüm maç, son iki/beş dakika ve mevcut periyot pencereleri örtüşüyor. Güncel 36 sinyalin 11'inde aynı snapshot birden fazla pencere için çıpa; altısında yalnız iki hesaplanan hız var. Yaklaşık iki dakikalık çıpa 21/36, beş dakikalık çıpa 32/36. Her sinyal dört bağımsız yakın gözlemle onaylanmış değil. Mevcut periyot penceresi gerçek periyot başlangıcı yerine o periyotta ilk gözlenen snapshot'tan başlayabilir.

Pencerelerin hepsi aynı maç önü öncülüne çekiliyor. Varsayılan on dakikalık öncül ağırlığıyla iki dakikalık gözlemin katkısı yaklaşık %16,7, beş dakikalığın %33,3. Bu yaklaşım gürültüyü azaltabilir; gerçek tempo değişimini de bastırabilir. Ağırlıkların ve pencere şartlarının lig/aşama bazında doğrulandığı gösterilmemiş.

En küçük–en büyük hız bandı istatistiksel güven aralığı değil. Aynı veri ve öncülden gelen tahminlerin yakın olması gelecekteki finalin belirsizliğini ölçmez. Modelde kalibre edilmiş ALT/ÜST olasılığı ve son toplam dağılımı yok.

## Güven göstergeleri ile gerçek yayın kuralları

Quality v1 farklı hareket varsayımlarıyla v5 kararını puanlıyor. Örneğin düşmüş baremde ALT kararı, piyasa hareketi bileşeninde ağır ceza alabilir. Güncel ilk 31 sinyalin 19'u Quality PAS etiketli olduğu halde motor ALT/ÜST üretmiş. Etiket kazanma olasılığı değildir; sonraki dönem AUC 0,503 bu puanın güven rozeti olarak kullanılmasını desteklemiyor.

Eski `THRESHOLD*`, çeyrek katsayıları ve `DISABLE_Q4_SIGNALS` v5 yön kararında etkin değil. Güncel 36 sinyalin üçü Q4. ÜST için sekiz sayı gibi bazı ayarlar da sert yayın filtresi yerine kalite bileşenine etki ediyor. Kullanıcının koruma sandığı ayarlar gerçek davranışla eşleşmeli. Bu görünüm sorunlarını düzeltmek gereklidir, ama tek başına tahmin başarısını artırmaz.

## Kayıtlı başarı neden yaşanan başarıyla ayrışabilir?

Yeni 47 kaydın 45'i sent, ikisi cancelled. Sonuçlanmış bütün yeni kayıtlar 24/44 = %54,55; teslim edilmiş sonuçlar 22/42 = %52,38. İki iptal kazanmış. Bu iki politikalı, tekrar içeren ayrım yalnız paydanın önemini gösteriyor. Güncel politikanın ilk 31 kaydının tümü sent; onun ana karşılaştırmasında teslimat iptali karışıklığı yok.

Gönderim öncesi doğrulama saklanmış ham yanıtı ve yaşını kontrol ediyor; yeni bookmaker fiyatı almıyor. Kullanıcı bahis ekranına ulaştığında aynı barem/oranın oynanabildiği kanıtlanmıyor. Kaynak doğrulaması ile gerçek giriş doğrulaması ayrı ölçülmeli.

Sadece duyarlılık örneği: güncel ilk 28 sonuç aynı tutulup ALT giriş baremi üç sayı aşağı, ÜST giriş baremi üç sayı yukarı varsayılırsa 15 kazanç 12'ye iner (%42,86). İki sayıda 14, beş sayıda 10 kazanç kalır. Bu gerçekleşmiş gecikme ya da gerçek bahis getirisi değildir; kayıtlı avantajın barem kaymasına hassasiyetidir.

Sinyal başına doğrulanmış ondalık oran ve gerçek giriş/tutar/getiri kaydı yok. Yeni ham history fiyat metinleri içeriyor; biçimi doğrulanmadan ondalık oran kabul edilmemeli. Örnek eşit tutar/iadesiz 1,90 oranda başa baş %52,63; 1,80'de %55,56; 1,70'te %58,82. Bunlar gerçek uygulama oranları değildir. Gerçek değişken oranlarda her işlemin net geri ödemesi ayrı hesaplanmalıdır.

Arşivde benzersiz hesabı maç+yön üzerinden yapılıyor; aynı maç iki yönle iki kez sayılabilir. Sonuçlanmış satırlar önce süzüldüğünden ilk bekleyen yerine sonraki sonuç seçilebilir. Araştırmada ilk sinyal sonuca bakılmadan seçilmeli. Mutlak 10 sayı tekrar kuralı daha iyi giriş baremi garantisi vermez. Karşıt yönler de otomatik güvenli hedge değildir.

Kalıcı arşiv temizleme, araştırma örnekleminin kaybı riskini taşıyor. Önceki silmelerin seçici yapıldığını gösteren kanıt sunulmuyor. Gelecek ölçümde araştırma kaydı korunmalı; görünümden kaldırma ayrıca yapılmalı. PAS/engellenen fırsatlar için tam kalıcı karar günlüğü olmadığı için mevcut arşivden bütün filtrelerin karşılaştırması çıkarılamaz.

## Önerilen sıra

1. **Ölçümü tamamlamak:** doğru fiyat biçimi, piyasa uzatma kapsamı, gerçek giriş baremi/oranı, kaynak/karar/teslim/giriş zamanları ve net getiri. Model kaydı, teslimat ve oynanan bahis için ayrı paydalar. Araştırma kayıtlarının kalıcı korunması.
2. **Mevcut politikayı sabit tutarak ileri dönem ölçmek:** maç başına ilk sinyal, ALT/ÜST ayrı, kaynak/kod politikası ayrı. Önceki en az yedi tam gün düzeni korunmalı; takvim süresi yeterli örneklem kanıtı değildir. Güncel ilk 31 maç 17 ligden geliyor; lig seçimi için yetersiz. Birkaç yüz maç/yön makul ilk araştırma hedefidir, gerekli sayı beklenen avantaj ve belirsizliğe bağlıdır.
3. **Yakın geçmiş ve belirsizliği sınamak:** aynı çıpayı ayrı onay saymamak, gerçek yakın pencere şartını açıklaştırmak; model hatasını aşama/format/yeterli lig örnekleminde ölçmek. Sabit puan farkı yerine gerçek fiyat karşısında kalibre edilmiş olasılık ve beklenen getiri hedeflemek.
4. **Basketbol verisini yalnız katkısı sınanarak eklemek:** hücum, şut, top kaybı, hücum ribaundu, faul/bonus. ALT araştırılmaya değer; ÜST'ü eşit güvenle sunmak için yeterli kanıt yok. ÜST politikasını ayrı doğrulamak veya doğrulanana kadar güven iddiasını azaltmak işletim tercihidir; bu inceleme botu değiştirmedi.
5. **Değişiklikleri tek hipotezle yapmak:** seçildiği dönemde iyi görünen kuralı daha sonraki maçlarda sınamak. Aynı anda birçok filtreyi değiştirip sonraki birkaç maçtan başarı sonucu çıkarmamak. Eski kazanç/kayıplara göre sürekli eşik veya kara liste seçmemek.

Başlangıç kuralına mekanik dönüş, başarısız yönü otomatik tersine çevirme, yeni dekoratif kalite puanı veya sırf sinyal sayısını azaltan daha sert eşikler şu an gerekçelendirilmiş çözüm değildir. Sistem basketbolu ve fiyatı daha iyi ayırt ettiğini gösterecek ölçüme ihtiyaç duyuyor.

## Doğrulama ve dayanaklar

1.317 sonuçlanmış satırda kayıtlı final/yön/barem sonuç aritmetiği uyuşmazlığı sıfır. Bu, bookmaker settlement sözleşmesini doğrulamaz. Yeni 47 sinyalde `_verified_prediction` kaynak kontrolü geçti. Motor, tempo kronolojisi, kalite, ileri doğrulama, tekrar ve maç formatı için mevcut ilgili **61 test geçti**. Üretim Python kodu değişmediğinden compileall gerekmedi. Servis/profil/Telegram işlemi yapılmadı.

Kod dayanakları: `live_signals.py:43`, `pace_calculator.py:55`, `signal_quality.py:31`, `forward_validation.py:153`, `main.py:234`, `main.py:308`, `live_market.py:284`, `match_state.py:100`, `dashboard.py:513`, `db.py:946`.

Önceki belgeler: `FORWARD_VALIDATION_2026-10-04.md`, `REVERSAL_AUDIT_2026-10-02.md`, `LIVE_TOTAL_AUDIT_2026-10-04.md`, mevcut `APPLICATION_LOGIC_REVIEW_2026-10-06.md`. Wilson aralığı için [NIST yöntemi](https://itl.nist.gov/div898/handbook/prc/section2/prc241.htm) kullanıldı. Belgelerin geçmiş tarihli politika kararları güncel kod davranışı yerine geçirilmedi.
