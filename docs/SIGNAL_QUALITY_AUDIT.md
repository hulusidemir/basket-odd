# 📊 Sinyal Kalitesi ve Geçmiş Performans Denetim Raporu

**Tarih:** 8 Eylül 2026
**Kapsam:** 134 Arşivlenmiş Sinyal (95 Tekil Maç)
**Metodoloji:** Uzman Subagent Ekibi (Veri Denetçisi, İstatistikçi, Basketbol Uzmanı, Red Team) Tartışması ve Out-of-Sample (OOS) Doğrulama Mimarisi

---

## 1. Yönetici Özeti

Mevcut sinyal sisteminin (açılış ile canlı barem farkına dayalı ters yön bahisleri) tüm arşivi incelendiğinde, **%53.0 ham başarı oranı** (71 Başarılı / 63 Başarısız) bulunmuştur. 1.85-1.90 oranlı bahis piyasasının kasa avantajı (juice) göz önüne alındığında, bu seviye başabaş (breakeven) noktasının (~%52.6 - %54.0) altında kalarak **negatif beklenen getiriye (Negative EV)** işaret etmektedir.

PPM (Points Per Minute) eklentisi, maç temposunun yönünü ve şiddetini açıklamada başarılı bir göstergedir. "Tempo Ters" durumunun %94.7 oranında çıkması bir hata değil, sistemin zıt yön (mean-reversion) mantığıyla çalışmasının doğal sonucudur.

**En Önemli 3 Bulgu:**
1. **Rejim Kırılması (Regime Shift):** Barem 20 sayıdan fazla saptığında (Diff $\ge$ 20) veya gereken tempo mevcut ortalamadan %20'den fazla farklılaştığında ($|PPM| > \%20$), maç geçici bir dalgalanma değil yapısal bir kırılma (sakatlık, oyun stili değişikliği, faul problemi) yaşamaktadır. Bu anlarda "düşen bıçağı tutmaya" çalışmak başarıyı **%37.0**'ye düşürmektedir.
2. **Bağımsızlık İhlali (Clustering Bias):** Aynı maçtan gelen 2. ve 3. sinyallerin (Repeat Signals) başarı oranı **%41.0**'a inmektedir. Piyasaya inat ederek aynı maçta maliyet düşürme (averaging down) zararı katlamaktadır.
3. **Çeyrek Dinamikleri:** Q1 ve Q4'teki sinyallerin kazanma oranı %36-37 bandında kalırken, Q2'de %66.1'e çıkmaktadır. Q1 maçın temposunun henüz stabilize olmadığı gürültülü bir evreyken; Q4 uzatma (overtime) riski, taktik fauller ve süreyi eritme çabalarının sinyal modelini çökerttiği kaotik bir aşamadır.

**En Önemli 3 Risk:**
1. **P-Hacking ve Aşırı Uyum (Overfitting):** Elimizdeki verinin sadece 95 tekil maçtan oluşması; veriyi periyotlara veya dar PPM bantlarına bölerek "kârlı filtreler" bulma yanılsaması (Texas Sharpshooter Fallacy) yaratmaktadır.
2. **Kasa Avantajı Gerçeği:** Uygulanan hiçbir filtre geriye dönük (in-sample) uydurulduğu için henüz kanıtlanmış bir "edge" taşımaz. Komisyonlu oranlarda %53 başarı zarar yazdırır.
3. **Piyasanın Haklılığı:** Aşırı barem/tempo değişimlerinde piyasa her zaman doğru fiyatı çizer. Basit ortalamaya dönüş kuralı bu tür güçlü trendlerin önünde ezilir.

**Şimdi Ne Yapmalıyız?**
Üretim ortamındaki ham bahis alımı durdurulmalı; makro filtreler (İlk Sinyal, Diff < 20) sisteme uygulanıp, yeni toplanacak en az 150 maçlık "Shadow Mode" (Kör Doğrulama) serüveniyle kârlılık Out-of-Sample olarak ispatlanmalıdır.

---

## 2. Veri Kapsamı ve Güvenilirliği

* **Kayıt Sayıları:** 134 silinmemiş arşiv kaydı (Başarılı: 71, Başarısız: 63). Bekleyen/İade durumunda değerlendirilmeyen kayıt yoktur.
* **Tekil Maç Sayısı:** 134 sinyal **95 tekil maçtan** üretilmiştir. Bu maçlardan en az 9'unda her iki yönlü (ALT ve ÜST) birbiriyle çelişen sinyaller yakalanmıştır.
* **PPM Kapsaması:** 134 kaydın tamamında (%100) snapshot ve PPM verisi bulunmaktadır. Ancak "Tempo Uyum Analizi" tablosunda 133 kayıt (7 Uyumlu, 126 Ters) sınıflandırılmış; 1 sinyal nötr/geçersiz olarak kalmıştır.
* **Eksik Veri ve Seçim Yanlılığı (Bias):** Snapshot sürümleri arasında ciddi bir veri boşluğu yoktur, tüm gözlemler PPM kapsamasındadır (yeni dönem / cohort effect bulunmamaktadır). Ancak aynı maçlardan gelen 39 tekrar sinyali analize dahil edildiği için istatistiksel bağımsızlık ihlali (pseudoreplication) bulunmaktadır.

---

## 3. Mevcut Sistemin Temel Performansı

| Kırılım | Sinyal (n) | Başarı Oranı | Wilson %95 Güven Aralığı |
| :--- | :---: | :---: | :---: |
| **Tüm Sinyaller** | 134 | **%53.0** | %44.6 - %61.2 |
| **Maç + Yön (İlk Sinyal)** | 104 | **%56.7** | %47.1 - %65.8 |
| **Maç (İlk Sinyal)** | 95 | **%57.9** | %47.8 - %67.3 |
| **Yön: ALT** | 76 | %56.6 | %45.4 - %67.1 |
| **Yön: ÜST** | 58 | %48.3 | %35.9 - %60.8 |
| **Periyot: Q1** | 27 | %37.0 | %21.5 - %55.8 |
| **Periyot: Q2** | 56 | %66.1 | %53.0 - %77.1 |
| **Periyot: Q3** | 26 | %57.7 | %38.9 - %74.5 |
| **Periyot: Q4** | 25 | %36.0 | %20.2 - %55.5 |
| **Tekrar Sinyalleri (Repeat)** | 39 | **%41.0** | %27.1 - %56.6 |

---

## 4. PPM Analizi

PPM'in (Dakika Başına Sayı) eklenen snapshot değerleri üzerinden yapılan analiz, sistemin "Tempo Ters" (Opposed) çalıştığını teyit etmiştir (Sinyallerin %94.7'si). Bu durum bir hata değil; yükselen bir baremin ardından gelen ALT bahsinin, haliyle anlık yüksek PPM'in tersine pozisyon alması gerçeğidir.

**Gereken PPM Değişimi Yüzdesi (required_change_pct) Bantları:**

| Band (%) | N | Win % | CI (%95) | ALT (n/%) | ÜST (n/%) | Benzersiz Maç |
| :--- | :---: | :---: | :--- | :---: | :---: | :---: |
| **< -20%** | 16 | %37.5 | 18.5 - 61.4 | 16 / %37.5 | 0 / %0.0 | 14 |
| **-20% – -10%** | 24 | %62.5 | 42.7 - 78.8 | 24 / %62.5 | 0 / %0.0 | 21 |
| **-10% – 0%** | 34 | %64.7 | 47.9 - 78.5 | 31 / %64.5 | 3 / %66.7 | 32 |
| **0% – +10%** | 12 | %66.7 | 39.1 - 86.2 | 2 / %0.0 | 10 / %80.0 | 12 |
| **+10% – +20%** | 18 | %50.0 | 29.0 - 71.0 | 1 / %0.0 | 17 / %52.9 | 18 |
| **> +20%** | 30 | %36.7 | 21.9 - 54.5 | 2 / %100.0| 28 / %32.1 | 23 |

**Sonuçlar:**
* PPM tek başına skoru, süreyi ve canlı baremi ifade etse de; **ek bilgi sağlamaktadır.**
* Orta karar bir tempo düzeltmesi gereken bantlarda ($-20\%$ ile $+10\%$ arası) başarı **%64.3** iken; takımın karakterini değiştirecek aşırı ivme bekleyen uçlarda ($<-20\%$ ve $>+20\%$) sistem batmaktadır (**%37.0**).
* "Sinyalle Uyumlu" (Aligned) tempo etiketli maç sayısı yalnızca 7 olduğu için buradan çıkarılacak hiçbir istatistik güvenilir değildir.

---

## 5. Aday Kalite Artırma Kuralları

| Kural | Mantık | Geliştirme Örneklemi (In-Sample) Sonucu | Doğrulama Örneklemi (OOS) Sonucu | Kalan Sinyal (Coverage) | Başarı Oranı Değişimi (Lift) | Güven Aralığı | Risk | Karar |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| **Tekrar Sinyalleri Engelle** | Maç başına yalnız İlk Sinyal. Aynı maçta averaj düşürmekten kaçınma. | %41'lik kaybeden grup elenir. | Henüz test edilmedi. | 134'ten 95'e düşer. | %53.0 → %57.9 (+4.9%) | %47.8 - 67.3 | Sinyal hacmi azalır. | 🟢 **Üretime Aday** |
| **Ekstrem Barem Sapması Vetosu (Diff >= 20)** | Piyasaya kafa tutmamak. 20 sayılık hareket yapısal kırılmadır. | %37.0 olan 27 maç elenir. | Henüz test edilmedi. | ~107 sinyal kalır. | %53.0 → %57.0 (+4.0%) | %47.5 - 66.0 | Değerli kriz fırsatları kaçabilir. | 🟢 **Üretime Aday** |
| **Ekstrem PPM Vetosu (\|PPM\| > 20%)** | Takımlar yapısal kapasitelerinin %20 dışına birden fırlayamaz. | %37.0 olan uç bantlar elenir. | Henüz test edilmedi. | ~88 sinyal kalır. | %53.0 → %61.4 (+8.4%) | %50.5 - 71.2 | Sinyal hacminde sert düşüş. | 🟡 **Shadow Mode** |
| **Yalnız Q2 ve Q3** | Q1 şut gürültüsü, Q4 taktik faul/uzatma kaosu barındırır. | Başarı Q2+Q3'te %63.4. | Henüz test edilmedi. | 134'ten 82'ye düşer. | %53.0 → %63.4 (+10.4%)| %52.6 - 73.0 | Küçük örneklemde aşırı uyum (overfitting). | 🟡 **Shadow Mode** |

---

## 6. Red Team Değerlendirmesi

1. **Aşırı Uyum (Overfitting) & P-Hacking:** Dar PPM bantlarındaki başarılar (örneğin %0-10 arası 10 maçlık grupta %80 başarı), Texas Sharpshooter Fallacy'dir. Bu tür kurallar canlıda uygulandığında kesin olarak çökecektir.
2. **Kârlılık İllüzyonu:** %53.0 genel başarı oranı, komisyon düşüldükten (juice/vigorish) sonra negatif getiri sağlar. Geriye dönük %60 bulunan kurallar **kendi kuyruğunu ısıran in-sample yanılsamalardır**.
3. **Piyasanın Verimliliği:** Canlı barem 20 sayı kaydığında ya da PPM %20 saptığında piyasa yanılmıyor; sahada pivot sakatlanıyor veya maç antrenman havasına dönüyordur. Sistem bu "düşen bıçakları" tutmayı acilen bırakmalıdır.
4. **Q1 ve Q4 Tespiti:** Q4'teki felaket, Red Team tarafından onaylanmıştır (uzatma ihtimali ve taktik fauller ALT bahsini mekanik olarak bitirir). Ancak sırf %66 gösteriyor diye Q2'ye sarılmak narrativa yanılgısıdır (Narrative Fallacy).

---

## 7. Ajanlar Arası Tartışma Özeti

* **Uzlaşılan Noktalar:**
  1. Ekstrem sapmalarda (Diff >= 20 ve $|PPM| > 20\%$) piyasa ortalamaya dönmez; bu sinyaller otomatik filtrelenmelidir.
  2. Tekrar sinyalleri (Repeat Signals) serbestlik derecesini bozar ve zararı katlar; maç başına tek sinyal alınmalıdır.
  3. "Tempo Uyumlu" kümesi (N=7) hiçbir çıkarım yapılamayacak kadar küçüktür.
* **Reddedilen İddialar:**
  * "PPM %0-10 aralığında müthiş başarılı." -> Reddedildi (Örneklem yetersiz, p-hacking).
  * "Q2/Q3 Filtresi %60+ kesin kârlılık sağlar." -> Reddedildi (OOS testi yapılmadan kabul edilemez).
* **Uzlaşılamayan Noktalar:**
  * Basketbol uzmanı Q1'deki düşük başarının şut gürültüsü olduğunu ve Q2'deki düzelmenin taktiksel olduğunu savunurken; Red Team ve İstatistikçi bunun veri setine ait küçük örneklem (N=27, N=56) tesadüfü olma ihtimalinin yüksek olduğunda ısrarcıdır. Çözüm "Shadow Mode" takibidir.

---

## 8. Önceliklendirilmiş Öneriler

**P0: Riskli Gözlemleri Derhal Durdur**
* Diff $\ge$ 20 olan maçlar ve Repeat (2. ve sonraki) sinyaller doğrudan filtrelenmeli, Telegram/Dashboard üretimi durdurulmalıdır.

**P1: Risksiz Gözlem ve Raporlama**
* Q4 sinyallerinde kullanıcıyı "Uzatma (OT) ve Taktik Faul Riski" uyarısıyla görsel olarak bilgilendir, ancak kurala gömme.
* "Tempo Ters/Uyumlu" etiketini "Ortalamaya Dönüş Beklentisi" şeklinde tarafsızlaştır.

**P2: Shadow Mode (Gölge Modu) Testleri**
* Q2-Q3 dönemi, $|PPM| \le 20\%$ bantları arka planda veritabanına loglanmalı, ancak ana sinyal oluşturucu eşik olarak sunulmamalıdır.

**P3: Şimdilik Uygulanmayacaklar (Kanıt Yetersiz)**
* Spesifik (dar) PPM sınırları.

---

## 9. Deney Planı (Out-of-Sample Doğrulama)

* **Sabitlenen (Dondurulan) Hipotez:** Yalnız İlk Sinyal + $10 \le \text{Diff} < 20$ + $|PPM| \le 20\%$.
* **Toplanacak Bağımsız Gözlem:** Eski 134 maç tamamen rafa kaldırılır. Gerçek bahis alınmadan en az **150 yeni, bağımsız tekil maç** sinyali toplanır.
* **Başarı Kriteri:** Bu 150 maçta kazanma oranının %95 Güven Aralığı alt sınırı, 1.90 piyasa oranı için gereken başabaş noktasının (**%52.63**) üzerinde kalmalıdır (bu yaklaşık **%58 net win-rate** gerektirir).
* **Erken Çıkış (Kill Switch):** İlk 50 yeni OOS maçta başarı %52'nin altında kalırsa, geçmişte bulunan Q2 veya PPM başarılarının overfit olduğu kesinleşir; öneriler geri çekilir.

---

## 10. Son Karar

**Belirli makro PPM/Diff filtreleri ve tekrar sinyal engeli SHADOW MODE olarak izlenmeli, mevcut %53.0'lık ham stratejiyle gerçek bütçeli bahis alınmamalıdır.** Veri (95 maç) istatistiksel bir *edge* kanıtlamak için yetersizdir; sunulan hipotezler kesin bir kârlılık reçetesi değil, gelecekte toplanacak 150 maçlık OOS serüveninin deney parametreleridir.
