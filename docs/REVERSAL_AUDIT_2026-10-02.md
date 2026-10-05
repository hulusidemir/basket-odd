# Direction/Reversal ve Quality v2 veri denetimi — 2 Ekim 2026

## Karar

Direction/Reversal Layer ve Quality v2 üretime alınmadı. Mevcut veride ALT→ÜST
ve ÜST→ALT kararını güvenle doğrulayacak sayıda, sinyal anında gözlenebilir
tersine dönüş olayı yok. Bu nedenle canlı yön, Quality v1, Telegram, şema ve
arşiv davranışı değişmedi. Yeni sistem devreye girmediği için eski sinyaller
silinmedi; ölçüm dönemi sıfırlanmadı.

## Kaynak ve yöntem

- Üretim kodu: `live_signals.py`, `pace_calculator.py`, `signal_quality.py`,
  `match_state.py`, `main.py`, `db.py`, `dashboard.py`.
- SQLite salt okunur bağlantıyla incelendi. 1.007 sinyalin 1.003'ü sonuçlanmış;
  52.137 canlı snapshot, 1.190 ayrı maç için saklanmış. Sinyal verisi
  18 Eylül–1 Ekim, snapshot verisi 18 Eylül–1 Ekim 2026 tarihli.
- İstekte adı geçen `silinen-sinyaller(2).csv` çalışma ağacında, ek klasöründe
  ve olağan kullanıcı belge/indirme klasörlerinde bulunamadı. CSV katkısı
  olmadan mevcut DB kullanıldı.
- v5/v1 ile karşılaştırılabilir grup, `quality_version='v1'` olan 544
  sonuçlanmış sinyal/458 maçtır (26 Eylül–1 Ekim). Daha eski 459 sonuçlanmış
  sinyalin Quality v1 kaydı yoktur; onları v5/v1 karşılaştırmasına katmak
  sürüm karışıklığı yaratır. İlgili motor/Quality değişiklikleri 26 Eylül
  commit'lerinde (`3c3c743`, `409dc1d`) görülür.
- Her sinyal için yalnız `recorded_at <= alerted_at` snapshot'ları kullanıldı.
  Son nokta kayıtlı sinyal skoru ve oyun saatidir. Yakın pencere çıpası sinyalden
  120–180 saniye önce, önceki pencere çıpası yakın çıpadan 180–300 saniye
  önce aranır. Böylece aralıklar `[eski, yakın]` ve `[yakın, sinyal]`
  kesişmez. Saat geri sarımı, skor düşüşü ve sinyalden ileri veri
  `chronological_snapshots` ile elenir. Eksik çıpa için tempo üretilmez.
- Sinyal anında kullanılan özellikler: pregame (varsa prematch, yoksa opening),
  sinyal skoru/saat, live total, Fair Total, Quality v1 ve sinyal sayısıdır.
  Final toplamı yalnız sonradan doğruluk ve kalan maç temposu ölçümünde
  kullanıldı.

## Mevcut motor ve veri kapsaması

Future Pace v5, pregame toplamını 40/48 dakikalık normal süreye bölerek prior
PPM üretir. Tüm maç, son 2 dakika, son 5 dakika ve mevcut çeyrek hızlarını
prior'a yaklaştırır. Bu pencereler birbirine çakışır. Oluşan future pace
bandı live piyasanın gerektirdiği kalan süre PPM'iyle karşılaştırılır; kenar,
oyun saati, volatilite ve büyük repricing kontrolleri sonrası RAW ALT/ÜST/PAS
verilir. Prematch ve opening DB'de ayrı alanlardır. 1.007 sinyalin 809'unda
prematch vardır; 433'ünde opening'den farklıdır. v1 grubunda 219/544 farklıdır.
Mevcut v5 zaten prematch'i öncelikli prior olarak kullanır.

Tüm sonuçlanmış 1.003 sinyalin 555'inde (%55,3), v5/v1 grubundaki 544
sinyalin 289'unda (%53,1) iki ayrı pencere kurulabilir. v5/v1 dönemindeki
289 sinyal 258 ayrı maça aittir. Bu kapsam eksikliği sinyal anında gözlenmeyen
bir pencereyi geriye dönük doldurmaya izin vermez.

| Grup | Başarılı | Başarısız | Başarı |
| --- | ---: | ---: | ---: |
| Tüm sonuçlanmış ALT | 394 | 268 | %59,5 |
| Tüm sonuçlanmış ÜST | 168 | 173 | %49,3 |
| v5/v1 ALT | 220 | 137 | %61,6 |
| v5/v1 ÜST | 95 | 92 | %50,8 |

Tüm sonuçlanmış ALT kayıplarında kalan maç PPM'i, sinyal anındaki toplam maç
PPM'inden medyan **0,994 sayı/dakika daha yüksek** (267/268 kayıpta artış).
ÜST kayıplarında medyan **0,966 sayı/dakika daha düşük** (173/173 kayıpta
düşüş). Bu, finalden hesaplanan sonuç sonrası tanımdır; canlı FLIP kanıtı
değildir. İki ayrık pencere kurulabilen v5/v1 ALT kayıplarında sinyal anındaki
medyan `recent_ppm - previous_ppm` **−1,098**, ALT başarılarında **−1,245**.
ÜST kayıplarında **+0,931**, ÜST başarılarında **+1,866**. Kayıpların tipik
anlık trendi dahi önerilen ters yöne güçlü işaret vermiyor.

v5/v1 döneminde medyan prior/live hareketi ve Fair Total edge, kazanan ve
kaybedeni açık ayırmıyor: ALT'de market hareketi sırasıyla −%4,6 / −%4,8,
Fair edge −7,6 / −8,3 sayı; ÜST'te +%7,4 / +%6,3, Fair edge +8,7 / +7,65
sayı. Sinyal anındaki tüm maç projeksiyon edge'i de ALT'de −9,53 / −10,98,
ÜST'te +10,27 / +10,06 sayıdır. Bu değerler yeniden yön tayini için tek
başına güvenilir eşik sunmuyor.

Initial opening ile live barem arasındaki medyan puan farkı ALT başarı/kayıpta
−7/−7, ÜST başarı/kayıpta +11/+10'dur. `opening_ppm - current_ppm` aynı
sırayla +0,440/+0,464 ve −0,538/−0,513'tür. Medyan geçen süre ALT'de
16,23/15,97, ÜST'te 18,60/17,37 dakikadır; dört grubun medyan
`signal_count` değeri 1'dir. ALT başarı/kayıplarının Q2/Q3/Q4 dağılımı
142/48/30 ve 91/26/20; ÜST için 56/32/7 ve 65/22/5'tir. Bu basit
ayrımlar da FLIP için yeterli kanıt sağlamaz.

## Zamana göre ayrılmış değerlendirme

v5/v1 kayıtları 27 Eylül öncesi ve sonrası olarak ayrıldı; aynı maçın iki
tarafa düşmediği doğrulandı. İlk dönem 164 sinyal/140 maç, sonraki dönem
380 sinyal/318 maçtır. Bu yalnız **bir günlük** ilk dönem içerdiğinden yeni
bir kuralı eğitmek için zayıftır. Aşağıdaki konservatif aday ölçüm amaçlıdır:
ALT için düşük mevcut tempo + `recent - previous >= 0,8 PPM` + en az %7
aşağı repricing + yakın temponun prior'a yaklaşması; ÜST için simetriği.
Tek bir faktör FLIP vermez. İlk ve sonraki dönemde bu dört koşulu aynı anda
karşılayan **0** sinyal vardır. Market koşulu çıkarıldığında ilk dönemde
yalnız 2, sonraki dönemde yalnız 3 aday kalır; ilk iki adayın ikisi de RAW
yönüyle kazanmıştır. Bu küçük örnek FLIP doğrulaması değildir.

| Sonraki dönem (27 Eylül–1 Ekim) | Sonuç |
| --- | ---: |
| RAW v5 | 211/380 = %55,5 |
| RAW ALT | 156/258 = %60,5 |
| RAW ÜST | 55/122 = %45,1 |
| Benzersiz maç+yön | 181/335 = %54,0 |
| Konservatif aday KEEP / FLIP / PAS | 380 / 0 / 0 |
| FLIP başarısı | Ölçülemez (N=0) |
| Quality v1 ROC-AUC | 0,475 |
| Quality v2 ROC-AUC | Ölçülemez; v2 geliştirilmedi |

Quality v1 sonraki dönem skor bantları sırasıyla 0–39: 113/200 (%56,5),
40–54: 46/86 (%53,5), 55–69: 41/73 (%56,2), 70–84: 10/18 (%55,6),
85–100: 1/3 (%33,3). Başarı skora göre monoton artmıyor; son iki bant küçük.
İlk dönemde v1 AUC 0,590 idi. `market_move` faktörü −60…+20 aralığında ve
standart sapması 26,72 puan; `fair_edge` 8,99, `pace_support` 6,77,
`game_state` 2,63. `data_quality` tüm v1 kayıtlarında 10 puandır. Mevcut v1
skoru hareket faktörüne aşırı duyarlı ve sonraki dönemde sıralama gücü
göstermiyor.

## Güvenli sonraki ölçüm için gerekli sinyal anı verisi

Yeni scraping/API çağrısı gerekmez. Her yeni sinyal oluştuğunda, ileride
geriye dönük veri seçimi değişmesin diye aşağıdaki **türetilmiş** alanlar
sinyal anı kayıtlarına dondurulmalıdır:

1. Yakın ve önceki **ayrık** pencerelerin başlangıç/bitiş skorları ve oyun
   saniyeleri, süreleri, atılan sayıları, PPM'leri ve eksik pencere nedeni.
2. Kaynak gözleminin gerçek `market_captured_at` zamanı, DB'ye yazılma zamanı,
   gözlem yaşı, saat/skor düzeltme bayrakları ve 40/48 dakika formatı.
3. Pregame prior'ın hangi alandan geldiği; initial opening, latest prematch,
   live total ve pregame/live hareketi. Son 2–5 dakikanın live total değişimi
   varsa aynı snapshot geçmişinden türetilmeli.
4. RAW v5 yönü ve bandı, current/required future PPM, Fair Total edge,
   projection edge, oyun yüzdesi ve `signal_count`.

Bu alanların hesaplanamadığı sinyalde eksik değer saklanmalı. Yeterli dönem ve
tersine dönüş adayı biriktiğinde maç bazlı kronolojik walk-forward sınamasıyla
ayrı ALT/ÜST eşikleri ve Quality v2 yeniden denenebilir. Mevcut snapshot
tablosu bu çalışma için korunmalıdır.

## Ölçüm sıfırlama

Yeni layer ve Quality v2 kurulup sınanmadığından koşullu temizlik aşamasına
geçilmedi. DB backup/CSV export alınmadı, hiçbir tabloya DELETE uygulanmadı,
**0 eski sinyal silindi**. Yeni ölçüm dönemi **hazır değil**.

Denetim sonrası mevcut tam test paketi: **271 geçti, 0 başarısız**
(`venv/bin/python -m pytest -q`). Python üretim kodu değiştirilmediği için
`compileall` gerekmiyordu.

Sonraki çalışma notu (2 Ekim): Bu denetimde önerilen sinyal anı feature
toplaması `reversal_features.py` ile ileriye dönük kuruldu. Yukarıdaki
Direction/Quality v2 ve ölçüm sıfırlama kararı değişmedi; eski sonuçlara
feature backfill yapılmadı.
