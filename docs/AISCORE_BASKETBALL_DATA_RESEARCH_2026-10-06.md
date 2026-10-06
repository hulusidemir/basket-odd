# AIScore basketbol verisine gerçek erişim — 6 Ekim 2026

Amaç yalnız ALT/ÜST yönünü daha doğru belirlemek için hangi basketbol verilerinin gerçekten alınabildiğini araştırmaktır. Önceki sinyal başarıları bu çalışmanın girdisi değildir. Bahis fiyatı veya getiri optimizasyonu yapılmadı. Çalışan servisler, DB, arşiv ve sinyal motoru değiştirilmedi.

## Denenen kaynaklar ve sonuç

Mevcut Scrapling/Patchright erişim ayarlarıyla, üretimden ayrı geçici profiller kullanılarak gerçek AIScore sayfalarına girildi. Doğrudan HTTP denemesi gerçek sayfa yerine erişim engeli içeriği verdi; yapılandırılmış tarayıcı erişimi çalıştı. Hiçbir kullanıcı hesabıyla giriş yapılmadı.

| Gerçek kontrol | Gelen veri | Gelmeyen veri |
| --- | --- | --- |
| Canlı Atlanta–Memphis, Detroit–Phoenix, Milwaukee–Minnesota | Oyuncu ve takım boxscore, FG/3PT/FT isabet ve deneme, hücum/savunma ribaundu, top kaybı, kişisel faul, olay akışı | Savunmacı uzaklığı ve kesin açık/contest şut etiketi |
| Canlı Quimsa–Atenas, Leonas–Real Esteli kadınlar | 2'lik/3'lük/serbest atış isabetleri ve arayüz faul sayaçları | Denemeler, oyuncu boxscore ve ayrıntılı olay akışı boş döndü |
| Tamamlanmış Kumamoto–Shinshu ve Slask–Aris | Ayrıntılı boxscore ve olay akışı | Canlı güncelleme bu tamamlanmış örneklerle kanıtlanmaz |

Bu örnekler bütün liglerin kalıcı kapsama garantisi değildir. Aynı lig içinde de alanların bulunması ve güncelliği maç bazında kontrol edilmelidir. Bulunan bir HTML alanındaki sıfır, eksik istatistik yerine kullanılamaz.

Milwaukee canlı kontrolünde devre skoru 48–60 iken ev sahibi FG 14/49, 3PT 10/31, FT 10/15, hücum ribaundu 9, top kaybı 7 idi. Buradan 2'likler 4/18 olarak hesaplanır. Skorun düşük olması hücum sayısının mutlaka düşük olduğunu göstermiyor: düşük 2'lik isabet ile şut hacmi ayrı gözlenebiliyor. Bir sonraki bölümün otomatik ÜST olacağı sonucuna da tek başına bu fotoğraftan varılmaz; sürdürülebilir verim ve kalan barem birlikte değerlendirilir.

## Çekme yöntemi

Maç state'i `window.$nuxt.$store.state.basketball` içinde. Patchright varsayılan isolated context bu state'i göremiyor; `isolated_context=False` gerekir.

Overview sayfasında `lineupData.lineup.homePlayerTotals.bkDetail` ve `awayPlayerTotals.bkDetail`, takımın `points`, `fieldGoals`, `threePoints`, `freeThrows`, `offensiveRebounds`, `defensiveRebounds`, `turnovers`, `personalFouls` alanlarını veriyor. `_tliveData.lives` periyot bazında olayları veriyor. Barem sayfasında boxscore ve timeline önceden yüklenmiş değildi.

Bu nedenle aynı barem sekmesinden şu public GET yolları denenip çalıştırıldı:

```text
https://api.aiscore.com/v1/m/api/match/lineups?match_id=ID&lang=2
https://api.aiscore.com/v1/m/api/match/tlive?match_id=ID&lang=2
https://api.aiscore.com/v1/m/api/match/total?match_id=ID&lang=2
https://api.aiscore.com/v1/m/api/match/h2h?match_id=ID&lang=2
```

Yanıtlar JSON değil protobuf. Sitenin yüklenmiş frontend paketindeki `Response` zarfı, `MatchLineup`, `TextLives`, `MatchStat` ve `HistoryMatches` şemaları kullanılarak çözülebiliyor. Araştırma okuyucusu şema modül numaralarını sabitlemek yerine ilgili API modülünün kaynak referanslarından çözüyor.

Sitenin GET yardımcısı `cache=1` taşıyan istekleri JavaScript LRU cache'inde 30 dakika tutabiliyor. Tekrarlayan canlı çekim bunun üzerinden yapılmamalı. `fetch_basketball_data()` doğrudan `fetch` ile iki public isteği paralel yapıyor, browser cache'i için `no-store` kullanıyor, sekiz saniyede istekleri iptal ediyor. Yeni sekme veya navigasyon yok. Üç ardışık gerçek çekimde olay sayısı 305 → 307 → 308 oldu; şut/ribaund/foul alanlarının da değiştiği görüldü.

Takım/sezon beklentisi için eski `/basketball/team` ve `/basketball/team/players_total` denemeleri boş döndü. Güncel yollar çalıştı:

```text
/v1/m/api/basketball/database/team/info?team_id=ID&lang=2
/v1/m/api/basketball/database/team/players/totals?team_id=ID&season_id=SEASON&lang=2
/v1/m/api/basketball/database/competition/info?comp_id=ID&lang=2
/v1/m/api/basketball/database/competition/teams/totals?comp_id=ID&season_id=SEASON&lang=2
```

Gerçek yanıtta 24 oyuncu ve 76 oyuncu istatistik grubu; explicit 25–26 sezon sorgusunda 30 takım ve beş takım ortalaması grubu geldi. H2H yanıtının ayrı home/away geçmişlerinde 18'er tamamlanmış maç bulundu. Bunlar eski bot sinyalleri değil, AIScore takım/lig verileridir. İstatistik grup ID'lerinin tümü henüz adlandırılmadı. Sezonun, normal sezon/preseason kapsamının ve güncel kadronun doğru seçimi ayrıca gerekir; eski kadronun tüm oyuncu ortalamaları toplanıp canlı hücum gücü sayılamaz.

## Canlı eşzamanlılık ve kullanılabilirlik

Önemli gerçek bulgu: skor, olay akışı ve boxscore aynı anda güncellenmiyor. Bir NBA okumasında skor 121–127 iken boxscore 121–126 idi. Diğerinde skor 97–97, boxscore 97–95 idi. Ayrı canlı kontrol sırasında skor 48–60 → 51–60 → 51–63 ilerledi, sayfadaki boxscore bir süre 48–60'ta kaldı.

Cache'siz public API çekimi verinin değiştiğini gösterdi; ancak üçünün de iki takım boxscore'u aynı anda barem sayfası skoruyla tam eşleşmedi. Dolayısıyla **çekilebilir olması her gözlemde sinyal anında kullanılabilir olması demek değildir**. Bu veriyle üretim kararı vermeden önce kimlik/skor/oyun zamanı ve provider güncelleme tutarlılığı korunmalıdır. API erişim zamanı provider güncelleme zamanı değildir; okuyucu `provider_freshness_verified=False` döndürür.

`aiscore_basketball_data.py` şu korumaları içerir:

- URL, Nuxt maç ID ve detailMatchId aynı maçı göstermeli.
- İsteğe bağlı `expected_score`, gerçek barem gözleminin skoruyla eşleşmeli.
- Boxscore puanı skorla ve `2 × FGM + 3PM + FTM` aritmetiğiyle eşleşmeli.
- Eksik denemeler yüzdelerden tahmin edilmez; NULL kalır.
- 2'lik denemesi FG denemesinden 3'lük denemesi çıkarılarak elde edilir.
- Arayüz faul sayacı ile toplam kişisel faul ayrı kalır; bonus durumu doğrulanmış varsayılmaz.
- Olayın `number` alanı her metinde doğrudan faul yapan/top kaybeden takım anlamına gelmeyebilir; bununla otomatik hücum sonu sayılmaz.

Şut mesafesi veya layup/dunk/jump-shot gibi türler bazı olay metinlerinde var. Açık şut, savunmacı uzaklığı ve güvenilir şut kalite puanı araştırılan veride yok. Metinden görülemeyen kalite uydurulmamalı.

## ALT/ÜST motoru için somut yaklaşım

Mevcut motor skor/dakika gözlemini gelecek sayı hızına taşıyor. Ayrıntılı veri olan maçlarda bunun yerine iki ayrı süreç tahmin edilmelidir: kalan hücum/şut hacmi ve bu hücumların sürdürülebilir verimi. Takımın isabet beklentisi AIScore sezon/son maç istatistikleriyle kurulabilir; canlı 2'lik/3'lük/FT isabeti bu beklentiye deneme sayısına göre yaklaştırılabilir. Şut ve hücum hacmi geçmişteki toplam sayıyla karıştırılmamalıdır.

İki takım için kalan beklenen sayı katkısı kabaca:

```text
2 × beklenen kalan 2PA × sürdürülebilir 2P isabeti
+ 3 × beklenen kalan 3PA × sürdürülebilir 3P isabeti
+ beklenen kalan FTA × sürdürülebilir FT isabeti
```

Faul/bonus, maç yakınlığı, skor farkı ve rotasyon bu kalan hacim tahminini değiştirir. Şut kalitesi gözlenmiyorsa gerçek ölçüm gibi eklenmez. Beklenen toplam, mevcut skorun üzerine eklenip canlı baremle karşılaştırılır. Bu yapı için sinyal yönü dışında kullanıcıya ek çıktı göndermek gerekmez.

Örneğin düşük skor + yüksek şut hacmi + düşük geçici isabet, kör ALT gerekçesi olmamalı. Yüksek skor + yüksek geçici üçlük isabeti, kör ÜST gerekçesi olmamalı. Gerçekten düşük hücum hacmi ve az faul/FT katkısı, ALT lehine daha farklı bir durumdur. Bu kararlar baremin kalan sürede istediği toplamla birlikte alınır.

Ayrıntılı veri olmayan maçlarda takımın AIScore son maç/lig beklentisi, çeyrek skorları, güvenilir ayrık son 2–3 ve önceki 3–5 dakikalık skor pencereleri, baremin gerektirdiği kalan sayı hızı, skor farkı ve gerçek maç formatı kullanılabilir. Pencereler örtüşmemeli; tek kısa seriden veya yalnız açılış hareketinden yön verilmemeli. Bu mod isabet ile hücum hızını ayıramadığı için aynı güven iddiasıyla sunulmamalıdır.

## Yapılan değişiklik ve doğrulama

Yeni okuyucu ve sekiz anlamlı doğrulama testi eklendi. Scoreboard/motor/kronoloji testleriyle birlikte **33 test geçti**; yeni Python dosyaları compileall geçti. Mevcut v5 yön kararı, DB ve Telegram akışı değişmedi. Bu çalışma yeni okuyucunun gerçek kaynak erişimini gösterir; yeni yön modelinin uygulanıp başarısının kanıtlandığı iddiası değildir. Bütün özel tarayıcı oturumları kapatıldı.
