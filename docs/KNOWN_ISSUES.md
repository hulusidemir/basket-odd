# Known Issues

Bu dosya, sonraki Codex oturumlarında hızlı devam etmek için kaydedilmiş mevcut risk ve hata listesidir. Yeni oturumda önce `AGENTS.md`, `docs/PROJECT_MAP.md`, `docs/CURRENT_STATE.md`, `docs/DECISIONS.md` ve ardından bu dosya okunmalıdır.

## Öncelikli Hatalar

### 1. Telegram gönderimi başarısız olsa bile sinyal gönderildi sanılıyor

- İlgili dosyalar: `main.py`, `notifier.py`
- Noktalar: `main.py` içinde alert DB'ye kaydedildikten sonra Telegram gönderiliyor; `notifier.py` chat bazlı Telegram hatalarını loglayıp boş sonuç dönebiliyor.
- Etki: Telegram'a gitmeyen sinyaller loglarda `telegram=sent` gibi görünebilir.
- Düzeltme fikri: `send_alert()` dönüşünü kontrol et; en az bir chat'e gönderilemediyse log metnini doğru yaz veya DB'de Telegram durumunu ayrı takip et.

### 2. Silinen maç ekranında final olmayan skordan sonuç hesaplanabiliyor

- İlgili dosyalar: `dashboard.py`, `finished_match_service.py`
- Noktalar: `_apply_current_deleted_result()` skor alanından sonuç hesaplıyor, ancak status final mi diye kontrol etmiyor.
- Etki: Devam eden veya ara skor taşıyan silinen maç `Başarılı/Başarısız` gibi görünebilir.
- Düzeltme fikri: Sonuç hesaplamasını yalnızca final status veya güvenilir finished-match servisinden gelen final skor için uygula.

### 3. DB açılışında eski profil kolonlarını düşürme denemesi riskli

- İlgili dosya: `db.py`
- Noktalar: `init()` içinde eski profil indeksleri ve kolonları drop edilmeye çalışılıyor; hata olursa sessiz geçiliyor.
- Etki: Eski DB'lerde hangi alanın temizlendiği belirsiz kalabilir; geri dönüşsüz tarihsel alan kaybı olabilir.
- Düzeltme fikri: Bu migration'ı açıkça dokümante et, gerekirse tek seferlik/manuel migration haline getir veya log ekle.

### 4. Geçersiz yön sessizce ALT'a düşüyor

- İlgili dosya: `signal_analysis.py`
- Nokta: `_normalize_direction()` `ALT/ÜST` dışındaki değerleri `ALT` kabul ediyor.
- Etki: Boş veya bozuk direction verisi yanlış analiz/backtest yönü üretebilir.
- Düzeltme fikri: Geçersiz yön için `""` veya güvenli hata durumu dön; çağıran yerlerde explicit fallback kullan.

### 5. README dashboard portu yanlış

- İlgili dosyalar: `README.md`, `run.py`, `AGENTS.md`
- Nokta: `run.py` varsayılan portu `5151`; README `localhost:5050` diyor.
- Etki: Kurulum yapan kişi yanlış adrese gider.
- Düzeltme fikri: README'i `http://localhost:5151` veya `DASHBOARD_PORT` bilgisini gösterecek şekilde güncelle.

### 6. Scraper kırılganlığı yüksek

- İlgili dosyalar: `aiscore_scraper.py`, `upcoming_scraper.py`
- Noktalar: AIScore DOM'una bağımlılık, sabit beklemeler, bazı hataların sessiz geçilmesi.
- Etki: AIScore değiştiğinde sinyal akışı sessizce azalabilir veya durabilir.
- Düzeltme fikri: Kritik parse başarısızlıklarında daha görünür log, debug snapshot ve selector fallback stratejileri ekle.

### 7. Test kapsamı kritik canlı akışları kapatmıyor

- İlgili klasör: `tests/`
- Eksik alanlar: Telegram gönderim başarısızlığı, final olmayan skorla sonuç hesaplama, DB migration davranışı, finished-match uçtan uca akışı.
- Etki: Operasyonel hatalar testlerden kaçabilir.
- Düzeltme fikri: Önce Telegram ve silinen maç sonuçlandırma için küçük unit testler ekle.

## Evde Devam İçin Önerilen Sıra

1. README port düzeltmesiyle hızlı başlangıç yap.
2. Telegram gönderim durumunu doğru logla/test et.
3. Silinen maç sonucu yalnızca final skorla hesaplanacak şekilde düzelt.
4. Direction normalize davranışını güvenli hale getir.
5. DB migration riskini netleştir.
6. Scraper gözlemlenebilirliğini artır.
