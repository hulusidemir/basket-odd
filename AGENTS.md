# Repository Guidelines

## Proje Özeti

Bu proje AIScore için Python/Flask tabanlı bir basketbol odds takip sistemidir. Canlı ve yaklaşan maç toplam sayı baremlerini çeker, açılış-canlı barem anomalilerini tespit eder, sinyalleri SQLite'a kaydeder, Telegram uyarısı gönderir ve dashboard üzerinden inceleme/silme/sonuçlandırma/etiketleme araçları sunar.

## Codex Çalışma Kuralları

Yeni bir Codex oturumu başladığında önce `AGENTS.md`, `docs/PROJECT_MAP.md`, `docs/CURRENT_STATE.md` ve `docs/DECISIONS.md` dosyalarını oku. Kullanıcı açıkça istemedikçe tüm projeyi baştan sona tarama. Görevle ilgili dosyaları `PROJECT_MAP.md` üzerinden belirle ve sadece gerekli dosyaları incele.

Kod dosyalarına dokunmadan önce mevcut çalışma ağacını kontrol et. Kullanıcıya ait değişiklikleri geri alma. Hassas veri, API anahtarı, chat ID, yerel DB içeriği veya kişisel bilgi yazma. Runtime dosyalarını (`basketball.db`, loglar, `venv/`, `__pycache__/`) kaynak gibi ele alma.

## Önce Okunacak Dosyalar

Normal görevlerde sırayla şunları oku:

1. `docs/PROJECT_MAP.md`
2. `docs/CURRENT_STATE.md`
3. `docs/DECISIONS.md`
4. `PROJECT_MAP.md` içinde işaret edilen görevle ilgili dosyalar

Kurulum ayrıntıları ve ortam değişkeni adları için `README.md` kullanılabilir.

## Çalıştırma Komutları

```bash
source venv/bin/activate
python main.py
python run.py
```

`main.py` canlı takip botunu çalıştırır. `run.py` Flask dashboard'u ve arka plan zamanlanmış kontrollerini başlatır. Dashboard portu `DASHBOARD_PORT` değişkeninden okunur, yoksa `5151` kullanılır.

## Test Komutları

```bash
python -m pytest -q
python -m unittest discover -s tests -v
python -m compileall -q .
```

`pytest` kuruluysa onu kullan. Mevcut testler için `unittest` güvenilir yedektir. Python import veya sözdizimi değişikliklerinden sonra `compileall` çalıştır.

## Kod Stili

Python kodunda 4 boşluk girinti, `snake_case` isimlendirme ve küçük yardımcı fonksiyonlar kullan. DB işleri `db.py`, scraper mantığı scraper modülleri, sinyal matematiği sinyal modülleri, dashboard veri şekillendirme `dashboard.py` içinde kalmalı. Frontend kodu çoğunlukla `templates/` içinde inline; UI değişikliklerini yerel ve mevcut class yapısıyla uyumlu tut.

## Değişiklik Sonrası Kontroller

Değişiklikten sonra en küçük ilgili test setini ve Python düzenlemelerinde `compileall` komutunu çalıştır. UI değişikliklerinde ilgili template ve route'ları manuel kontrol et. DB değişikliklerinde migration etkisini dokümante et ve eski SQLite dosyalarının güvenli şekilde çalıştığından emin ol.
