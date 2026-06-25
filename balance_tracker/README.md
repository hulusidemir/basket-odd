# Bakiye Takip

Mevcut basket-odd uygulamasına `/balance-tracker/` rotasıyla bağlanabilen bahis bakiye takip modülü.

Ana servis `run.py` ile çalışıyorsa:

```text
http://localhost:5151/balance-tracker/
```

Modül bağımsız olarak da çalıştırılabilir.

## Çalıştırma

```bash
python -m balance_tracker.app
```

Varsayılan adres:

```text
http://localhost:5161
```

## Veri

Modül kendi SQLite dosyasını kullanır:

```text
balance_tracker/balance_tracker.sqlite3
```

Farklı bir dosya kullanmak için:

```bash
BALANCE_TRACKER_DB=/path/to/balance_tracker.sqlite3 python -m balance_tracker.app
```

Port değiştirmek için:

```bash
BALANCE_TRACKER_PORT=5162 python -m balance_tracker.app
```
