# Known Issues

- AIScore üçüncü taraf bir kaynaktır; mobil DOM değişirse selector'lar yeniden
  uyarlanmalıdır. Scraper doğrulanamayan sıfır maç durumunu sağlıklı boş liste
  kabul etmez.
- Proxy/Tor erişimi çalışmıyorsa tarama başarısız olur; ardışık hata alarmı bunu
  Telegram üzerinden bir kez bildirir ve sağlıklı çevrimden sonra yeniden kurulur.
- Eski SQLite dosyalarında artık kullanılmayan model sütunları veya
  `signal_trials` tablosu bulunabilir. Bunlar yalnız tarihsel veri olarak kalır.
