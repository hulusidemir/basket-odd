# Known Issues

- AIScore üçüncü taraf bir kaynaktır; mobil DOM değişirse selector'lar yeniden
  uyarlanmalıdır. Scraper doğrulanamayan sıfır maç durumunu sağlıklı boş liste
  kabul etmez.
- Proxy/Tor erişimi çalışmıyorsa tarama başarısız olur; ardışık hata alarmı bunu
  Telegram üzerinden bir kez bildirir ve sağlıklı çevrimden sonra yeniden kurulur.
- Biten maç kontrolü proxy kararlılığı için sıralı çalışır. Aktif maç sayısı
  yüksekse dashboard isteğinin tamamlanması birkaç dakika sürebilir.
