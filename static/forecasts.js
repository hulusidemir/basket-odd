'use strict';
(() => {
  let scenarioLine = null;
  let requestId = 0;
  let historyRequestId = 0;
  let historyCursor = null;
  let nextHistoryCursor = null;
  const status = document.getElementById('status');
  const rows = document.getElementById('forecastRows');
  const number = value => Number(value).toLocaleString('tr-TR', {maximumFractionDigits: 1});
  const historyNumber = value => value === null || value === undefined || value === ''
    || !Number.isFinite(Number(value)) ? '—' : number(value);
  const outcomeLabels = {win: 'Doğru', loss: 'Yanlış', pending: 'Bekliyor', push: 'İade',
    no_direction: 'Yön yok', invalid: 'Değerlendirilemedi'};
  function historyDate(value) {
    const normalized = String(value || '').replace(' ', 'T');
    const date = new Date(normalized + (/(Z|[+-]\d{2}:\d{2})$/i.test(normalized) ? '' : 'Z'));
    return Number.isNaN(date.getTime()) ? '—' : date.toLocaleString('tr-TR', {
      timeZone: 'Europe/Istanbul', day: '2-digit', month: '2-digit', year: 'numeric',
      hour: '2-digit', minute: '2-digit'});
  }
  async function refreshHistory() {
    const currentRequest = ++historyRequestId;
    const historyStatus = document.getElementById('forecastHistoryStatus');
    const older = document.getElementById('olderForecastHistory');
    const newest = document.getElementById('newestForecastHistory');
    const update = document.getElementById('refreshForecastHistory');
    older.disabled = newest.disabled = update.disabled = true;
    try {
      const response = await fetch('/api/forecasts/history' + (historyCursor === null ? '' : '?before=' + historyCursor));
      if (!response.ok) throw new Error('Tahmin geçmişi alınamadı.');
      const data = await response.json();
      if (currentRequest !== historyRequestId) return;
      const summary = data.summary;
      document.getElementById('forecastSuccessRate').textContent = summary.success_rate === null
        ? '—' : '%' + number(summary.success_rate);
      document.getElementById('forecastWinLoss').textContent = number(summary.wins) + ' / ' + number(summary.losses);
      document.getElementById('forecastPending').textContent = number(summary.pending);
      document.getElementById('forecastCounts').textContent = number(summary.matches) + ' / ' + number(summary.forecast_count);
      for (const [direction, prefix] of [['ALT', 'forecastUnder'], ['ÜST', 'forecastOver']]) {
        const result = summary.by_direction[direction];
        document.getElementById(prefix + 'SuccessRate').textContent = result.success_rate === null
          ? '—' : '%' + number(result.success_rate);
        document.getElementById(prefix + 'Results').textContent = number(result.wins) + ' doğru / '
          + number(result.losses) + ' yanlış · ' + number(result.pending) + ' bekleyen';
      }
      document.getElementById('forecastSummaryStatus').textContent =
        'Başarı oranı maç başına ilk kayıtlı tahminin doğru / (doğru + yanlış) sonucudur. '
        + 'Bekleyen, iade (' + number(summary.pushes) + ') ve yönsüz (' + number(summary.no_direction) + ') kayıtlar orana katılmaz.'
        + (summary.invalid ? ' Değerlendirilemeyen ilk kayıt: ' + number(summary.invalid) + '.' : '');
      const fragment = document.createDocumentFragment();
      for (const item of data.items) {
        const row = document.createElement('tr');
        const forecast = item.forecast;
        const values = [historyDate(item.recorded_at),
          (item.match_name || '—') + ' / ' + (item.tournament || '—'),
          (item.score || '—') + ' / ' + (item.status || '—'),
          historyNumber(forecast.line), historyNumber(forecast.predicted_total), forecast.direction || '—',
          item.final_total === null ? '—' : (item.final_score || '—') + ' (' + historyNumber(item.final_total) + ')',
          (outcomeLabels[item.outcome] || '—') + (item.is_first ? ' · İlk' : '')];
        for (const value of values) {
          const cell = document.createElement('td');
          cell.textContent = value;
          cell.style.padding = '12px';
          row.append(cell);
        }
        fragment.append(row);
      }
      document.getElementById('forecastHistoryRows').replaceChildren(fragment);
      nextHistoryCursor = data.next_cursor;
      historyStatus.textContent = data.items.length
        ? data.items.length + ' kayıt gösteriliyor. İlk etiketli kayıtlar başarı özetine girer. Saatler Türkiye saatidir.'
        : 'Bu sayfada kayıt yok. Yeni tahminler gözlem anındaki değerleriyle burada saklanır.';
    } catch (error) {
      if (currentRequest === historyRequestId) {
        historyStatus.textContent = error.message;
        document.getElementById('forecastSummaryStatus').textContent = 'Başarı takibi güncellenemedi.';
      }
    } finally {
      if (currentRequest === historyRequestId) {
        older.disabled = nextHistoryCursor === null;
        newest.disabled = historyCursor === null;
        update.disabled = false;
      }
    }
  }
  async function refresh() {
    const currentRequest = ++requestId;
    try {
      const response = await fetch('/api/forecasts' + (scenarioLine === null ? '' : '?line=' + encodeURIComponent(scenarioLine)));
      if (!response.ok) throw new Error('Tahminler alınamadı.');
      const items = await response.json();
      if (currentRequest !== requestId) return;
      const fragment = document.createDocumentFragment();
      for (const item of items) {
        const row = document.createElement('tr');
        const forecast = item.forecast;
        const compared = item.scenario || forecast;
        const edge = Number(compared.signed_edge_points);
        const values = [item.match_name + ' / ' + item.tournament,
          item.score + ' / ' + item.status, item.bookmaker || '—',
          number(forecast.line), number(forecast.predicted_total),
          compared.direction + ' / ' + (edge > 0 ? '+' : '') + number(edge)
          + (item.scenario ? ' (' + number(item.scenario.line) + ' barem)' : '')];
        for (const value of values) {
          const cell = document.createElement('td');
          cell.textContent = value;
          cell.style.padding = '12px';
          row.append(cell);
        }
        fragment.append(row);
      }
      rows.replaceChildren(fragment);
      status.textContent = items.length ? items.length + ' güncel maç tahmini. Tahminin baremle eşitliği EŞİT olarak gösterilir.'
        : 'Son üç dakikada doğrulanmış maç/barem gözlemi yok. Tarama yeni veri aldığında tahminler burada görünür.';
    } catch (error) {
      if (currentRequest === requestId) status.textContent = error.message;
    }
  }
  document.getElementById('scenarioForm').addEventListener('submit', event => {
    event.preventDefault();
    const input = document.getElementById('scenarioLine');
    if (!input.value.trim()) { scenarioLine = null; refresh(); return; }
    const line = Number(input.value);
    if (!Number.isFinite(line) || line <= 0 || line > 1000) { status.textContent = 'Geçerli bir barem girin.'; return; }
    scenarioLine = line;
    refresh();
  });
  document.getElementById('resetScenario').addEventListener('click', () => {
    scenarioLine = null;
    document.getElementById('scenarioLine').value = '';
    refresh();
  });
  document.getElementById('refreshForecastHistory').addEventListener('click', refreshHistory);
  document.getElementById('newestForecastHistory').addEventListener('click', () => {
    historyCursor = null;
    refreshHistory();
  });
  document.getElementById('olderForecastHistory').addEventListener('click', () => {
    if (nextHistoryCursor === null) return;
    historyCursor = nextHistoryCursor;
    refreshHistory();
  });
  refresh();
  refreshHistory();
  setInterval(refresh, 10000);
  setInterval(() => { if (!document.hidden) refreshHistory(); }, 60000);
})();
