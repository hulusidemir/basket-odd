// Presentation only: all signal calculations come from the live payload or its frozen snapshot.
  const hasMetric = value => value !== null && value !== undefined && value !== '' && Number.isFinite(Number(value));
  const ppmMetric = (value, digits=2) => hasMetric(value) ? Number(value).toLocaleString('tr-TR', {minimumFractionDigits:digits, maximumFractionDigits:digits}) : '–';
  const ppmChange = value => hasMetric(value) ? `(${Number(value) > 0 ? '+' : Number(value) < 0 ? '−' : ''}%${ppmMetric(Math.abs(Number(value)), 1)})` : '';
  function resultClass(value) {
    if (value === 'Başarılı') return 'success';
    if (value === 'Başarısız') return 'failed';
    return '';
  }

  function historyVerdict(match, finalTotal) {
    const verdict = match.verdict;
    if (!verdict) return '';
    const tone = ['success', 'failed'].includes(verdict.tone) ? verdict.tone : '';
    return `<span class="history-verdict ${tone}"><strong>${fmt(finalTotal)} ${esc(verdict.relation)} ${fmt(match.live)}</strong> · ${esc(verdict.label)}${verdict.margin ? ` (${fmt(verdict.margin)} sayı fark)` : ''}</span>`;
  }

  function historyTeamCard(team) {
    const summary = team.summary || {};
    const successRate = hasMetric(summary.success_rate) ? `${ppmMetric(summary.success_rate, 0)}%` : '–';
    const matches = Array.isArray(team.matches) ? team.matches.slice(0, 5) : [];
    const matchRows = matches.length ? matches.map(match => {
      const direction = String(match.direction || '–').toLocaleUpperCase('tr-TR').replace('UST', 'ÜST');
      const directionClass = direction === 'ALT' ? 'alt' : 'ust';
      const finalScore = String(match.final_score || '').trim();
      const total = match.final_total;
      const change = Number(match.barem_change);
      const signedChange = hasMetric(match.barem_change) ? `${change > 0 ? '+' : ''}${change.toFixed(1)}` : '–';
      const historyUrl = desktopMatchUrl(match.url);
      const rowOpen = historyUrl ? `<a class="modal-history-row history-match-link" href="${esc(historyUrl)}" target="_blank" rel="noopener noreferrer" title="Maç detayını aç">` : '<div class="modal-history-row">';
      const rowClose = historyUrl ? '</a>' : '</div>';
      return `${rowOpen}
        <div class="history-row-head">
          <div class="history-match-title"><strong class="history-match-name">${esc(match.match_name || 'Maç')}</strong>${historyUrl ? '<span class="history-open-label">Maç detayını aç ↗</span>' : ''}</div>
          <div class="history-badges">
            <span class="direction-pill ${directionClass}">${esc(direction)} ${fmt(match.live)}</span>
            <span class="result-pill ${resultClass(match.result)}">${esc(match.result || 'Sonuç bekliyor')}</span>
          </div>
        </div>
        <div class="history-facts">
          <div><span>Barem hareketi</span><strong>${fmt(match.opening)} → ${fmt(match.live)} <small>(${signedChange})</small></strong></div>
          <div><span>Final skor</span><strong>${esc(finalScore || 'Final skor yok')}</strong>${Number.isFinite(total) ? `<small>Toplam ${fmt(total)}</small>` : ''}</div>
        </div>
        ${historyVerdict(match, total)}
      ${rowClose}`;
    }).join('') : '<div class="modal-history-empty">Henüz sonuçlandırılmış geçmiş sinyal yok.</div>';
    return `<article class="modal-team-card">
      <div class="modal-team-heading"><span>${esc(team.role || 'Takım')}</span><strong>${esc(team.name || 'Takım ayrıştırılamadı')}</strong></div>
      <div class="modal-team-stats">
        <div><span>Geçmiş sinyal</span><strong>${Number(summary.total || 0)}</strong></div>
        <div><span>Başarılı</span><strong class="positive-text">${Number(summary.successful || 0)}</strong></div>
        <div><span>Başarısız</span><strong class="negative-text">${Number(summary.failed || 0)}</strong></div>
        <div><span>Başarı oranı</span><strong>${successRate}</strong></div>
      </div>
      <details class="modal-history-details"><summary>Son ${matches.length} sinyalin ayrıntıları</summary><div class="modal-history-list">${matchRows}</div></details>
    </article>`;
  }

  function ppmComparisonSection(alert) {
    const comparison = alert.ppm_comparison || {};
    const metric = ppmMetric;
    const remainingTime = comparison.remaining_time || '–';
    const changePct = comparison.required_change_pct;
    const hasChangePct = changePct !== null && changePct !== undefined && changePct !== '' && Number.isFinite(Number(changePct));
    const changeLabel = ppmChange(changePct);
    const changeHint = hasChangePct
      ? Number(changePct) > 0 ? 'Bareme ulaşmak için mevcut temponun bu oranda artması gerekiyor.'
        : Number(changePct) < 0 ? 'Gereken tempo mevcut ortalamadan bu oranda daha düşük.'
          : 'Gereken tempo mevcut ortalamayla aynı düzeyde.'
      : 'Yüzdelik fark için sıfırdan büyük mevcut tempo ve kalan süre gerekir.';
    const status = ['above', 'below', 'level', 'reached', 'exceeded', 'ended'].includes(comparison.status) ? comparison.status : 'unavailable';
    const relationTone = ['aligned', 'opposed'].includes(comparison.signal_relation_tone) ? comparison.signal_relation_tone : '';
    const signalRelation = comparison.signal_relation
      ? `<span class="ppm-signal-relation ${relationTone}">${esc(comparison.signal_relation)}</span>` : '';
    return `<section class="ppm-comparison" aria-labelledby="ppmComparisonTitle">
      <div class="ppm-comparison-heading"><h3 id="ppmComparisonTitle"><svg viewBox="0 0 24 24" aria-hidden="true"><path d="M2 12h5l3-7 4 14 3-7h5"/></svg> PPM</h3><span>Sinyal anı · sayı/dk</span></div>
      <div class="ppm-comparison-stats">
        <div><span>Bareme kalan sayı</span><strong>${metric(comparison.remaining_points, 1)}</strong></div>
        <div><span>Kalan süre</span><strong>${remainingTime}<small> dk:sn</small></strong></div>
        <div><span>Gereken PPM</span><strong class="ppm-required-value">${metric(comparison.required_ppm)}<small class="ppm-required-change" title="${esc(changeHint)}">${changeLabel}</small></strong></div>
        <div><span>Mevcut PPM</span><strong>${metric(comparison.current_ppm)}</strong></div>
      </div>
      <div class="ppm-comparison-comment ${status}"><div class="ppm-verdict-heading"><strong>${esc(comparison.heading || '–')}</strong>${signalRelation}</div><p>${esc(comparison.comment || 'Bu sinyal için PPM karşılaştırması bulunmuyor.')}</p></div>
    </section>`;
  }

  function openingLine(alert) {
    return `<span class="opening-line">${fmt(alert.opening)}<small class="opening-ppm">${hasMetric(alert.opening_ppm) ? ` (${ppmMetric(alert.opening_ppm)} PPM)` : ''}</small></span>`;
  }

  function ppmFlow(alert, handler='openSignalModal') {
    const comparison = alert.ppm_comparison || {};
    return `<button type="button" class="signal-modal-trigger ppm-modal-trigger ${hasMetric(alert.pace_ppm) ? '' : 'unavailable'}" onclick="${handler}(event, ${Number(alert.id)})" title="Tempo detayını aç" aria-label="Mevcut ${ppmMetric(alert.pace_ppm)}, gereken ${ppmMetric(comparison.required_ppm)} PPM. Tempo detayını aç"><span class="ppm-current">${ppmMetric(alert.pace_ppm)}</span><span class="ppm-flow-arrow" aria-hidden="true">→</span><span class="ppm-target">${ppmMetric(comparison.required_ppm)}</span><small class="ppm-flow-change">${ppmChange(comparison.required_change_pct)}</small></button>`;
  }

  function frozenListMarkers(alert) {
    return (alert.list_markers || []).map(marker => `<span class="marker ${marker.type === 'black' ? 'list-black-marker' : 'list-white-marker'}" title="${esc(marker.title)}">●</span>`).join('');
  }

  function savedStatusLabels(alert) {
    const labels = [['bet_placed', 'Oynandı'], ['followed', 'Takip'], ['ignored', 'Gözardı'], ['upcoming_followed', 'Ön takip']];
    return labels.filter(([key]) => alert[key]).map(([, label]) => `<span class="saved-status">${label}</span>`).join('');
  }
