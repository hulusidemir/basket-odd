// Presentation only: all signal calculations come from the live payload or its frozen snapshot.
  const hasMetric = value => value !== null && value !== undefined && value !== '' && Number.isFinite(Number(value));
  const ppmMetric = (value, digits=2) => hasMetric(value) ? Number(value).toLocaleString('tr-TR', {minimumFractionDigits:digits, maximumFractionDigits:digits}) : '–';
  const ppmChange = value => hasMetric(value) ? `(${Number(value) > 0 ? '+' : Number(value) < 0 ? '−' : ''}%${ppmMetric(Math.abs(Number(value)), 1)})` : '';
  const ppmChangeTone = value => { if (!hasMetric(value)) return ''; const n = Number(value); if (n > 0) return 'ppm-change-positive'; if (n < 0) return 'ppm-change-negative'; return 'ppm-change-neutral'; };
  const hasProbability = value => typeof value === 'number' && Number.isFinite(value) && value >= 0 && value <= 1;
  const probabilityMetric = value => !hasProbability(value) ? '—' : value >= .9995 ? '>%99,9'
    : value <= .0005 ? '<%0,1' : `%${ppmMetric(value * 100, 1)}`;
  const probabilityTone = value => !hasProbability(value) ? '' : value < .60 ? 'low'
    : value < .70 ? 'medium' : value < .80 ? 'high' : 'very-high';
  function qualityBadge(alert) {
    const estimate = alert.win_probability;
    if (estimate && typeof estimate === 'object') {
      if (hasProbability(estimate.probability)) {
        return `<span class="quality-badge ${probabilityTone(estimate.probability)}" title="Hesaplanan kazanma olasılığı"><strong>${probabilityMetric(estimate.probability)}</strong></span>`;
      }
      return '<span class="quality-empty">—</span>';
    }
    return '<span class="quality-empty">—</span>';
  }
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
    const total = Number(summary.total || 0);
    const successful = Number(summary.successful || 0);
    const successRate = total === 0 ? '0 geçmiş sinyal' : total < 10 ? `${successful}/${total} başarılı` : hasMetric(summary.success_rate) ? `${ppmMetric(summary.success_rate, 0)}%` : '–';
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
        <div><span>${total < 10 ? 'Küçük örneklem' : 'Başarı oranı'}</span><strong>${successRate}</strong>${total < 10 ? '<small class="sample-warning">Örneklem yetersiz</small>' : ''}</div>
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

  function signalReferenceFacts(alert) {
    if (!['prematch', 'opening'].includes(alert.reference_used) || !hasMetric(alert.reference_total)) return '';
    const label = alert.reference_used === 'prematch' ? 'Son Maç Önü Baremi' : 'İlk Açılış Baremi';
    const change = hasMetric(alert.decision_change)
      ? `${Number(alert.decision_change) > 0 ? '+' : ''}${fmt(alert.decision_change)}` : '–';
    return `<div><span>Sinyal referansı</span><strong>${alert.reference_used === 'opening' ? esc(label) : `${esc(label)} · ${fmt(alert.reference_total)}`}</strong></div>
      <div><span>${alert.reference_used === 'prematch' ? 'Maç önü referansına göre fark' : 'Açılış referansına göre fark'}</span><strong>${change}</strong></div>
      ${hasMetric(alert.effective_threshold) && Number(alert.effective_threshold) !== 0 ? `<div><span>Uygulanan eşik</span><strong>${ppmMetric(alert.effective_threshold)} sayı</strong></div>` : ''}`;
  }

  function signedModalNumber(value) {
    return `${value > 0 ? '+' : value < 0 ? '−' : ''}${ppmMetric(Math.abs(value), 1)}`;
  }

  function liveDifference(alert, value) {
    if (!hasMetric(value) || !hasMetric(alert.live)) return '<small>Canlıya —</small>';
    const difference = Number(value) - Number(alert.live);
    const favorable = alert.direction === 'ALT' ? difference < 0 : alert.direction === 'ÜST' ? difference > 0 : false;
    const unfavorable = alert.direction === 'ALT' ? difference > 0 : alert.direction === 'ÜST' ? difference < 0 : false;
    const tone = favorable ? 'positive-text' : unfavorable ? 'negative-text' : '';
    return `<small class="${tone}">Canlıya ${signedModalNumber(difference)}</small>`;
  }

  function signalFormat(alert) {
    const duration = Number(alert.pace_game_minutes);
    if (!hasMetric(alert.pace_game_minutes) || ![40, 48].includes(duration)) return '—';
    const status = String(alert.status || '').trim();
    const periods = /^H[12](?:\b|\s)/i.test(status) && duration === 40 ? '2×20' : /^Q[1-4](?:\b|\s)/i.test(status) ? `4×${duration / 4}` : '';
    return `${periods ? `${periods} / ` : ''}${duration} dk`;
  }

  function signalSectionSubtitle(alert) {
    return `<span class="signal-modal-subtitle">${esc(alert.tournament || '—')} · ${esc(signalFormat(alert))}</span>`;
  }

  function signalSummary(alert) {
    const comparison = alert.ppm_comparison || {};
    return `<section class="modal-signal-summary" aria-label="Sinyal özeti">
      <div class="modal-summary-lead"><span class="direction-pill ${alert.direction === 'ALT' ? 'alt' : 'ust'}">${esc(alert.direction || '—')}</span>${qualityBadge(alert)}</div>
      <div class="modal-summary-facts">
        <div><span>Skor</span><strong>${esc(alert.score || '—')}</strong></div>
        <div><span>Periyot / saat</span><strong>${esc(alert.status || '—')}</strong></div>
        <div><span>Canlı Barem</span><strong>${fmt(alert.live)}</strong></div>
        <div><span>Tahmini toplam</span><strong>${fmt(alert.fair_total)}</strong>${liveDifference(alert, alert.fair_total)}</div>
        <div><span>Projeksiyon</span><strong>${fmt(alert.pace_projection)}</strong>${liveDifference(alert, alert.pace_projection)}</div>
        <div><span>Mevcut → Gereken PPM</span><strong>${ppmMetric(comparison.current_ppm)} → ${ppmMetric(comparison.required_ppm)} <small>${ppmChange(comparison.required_change_pct) || '—'}</small></strong></div>
      </div>
    </section>`;
  }

  function qualityReasons(alert) {
    const estimate = alert.win_probability;
    if (!estimate || !hasProbability(estimate.probability)) return '';
    const edge = hasMetric(alert.model_edge_points) ? `${ppmMetric(alert.model_edge_points, 1)} sayı` : '—';
    const threshold = hasMetric(alert.required_edge_points) ? `${ppmMetric(alert.required_edge_points, 1)} sayı` : '—';
    const under = probabilityMetric(estimate.under_probability);
    const over = probabilityMetric(estimate.over_probability);
    const push = hasMetric(estimate.push_probability) && Number(estimate.push_probability) > .001
      ? ` · İade %${ppmMetric(Number(estimate.push_probability) * 100, 1)}` : '';
    return `<section class="modal-section modal-quality-reasons"><h3 class="modal-section-title">Kazanma olasılığı</h3><p>ALT ${under} · ÜST ${over}${push}</p><p>${esc(alert.direction || '—')} yönünde toplam farkı: ${edge}. Gereken fark: ${threshold}.</p><p>Skor, kalan süre ve geçmiş tahmin hatalarından hesaplanır. Eğitim: ${esc(estimate.training_matches ?? 0)} maç.</p></section>`;
  }

  function signalLineFacts(alert) {
    const change = Number(alert.barem_change);
    const signed = hasMetric(alert.barem_change) ? `${change > 0 ? '+' : ''}${change.toFixed(1)}` : '—';
    return `<div class="modal-current-stats">
      <div><span>İlk Açılış Baremi</span><strong>${fmt(alert.opening)}</strong></div>
      <div><span>Canlı Barem</span><strong>${fmt(alert.live)}</strong></div>
      <div><span>Açılışa göre değişim</span><strong>${signed}</strong></div>
      ${signalReferenceFacts(alert)}
      <div><span>Sinyal sırası</span><strong>${hasMetric(alert.signal_count) && Number(alert.signal_count) > 0 ? Number(alert.signal_count) === 1 ? 'İlk sinyal' : `${Number(alert.signal_count)}. sinyal` : '—'}</strong></div>
    </div>`;
  }

  function openingLine(alert) {
    return `<span class="opening-line">${fmt(alert.opening)}<small class="opening-ppm">${hasMetric(alert.opening_ppm) ? ` (${ppmMetric(alert.opening_ppm)} PPM)` : ''}</small></span>`;
  }

  function ppmFlow(alert, handler='openSignalModal') {
    const comparison = alert.ppm_comparison || {};
    const aligned = comparison.signal_relation_tone === 'aligned';
    return `<button type="button" class="signal-modal-trigger ppm-modal-trigger ${hasMetric(alert.pace_ppm) ? '' : 'unavailable'}" onclick="${handler}(event, ${Number(alert.id)})" title="Tempo detayını aç" aria-label="Mevcut ${ppmMetric(alert.pace_ppm)}, gereken ${ppmMetric(comparison.required_ppm)} PPM. Tempo detayını aç"><span class="ppm-current">${ppmMetric(alert.pace_ppm)}</span><span class="ppm-flow-arrow" aria-hidden="true">→</span><span class="ppm-target">${ppmMetric(comparison.required_ppm)}</span><small class="ppm-flow-change ${ppmChangeTone(comparison.required_change_pct)}">${ppmChange(comparison.required_change_pct)}${aligned ? '<span class="ppm-aligned-tick" title="Tempo sinyal yönüyle uyumlu">✓</span>' : ''}</small></button>`;
  }

  function frozenListMarkers(alert) {
    return (alert.list_markers || []).map(marker => `<span class="marker ${marker.type === 'black' ? 'list-black-marker' : 'list-white-marker'}" title="${esc(marker.title)}">●</span>`).join('');
  }

  function savedStatusLabels(alert) {
    const labels = [['bet_placed', 'Oynandı'], ['followed', 'Takip'], ['ignored', 'Gözardı'], ['upcoming_followed', 'Ön takip']];
    return labels.filter(([key]) => alert[key]).map(([, label]) => `<span class="saved-status">${label}</span>`).join('');
  }
