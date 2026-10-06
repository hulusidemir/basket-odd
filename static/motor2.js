/* M2 reads the saved assessment; opening a modal never fetches new match data. */
const Motor2 = (() => {
  const escape = value => String(value ?? '').replace(/[&<>"']/g,
    ch => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[ch]));
  const number = value => typeof value === 'number' && Number.isFinite(value)
    ? value.toLocaleString('tr-TR', {maximumFractionDigits: 1}) : '—';
  let trigger = null;
  let activeId = null;
  function label(assessment, archived=false) {
    if (!assessment) return 'Kayıt yok';
    if (assessment.state === 'pending') return archived ? 'Analiz tamamlanmadı' : 'Veri bekleniyor';
    if (assessment.state === 'disabled') return 'M2 kapalı';
    if (assessment.state === 'ready') return assessment.direction === 'PAS' ? 'PAS' : assessment.direction;
    const failureLabels = {
      fetch_failed: 'Veri çekilemedi', capture_expired: 'Analiz süresi doldu',
      identity_mismatch: 'Kimlik uyuşmazlığı', score_mismatch: 'Skor uyuşmazlığı',
      clock_mismatch: 'Saat uyuşmazlığı', freshness_unverified: 'Güncellik doğrulanamadı',
      boxscore_missing: 'Şut verisi yok', boxscore_mismatch: 'Şut verisi uyuşmuyor',
      timeline_missing: 'Olay verisi yok', timeline_mismatch: 'Olay verisi uyuşmuyor',
      possession_fields_missing: 'Hücum verisi eksik', game_ineligible: 'Oyun aralığı uygun değil',
      sample_invalid: 'Şut örneklemi uygun değil', possessions_mismatch: 'Hücum hacmi uyuşmuyor',
    };
    if (failureLabels[assessment.reason_code]) return failureLabels[assessment.reason_code];
    if (/^statistics_|^site_codec/.test(assessment.source_error || '')) return 'Veri çekilemedi';
    return 'Veri yetersiz';
  }
  function button(alert, archived=false) {
    const m2 = alert.m2;
    const tone = m2?.state === 'ready' && m2.direction !== 'PAS'
      ? (m2.direction === 'ALT' ? 'alt' : 'ust') : 'm2-neutral';
    const title = m2?.message || 'Bu sinyal için M2 kaydı yok';
    return `<button type="button" class="m2-trigger" onclick="openM2Modal(event, ${Number(alert.id)})" title="${escape(title)}" aria-label="M2 analizini aç: ${escape(title)}"><span class="direction-pill ${tone}">${escape(label(m2, archived))}</span></button>`;
  }
  function content(alert, archived=false) {
    const m2 = alert.m2;
    if (!m2) return '<p>Bu sinyalin oluştuğu anda M2 verisi kaydedilmedi. Geçmiş maçtan yeni tahmin üretilmez.</p>';
    const context = m2.context || {};
    const ready = m2.state === 'ready';
    const rows = (m2.teams || []).map(team => {
      const shots = team.shooting || {};
      const pair = type => `${escape(shots[type]?.made ?? '—')}/${escape(shots[type]?.attempted ?? '—')}`;
      return `<tr><th>${team.side === 'home' ? 'Ev' : 'Deplasman'}</th><td>${pair('two_points')}</td><td>${pair('three_points')}</td><td>${pair('free_throws')}</td><td>${escape(team.offensive_rebounds ?? '—')}</td><td>${escape(team.turnovers ?? '—')}</td><td>${escape(team.personal_fouls ?? '—')}</td></tr>`;
    }).join('');
    const checks = (m2.checks || []).map(check => `<li class="${check.passed ? '' : 'negative-text'}">${check.passed ? '✓' : '×'} ${escape(check.label)}</li>`).join('');
    const reasons = (m2.reasons || []).map(reason => `<li>${escape(reason)}</li>`).join('');
    const limitations = (m2.limitations || []).map(reason => `<li>${escape(reason)}</li>`).join('');
    let time = '—';
    if (m2.captured_at) {
      const date = new Date(m2.captured_at);
      if (!Number.isNaN(date.getTime())) time = date.toLocaleString('tr-TR', {timeZone: 'Europe/Istanbul'});
    }
    return `<section class="modal-section"><h3>${escape(label(m2, archived))} · ${escape(m2.quality || 'YETERSİZ')} veri</h3>
      <p>${escape(m2.state === 'pending' && archived ? 'Arşivlendiğinde M2 analizi tamamlanmamıştı.' : m2.message)}</p>
      <div class="m2-facts"><span>Sinyal skoru <strong>${escape(context.score || '—')}</strong></span><span>Oyun saati <strong>${escape(context.status || '—')}</strong></span><span>Değerlendirilen barem <strong>${number(context.line)}</strong></span><span>Veri okuma <strong>${escape(time)}</strong></span></div>
      ${ready ? `<div class="m2-facts"><span>M2 tahmini <strong>${number(m2.projection)}</strong></span><span>Senaryo aralığı <strong>${number(m2.projection_low)}–${number(m2.projection_high)}</strong></span><span>Bareme fark <strong>${number(m2.edge)}</strong></span><span>Takım başı pozisyon/dk <strong>${number(m2.possessions_per_team_per_minute)}</strong></span></div>` : ''}</section>
      <section class="modal-section"><h3>${ready && m2.direction !== 'PAS' ? `Neden ${escape(m2.direction)}?` : 'Değerlendirme gerekçesi'}</h3>${reasons ? `<ul>${reasons}</ul>` : `<p>${escape(m2.message)}</p>`}</section>
      ${rows ? `<section class="modal-section"><h3>Şut ve hücum hacmi</h3><div class="m2-table-scroll"><table class="data-table"><thead><tr><th>Takım</th><th>2 sayı</th><th>3 sayı</th><th>Serbest atış</th><th>Hüc. rib.</th><th>Top kaybı</th><th>Kişisel faul</th></tr></thead><tbody>${rows}</tbody></table></div><p class="signal-modal-subtitle">Şutlar isabet/deneme olarak gösterilir. Kişisel faul toplamı, periyot bonusu değildir.</p></section>` : ''}
      <section class="modal-section"><h3>Veri kontrolleri</h3>${checks ? `<ul>${checks}</ul>` : '<p>Doğrulanmış veri kontrolü yok.</p>'}</section>
      ${limitations ? `<section class="modal-section"><h3>Analizin sınırları</h3><ul>${limitations}</ul><p class="signal-modal-subtitle">Şut dengeleme varsayımları: 2 sayı %52 (12 deneme), 3 sayı %35 (18 deneme), serbest atış %75 (8 deneme). Tempo senaryosu ±%5. M2 sürümü: ${escape(m2.version)}</p></section>` : ''}`;
  }
  function close() {
    const modal = document.getElementById('m2Modal');
    if (!modal || modal.hidden) return;
    modal.hidden = true;
    if (!document.querySelector('.signal-modal-backdrop:not([hidden])')) document.body.classList.remove('modal-open');
    trigger?.focus();
    trigger = null;
    activeId = null;
  }
  function open(alert, event, archived=false) {
    if (!alert) return;
    event?.preventDefault();
    event?.stopPropagation();
    trigger = event?.currentTarget || document.activeElement;
    activeId = Number(alert.id);
    const modal = document.getElementById('m2Modal');
    document.getElementById('m2ModalTitle').textContent = alert.match_name || 'Basketbol analizi';
    document.getElementById('m2ModalBody').innerHTML = content(alert, archived);
    modal.hidden = false;
    document.body.classList.add('modal-open');
    document.getElementById('m2ModalClose').focus();
  }
  if (typeof document !== 'undefined') {
    document.getElementById('m2ModalClose')?.addEventListener('click', close);
    document.getElementById('m2Modal')?.addEventListener('click', event => {
      if (event.target.id === 'm2Modal') close();
    });
    document.addEventListener('keydown', event => {
      const modal = document.getElementById('m2Modal');
      if (!modal || modal.hidden) return;
      if (event.key === 'Escape') { close(); event.stopImmediatePropagation(); }
      if (event.key === 'Tab') { event.preventDefault(); document.getElementById('m2ModalClose').focus(); }
    }, true);
  }
  function refresh(alerts) {
    const alert = alerts.find(row => Number(row.id) === activeId);
    if (alert && !document.getElementById('m2Modal')?.hidden)
      document.getElementById('m2ModalBody').innerHTML = content(alert);
  }
  return {button, content, open, close, refresh};
})();
