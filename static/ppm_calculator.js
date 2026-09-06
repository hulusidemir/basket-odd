// User-controlled scenarios only; never mutate a signal or its archived display values.
const PpmCalculator = (() => {
  const element = id => document.getElementById(`ppmCalculator${id}`);
  const dialog = element('');
  const slider = element('Slider');
  const paceInput = element('Pace');
  const number = value => typeof value === 'number' && Number.isFinite(value);
  const format = (value, digits = 1) => number(value)
    ? value.toLocaleString('tr-TR', {minimumFractionDigits: digits, maximumFractionDigits: digits}) : '–';
  let scenario = null;
  let trigger = null;

  function render(syncInput = true) {
    if (!scenario) return;
    const ppm = Number(slider.value);
    const added = ppm * scenario.remaining;
    const total = scenario.score + added;
    if (syncInput) paceInput.value = ppm.toFixed(2).replace('.', ',');
    paceInput.removeAttribute('aria-invalid');
    slider.setAttribute('aria-valuetext', `${format(ppm, 2)} sayı/dakika`);
    element('Total').textContent = format(total);
    element('Formula').textContent = `${format(scenario.score, 0)} mevcut + ${format(added)} kalan sürede = ${format(total)} sayı`;
    const difference = total - scenario.line;
    element('Comparison').textContent = !number(scenario.line) ? 'Sinyal baremi bulunmuyor.'
      : Math.abs(difference) < 0.05 ? 'Sinyal baremiyle aynı düzeyde.'
        : `Sinyal bareminin ${format(Math.abs(difference))} sayı ${difference > 0 ? 'üstünde' : 'altında'}.`;
  }

  function open(alert, button) {
    if (!alert) return;
    const comparison = alert.ppm_comparison || {};
    const score = alert.pace_score_total;
    const remaining = comparison.remaining_minutes;
    const current = comparison.current_ppm;
    const available = number(score) && score >= 0 && number(remaining) && remaining > 0
      && number(current) && current >= 0;
    trigger = button;
    scenario = available ? {score, remaining, current, line: alert.live} : null;
    element('Title').textContent = alert.match_name || 'Maç sonu hesabı';
    element('Score').textContent = number(score) ? `${alert.score || '–'} · ${format(score, 0)} sayı` : '–';
    const seconds = number(remaining) && remaining >= 0 ? Math.round(remaining * 60) : null;
    element('Time').textContent = seconds === null ? '–' : `${Math.floor(seconds / 60)}:${String(seconds % 60).padStart(2, '0')} dk:sn`;
    element('Line').textContent = format(alert.live);
    element('Error').hidden = available;
    element('Error').textContent = remaining === 0 ? 'Normal süre dolmuş; kalan süre için hesap yapılamıyor.'
      : 'Bu sinyalde hesaplama için gereken skor, süre veya PPM bilgisi bulunmuyor.';
    element('Controls').hidden = !available;
    slider.disabled = !available;
    paceInput.disabled = !available;
    if (available) {
      slider.max = Math.max(12, Math.ceil(current));
      slider.value = current;
      element('Max').textContent = `${format(Number(slider.max), 0)} PPM`;
      render();
    }
    if (!dialog.open) dialog.showModal();
    document.body.classList.add('ppm-calculator-open');
    (available ? slider : element('Close')).focus();
  }

  function button(alert) {
    return `<button class="button icon-action ppm-calculator-action" type="button" onclick="openPpmCalculator(event, ${Number(alert.id)})" title="PPM ile maç sonunu hesapla" aria-label="PPM hesaplayıcıyı aç"><svg viewBox="0 0 24 24" aria-hidden="true"><rect x="5" y="3" width="14" height="18" rx="2"></rect><path d="M8 7h8M8 11h1M12 11h1M16 11h.01M8 15h1M12 15h1M16 15v3M8 18h1M12 18h1"></path></svg></button>`;
  }

  slider.addEventListener('input', () => render());
  paceInput.addEventListener('input', () => {
    if (!scenario) return;
    const text = paceInput.value.trim().replace(',', '.');
    const ppm = Number(text);
    if (!/^(?:\d+(?:\.\d{0,2})?|\.\d{1,2})$/.test(text) || !number(ppm)
      || !number(scenario.score + ppm * scenario.remaining)) {
      paceInput.setAttribute('aria-invalid', 'true');
      element('Total').textContent = '–';
      element('Formula').textContent = '';
      element('Comparison').textContent = 'Sıfır veya daha büyük bir PPM gir (en fazla 2 ondalık).';
      return;
    }
    slider.max = Math.max(Number(slider.max), Math.ceil(ppm));
    element('Max').textContent = `${format(Number(slider.max), 0)} PPM`;
    slider.value = ppm;
    render(false);
  });
  element('Reset').addEventListener('click', () => {
    if (!scenario) return;
    slider.value = scenario.current;
    render();
  });
  element('Close').addEventListener('click', () => dialog.close());
  dialog.addEventListener('click', event => {
    if (event.target !== dialog) return;
    const bounds = dialog.getBoundingClientRect();
    if (event.clientX < bounds.left || event.clientX > bounds.right
      || event.clientY < bounds.top || event.clientY > bounds.bottom) dialog.close();
  });
  dialog.addEventListener('close', () => {
    document.body.classList.remove('ppm-calculator-open');
    scenario = null;
    if (trigger?.isConnected) trigger.focus();
    trigger = null;
  });
  return {open, button};
})();
