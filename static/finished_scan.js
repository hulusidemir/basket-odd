/* Poll the shared scan; page reloads and duplicate clicks attach to the same job. */
function setupFinishedScan({button, status, load, stopAutoRefresh, startAutoRefresh}) {
  let timer = null;
  let requesting = false;
  let activeJob = null;
  let lastCompletedJob = null;
  const endpoint = '/api/alerts/check-finished';

  async function request(method = 'GET') {
    const controller = new AbortController();
    const deadline = setTimeout(() => controller.abort(), 15000);
    try {
      const response = await fetch(endpoint, {method, cache: 'no-store', signal: controller.signal});
      const data = await response.json();
      if (!response.ok) throw new Error(data.error || 'Tarama bilgisi alınamadı.');
      return data;
    } finally {
      clearTimeout(deadline);
    }
  }

  function schedule(delay = 2000) {
    clearTimeout(timer);
    timer = setTimeout(poll, delay);
  }

  async function show(job) {
    if (job.state === 'running') {
      activeJob = job.job_id;
      stopAutoRefresh();
      button.disabled = true;
      button.textContent = 'Taranıyor…';
      const p = job.progress || {};
      const phase = p.phase === 'opening_browser' ? 'Kaynağa bağlanılıyor' : 'Biten maçlar taranıyor';
      status.classList.remove('error');
      status.textContent = `${phase} · ${p.processed_count || 0}/${p.tracked_count || 0} maç kontrol edildi · ${p.moved_count || 0} arşivlendi · ${p.check_failed_count || 0} okunamadı`;
      schedule();
      return;
    }
    const wasRunning = Boolean(activeJob);
    activeJob = null;
    if (wasRunning || (job.job_id && job.job_id !== lastCompletedJob)) {
      lastCompletedJob = job.job_id;
      await load();
      const result = job.result || {};
      status.classList.toggle('error', job.state === 'failed' || Boolean(result.check_failed_count || result.archive_failed_count));
      status.textContent = job.error || result.message || 'Tarama tamamlandı.';
    }
    button.disabled = false;
    button.textContent = 'Bitenleri kontrol et';
    if (wasRunning) startAutoRefresh();
    schedule(10000); // Also discover scans started by the hourly worker.
  }

  async function poll() {
    if (requesting) return;
    requesting = true;
    try {
      await show(await request());
    } catch (error) {
      if (activeJob) {
        status.classList.add('error');
        status.textContent = 'Tarama durumu alınamadı; bağlantı yeniden deneniyor…';
      }
      schedule(5000);
    } finally {
      requesting = false;
    }
  }

  button.onclick = async () => {
    if (requesting) return;
    requesting = true;
    clearTimeout(timer);
    button.disabled = true;
    button.textContent = 'Başlatılıyor…';
    try {
      await show(await request('POST'));
    } catch (error) {
      button.disabled = false;
      button.textContent = 'Bitenleri kontrol et';
      status.classList.add('error');
      status.textContent = error.message || 'Tarama başlatılamadı.';
      schedule(5000);
    } finally {
      requesting = false;
    }
  };
  poll();
}
