/* ─── Pulso SAT — app.js ─────────────────────────────────────────────────── */
'use strict';

/* ── Dark / light theme toggle ────────────────────────────────────────── */
document.getElementById('themeToggle')?.addEventListener('click', () => {
  const next = document.documentElement.dataset.theme === 'dark' ? 'light' : 'dark';
  document.documentElement.dataset.theme = next;
  localStorage.setItem('theme', next);
});

/* ══════════════════════════════════════════════════════════════════════════
   FORM PAGE — shared helpers
   ══════════════════════════════════════════════════════════════════════════ */

/* ── Auth tab switching ───────────────────────────────────────────────── */
document.querySelectorAll('.tab-btn').forEach(btn => {
  btn.addEventListener('click', () => {
    const tab = btn.dataset.tab;
    document.querySelectorAll('.tab-btn').forEach(b => b.classList.remove('active'));
    document.querySelectorAll('.tab-panel').forEach(p => p.classList.add('hidden'));
    btn.classList.add('active');
    document.getElementById('panel-' + tab)?.classList.remove('hidden');
    const authTypeInput = document.getElementById('authTypeInput');
    if (authTypeInput) authTypeInput.value = tab;
  });
});

/* ── File upload zones ────────────────────────────────────────────────── */
function initUploadZone(inputId, hintId, zoneId) {
  const input = document.getElementById(inputId);
  const hint  = document.getElementById(hintId);
  const zone  = document.getElementById(zoneId);
  if (!input || !hint || !zone) return;

  const update = file => {
    if (file) {
      hint.textContent = file.name;
      zone.classList.add('has-file');
    } else {
      hint.textContent = 'Arrastra o haz clic para seleccionar';
      zone.classList.remove('has-file');
    }
  };

  input.addEventListener('change', () => update(input.files[0] || null));
  zone.addEventListener('dragover',  e => { e.preventDefault(); zone.classList.add('dragover'); });
  zone.addEventListener('dragleave', () => zone.classList.remove('dragover'));
  zone.addEventListener('drop', e => {
    e.preventDefault();
    zone.classList.remove('dragover');
    const file = e.dataTransfer?.files[0];
    if (file) {
      const dt = new DataTransfer();
      dt.items.add(file);
      input.files = dt.files;
      update(file);
    }
  });
}

initUploadZone('cert_file', 'cert-name', 'zone-cert');
initUploadZone('key_file',  'key-name',  'zone-key');

/* ── Password toggle ──────────────────────────────────────────────────── */
document.querySelectorAll('.toggle-pw').forEach(btn => {
  btn.addEventListener('click', () => {
    const inp = document.getElementById(btn.dataset.target);
    if (inp) inp.type = inp.type === 'password' ? 'text' : 'password';
  });
});

/* ── RFC → uppercase ──────────────────────────────────────────────────── */
document.getElementById('rfc')?.addEventListener('input', function () {
  const pos = this.selectionStart;
  this.value = this.value.toUpperCase();
  this.setSelectionRange(pos, pos);
});

/* ── Year / Month pill selectors ─────────────────────────────────────── */
(function initPeriodPills() {
  const yearPills  = document.getElementById('yearPills');
  const monthPills = document.getElementById('monthPills');
  const fromEl     = document.getElementById('period_from');
  const toEl       = document.getElementById('period_to');
  const summary    = document.getElementById('periodSummary');
  const yearHint   = document.getElementById('yearHint');
  const monthHint  = document.getElementById('monthHint');
  if (!yearPills || !monthPills) return;

  const now         = new Date();
  const currentYear = now.getFullYear();
  const currentMonth = now.getMonth() + 1; // 1-based
  const FIRST_YEAR  = 2020;
  const MONTH_NAMES = ['Ene','Feb','Mar','Abr','May','Jun','Jul','Ago','Sep','Oct','Nov','Dic'];

  const selectedYears  = new Set();
  const selectedMonths = new Set();

  // ── helpers ────────────────────────────────────────────────
  function lastDayOf(year, month) {
    return new Date(year, month, 0).getDate();
  }

  function pad(n) { return String(n).padStart(2, '0'); }

  function maxAllowedMonth() {
    // If current year is selected, only allow months already concluded
    if (selectedYears.has(currentYear)) return currentMonth - 1;
    return 12;
  }

  function updatePeriod() {
    if (!selectedYears.size || !selectedMonths.size) {
      if (fromEl) fromEl.value = '';
      if (toEl)   toEl.value   = '';
      if (summary) summary.textContent = '';
      return;
    }

    const minYear  = Math.min(...selectedYears);
    const maxYear  = Math.max(...selectedYears);
    const minMonth = Math.min(...selectedMonths);
    const maxMonth = Math.max(...selectedMonths);

    const from = `${minYear}-${pad(minMonth)}-01 00:00:00`;
    const last = lastDayOf(maxYear, maxMonth);
    const to   = `${maxYear}-${pad(maxMonth)}-${last} 23:59:59`;

    if (fromEl) fromEl.value = from;
    if (toEl)   toEl.value   = to;

    if (summary) {
      summary.textContent =
        `${MONTH_NAMES[minMonth-1]} ${minYear} → ${MONTH_NAMES[maxMonth-1]} ${maxYear}`;
    }

    yearHint.textContent  = `(${selectedYears.size} seleccionado${selectedYears.size > 1 ? 's' : ''})`;
    monthHint.textContent = `(${selectedMonths.size} seleccionado${selectedMonths.size > 1 ? 's' : ''})`;
  }

  function refreshMonthPills() {
    const max = maxAllowedMonth();
    monthPills.querySelectorAll('.pill').forEach(pill => {
      const m = +pill.dataset.value;
      const disabled = m > max;
      pill.classList.toggle('pill-disabled', disabled);
      pill.disabled = disabled;
      if (disabled && selectedMonths.has(m)) {
        selectedMonths.delete(m);
        pill.classList.remove('pill-active');
      }
    });
  }

  // ── build year pills ───────────────────────────────────────
  for (let y = FIRST_YEAR; y <= currentYear; y++) {
    const btn = document.createElement('button');
    btn.type = 'button';
    btn.className = 'pill';
    btn.dataset.value = y;
    btn.textContent = y;
    btn.addEventListener('click', () => {
      if (selectedYears.has(y)) {
        selectedYears.delete(y);
        btn.classList.remove('pill-active');
      } else {
        selectedYears.add(y);
        btn.classList.add('pill-active');
      }
      refreshMonthPills();
      updatePeriod();
    });
    yearPills.appendChild(btn);
  }

  // ── build month pills ──────────────────────────────────────
  MONTH_NAMES.forEach((name, i) => {
    const m = i + 1;
    const btn = document.createElement('button');
    btn.type = 'button';
    btn.className = 'pill';
    btn.dataset.value = m;
    btn.textContent = name;
    btn.addEventListener('click', () => {
      if (btn.disabled) return;
      if (selectedMonths.has(m)) {
        selectedMonths.delete(m);
        btn.classList.remove('pill-active');
      } else {
        selectedMonths.add(m);
        btn.classList.add('pill-active');
      }
      updatePeriod();
    });
    monthPills.appendChild(btn);
  });

  // ── defaults: current year + all concluded months ──────────
  const defaultYear = currentYear;
  yearPills.querySelectorAll('.pill').forEach(p => {
    if (+p.dataset.value === defaultYear) {
      p.classList.add('pill-active');
      selectedYears.add(defaultYear);
    }
  });

  refreshMonthPills();

  const defaultMaxMonth = currentMonth - 1; // last concluded month
  monthPills.querySelectorAll('.pill').forEach(p => {
    const m = +p.dataset.value;
    if (m >= 1 && m <= defaultMaxMonth && !p.disabled) {
      p.classList.add('pill-active');
      selectedMonths.add(m);
    }
  });

  updatePeriod();
})();

/* ── File → base64 helper ─────────────────────────────────────────────── */
function fileToBase64(file) {
  return new Promise((resolve, reject) => {
    const reader = new FileReader();
    reader.onload  = () => {
      // result is "data:<mime>;base64,<data>" — we want only the data part
      const b64 = reader.result.split(',')[1];
      resolve(b64);
    };
    reader.onerror = reject;
    reader.readAsDataURL(file);
  });
}

/* ══════════════════════════════════════════════════════════════════════════
   STREAMING QUERY
   ══════════════════════════════════════════════════════════════════════════ */

let activeReader   = null;   // current ReadableStreamDefaultReader
let streamTotal    = 0;
let streamDlType   = 'emitidos';
const streamTypeCounts = { ingreso: 0, egreso: 0, nomina: 0, pago: 0, traslado: 0 };
window.__captchaSessionId = null;   // session ID of the pending captcha challenge

/* ── Grab DOM references ──────────────────────────────────────────────── */
const queryForm      = document.getElementById('queryForm');
const submitBtn      = document.getElementById('submitBtn');
const btnLabel       = submitBtn?.querySelector('.btn-label');
const btnSpinner     = submitBtn?.querySelector('.btn-spinner');
const streamSection  = document.getElementById('streamSection');
const streamCount    = document.getElementById('streamCount');
const streamBadge    = document.getElementById('streamBadge');
const streamPeriod   = document.getElementById('streamPeriodLabel');
const streamTableBody = document.getElementById('streamTableBody');
const streamSearch   = document.getElementById('streamSearch');
const streamError    = document.getElementById('streamError');
const streamCancel   = document.getElementById('streamCancelBtn');
const streamCheckAll   = document.getElementById('streamCheckAll');
const streamDlSelected = document.getElementById('streamDlSelected');
const streamSelectAll  = document.getElementById('streamSelectAll');

/* ── Single-type stream helper ────────────────────────────────────────── */
// Runs one POST to /list/stream for a specific dlType ('emitidos' or 'recibidos').
// Appends rows into the shared table. Does NOT reset the UI or call markStreamDone.
async function runSingleStream(auth, periodFrom, periodTo, dlType) {
  let response;
  try {
    response = await fetch('/api/v1/invoices/list/stream', {
      method:  'POST',
      headers: { 'Content-Type': 'application/json' },
      body:    JSON.stringify({ auth, period_from: periodFrom, period_to: periodTo, download_type: dlType }),
    });
  } catch (err) {
    showStreamError(`[${dlType}] No se pudo conectar: ` + err.message);
    return;
  }

  if (!response.ok) {
    const err = await response.json().catch(() => ({ error: `HTTP ${response.status}` }));
    showStreamError(`[${dlType}] ` + (err.error || `Error ${response.status}`));
    return;
  }

  activeReader = response.body.getReader();
  const decoder = new TextDecoder();
  let buffer = '';

  try {
    while (true) {
      const { done, value } = await activeReader.read();
      if (done) break;

      buffer += decoder.decode(value, { stream: true });
      const events = buffer.split('\n\n');
      buffer = events.pop();

      for (const event of events) {
        if (!event.startsWith('data: ')) continue;
        const jsonStr = event.slice(6).trim();
        if (!jsonStr) continue;
        let data;
        try { data = JSON.parse(jsonStr); } catch (_) { continue; }

        if (data.__done__) {
          // Consume silently here; caller calls markStreamDone at the end
        } else if (data.__dl_type__) {
          // PHP signals a switch between emitidos / recibidos within the same stream.
          // Insert a visual separator row when switching to recibidos.
          if (data.__dl_type__ === 'recibidos' && streamTableBody && streamTotal > 0) {
            const sep = document.createElement('tr');
            sep.className = 'type-separator-row';
            sep.innerHTML = '<td colspan="9"><span class="type-separator-label">— Recibidas —</span></td>';
            streamTableBody.appendChild(sep);
          }
        } else if (data.__limit_reached__) {
          const reason = data.reason || 'Límite de descarga del SAT alcanzado';
          showStreamError(`[${dlType}] ${reason}`);
        } else if (data.__error__) {
          showStreamError(`[${dlType}] ${data.__error__ || data.error || 'Error desconocido'}`);
        } else if (data.__captcha__) {
          showCaptchaModal(data.session_id, data.image_base64, data.mime);
        } else {
          // Use per-invoice _dl_type (present in "ambas" mode) or fall back to request dlType
          appendInvoiceRow(data, data._dl_type || dlType);
        }
      }
    }
  } catch (err) {
    if (err.name !== 'AbortError') showStreamError(`[${dlType}] Stream interrumpido: ` + err.message);
  } finally {
    activeReader = null;
  }
}

/* ── Form intercept — streaming fetch ────────────────────────────────── */
queryForm?.addEventListener('submit', async e => {
  e.preventDefault();

  // Cancel any previous stream
  if (activeReader) { try { await activeReader.cancel(); } catch (_) {} }

  // Reset UI
  streamTotal = 0;
  Object.keys(streamTypeCounts).forEach(k => streamTypeCounts[k] = 0);
  if (streamTableBody) streamTableBody.innerHTML = '';
  if (streamError)     { streamError.textContent = ''; streamError.classList.add('hidden'); }
  if (streamSection)   streamSection.classList.add('hidden');
  if (streamCheckAll)  streamCheckAll.checked = false;
  updateTypeCounters();
  updateBulkBtn();

  // Loading state
  if (submitBtn)  submitBtn.disabled = true;
  if (btnLabel)   btnLabel.classList.add('hidden');
  if (btnSpinner) btnSpinner.classList.remove('hidden');

  // Read form values
  const authType    = document.getElementById('authTypeInput')?.value || 'fiel';
  const periodFrom  = document.getElementById('period_from')?.value || '';
  const periodTo    = document.getElementById('period_to')?.value   || '';
  const dlType      = document.getElementById('download_type')?.value || 'emitidos';
  streamDlType = dlType;

  // Build auth payload
  let auth;
  try {
    if (authType === 'fiel') {
      const certFile = document.getElementById('cert_file')?.files[0];
      const keyFile  = document.getElementById('key_file')?.files[0];
      if (!certFile) throw new Error('Selecciona el certificado (.cer)');
      if (!keyFile)  throw new Error('Selecciona la clave privada (.key)');
      const [certB64, keyB64] = await Promise.all([fileToBase64(certFile), fileToBase64(keyFile)]);
      auth = {
        type:        'fiel',
        certificate: certB64,
        private_key: keyB64,
        password:    document.getElementById('fiel_password')?.value || '',
      };
    } else {
      auth = {
        type:     'ciec',
        rfc:      (document.getElementById('rfc')?.value || '').toUpperCase(),
        password: document.getElementById('ciec_password')?.value || '',
      };
    }
  } catch (err) {
    resetSubmitBtn();
    showStreamError(err.message);
    return;
  }

  // Store credentials globally so download buttons can reuse them
  window.__CREDS__         = auth;
  window.__DOWNLOAD_TYPE__ = dlType;

  // Update period label
  if (streamPeriod) {
    streamPeriod.innerHTML =
      `<span class="summary-num">${periodFrom}</span>
       <span class="summary-label">–</span>
       <span class="summary-num">${periodTo}</span>`;
  }

  if (streamSection) streamSection.classList.remove('hidden');

  try {
    // "ambas" is handled in a single PHP process (one CIEC auth, both types
    // streamed sequentially) to avoid the SAT resetting a second rapid login.
    await runSingleStream(auth, periodFrom, periodTo, dlType);
  } finally {
    resetSubmitBtn();
    markStreamDone(streamTotal);
  }
});

/* ── Cancel button ────────────────────────────────────────────────────── */
streamCancel?.addEventListener('click', async () => {
  if (activeReader) { try { await activeReader.cancel(); } catch (_) {} activeReader = null; }
  markStreamDone(streamTotal);
});

/* ── Reset submit button ──────────────────────────────────────────────── */
function resetSubmitBtn() {
  if (submitBtn)  submitBtn.disabled = false;
  if (btnLabel)   btnLabel.classList.remove('hidden');
  if (btnSpinner) btnSpinner.classList.add('hidden');
}

/* ── Mark stream as complete ──────────────────────────────────────────── */
function markStreamDone(total) {
  if (!streamBadge) return;
  streamBadge.innerHTML = `<svg class="icon-sm" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5"><polyline points="20 6 9 17 4 12"/></svg> ${total} facturas`;
  streamBadge.className = 'badge badge-green';
  streamCancel?.classList.add('hidden');
  if (total > 0) startBackgroundCache();
}

/* ── Background XML cache ────────────────────────────────────────────── */
async function startBackgroundCache() {
  if (!window.__CREDS__) return;

  const btns = [...document.querySelectorAll('#streamTableBody [data-conceptos-uuid]')];
  if (!btns.length) return;

  const cacheBadge = document.getElementById('cacheBadge');
  const total = btns.length;
  let done = 0;

  const updateBadge = () => {
    if (!cacheBadge) return;
    if (done >= total) {
      cacheBadge.textContent = `✓ XMLs cacheados`;
      cacheBadge.className = 'badge badge-green';
      setTimeout(() => cacheBadge.classList.add('hidden'), 3000);
    } else {
      cacheBadge.textContent = `Cacheando XMLs ${done}/${total}…`;
      cacheBadge.className = 'badge badge-gray';
      cacheBadge.classList.remove('hidden');
    }
  };

  updateBadge();

  for (const btn of btns) {
    const uuid        = btn.dataset.conceptosUuid;
    const fecha       = btn.dataset.fecha;
    const rfcEmisor   = btn.dataset.rfcEmisor   || '';
    const rfcReceptor = btn.dataset.rfcReceptor || '';
    try {
      await fetchXmlText(uuid, rfcEmisor, rfcReceptor, fecha);
    } catch (_) { /* ignore individual failures */ }
    done++;
    updateBadge();
    // Small delay to not hammer SAT
    await new Promise(r => setTimeout(r, 300));
  }
}

/* ── Show stream error ────────────────────────────────────────────────── */
function showStreamError(msg) {
  if (!streamError) return;
  streamError.textContent = '⚠ ' + msg;
  streamError.classList.remove('hidden');
  if (streamSection) streamSection.classList.remove('hidden');
}

/* ── Append one invoice row ───────────────────────────────────────────── */
function statusBadge(state) {
  const s = (state || '').toLowerCase();
  if (s === 'vigente')   return `<span class="badge badge-green">Vigente</span>`;
  if (s === 'cancelado') return `<span class="badge badge-red">Cancelado</span>`;
  return `<span class="badge badge-gray">${state || '—'}</span>`;
}

function typeBadge(tipo) {
  const t = (tipo || '').trim().toLowerCase();
  if (t === 'ingreso'  || t === 'i') return `<span class="badge badge-indigo">Ingreso</span>`;
  if (t === 'egreso'   || t === 'e') return `<span class="badge badge-amber">Egreso</span>`;
  if (t === 'nómina'   || t === 'nomina' || t === 'n') return `<span class="badge badge-purple">Nómina</span>`;
  if (t === 'pago'     || t === 'p') return `<span class="badge badge-teal">Pago</span>`;
  if (t === 'traslado' || t === 't') return `<span class="badge badge-gray">Traslado</span>`;
  return t ? `<span class="badge badge-gray">${tipo}</span>` : '—';
}

function formatTotal(val) {
  if (val == null || val === '') return '—';
  const n = parseFloat(String(val).replace(/[$,\s]/g, ''));
  if (isNaN(n)) return '—';
  return '$' + n.toLocaleString('es-MX', { minimumFractionDigits: 2, maximumFractionDigits: 2 });
}

function updateTypeCounters() {
  const types = ['ingreso', 'egreso', 'nomina', 'pago', 'traslado'];
  types.forEach(t => {
    const el = document.getElementById('counter-' + t);
    if (el) el.textContent = streamTypeCounts[t];
  });
}

function appendInvoiceRow(inv, invoiceDlType = 'emitidos') {
  if (!streamTableBody) return;
  // Skip rows where the SAT portal returned only the UUID with no other metadata.
  if (!inv.rfcEmisor && !inv.rfcReceptor) return;
  streamTotal++;
  if (streamCount) streamCount.textContent = streamTotal;

  const uuid    = inv.uuid || '';
  const short   = uuid.length > 13 ? uuid.slice(0, 13) + '…' : uuid;
  const total   = formatTotal(inv.total);

  // Update type counter
  const tipo = (inv.efectoComprobante || '').trim().toLowerCase();
  if      (tipo === 'ingreso'  || tipo === 'i') streamTypeCounts.ingreso++;
  else if (tipo === 'egreso'   || tipo === 'e') streamTypeCounts.egreso++;
  else if (tipo === 'nómina'   || tipo === 'nomina' || tipo === 'n') streamTypeCounts.nomina++;
  else if (tipo === 'pago'     || tipo === 'p') streamTypeCounts.pago++;
  else if (tipo === 'traslado' || tipo === 't') streamTypeCounts.traslado++;
  updateTypeCounters();

  const tr = document.createElement('tr');
  tr.classList.add('row-new');
  tr.dataset.invoiceDlType = invoiceDlType;
  tr.innerHTML = `
    <td class="col-check"><input type="checkbox" class="row-check" data-uuid="${uuid}" /></td>
    <td>
      <span class="uuid-cell" title="${uuid}" data-copy="${uuid}">
        ${short}
        <svg class="copy-icon" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><rect x="9" y="9" width="13" height="13" rx="2"/><path d="M5 15H4a2 2 0 01-2-2V4a2 2 0 012-2h9a2 2 0 012 2v1"/></svg>
      </span>
    </td>
    <td><code class="rfc">${inv.rfcEmisor || '—'}</code></td>
    <td class="nombre">${inv.nombreEmisor || '—'}</td>
    <td><code class="rfc">${inv.rfcReceptor || '—'}</code></td>
    <td class="fecha">${inv.fechaEmision || '—'}</td>
    <td class="num">${total}</td>
    <td>${typeBadge(inv.efectoComprobante)}</td>
    <td>${statusBadge(inv.estadoComprobante)}</td>
    <td class="col-actions">
      <button class="btn-icon btn-icon-conceptos" data-conceptos-uuid="${uuid}" data-fecha="${inv.fechaEmision || ''}" data-rfc-emisor="${inv.rfcEmisor || ''}" data-rfc-receptor="${inv.rfcReceptor || ''}" title="Ver conceptos">
        <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" width="14" height="14"><path d="M1 12s4-8 11-8 11 8 11 8-4 8-11 8-11-8-11-8z"/><circle cx="12" cy="12" r="3"/></svg>
      </button>
      <button class="btn-icon" data-dl-uuid="${uuid}" data-dl-type="xml" title="Descargar XML">
        <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" width="14" height="14"><path d="M21 15v4a2 2 0 01-2 2H5a2 2 0 01-2-2v-4"/><polyline points="7 10 12 15 17 10"/><line x1="12" y1="15" x2="12" y2="3"/></svg>
        XML
      </button>
      <button class="btn-icon btn-icon-pdf" data-dl-uuid="${uuid}" data-dl-type="pdf" title="Descargar PDF">
        <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" width="14" height="14"><path d="M21 15v4a2 2 0 01-2 2H5a2 2 0 01-2-2v-4"/><polyline points="7 10 12 15 17 10"/><line x1="12" y1="15" x2="12" y2="3"/></svg>
        PDF
      </button>
    </td>`;

  // UUID copy
  tr.querySelector('.uuid-cell')?.addEventListener('click', () => {
    navigator.clipboard.writeText(uuid).then(() => {
      const el = tr.querySelector('.uuid-cell');
      el.style.color = 'var(--success)';
      setTimeout(() => { el.style.color = ''; }, 1200);
    });
  });

  // Conceptos button
  tr.querySelector('[data-conceptos-uuid]')?.addEventListener('click', e => {
    const btn = e.currentTarget;
    showConceptos(uuid, btn.dataset.fecha, btn.dataset.rfcEmisor, btn.dataset.rfcReceptor);
  });

  // Download buttons — pass per-row invoice type
  tr.querySelectorAll('[data-dl-uuid]').forEach(btn => {
    btn.addEventListener('click', () => downloadInvoices([btn.dataset.dlUuid], btn.dataset.dlType || 'xml', invoiceDlType));
  });

  // Checkbox
  tr.querySelector('.row-check')?.addEventListener('change', updateBulkBtn);

  // Remove highlight class after animation
  tr.addEventListener('animationend', () => tr.classList.remove('row-new'), { once: true });

  streamTableBody.appendChild(tr);
}

/* ── Live search ──────────────────────────────────────────────────────── */
streamSearch?.addEventListener('input', () => {
  const q = streamSearch.value.toLowerCase().trim();
  document.querySelectorAll('#streamTableBody tr').forEach(row => {
    row.style.display = (q && !row.textContent.toLowerCase().includes(q)) ? 'none' : '';
  });
});

/* ── Checkbox bulk select ─────────────────────────────────────────────── */
function updateBulkBtn() {
  const checked = document.querySelectorAll('#streamTableBody .row-check:checked');
  if (streamDlSelected) streamDlSelected.disabled = checked.length === 0;
  if (streamCheckAll) {
    const all = document.querySelectorAll('#streamTableBody .row-check');
    streamCheckAll.checked = all.length > 0 && checked.length === all.length;
    streamCheckAll.indeterminate = checked.length > 0 && checked.length < all.length;
  }
}

streamCheckAll?.addEventListener('change', () => {
  document.querySelectorAll('#streamTableBody .row-check').forEach(c => { c.checked = streamCheckAll.checked; });
  updateBulkBtn();
});

streamSelectAll?.addEventListener('click', () => {
  const all = [...document.querySelectorAll('#streamTableBody .row-check')];
  const allChecked = all.every(c => c.checked);
  all.forEach(c => { c.checked = !allChecked; });
  if (streamCheckAll) streamCheckAll.checked = !allChecked;
  updateBulkBtn();
});

streamDlSelected?.addEventListener('click', async () => {
  const checked = [...document.querySelectorAll('#streamTableBody .row-check:checked')];
  if (!checked.length) return;

  if (window.__DOWNLOAD_TYPE__ === 'ambas') {
    // Group by per-row invoice type and download each group separately
    const byType = { emitidos: [], recibidos: [] };
    checked.forEach(c => {
      const t = c.closest('tr')?.dataset.invoiceDlType || 'emitidos';
      (byType[t] = byType[t] || []).push(c.dataset.uuid);
    });
    for (const [t, uuids] of Object.entries(byType)) {
      if (uuids.length) await downloadInvoices(uuids.filter(Boolean), 'xml', t);
    }
  } else {
    const uuids = checked.map(c => c.dataset.uuid).filter(Boolean);
    if (uuids.length) downloadInvoices(uuids, 'xml');
  }
});

/* ══════════════════════════════════════════════════════════════════════════
   DOWNLOAD (used by both streaming table and invoices.html table)
   ══════════════════════════════════════════════════════════════════════════ */

const toast    = document.getElementById('dlToast');
const toastMsg = document.getElementById('dlToastMsg');

function showToast(msg) {
  if (!toast || !toastMsg) return;
  toastMsg.textContent = msg;
  toast.classList.remove('hidden');
}
function hideToast() { toast?.classList.add('hidden'); }

// dlType is optional — if omitted falls back to window.__DOWNLOAD_TYPE__.
// Pass explicitly when downloading from a mixed (ambas) result set.
async function downloadInvoices(uuids, resourceType, dlType) {
  const creds          = window.__CREDS__;
  const effectiveDlType = dlType || window.__DOWNLOAD_TYPE__ || 'emitidos';
  if (!creds) { alert('Credenciales no disponibles. Vuelve a hacer la consulta.'); return; }

  showToast(`Descargando ${uuids.length} factura(s) en ${resourceType.toUpperCase()}…`);

  try {
    const res = await fetch('/api/v1/invoices/download/stream', {
      method:  'POST',
      headers: { 'Content-Type': 'application/json' },
      body:    JSON.stringify({ auth: creds, uuids, download_type: effectiveDlType, resource_type: resourceType }),
    });

    if (!res.ok) {
      const err = await res.json().catch(() => ({ error: 'Error desconocido' }));
      throw new Error(err.error || `HTTP ${res.status}`);
    }

    const reader  = res.body.getReader();
    const decoder = new TextDecoder();
    let buffer    = '';

    while (true) {
      const { done, value } = await reader.read();
      if (done) break;
      buffer += decoder.decode(value, { stream: true });

      const events = buffer.split('\n\n');
      buffer = events.pop();

      for (const event of events) {
        if (!event.startsWith('data: ')) continue;
        let data;
        try { data = JSON.parse(event.slice(6).trim()); } catch (_) { continue; }

        if (data.__captcha__) {
          showCaptchaModal(data.session_id, data.image_base64, data.mime);

        } else if (data.__download__) {
          // Decode base64 → Blob → trigger save
          const bytes  = Uint8Array.from(atob(data.data_b64), c => c.charCodeAt(0));
          const blob   = new Blob([bytes], { type: data.content_type });
          const url    = URL.createObjectURL(blob);
          const a      = document.createElement('a');
          a.href       = url;
          a.download   = data.filename;
          a.click();
          URL.revokeObjectURL(url);
          hideToast();

        } else if (data.__error__) {
          throw new Error(data.__error__);
        }
      }
    }
  } catch (err) {
    hideToast();
    alert(`Error al descargar: ${err.message}`);
  }
}

/* ── invoices.html page: wire existing download buttons ──────────────── */
document.querySelectorAll('[data-dl-uuid]').forEach(btn => {
  btn.addEventListener('click', () =>
    downloadInvoices([btn.dataset.dlUuid], btn.dataset.dlType || 'xml'));
});
document.getElementById('downloadSelectedBtn')?.addEventListener('click', () => {
  const uuids = [...document.querySelectorAll('.row-check:checked')]
    .map(c => c.dataset.uuid).filter(Boolean);
  if (uuids.length) downloadInvoices(uuids, 'xml');
});

/* ── invoices.html: UUID copy ─────────────────────────────────────────── */
document.querySelectorAll('.uuid-cell').forEach(el => {
  el.addEventListener('click', () => {
    navigator.clipboard.writeText(el.dataset.copy || '').then(() => {
      el.style.color = 'var(--success)';
      setTimeout(() => { el.style.color = ''; }, 1200);
    });
  });
});

/* ── invoices.html: table search ──────────────────────────────────────── */
document.getElementById('tableSearch')?.addEventListener('input', function () {
  const q = this.value.toLowerCase().trim();
  document.querySelectorAll('#invoiceTable tbody tr').forEach(row => {
    row.classList.toggle('hidden-row', q.length > 0 && !row.textContent.toLowerCase().includes(q));
  });
});

/* ══════════════════════════════════════════════════════════════════════════
   CAPTCHA MODAL
   ══════════════════════════════════════════════════════════════════════════ */

function showCaptchaModal(sessionId, imageBase64, mime) {
  window.__captchaSessionId = sessionId;

  const modal   = document.getElementById('captchaModal');
  const img     = document.getElementById('captchaImg');
  const input   = document.getElementById('captchaInput');
  const errorEl = document.getElementById('captchaError');

  img.src   = `data:${mime || 'image/gif'};base64,${imageBase64}`;
  img.alt   = 'Captcha del SAT';
  input.value = '';
  errorEl.classList.add('hidden');
  modal.classList.remove('hidden');

  // Focus input after the image renders
  requestAnimationFrame(() => input.focus());
}

function hideCaptchaModal() {
  document.getElementById('captchaModal')?.classList.add('hidden');
  window.__captchaSessionId = null;
}

async function submitCaptcha() {
  const input   = document.getElementById('captchaInput');
  const btn     = document.getElementById('captchaSubmitBtn');
  const errorEl = document.getElementById('captchaError');
  const answer  = input?.value?.trim();

  if (!answer) {
    input?.focus();
    return;
  }
  if (!window.__captchaSessionId) return;

  btn.disabled = true;
  errorEl.classList.add('hidden');

  try {
    const res = await fetch('/api/v1/invoices/captcha/solve', {
      method:  'POST',
      headers: { 'Content-Type': 'application/json' },
      body:    JSON.stringify({ session_id: window.__captchaSessionId, answer }),
    });

    if (!res.ok) {
      const err = await res.json().catch(() => ({ error: 'Error desconocido' }));
      throw new Error(err.error || `HTTP ${res.status}`);
    }

    hideCaptchaModal();
  } catch (err) {
    errorEl.textContent = 'Error al enviar: ' + err.message;
    errorEl.classList.remove('hidden');
  } finally {
    btn.disabled = false;
  }
}

document.getElementById('captchaSubmitBtn')?.addEventListener('click', submitCaptcha);

document.getElementById('captchaInput')?.addEventListener('keydown', e => {
  if (e.key === 'Enter') submitCaptcha();
});

// Close modal on backdrop click
document.getElementById('captchaModal')?.addEventListener('click', e => {
  if (e.target === e.currentTarget) hideCaptchaModal();
});

/* ══════════════════════════════════════════════════════════════════════════
   CONCEPTOS MODAL
   ══════════════════════════════════════════════════════════════════════════ */

function hideConceptosModal() {
  document.getElementById('conceptosModal')?.classList.add('hidden');
}

document.getElementById('conceptosCloseBtn')?.addEventListener('click', hideConceptosModal);
document.getElementById('conceptosModal')?.addEventListener('click', e => {
  if (e.target === e.currentTarget) hideConceptosModal();
});

/* Fetch XML text — checks S3 cache first via /xml-content endpoint */
async function fetchXmlText(uuid, rfcEmisor, rfcReceptor, fecha) {
  const creds = window.__CREDS__;
  if (!creds) throw new Error('Credenciales no disponibles. Vuelve a hacer la consulta.');

  // Normalise fecha to "YYYY-MM-DD" (strip time portion if present)
  const fechaDate = (fecha || '').slice(0, 10);

  const res = await fetch('/api/v1/invoices/xml-content', {
    method:  'POST',
    headers: { 'Content-Type': 'application/json' },
    body:    JSON.stringify({ auth: creds, uuid, rfc_emisor: rfcEmisor, rfc_receptor: rfcReceptor, fecha: fechaDate }),
  });

  if (!res.ok) {
    const err = await res.json().catch(() => ({ error: `HTTP ${res.status}` }));
    throw new Error(err.error || `HTTP ${res.status}`);
  }

  const data  = await res.json();
  const bytes = Uint8Array.from(atob(data.data_b64), c => c.charCodeAt(0));
  return new TextDecoder('utf-8').decode(bytes);
}

/* Parse cfdi:Concepto elements from parsed XML doc */
function parseConceptos(doc) {
  // getElementsByTagNameNS with wildcard handles cfdi: namespace prefixes
  const nodes = doc.getElementsByTagNameNS('*', 'Concepto');

  return Array.from(nodes).map(c => {
    const traslado = c.getElementsByTagNameNS('*', 'Traslado')[0] || null;
    return {
      claveProdServ:  c.getAttribute('ClaveProdServ') || '—',
      claveUnidad:    c.getAttribute('ClaveUnidad')   || '—',
      cantidad:       c.getAttribute('Cantidad')      || '—',
      unidad:         c.getAttribute('Unidad')        || '—',
      noIdentif:      c.getAttribute('NoIdentificacion') || '—',
      descripcion:    c.getAttribute('Descripcion')   || '—',
      valorUnitario:  c.getAttribute('ValorUnitario') || '—',
      importe:        c.getAttribute('Importe')       || '—',
      objetoImp:      c.getAttribute('ObjetoImp')     || '—',
      impuesto:       traslado ? traslado.getAttribute('Impuesto')  || '—' : '—',
      tasaFactor:     traslado ? traslado.getAttribute('TipoFactor')|| '—' : '—',
      tasa:           traslado ? traslado.getAttribute('TasaOCuota')|| '—' : '—',
      importeIva:     traslado ? traslado.getAttribute('Importe')   || '—' : '—',
    };
  });
}

function fmt(val) {
  if (val === '—' || val == null) return '—';
  const n = parseFloat(val);
  if (isNaN(n)) return val;
  return '$' + n.toLocaleString('es-MX', { minimumFractionDigits: 2, maximumFractionDigits: 2 });
}

function parseDoctos(doc) {
  const pagos = doc.getElementsByTagNameNS('*', 'Pago');
  const result = [];
  Array.from(pagos).forEach(pago => {
    const fechaPago     = pago.getAttribute('FechaPago')      || '—';
    const formaPago     = pago.getAttribute('FormaDePagoP')   || '—';
    const monedaPago    = pago.getAttribute('MonedaP')        || '—';
    const monto         = pago.getAttribute('Monto')          || '—';
    Array.from(pago.getElementsByTagNameNS('*', 'DoctoRelacionado')).forEach(d => {
      result.push({
        idDocumento:      d.getAttribute('IdDocumento')      || '—',
        folio:            d.getAttribute('Folio')            || '—',
        monedaDR:         d.getAttribute('MonedaDR')         || '—',
        numParcialidad:   d.getAttribute('NumParcialidad')   || '—',
        impSaldoAnt:      d.getAttribute('ImpSaldoAnt')      || '—',
        impPagado:        d.getAttribute('ImpPagado')        || '—',
        impSaldoInsoluto: d.getAttribute('ImpSaldoInsoluto') || '—',
        fechaPago, formaPago, monedaPago, monto,
      });
    });
  });
  return result;
}

function renderConceptosTable(conceptos) {
  if (!conceptos.length) return '<p class="modal-sub">No se encontraron conceptos en el XML.</p>';

  const rows = conceptos.map(c => `
    <tr>
      <td><code class="rfc">${c.claveProdServ}</code></td>
      <td class="nombre">${c.descripcion}</td>
      <td class="num">${c.cantidad}</td>
      <td>${c.unidad}</td>
      <td class="num">${fmt(c.valorUnitario)}</td>
      <td class="num">${fmt(c.importe)}</td>
      <td class="num">${fmt(c.importeIva)}</td>
    </tr>`).join('');

  return `
    <div class="table-wrap conceptos-table-wrap">
      <table class="invoice-table conceptos-table">
        <thead>
          <tr>
            <th>Clave Prod/Serv</th>
            <th>Descripción</th>
            <th class="num">Cantidad</th>
            <th>Unidad</th>
            <th class="num">Valor Unit.</th>
            <th class="num">Importe</th>
            <th class="num">IVA</th>
          </tr>
        </thead>
        <tbody>${rows}</tbody>
      </table>
    </div>`;
}

function renderDoctosTable(doctos) {
  if (!doctos.length) return '';
  const rows = doctos.map(d => `
    <tr>
      <td><span class="uuid-cell" title="${d.idDocumento}">${d.idDocumento.slice(0,13)}…</span></td>
      <td>${d.folio}</td>
      <td>${d.fechaPago.replace('T', ' ')}</td>
      <td>${d.formaPago}</td>
      <td class="num">${fmt(d.monto)} ${d.monedaPago}</td>
      <td class="num">${fmt(d.impSaldoAnt)}</td>
      <td class="num">${fmt(d.impPagado)}</td>
      <td class="num">${fmt(d.impSaldoInsoluto)}</td>
      <td class="num">${d.numParcialidad}</td>
    </tr>`).join('');

  return `
    <h3 class="conceptos-section-title">Documentos relacionados</h3>
    <div class="table-wrap conceptos-table-wrap">
      <table class="invoice-table conceptos-table">
        <thead>
          <tr>
            <th>UUID Doc.</th>
            <th>Folio</th>
            <th>Fecha Pago</th>
            <th>Forma</th>
            <th class="num">Monto</th>
            <th class="num">Saldo Ant.</th>
            <th class="num">Pagado</th>
            <th class="num">Saldo Rest.</th>
            <th class="num">Parcialidad</th>
          </tr>
        </thead>
        <tbody>${rows}</tbody>
      </table>
    </div>`;
}

async function showConceptos(uuid, fecha, rfcEmisor, rfcReceptor) {
  const modal      = document.getElementById('conceptosModal');
  const content    = document.getElementById('conceptosContent');
  const uuidLabel  = document.getElementById('conceptosUuidLabel');

  if (!modal || !content) return;

  uuidLabel.textContent = uuid;
  content.innerHTML = '<div class="conceptos-loading"><span class="toast-spinner"></span> Descargando XML…</div>';
  modal.classList.remove('hidden');

  try {
    const xmlText = await fetchXmlText(uuid, rfcEmisor, rfcReceptor, fecha);
    const parser  = new DOMParser();
    const doc     = parser.parseFromString(xmlText, 'application/xml');
    const conceptos = parseConceptos(doc);
    const doctos    = parseDoctos(doc);
    content.innerHTML = renderConceptosTable(conceptos) + renderDoctosTable(doctos);
  } catch (err) {
    content.innerHTML = `<p class="stream-error">Error: ${err.message}</p>`;
  }
}

/* Wire conceptos buttons on invoices.html (static rows) */
document.querySelectorAll('[data-conceptos-uuid]').forEach(btn => {
  btn.addEventListener('click', () => {
    showConceptos(
      btn.dataset.conceptosUuid,
      btn.dataset.fecha,
      btn.dataset.rfcEmisor   || '',
      btn.dataset.rfcReceptor || '',
    );
  });
});
